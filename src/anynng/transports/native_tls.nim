# native_tls.nim - Native Async TLS Transport over TCP (tls+tcp://, tls+tcp4://, tls+tcp6://)
# Licensed under MIT License.

when not defined(js):
  import std/[asyncnet, asyncdispatch, net]
  import ../core/types
  import ../core/tls
  import ../core/framing/stream_framing

  type TlsTcpListener* = ref object
    serverSock*: AsyncSocket
    closed*: bool
    url*: string
    proto*: SpProto
    tlsCfg*: TlsConfig
    boundPort*: int

  proc readTlsStreamLoop(
      sock: AsyncSocket,
      pid: PipeId,
      onFrame: proc(pid: PipeId, frame: seq[byte]) {.closure.},
      onClose: proc(pid: PipeId) {.closure.},
  ) {.async.} =
    var decoder = initStreamDecoder()
    try:
      while not sock.isClosed:
        let chunk = await sock.recv(4096)
        if chunk.len == 0:
          break
        var chunkBytes = newSeq[byte](chunk.len)
        for i in 0 ..< chunk.len:
          chunkBytes[i] = byte(chunk[i])
        decoder.feed(
          chunkBytes,
          proc(frame: seq[byte]) =
            if onFrame != nil:
              onFrame(pid, frame)
          ,
        )
    except CatchableError:
      discard
    finally:
      if onClose != nil:
        onClose(pid)
      if not sock.isClosed:
        sock.close()

  proc connectTlsTcpAsync*(
      host: string,
      port: int,
      proto: SpProto,
      tlsCfg: TlsConfig,
      onFrame: proc(pid: PipeId, frame: seq[byte]) {.closure.},
      onClose: proc(pid: PipeId) {.closure.},
  ): Future[Pipe] {.async.} =
    let sock = newAsyncSocket(buffered = false)
    sock.setSockOpt(OptNoDelay, true, level = cint(Protocol.IPPROTO_TCP))

    let cfg =
      if tlsCfg != nil:
        tlsCfg
      else:
        newTlsConfig(tlsModeClient)
    let sslCtx = cfg.toSslContext()
    sslCtx.wrapSocket(sock)

    await sock.connect(host, Port(port))

    # Perform NNG stream handshake over TLS
    let greeting = encodeGreeting(proto)
    var gStr = newString(8)
    for i in 0 ..< 8:
      gStr[i] = char(greeting[i])
    await sock.send(gStr)

    let peerGStr = await sock.recv(8)
    if peerGStr.len < 8:
      sock.close()
      raise newException(ValueError, "TLS+TCP peer closed during greeting handshake")

    var peerGBytes: array[8, byte]
    for i in 0 ..< 8:
      peerGBytes[i] = byte(peerGStr[i])

    let v = verifyGreeting(peerGBytes, peerProto(proto))
    if not v.ok:
      sock.close()
      raise newException(ValueError, "TLS+TCP greeting verification failed: " & $v.err)

    let pid = allocPipeId()
    let sendCb = proc(data: openArray[byte]) =
      if not sock.isClosed:
        let encoded = encodeStreamFrame(data)
        var sOut = newString(encoded.len)
        for i in 0 ..< encoded.len:
          sOut[i] = char(encoded[i])
        asyncCheck sock.send(sOut)

    let closeCb = proc() =
      if not sock.isClosed:
        sock.close()

    let pipe = newPipe(pid, proto, sendCb, closeCb)
    pipe.tlsVerified =
      (cfg.authMode == tlsAuthModeRequired or cfg.authMode == tlsAuthModeOptional)
    pipe.tlsPeerCn = cfg.serverName
    pipe.locAddr = "127.0.0.1"
    pipe.remAddr = host & ":" & $port

    asyncCheck readTlsStreamLoop(sock, pid, onFrame, onClose)
    return pipe

  proc listenTlsTcpAsync*(
      host: string,
      port: int,
      proto: SpProto,
      tlsCfg: TlsConfig,
      onPipeAdded: proc(pipe: Pipe) {.closure.},
      onFrame: proc(pid: PipeId, frame: seq[byte]) {.closure.},
      onClose: proc(pid: PipeId) {.closure.},
  ): Future[TlsTcpListener] {.async.} =
    let server = newAsyncSocket(buffered = false)
    server.setSockOpt(OptNoDelay, true, level = cint(Protocol.IPPROTO_TCP))
    server.setSockOpt(OptReuseAddr, true)
    server.bindAddr(Port(port), host)
    server.listen()

    let (_, allocatedPort) = server.getLocalAddr()

    new(result)
    result.serverSock = server
    result.closed = false
    result.proto = proto
    result.tlsCfg =
      if tlsCfg != nil:
        tlsCfg
      else:
        newTlsConfig(tlsModeServer)
    result.boundPort = int(allocatedPort)
    result.url = "tls+tcp://" & host & ":" & $result.boundPort

    let l = result

    # Accept loop
    proc acceptLoop() {.async.} =
      let sslCtx = l.tlsCfg.toSslContext()
      while not l.closed and not l.serverSock.isClosed:
        try:
          let client = await l.serverSock.accept()
          client.setSockOpt(OptNoDelay, true, level = cint(Protocol.IPPROTO_TCP))
          sslCtx.wrapConnectedSocket(client, handshake = handshakeAsServer)

          # Greeting handshake
          let peerGStr = await client.recv(8)
          if peerGStr.len < 8:
            client.close()
            continue

          var peerGBytes: array[8, byte]
          for i in 0 ..< 8:
            peerGBytes[i] = byte(peerGStr[i])

          let v = verifyGreeting(peerGBytes, peerProto(proto))
          if not v.ok:
            client.close()
            continue

          let greeting = encodeGreeting(proto)
          var gStr = newString(8)
          for i in 0 ..< 8:
            gStr[i] = char(greeting[i])
          await client.send(gStr)

          let pid = allocPipeId()
          let sendCb = proc(data: openArray[byte]) =
            if not client.isClosed:
              let encoded = encodeStreamFrame(data)
              var sOut = newString(encoded.len)
              for i in 0 ..< encoded.len:
                sOut[i] = char(encoded[i])
              asyncCheck client.send(sOut)

          let closeCb = proc() =
            if not client.isClosed:
              client.close()

          let pipe = newPipe(pid, proto, sendCb, closeCb)
          pipe.tlsVerified = (l.tlsCfg.authMode == tlsAuthModeRequired)
          pipe.locAddr = host & ":" & $l.boundPort

          if onPipeAdded != nil:
            onPipeAdded(pipe)

          asyncCheck readTlsStreamLoop(client, pid, onFrame, onClose)
        except CatchableError:
          if l.closed or l.serverSock.isClosed:
            break

    asyncCheck acceptLoop()

  proc close*(l: TlsTcpListener) =
    if not l.closed:
      l.closed = true
      if not l.serverSock.isClosed:
        l.serverSock.close()
