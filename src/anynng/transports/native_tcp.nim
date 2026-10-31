# native_tcp.nim - Native Async TCP Transport (tcp://, tcp4://, tcp6://)
# Licensed under MIT License.

when not defined(js):
  import std/[asyncnet, asyncdispatch, net]
  import ../core/types
  import ../core/framing/stream_framing

  type TcpListener* = ref object
    serverSock*: AsyncSocket
    closed*: bool
    url*: string
    proto*: SpProto

  proc readStreamLoop(
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
          break # EOF
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

  proc connectTcpAsync*(
      host: string,
      port: int,
      proto: SpProto,
      onFrame: proc(pid: PipeId, frame: seq[byte]) {.closure.},
      onClose: proc(pid: PipeId) {.closure.},
  ): Future[Pipe] {.async.} =
    let sock = newAsyncSocket(buffered = false)
    sock.setSockOpt(OptNoDelay, true, level = cint(Protocol.IPPROTO_TCP))
    await sock.connect(host, Port(port))

    # Perform NNG stream handshake
    let greeting = encodeGreeting(proto)
    var gStr = newString(8)
    for i in 0 ..< 8:
      gStr[i] = char(greeting[i])
    await sock.send(gStr)

    let peerGStr = await sock.recv(8)
    if peerGStr.len < 8:
      sock.close()
      raise newException(ValueError, "TCP peer closed during greeting handshake")

    var peerGBytes: array[8, byte]
    for i in 0 ..< 8:
      peerGBytes[i] = byte(peerGStr[i])

    let v = verifyGreeting(peerGBytes, peerProto(proto))
    if not v.ok:
      sock.close()
      raise newException(ValueError, "TCP greeting verification failed: " & $v.err)

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

    asyncCheck readStreamLoop(sock, pid, onFrame, onClose)
    return pipe

  proc listenTcpAsync*(
      host: string,
      port: int,
      proto: SpProto,
      onPipeAdded: proc(pipe: Pipe) {.closure.},
      onFrame: proc(pid: PipeId, frame: seq[byte]) {.closure.},
      onClose: proc(pid: PipeId) {.closure.},
  ): Future[TcpListener] {.async.} =
    let serverSock = newAsyncSocket(buffered = false)
    serverSock.setSockOpt(OptReuseAddr, true)
    serverSock.setSockOpt(OptReusePort, true)
    serverSock.bindAddr(Port(port), host)
    serverSock.listen(128)

    let listener = TcpListener(
      serverSock: serverSock,
      closed: false,
      url: "tcp://" & host & ":" & $port,
      proto: proto,
    )

    proc acceptLoop() {.async.} =
      while not listener.closed and not serverSock.isClosed:
        try:
          let clientSock = await serverSock.accept()
          clientSock.setSockOpt(OptNoDelay, true, level = cint(Protocol.IPPROTO_TCP))
          # Handshake
          let greeting = encodeGreeting(proto)
          var gStr = newString(8)
          for i in 0 ..< 8:
            gStr[i] = char(greeting[i])
          await clientSock.send(gStr)

          let peerGStr = await clientSock.recv(8)
          if peerGStr.len < 8:
            clientSock.close()
            continue

          var peerGBytes: array[8, byte]
          for i in 0 ..< 8:
            peerGBytes[i] = byte(peerGStr[i])

          let v = verifyGreeting(peerGBytes, peerProto(proto))
          if not v.ok:
            clientSock.close()
            continue

          let pid = allocPipeId()
          let sendCb = proc(data: openArray[byte]) =
            if not clientSock.isClosed:
              let encoded = encodeStreamFrame(data)
              var sOut = newString(encoded.len)
              for i in 0 ..< encoded.len:
                sOut[i] = char(encoded[i])
              asyncCheck clientSock.send(sOut)

          let closeCb = proc() =
            if not clientSock.isClosed:
              clientSock.close()

          let pipe = newPipe(pid, proto, sendCb, closeCb)

          if onPipeAdded != nil:
            onPipeAdded(pipe)
          asyncCheck readStreamLoop(clientSock, pid, onFrame, onClose)
        except CatchableError:
          if listener.closed or serverSock.isClosed:
            break

    asyncCheck acceptLoop()
    return listener

  proc close*(listener: TcpListener) =
    if not listener.closed:
      listener.closed = true
      if not listener.serverSock.isClosed:
        listener.serverSock.close()
