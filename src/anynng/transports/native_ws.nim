# native_ws.nim - Native Async RFC 6455 WebSocket Transport (ws://)
# Licensed under MIT License.

when not defined(js):
  import std/[asyncnet, asyncdispatch, base64, net, random, strutils]
  import ../core/types
  import ../core/tls
  import ../core/sha1
  import ../core/framing/ws_framing

  type WsListener* = ref object
    serverSock*: AsyncSocket
    closed*: bool
    url*: string
    proto*: SpProto
    boundPort*: int
    tlsCfg*: TlsConfig

  proc packWsFrame(opcode: byte, payload: openArray[byte], mask: bool): seq[byte] =
    result = @[]
    result.add(0x80'u8 or (opcode and 0x0f'u8)) # FIN=1
    let maskBit: byte = if mask: 0x80'u8 else: 0x00'u8

    if payload.len <= 125:
      result.add(maskBit or byte(payload.len))
    elif payload.len <= 65535:
      result.add(maskBit or 126'u8)
      result.add(byte((payload.len shr 8) and 0xff))
      result.add(byte(payload.len and 0xff))
    else:
      result.add(maskBit or 127'u8)
      for i in countdown(7, 0):
        result.add(byte((payload.len shr (i * 8)) and 0xff))

    if mask:
      var maskKey: array[4, byte]
      for i in 0 ..< 4:
        maskKey[i] = byte(rand(255))
      for i in 0 ..< 4:
        result.add(maskKey[i])
      for i in 0 ..< payload.len:
        result.add(payload[i] xor maskKey[i mod 4])
    else:
      for i in 0 ..< payload.len:
        result.add(payload[i])

  proc readWsFrame(sock: AsyncSocket): Future[(byte, seq[byte])] {.async.} =
    let hdr = await sock.recv(2)
    if hdr.len < 2:
      raise newException(ValueError, "Connection closed reading WS frame header")
    let b0 = byte(hdr[0])
    let b1 = byte(hdr[1])
    let opcode = b0 and 0x0f'u8
    let masked = (b1 and 0x80'u8) != 0
    var payloadLen = int(b1 and 0x7f'u8)

    if payloadLen == 126:
      let lenBytes = await sock.recv(2)
      if lenBytes.len < 2:
        raise newException(ValueError, "Truncated WS length (126)")
      payloadLen = (int(byte(lenBytes[0])) shl 8) or int(byte(lenBytes[1]))
    elif payloadLen == 127:
      let lenBytes = await sock.recv(8)
      if lenBytes.len < 8:
        raise newException(ValueError, "Truncated WS length (127)")
      payloadLen = 0
      for i in 0 ..< 8:
        payloadLen = (payloadLen shl 8) or int(byte(lenBytes[i]))

    var maskKey: array[4, byte]
    if masked:
      let m = await sock.recv(4)
      if m.len < 4:
        raise newException(ValueError, "Truncated WS mask")
      for i in 0 ..< 4:
        maskKey[i] = byte(m[i])

    var payload = newSeq[byte](payloadLen)
    var received = 0
    while received < payloadLen:
      let chunk = await sock.recv(min(4096, payloadLen - received))
      if chunk.len == 0:
        raise newException(ValueError, "Truncated WS payload bytes")
      for i in 0 ..< chunk.len:
        let rawByte = byte(chunk[i])
        payload[received + i] =
          if masked:
            rawByte xor maskKey[(received + i) mod 4]
          else:
            rawByte
      received += chunk.len

    return (opcode, payload)

  proc readHttpHeaders(sock: AsyncSocket): Future[seq[string]] {.async.} =
    result = @[]
    var buf = ""
    while not sock.isClosed:
      let chunk = await sock.recv(1024)
      if chunk.len == 0:
        break
      buf.add(chunk)
      let endIdx = buf.find("\r\n\r\n")
      if endIdx >= 0:
        let hdrText = buf[0 ..< endIdx]
        for line in hdrText.split("\r\n"):
          if line.len > 0:
            result.add(line)
        break

  proc computeWsAccept(clientKey: string): string =
    const Magic = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
    let combined = clientKey & Magic
    var combinedBytes = newSeq[byte](combined.len)
    if combined.len > 0:
      copyMem(addr combinedBytes[0], unsafeAddr combined[0], combined.len)
    let digest = sha1Digest(combinedBytes)
    return encode(digest)

  proc sendBytes(sock: AsyncSocket, data: seq[byte]) {.async.} =
    var s = newString(data.len)
    if data.len > 0:
      copyMem(addr s[0], unsafeAddr data[0], data.len)
    await sock.send(s)

  proc readWsLoop(
      sock: AsyncSocket,
      pid: PipeId,
      isClient: bool,
      onFrame: proc(pid: PipeId, frame: seq[byte]) {.closure.},
      onClose: proc(pid: PipeId) {.closure.},
  ) {.async.} =
    try:
      while not sock.isClosed:
        let (opcode, payload) = await readWsFrame(sock)
        if opcode == 0x2: # Binary
          if onFrame != nil:
            onFrame(pid, payload)
        elif opcode == 0x1: # Text - REJECT with 1003 per CRIT-02
          var closePayload: array[2, byte]
          closePayload[0] = byte((WsCloseUnsuppFormat shr 8) and 0xff'u16)
          closePayload[1] = byte(WsCloseUnsuppFormat and 0xff'u16)
          let frame = packWsFrame(0x8, closePayload, mask = isClient)
          await sock.sendBytes(frame)
          break
        elif opcode == 0x8: # Close
          # Echo close frame
          let frame = packWsFrame(0x8, payload, mask = isClient)
          await sock.sendBytes(frame)
          break
        elif opcode == 0x9: # Ping
          # Reply Pong with same payload
          let frame = packWsFrame(0xA, payload, mask = isClient)
          await sock.sendBytes(frame)
        elif opcode == 0xA: # Pong
          discard
    except CatchableError:
      discard
    finally:
      if onClose != nil:
        onClose(pid)
      if not sock.isClosed:
        sock.close()

  proc connectWsAsync*(
      host: string,
      port: int,
      path: string,
      proto: SpProto,
      onFrame: proc(pid: PipeId, frame: seq[byte]) {.closure.},
      onClose: proc(pid: PipeId) {.closure.},
      tlsCfg: TlsConfig = nil,
  ): Future[Pipe] {.async.} =
    let sock = newAsyncSocket(buffered = false)
    let isSecure = tlsCfg != nil
    if isSecure:
      let sslCtx = tlsCfg.toSslContext()
      sslCtx.wrapSocket(sock)

    await sock.connect(host, Port(port))

    # Generate random 16-byte nonce
    randomize()
    var nonce: array[16, byte]
    for i in 0 ..< 16:
      nonce[i] = byte(rand(255))
    let clientKey = encode(nonce)

    # CRIT-02: Dialer sends PEER protocol name in Sec-WebSocket-Protocol
    let subproto = dialerSubprotocol(proto)
    let wsPath = if path.len == 0: "/" else: path

    var req = "GET " & wsPath & " HTTP/1.1\r\n"
    req.add("Host: " & host & ":" & $port & "\r\n")
    req.add("Upgrade: websocket\r\n")
    req.add("Connection: Upgrade\r\n")
    req.add("Sec-WebSocket-Key: " & clientKey & "\r\n")
    req.add("Sec-WebSocket-Version: 13\r\n")
    req.add("Sec-WebSocket-Protocol: " & subproto & "\r\n")
    req.add("\r\n")

    await sock.send(req)

    # Read response headers
    let lines = await readHttpHeaders(sock)
    if lines.len == 0 or not lines[0].startsWith("HTTP/1.1 101"):
      sock.close()
      let status =
        if lines.len > 0:
          lines[0]
        else:
          "Empty response"
      raise newException(ValueError, "WebSocket upgrade failed: " & status)

    var acceptOk = false
    let expectedAccept = computeWsAccept(clientKey)
    for line in lines:
      let parts = line.split(":", 1)
      if parts.len == 2:
        let k = parts[0].strip().toLowerAscii()
        let v = parts[1].strip()
        if k == "sec-websocket-accept" and v == expectedAccept:
          acceptOk = true

    if not acceptOk:
      sock.close()
      raise newException(ValueError, "WebSocket Sec-WebSocket-Accept mismatch")

    let pid = allocPipeId()
    let sendCb = proc(data: openArray[byte]) =
      if not sock.isClosed:
        # Client sends MASKED frames (WARN-04)
        let frame = packWsFrame(0x2, data, mask = true)
        var sOut = newString(frame.len)
        if frame.len > 0:
          copyMem(addr sOut[0], unsafeAddr frame[0], frame.len)
        asyncCheck sock.send(sOut)

    let closeCb = proc() =
      if not sock.isClosed:
        sock.close()

    let pipe = newPipe(pid, proto, sendCb, closeCb)

    asyncCheck readWsLoop(sock, pid, isClient = true, onFrame, onClose)
    return pipe

  proc listenWsAsync*(
      host: string,
      port: int,
      path: string,
      proto: SpProto,
      onPipeAdded: proc(pipe: Pipe) {.closure.},
      onFrame: proc(pid: PipeId, frame: seq[byte]) {.closure.},
      onClose: proc(pid: PipeId) {.closure.},
      tlsCfg: TlsConfig = nil,
  ): Future[WsListener] {.async.} =
    let serverSock = newAsyncSocket(buffered = false)
    serverSock.setSockOpt(OptReuseAddr, true)
    serverSock.setSockOpt(OptReusePort, true)
    serverSock.bindAddr(Port(port), host)
    serverSock.listen(128)

    let (_, allocatedPort) = serverSock.getLocalAddr()
    let scheme = if tlsCfg != nil: "wss" else: "ws"

    let listener = WsListener(
      serverSock: serverSock,
      closed: false,
      url:
        scheme & "://" & host & ":" & $allocatedPort & (
          if path.len == 0: "/" else: path
        ),
      proto: proto,
      boundPort: int(allocatedPort),
      tlsCfg: tlsCfg,
    )

    # CRIT-02: Listener matches SELF protocol name
    let expectedSubproto = listenerSubprotocol(proto)

    proc acceptLoop() {.async.} =
      let sslCtx =
        if tlsCfg != nil:
          tlsCfg.toSslContext()
        else:
          nil
      while not listener.closed and not serverSock.isClosed:
        try:
          let clientSock = await serverSock.accept()
          if sslCtx != nil:
            sslCtx.wrapConnectedSocket(clientSock, handshake = handshakeAsServer)
          let lines = await readHttpHeaders(clientSock)
          if lines.len == 0:
            clientSock.close()
            continue

          var clientKey = ""
          var clientSubprotos: seq[string] = @[]
          for line in lines:
            let parts = line.split(":", 1)
            if parts.len == 2:
              let k = parts[0].strip().toLowerAscii()
              let v = parts[1].strip()
              if k == "sec-websocket-key":
                clientKey = v
              elif k == "sec-websocket-protocol":
                for sp in v.split(','):
                  clientSubprotos.add(sp.strip())

          # Check subprotocol match
          var matchedSubproto = ""
          for sp in clientSubprotos:
            if sp == expectedSubproto:
              matchedSubproto = sp
              break

          if clientKey.len == 0 or matchedSubproto.len == 0:
            # Reject with 400 Bad Request
            await clientSock.send(
              "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n"
            )
            clientSock.close()
            continue

          let acceptVal = computeWsAccept(clientKey)
          var resp = "HTTP/1.1 101 Switching Protocols\r\n"
          resp.add("Upgrade: websocket\r\n")
          resp.add("Connection: Upgrade\r\n")
          resp.add("Sec-WebSocket-Accept: " & acceptVal & "\r\n")
          resp.add("Sec-WebSocket-Protocol: " & matchedSubproto & "\r\n")
          resp.add("\r\n")

          await clientSock.send(resp)

          let pid = allocPipeId()
          let sendCb = proc(data: openArray[byte]) =
            if not clientSock.isClosed:
              # Server sends UNMASKED frames (WARN-04)
              let frame = packWsFrame(0x2, data, mask = false)
              var sOut = newString(frame.len)
              if frame.len > 0:
                copyMem(addr sOut[0], unsafeAddr frame[0], frame.len)
              asyncCheck clientSock.send(sOut)

          let closeCb = proc() =
            if not clientSock.isClosed:
              clientSock.close()

          let pipe = newPipe(pid, proto, sendCb, closeCb)

          if onPipeAdded != nil:
            onPipeAdded(pipe)
          asyncCheck readWsLoop(clientSock, pid, isClient = false, onFrame, onClose)
        except CatchableError:
          if listener.closed or serverSock.isClosed:
            break

    asyncCheck acceptLoop()
    return listener

  proc close*(listener: WsListener) =
    if not listener.closed:
      listener.closed = true
      if not listener.serverSock.isClosed:
        listener.serverSock.close()
