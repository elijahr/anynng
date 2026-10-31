# native_ipc.nim - Native Async Unix Domain Socket Transport (ipc://)
# Licensed under MIT License.

when not defined(js):
  import std/[asyncnet, asyncdispatch, nativesockets, net, posix, os]
  import ../core/types
  import ../core/framing/stream_framing

  type IpcListener* = ref object
    serverSock*: AsyncSocket
    closed*: bool
    path*: string
    proto*: SpProto

  proc readIpcStreamLoop(
      sock: AsyncSocket,
      pid: PipeId,
      onFrame: proc(pid: PipeId, frame: seq[byte]) {.closure.},
      onClose: proc(pid: PipeId) {.closure.},
  ) {.async.} =
    var decoder = initStreamDecoder(hasTypeByte = true)
    try:
      while not sock.isClosed:
        let chunk = await sock.recv(4096)
        if chunk.len == 0:
          break # EOF
        var chunkBytes = newSeq[byte](chunk.len)
        if chunk.len > 0:
          copyMem(addr chunkBytes[0], unsafeAddr chunk[0], chunk.len)
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

  proc connectIpcAsync*(
      path: string,
      proto: SpProto,
      onFrame: proc(pid: PipeId, frame: seq[byte]) {.closure.},
      onClose: proc(pid: PipeId) {.closure.},
  ): Future[Pipe] {.async.} =
    let cliFd =
      createNativeSocket(Domain.AF_UNIX, SockType.SOCK_STREAM, Protocol.IPPROTO_IP)
    if cliFd == osInvalidSocket:
      raiseOSError(osLastError())
    setBlocking(cliFd, false)

    var saddr: Sockaddr_un
    saddr.sun_family = AF_UNIX.TSa_Family
    copyMem(
      addr saddr.sun_path[0], cstring(path), min(path.len, sizeof(saddr.sun_path) - 1)
    )

    if posix.connect(cliFd, cast[ptr SockAddr](addr saddr), SockLen(sizeof(saddr))) < 0:
      let err = osLastError()
      if err.int != EINPROGRESS and err.int != 0:
        discard posix.close(cliFd)
        raiseOSError(err)

    asyncdispatch.register(cliFd.AsyncFD)
    let sock = newAsyncSocket(cliFd.AsyncFD, domain = Domain.AF_UNIX, buffered = false)

    # Perform NNG stream handshake (CRIT-01: byte 0 is strictly 0x00 per nng/ipc.c:604)
    let greeting = encodeGreeting(proto)
    var gStr = newString(8)
    for i in 0 ..< 8:
      gStr[i] = char(greeting[i])
    await sock.send(gStr)

    let peerGStr = await sock.recv(8)
    if peerGStr.len < 8:
      sock.close()
      raise newException(ValueError, "IPC peer closed during greeting handshake")

    var peerGBytes: array[8, byte]
    for i in 0 ..< 8:
      peerGBytes[i] = byte(peerGStr[i])

    let v = verifyGreeting(peerGBytes, peerProto(proto))
    if not v.ok:
      sock.close()
      raise newException(ValueError, "IPC greeting verification failed: " & $v.err)

    let pid = allocPipeId()
    let sendCb = proc(data: openArray[byte]) =
      if not sock.isClosed:
        let encoded = encodeStreamFrame(data, hasTypeByte = true)
        var sOut = newString(encoded.len)
        if encoded.len > 0:
          copyMem(addr sOut[0], unsafeAddr encoded[0], encoded.len)
        asyncCheck sock.send(sOut)

    let closeCb = proc() =
      if not sock.isClosed:
        sock.close()

    let pipe = newPipe(pid, proto, sendCb, closeCb)

    asyncCheck readIpcStreamLoop(sock, pid, onFrame, onClose)
    return pipe

  proc listenIpcAsync*(
      path: string,
      proto: SpProto,
      onPipeAdded: proc(pipe: Pipe) {.closure.},
      onFrame: proc(pid: PipeId, frame: seq[byte]) {.closure.},
      onClose: proc(pid: PipeId) {.closure.},
  ): Future[IpcListener] {.async.} =
    # Remove stale socket file if present (using unlink to remove socket special file)
    discard posix.unlink(cstring(path))

    let srvFd =
      createNativeSocket(Domain.AF_UNIX, SockType.SOCK_STREAM, Protocol.IPPROTO_IP)
    if srvFd == osInvalidSocket:
      raiseOSError(osLastError())
    setBlocking(srvFd, false)

    var saddr: Sockaddr_un
    saddr.sun_family = AF_UNIX.TSa_Family
    copyMem(
      addr saddr.sun_path[0], cstring(path), min(path.len, sizeof(saddr.sun_path) - 1)
    )

    if posix.bindSocket(srvFd, cast[ptr SockAddr](addr saddr), SockLen(sizeof(saddr))) <
        0:
      let err = osLastError()
      discard posix.close(srvFd)
      raiseOSError(err)

    if posix.listen(srvFd, 128.cint) < 0:
      let err = osLastError()
      discard posix.close(srvFd)
      raiseOSError(err)

    asyncdispatch.register(srvFd.AsyncFD)
    let serverSock =
      newAsyncSocket(srvFd.AsyncFD, domain = Domain.AF_UNIX, buffered = false)

    let listener =
      IpcListener(serverSock: serverSock, closed: false, path: path, proto: proto)

    proc acceptLoop() {.async.} =
      while not listener.closed and not serverSock.isClosed:
        try:
          let clientSock = await serverSock.accept()
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
              let encoded = encodeStreamFrame(data, hasTypeByte = true)
              var sOut = newString(encoded.len)
              if encoded.len > 0:
                copyMem(addr sOut[0], unsafeAddr encoded[0], encoded.len)
              asyncCheck clientSock.send(sOut)

          let closeCb = proc() =
            if not clientSock.isClosed:
              clientSock.close()

          let pipe = newPipe(pid, proto, sendCb, closeCb)

          if onPipeAdded != nil:
            onPipeAdded(pipe)
          asyncCheck readIpcStreamLoop(clientSock, pid, onFrame, onClose)
        except CatchableError:
          if listener.closed or serverSock.isClosed:
            break

    asyncCheck acceptLoop()
    return listener

  proc close*(listener: IpcListener) =
    if not listener.closed:
      listener.closed = true
      if not listener.serverSock.isClosed:
        listener.serverSock.close()
      discard posix.unlink(cstring(listener.path))
