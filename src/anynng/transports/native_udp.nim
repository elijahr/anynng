# native_udp.nim - Native Async NNG v2 UDP Transport (udp://, udp4://, udp6://)
# Licensed under MIT License.

when not defined(js):
  import std/[asyncnet, asyncdispatch, net, tables]
  import ../core/types
  import ../core/framing/udp_framing

  type UdpListener* = ref object
    sock*: AsyncSocket
    closed*: bool
    url*: string
    proto*: SpProto
    pipes*: Table[string, Pipe] # Key: "host:port"

  proc connectUdpAsync*(
      host: string,
      port: int,
      proto: SpProto,
      onFrame: proc(pid: PipeId, frame: seq[byte]) {.closure.},
      onClose: proc(pid: PipeId) {.closure.},
  ): Future[Pipe] {.async.} =
    let sock = newAsyncSocket(
      domain = Domain.AF_INET,
      sockType = SockType.SOCK_DGRAM,
      protocol = Protocol.IPPROTO_UDP,
      buffered = false,
    )

    # Send CREQ packet
    var creqHdr: UdpSpHeader
    creqHdr.ver = UdpSpVer
    creqHdr.opCode = UdpOpCreq
    creqHdr.protoType = uint16(proto)
    let creqBytes = encodeUdpSpHeader(creqHdr)
    var creqStr = newString(8)
    for i in 0 ..< 8:
      creqStr[i] = char(creqBytes[i])

    await sock.sendTo(host, Port(port), creqStr)

    # Await CACK packet
    let resp = await sock.recvFrom(512)
    if resp.data.len < 8:
      sock.close()
      raise newException(ValueError, "UDP peer sent invalid response to CREQ")

    var respBytes: array[8, byte]
    for i in 0 ..< 8:
      respBytes[i] = byte(resp.data[i])
    let cackHdr = decodeUdpSpHeader(respBytes)
    if cackHdr.opCode != UdpOpCack:
      sock.close()
      raise newException(
        ValueError, "UDP expected CACK (0x02) but received opcode: " & $cackHdr.opCode
      )

    let pid = allocPipeId()
    let sendCb = proc(data: openArray[byte]) =
      if not sock.isClosed:
        let pkt = encodeUdpDataFrame(proto, SpMsg(body: @data))
        var sOut = newString(pkt.len)
        if pkt.len > 0:
          copyMem(addr sOut[0], unsafeAddr pkt[0], pkt.len)
        asyncCheck sock.sendTo(host, Port(port), sOut)

    let closeCb = proc() =
      if not sock.isClosed:
        sock.close()

    let pipe = newPipe(pid, proto, sendCb, closeCb)

    proc dialerReadLoop() {.async.} =
      try:
        while not sock.isClosed:
          let dgram = await sock.recvFrom(65536)
          if dgram.data.len >= 8:
            var buf = newSeq[byte](dgram.data.len)
            for i in 0 ..< dgram.data.len:
              buf[i] = byte(dgram.data[i])
            let (hdr, payload) = decodeUdpDataFrame(buf)
            if hdr.opCode == UdpOpData:
              if onFrame != nil:
                onFrame(pid, payload)
            elif hdr.opCode == UdpOpDisc:
              break
      except CatchableError:
        discard
      finally:
        if onClose != nil:
          onClose(pid)
        if not sock.isClosed:
          sock.close()

    asyncCheck dialerReadLoop()
    return pipe

  proc listenUdpAsync*(
      host: string,
      port: int,
      proto: SpProto,
      onPipeAdded: proc(pipe: Pipe) {.closure.},
      onFrame: proc(pid: PipeId, frame: seq[byte]) {.closure.},
      onClose: proc(pid: PipeId) {.closure.},
  ): Future[UdpListener] {.async.} =
    let sock = newAsyncSocket(
      domain = Domain.AF_INET,
      sockType = SockType.SOCK_DGRAM,
      protocol = Protocol.IPPROTO_UDP,
      buffered = false,
    )
    sock.setSockOpt(OptReuseAddr, true)
    sock.setSockOpt(OptReusePort, true)
    sock.bindAddr(Port(port), host)

    let listener = UdpListener(
      sock: sock,
      closed: false,
      url: "udp://" & host & ":" & $port,
      proto: proto,
      pipes: initTable[string, Pipe](),
    )

    proc serverLoop() {.async.} =
      while not listener.closed and not sock.isClosed:
        try:
          let dgram = await sock.recvFrom(65536)
          let clientKey = dgram.address & ":" & $dgram.port.int
          if dgram.data.len >= 8:
            var buf = newSeq[byte](dgram.data.len)
            for i in 0 ..< dgram.data.len:
              buf[i] = byte(dgram.data[i])
            let hdr = decodeUdpSpHeader(buf)

            if hdr.opCode == UdpOpCreq:
              # Check peer protocol compatibility
              let (valid, p) = toSpProto(hdr.protoType)
              if valid and peerProto(p) == proto:
                # Reply CACK
                var cackHdr: UdpSpHeader
                cackHdr.ver = UdpSpVer
                cackHdr.opCode = UdpOpCack
                cackHdr.protoType = uint16(proto)
                let cackBytes = encodeUdpSpHeader(cackHdr)
                var cackStr = newString(8)
                for i in 0 ..< 8:
                  cackStr[i] = char(cackBytes[i])
                await sock.sendTo(dgram.address, dgram.port, cackStr)

                let pid = allocPipeId()
                let targetAddr = dgram.address
                let targetPort = dgram.port
                let sendCb = proc(data: openArray[byte]) =
                  if not sock.isClosed:
                    let pkt = encodeUdpDataFrame(proto, SpMsg(body: @data))
                    var sOut = newString(pkt.len)
                    if pkt.len > 0:
                      copyMem(addr sOut[0], unsafeAddr pkt[0], pkt.len)
                    asyncCheck sock.sendTo(targetAddr, targetPort, sOut)

                let closeCb = proc() =
                  listener.pipes.del(clientKey)

                let pipe = newPipe(pid, proto, sendCb, closeCb)
                listener.pipes[clientKey] = pipe
                if onPipeAdded != nil:
                  onPipeAdded(pipe)
            elif hdr.opCode == UdpOpData:
              if clientKey in listener.pipes:
                let pipe = listener.pipes[clientKey]
                let (_, payload) = decodeUdpDataFrame(buf)
                if onFrame != nil:
                  onFrame(pipe.pipeId, payload)
            elif hdr.opCode == UdpOpDisc:
              if clientKey in listener.pipes:
                let pipe = listener.pipes[clientKey]
                listener.pipes.del(clientKey)
                if onClose != nil:
                  onClose(pipe.pipeId)
        except CatchableError:
          if listener.closed or sock.isClosed:
            break

    asyncCheck serverLoop()
    return listener

  proc close*(listener: UdpListener) =
    if not listener.closed:
      listener.closed = true
      if not listener.sock.isClosed:
        listener.sock.close()
