# anynng - Pure-Nim multi-target (C, JS, WASM) and multi-transport NNG / Scalability Protocols
# Licensed under MIT License.

import std/tables
import anynng/core/types
import anynng/core/endian
import anynng/core/url
import anynng/core/framing/[stream_framing, ws_framing, udp_framing]
import anynng/core/protocols/[pair0, pair1, pubsub0, reqrep0, pipeline0, survey0, bus0]
import anynng/transports/inproc

when not defined(js):
  import std/[asyncdispatch, monotimes, times]
  import anynng/transports/[native_tcp, native_ipc, native_udp, native_ws]
  export native_tcp, native_ipc, native_udp, native_ws

export types, endian, url, stream_framing, ws_framing, udp_framing

const projectVersion* = "0.0.1"

type
  EngineVariantKind = enum
    engPair0
    engPair1
    engPub0
    engSub0
    engReq0
    engRep0
    engPush0
    engPull0
    engSurveyor0
    engRespondent0
    engBus0

  Socket* = ref object
    proto*: SpProto
    engineKind: EngineVariantKind
    pair0Eng: Pair0Engine
    pair1Eng: Pair1Engine
    pub0Eng: Pub0Engine
    sub0Eng: Sub0Engine
    req0Eng: Req0Engine
    rep0Eng: Rep0Engine
    push0Eng: Push0Engine
    pull0Eng: Pull0Engine
    surveyor0Eng: Surveyor0Engine
    respondent0Eng: Respondent0Engine
    bus0Eng: Bus0Engine
    pipes: Table[PipeId, Pipe]
    recvQueue: seq[SpMsg]
    onRecvCb: proc(msg: SpMsg) {.closure.}
    closed*: bool
    raw*: bool

  Listener* = ref object
    socket*: Socket
    url*: string
    closed*: bool
    closeImpl: proc() {.closure.}

  Context* = ref object
    socket*: Socket
    ctxId*: uint32

var gNextCtxId: uint32 = 1'u32

proc allocCtxId(): uint32 =
  result = gNextCtxId
  inc gNextCtxId

proc newSocket*(proto: SpProto, raw: bool = false): Socket =
  new(result)
  result.proto = proto
  result.raw = raw
  result.closed = false
  result.recvQueue = @[]
  result.pipes = initTable[PipeId, Pipe]()

  case proto
  of spPair0:
    result.engineKind = engPair0
    result.pair0Eng = initPair0Engine(raw)
  of spPair1:
    result.engineKind = engPair1
    result.pair1Eng = initPair1Engine(false, raw)
  of spPub0:
    result.engineKind = engPub0
    result.pub0Eng = initPub0Engine(raw)
  of spSub0:
    result.engineKind = engSub0
    result.sub0Eng = initSub0Engine(raw)
  of spReq0:
    result.engineKind = engReq0
    result.req0Eng = initReq0Engine(raw)
  of spRep0:
    result.engineKind = engRep0
    result.rep0Eng = initRep0Engine(raw)
  of spPush0:
    result.engineKind = engPush0
    result.push0Eng = initPush0Engine(raw)
  of spPull0:
    result.engineKind = engPull0
    result.pull0Eng = initPull0Engine(raw)
  of spSurveyor0:
    result.engineKind = engSurveyor0
    result.surveyor0Eng = initSurveyor0Engine(raw)
  of spRespondent0:
    result.engineKind = engRespondent0
    result.respondent0Eng = initRespondent0Engine(raw)
  of spBus0:
    result.engineKind = engBus0
    result.bus0Eng = initBus0Engine(raw)

proc executeActions*(s: Socket, actions: seq[EngineAction]) =
  for act in actions:
    case act.kind
    of actSendPipe:
      if act.pipeId in s.pipes:
        s.pipes[act.pipeId].send(act.rawFrame)
    of actClosePipe:
      if act.pipeId in s.pipes:
        let p = s.pipes[act.pipeId]
        s.pipes.del(act.pipeId)
        p.close()
    of actDeliverRecv:
      if s.onRecvCb != nil:
        let cb = s.onRecvCb
        s.onRecvCb = nil
        cb(act.msg)
      else:
        s.recvQueue.add(act.msg)
    of actFinishSend:
      discard
    of actFailContext:
      if act.err != spOk:
        raise newException(ValueError, "Operation rejected by protocol: " & $act.err)

proc stepEngine*(s: Socket, ev: EngineEvent): seq[EngineAction] =
  case s.engineKind
  of engPair0:
    s.pair0Eng.step(ev)
  of engPair1:
    s.pair1Eng.step(ev)
  of engPub0:
    s.pub0Eng.step(ev)
  of engSub0:
    s.sub0Eng.step(ev)
  of engReq0:
    s.req0Eng.step(ev)
  of engRep0:
    s.rep0Eng.step(ev)
  of engPush0:
    s.push0Eng.step(ev)
  of engPull0:
    s.pull0Eng.step(ev)
  of engSurveyor0:
    s.surveyor0Eng.step(ev)
  of engRespondent0:
    s.respondent0Eng.step(ev)
  of engBus0:
    s.bus0Eng.step(ev)

proc attachCustomPipe*(s: Socket, pipe: Pipe) =
  s.pipes[pipe.pipeId] = pipe
  let pid = pipe.pipeId
  let ev = EngineEvent(kind: evPipeAdded, pipeId: pid)
  let actions = s.stepEngine(ev)
  s.executeActions(actions)

proc detachCustomPipe*(s: Socket, pipeId: PipeId) =
  if pipeId in s.pipes:
    s.pipes.del(pipeId)
    let ev = EngineEvent(kind: evPipeRemoved, pipeId: pipeId)
    let actions = s.stepEngine(ev)
    s.executeActions(actions)

proc feedPipeFrame*(s: Socket, pipeId: PipeId, frame: openArray[byte]) =
  if s.closed:
    return
  let ev = EngineEvent(kind: evPipeMessage, pipeId: pipeId, rawFrame: @frame)
  let actions = s.stepEngine(ev)
  s.executeActions(actions)

proc attachEndpoint(s: Socket, ep: InprocEndpoint) =
  let pid = ep.pipeId
  ep.onMessage = proc(rawFrame: seq[byte]) =
    s.feedPipeFrame(pid, rawFrame)

  ep.onClose = proc() =
    s.detachCustomPipe(pid)

  let pipe = newPipe(
    pid,
    s.proto,
    proc(data: openArray[byte]) =
      ep.send(data),
    proc() =
      ep.close(),
  )
  s.attachCustomPipe(pipe)

proc dial*(s: Socket, urlStr: string) =
  let parsed = parseSpUrl(urlStr)
  case parsed.kind
  of tranInproc:
    let ep = connectInproc(
      parsed.raw,
      s.proto,
      proc(rawFrame: seq[byte]) =
        discard,
      proc() =
        discard,
    )
    s.attachEndpoint(ep)
  of tranTcp:
    when not defined(js):
      let host = if parsed.host.len > 0: parsed.host else: "127.0.0.1"
      let pipe = waitFor connectTcpAsync(
        host,
        parsed.port,
        s.proto,
        proc(pid: PipeId, frame: seq[byte]) =
          s.feedPipeFrame(pid, frame),
        proc(pid: PipeId) =
          s.detachCustomPipe(pid),
      )
      s.attachCustomPipe(pipe)
    else:
      raise
        newException(ValueError, "TCP transport is only available on native C target")
  of tranIpc:
    when not defined(js):
      let pipe = waitFor connectIpcAsync(
        parsed.path,
        s.proto,
        proc(pid: PipeId, frame: seq[byte]) =
          s.feedPipeFrame(pid, frame),
        proc(pid: PipeId) =
          s.detachCustomPipe(pid),
      )
      s.attachCustomPipe(pipe)
    else:
      raise
        newException(ValueError, "IPC transport is only available on native C target")
  of tranUdp:
    when not defined(js):
      let host = if parsed.host.len > 0: parsed.host else: "127.0.0.1"
      let pipe = waitFor connectUdpAsync(
        host,
        parsed.port,
        s.proto,
        proc(pid: PipeId, frame: seq[byte]) =
          s.feedPipeFrame(pid, frame),
        proc(pid: PipeId) =
          s.detachCustomPipe(pid),
      )
      s.attachCustomPipe(pipe)
    else:
      raise
        newException(ValueError, "UDP transport is only available on native C target")
  of tranWs:
    when not defined(js):
      let host = if parsed.host.len > 0: parsed.host else: "127.0.0.1"
      let pipe = waitFor connectWsAsync(
        host,
        parsed.port,
        parsed.path,
        s.proto,
        proc(pid: PipeId, frame: seq[byte]) =
          s.feedPipeFrame(pid, frame),
        proc(pid: PipeId) =
          s.detachCustomPipe(pid),
      )
      s.attachCustomPipe(pipe)
    else:
      raise newException(
        ValueError, "Native WebSocket dialer only on C target (use js_ws on js)"
      )

proc listen*(s: Socket, urlStr: string): Listener =
  let parsed = parseSpUrl(urlStr)
  new(result)
  result.socket = s
  result.url = urlStr
  result.closed = false

  case parsed.kind
  of tranInproc:
    let inprocListener = registerInprocListener(
      parsed.raw,
      s.proto,
      proc(ep: InprocEndpoint) =
        s.attachEndpoint(ep),
    )
    result.closeImpl = proc() =
      inprocListener.close()
  of tranTcp:
    when not defined(js):
      let host = if parsed.host.len > 0: parsed.host else: "127.0.0.1"
      let tcpListener = waitFor listenTcpAsync(
        host,
        parsed.port,
        s.proto,
        proc(pipe: Pipe) =
          s.attachCustomPipe(pipe),
        proc(pid: PipeId, frame: seq[byte]) =
          s.feedPipeFrame(pid, frame),
        proc(pid: PipeId) =
          s.detachCustomPipe(pid),
      )
      result.closeImpl = proc() =
        tcpListener.close()
    else:
      raise newException(ValueError, "TCP listener only available on native C target")
  of tranIpc:
    when not defined(js):
      let ipcListener = waitFor listenIpcAsync(
        parsed.path,
        s.proto,
        proc(pipe: Pipe) =
          s.attachCustomPipe(pipe),
        proc(pid: PipeId, frame: seq[byte]) =
          s.feedPipeFrame(pid, frame),
        proc(pid: PipeId) =
          s.detachCustomPipe(pid),
      )
      result.closeImpl = proc() =
        ipcListener.close()
    else:
      raise newException(ValueError, "IPC listener only available on native C target")
  of tranUdp:
    when not defined(js):
      let host = if parsed.host.len > 0: parsed.host else: "127.0.0.1"
      let udpListener = waitFor listenUdpAsync(
        host,
        parsed.port,
        s.proto,
        proc(pipe: Pipe) =
          s.attachCustomPipe(pipe),
        proc(pid: PipeId, frame: seq[byte]) =
          s.feedPipeFrame(pid, frame),
        proc(pid: PipeId) =
          s.detachCustomPipe(pid),
      )
      result.closeImpl = proc() =
        udpListener.close()
    else:
      raise newException(ValueError, "UDP listener only available on native C target")
  of tranWs:
    when not defined(js):
      let host = if parsed.host.len > 0: parsed.host else: "127.0.0.1"
      let wsListener = waitFor listenWsAsync(
        host,
        parsed.port,
        parsed.path,
        s.proto,
        proc(pipe: Pipe) =
          s.attachCustomPipe(pipe),
        proc(pid: PipeId, frame: seq[byte]) =
          s.feedPipeFrame(pid, frame),
        proc(pid: PipeId) =
          s.detachCustomPipe(pid),
      )
      result.closeImpl = proc() =
        wsListener.close()
    else:
      raise newException(ValueError, "WS listener only available on native C target")

proc send*(s: Socket, data: openArray[byte], ctxId: uint32 = 0) =
  if s.closed:
    raise newException(ValueError, "Socket is closed")
  let cid =
    if ctxId != 0:
      ctxId
    else:
      allocCtxId()
  let msg = initSpMsg(data)
  let ev = EngineEvent(kind: evUserSend, ctxId: cid, msg: msg)
  let actions = s.stepEngine(ev)
  s.executeActions(actions)

proc send*(s: Socket, str: string) =
  var bytes = newSeq[byte](str.len)
  for i in 0 ..< str.len:
    bytes[i] = byte(str[i])
  s.send(bytes)

proc recvMsg*(s: Socket, timeoutMs: int = 5000): SpMsg =
  if s.closed:
    raise newException(ValueError, "Socket is closed")
  when not defined(js):
    let start = getMonoTime()
    while s.recvQueue.len == 0 and not s.closed:
      asyncdispatch.poll(10)
      if timeoutMs >= 0:
        let elapsed = (getMonoTime() - start).inMilliseconds
        if elapsed > timeoutMs:
          raise newException(
            SpTimeoutError, "Socket recvMsg timed out after " & $timeoutMs & "ms"
          )
  if s.recvQueue.len > 0:
    result = s.recvQueue[0]
    s.recvQueue.delete(0)
  else:
    raise newException(ValueError, "No message ready in synchronous recvMsg")

proc recv*(s: Socket, timeoutMs: int = 5000): seq[byte] =
  result = s.recvMsg(timeoutMs).body

proc recvStr*(s: Socket, timeoutMs: int = 5000): string =
  let body = s.recv(timeoutMs)
  result = newString(body.len)
  for i in 0 ..< body.len:
    result[i] = char(body[i])

proc poll*(s: Socket, timeoutMs: int = 0): bool =
  if s.closed:
    return false
  when not defined(js):
    asyncdispatch.poll(timeoutMs)
  return s.recvQueue.len > 0

proc hasRecv*(s: Socket): bool =
  s.recvQueue.len > 0

proc subscribe*(s: Socket, topic: openArray[byte]) =
  if s.engineKind == engSub0:
    s.sub0Eng.subscribe(topic)
  else:
    raise newException(ValueError, "subscribe() is only supported on SUB0 sockets")

proc subscribe*(s: Socket, topic: string) =
  var bytes = newSeq[byte](topic.len)
  for i in 0 ..< topic.len:
    bytes[i] = byte(topic[i])
  s.subscribe(bytes)

proc unsubscribe*(s: Socket, topic: openArray[byte]) =
  if s.engineKind == engSub0:
    s.sub0Eng.unsubscribe(topic)
  else:
    raise newException(ValueError, "unsubscribe() is only supported on SUB0 sockets")

proc unsubscribe*(s: Socket, topic: string) =
  var bytes = newSeq[byte](topic.len)
  for i in 0 ..< topic.len:
    bytes[i] = byte(topic[i])
  s.unsubscribe(bytes)

proc close*(s: Socket) =
  if not s.closed:
    s.closed = true
    var pipesToClose: seq[Pipe] = @[]
    for p in s.pipes.values:
      pipesToClose.add(p)
    s.pipes.clear()
    for p in pipesToClose:
      p.close()

proc close*(l: Listener) =
  if not l.closed:
    l.closed = true
    if l.closeImpl != nil:
      l.closeImpl()
    l.socket.close()

# Convenience Socket Constructors
proc openPair0*(): Socket =
  newSocket(spPair0)

proc openPair1*(polyamorous: bool = false): Socket =
  result = newSocket(spPair1)
  result.pair1Eng.polyamorous = polyamorous

proc openPub0*(): Socket =
  newSocket(spPub0)

proc openSub0*(): Socket =
  newSocket(spSub0)

proc openReq0*(): Socket =
  newSocket(spReq0)

proc openRep0*(): Socket =
  newSocket(spRep0)

proc openPush0*(): Socket =
  newSocket(spPush0)

proc openPull0*(): Socket =
  newSocket(spPull0)

proc openSurveyor0*(): Socket =
  newSocket(spSurveyor0)

proc openRespondent0*(): Socket =
  newSocket(spRespondent0)

proc openBus0*(): Socket =
  newSocket(spBus0)
