# anynng - Pure-Nim multi-target (C, JS, WASM) and multi-transport NNG / Scalability Protocols
# Licensed under MIT License.

import std/[tables, deques]
import anynng/core/[types, options, tls, endian, url]
import anynng/core/framing/[stream_framing, ws_framing, udp_framing]
import anynng/core/protocols/[pair0, pair1, pubsub0, reqrep0, pipeline0, survey0, bus0]
import anynng/transports/inproc

when not defined(js):
  import std/[asyncdispatch, monotimes, times, net, nativesockets, posix, os]
  import anynng/transports/[native_tcp, native_ipc, native_udp, native_ws, native_tls]
  export native_tcp, native_ipc, native_udp, native_ws, native_tls
else:
  import anynng/transports/js_ws
  export js_ws

export types, options, tls, endian, url, stream_framing, ws_framing, udp_framing

const projectVersion* = "0.0.1"

proc currentNowMs*(): int64 =
  when defined(js):
    int64(jsNow())
  else:
    getMonoTime().ticks div 1_000_000

type
  EngineVariantKind* = enum
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
    id*: uint32
    proto*: SpProto
    engineKind*: EngineVariantKind
    pair0Eng*: Pair0Engine
    pair1Eng*: Pair1Engine
    pub0Eng*: Pub0Engine
    sub0Eng*: Sub0Engine
    req0Eng*: Req0Engine
    rep0Eng*: Rep0Engine
    push0Eng*: Push0Engine
    pull0Eng*: Pull0Engine
    surveyor0Eng*: Surveyor0Engine
    respondent0Eng*: Respondent0Engine
    bus0Eng*: Bus0Engine
    pipes*: Table[PipeId, Pipe]
    recvQueue*: Deque[SpMsg]
    sendQueue*: Deque[SpMsg]
    inFlight*: int
    lastErr*: cint
    onRecvCb*: proc(msg: SpMsg) {.closure.}
    pendingRecvCb*: proc(res: int, msg: SpMsg) {.closure.}
    deviceHook*: proc(msg: SpMsg) {.closure.}
    closed*: bool
    raw*: bool
    options*: OptionStore
    pipeCallbacks*: Table[PipeEvent, PipeNotifyCb]
    contexts*: Table[uint32, Context]
    when not defined(js):
      recvFds*: array[2, cint]
      sendFds*: array[2, cint]
      recvFdReadable*: bool
      sendFdWritable*: bool

  Dialer* = ref object
    socket*: Socket
    url*: string
    options*: OptionStore
    tlsCfg*: TlsConfig
    closed*: bool
    started*: bool
    closeImpl*: proc() {.closure.}

  Listener* = ref object
    socket*: Socket
    url*: string
    boundPort*: int
    options*: OptionStore
    tlsCfg*: TlsConfig
    closed*: bool
    started*: bool
    closeImpl*: proc() {.closure.}

  Context* = ref object
    socket*: Socket
    ctxId*: uint32
    options*: OptionStore
    closed*: bool
    lastErr*: cint
    recvQueue*: Deque[SpMsg]
    onRecvCb*: proc(msg: SpMsg) {.closure.}
    pendingRecvCb*: proc(res: int, msg: SpMsg) {.closure.}
    sub0Topics*: seq[seq[byte]]

func matchesTopicList*(topics: seq[seq[byte]], frame: openArray[byte]): bool =
  if topics.len == 0:
    return false
  for sub in topics:
    if sub.len == 0:
      return true
    if frame.len >= sub.len:
      if equalMem(unsafeAddr frame[0], unsafeAddr sub[0], sub.len):
        return true
  return false

var gNextCtxId: uint32 = 1'u32
var gOnPipeHook*: proc() {.closure.}
var gPendingAioCountProc*: proc(sockId: uint32): int {.closure.}

proc allocCtxId(): uint32 =
  result = gNextCtxId
  inc gNextCtxId

# Forward Declarations
proc executeActions*(s: Socket, actions: seq[EngineAction])
proc stepEngine*(s: Socket, ev: EngineEvent): seq[EngineAction]
proc attachCustomPipe*(s: Socket, pipe: Pipe)
proc detachCustomPipe*(s: Socket, pipeId: PipeId)
proc feedPipeFrame*(s: Socket, pipeId: PipeId, frame: openArray[byte])
proc close*(s: Socket)
proc updateRecvFd*(s: Socket)
proc updateSendFd*(s: Socket)

# Socket Implementation
proc newSocket*(proto: SpProto, raw: bool = false): Socket =
  new(result)
  result.proto = proto
  result.raw = raw
  result.closed = false
  result.recvQueue = initDeque[SpMsg]()
  result.sendQueue = initDeque[SpMsg]()
  result.inFlight = 0
  result.pipes = initTable[PipeId, Pipe]()
  result.options = newOptionStore(entSocket)
  result.pipeCallbacks = initTable[PipeEvent, PipeNotifyCb]()
  result.contexts = initTable[uint32, Context]()

  when not defined(js):
    discard posix.pipe(result.recvFds)
    discard posix.pipe(result.sendFds)
    for fd in [
      result.recvFds[0], result.recvFds[1], result.sendFds[0], result.sendFds[1]
    ]:
      let fl = posix.fcntl(fd, F_GETFL, 0)
      discard posix.fcntl(fd, F_SETFL, fl or O_NONBLOCK)
    result.recvFdReadable = false
    result.sendFdWritable = false

  result.options.setIntInternal(NNG_OPT_PROTO, int(proto))
  result.options.setStringInternal(NNG_OPT_PROTONAME, protoName(proto))
  result.options.setIntInternal(NNG_OPT_PEER, int(peerProto(proto)))
  result.options.setStringInternal(NNG_OPT_PEERNAME, protoName(peerProto(proto)))
  result.options.setBoolInternal(NNG_OPT_RAW, raw)

  if proto in {spPair0, spPair1, spPush0}:
    result.options.setIntInternal(NNG_OPT_SENDBUF, 0)
    result.options.setIntInternal(NNG_OPT_RECVBUF, 0)

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

  result.updateRecvFd()
  result.updateSendFd()

proc pipeNotify*(s: Socket, ev: PipeEvent, cb: PipeNotifyCb) =
  if cb == nil:
    s.pipeCallbacks.del(ev)
  else:
    s.pipeCallbacks[ev] = cb

proc inSurvey*(s: Socket, ctxId: uint32 = 0): bool =
  if s.engineKind == engSurveyor0:
    if ctxId in s.surveyor0Eng.contexts:
      let st = s.surveyor0Eng.contexts[ctxId]
      if st.inSurvey:
        if currentNowMs() >= st.deadlineMs:
          s.surveyor0Eng.contexts[ctxId].inSurvey = false
          s.surveyor0Eng.surveyToCtx.del(st.surveyId)
          return false
        return true
    false
  else:
    false

proc pipeCount*(s: Socket): int =
  s.pipes.len

proc updateRecvFd*(s: Socket) =
  when not defined(js):
    let shouldBeReadable =
      s.recvQueue.len > 0 and (s.engineKind != engPull0 or s.pipes.len > 0)
    if shouldBeReadable and not s.recvFdReadable:
      var b: byte = 1
      discard posix.write(s.recvFds[1], addr b, 1)
      s.recvFdReadable = true
    elif not shouldBeReadable and s.recvFdReadable:
      var buf: array[64, byte]
      while posix.read(s.recvFds[0], addr buf[0], 64) > 0:
        discard
      s.recvFdReadable = false

proc updateSendFd*(s: Socket) =
  when not defined(js):
    let pendingAioCount =
      if gPendingAioCountProc != nil:
        gPendingAioCountProc(s.id)
      else:
        0
    let sendBuf = s.options.getInt(NNG_OPT_SENDBUF)
    let totalUsed = s.inFlight + s.sendQueue.len + pendingAioCount
    let canSendProto =
      case s.engineKind
      of engReq0:
        s.pipes.len > 0 and (s.raw or totalUsed < (s.pipes.len + sendBuf))
      of engRep0:
        s.raw or s.rep0Eng.reqStack.len > 0 or s.rep0Eng.contexts.len > 0
      of engRespondent0:
        s.raw or s.respondent0Eng.reqStack.len > 0 or s.respondent0Eng.contexts.len > 0
      of engPush0:
        s.pipes.len > 0 or sendBuf > 0
      else:
        true
    let totalCap =
      if s.engineKind == engReq0:
        (if s.pipes.len > 0: s.pipes.len else: 0) + sendBuf
      else:
        (if s.pipes.len > 0: s.pipes.len * 2 else: 0) + sendBuf
    let shouldBeWritable =
      not s.closed and canSendProto and (
        s.engineKind in {engPub0, engSurveyor0} or
        (s.raw and s.engineKind in {engRep0, engRespondent0}) or
        (totalCap > 0 and totalUsed < totalCap)
      )
    if shouldBeWritable and not s.sendFdWritable:
      var b: byte = 1
      discard posix.write(s.sendFds[1], addr b, 1)
      s.sendFdWritable = true
    elif not shouldBeWritable and s.sendFdWritable:
      var buf: array[64, byte]
      while posix.read(s.sendFds[0], addr buf[0], 64) > 0:
        discard
      s.sendFdWritable = false

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
        s.updateSendFd()
    of actDeliverRecv:
      var msg = act.msg
      if msg.pipeId == 0 and act.pipeId != 0:
        msg.pipeId = act.pipeId
      if s.deviceHook != nil:
        s.deviceHook(msg)
        s.updateRecvFd()
        s.updateSendFd()
        continue
      var delivered = false
      if act.ctxId != 0 and act.ctxId in s.contexts:
        let ctx = s.contexts[act.ctxId]
        if not ctx.closed:
          if ctx.pendingRecvCb != nil:
            let cb = ctx.pendingRecvCb
            ctx.pendingRecvCb = nil
            cb(0, msg)
            delivered = true
          elif ctx.onRecvCb != nil:
            let cb = ctx.onRecvCb
            ctx.onRecvCb = nil
            cb(msg)
            delivered = true
          else:
            let maxBuf = ctx.options.getInt(NNG_OPT_RECVBUF)
            if maxBuf == 0 or ctx.recvQueue.len < maxBuf:
              ctx.recvQueue.addLast(msg)
              delivered = true
      elif s.engineKind == engSub0:
        # Fan out to all contexts whose topics match
        for ctx in s.contexts.values:
          if not ctx.closed and matchesTopicList(ctx.sub0Topics, msg.body):
            if ctx.pendingRecvCb != nil:
              let cb = ctx.pendingRecvCb
              ctx.pendingRecvCb = nil
              cb(0, msg)
            elif ctx.onRecvCb != nil:
              let cb = ctx.onRecvCb
              ctx.onRecvCb = nil
              cb(msg)
            else:
              let maxBuf = ctx.options.getInt(NNG_OPT_RECVBUF)
              if maxBuf == 0 or ctx.recvQueue.len < maxBuf:
                ctx.recvQueue.addLast(msg)
              elif ctx.options.getBool(NNG_OPT_SUB_PREFNEW):
                discard ctx.recvQueue.popFirst()
                ctx.recvQueue.addLast(msg)
        if gOnPipeHook != nil:
          gOnPipeHook()
      elif s.engineKind in {engRep0, engRespondent0} and s.contexts.len > 0:
        var targetCtx: Context = nil
        for ctx in s.contexts.values:
          if not ctx.closed and (ctx.pendingRecvCb != nil or ctx.onRecvCb != nil):
            targetCtx = ctx
            break
        if targetCtx != nil:
          if s.engineKind == engRespondent0 and s.respondent0Eng.reqStack.len > 0:
            let st = s.respondent0Eng.reqStack.pop()
            s.respondent0Eng.contexts[targetCtx.ctxId] = st
          elif s.engineKind == engRep0 and s.rep0Eng.reqStack.len > 0:
            let st = s.rep0Eng.reqStack.pop()
            s.rep0Eng.contexts[targetCtx.ctxId] = st
          if targetCtx.pendingRecvCb != nil:
            let cb = targetCtx.pendingRecvCb
            targetCtx.pendingRecvCb = nil
            cb(0, msg)
            delivered = true
          elif targetCtx.onRecvCb != nil:
            let cb = targetCtx.onRecvCb
            targetCtx.onRecvCb = nil
            cb(msg)
            delivered = true

      if s.engineKind != engSub0 or s.sub0Eng.matchesTopic(msg.body):
        if not delivered:
          if s.pendingRecvCb != nil:
            let cb = s.pendingRecvCb
            s.pendingRecvCb = nil
            cb(0, msg)
          elif s.onRecvCb != nil:
            let cb = s.onRecvCb
            s.onRecvCb = nil
            cb(msg)
          else:
            let maxBuf = s.options.getInt(NNG_OPT_RECVBUF)
            if maxBuf == 0 or s.recvQueue.len < maxBuf:
              s.recvQueue.addLast(msg)
            elif s.engineKind == engSub0 and s.options.getBool(NNG_OPT_SUB_PREFNEW):
              discard s.recvQueue.popFirst()
              s.recvQueue.addLast(msg)
      s.updateRecvFd()
      s.updateSendFd()
    of actFinishSend:
      s.updateSendFd()
    of actFailContext:
      if act.err != spOk:
        let nngErrCode =
          case act.err
          of spErrClosed: NNG_ECLOSED
          of spErrTimedOut: NNG_ETIMEDOUT
          of spErrProto: NNG_EPROTO
          of spErrNotSupported: NNG_ENOTSUP
          of spErrCanceled: NNG_ECANCELED
          of spErrMsgSize: NNG_EMSGSIZE
          of spErrState: NNG_ESTATE
          of spErrConnReset: NNG_ECONNRESET
          else: NNG_EINVAL
        if act.ctxId != 0 and act.ctxId in s.contexts:
          let ctx = s.contexts[act.ctxId]
          if act.err != spErrCanceled:
            ctx.lastErr = cint(nngErrCode)
          if ctx.pendingRecvCb != nil:
            let cb = ctx.pendingRecvCb
            ctx.pendingRecvCb = nil
            cb(nngErrCode, SpMsg())
        else:
          if act.err != spErrCanceled:
            s.lastErr = cint(nngErrCode)
          if s.pendingRecvCb != nil:
            let cb = s.pendingRecvCb
            s.pendingRecvCb = nil
            cb(nngErrCode, SpMsg())

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
  pipe.sockPtr = cast[pointer](s)

  # NNG_PIPE_EV_ADD_PRE hook
  if pipeEvAddPre in s.pipeCallbacks:
    var rejected = false
    let origClose = pipe.closeImpl
    pipe.closeImpl = proc() =
      rejected = true
      if origClose != nil:
        origClose()

    s.pipeCallbacks[pipeEvAddPre](pipe, pipeEvAddPre)
    pipe.closeImpl = origClose
    if rejected:
      pipe.close()
      return

  s.pipes[pipe.pipeId] = pipe
  let pid = pipe.pipeId
  let ev = EngineEvent(kind: evPipeAdded, pipeId: pid)
  let actions = s.stepEngine(ev)
  s.executeActions(actions)
  s.updateSendFd()
  if s.lastErr != 0:
    let err = s.lastErr
    s.lastErr = 0
    raise newNngError(err, "Socket send failed: " & $err)

  # Flush send queue if any
  while s.sendQueue.len > 0 and s.pipes.len > 0:
    let msg = s.sendQueue.popFirst()
    let evSend =
      EngineEvent(kind: evUserSend, ctxId: 0, msg: msg, nowMs: currentNowMs())
    let sendActions = s.stepEngine(evSend)
    s.executeActions(sendActions)

  # NNG_PIPE_EV_ADD_POST hook
  if pipeEvAddPost in s.pipeCallbacks:
    s.pipeCallbacks[pipeEvAddPost](pipe, pipeEvAddPost)

  if gOnPipeHook != nil:
    gOnPipeHook()

proc detachCustomPipe*(s: Socket, pipeId: PipeId) =
  if pipeId in s.pipes:
    let p = s.pipes[pipeId]
    s.pipes.del(pipeId)
    s.inFlight = 0
    let ev = EngineEvent(kind: evPipeRemoved, pipeId: pipeId)
    let actions = s.stepEngine(ev)
    s.executeActions(actions)
    s.updateSendFd()
    s.updateRecvFd()

    # NNG_PIPE_EV_REM_POST hook
    if pipeEvRemPost in s.pipeCallbacks:
      s.pipeCallbacks[pipeEvRemPost](p, pipeEvRemPost)

proc feedPipeFrame*(s: Socket, pipeId: PipeId, frame: openArray[byte]) =
  if s.closed:
    return
  let maxRecv = s.options.getInt(NNG_OPT_RECVBUF)
  var hasWaiting = s.pendingRecvCb != nil or s.onRecvCb != nil
  if not hasWaiting and s.contexts.len > 0:
    for ctx in s.contexts.values:
      if not ctx.closed and (ctx.pendingRecvCb != nil or ctx.onRecvCb != nil):
        hasWaiting = true
        break
  if not hasWaiting and maxRecv > 0 and s.recvQueue.len >= maxRecv:
    if s.engineKind == engSub0 and
        (s.options.getBool(NNG_OPT_SUB_PREFNEW) or s.contexts.len > 0):
      discard
    elif s.engineKind in {engPub0, engSub0} or
        (s.engineKind == engPair1 and s.pair1Eng.polyamorous):
      return
    elif s.raw and s.engineKind in {engReq0, engRep0, engSurveyor0, engRespondent0}:
      if s.recvQueue.len < maxRecv + 20:
        discard
      elif s.engineKind == engSurveyor0:
        return
      else:
        raise newNngError(NNG_ETIMEDOUT, "Receive buffer full")
    else:
      raise newNngError(NNG_ETIMEDOUT, "Receive buffer full")
  let ev = EngineEvent(
    kind: evPipeMessage, pipeId: pipeId, rawFrame: @frame, nowMs: currentNowMs()
  )
  let actions = s.stepEngine(ev)
  s.executeActions(actions)

proc attachEndpoint(
    s: Socket, ep: InprocEndpoint, dialerPtr: pointer = nil, listenerPtr: pointer = nil
) =
  let pid = ep.pipeId
  ep.onMessage = proc(rawFrame: seq[byte]) =
    s.feedPipeFrame(pid, rawFrame)

  ep.onClose = proc() =
    s.detachCustomPipe(pid)

  let pipe = newPipe(
    pid,
    s.proto,
    proc(data: openArray[byte]) =
      try:
        ep.send(data)
      except NngError as e:
        if (s.raw and s.engineKind in {engRep0, engRespondent0, engSurveyor0}) or
           (s.engineKind in {engPub0, engSurveyor0, engBus0}):
          discard
        else:
          raise e
      inc s.inFlight
      s.updateSendFd(),
    proc() =
      ep.close(),
  )
  pipe.sockPtr = cast[pointer](s)
  pipe.dialerPtr = dialerPtr
  pipe.listenerPtr = listenerPtr
  pipe.drainImpl = proc() =
    if ep.peer != nil and ep.peer.pipe != nil:
      let peerPipe = ep.peer.pipe
      if peerPipe.sockPtr != nil:
        let peerSock = cast[Socket](peerPipe.sockPtr)
        if peerSock.inFlight > 0:
          dec peerSock.inFlight
        peerSock.updateSendFd()
        if gOnPipeHook != nil:
          gOnPipeHook()
  ep.pipe = pipe
  s.attachCustomPipe(pipe)
  if ep.recvBuf.len > 0:
    let pending = ep.recvBuf
    ep.recvBuf = @[]
    for f in pending:
      s.feedPipeFrame(pid, f)

# Option Methods for Socket
proc getOpt*(s: Socket, name: string): OptionValue =
  if name notin s.options.values:
    raise newNngError(NNG_ENOTSUP, "Option " & name & " not supported on socket")
  s.options.values[name]

proc setOpt*(s: Socket, name: string, val: bool) =
  s.options.setBool(name, val)
  if name == NNG_OPT_PAIR1_POLY and s.engineKind == engPair1:
    s.pair1Eng.polyamorous = val
  elif name == NNG_OPT_PAIR1_TEST_INJECT_HEADER and s.engineKind == engPair1:
    s.pair1Eng.injectHeader = val

proc setOpt*(s: Socket, name: string, val: int) =
  if (name == NNG_OPT_RECVBUF or name == NNG_OPT_SENDBUF) and
      ((s.engineKind in {engPub0, engBus0} and val == 0) or
       (s.engineKind == engSub0 and not s.raw and val == 0)):
    raise newNngError(NNG_EINVAL, "Buffer size 0 not supported on pub/sub/bus")
  s.options.setInt(name, val)
  if name == NNG_OPT_SENDBUF:
    if val == 0:
      s.sendQueue.clear()
    else:
      while s.sendQueue.len > val:
        discard s.sendQueue.popLast()
    s.updateSendFd()
  elif name == NNG_OPT_MAXTTL:
    if s.engineKind == engReq0:
      s.req0Eng.maxTtl = uint32(val)
    elif s.engineKind == engRep0:
      s.rep0Eng.maxTtl = uint32(val)
    elif s.engineKind == engRespondent0:
      s.respondent0Eng.maxTtl = uint32(val)
    elif s.engineKind == engPair1:
      s.pair1Eng.maxTtl = uint32(val)
  elif name == NNG_OPT_REQ_RESENDTIME and s.engineKind == engReq0:
    s.req0Eng.resendTimeMs = int64(val)
  elif name == NNG_OPT_SURVEYOR_SURVEYTIME and s.engineKind == engSurveyor0:
    s.surveyor0Eng.surveyTimeMs = int64(val)
  elif name == NNG_OPT_RECVBUF:
    if val == 0:
      s.recvQueue.clear()
      s.updateRecvFd()
    else:
      while s.recvQueue.len > val:
        discard s.recvQueue.popLast()
      s.updateRecvFd()

proc setOpt*(s: Socket, name: string, val: int64) =
  s.options.setMs(name, val)
  if name == NNG_OPT_REQ_RESENDTIME and s.engineKind == engReq0:
    s.req0Eng.resendTimeMs = val
  elif name == NNG_OPT_SURVEYOR_SURVEYTIME and s.engineKind == engSurveyor0:
    s.surveyor0Eng.surveyTimeMs = val

proc setOpt*(s: Socket, name: string, val: uint64) =
  s.options.setSize(name, val)

proc setOpt*(s: Socket, name: string, val: string) =
  if name == NNG_OPT_SUB_SUBSCRIBE:
    s.options.validateAccess(name, isSet = true, optKindString)
    if s.engineKind == engSub0:
      var b = newSeq[byte](val.len)
      if val.len > 0:
        copyMem(addr b[0], unsafeAddr val[0], val.len)
      s.sub0Eng.subscribe(b)
  elif name == NNG_OPT_SUB_UNSUBSCRIBE:
    s.options.validateAccess(name, isSet = true, optKindString)
    if s.engineKind == engSub0:
      var b = newSeq[byte](val.len)
      if val.len > 0:
        copyMem(addr b[0], unsafeAddr val[0], val.len)
      s.sub0Eng.unsubscribe(b)
  else:
    s.options.setString(name, val)

proc getBool*(s: Socket, name: string): bool =
  if name == NNG_OPT_PAIR1_POLY and s.engineKind == engPair1:
    return s.pair1Eng.polyamorous
  s.options.getBool(name)

proc getInt*(s: Socket, name: string): int =
  when not defined(js):
    if name == NNG_OPT_RECVFD:
      return int(s.recvFds[0])
    elif name == NNG_OPT_SENDFD:
      return int(s.sendFds[0])
  s.options.getInt(name)

proc getMs*(s: Socket, name: string): int64 =
  s.options.getMs(name)

proc getSize*(s: Socket, name: string): uint64 =
  s.options.getSize(name)

proc getString*(s: Socket, name: string): string =
  s.options.getString(name)

# Context Implementation
proc openContext*(s: Socket): Context =
  if s.raw:
    raise newNngError(NNG_ENOTSUP, "Contexts not supported on raw sockets")
  if s.engineKind in {engPair0, engPair1, engPub0, engPush0, engPull0, engBus0}:
    raise newNngError(NNG_ENOTSUP, "Contexts are not supported on this protocol")
  if s.engineKind == engSub0:
    s.sub0Eng.hasContexts = true
  let cid = allocCtxId()
  new(result)
  result.socket = s
  result.ctxId = cid
  result.options = newOptionStore(entContext)
  result.closed = false
  result.recvQueue = initDeque[SpMsg]()
  s.contexts[cid] = result

proc close*(ctx: Context) =
  if not ctx.closed:
    ctx.closed = true
    if ctx.pendingRecvCb != nil:
      let cb = ctx.pendingRecvCb
      ctx.pendingRecvCb = nil
      cb(NNG_ECLOSED, SpMsg())
    if ctx.socket != nil and ctx.ctxId in ctx.socket.contexts:
      ctx.socket.contexts.del(ctx.ctxId)

proc subscribe*(ctx: Context, topic: openArray[byte]) =
  let t = @topic
  if t notin ctx.sub0Topics:
    ctx.sub0Topics.add(t)

proc unsubscribe*(ctx: Context, topic: openArray[byte]) =
  let t = @topic
  var idx = -1
  for i, s in ctx.sub0Topics:
    if s == t:
      idx = i
      break
  if idx >= 0:
    ctx.sub0Topics.delete(idx)
    var kept = initDeque[SpMsg]()
    for m in ctx.recvQueue:
      if matchesTopicList(ctx.sub0Topics, m.body):
        kept.addLast(m)
    ctx.recvQueue = kept
  else:
    raise newNngError(NNG_ENOENT, "Subscription not found")

proc sendMsg*(ctx: Context, msg: SpMsg) =
  if ctx.closed or ctx.socket.closed:
    raise newNngError(NNG_ECLOSED, "Context or socket is closed")
  if ctx.socket.engineKind == engReq0:
    ctx.recvQueue.clear()
    ctx.socket.updateRecvFd()
  let ev =
    EngineEvent(kind: evUserSend, ctxId: ctx.ctxId, msg: msg, nowMs: currentNowMs())
  let actions = ctx.socket.stepEngine(ev)
  ctx.socket.executeActions(actions)
  if ctx.lastErr != 0:
    let err = ctx.lastErr
    ctx.lastErr = 0
    raise newNngError(err, "Context send failed: " & $err)
  if ctx.socket.lastErr != 0:
    let err = ctx.socket.lastErr
    ctx.socket.lastErr = 0
    raise newNngError(err, "Socket send failed: " & $err)

proc send*(ctx: Context, data: openArray[byte]) =
  ctx.sendMsg(initSpMsg(data))

proc send*(ctx: Context, str: string) =
  var bytes = newSeq[byte](str.len)
  if str.len > 0:
    copyMem(addr bytes[0], unsafeAddr str[0], str.len)
  ctx.send(bytes)

proc recvMsg*(ctx: Context, timeoutMs: int = -1): SpMsg =
  if ctx.closed or ctx.socket.closed:
    raise newNngError(NNG_ECLOSED, "Context or socket is closed")
  if ctx.socket.engineKind == engSurveyor0 and not ctx.socket.raw:
    if ctx.recvQueue.len == 0 and not ctx.socket.inSurvey(ctx.ctxId):
      raise newNngError(NNG_ESTATE, "Incorrect state: no survey in progress")
  let tmo =
    if timeoutMs >= 0:
      timeoutMs
    else:
      int(ctx.options.getMs(NNG_OPT_RECVTIMEO))
  when not defined(js):
    let start = getMonoTime()
    while ctx.recvQueue.len == 0 and not ctx.closed and not ctx.socket.closed:
      if ctx.lastErr != 0:
        let err = ctx.lastErr
        ctx.lastErr = 0
        raise newNngError(err, "Context error: " & $err)
      try:
        asyncdispatch.poll(1)
      except CatchableError:
        os.sleep(1)
      if tmo >= 0:
        let elapsed = (getMonoTime() - start).inMilliseconds
        if elapsed > tmo:
          raise newNngError(NNG_ETIMEDOUT, "Context recvMsg timed out")
    if ctx.lastErr != 0:
      let err = ctx.lastErr
      ctx.lastErr = 0
      raise newNngError(err, "Context error: " & $err)
  if ctx.recvQueue.len > 0:
    result = ctx.recvQueue.popFirst()
    if result.pipeId != 0 and result.pipeId in ctx.socket.pipes:
      ctx.socket.pipes[result.pipeId].drain()
    ctx.socket.updateRecvFd()
    ctx.socket.updateSendFd()
  else:
    raise newNngError(NNG_ECLOSED, "Context closed or empty")

proc recv*(ctx: Context, timeoutMs: int = -1): seq[byte] =
  result = ctx.recvMsg(timeoutMs).body

proc recvStr*(ctx: Context, timeoutMs: int = -1): string =
  let body = ctx.recv(timeoutMs)
  result = newString(body.len)
  if body.len > 0:
    copyMem(addr result[0], unsafeAddr body[0], body.len)

proc setOpt*(ctx: Context, name: string, val: int) =
  if (name == NNG_OPT_RECVBUF or name == NNG_OPT_SENDBUF) and
      ((ctx.socket.engineKind in {engPub0, engBus0} and val == 0) or
       (ctx.socket.engineKind == engSub0 and not ctx.socket.raw and val == 0)):
    raise newNngError(NNG_EINVAL, "Buffer size 0 not supported on pub/sub/bus")
  ctx.options.setInt(name, val)

proc setOpt*(ctx: Context, name: string, val: int64) =
  ctx.options.setMs(name, val)

proc getInt*(ctx: Context, name: string): int =
  ctx.options.getInt(name)

proc getMs*(ctx: Context, name: string): int64 =
  ctx.options.getMs(name)

# Pipe Introspection Methods
proc id*(p: Pipe): PipeId =
  p.pipeId

proc socket*(p: Pipe): Socket =
  (if p == nil or p.sockPtr == nil: nil
  else: cast[Socket](p.sockPtr))

proc dialer*(p: Pipe): Dialer =
  (if p == nil or p.dialerPtr == nil: nil
  else: cast[Dialer](p.dialerPtr))

proc listener*(p: Pipe): Listener =
  (if p == nil or p.listenerPtr == nil: nil
  else: cast[Listener](p.listenerPtr))

proc getBool*(p: Pipe, name: string): bool =
  case name
  of NNG_OPT_TLS_VERIFIED:
    p.tlsVerified
  else:
    raise newNngError(NNG_ENOTSUP, "Option " & name & " not supported on pipe")

proc getString*(p: Pipe, name: string): string =
  case name
  of NNG_OPT_TLS_PEER_CN:
    p.tlsPeerCn
  of NNG_OPT_LOCADDR:
    p.locAddr
  of NNG_OPT_REMADDR:
    p.remAddr
  else:
    raise newNngError(NNG_ENOTSUP, "Option " & name & " not supported on pipe")

# Dialer Implementation
proc newDialer*(s: Socket, urlStr: string): Dialer =
  new(result)
  result.socket = s
  result.url = urlStr
  result.options = newOptionStore(entDialer)
  result.options.setStringInternal(NNG_OPT_URL, urlStr)
  result.closed = false
  result.started = false
  result.tlsCfg = nil

proc setOpt*(d: Dialer, name: string, val: int64) =
  d.options.setMs(name, val)

proc setOpt*(d: Dialer, name: string, val: bool) =
  d.options.setBool(name, val)

proc setOpt*(d: Dialer, name: string, val: TlsConfig) =
  d.options.setPtr(NNG_OPT_TLS_CONFIG, cast[pointer](val))
  d.tlsCfg = val

proc setTlsConfig*(d: Dialer, val: TlsConfig) {.inline.} =
  d.setOpt(NNG_OPT_TLS_CONFIG, val)

proc getOpt*(d: Dialer, name: string): OptionValue =
  if name notin d.options.values:
    raise newNngError(NNG_ENOTSUP, "Option " & name & " not supported on dialer")
  d.options.values[name]

proc getMs*(d: Dialer, name: string): int64 =
  d.options.getMs(name)

proc getString*(d: Dialer, name: string): string =
  d.options.getString(name)

proc start*(d: Dialer) =
  if d.closed:
    raise newNngError(NNG_ECLOSED, "Dialer is closed")
  if d.started:
    return
  d.started = true

  let parsed = parseSpUrl(d.url)
  case parsed.kind
  of tranInproc:
    let ep = connectInproc(parsed.raw, d.socket.proto, nil, nil)
    if not ep.closed:
      d.socket.attachEndpoint(ep, dialerPtr = cast[pointer](d))
  of tranTcp:
    when not defined(js):
      let host = if parsed.host.len > 0: parsed.host else: "127.0.0.1"
      let pipe = waitFor connectTcpAsync(
        host,
        parsed.port,
        d.socket.proto,
        proc(pid: PipeId, frame: seq[byte]) =
          d.socket.feedPipeFrame(pid, frame),
        proc(pid: PipeId) =
          d.socket.detachCustomPipe(pid),
      )
      pipe.dialerPtr = cast[pointer](d)
      d.socket.attachCustomPipe(pipe)
    else:
      raise newNngError(NNG_ENOTSUP, "TCP dialer not supported on JS")
  of tranTlsTcp:
    when not defined(js):
      let host = if parsed.host.len > 0: parsed.host else: "127.0.0.1"
      let pipe = waitFor connectTlsTcpAsync(
        host,
        parsed.port,
        d.socket.proto,
        d.tlsCfg,
        proc(pid: PipeId, frame: seq[byte]) =
          d.socket.feedPipeFrame(pid, frame),
        proc(pid: PipeId) =
          d.socket.detachCustomPipe(pid),
      )
      pipe.dialerPtr = cast[pointer](d)
      d.socket.attachCustomPipe(pipe)
    else:
      raise newNngError(NNG_ENOTSUP, "TLS TCP dialer not supported on JS")
  of tranIpc:
    when not defined(js):
      let pipe = waitFor connectIpcAsync(
        parsed.path,
        d.socket.proto,
        proc(pid: PipeId, frame: seq[byte]) =
          d.socket.feedPipeFrame(pid, frame),
        proc(pid: PipeId) =
          d.socket.detachCustomPipe(pid),
      )
      pipe.dialerPtr = cast[pointer](d)
      d.socket.attachCustomPipe(pipe)
    else:
      raise newNngError(NNG_ENOTSUP, "IPC dialer not supported on JS")
  of tranUdp:
    when not defined(js):
      let host = if parsed.host.len > 0: parsed.host else: "127.0.0.1"
      let pipe = waitFor connectUdpAsync(
        host,
        parsed.port,
        d.socket.proto,
        proc(pid: PipeId, frame: seq[byte]) =
          d.socket.feedPipeFrame(pid, frame),
        proc(pid: PipeId) =
          d.socket.detachCustomPipe(pid),
      )
      pipe.dialerPtr = cast[pointer](d)
      d.socket.attachCustomPipe(pipe)
    else:
      raise newNngError(NNG_ENOTSUP, "UDP dialer not supported on JS")
  of tranWs:
    when not defined(js):
      let host = if parsed.host.len > 0: parsed.host else: "127.0.0.1"
      let pipe = waitFor connectWsAsync(
        host,
        parsed.port,
        parsed.path,
        d.socket.proto,
        proc(pid: PipeId, frame: seq[byte]) =
          d.socket.feedPipeFrame(pid, frame),
        proc(pid: PipeId) =
          d.socket.detachCustomPipe(pid),
        tlsCfg = d.tlsCfg,
      )
      pipe.dialerPtr = cast[pointer](d)
      d.socket.attachCustomPipe(pipe)
    else:
      raise newNngError(NNG_ENOTSUP, "Use js_ws on JS target")

proc close*(d: Dialer) =
  if not d.closed:
    d.closed = true
    if d.closeImpl != nil:
      d.closeImpl()

# Listener Implementation
proc newListener*(s: Socket, urlStr: string): Listener =
  new(result)
  result.socket = s
  result.url = urlStr
  result.options = newOptionStore(entListener)
  result.options.setStringInternal(NNG_OPT_URL, urlStr)
  result.closed = false
  result.started = false
  result.boundPort = 0
  result.tlsCfg = nil

proc setOpt*(l: Listener, name: string, val: bool) =
  l.options.setBool(name, val)

proc setOpt*(l: Listener, name: string, val: TlsConfig) =
  l.options.setPtr(NNG_OPT_TLS_CONFIG, cast[pointer](val))
  l.tlsCfg = val

proc setTlsConfig*(l: Listener, val: TlsConfig) {.inline.} =
  l.setOpt(NNG_OPT_TLS_CONFIG, val)

proc getOpt*(l: Listener, name: string): OptionValue =
  if name notin l.options.values:
    raise newNngError(NNG_ENOTSUP, "Option " & name & " not supported on listener")
  l.options.values[name]

proc getInt*(l: Listener, name: string): int =
  l.options.getInt(name)

proc getString*(l: Listener, name: string): string =
  l.options.getString(name)

proc getSockAddr*(l: Listener, name: string): string =
  l.options.getSockAddr(name)

proc start*(l: Listener) =
  if l.closed:
    raise newNngError(NNG_ECLOSED, "Listener is closed")
  if l.started:
    return
  l.started = true

  let parsed = parseSpUrl(l.url)
  case parsed.kind
  of tranInproc:
    let inprocListener = registerInprocListener(
      parsed.raw,
      l.socket.proto,
      proc(ep: InprocEndpoint) =
        l.socket.attachEndpoint(ep, listenerPtr = cast[pointer](l)),
    )
    l.closeImpl = proc() =
      inprocListener.close()
  of tranTcp:
    when not defined(js):
      let host = if parsed.host.len > 0: parsed.host else: "127.0.0.1"
      let tcpListener = waitFor listenTcpAsync(
        host,
        parsed.port,
        l.socket.proto,
        proc(pipe: Pipe) =
          pipe.listenerPtr = cast[pointer](l)
          l.socket.attachCustomPipe(pipe),
        proc(pid: PipeId, frame: seq[byte]) =
          l.socket.feedPipeFrame(pid, frame),
        proc(pid: PipeId) =
          l.socket.detachCustomPipe(pid),
      )
      l.boundPort = tcpListener.boundPort
      l.options.setIntInternal(NNG_OPT_TCP_BOUND_PORT, l.boundPort)
      l.options.setIntInternal(NNG_OPT_BOUND_PORT, l.boundPort)
      l.options.setSockAddrInternal(NNG_OPT_LOCADDR, "127.0.0.1:" & $l.boundPort)
      l.closeImpl = proc() =
        tcpListener.close()
    else:
      raise newNngError(NNG_ENOTSUP, "TCP listener not supported on JS")
  of tranTlsTcp:
    when not defined(js):
      let host = if parsed.host.len > 0: parsed.host else: "127.0.0.1"
      let tlsListener = waitFor listenTlsTcpAsync(
        host,
        parsed.port,
        l.socket.proto,
        l.tlsCfg,
        proc(pipe: Pipe) =
          pipe.listenerPtr = cast[pointer](l)
          l.socket.attachCustomPipe(pipe),
        proc(pid: PipeId, frame: seq[byte]) =
          l.socket.feedPipeFrame(pid, frame),
        proc(pid: PipeId) =
          l.socket.detachCustomPipe(pid),
      )
      l.boundPort = tlsListener.boundPort
      l.options.setIntInternal(NNG_OPT_TCP_BOUND_PORT, l.boundPort)
      l.options.setIntInternal(NNG_OPT_BOUND_PORT, l.boundPort)
      l.options.setSockAddrInternal(NNG_OPT_LOCADDR, "127.0.0.1:" & $l.boundPort)
      l.closeImpl = proc() =
        tlsListener.close()
    else:
      raise newNngError(NNG_ENOTSUP, "TLS TCP listener not supported on JS")
  of tranIpc:
    when not defined(js):
      let ipcListener = waitFor listenIpcAsync(
        parsed.path,
        l.socket.proto,
        proc(pipe: Pipe) =
          pipe.listenerPtr = cast[pointer](l)
          l.socket.attachCustomPipe(pipe),
        proc(pid: PipeId, frame: seq[byte]) =
          l.socket.feedPipeFrame(pid, frame),
        proc(pid: PipeId) =
          l.socket.detachCustomPipe(pid),
      )
      l.closeImpl = proc() =
        ipcListener.close()
    else:
      raise newNngError(NNG_ENOTSUP, "IPC listener not supported on JS")
  of tranUdp:
    when not defined(js):
      let host = if parsed.host.len > 0: parsed.host else: "127.0.0.1"
      let udpListener = waitFor listenUdpAsync(
        host,
        parsed.port,
        l.socket.proto,
        proc(pipe: Pipe) =
          pipe.listenerPtr = cast[pointer](l)
          l.socket.attachCustomPipe(pipe),
        proc(pid: PipeId, frame: seq[byte]) =
          l.socket.feedPipeFrame(pid, frame),
        proc(pid: PipeId) =
          l.socket.detachCustomPipe(pid),
      )
      l.closeImpl = proc() =
        udpListener.close()
    else:
      raise newNngError(NNG_ENOTSUP, "UDP listener not supported on JS")
  of tranWs:
    when not defined(js):
      let host = if parsed.host.len > 0: parsed.host else: "127.0.0.1"
      let wsListener = waitFor listenWsAsync(
        host,
        parsed.port,
        parsed.path,
        l.socket.proto,
        proc(pipe: Pipe) =
          pipe.listenerPtr = cast[pointer](l)
          l.socket.attachCustomPipe(pipe),
        proc(pid: PipeId, frame: seq[byte]) =
          l.socket.feedPipeFrame(pid, frame),
        proc(pid: PipeId) =
          l.socket.detachCustomPipe(pid),
        tlsCfg = l.tlsCfg,
      )
      l.boundPort = wsListener.boundPort
      l.options.setIntInternal(NNG_OPT_TCP_BOUND_PORT, l.boundPort)
      l.options.setIntInternal(NNG_OPT_BOUND_PORT, l.boundPort)
      l.options.setSockAddrInternal(NNG_OPT_LOCADDR, "127.0.0.1:" & $l.boundPort)
      l.closeImpl = proc() =
        wsListener.close()
    else:
      raise newNngError(NNG_ENOTSUP, "WS listener not supported on JS")

proc close*(l: Listener) =
  if not l.closed:
    l.closed = true
    if l.closeImpl != nil:
      l.closeImpl()

# High-Level Convenience Dial & Listen
proc dial*(s: Socket, urlStr: string): Dialer {.discardable.} =
  result = s.newDialer(urlStr)
  result.start()

proc listen*(s: Socket, urlStr: string): Listener =
  result = s.newListener(urlStr)
  result.start()

# Sending & Receiving on Socket
proc sendMsg*(s: Socket, msg: SpMsg, ctxId: uint32 = 0) =
  if s.closed:
    raise newNngError(NNG_ECLOSED, "Socket is closed")
  if s.engineKind == engReq0:
    if ctxId != 0 and ctxId in s.contexts:
      s.contexts[ctxId].recvQueue.clear()
    else:
      s.recvQueue.clear()
    s.updateRecvFd()
  elif s.engineKind == engSurveyor0:
    if ctxId != 0 and ctxId in s.contexts:
      let ctx = s.contexts[ctxId]
      ctx.recvQueue.clear()
      if ctx.pendingRecvCb != nil:
        let cb = ctx.pendingRecvCb
        ctx.pendingRecvCb = nil
        cb(NNG_ECANCELED, SpMsg())
    else:
      s.recvQueue.clear()
      if s.pendingRecvCb != nil:
        let cb = s.pendingRecvCb
        s.pendingRecvCb = nil
        cb(NNG_ECANCELED, SpMsg())
    s.updateRecvFd()
  if s.pipes.len == 0 and s.options.getInt(NNG_OPT_SENDBUF) > 0:
    let maxSend = s.options.getInt(NNG_OPT_SENDBUF)
    if s.sendQueue.len < maxSend:
      s.sendQueue.addLast(msg)
      s.updateSendFd()
      return
    else:
      raise newNngError(NNG_ETIMEDOUT, "Send buffer full")
  let cid = ctxId
  let ev = EngineEvent(kind: evUserSend, ctxId: cid, msg: msg, nowMs: currentNowMs())
  let actions = s.stepEngine(ev)
  try:
    s.executeActions(actions)
  except NngError as e:
    if s.engineKind == engPair1 and s.pair1Eng.polyamorous:
      return
    raise e
  s.updateSendFd()
  if s.lastErr != 0:
    let err = s.lastErr
    s.lastErr = 0
    raise newNngError(err, "Socket send failed: " & $err)

proc send*(s: Socket, data: openArray[byte], ctxId: uint32 = 0) =
  s.sendMsg(initSpMsg(data), ctxId)

proc send*(s: Socket, str: string) =
  var bytes = newSeq[byte](str.len)
  if str.len > 0:
    copyMem(addr bytes[0], unsafeAddr str[0], str.len)
  s.send(bytes)

proc recvMsg*(s: Socket, timeoutMs: int = -1): SpMsg =
  if s.closed:
    raise newNngError(NNG_ECLOSED, "Socket is closed")
  if s.engineKind == engSurveyor0 and not s.raw:
    if s.recvQueue.len == 0 and not s.inSurvey(0):
      raise newNngError(NNG_ESTATE, "Incorrect state: no survey in progress")
  if s.engineKind == engReq0 and not s.raw:
    if s.recvQueue.len == 0 and 0 notin s.req0Eng.ctxToReqId:
      raise newNngError(NNG_ESTATE, "Incorrect state: no request in flight")
  let tmo =
    if timeoutMs >= 0:
      timeoutMs
    else:
      int(s.options.getMs(NNG_OPT_RECVTIMEO))
  when not defined(js):
    let start = getMonoTime()
    while s.recvQueue.len == 0 and not s.closed:
      if s.lastErr != 0:
        let err = s.lastErr
        s.lastErr = 0
        raise newNngError(err, "Socket error: " & $err)
      try:
        asyncdispatch.poll(1)
      except CatchableError:
        os.sleep(1)
      if tmo >= 0:
        let elapsed = (getMonoTime() - start).inMilliseconds
        if elapsed > tmo:
          raise
            newNngError(NNG_ETIMEDOUT, "Socket recvMsg timed out after " & $tmo & "ms")
    if s.lastErr != 0:
      let err = s.lastErr
      s.lastErr = 0
      raise newNngError(err, "Socket error: " & $err)
  if s.recvQueue.len > 0:
    result = s.recvQueue.popFirst()
    if result.pipeId != 0 and result.pipeId in s.pipes:
      s.pipes[result.pipeId].drain()
    s.updateRecvFd()
    s.updateSendFd()
  else:
    raise newNngError(NNG_ECLOSED, "No message ready or socket closed")

proc recv*(s: Socket, timeoutMs: int = -1): seq[byte] =
  result = s.recvMsg(timeoutMs).body

proc recvStr*(s: Socket, timeoutMs: int = -1): string =
  let body = s.recv(timeoutMs)
  result = newString(body.len)
  if body.len > 0:
    copyMem(addr result[0], unsafeAddr body[0], body.len)

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
    raise newNngError(NNG_ENOTSUP, "subscribe() is only supported on SUB0 sockets")

proc subscribe*(s: Socket, topic: string) =
  var bytes = newSeq[byte](topic.len)
  if topic.len > 0:
    copyMem(addr bytes[0], unsafeAddr topic[0], topic.len)
  s.subscribe(bytes)

proc unsubscribe*(s: Socket, topic: openArray[byte]) =
  if s.engineKind == engSub0:
    s.sub0Eng.unsubscribe(topic)
    var kept = initDeque[SpMsg]()
    for m in s.recvQueue:
      if s.sub0Eng.matchesTopic(m.body):
        kept.addLast(m)
    s.recvQueue = kept
    s.updateRecvFd()
  else:
    raise newNngError(NNG_ENOTSUP, "unsubscribe() is only supported on SUB0 sockets")

proc unsubscribe*(s: Socket, topic: string) =
  var bytes = newSeq[byte](topic.len)
  if topic.len > 0:
    copyMem(addr bytes[0], unsafeAddr topic[0], topic.len)
  s.unsubscribe(bytes)

proc close*(s: Socket) =
  if not s.closed:
    s.closed = true
    if s.pendingRecvCb != nil:
      let cb = s.pendingRecvCb
      s.pendingRecvCb = nil
      cb(NNG_ECLOSED, SpMsg())
    s.deviceHook = nil
    when not defined(js):
      for fd in [s.recvFds[0], s.recvFds[1], s.sendFds[0], s.sendFds[1]]:
        discard posix.close(fd)
    var ctxsToClose: seq[Context] = @[]
    for ctx in s.contexts.values:
      ctxsToClose.add(ctx)
    s.contexts.clear()
    for ctx in ctxsToClose:
      ctx.close()
    var pipesToClose: seq[Pipe] = @[]
    for p in s.pipes.values:
      pipesToClose.add(p)
    s.pipes.clear()
    for p in pipesToClose:
      p.close()

# Raw Mode Device Forwarding
proc device*(s1, s2: Socket): Future[void] {.async.} =
  let target2 = if s2 != nil and s2 != s1: s2 else: s1
  if not s1.raw or (s2 != nil and not s2.raw):
    raise newNngError(NNG_EINVAL, "device requires raw mode sockets")

  while not s1.closed and (target2 == s1 or not target2.closed):
    # Forward s1 -> target2 if message ready
    if s1.hasRecv():
      let msg = s1.recvMsg(0)
      target2.sendMsg(msg)
    # Forward target2 -> s1 if message ready
    if target2 != s1 and target2.hasRecv():
      let msg = target2.recvMsg(0)
      s1.sendMsg(msg)
    await sleepAsync(1)

# Convenience Constructors
proc openPair0*(raw: bool = false): Socket =
  newSocket(spPair0, raw)

proc openPair1*(polyamorous: bool = false, raw: bool = false): Socket =
  result = newSocket(spPair1, raw)
  result.pair1Eng.polyamorous = polyamorous
  result.options.setBool(NNG_OPT_PAIR1_POLY, polyamorous)

proc openPub0*(raw: bool = false): Socket =
  newSocket(spPub0, raw)

proc openSub0*(raw: bool = false): Socket =
  newSocket(spSub0, raw)

proc openReq0*(raw: bool = false): Socket =
  newSocket(spReq0, raw)

proc openRep0*(raw: bool = false): Socket =
  newSocket(spRep0, raw)

proc openPush0*(raw: bool = false): Socket =
  newSocket(spPush0, raw)

proc openPull0*(raw: bool = false): Socket =
  newSocket(spPull0, raw)

proc openSurveyor0*(raw: bool = false): Socket =
  newSocket(spSurveyor0, raw)

proc openRespondent0*(raw: bool = false): Socket =
  newSocket(spRespondent0, raw)

proc openBus0*(raw: bool = false): Socket =
  newSocket(spBus0, raw)

const cApiModule* = "src/anynng/c_api.nim"
