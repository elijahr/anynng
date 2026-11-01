# anynng - Pure-Nim multi-target (C, JS, WASM) and multi-transport NNG / Scalability Protocols
# Licensed under MIT License.

import std/[tables, deques]
import ./[types, options, runtime_types]
import ./protocols/[pair0, pair1, pubsub0, reqrep0, pipeline0, survey0, bus0]

when not defined(js):
  import std/[asyncdispatch, monotimes, times, posix, os]

proc updateRecvFd*(s: Socket)
proc updateSendFd*(s: Socket)
proc executeActions*(s: Socket, actions: seq[EngineAction])
proc stepEngine*(s: Socket, ev: EngineEvent): seq[EngineAction]

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
  if (name == NNG_OPT_RECVBUF or name == NNG_OPT_SENDBUF) and (
    (s.engineKind in {engPub0, engBus0} and val == 0) or
    (s.engineKind == engSub0 and not s.raw and val == 0)
  ):
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

proc protoId*(s: Socket): uint16 =
  uint16(s.proto)

proc peerId*(s: Socket): uint16 =
  uint16(peerProto(s.proto))

proc protoName*(s: Socket): string =
  protoName(s.proto)

proc peerName*(s: Socket): string =
  protoName(peerProto(s.proto))

proc isRaw*(s: Socket): bool =
  s.raw

proc recvPollFd*(s: Socket): cint =
  when defined(js):
    -1
  else:
    if s.closed or s.engineKind in {engPub0, engPush0}:
      -1
    else:
      cint(s.recvFds[0])

proc sendPollFd*(s: Socket): cint =
  when defined(js):
    -1
  else:
    if s.closed or s.engineKind in {engSub0, engPull0}:
      -1
    else:
      cint(s.sendFds[0])

proc setOpt*(s: Socket, name: string, val: uint64) =
  if name in gOptionDescriptors and gOptionDescriptors[name].kind == optKindUint64:
    s.options.setUint64(name, val)
  else:
    s.options.setSize(name, val)

proc setOpt*(s: Socket, name: string, val: string) =
  s.options.setString(name, val)

proc getBool*(s: Socket, name: string): bool =
  if name == NNG_OPT_PAIR1_POLY and s.engineKind == engPair1:
    return s.pair1Eng.polyamorous
  s.options.getBool(name)

proc getInt*(s: Socket, name: string): int =
  s.options.getInt(name)

proc getMs*(s: Socket, name: string): int64 =
  s.options.getMs(name)

proc getSize*(s: Socket, name: string): uint64 =
  s.options.getSize(name)

proc getUint64*(s: Socket, name: string): uint64 =
  s.options.getUint64(name)

proc getString*(s: Socket, name: string): string =
  s.options.getString(name)

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
      if gCloseContext != nil:
        gCloseContext(ctx)
    var pipesToClose: seq[Pipe] = @[]
    for p in s.pipes.values:
      pipesToClose.add(p)
    s.pipes.clear()
    for p in pipesToClose:
      p.close()

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
