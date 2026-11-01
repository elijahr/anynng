# anynng - Pure-Nim multi-target (C, JS, WASM) and multi-transport NNG / Scalability Protocols
# Licensed under MIT License.

import std/[tables, deques]
import ./[types, options, runtime_types, socket]

when not defined(js):
  import std/[asyncdispatch, monotimes, times, os]

# Context Implementation
proc close*(ctx: Context)

gCloseContext = close

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
  if (name == NNG_OPT_RECVBUF or name == NNG_OPT_SENDBUF) and (
    (ctx.socket.engineKind in {engPub0, engBus0} and val == 0) or
    (ctx.socket.engineKind == engSub0 and not ctx.socket.raw and val == 0)
  ):
    raise newNngError(NNG_EINVAL, "Buffer size 0 not supported on pub/sub/bus")
  ctx.options.setInt(name, val)

proc setOpt*(ctx: Context, name: string, val: int64) =
  ctx.options.setMs(name, val)

proc setOpt*(ctx: Context, name: string, val: bool) =
  ctx.options.setBool(name, val)

proc getBool*(ctx: Context, name: string): bool =
  ctx.options.getBool(name)

proc setOpt*(ctx: Context, name: string, val: uint64) =
  ctx.options.setSize(name, val)

proc getSize*(ctx: Context, name: string): uint64 =
  ctx.options.getSize(name)

proc getInt*(ctx: Context, name: string): int =
  ctx.options.getInt(name)

proc getMs*(ctx: Context, name: string): int64 =
  ctx.options.getMs(name)
