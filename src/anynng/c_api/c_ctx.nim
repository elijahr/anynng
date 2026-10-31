# ----------------------------------------------------------------------
# Contexts (nng_ctx)
# ----------------------------------------------------------------------

proc regCtx(ctx: Context): NngCtxObj =
  let id = gNextCtxId
  inc gNextCtxId
  gContexts[id] = ctx
  gClosedCtxs.excl(id)
  result.id = id

proc nng_ctx_open*(
    cp: ptr NngCtxObj, s: NngSocketObj
): cint {.exportc, dynlib, cdecl.} =
  if s.id notin gSockets or cp == nil:
    return cint(NNG_EINVAL)
  let sock = gSockets[s.id]
  try:
    let ctx = sock.openContext()
    cp[] = regCtx(ctx)
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_ctx_close*(c: NngCtxObj): cint {.exportc, dynlib, cdecl.} =
  if c.id in gClosedCtxs:
    return cint(NNG_ECLOSED)
  if c.id notin gContexts:
    return cint(NNG_ECLOSED)
  let ctx = gContexts[c.id]
  closePendingSendsForCtx(c.id)
  gContexts.del(c.id)
  gClosedCtxs.incl(c.id)
  try:
    ctx.close()
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_ctx_id*(c: NngCtxObj): cint {.exportc, dynlib, cdecl.} =
  return cint(c.id)

proc nng_ctx_sendmsg*(
    c: NngCtxObj, msg: ptr NngMsgObj, flags: cint
): cint {.exportc, dynlib, cdecl.} =
  if c.id notin gContexts or msg == nil:
    return cint(NNG_EINVAL)
  let ctx = gContexts[c.id]
  if ctx.socket.engineKind in {engSub0, engPull0}:
    return cint(NNG_ENOTSUP)
  var spMsg = SpMsg(header: msg.header, body: msg.body, pipeId: msg.pipeId)
  try:
    ctx.sendMsg(spMsg)
    nng_msg_free(msg)
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_ctx_recvmsg*(
    c: NngCtxObj, msgp: ptr ptr NngMsgObj, flags: cint
): cint {.exportc, dynlib, cdecl.} =
  if c.id notin gContexts or msgp == nil:
    return cint(NNG_EINVAL)
  let ctx = gContexts[c.id]
  if ctx.socket.engineKind in {engPub0, engPush0}:
    return cint(NNG_ENOTSUP)
  let nonblock = (flags and 2) != 0
  let recvTimeout = ctx.socket.getMs(NNG_OPT_RECVTIMEO)

  if ctx.lastErr != 0:
    let err = ctx.lastErr
    ctx.lastErr = 0
    return err

  # If context recvQueue is empty but socket has messages for Rep/Respondent, claim one
  if ctx.recvQueue.len == 0 and ctx.socket.recvQueue.len > 0 and
      ctx.socket.engineKind in {engRep0, engRespondent0}:
    let msg = ctx.socket.recvQueue.popFirst()
    if ctx.socket.engineKind == engRespondent0 and
        ctx.socket.respondent0Eng.reqStack.len > 0:
      ctx.socket.respondent0Eng.contexts[ctx.ctxId] =
        ctx.socket.respondent0Eng.reqStack.pop()
    elif ctx.socket.engineKind == engRep0 and ctx.socket.rep0Eng.reqStack.len > 0:
      ctx.socket.rep0Eng.contexts[ctx.ctxId] = ctx.socket.rep0Eng.reqStack.pop()
    ctx.recvQueue.addLast(msg)

  let start = getMonoTime().ticks div 1_000_000
  if ctx.socket.engineKind == engSurveyor0 and not ctx.socket.raw:
    if ctx.recvQueue.len == 0 and not ctx.socket.inSurvey(ctx.ctxId):
      return cint(NNG_ESTATE)
  if ctx.socket.engineKind == engReq0 and not ctx.socket.raw:
    if ctx.recvQueue.len == 0 and not hasReqInFlight(ctx.socket, c.id):
      return cint(NNG_ESTATE)
  while ctx.recvQueue.len == 0:
    tickSockets()
    servicePendingSends()
    if ctx.lastErr != 0:
      let err = ctx.lastErr
      ctx.lastErr = 0
      return err
    if nonblock:
      return cint(NNG_EAGAIN)
    if ctx.socket.engineKind == engSurveyor0 and not ctx.socket.raw:
      if not ctx.socket.inSurvey(ctx.ctxId):
        return cint(NNG_ETIMEDOUT)
    if recvTimeout >= 0:
      let now = getMonoTime().ticks div 1_000_000
      if (now - start) >= recvTimeout:
        return cint(NNG_ETIMEDOUT)
    try:
      poll(10)
    except CatchableError:
      sleep(5)
    if ctx.recvQueue.len == 0 and ctx.socket.recvQueue.len > 0 and
        ctx.socket.engineKind in {engRep0, engRespondent0}:
      let msg = ctx.socket.recvQueue.popFirst()
      if ctx.socket.engineKind == engRespondent0 and
          ctx.socket.respondent0Eng.reqStack.len > 0:
        ctx.socket.respondent0Eng.contexts[ctx.ctxId] =
          ctx.socket.respondent0Eng.reqStack.pop()
      elif ctx.socket.engineKind == engRep0 and ctx.socket.rep0Eng.reqStack.len > 0:
        ctx.socket.rep0Eng.contexts[ctx.ctxId] = ctx.socket.rep0Eng.reqStack.pop()
      ctx.recvQueue.addLast(msg)

  if ctx.recvQueue.len == 0:
    return cint(NNG_ETIMEDOUT)

  try:
    let spMsg = ctx.recvMsg()
    let m = createShared(NngMsgObj)
    m.header = spMsg.header
    m.body = spMsg.body
    m.pipeId = spMsg.pipeId
    msgp[] = m
    return 0
  except CatchableError as e:
    return toCError(e)

# Context Options
proc nng_ctx_get*(
    c: NngCtxObj, name: cstring, val: pointer, szp: ptr csize_t
): cint {.exportc, dynlib, cdecl.} =
  if c.id in gClosedCtxs:
    return cint(NNG_ECLOSED)
  if c.id notin gContexts or name == nil or szp == nil:
    return cint(NNG_EINVAL)
  let optName = $name
  if optName in [NNG_OPT_SUB_SUBSCRIBE, NNG_OPT_SUB_UNSUBSCRIBE]:
    return cint(NNG_EWRITEONLY)
  return cint(NNG_ENOTSUP)

proc nng_ctx_get_int*(
    c: NngCtxObj, name: cstring, val: ptr cint
): cint {.exportc, dynlib, cdecl.} =
  if c.id in gClosedCtxs:
    return cint(NNG_ECLOSED)
  if c.id notin gContexts or name == nil or val == nil:
    return cint(NNG_EINVAL)
  let optName = $name
  if optName in [NNG_OPT_SENDFD, NNG_OPT_RECVFD]:
    return cint(NNG_ENOTSUP)
  let ctx = gContexts[c.id]
  try:
    val[] = cint(ctx.getInt(optName))
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_ctx_set*(
    c: NngCtxObj, name: cstring, val: pointer, sz: csize_t
): cint {.exportc, dynlib, cdecl.} =
  if c.id in gClosedCtxs:
    return cint(NNG_ECLOSED)
  if c.id notin gContexts or name == nil:
    return cint(NNG_EINVAL)
  let ctx = gContexts[c.id]
  let optName = $name
  if optName == NNG_OPT_SUB_SUBSCRIBE:
    if ctx.socket.engineKind == engSub0:
      var b = newSeq[byte](sz)
      if sz > 0 and val != nil:
        copyMem(addr b[0], val, int(sz))
      try:
        ctx.subscribe(b)
        return 0
      except CatchableError as e:
        return toCError(e)
    return cint(NNG_ENOTSUP)
  elif optName == NNG_OPT_SUB_UNSUBSCRIBE:
    if ctx.socket.engineKind == engSub0:
      var b = newSeq[byte](sz)
      if sz > 0 and val != nil:
        copyMem(addr b[0], val, int(sz))
      try:
        ctx.unsubscribe(b)
        return 0
      except CatchableError as e:
        return toCError(e)
    return cint(NNG_ENOTSUP)
  try:
    if optName in ctx.options.values:
      let entry = ctx.options.values[optName]
      case entry.kind
      of optKindInt:
        if sz != csize_t(sizeof(cint)) or val == nil:
          return cint(NNG_EINVAL)
        let iv = cast[ptr cint](val)[]
        ctx.setOpt(optName, int(iv))
        return 0
      of optKindDuration:
        if sz != csize_t(sizeof(int32)) or val == nil:
          return cint(NNG_EINVAL)
        let mv = cast[ptr int32](val)[]
        ctx.setOpt(optName, int(mv))
        return 0
      else:
        return cint(NNG_ENOTSUP)
    else:
      return cint(NNG_ENOTSUP)
  except CatchableError as e:
    return toCError(e)

# SUB0 Specific Convenience Wrappers
proc nng_sub0_socket_subscribe*(
    s: NngSocketObj, buf: pointer, sz: csize_t
): cint {.exportc, dynlib, cdecl.} =
  if s.id in gClosedSockets or s.id notin gSockets:
    return cint(NNG_ECLOSED)
  let sock = gSockets[s.id]
  if sock.closed:
    return cint(NNG_ECLOSED)
  if sock.engineKind != engSub0 or sock.raw:
    return cint(NNG_ENOTSUP)
  var b = newSeq[byte](sz)
  if sz > 0 and buf != nil:
    copyMem(addr b[0], buf, int(sz))
  try:
    sock.subscribe(b)
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_sub0_socket_unsubscribe*(
    s: NngSocketObj, buf: pointer, sz: csize_t
): cint {.exportc, dynlib, cdecl.} =
  if s.id in gClosedSockets or s.id notin gSockets:
    return cint(NNG_ECLOSED)
  let sock = gSockets[s.id]
  if sock.closed:
    return cint(NNG_ECLOSED)
  if sock.engineKind != engSub0 or sock.raw:
    return cint(NNG_ENOTSUP)
  var b = newSeq[byte](sz)
  if sz > 0 and buf != nil:
    copyMem(addr b[0], buf, int(sz))
  try:
    sock.unsubscribe(b)
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_sub0_ctx_subscribe*(
    c: NngCtxObj, buf: pointer, sz: csize_t
): cint {.exportc, dynlib, cdecl.} =
  if c.id in gClosedCtxs or c.id notin gContexts:
    return cint(NNG_ECLOSED)
  let ctx = gContexts[c.id]
  if ctx.closed or ctx.socket.closed or ctx.socket.id in gClosedSockets:
    return cint(NNG_ECLOSED)
  if ctx.socket.engineKind != engSub0:
    return cint(NNG_ENOTSUP)
  var b = newSeq[byte](sz)
  if sz > 0 and buf != nil:
    copyMem(addr b[0], buf, int(sz))
  try:
    ctx.subscribe(b)
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_sub0_ctx_unsubscribe*(
    c: NngCtxObj, buf: pointer, sz: csize_t
): cint {.exportc, dynlib, cdecl.} =
  if c.id in gClosedCtxs or c.id notin gContexts:
    return cint(NNG_ECLOSED)
  let ctx = gContexts[c.id]
  if ctx.closed or ctx.socket.closed or ctx.socket.id in gClosedSockets:
    return cint(NNG_ECLOSED)
  if ctx.socket.engineKind != engSub0:
    return cint(NNG_ENOTSUP)
  var b = newSeq[byte](sz)
  if sz > 0 and buf != nil:
    copyMem(addr b[0], buf, int(sz))
  try:
    ctx.unsubscribe(b)
    return 0
  except CatchableError as e:
    return toCError(e)
