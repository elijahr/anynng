# ----------------------------------------------------------------------
# AIO (Asynchronous I/O)
# ----------------------------------------------------------------------

proc nng_aio_alloc*(
    aiop: ptr ptr NngAioObj, cb: proc(arg: pointer) {.cdecl.}, arg: pointer
): cint {.exportc, dynlib, cdecl.} =
  if aiop == nil:
    return cint(NNG_EINVAL)
  let aio = createShared(NngAioObj)
  aio.cb = cb
  aio.arg = arg
  aio.msg = nil
  aio.err = 0
  aio.timeout = -1
  aio.finished = false
  aiop[] = aio
  return 0

proc nng_aio_free*(aio: ptr NngAioObj) {.exportc, dynlib, cdecl.} =
  if aio != nil:
    cleanupAio(aio)
    let idx = gPendingSendAios.find(aio)
    if idx >= 0:
      gPendingSendAios.delete(idx)
    freeShared(aio)

proc nng_aio_stop*(aio: ptr NngAioObj) {.exportc, dynlib, cdecl.} =
  if aio != nil:
    cleanupAio(aio)
    let idx = gPendingSendAios.find(aio)
    if idx >= 0:
      gPendingSendAios.delete(idx)
    aio.stopped = true
    aio.err = cint(NNG_ECANCELED)
    aio.finished = true

proc nng_aio_wait*(aio: ptr NngAioObj) {.exportc, dynlib, cdecl.} =
  if aio == nil:
    return
  let start = getMonoTime().ticks div 1_000_000
  while not aio.finished:
    servicePendingSends()
    if aio.finished:
      break
    if aio.timeout >= 0:
      let now = getMonoTime().ticks div 1_000_000
      if (now - start) >= aio.timeout:
        let idx = gPendingSendAios.find(aio)
        if idx >= 0:
          gPendingSendAios.delete(idx)
        aio.err = cint(NNG_ETIMEDOUT)
        aio.finished = true
        break
    for id, s in gSockets:
      if s.closed:
        continue
      if s.engineKind == engSurveyor0 and not s.raw:
        discard s.inSurvey(0)
        if s.pendingRecvCb != nil and not s.inSurvey(0):
          let cb = s.pendingRecvCb
          s.pendingRecvCb = nil
          cb(NNG_ETIMEDOUT, SpMsg())
      elif s.engineKind == engReq0 and not s.raw:
        let rcvTmo = s.getMs(NNG_OPT_RECVTIMEO)
        if rcvTmo >= 0 and s.pendingRecvCb != nil:
          let now = getMonoTime().ticks div 1_000_000
          if (now - start) >= rcvTmo:
            let cb = s.pendingRecvCb
            s.pendingRecvCb = nil
            cb(NNG_ETIMEDOUT, SpMsg())
            var j = 0
            while j < gPendingSendAios.len:
              let snd = gPendingSendAios[j]
              if snd.sockId == s.id and snd.ctxId == 0:
                gPendingSendAios.delete(j)
                nng_aio_finish(snd, cint(NNG_ECANCELED))
              else:
                inc j
    for id, c in gContexts:
      if c.closed or c.socket.closed:
        continue
      if c.socket.engineKind == engSurveyor0 and not c.socket.raw:
        discard c.socket.inSurvey(c.ctxId)
        if c.pendingRecvCb != nil and not c.socket.inSurvey(c.ctxId):
          let cb = c.pendingRecvCb
          c.pendingRecvCb = nil
          cb(NNG_ETIMEDOUT, SpMsg())
      elif c.socket.engineKind == engReq0 and not c.socket.raw:
        let rcvTmo = c.socket.getMs(NNG_OPT_RECVTIMEO)
        if rcvTmo >= 0 and c.pendingRecvCb != nil:
          let now = getMonoTime().ticks div 1_000_000
          if (now - start) >= rcvTmo:
            let cb = c.pendingRecvCb
            c.pendingRecvCb = nil
            cb(NNG_ETIMEDOUT, SpMsg())
            var j = 0
            while j < gPendingSendAios.len:
              let snd = gPendingSendAios[j]
              if snd.sockId == c.socket.id and snd.ctxId == c.ctxId:
                gPendingSendAios.delete(j)
                nng_aio_finish(snd, cint(NNG_ECANCELED))
              else:
                inc j
    try:
      poll(10)
    except CatchableError:
      sleep(5)

proc nng_aio_cancel*(aio: ptr NngAioObj) {.exportc, dynlib, cdecl.} =
  if aio != nil:
    cleanupAio(aio)
    let idx = gPendingSendAios.find(aio)
    if idx >= 0:
      gPendingSendAios.delete(idx)
    aio.err = cint(NNG_ECANCELED)
    aio.finished = true

proc nng_aio_abort*(aio: ptr NngAioObj, err: cint) {.exportc, dynlib, cdecl.} =
  if aio != nil:
    cleanupAio(aio)
    let idx = gPendingSendAios.find(aio)
    if idx >= 0:
      gPendingSendAios.delete(idx)
    aio.err = err
    aio.finished = true
    aio.stopped = true

proc nng_aio_result*(aio: ptr NngAioObj): cint {.exportc, dynlib, cdecl.} =
  if aio == nil:
    return cint(NNG_EINVAL)
  return aio.err

proc nng_aio_set_timeout*(aio: ptr NngAioObj, ms: int64) {.exportc, dynlib, cdecl.} =
  if aio != nil:
    aio.timeout = ms

proc nng_aio_set_msg*(
    aio: ptr NngAioObj, msg: ptr NngMsgObj
) {.exportc, dynlib, cdecl.} =
  if aio != nil:
    aio.msg = msg

proc nng_aio_get_msg*(aio: ptr NngAioObj): ptr NngMsgObj {.exportc, dynlib, cdecl.} =
  if aio == nil:
    return nil
  result = aio.msg

proc nng_aio_finish*(aio: ptr NngAioObj, err: cint) {.exportc, dynlib, cdecl.} =
  if aio != nil:
    aio.err = err
    aio.finished = true
    if aio.cb != nil:
      aio.cb(aio.arg)

proc nng_sleep_aio*(ms: int64, aio: ptr NngAioObj) {.exportc, dynlib, cdecl.} =
  if aio == nil:
    return
  if aio.stopped:
    nng_aio_finish(aio, cint(NNG_ESTOPPED))
    return
  aio.finished = false
  aio.err = 0
  let now = getMonoTime().ticks div 1_000_000
  aio.wakeTime = now + max(0'i64, ms)
  if aio notin gPendingSleepAios:
    gPendingSleepAios.add(aio)

proc nng_socket_send*(s: NngSocketObj, aio: ptr NngAioObj) {.exportc, dynlib, cdecl.} =
  if aio == nil:
    return
  if aio.stopped:
    nng_aio_finish(aio, cint(NNG_ESTOPPED))
    return
  if s.id in gClosedSockets:
    nng_aio_finish(aio, cint(NNG_ECLOSED))
    return
  if s.id notin gSockets or aio.msg == nil:
    nng_aio_finish(aio, cint(NNG_EINVAL))
    return
  let sock = gSockets[s.id]
  if sock.closed:
    nng_aio_finish(aio, cint(NNG_ECLOSED))
    return
  if sock.engineKind in {engSub0, engPull0}:
    nng_aio_finish(aio, cint(NNG_ENOTSUP))
    return
  aio.sockId = s.id
  aio.ctxId = 0
  aio.startTime = getMonoTime().ticks div 1_000_000
  if sock.engineKind == engReq0:
    var i = 0
    while i < gPendingSendAios.len:
      let prev = gPendingSendAios[i]
      if prev.sockId == s.id and prev.ctxId == 0:
        gPendingSendAios.delete(i)
        nng_aio_finish(prev, cint(NNG_ECANCELED))
      else:
        inc i
  if sock.pipes.len == 0 and sock.engineKind in {engPush0, engPair0, engPair1, engReq0}:
    if aio.timeout == 0:
      nng_aio_finish(aio, cint(NNG_ETIMEDOUT))
      return
    aio.finished = false
    aio.err = 0
    gPendingSendAios.add(aio)
    return
  let res = nng_sendmsg(s, aio.msg, 0)
  aio.msg = nil
  nng_aio_finish(aio, res)

proc nng_socket_recv*(s: NngSocketObj, aio: ptr NngAioObj) {.exportc, dynlib, cdecl.} =
  if aio != nil and aio.stopped:
    nng_aio_finish(aio, cint(NNG_ESTOPPED))
    return
  if s.id in gClosedSockets:
    if aio != nil:
      nng_aio_finish(aio, cint(NNG_ECLOSED))
    return
  if s.id notin gSockets or aio == nil:
    if aio != nil:
      nng_aio_finish(aio, cint(NNG_EINVAL))
    return
  let sock = gSockets[s.id]
  if sock.closed:
    nng_aio_finish(aio, cint(NNG_ECLOSED))
    return
  if sock.engineKind in {engPub0, engPush0}:
    nng_aio_finish(aio, cint(NNG_ENOTSUP))
    return
  if sock.pendingRecvCb != nil and sock.engineKind in {engRep0, engRespondent0}:
    nng_aio_finish(aio, cint(NNG_ESTATE))
    return
  if sock.engineKind == engSurveyor0 and not sock.raw and sock.recvQueue.len == 0 and
      not sock.inSurvey(0):
    nng_aio_finish(aio, cint(NNG_ESTATE))
    return
  if sock.engineKind == engReq0 and not sock.raw and sock.recvQueue.len == 0 and
      not hasReqInFlight(sock, 0):
    nng_aio_finish(aio, cint(NNG_ESTATE))
    return
  if sock.recvQueue.len > 0:
    var msg: ptr NngMsgObj = nil
    let res = nng_recvmsg(s, addr msg, 2)
    if res == 0:
      nng_aio_set_msg(aio, msg)
    nng_aio_finish(aio, res)
  else:
    aio.finished = false
    aio.err = 0
    sock.pendingRecvCb = proc(res: int, msg: SpMsg) =
      if res == 0:
        let m = createShared(NngMsgObj)
        m.header = msg.header
        m.body = msg.body
        m.pipeId = msg.pipeId
        nng_aio_set_msg(aio, m)
      nng_aio_finish(aio, cint(res))

proc nng_ctx_send*(c: NngCtxObj, aio: ptr NngAioObj) {.exportc, dynlib, cdecl.} =
  if aio != nil and aio.stopped:
    nng_aio_finish(aio, cint(NNG_ESTOPPED))
    return
  if c.id in gClosedCtxs:
    if aio != nil:
      nng_aio_finish(aio, cint(NNG_ECLOSED))
    return
  if c.id notin gContexts or aio == nil or aio.msg == nil:
    if aio != nil:
      nng_aio_finish(aio, cint(NNG_EINVAL))
    return
  let ctx = gContexts[c.id]
  let sock = ctx.socket
  if ctx.closed or sock.closed:
    nng_aio_finish(aio, cint(NNG_ECLOSED))
    return
  if sock.engineKind in {engSub0, engPull0}:
    nng_aio_finish(aio, cint(NNG_ENOTSUP))
    return
  if sock.engineKind == engRespondent0:
    if ctx.ctxId notin sock.respondent0Eng.contexts and
        (sock.respondent0Eng.reqStack.len == 0 or not sock.raw):
      nng_aio_finish(aio, cint(NNG_ESTATE))
      return
    if aio.timeout == 0:
      nng_aio_finish(aio, cint(NNG_ETIMEDOUT))
      return
  elif sock.engineKind == engRep0:
    if sock.inFlight > 0:
      if aio.timeout == 0:
        nng_aio_finish(aio, cint(NNG_ETIMEDOUT))
        return
      aio.finished = false
      aio.err = 0
      gPendingSendAios.add(aio)
      return
    inc sock.inFlight
  aio.sockId = sock.id
  aio.ctxId = c.id
  aio.startTime = getMonoTime().ticks div 1_000_000
  if sock.engineKind == engReq0:
    var i = 0
    while i < gPendingSendAios.len:
      let prev = gPendingSendAios[i]
      if prev.sockId == sock.id and prev.ctxId == c.id:
        gPendingSendAios.delete(i)
        nng_aio_finish(prev, cint(NNG_ECANCELED))
      else:
        inc i
  if sock.pipes.len == 0 and sock.engineKind in {engPush0, engPair0, engPair1, engReq0}:
    if aio.timeout == 0:
      nng_aio_finish(aio, cint(NNG_ETIMEDOUT))
      return
    aio.finished = false
    aio.err = 0
    gPendingSendAios.add(aio)
    return
  let res = nng_ctx_sendmsg(c, aio.msg, 0)
  aio.msg = nil
  nng_aio_finish(aio, res)

proc nng_ctx_recv*(c: NngCtxObj, aio: ptr NngAioObj) {.exportc, dynlib, cdecl.} =
  if aio != nil and aio.stopped:
    nng_aio_finish(aio, cint(NNG_ESTOPPED))
    return
  if c.id in gClosedCtxs:
    if aio != nil:
      nng_aio_finish(aio, cint(NNG_ECLOSED))
    return
  if c.id notin gContexts or aio == nil:
    if aio != nil:
      nng_aio_finish(aio, cint(NNG_EINVAL))
    return
  let ctx = gContexts[c.id]
  if ctx.closed or ctx.socket.closed:
    nng_aio_finish(aio, cint(NNG_ECLOSED))
    return
  if ctx.socket.engineKind in {engPub0, engPush0}:
    nng_aio_finish(aio, cint(NNG_ENOTSUP))
    return
  if ctx.pendingRecvCb != nil and ctx.socket.engineKind in {engRep0, engRespondent0}:
    nng_aio_finish(aio, cint(NNG_ESTATE))
    return
  if ctx.socket.engineKind == engSurveyor0 and not ctx.socket.raw and
      ctx.recvQueue.len == 0 and not ctx.socket.inSurvey(ctx.ctxId):
    nng_aio_finish(aio, cint(NNG_ESTATE))
    return
  if ctx.socket.engineKind == engReq0 and not ctx.socket.raw and ctx.recvQueue.len == 0 and
      not hasReqInFlight(ctx.socket, c.id):
    nng_aio_finish(aio, cint(NNG_ESTATE))
    return
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
  if ctx.recvQueue.len > 0:
    var msg: ptr NngMsgObj = nil
    let res = nng_ctx_recvmsg(c, addr msg, 2)
    if res == 0:
      nng_aio_set_msg(aio, msg)
    nng_aio_finish(aio, res)
  else:
    aio.finished = false
    aio.err = 0
    aio.sockId = ctx.socket.id
    aio.ctxId = c.id
    aio.startTime = getMonoTime().ticks div 1_000_000
    ctx.pendingRecvCb = proc(res: int, msg: SpMsg) =
      if res == 0:
        let m = createShared(NngMsgObj)
        m.header = msg.header
        m.body = msg.body
        m.pipeId = msg.pipeId
        nng_aio_set_msg(aio, m)
      nng_aio_finish(aio, cint(res))
