# ----------------------------------------------------------------------
# Send / Recv Sockets
# ----------------------------------------------------------------------

proc nng_send*(
    s: NngSocketObj, data: pointer, sz: csize_t, flags: cint
): cint {.exportc, dynlib, cdecl.} =
  if s.id notin gSockets:
    return cint(NNG_ECLOSED)
  let sock = gSockets[s.id]
  if sock.engineKind in {engSub0, engPull0}:
    return cint(NNG_ENOTSUP)
  var buf = newSeq[byte](sz)
  if sz > 0 and data != nil:
    copyMem(addr buf[0], data, int(sz))
  try:
    sock.send(buf)
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_recv*(
    s: NngSocketObj, data: pointer, szp: ptr csize_t, flags: cint
): cint {.exportc, dynlib, cdecl.} =
  if s.id notin gSockets:
    return cint(NNG_EINVAL)
  let sock = gSockets[s.id]
  if sock.engineKind in {engPub0, engPush0}:
    return cint(NNG_ENOTSUP)
  if szp == nil:
    return cint(NNG_EINVAL)
  let nonblock = (flags and 2) != 0 # NNG_FLAG_NONBLOCK
  let recvTimeout = sock.getMs(NNG_OPT_RECVTIMEO)

  if sock.lastErr != 0:
    let err = sock.lastErr
    sock.lastErr = 0
    return err
  let start = getMonoTime().ticks div 1_000_000
  if sock.engineKind == engSurveyor0 and not sock.raw:
    if sock.recvQueue.len == 0 and not sock.inSurvey(0):
      return cint(NNG_ESTATE)
  if sock.engineKind == engReq0 and not sock.raw:
    if sock.recvQueue.len == 0 and not hasReqInFlight(sock, 0):
      return cint(NNG_ESTATE)
  while sock.recvQueue.len == 0:
    tickSockets()
    servicePendingSends()
    if sock.lastErr != 0:
      let err = sock.lastErr
      sock.lastErr = 0
      return err
    if nonblock:
      return cint(NNG_EAGAIN)
    if sock.engineKind == engSurveyor0 and not sock.raw:
      if not sock.inSurvey(0):
        return cint(NNG_ETIMEDOUT)
    if recvTimeout >= 0:
      let now = getMonoTime().ticks div 1_000_000
      if (now - start) >= recvTimeout:
        return cint(NNG_ETIMEDOUT)
    try:
      poll(10)
    except CatchableError:
      sleep(5)

  if sock.recvQueue.len == 0:
    return cint(NNG_ETIMEDOUT)

  let msg = sock.recvMsg()
  let copyLen = min(int(szp[]), msg.body.len)
  if copyLen > 0 and data != nil:
    copyMem(data, unsafeAddr msg.body[0], copyLen)
  szp[] = csize_t(msg.body.len)
  return 0

proc nng_sendmsg*(
    s: NngSocketObj, msg: ptr NngMsgObj, flags: cint
): cint {.exportc, dynlib, cdecl.} =
  if s.id notin gSockets or msg == nil:
    return cint(NNG_EINVAL)
  let sock = gSockets[s.id]
  if sock.engineKind in {engSub0, engPull0}:
    return cint(NNG_ENOTSUP)
  var spMsg =
    SpMsg(header: msg.impl.header, body: msg.impl.body, pipeId: msg.impl.pipeId)
  try:
    sock.sendMsg(spMsg)
    nng_msg_free(msg)
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_recvmsg*(
    s: NngSocketObj, msgp: ptr ptr NngMsgObj, flags: cint
): cint {.exportc, dynlib, cdecl.} =
  if s.id notin gSockets:
    return cint(NNG_EINVAL)
  let sock = gSockets[s.id]
  if sock.engineKind in {engPub0, engPush0}:
    return cint(NNG_ENOTSUP)
  if msgp == nil:
    return cint(NNG_EINVAL)
  let nonblock = (flags and 2) != 0
  let recvTimeout = sock.getMs(NNG_OPT_RECVTIMEO)

  if sock.lastErr != 0:
    let err = sock.lastErr
    sock.lastErr = 0
    return err
  let start = getMonoTime().ticks div 1_000_000
  if sock.engineKind == engSurveyor0 and not sock.raw:
    if sock.recvQueue.len == 0 and not sock.inSurvey(0):
      return cint(NNG_ESTATE)
  if sock.engineKind == engReq0 and not sock.raw:
    if sock.recvQueue.len == 0 and not hasReqInFlight(sock, 0):
      return cint(NNG_ESTATE)
  while sock.recvQueue.len == 0:
    tickSockets()
    servicePendingSends()
    if sock.lastErr != 0:
      let err = sock.lastErr
      sock.lastErr = 0
      return err
    if nonblock:
      return cint(NNG_EAGAIN)
    if sock.engineKind == engSurveyor0 and not sock.raw:
      if not sock.inSurvey(0):
        return cint(NNG_ETIMEDOUT)
    if recvTimeout >= 0:
      let now = getMonoTime().ticks div 1_000_000
      if (now - start) >= recvTimeout:
        return cint(NNG_ETIMEDOUT)
    try:
      poll(10)
    except CatchableError:
      sleep(5)

  if sock.recvQueue.len == 0:
    return cint(NNG_ETIMEDOUT)

  try:
    let spMsg = sock.recvMsg()
    let m = createShared(NngMsgImpl)
    m.header = spMsg.header
    m.body = spMsg.body
    m.pipeId = spMsg.pipeId
    msgp[] = cast[ptr NngMsgObj](m)
    return 0
  except CatchableError as e:
    return toCError(e)
