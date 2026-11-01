# ----------------------------------------------------------------------
# Sockets & Protocol Openers
# ----------------------------------------------------------------------

proc regSocket(s: Socket): NngSocketObj =
  let id = gNextSockId
  inc gNextSockId
  s.id = id
  gSockets[id] = s
  result.id = id

proc nng_socket_close*(s: NngSocketObj): cint {.exportc, dynlib, cdecl.} =
  if s.id in gClosedSockets or s.id notin gSockets:
    return cint(NNG_ECLOSED)
  if s.id in gDeviceSockets:
    return cint(NNG_EBUSY)
  let sock = gSockets[s.id]
  closePendingSendsForSocket(s.id)
  gSockets.del(s.id)
  gClosedSockets.incl(s.id)
  gPipeCallbacks.del(s.id)
  var ctxsToDel: seq[uint32] = @[]
  for cid, ctx in gContexts:
    if ctx.socket == sock:
      ctxsToDel.add(cid)
  for cid in ctxsToDel:
    closePendingSendsForCtx(cid)
    gClosedCtxs.incl(cid)
    gContexts.del(cid)
  try:
    sock.close()
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_socket_id*(s: NngSocketObj): cint {.exportc, dynlib, cdecl.} =
  if s.id == 0 or s.id notin gSockets or s.id in gClosedSockets:
    return -1
  return cint(s.id)

proc nng_pair0_open*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
  if s == nil:
    return cint(NNG_EINVAL)
  s[] = regSocket(openPair0())
  return 0

proc nng_pair0_open_raw*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
  if s == nil:
    return cint(NNG_EINVAL)
  s[] = regSocket(openPair0(raw = true))
  return 0

proc nng_pair1_open*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
  if s == nil:
    return cint(NNG_EINVAL)
  s[] = regSocket(openPair1())
  return 0

proc nng_pair1_open_raw*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
  if s == nil:
    return cint(NNG_EINVAL)
  s[] = regSocket(openPair1(raw = true))
  return 0

proc nng_pair1_open_poly*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
  if s == nil:
    return cint(NNG_EINVAL)
  s[] = regSocket(openPair1(polyamorous = true))
  return 0

proc nng_pub0_open*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
  if s == nil:
    return cint(NNG_EINVAL)
  s[] = regSocket(openPub0())
  return 0

proc nng_pub0_open_raw*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
  if s == nil:
    return cint(NNG_EINVAL)
  s[] = regSocket(openPub0(raw = true))
  return 0

proc nng_sub0_open*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
  if s == nil:
    return cint(NNG_EINVAL)
  s[] = regSocket(openSub0())
  return 0

proc nng_sub0_open_raw*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
  if s == nil:
    return cint(NNG_EINVAL)
  s[] = regSocket(openSub0(raw = true))
  return 0

proc nng_req0_open*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
  if s == nil:
    return cint(NNG_EINVAL)
  s[] = regSocket(openReq0())
  return 0

proc nng_req0_open_raw*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
  if s == nil:
    return cint(NNG_EINVAL)
  s[] = regSocket(openReq0(raw = true))
  return 0

proc nng_rep0_open*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
  if s == nil:
    return cint(NNG_EINVAL)
  s[] = regSocket(openRep0())
  return 0

proc nng_rep0_open_raw*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
  if s == nil:
    return cint(NNG_EINVAL)
  s[] = regSocket(openRep0(raw = true))
  return 0

proc nng_push0_open*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
  if s == nil:
    return cint(NNG_EINVAL)
  s[] = regSocket(openPush0())
  return 0

proc nng_push0_open_raw*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
  if s == nil:
    return cint(NNG_EINVAL)
  s[] = regSocket(openPush0(raw = true))
  return 0

proc nng_pull0_open*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
  if s == nil:
    return cint(NNG_EINVAL)
  s[] = regSocket(openPull0())
  return 0

proc nng_pull0_open_raw*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
  if s == nil:
    return cint(NNG_EINVAL)
  s[] = regSocket(openPull0(raw = true))
  return 0

proc nng_surveyor0_open*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
  if s == nil:
    return cint(NNG_EINVAL)
  s[] = regSocket(openSurveyor0())
  return 0

proc nng_surveyor0_open_raw*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
  if s == nil:
    return cint(NNG_EINVAL)
  s[] = regSocket(openSurveyor0(raw = true))
  return 0

proc nng_respondent0_open*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
  if s == nil:
    return cint(NNG_EINVAL)
  s[] = regSocket(openRespondent0())
  return 0

proc nng_respondent0_open_raw*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
  if s == nil:
    return cint(NNG_EINVAL)
  s[] = regSocket(openRespondent0(raw = true))
  return 0

proc nng_bus0_open*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
  if s == nil:
    return cint(NNG_EINVAL)
  s[] = regSocket(openBus0())
  return 0

proc nng_bus0_open_raw*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
  if s == nil:
    return cint(NNG_EINVAL)
  s[] = regSocket(openBus0(raw = true))
  return 0
