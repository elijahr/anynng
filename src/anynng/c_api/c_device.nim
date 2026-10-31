# ----------------------------------------------------------------------
# Device (raw forwarder)
# ----------------------------------------------------------------------

proc nng_device_aio*(
    aio: ptr NngAioObj, s1: NngSocketObj, s2: NngSocketObj
) {.exportc, dynlib, cdecl.} =
  if aio == nil:
    return
  if s1.id notin gSockets and s2.id notin gSockets:
    nng_aio_finish(aio, cint(NNG_EINVAL))
    return
  let sock1 =
    if s1.id in gSockets:
      gSockets[s1.id]
    else:
      nil
  let sock2 =
    if s2.id in gSockets:
      gSockets[s2.id]
    else:
      nil
  let src = if sock1 != nil: sock1 else: sock2
  let dst = if sock2 != nil: sock2 else: sock1
  if not src.raw or not dst.raw:
    nng_aio_finish(aio, cint(NNG_EINVAL))
    return

  aio.finished = false
  aio.err = 0
  if s1.id != 0:
    gDeviceSockets.incl(s1.id)
    aio.devS1 = s1.id
  if s2.id != 0:
    gDeviceSockets.incl(s2.id)
    aio.devS2 = s2.id
  if src == dst:
    src.deviceHook = proc(msg: SpMsg) =
      src.sendMsg(msg)
    while src.recvQueue.len > 0:
      let m = src.recvQueue.popFirst()
      src.sendMsg(m)
  else:
    src.deviceHook = proc(msg: SpMsg) =
      dst.sendMsg(msg)
    dst.deviceHook = proc(msg: SpMsg) =
      src.sendMsg(msg)
    while src.recvQueue.len > 0:
      let m = src.recvQueue.popFirst()
      dst.sendMsg(m)
    while dst.recvQueue.len > 0:
      let m = dst.recvQueue.popFirst()
      src.sendMsg(m)

proc nng_device*(s1: NngSocketObj, s2: NngSocketObj): cint {.exportc, dynlib, cdecl.} =
  var aio: ptr NngAioObj = nil
  let rv = nng_aio_alloc(addr aio, nil, nil)
  if rv != 0:
    return rv
  nng_device_aio(aio, s1, s2)
  nng_aio_wait(aio)
  result = nng_aio_result(aio)
  nng_aio_free(aio)
