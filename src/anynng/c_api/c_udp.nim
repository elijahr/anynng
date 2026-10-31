proc nng_udp_open*(udpp: ptr pointer, sa: pointer): cint {.exportc, dynlib, cdecl.} =
  if udpp == nil:
    return cint(NNG_EINVAL)
  let p = allocShared0(sizeof(NngUdpObj))
  udpp[] = p
  return 0

proc nng_udp_stop*(udp: pointer) {.exportc, dynlib, cdecl.} =
  discard

proc nng_udp_close*(udp: pointer) {.exportc, dynlib, cdecl.} =
  if udp != nil:
    deallocShared(udp)

proc nng_udp_sockname*(udp: pointer, sa: pointer): cint {.exportc, dynlib, cdecl.} =
  return 0

proc nng_udp_send*(udp: pointer, aio: ptr NngAioObj) {.exportc, dynlib, cdecl.} =
  if aio != nil:
    nng_aio_finish(aio, 0)

proc nng_udp_recv*(udp: pointer, aio: ptr NngAioObj) {.exportc, dynlib, cdecl.} =
  if aio != nil:
    nng_aio_finish(aio, 0)
