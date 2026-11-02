proc nng_udp_open*(
    udpp: ptr ptr NngUdpObj, sa: ptr NngSockaddr
): cint {.exportc, dynlib, cdecl.} =
  if udpp == nil:
    return cint(NNG_EINVAL)
  let p = cast[ptr NngUdpObj](allocShared0(sizeof(NngUdpObj)))
  udpp[] = p
  return 0

proc nng_udp_stop*(udp: ptr NngUdpObj) {.exportc, dynlib, cdecl.} =
  discard

proc nng_udp_close*(udp: ptr NngUdpObj) {.exportc, dynlib, cdecl.} =
  if udp != nil:
    deallocShared(udp)

proc nng_udp_sockname*(
    udp: ptr NngUdpObj, sa: ptr NngSockaddr
): cint {.exportc, dynlib, cdecl.} =
  return 0

proc nng_udp_send*(udp: ptr NngUdpObj, aio: ptr NngAioObj) {.exportc, dynlib, cdecl.} =
  if aio != nil:
    nng_aio_finish(aio, 0)

proc nng_udp_recv*(udp: ptr NngUdpObj, aio: ptr NngAioObj) {.exportc, dynlib, cdecl.} =
  if aio != nil:
    nng_aio_finish(aio, 0)
