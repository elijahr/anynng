# ----------------------------------------------------------------------
# Library Lifecycle & Utilities
# ----------------------------------------------------------------------

proc nng_version*(): cstring {.exportc, dynlib, cdecl.} =
  return "2.1.0-anynng"

proc nng_init*(params: pointer): cint {.exportc, dynlib, cdecl.} =
  return 0

proc nng_alloc*(sz: csize_t): pointer {.exportc, dynlib, cdecl.} =
  if sz == 0:
    return nil
  allocShared0(int(sz))

proc nng_free*(p: pointer, sz: csize_t) {.exportc, dynlib, cdecl.} =
  if p != nil:
    deallocShared(p)

proc nng_strdup*(src: cstring): cstring {.exportc, dynlib, cdecl.} =
  if src == nil:
    return nil
  let len = src.len
  let p = allocShared0(len + 1)
  if len > 0:
    copyMem(p, cast[pointer](src), len)
  return cast[cstring](p)

proc nng_fini*() {.exportc, dynlib, cdecl.} =
  # Teardown all open sockets
  var sockList: seq[Socket] = @[]
  for s in gSockets.values:
    sockList.add(s)
  for s in sockList:
    try:
      s.close()
    except CatchableError:
      discard
  gSockets.clear()
  gDialers.clear()
  gListeners.clear()
  gContexts.clear()
  gPipes.clear()

proc nng_strerror*(err: cint): cstring {.exportc, dynlib, cdecl.} =
  case int(err)
  of NNG_OK: "Success"
  of NNG_EINTR: "Interrupted"
  of NNG_ENOMEM: "Out of memory"
  of NNG_EINVAL: "Invalid argument"
  of NNG_EBUSY: "Resource busy"
  of NNG_ETIMEDOUT: "Timed out"
  of NNG_ECONNREFUSED: "Connection refused"
  of NNG_ECLOSED: "Object closed"
  of NNG_EAGAIN: "Try again"
  of NNG_ENOTSUP: "Not supported"
  of NNG_EADDRINUSE: "Address in use"
  of NNG_ESTATE: "Incorrect state"
  of NNG_ENOENT: "Entry not found"
  of NNG_EPROTO: "Protocol error"
  of NNG_EUNREACHABLE: "Destination unreachable"
  of NNG_EADDRINVAL: "Address invalid"
  of NNG_EPERM: "Permission denied"
  of NNG_EMSGSIZE: "Message too large"
  of NNG_ECONNABORTED: "Connection aborted"
  of NNG_ECONNRESET: "Connection reset"
  of NNG_ECANCELED: "Operation canceled"
  of NNG_ENOFILES: "Out of files"
  of NNG_ENOSPC: "Out of space"
  of NNG_EEXIST: "Resource already exists"
  of NNG_EREADONLY: "Read-only resource"
  of NNG_EWRITEONLY: "Write-only resource"
  of NNG_ECRYPTO: "Cryptographic error"
  of NNG_EPEERAUTH: "Peer authentication failed"
  of NNG_ENOARG: "Option requires an argument"
  of NNG_EAMBIGUOUS: "Ambiguous option"
  of NNG_EBADTYPE: "Incorrect type"
  of NNG_ECONNSHUT: "Connection shut down"
  of NNG_EINTERNAL: "Internal error"
  else: "Unknown error"

proc nng_strfree*(str: cstring) {.exportc, dynlib, cdecl.} =
  if str != nil:
    deallocShared(cast[pointer](str))

proc nng_clock*(): uint64 {.exportc, dynlib, cdecl.} =
  return uint64(getMonoTime().ticks div 1_000_000)

proc nng_msleep*(ms: uint32) {.exportc, dynlib, cdecl.} =
  let start = getMonoTime().ticks div 1_000_000
  let target = int64(ms)
  while true:
    servicePendingSends()
    let now = getMonoTime().ticks div 1_000_000
    if (now - start) >= target:
      break
    let rem = int(target - (now - start))
    let pollMs =
      if rem > 10:
        10
      elif rem > 0:
        rem
      else:
        1
    try:
      poll(pollMs)
    except CatchableError:
      sleep(min(rem, 5))

proc nng_random*(): uint32 {.exportc, dynlib, cdecl.} =
  return uint32(rand(high(int32)))

var gCurrentLogger: NngLogger = nil
var gLogLevel: cint = 3 # NNG_LOG_ERR
var gLogFacility: cint = 1 # NNG_LOG_USER
