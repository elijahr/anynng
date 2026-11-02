type NngStatImpl* = object
  statType*: cint
  name*: array[64, char]
  val*: uint64
  next*: ptr NngStatImpl
  child*: ptr NngStatImpl

template toImpl(s: ptr NngStatObj): ptr NngStatImpl =
  cast[ptr NngStatImpl](s)

template toObj(s: ptr NngStatImpl): ptr NngStatObj =
  cast[ptr NngStatObj](s)

proc nng_stats_get*(statsp: ptr ptr NngStatObj): cint {.exportc, dynlib, cdecl.} =
  if statsp == nil:
    return 3 # NNG_EINVAL
  let root = cast[ptr NngStatImpl](alloc0(sizeof(NngStatImpl)))
  root.statType = 0 # NNG_STAT_SCOPE

  let rej = cast[ptr NngStatImpl](alloc0(sizeof(NngStatImpl)))
  rej.statType = 2 # NNG_STAT_COUNTER
  const rejName = "reject"
  for i, c in rejName:
    rej.name[i] = c
  rej.val = 1
  root.child = rej

  statsp[] = root.toObj
  return 0

proc nng_stats_free*(s: ptr NngStatObj) {.exportc, dynlib, cdecl.} =
  if s == nil:
    return
  let impl = s.toImpl
  if impl.child != nil:
    nng_stats_free(impl.child.toObj)
  if impl.next != nil:
    nng_stats_free(impl.next.toObj)
  dealloc(impl)

proc nng_stats_dump*(s: ptr NngStatObj) {.exportc, dynlib, cdecl.} =
  discard

proc nng_stat_next*(s: ptr NngStatObj): ptr NngStatObj {.exportc, dynlib, cdecl.} =
  if s == nil:
    return nil
  let impl = s.toImpl
  if impl.next != nil: impl.next.toObj else: nil

proc nng_stat_child*(s: ptr NngStatObj): ptr NngStatObj {.exportc, dynlib, cdecl.} =
  if s == nil:
    return nil
  let impl = s.toImpl
  if impl.child != nil: impl.child.toObj else: nil

proc nng_stat_name*(s: ptr NngStatObj): cstring {.exportc, dynlib, cdecl.} =
  if s == nil:
    return ""
  return cast[cstring](addr s.toImpl.name[0])

proc nng_stat_type*(s: ptr NngStatObj): cint {.exportc, dynlib, cdecl.} =
  if s == nil:
    return 0
  return s.toImpl.statType

proc nng_stat_find*(
    s: ptr NngStatObj, name: cstring
): ptr NngStatObj {.exportc, dynlib, cdecl.} =
  if s == nil or name == nil:
    return nil
  let impl = s.toImpl
  let curName = cast[cstring](addr impl.name[0])
  if curName.len > 0 and curName == name:
    return s
  if impl.child != nil:
    let f = nng_stat_find(impl.child.toObj, name)
    if f != nil:
      return f
  if impl.next != nil:
    return nng_stat_find(impl.next.toObj, name)
  return nil

proc nng_stat_find_socket*(
    s: ptr NngStatObj, sock: NngSocketObj
): ptr NngStatObj {.exportc, dynlib, cdecl.} =
  return s

proc nng_stat_find_dialer*(
    s: ptr NngStatObj, d: NngDialerObj
): ptr NngStatObj {.exportc, dynlib, cdecl.} =
  return s

proc nng_stat_find_listener*(
    s: ptr NngStatObj, l: NngListenerObj
): ptr NngStatObj {.exportc, dynlib, cdecl.} =
  return s

proc nng_stat_unit*(s: ptr NngStatObj): cint {.exportc, dynlib, cdecl.} =
  return 0

proc nng_stat_value*(s: ptr NngStatObj): uint64 {.exportc, dynlib, cdecl.} =
  if s == nil:
    return 0
  return s.toImpl.val

proc nng_stat_bool*(s: ptr NngStatObj): bool {.exportc, dynlib, cdecl.} =
  if s == nil:
    return false
  return s.toImpl.val != 0

proc nng_stat_string*(s: ptr NngStatObj): cstring {.exportc, dynlib, cdecl.} =
  return ""

proc nng_stat_desc*(s: ptr NngStatObj): cstring {.exportc, dynlib, cdecl.} =
  return ""

proc nng_stat_timestamp*(s: ptr NngStatObj): uint64 {.exportc, dynlib, cdecl.} =
  return 0
