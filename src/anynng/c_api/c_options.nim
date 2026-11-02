# ----------------------------------------------------------------------
# Options API (Pure NNG v2.1.0)
# ----------------------------------------------------------------------

# Socket Introspection
proc nng_socket_proto_id*(
    s: NngSocketObj, val: ptr uint16
): cint {.exportc, dynlib, cdecl.} =
  if s.id notin gSockets or val == nil:
    return cint(NNG_EINVAL)
  let sock = gSockets[s.id]
  val[] = sock.protoId()
  return 0

proc nng_socket_peer_id*(
    s: NngSocketObj, val: ptr uint16
): cint {.exportc, dynlib, cdecl.} =
  if s.id notin gSockets or val == nil:
    return cint(NNG_EINVAL)
  let sock = gSockets[s.id]
  val[] = sock.peerId()
  return 0

proc nng_socket_proto_name*(
    s: NngSocketObj, val: ptr cstring
): cint {.exportc, dynlib, cdecl.} =
  if s.id notin gSockets or val == nil:
    return cint(NNG_EINVAL)
  let sock = gSockets[s.id]
  val[] = cstring(sock.protoName())
  return 0

proc nng_socket_peer_name*(
    s: NngSocketObj, val: ptr cstring
): cint {.exportc, dynlib, cdecl.} =
  if s.id notin gSockets or val == nil:
    return cint(NNG_EINVAL)
  let sock = gSockets[s.id]
  val[] = cstring(sock.peerName())
  return 0

proc nng_socket_raw*(s: NngSocketObj, val: ptr bool): cint {.exportc, dynlib, cdecl.} =
  if s.id notin gSockets or val == nil:
    return cint(NNG_EINVAL)
  let sock = gSockets[s.id]
  val[] = sock.isRaw()
  return 0

proc nng_socket_get_int*(
    s: NngSocketObj, name: cstring, val: ptr cint
): cint {.exportc, dynlib, cdecl.} =
  if s.id notin gSockets or name == nil or val == nil:
    return cint(NNG_EINVAL)
  let sock = gSockets[s.id]
  try:
    val[] = cint(sock.getInt($name))
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_socket_set_int*(
    s: NngSocketObj, name: cstring, val: cint
): cint {.exportc, dynlib, cdecl.} =
  if s.id notin gSockets or name == nil:
    return cint(NNG_EINVAL)
  let sock = gSockets[s.id]
  try:
    sock.setOpt($name, int(val))
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_socket_get_recv_poll_fd*(
    s: NngSocketObj, fdp: ptr cint
): cint {.exportc, dynlib, cdecl.} =
  if s.id notin gSockets or fdp == nil:
    return cint(NNG_EINVAL)
  let sock = gSockets[s.id]
  try:
    let fd = sock.recvPollFd()
    if fd < 0:
      return cint(NNG_ENOTSUP)
    fdp[] = fd
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_socket_get_send_poll_fd*(
    s: NngSocketObj, fdp: ptr cint
): cint {.exportc, dynlib, cdecl.} =
  if s.id notin gSockets or fdp == nil:
    return cint(NNG_EINVAL)
  let sock = gSockets[s.id]
  try:
    let fd = sock.sendPollFd()
    if fd < 0:
      return cint(NNG_ENOTSUP)
    fdp[] = fd
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_socket_pair*(fds: ptr cint): cint {.exportc, dynlib, cdecl.} =
  cint(NNG_ENOTSUP)

proc nng_socket_get_bool*(
    s: NngSocketObj, name: cstring, val: ptr bool
): cint {.exportc, dynlib, cdecl.} =
  if s.id notin gSockets or name == nil or val == nil:
    return cint(NNG_EINVAL)
  let sock = gSockets[s.id]
  try:
    val[] = sock.getBool($name)
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_socket_set_bool*(
    s: NngSocketObj, name: cstring, val: bool
): cint {.exportc, dynlib, cdecl.} =
  if s.id notin gSockets or name == nil:
    return cint(NNG_EINVAL)
  let sock = gSockets[s.id]
  try:
    sock.setOpt($name, val)
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_socket_get_ms*(
    s: NngSocketObj, name: cstring, val: ptr int32
): cint {.exportc, dynlib, cdecl.} =
  if s.id notin gSockets or name == nil or val == nil:
    return cint(NNG_EINVAL)
  let sock = gSockets[s.id]
  try:
    val[] = int32(sock.getMs($name))
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_socket_set_ms*(
    s: NngSocketObj, name: cstring, val: int32
): cint {.exportc, dynlib, cdecl.} =
  if s.id notin gSockets or name == nil:
    return cint(NNG_EINVAL)
  let sock = gSockets[s.id]
  try:
    sock.setOpt($name, int64(val))
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_socket_get_size*(
    s: NngSocketObj, name: cstring, val: ptr csize_t
): cint {.exportc, dynlib, cdecl.} =
  if s.id notin gSockets or name == nil or val == nil:
    return cint(NNG_EINVAL)
  let sock = gSockets[s.id]
  try:
    val[] = csize_t(sock.getSize($name))
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_socket_set_size*(
    s: NngSocketObj, name: cstring, val: csize_t
): cint {.exportc, dynlib, cdecl.} =
  if s.id notin gSockets or name == nil:
    return cint(NNG_EINVAL)
  let sock = gSockets[s.id]
  try:
    sock.setOpt($name, uint64(val))
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_socket_get_uint64*(
    s: NngSocketObj, name: cstring, val: ptr uint64
): cint {.exportc, dynlib, cdecl.} =
  if s.id notin gSockets or name == nil or val == nil:
    return cint(NNG_EINVAL)
  let sock = gSockets[s.id]
  try:
    val[] = sock.getUint64($name)
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_socket_set_uint64*(
    s: NngSocketObj, name: cstring, val: uint64
): cint {.exportc, dynlib, cdecl.} =
  if s.id notin gSockets or name == nil:
    return cint(NNG_EINVAL)
  let sock = gSockets[s.id]
  try:
    sock.setOpt($name, val)
    return 0
  except CatchableError as e:
    return toCError(e)

# Dialer Options
proc nng_dialer_get_int*(
    d: NngDialerObj, name: cstring, val: ptr cint
): cint {.exportc, dynlib, cdecl.} =
  if d.id notin gDialers or name == nil or val == nil:
    return cint(NNG_EINVAL)
  let dialer = gDialers[d.id]
  try:
    val[] = cint(dialer.options.getInt($name))
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_dialer_set_int*(
    d: NngDialerObj, name: cstring, val: cint
): cint {.exportc, dynlib, cdecl.} =
  if d.id notin gDialers or name == nil:
    return cint(NNG_EINVAL)
  let dialer = gDialers[d.id]
  try:
    dialer.options.setInt($name, int(val))
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_dialer_get_bool*(
    d: NngDialerObj, name: cstring, val: ptr bool
): cint {.exportc, dynlib, cdecl.} =
  if d.id notin gDialers or name == nil or val == nil:
    return cint(NNG_EINVAL)
  let dialer = gDialers[d.id]
  try:
    val[] = dialer.options.getBool($name)
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_dialer_set_bool*(
    d: NngDialerObj, name: cstring, val: bool
): cint {.exportc, dynlib, cdecl.} =
  if d.id notin gDialers or name == nil:
    return cint(NNG_EINVAL)
  let dialer = gDialers[d.id]
  try:
    dialer.options.setBool($name, val)
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_dialer_get_size*(
    d: NngDialerObj, name: cstring, val: ptr csize_t
): cint {.exportc, dynlib, cdecl.} =
  if d.id notin gDialers or name == nil or val == nil:
    return cint(NNG_EINVAL)
  let dialer = gDialers[d.id]
  try:
    val[] = csize_t(dialer.options.getSize($name))
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_dialer_set_size*(
    d: NngDialerObj, name: cstring, val: csize_t
): cint {.exportc, dynlib, cdecl.} =
  if d.id notin gDialers or name == nil:
    return cint(NNG_EINVAL)
  let dialer = gDialers[d.id]
  try:
    dialer.options.setSize($name, uint64(val))
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_dialer_get_uint64*(
    d: NngDialerObj, name: cstring, val: ptr uint64
): cint {.exportc, dynlib, cdecl.} =
  if d.id notin gDialers or name == nil or val == nil:
    return cint(NNG_EINVAL)
  let dialer = gDialers[d.id]
  try:
    val[] = dialer.options.getUint64($name)
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_dialer_set_uint64*(
    d: NngDialerObj, name: cstring, val: uint64
): cint {.exportc, dynlib, cdecl.} =
  if d.id notin gDialers or name == nil:
    return cint(NNG_EINVAL)
  let dialer = gDialers[d.id]
  try:
    dialer.options.setUint64($name, val)
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_dialer_get_string*(
    d: NngDialerObj, name: cstring, val: ptr cstring
): cint {.exportc, dynlib, cdecl.} =
  if d.id notin gDialers or name == nil or val == nil:
    return cint(NNG_EINVAL)
  let dialer = gDialers[d.id]
  try:
    let str = dialer.getString($name)
    let cStr = cast[cstring](allocShared0(str.len + 1))
    copyMem(cStr, cstring(str), str.len + 1)
    val[] = cStr
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_dialer_set_string*(
    d: NngDialerObj, name: cstring, val: cstring
): cint {.exportc, dynlib, cdecl.} =
  if d.id notin gDialers or name == nil or val == nil:
    return cint(NNG_EINVAL)
  let dialer = gDialers[d.id]
  try:
    dialer.options.setString($name, $val)
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_dialer_get_ms*(
    d: NngDialerObj, name: cstring, val: ptr int32
): cint {.exportc, dynlib, cdecl.} =
  if d.id notin gDialers or name == nil or val == nil:
    return cint(NNG_EINVAL)
  let dialer = gDialers[d.id]
  try:
    val[] = int32(dialer.getMs($name))
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_dialer_set_ms*(
    d: NngDialerObj, name: cstring, val: int32
): cint {.exportc, dynlib, cdecl.} =
  if d.id notin gDialers or name == nil:
    return cint(NNG_EINVAL)
  let dialer = gDialers[d.id]
  try:
    dialer.setOpt($name, int64(val))
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_dialer_get_tls*(
    d: NngDialerObj, cfgp: ptr ptr NngTlsConfigObj
): cint {.exportc, dynlib, cdecl.} =
  if d.id notin gDialers or cfgp == nil:
    return cint(NNG_EINVAL)
  let dialer = gDialers[d.id]
  if dialer.tlsCfg == nil:
    return cint(NNG_ENOENT)
  cfgp[] = cast[ptr NngTlsConfigObj](dialer.tlsCfg)
  return 0

proc nng_dialer_set_tls*(
    d: NngDialerObj, cfg: ptr NngTlsConfigObj
): cint {.exportc, dynlib, cdecl.} =
  if d.id notin gDialers:
    return cint(NNG_EINVAL)
  let dialer = gDialers[d.id]
  if cfg == nil:
    dialer.setTls(nil)
    return 0
  for _, c in gTlsConfigs:
    if cast[ptr NngTlsConfigObj](c) == cfg:
      dialer.setTls(c)
      return 0
  dialer.setTls(cast[TlsConfig](cfg))
  return 0

# Listener Options
proc nng_listener_get_int*(
    l: NngListenerObj, name: cstring, val: ptr cint
): cint {.exportc, dynlib, cdecl.} =
  if l.id notin gListeners or name == nil or val == nil:
    return cint(NNG_EINVAL)
  let listener = gListeners[l.id]
  try:
    val[] = cint(listener.getInt($name))
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_listener_set_int*(
    l: NngListenerObj, name: cstring, val: cint
): cint {.exportc, dynlib, cdecl.} =
  if l.id notin gListeners or name == nil:
    return cint(NNG_EINVAL)
  let listener = gListeners[l.id]
  try:
    listener.options.setInt($name, int(val))
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_listener_get_bool*(
    l: NngListenerObj, name: cstring, val: ptr bool
): cint {.exportc, dynlib, cdecl.} =
  if l.id notin gListeners or name == nil or val == nil:
    return cint(NNG_EINVAL)
  let listener = gListeners[l.id]
  try:
    val[] = listener.options.getBool($name)
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_listener_set_bool*(
    l: NngListenerObj, name: cstring, val: bool
): cint {.exportc, dynlib, cdecl.} =
  if l.id notin gListeners or name == nil:
    return cint(NNG_EINVAL)
  let listener = gListeners[l.id]
  try:
    listener.options.setBool($name, val)
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_listener_get_size*(
    l: NngListenerObj, name: cstring, val: ptr csize_t
): cint {.exportc, dynlib, cdecl.} =
  if l.id notin gListeners or name == nil or val == nil:
    return cint(NNG_EINVAL)
  let listener = gListeners[l.id]
  try:
    val[] = csize_t(listener.options.getSize($name))
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_listener_set_size*(
    l: NngListenerObj, name: cstring, val: csize_t
): cint {.exportc, dynlib, cdecl.} =
  if l.id notin gListeners or name == nil:
    return cint(NNG_EINVAL)
  let listener = gListeners[l.id]
  try:
    listener.options.setSize($name, uint64(val))
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_listener_get_uint64*(
    l: NngListenerObj, name: cstring, val: ptr uint64
): cint {.exportc, dynlib, cdecl.} =
  if l.id notin gListeners or name == nil or val == nil:
    return cint(NNG_EINVAL)
  let listener = gListeners[l.id]
  try:
    val[] = listener.options.getUint64($name)
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_listener_set_uint64*(
    l: NngListenerObj, name: cstring, val: uint64
): cint {.exportc, dynlib, cdecl.} =
  if l.id notin gListeners or name == nil:
    return cint(NNG_EINVAL)
  let listener = gListeners[l.id]
  try:
    listener.options.setUint64($name, val)
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_listener_get_string*(
    l: NngListenerObj, name: cstring, val: ptr cstring
): cint {.exportc, dynlib, cdecl.} =
  if l.id notin gListeners or name == nil or val == nil:
    return cint(NNG_EINVAL)
  let listener = gListeners[l.id]
  try:
    let str = listener.getString($name)
    let cStr = cast[cstring](allocShared0(str.len + 1))
    copyMem(cStr, cstring(str), str.len + 1)
    val[] = cStr
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_listener_set_string*(
    l: NngListenerObj, name: cstring, val: cstring
): cint {.exportc, dynlib, cdecl.} =
  if l.id notin gListeners or name == nil or val == nil:
    return cint(NNG_EINVAL)
  let listener = gListeners[l.id]
  try:
    listener.options.setString($name, $val)
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_listener_get_ms*(
    l: NngListenerObj, name: cstring, val: ptr int32
): cint {.exportc, dynlib, cdecl.} =
  if l.id notin gListeners or name == nil or val == nil:
    return cint(NNG_EINVAL)
  let listener = gListeners[l.id]
  try:
    val[] = int32(listener.getInt($name))
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_listener_set_ms*(
    l: NngListenerObj, name: cstring, val: int32
): cint {.exportc, dynlib, cdecl.} =
  if l.id notin gListeners or name == nil:
    return cint(NNG_EINVAL)
  let listener = gListeners[l.id]
  try:
    listener.options.setInt($name, int(val))
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_listener_get_tls*(
    l: NngListenerObj, cfgp: ptr ptr NngTlsConfigObj
): cint {.exportc, dynlib, cdecl.} =
  if l.id notin gListeners or cfgp == nil:
    return cint(NNG_EINVAL)
  let listener = gListeners[l.id]
  if listener.tlsCfg == nil:
    return cint(NNG_ENOENT)
  cfgp[] = cast[ptr NngTlsConfigObj](listener.tlsCfg)
  return 0

proc nng_listener_set_tls*(
    l: NngListenerObj, cfg: ptr NngTlsConfigObj
): cint {.exportc, dynlib, cdecl.} =
  if l.id notin gListeners:
    return cint(NNG_EINVAL)
  let listener = gListeners[l.id]
  if cfg == nil:
    listener.setTls(nil)
    return 0
  for _, c in gTlsConfigs:
    if cast[ptr NngTlsConfigObj](c) == cfg:
      listener.setTls(c)
      return 0
  listener.setTls(cast[TlsConfig](cfg))
  return 0
