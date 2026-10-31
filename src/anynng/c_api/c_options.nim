# ----------------------------------------------------------------------
# Options API
# ----------------------------------------------------------------------

proc nng_socket_get*(
    s: NngSocketObj, name: cstring, val: pointer, szp: ptr csize_t
): cint {.exportc, dynlib, cdecl.} =
  if s.id notin gSockets or name == nil or szp == nil:
    return cint(NNG_EINVAL)
  let sock = gSockets[s.id]
  let optName = $name
  if optName in [NNG_OPT_SUB_SUBSCRIBE, NNG_OPT_SUB_UNSUBSCRIBE]:
    return cint(NNG_EWRITEONLY)
  try:
    if optName in sock.options.values:
      let entry = sock.options.values[optName]
      case entry.kind
      of optKindInt:
        if szp[] < csize_t(sizeof(cint)):
          return cint(NNG_EINVAL)
        if val != nil:
          cast[ptr cint](val)[] = cint(entry.iVal)
        szp[] = csize_t(sizeof(cint))
        return 0
      of optKindBool:
        if szp[] < csize_t(sizeof(bool)):
          return cint(NNG_EINVAL)
        if val != nil:
          cast[ptr bool](val)[] = entry.bVal
        szp[] = csize_t(sizeof(bool))
        return 0
      of optKindSize:
        if szp[] < csize_t(sizeof(csize_t)):
          return cint(NNG_EINVAL)
        if val != nil:
          cast[ptr csize_t](val)[] = csize_t(entry.sVal)
        szp[] = csize_t(sizeof(csize_t))
        return 0
      of optKindDuration:
        if szp[] < csize_t(sizeof(int32)):
          return cint(NNG_EINVAL)
        if val != nil:
          cast[ptr int32](val)[] = int32(entry.dVal)
        szp[] = csize_t(sizeof(int32))
        return 0
      of optKindString:
        let slen = entry.strVal.len + 1
        if szp[] < csize_t(slen):
          return cint(NNG_EINVAL)
        if val != nil:
          copyMem(val, cstring(entry.strVal), slen)
        szp[] = csize_t(slen)
        return 0
      else:
        return cint(NNG_ENOTSUP)
    else:
      return cint(NNG_ENOTSUP)
  except CatchableError as e:
    return toCError(e)

proc nng_socket_set*(
    s: NngSocketObj, name: cstring, val: pointer, sz: csize_t
): cint {.exportc, dynlib, cdecl.} =
  if s.id notin gSockets or name == nil:
    return cint(NNG_EINVAL)
  let sock = gSockets[s.id]
  let optName = $name
  if optName in [NNG_OPT_SUB_SUBSCRIBE, NNG_OPT_SUB_UNSUBSCRIBE]:
    if sock.engineKind != engSub0 or sock.raw:
      return cint(NNG_ENOTSUP)
    var topic = newSeq[byte](sz)
    if sz > 0 and val != nil:
      copyMem(addr topic[0], val, int(sz))
    try:
      if optName == NNG_OPT_SUB_SUBSCRIBE:
        sock.subscribe(topic)
      else:
        sock.unsubscribe(topic)
      return 0
    except CatchableError as e:
      return toCError(e)
  try:
    if optName in sock.options.values:
      let entry = sock.options.values[optName]
      case entry.kind
      of optKindInt:
        if sz != csize_t(sizeof(cint)) or val == nil:
          return cint(NNG_EINVAL)
        let iv = cast[ptr cint](val)[]
        sock.setOpt(optName, int(iv))
        return 0
      of optKindBool:
        if sz != csize_t(sizeof(bool)) or val == nil:
          return cint(NNG_EINVAL)
        let bv = cast[ptr bool](val)[]
        sock.setOpt(optName, bv)
        return 0
      of optKindSize:
        if sz != csize_t(sizeof(csize_t)) or val == nil:
          return cint(NNG_EINVAL)
        let sv = cast[ptr csize_t](val)[]
        sock.setOpt(optName, int(sv))
        return 0
      of optKindDuration:
        if sz != csize_t(sizeof(int32)) or val == nil:
          return cint(NNG_EINVAL)
        let mv = cast[ptr int32](val)[]
        sock.setOpt(optName, int64(mv))
        return 0
      else:
        return cint(NNG_ENOTSUP)
    else:
      return cint(NNG_ENOTSUP)
  except CatchableError as e:
    return toCError(e)

proc nng_socket_proto_id*(
    s: NngSocketObj, val: ptr uint16
): cint {.exportc, dynlib, cdecl.} =
  if s.id notin gSockets or val == nil:
    return cint(NNG_EINVAL)
  let sock = gSockets[s.id]
  try:
    val[] = uint16(sock.getInt(NNG_OPT_PROTO))
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_socket_peer_id*(
    s: NngSocketObj, val: ptr uint16
): cint {.exportc, dynlib, cdecl.} =
  if s.id notin gSockets or val == nil:
    return cint(NNG_EINVAL)
  let sock = gSockets[s.id]
  try:
    val[] = uint16(sock.getInt(NNG_OPT_PEER))
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_socket_proto_name*(
    s: NngSocketObj, val: ptr cstring
): cint {.exportc, dynlib, cdecl.} =
  if s.id notin gSockets or val == nil:
    return cint(NNG_EINVAL)
  let sock = gSockets[s.id]
  try:
    let str = sock.getString(NNG_OPT_PROTONAME)
    val[] = cstring(str)
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_socket_peer_name*(
    s: NngSocketObj, val: ptr cstring
): cint {.exportc, dynlib, cdecl.} =
  if s.id notin gSockets or val == nil:
    return cint(NNG_EINVAL)
  let sock = gSockets[s.id]
  try:
    let str = sock.getString(NNG_OPT_PEERNAME)
    val[] = cstring(str)
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_socket_raw*(s: NngSocketObj, val: ptr bool): cint {.exportc, dynlib, cdecl.} =
  if s.id notin gSockets or val == nil:
    return cint(NNG_EINVAL)
  let sock = gSockets[s.id]
  try:
    val[] = sock.getBool(NNG_OPT_RAW)
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_socket_get_int*(
    s: NngSocketObj, name: cstring, val: ptr cint
): cint {.exportc, dynlib, cdecl.} =
  if s.id notin gSockets or name == nil or val == nil:
    return cint(NNG_EINVAL)
  let sock = gSockets[s.id]
  let optName = $name
  if optName == NNG_OPT_SENDFD and sock.engineKind in {engSub0, engPull0}:
    return cint(NNG_ENOTSUP)
  if optName == NNG_OPT_RECVFD and sock.engineKind in {engPub0, engPush0}:
    return cint(NNG_ENOTSUP)
  try:
    val[] = cint(sock.getInt(optName))
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_socket_get_recv_poll_fd*(
    s: NngSocketObj, fdp: ptr cint
): cint {.exportc, dynlib, cdecl.} =
  nng_socket_get_int(s, NNG_OPT_RECVFD.cstring, fdp)

proc nng_socket_get_send_poll_fd*(
    s: NngSocketObj, fdp: ptr cint
): cint {.exportc, dynlib, cdecl.} =
  nng_socket_get_int(s, NNG_OPT_SENDFD.cstring, fdp)

proc nng_socket_pair*(fds: ptr cint): cint {.exportc, dynlib, cdecl.} =
  cint(NNG_ENOTSUP)

proc nng_socket_set_int*(
    s: NngSocketObj, name: cstring, val: cint
): cint {.exportc, dynlib, cdecl.} =
  if s.id notin gSockets or name == nil:
    return cint(NNG_EINVAL)
  let sock = gSockets[s.id]
  let optName = $name
  if optName in [NNG_OPT_SUB_SUBSCRIBE, NNG_OPT_SUB_UNSUBSCRIBE]:
    if sock.engineKind != engSub0 or sock.raw:
      return cint(NNG_ENOTSUP)
    var b = newSeq[byte](sizeof(cint))
    var v = val
    copyMem(addr b[0], addr v, sizeof(cint))
    try:
      if optName == NNG_OPT_SUB_SUBSCRIBE:
        sock.subscribe(b)
      else:
        sock.unsubscribe(b)
      return 0
    except CatchableError as e:
      return toCError(e)
  try:
    sock.setOpt(optName, int(val))
    return 0
  except CatchableError as e:
    return toCError(e)

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
  let optName = $name
  if optName in [NNG_OPT_SUB_SUBSCRIBE, NNG_OPT_SUB_UNSUBSCRIBE]:
    if sock.engineKind != engSub0 or sock.raw:
      return cint(NNG_ENOTSUP)
    var b = newSeq[byte](sizeof(bool))
    var v = val
    copyMem(addr b[0], addr v, sizeof(bool))
    try:
      if optName == NNG_OPT_SUB_SUBSCRIBE:
        sock.subscribe(b)
      else:
        sock.unsubscribe(b)
      return 0
    except CatchableError as e:
      return toCError(e)
  try:
    sock.setOpt(optName, val)
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

proc nng_socket_get_string*(
    s: NngSocketObj, name: cstring, val: ptr cstring
): cint {.exportc, dynlib, cdecl.} =
  if s.id notin gSockets or name == nil or val == nil:
    return cint(NNG_EINVAL)
  let sock = gSockets[s.id]
  try:
    let str = sock.getString($name)
    let cStr = cast[cstring](allocShared0(str.len + 1))
    copyMem(cStr, cstring(str), str.len + 1)
    val[] = cStr
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_socket_set_string*(
    s: NngSocketObj, name: cstring, val: cstring
): cint {.exportc, dynlib, cdecl.} =
  if s.id notin gSockets or name == nil or val == nil:
    return cint(NNG_EINVAL)
  let sock = gSockets[s.id]
  try:
    sock.setOpt($name, $val)
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_socket_get_ptr*(
    s: NngSocketObj, name: cstring, val: ptr pointer
): cint {.exportc, dynlib, cdecl.} =
  if s.id notin gSockets or name == nil or val == nil:
    return cint(NNG_EINVAL)
  let sock = gSockets[s.id]
  try:
    val[] = sock.options.getPtr($name)
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_socket_set_ptr*(
    s: NngSocketObj, name: cstring, val: pointer
): cint {.exportc, dynlib, cdecl.} =
  if s.id notin gSockets or name == nil:
    return cint(NNG_EINVAL)
  let sock = gSockets[s.id]
  try:
    sock.options.setPtr($name, val)
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

proc nng_dialer_set_ptr*(
    d: NngDialerObj, name: cstring, val: pointer
): cint {.exportc, dynlib, cdecl.} =
  if d.id notin gDialers or name == nil:
    return cint(NNG_EINVAL)
  let dialer = gDialers[d.id]
  if $name == NNG_OPT_TLS_CONFIG:
    for id, cfg in gTlsConfigs:
      if cast[pointer](cfg) == val:
        dialer.setTlsConfig(cfg)
        return 0
  try:
    dialer.options.setPtr($name, val)
    return 0
  except CatchableError as e:
    return toCError(e)

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

proc nng_listener_set_ptr*(
    l: NngListenerObj, name: cstring, val: pointer
): cint {.exportc, dynlib, cdecl.} =
  if l.id notin gListeners or name == nil:
    return cint(NNG_EINVAL)
  let listener = gListeners[l.id]
  if $name == NNG_OPT_TLS_CONFIG:
    for id, cfg in gTlsConfigs:
      if cast[pointer](cfg) == val:
        listener.setTlsConfig(cfg)
        return 0
  try:
    listener.options.setPtr($name, val)
    return 0
  except CatchableError as e:
    return toCError(e)

# Pipe Options
proc nng_pipe_get_bool*(
    p: NngPipeObj, name: cstring, val: ptr bool
): cint {.exportc, dynlib, cdecl.} =
  if p.id notin gPipes or name == nil or val == nil:
    return cint(NNG_EINVAL)
  let pipe = gPipes[p.id]
  try:
    val[] = pipe.getBool($name)
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_pipe_get_string*(
    p: NngPipeObj, name: cstring, val: ptr cstring
): cint {.exportc, dynlib, cdecl.} =
  if p.id notin gPipes or name == nil or val == nil:
    return cint(NNG_EINVAL)
  let pipe = gPipes[p.id]
  try:
    let str = pipe.getString($name)
    let cStr = cast[cstring](allocShared0(str.len + 1))
    copyMem(cStr, cstring(str), str.len + 1)
    val[] = cStr
    return 0
  except CatchableError as e:
    return toCError(e)
