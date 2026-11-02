# ----------------------------------------------------------------------
# Pipes & Introspection
# ----------------------------------------------------------------------

proc nng_pipe_id*(p: NngPipeObj): cint {.exportc, dynlib, cdecl.} =
  return cint(p.id)

proc nng_pipe_socket*(p: NngPipeObj): NngSocketObj {.exportc, dynlib, cdecl.} =
  if p.id in gPipes:
    let pipe = gPipes[p.id]
    let s = pipe.socket
    if s != nil and not s.closed and s.id notin gClosedSockets:
      for id, sock in gSockets:
        if sock == s:
          return NngSocketObj(id: id)
  for id, s in gSockets:
    if not s.closed and s.id notin gClosedSockets and PipeId(p.id) in s.pipes:
      return NngSocketObj(id: id)
  return NngSocketObj(id: 0)

proc nng_pipe_dialer*(p: NngPipeObj): NngDialerObj {.exportc, dynlib, cdecl.} =
  if p.id in gPipes:
    let pipe = gPipes[p.id]
    let d = pipe.dialer
    if d != nil:
      for id, dialer in gDialers:
        if dialer == d:
          return NngDialerObj(id: id)
  return NngDialerObj(id: 0)

proc nng_pipe_listener*(p: NngPipeObj): NngListenerObj {.exportc, dynlib, cdecl.} =
  if p.id in gPipes:
    let pipe = gPipes[p.id]
    let l = pipe.listener
    if l != nil:
      for id, listener in gListeners:
        if listener == l:
          return NngListenerObj(id: id)
  return NngListenerObj(id: 0)

proc nng_pipe_close*(p: NngPipeObj): cint {.exportc, dynlib, cdecl.} =
  var found = false
  if p.id in gPipes:
    let pipe = gPipes[p.id]
    let sock = pipe.socket
    pipe.close()
    gPipes.del(p.id)
    if sock != nil:
      closePendingSendsForSocket(sock.id)
    found = true
  for id, sock in gSockets:
    if PipeId(p.id) in sock.pipes:
      let pipe = sock.pipes[PipeId(p.id)]
      sock.pipes.del(PipeId(p.id))
      pipe.close()
      closePendingSendsForSocket(id)
      found = true
  if found:
    return 0
  return cint(NNG_ECLOSED)

proc nng_pipe_notify*(
    s: NngSocketObj,
    ev: cint,
    cb: proc(p: NngPipeObj, ev: cint, arg: pointer) {.cdecl.},
    arg: pointer,
): cint {.exportc, dynlib, cdecl.} =
  if s.id notin gSockets:
    return cint(NNG_EINVAL)
  let sock = gSockets[s.id]
  let pipeEv =
    case ev
    of 1:
      pipeEvAddPre
    of 2:
      pipeEvAddPost
    of 3:
      pipeEvRemPost
    else:
      return cint(NNG_EINVAL)

  if cb == nil:
    sock.pipeNotify(pipeEv, nil)
    return 0

  sock.pipeNotify(
    pipeEv,
    proc(p: Pipe, e: PipeEvent) =
      let pid = uint32(p.pipeId)
      gPipes[pid] = p
      cb(NngPipeObj(id: pid), cint(ev), arg),
  )
  return 0

proc findPipe(id: uint32): Pipe =
  if id in gPipes:
    return gPipes[id]
  for _, s in gSockets:
    if not s.closed and PipeId(id) in s.pipes:
      return s.pipes[PipeId(id)]
  return nil

proc nng_pipe_get_bool*(
    p: NngPipeObj, name: cstring, val: ptr bool
): cint {.exportc, dynlib, cdecl.} =
  if name == nil or val == nil:
    return cint(NNG_EINVAL)
  let pipe = findPipe(p.id)
  if pipe == nil:
    return cint(NNG_ECLOSED)
  try:
    val[] = pipe.getBool($name)
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_pipe_get_int*(
    p: NngPipeObj, name: cstring, val: ptr cint
): cint {.exportc, dynlib, cdecl.} =
  if name == nil or val == nil:
    return cint(NNG_EINVAL)
  let pipe = findPipe(p.id)
  if pipe == nil:
    return cint(NNG_ECLOSED)
  return cint(NNG_ENOTSUP)

proc nng_pipe_get_ms*(
    p: NngPipeObj, name: cstring, val: ptr int32
): cint {.exportc, dynlib, cdecl.} =
  if name == nil or val == nil:
    return cint(NNG_EINVAL)
  let pipe = findPipe(p.id)
  if pipe == nil:
    return cint(NNG_ECLOSED)
  return cint(NNG_ENOTSUP)

proc nng_pipe_get_size*(
    p: NngPipeObj, name: cstring, val: ptr csize_t
): cint {.exportc, dynlib, cdecl.} =
  if name == nil or val == nil:
    return cint(NNG_EINVAL)
  let pipe = findPipe(p.id)
  if pipe == nil:
    return cint(NNG_ECLOSED)
  return cint(NNG_ENOTSUP)

proc nng_pipe_get_uint64*(
    p: NngPipeObj, name: cstring, val: ptr uint64
): cint {.exportc, dynlib, cdecl.} =
  if name == nil or val == nil:
    return cint(NNG_EINVAL)
  let pipe = findPipe(p.id)
  if pipe == nil:
    return cint(NNG_ECLOSED)
  return cint(NNG_ENOTSUP)

proc nng_pipe_get_string*(
    p: NngPipeObj, name: cstring, val: ptr cstring
): cint {.exportc, dynlib, cdecl.} =
  if name == nil or val == nil:
    return cint(NNG_EINVAL)
  let pipe = findPipe(p.id)
  if pipe == nil:
    return cint(NNG_ECLOSED)
  try:
    let str = pipe.getString($name)
    val[] = cstring(str)
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_pipe_get_strdup*(
    p: NngPipeObj, name: cstring, val: ptr cstring
): cint {.exportc, dynlib, cdecl.} =
  if name == nil or val == nil:
    return cint(NNG_EINVAL)
  let pipe = findPipe(p.id)
  if pipe == nil:
    return cint(NNG_ECLOSED)
  try:
    let str = pipe.getString($name)
    let cStr = cast[cstring](allocShared0(str.len + 1))
    copyMem(cStr, cstring(str), str.len + 1)
    val[] = cStr
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_pipe_get_strcpy*(
    p: NngPipeObj, name: cstring, buf: cstring, bufsz: csize_t
): cint {.exportc, dynlib, cdecl.} =
  if name == nil or buf == nil or bufsz == 0:
    return cint(NNG_EINVAL)
  let pipe = findPipe(p.id)
  if pipe == nil:
    return cint(NNG_ECLOSED)
  try:
    let str = pipe.getString($name)
    let copyLen = min(str.len, int(bufsz) - 1)
    if copyLen > 0:
      copyMem(buf, cstring(str), copyLen)
    cast[ptr UncheckedArray[char]](buf)[copyLen] = '\0'
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_pipe_get_strlen*(
    p: NngPipeObj, name: cstring, szp: ptr csize_t
): cint {.exportc, dynlib, cdecl.} =
  if name == nil or szp == nil:
    return cint(NNG_EINVAL)
  let pipe = findPipe(p.id)
  if pipe == nil:
    return cint(NNG_ECLOSED)
  try:
    let str = pipe.getString($name)
    szp[] = csize_t(str.len)
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_pipe_get_scheme*(
    p: NngPipeObj, schemep: ptr cstring
): cint {.exportc, dynlib, cdecl.} =
  if schemep == nil:
    return cint(NNG_EINVAL)
  let pipe = findPipe(p.id)
  if pipe == nil:
    return cint(NNG_ECLOSED)
  let sc = pipe.scheme()
  schemep[] = cstring(sc)
  return 0

proc nng_pipe_self_addr*(
    p: NngPipeObj, sa: ptr NngSockaddr
): cint {.exportc, dynlib, cdecl.} =
  if sa == nil:
    return cint(NNG_EINVAL)
  let pipe = findPipe(p.id)
  if pipe == nil:
    return cint(NNG_ENOENT)
  zeroMem(sa, sizeof(NngSockaddr))
  sa.sa_family = 1 # NNG_AF_INPROC
  let addrStr = pipe.selfAddr()
  if addrStr.len > 0:
    copyMem(addr sa.sa_pad[0], cstring(addrStr), min(addrStr.len, 127))
  return 0

proc nng_pipe_peer_addr*(
    p: NngPipeObj, sa: ptr NngSockaddr
): cint {.exportc, dynlib, cdecl.} =
  if sa == nil:
    return cint(NNG_EINVAL)
  let pipe = findPipe(p.id)
  if pipe == nil:
    return cint(NNG_ENOENT)
  zeroMem(sa, sizeof(NngSockaddr))
  sa.sa_family = 1 # NNG_AF_INPROC
  let addrStr = pipe.peerAddr()
  if addrStr.len > 0:
    copyMem(addr sa.sa_pad[0], cstring(addrStr), min(addrStr.len, 127))
  return 0

proc nng_pipe_peer_cert*(
    p: NngPipeObj, certp: ptr ptr NngTlsCertObj
): cint {.exportc, dynlib, cdecl.} =
  if certp == nil:
    return cint(NNG_EINVAL)
  certp[] = nil
  return cint(NNG_ENOTSUP)
