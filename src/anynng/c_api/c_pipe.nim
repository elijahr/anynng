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
