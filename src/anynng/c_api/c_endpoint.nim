# ----------------------------------------------------------------------
# Dialing & Listening
# ----------------------------------------------------------------------

proc regDialer(d: Dialer): NngDialerObj =
  let id = gNextDialerId
  inc gNextDialerId
  gDialers[id] = d
  result.id = id

proc regListener(l: Listener): NngListenerObj =
  let id = gNextListenerId
  inc gNextListenerId
  gListeners[id] = l
  result.id = id

proc nng_dialer_create*(
    dp: ptr NngDialerObj, s: NngSocketObj, url: cstring
): cint {.exportc, dynlib, cdecl.} =
  if s.id notin gSockets or dp == nil or url == nil:
    return cint(NNG_EINVAL)
  let sock = gSockets[s.id]
  try:
    let d = sock.newDialer($url)
    dp[] = regDialer(d)
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_dialer_start*(d: NngDialerObj, flags: cint): cint {.exportc, dynlib, cdecl.} =
  if d.id notin gDialers:
    return cint(NNG_ECLOSED)
  let dialer = gDialers[d.id]
  try:
    dialer.start()
    return 0
  except CatchableError as e:
    if (flags and 2) != 0: # NNG_FLAG_NONBLOCK
      return 0
    return toCError(e)

proc nng_dialer_close*(d: NngDialerObj): cint {.exportc, dynlib, cdecl.} =
  if d.id notin gDialers:
    return cint(NNG_ECLOSED)
  let dialer = gDialers[d.id]
  gDialers.del(d.id)
  try:
    dialer.close()
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_dialer_id*(d: NngDialerObj): cint {.exportc, dynlib, cdecl.} =
  return cint(d.id)

proc nng_listener_create*(
    lp: ptr NngListenerObj, s: NngSocketObj, url: cstring
): cint {.exportc, dynlib, cdecl.} =
  if s.id notin gSockets or lp == nil or url == nil:
    return cint(NNG_EINVAL)
  let sock = gSockets[s.id]
  try:
    let l = sock.newListener($url)
    lp[] = regListener(l)
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_listener_start*(
    l: NngListenerObj, flags: cint
): cint {.exportc, dynlib, cdecl.} =
  if l.id notin gListeners:
    return cint(NNG_ECLOSED)
  let listener = gListeners[l.id]
  try:
    listener.start()
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_listener_close*(l: NngListenerObj): cint {.exportc, dynlib, cdecl.} =
  if l.id notin gListeners:
    return cint(NNG_ECLOSED)
  let listener = gListeners[l.id]
  gListeners.del(l.id)
  try:
    listener.close()
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_listener_id*(l: NngListenerObj): cint {.exportc, dynlib, cdecl.} =
  return cint(l.id)

proc nng_dial*(
    s: NngSocketObj, url: cstring, dp: ptr NngDialerObj, flags: cint
): cint {.exportc, dynlib, cdecl.} =
  if s.id notin gSockets or url == nil:
    return cint(NNG_EINVAL)
  let sock = gSockets[s.id]
  try:
    let d = sock.newDialer($url)
    if dp != nil:
      dp[] = regDialer(d)
    try:
      d.start()
    except CatchableError as e:
      if (flags and 2) != 0: # NNG_FLAG_NONBLOCK
        return 0
      raise e
    return 0
  except CatchableError as e:
    return toCError(e)

proc nng_listen*(
    s: NngSocketObj, url: cstring, lp: ptr NngListenerObj, flags: cint
): cint {.exportc, dynlib, cdecl.} =
  if s.id notin gSockets or url == nil:
    return cint(NNG_EINVAL)
  let sock = gSockets[s.id]
  try:
    let l = sock.listen($url)
    if lp != nil:
      lp[] = regListener(l)
    return 0
  except CatchableError as e:
    return toCError(e)
