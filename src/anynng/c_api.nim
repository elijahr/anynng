# c_api.nim - Full C-ABI Export Layer for anynng
# Implements <nng/nng.h> C ABI enabling unmodified upstream nanomsg/nng v1.12.4 test suites
# Licensed under MIT License.

when not defined(js):
  import std/[tables, sets, monotimes, os, random, asyncdispatch, posix, deques]
  import ../anynng
  import ./core/[types, options, tls]
  import ./core/protocols/pubsub0

  type
    NngSocketObj* = object
      id*: uint32

    NngDialerObj* = object
      id*: uint32

    NngListenerObj* = object
      id*: uint32

    NngCtxObj* = object
      id*: uint32

    NngPipeObj* = object
      id*: uint32

    NngMsgObj* = object
      header*: seq[byte]
      body*: seq[byte]
      pipeId*: PipeId

    NngAioObj* = object
      cb*: proc(arg: pointer) {.cdecl.}
      arg*: pointer
      msg*: ptr NngMsgObj
      err*: cint
      timeout*: int64
      finished*: bool
      stopped*: bool
      sockId*: uint32
      ctxId*: uint32
      startTime*: int64

    NngPipeNotifyCbCdecl* = proc(pipe: NngPipeObj, ev: cint, arg: pointer) {.cdecl.}

    PipeNotifyWrapper = ref object
      cb: NngPipeNotifyCbCdecl
      arg: pointer

    NngMtxObj* = object
      m*: ptr Pthread_mutex

    NngCvObj* = object
      c*: ptr Pthread_cond
      mtx*: ptr Pthread_mutex

    NngThreadObj = object
      fn: proc(arg: pointer) {.cdecl.}
      arg: pointer
      thr: Thread[pointer]

  var gHandleMtx: ptr Pthread_mutex = nil

  proc getHandleMtx(): ptr Pthread_mutex =
    if gHandleMtx == nil:
      gHandleMtx = cast[ptr Pthread_mutex](allocShared0(128))
      discard pthread_mutex_init(gHandleMtx, nil)
    gHandleMtx

  template withHandleLock(body: untyped) =
    let hm = getHandleMtx()
    discard pthread_mutex_lock(hm)
    try:
      body
    finally:
      discard pthread_mutex_unlock(hm)

  var
    gNextSockId: uint32 = 1
    gSockets = initTable[uint32, Socket]()

    gNextDialerId: uint32 = 1
    gDialers = initTable[uint32, Dialer]()

    gNextListenerId: uint32 = 1
    gListeners = initTable[uint32, Listener]()

    gNextCtxId: uint32 = 1
    gContexts = initTable[uint32, Context]()
    gClosedCtxs = initHashSet[uint32]()

    gPipes = initTable[uint32, Pipe]()

    gTlsConfigs = initTable[uint32, TlsConfig]()
    gNextTlsCfgId: uint32 = 1

    gPipeCallbacks = initTable[uint32, seq[PipeNotifyWrapper]]()
    gPendingSendAios: seq[ptr NngAioObj] = @[]

  proc nng_aio_finish*(aio: ptr NngAioObj, err: cint) {.exportc, dynlib, cdecl.}
  proc nng_sendmsg*(
    s: NngSocketObj, msg: ptr NngMsgObj, flags: cint
  ): cint {.exportc, dynlib, cdecl.}

  proc nng_ctx_sendmsg*(
    c: NngCtxObj, msg: ptr NngMsgObj, flags: cint
  ): cint {.exportc, dynlib, cdecl.}

  proc hasReqInFlight*(s: Socket, ctxId: uint32): bool =
    if ctxId in s.req0Eng.ctxToReqId:
      return true
    for a in gPendingSendAios:
      if a.sockId == s.id and a.ctxId == ctxId:
        return true
    return false

  proc tickSockets*() =
    let now = currentNowMs()
    for s in gSockets.values:
      if not s.closed and s.engineKind == engReq0 and s.req0Eng.pending.len > 0:
        let actions = s.stepEngine(EngineEvent(kind: evTimerTick, nowMs: now))
        s.executeActions(actions)
        s.updateSendFd()

  proc servicePendingSends*() =
    tickSockets()
    var i = 0
    let now = getMonoTime().ticks div 1_000_000
    while i < gPendingSendAios.len:
      let a = gPendingSendAios[i]
      if a.finished or a.stopped:
        gPendingSendAios.delete(i)
        continue
      if a.sockId notin gSockets or gSockets[a.sockId].closed:
        gPendingSendAios.delete(i)
        nng_aio_finish(a, cint(NNG_ECLOSED))
        continue
      if a.ctxId != 0 and (
        a.ctxId in gClosedCtxs or a.ctxId notin gContexts or gContexts[a.ctxId].closed
      ):
        gPendingSendAios.delete(i)
        nng_aio_finish(a, cint(NNG_ECLOSED))
        continue
      let s = gSockets[a.sockId]
      let maxInFlight = if s.pipes.len > 0: s.pipes.len else: 1
      if s.engineKind in {engRep0, engReq0} and s.inFlight >= maxInFlight:
        inc i
        continue
      if s.pipes.len > 0:
        gPendingSendAios.delete(i)
        if a.ctxId != 0:
          let res = nng_ctx_sendmsg(NngCtxObj(id: a.ctxId), a.msg, 0)
          a.msg = nil
          nng_aio_finish(a, res)
        else:
          let res = nng_sendmsg(NngSocketObj(id: a.sockId), a.msg, 0)
          a.msg = nil
          nng_aio_finish(a, res)
        continue
      if a.timeout >= 0 and (now - a.startTime) >= a.timeout:
        gPendingSendAios.delete(i)
        nng_aio_finish(a, cint(NNG_ETIMEDOUT))
        continue
      inc i

  proc closePendingSendsForSocket*(sockId: uint32) =
    var i = 0
    while i < gPendingSendAios.len:
      let a = gPendingSendAios[i]
      if a.sockId == sockId:
        gPendingSendAios.delete(i)
        if not a.finished:
          nng_aio_finish(a, cint(NNG_ECLOSED))
      else:
        inc i

  proc closePendingSendsForCtx*(ctxId: uint32) =
    var i = 0
    while i < gPendingSendAios.len:
      let a = gPendingSendAios[i]
      if a.ctxId == ctxId:
        gPendingSendAios.delete(i)
        if not a.finished:
          nng_aio_finish(a, cint(NNG_ECLOSED))
      else:
        inc i

  gOnPipeHook = proc() =
    servicePendingSends()

  gPendingAioCountProc = proc(sockId: uint32): int =
    for a in gPendingSendAios:
      if a.sockId == sockId:
        inc result

  # Helper: translate NngError / exceptions to C error codes
  proc toCError(e: ref Exception): cint =
    if e of NngError:
      return cint((ref NngError)(e).errCode)
    elif e of ValueError:
      return cint(NNG_EINVAL)
    return cint(NNG_EINTERNAL)

  # ----------------------------------------------------------------------
  # Library Lifecycle & Utilities
  # ----------------------------------------------------------------------

  proc nng_version*(): cstring {.exportc, dynlib, cdecl.} =
    return "1.12.4-anynng"

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
    sleep(int(ms))

  proc nng_random*(): uint32 {.exportc, dynlib, cdecl.} =
    return uint32(rand(high(int32)))

  proc nng_log_set_logger*(fn: pointer) {.exportc, dynlib, cdecl.} =
    discard

  proc nng_log_set_level*(level: cint) {.exportc, dynlib, cdecl.} =
    discard

  # ----------------------------------------------------------------------
  # Sockets & Protocol Openers
  # ----------------------------------------------------------------------

  proc regSocket(s: Socket): NngSocketObj =
    let id = gNextSockId
    inc gNextSockId
    s.id = id
    gSockets[id] = s
    result.id = id

  proc nng_close*(s: NngSocketObj): cint {.exportc, dynlib, cdecl.} =
    if s.id notin gSockets:
      return cint(NNG_ECLOSED)
    let sock = gSockets[s.id]
    closePendingSendsForSocket(s.id)
    gSockets.del(s.id)
    gPipeCallbacks.del(s.id)
    var ctxsToDel: seq[uint32] = @[]
    for cid, ctx in gContexts:
      if ctx.socket == sock:
        ctxsToDel.add(cid)
    for cid in ctxsToDel:
      closePendingSendsForCtx(cid)
      gClosedCtxs.incl(cid)
      gContexts.del(cid)
    try:
      sock.close()
      return 0
    except CatchableError as e:
      return toCError(e)

  proc nng_socket_id*(s: NngSocketObj): cint {.exportc, dynlib, cdecl.} =
    return cint(s.id)

  proc nng_pair0_open*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
    if s == nil:
      return cint(NNG_EINVAL)
    s[] = regSocket(openPair0())
    return 0

  proc nng_pair0_open_raw*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
    if s == nil:
      return cint(NNG_EINVAL)
    s[] = regSocket(openPair0(raw = true))
    return 0

  proc nng_pair1_open*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
    if s == nil:
      return cint(NNG_EINVAL)
    s[] = regSocket(openPair1())
    return 0

  proc nng_pair1_open_raw*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
    if s == nil:
      return cint(NNG_EINVAL)
    s[] = regSocket(openPair1(raw = true))
    return 0

  proc nng_pair1_open_poly*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
    if s == nil:
      return cint(NNG_EINVAL)
    s[] = regSocket(openPair1(polyamorous = true))
    return 0

  proc nng_pub0_open*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
    if s == nil:
      return cint(NNG_EINVAL)
    s[] = regSocket(openPub0())
    return 0

  proc nng_pub0_open_raw*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
    if s == nil:
      return cint(NNG_EINVAL)
    s[] = regSocket(openPub0(raw = true))
    return 0

  proc nng_sub0_open*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
    if s == nil:
      return cint(NNG_EINVAL)
    s[] = regSocket(openSub0())
    return 0

  proc nng_sub0_open_raw*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
    if s == nil:
      return cint(NNG_EINVAL)
    s[] = regSocket(openSub0(raw = true))
    return 0

  proc nng_req0_open*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
    if s == nil:
      return cint(NNG_EINVAL)
    s[] = regSocket(openReq0())
    return 0

  proc nng_req0_open_raw*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
    if s == nil:
      return cint(NNG_EINVAL)
    s[] = regSocket(openReq0(raw = true))
    return 0

  proc nng_rep0_open*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
    if s == nil:
      return cint(NNG_EINVAL)
    s[] = regSocket(openRep0())
    return 0

  proc nng_rep0_open_raw*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
    if s == nil:
      return cint(NNG_EINVAL)
    s[] = regSocket(openRep0(raw = true))
    return 0

  proc nng_push0_open*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
    if s == nil:
      return cint(NNG_EINVAL)
    s[] = regSocket(openPush0())
    return 0

  proc nng_push0_open_raw*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
    if s == nil:
      return cint(NNG_EINVAL)
    s[] = regSocket(openPush0(raw = true))
    return 0

  proc nng_pull0_open*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
    if s == nil:
      return cint(NNG_EINVAL)
    s[] = regSocket(openPull0())
    return 0

  proc nng_pull0_open_raw*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
    if s == nil:
      return cint(NNG_EINVAL)
    s[] = regSocket(openPull0(raw = true))
    return 0

  proc nng_surveyor0_open*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
    if s == nil:
      return cint(NNG_EINVAL)
    s[] = regSocket(openSurveyor0())
    return 0

  proc nng_surveyor0_open_raw*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
    if s == nil:
      return cint(NNG_EINVAL)
    s[] = regSocket(openSurveyor0(raw = true))
    return 0

  proc nng_respondent0_open*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
    if s == nil:
      return cint(NNG_EINVAL)
    s[] = regSocket(openRespondent0())
    return 0

  proc nng_respondent0_open_raw*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
    if s == nil:
      return cint(NNG_EINVAL)
    s[] = regSocket(openRespondent0(raw = true))
    return 0

  proc nng_bus0_open*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
    if s == nil:
      return cint(NNG_EINVAL)
    s[] = regSocket(openBus0())
    return 0

  proc nng_bus0_open_raw*(s: ptr NngSocketObj): cint {.exportc, dynlib, cdecl.} =
    if s == nil:
      return cint(NNG_EINVAL)
    s[] = regSocket(openBus0(raw = true))
    return 0

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

  proc nng_dialer_start*(
      d: NngDialerObj, flags: cint
  ): cint {.exportc, dynlib, cdecl.} =
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

  # ----------------------------------------------------------------------
  # Message Objects (nng_msg)
  # ----------------------------------------------------------------------

  proc nng_msg_alloc*(
      msgp: ptr ptr NngMsgObj, sz: csize_t
  ): cint {.exportc, dynlib, cdecl.} =
    if msgp == nil:
      return cint(NNG_EINVAL)
    let m = createShared(NngMsgObj)
    m.header = @[]
    m.body = newSeq[byte](sz)
    m.pipeId = 0
    msgp[] = m
    return 0

  proc nng_msg_free*(msg: ptr NngMsgObj) {.exportc, dynlib, cdecl.} =
    if msg != nil:
      msg.header = @[]
      msg.body = @[]
      freeShared(msg)

  proc nng_msg_body*(msg: ptr NngMsgObj): pointer {.exportc, dynlib, cdecl.} =
    if msg == nil or msg.body.len == 0:
      return nil
    return addr msg.body[0]

  proc nng_msg_len*(msg: ptr NngMsgObj): csize_t {.exportc, dynlib, cdecl.} =
    if msg == nil:
      return 0
    return csize_t(msg.body.len)

  proc nng_msg_append*(
      msg: ptr NngMsgObj, data: pointer, sz: csize_t
  ): cint {.exportc, dynlib, cdecl.} =
    if msg == nil:
      return cint(NNG_EINVAL)
    if sz > 0 and data != nil:
      let oldLen = msg.body.len
      msg.body.setLen(oldLen + int(sz))
      copyMem(addr msg.body[oldLen], data, int(sz))
    return 0

  proc nng_msg_insert*(
      msg: ptr NngMsgObj, data: pointer, sz: csize_t
  ): cint {.exportc, dynlib, cdecl.} =
    if msg == nil:
      return cint(NNG_EINVAL)
    if sz > 0 and data != nil:
      var newBody = newSeq[byte](msg.body.len + int(sz))
      copyMem(addr newBody[0], data, int(sz))
      if msg.body.len > 0:
        copyMem(addr newBody[int(sz)], addr msg.body[0], msg.body.len)
      msg.body = newBody
    return 0

  proc nng_msg_trim*(msg: ptr NngMsgObj, sz: csize_t): cint {.exportc, dynlib, cdecl.} =
    if msg == nil or int(sz) > msg.body.len:
      return cint(NNG_EINVAL)
    let s = int(sz)
    if s > 0:
      msg.body = msg.body[s .. ^1]
    return 0

  proc nng_msg_chop*(msg: ptr NngMsgObj, sz: csize_t): cint {.exportc, dynlib, cdecl.} =
    if msg == nil or int(sz) > msg.body.len:
      return cint(NNG_EINVAL)
    msg.body.setLen(msg.body.len - int(sz))
    return 0

  proc nng_msg_clear*(msg: ptr NngMsgObj) {.exportc, dynlib, cdecl.} =
    if msg != nil:
      msg.body.setLen(0)

  proc nng_msg_header*(msg: ptr NngMsgObj): pointer {.exportc, dynlib, cdecl.} =
    if msg == nil or msg.header.len == 0:
      return nil
    return addr msg.header[0]

  proc nng_msg_header_len*(msg: ptr NngMsgObj): csize_t {.exportc, dynlib, cdecl.} =
    if msg == nil:
      return 0
    return csize_t(msg.header.len)

  proc nng_msg_header_append*(
      msg: ptr NngMsgObj, data: pointer, sz: csize_t
  ): cint {.exportc, dynlib, cdecl.} =
    if msg == nil:
      return cint(NNG_EINVAL)
    if sz > 0 and data != nil:
      let oldLen = msg.header.len
      msg.header.setLen(oldLen + int(sz))
      copyMem(addr msg.header[oldLen], data, int(sz))
    return 0

  proc nng_msg_header_insert*(
      msg: ptr NngMsgObj, data: pointer, sz: csize_t
  ): cint {.exportc, dynlib, cdecl.} =
    if msg == nil:
      return cint(NNG_EINVAL)
    if sz > 0 and data != nil:
      var newHdr = newSeq[byte](msg.header.len + int(sz))
      copyMem(addr newHdr[0], data, int(sz))
      if msg.header.len > 0:
        copyMem(addr newHdr[int(sz)], addr msg.header[0], msg.header.len)
      msg.header = newHdr
    return 0

  proc nng_msg_header_trim*(
      msg: ptr NngMsgObj, sz: csize_t
  ): cint {.exportc, dynlib, cdecl.} =
    if msg == nil or int(sz) > msg.header.len:
      return cint(NNG_EINVAL)
    let s = int(sz)
    if s > 0:
      msg.header = msg.header[s .. ^1]
    return 0

  proc nng_msg_header_chop*(
      msg: ptr NngMsgObj, sz: csize_t
  ): cint {.exportc, dynlib, cdecl.} =
    if msg == nil or int(sz) > msg.header.len:
      return cint(NNG_EINVAL)
    msg.header.setLen(msg.header.len - int(sz))
    return 0

  proc nng_msg_header_clear*(msg: ptr NngMsgObj) {.exportc, dynlib, cdecl.} =
    if msg != nil:
      msg.header.setLen(0)

  proc nng_msg_dup*(
      dup: ptr ptr NngMsgObj, src: ptr NngMsgObj
  ): cint {.exportc, dynlib, cdecl.} =
    if dup == nil or src == nil:
      return cint(NNG_EINVAL)
    let m = createShared(NngMsgObj)
    m.header = src.header
    m.body = src.body
    m.pipeId = src.pipeId
    dup[] = m
    return 0

  proc nng_msg_append_u32*(
      msg: ptr NngMsgObj, val: uint32
  ): cint {.exportc, dynlib, cdecl.} =
    if msg == nil:
      return cint(NNG_EINVAL)
    var beVal = [
      byte(val shr 24 and 0xFF),
      byte(val shr 16 and 0xFF),
      byte(val shr 8 and 0xFF),
      byte(val and 0xFF),
    ]
    return nng_msg_append(msg, addr beVal[0], 4)

  proc nng_msg_trim_u32*(
      msg: ptr NngMsgObj, valp: ptr uint32
  ): cint {.exportc, dynlib, cdecl.} =
    if msg == nil or msg.body.len < 4:
      return cint(NNG_EINVAL)
    if valp != nil:
      valp[] =
        (uint32(msg.body[0]) shl 24) or (uint32(msg.body[1]) shl 16) or
        (uint32(msg.body[2]) shl 8) or uint32(msg.body[3])
    return nng_msg_trim(msg, 4)

  proc nng_msg_header_append_u32*(
      msg: ptr NngMsgObj, val: uint32
  ): cint {.exportc, dynlib, cdecl.} =
    if msg == nil:
      return cint(NNG_EINVAL)
    var beVal = [
      byte(val shr 24 and 0xFF),
      byte(val shr 16 and 0xFF),
      byte(val shr 8 and 0xFF),
      byte(val and 0xFF),
    ]
    return nng_msg_header_append(msg, addr beVal[0], 4)

  proc nng_msg_header_chop_u32*(
      msg: ptr NngMsgObj, valp: ptr uint32
  ): cint {.exportc, dynlib, cdecl.} =
    if msg == nil or msg.header.len < 4:
      return cint(NNG_EINVAL)
    let idx = msg.header.len - 4
    if valp != nil:
      valp[] =
        (uint32(msg.header[idx]) shl 24) or (uint32(msg.header[idx + 1]) shl 16) or
        (uint32(msg.header[idx + 2]) shl 8) or uint32(msg.header[idx + 3])
    return nng_msg_header_chop(msg, 4)

  proc nng_msg_header_trim_u32*(
      msg: ptr NngMsgObj, valp: ptr uint32
  ): cint {.exportc, dynlib, cdecl.} =
    if msg == nil or msg.header.len < 4:
      return cint(NNG_EINVAL)
    if valp != nil:
      valp[] =
        (uint32(msg.header[0]) shl 24) or (uint32(msg.header[0 + 1]) shl 16) or
        (uint32(msg.header[0 + 2]) shl 8) or uint32(msg.header[0 + 3])
    return nng_msg_header_trim(msg, 4)

  proc nng_msg_get_pipe*(msg: ptr NngMsgObj): NngPipeObj {.exportc, dynlib, cdecl.} =
    if msg == nil:
      return NngPipeObj(id: 0)
    return NngPipeObj(id: uint32(msg.pipeId))

  proc nng_msg_set_pipe*(msg: ptr NngMsgObj, p: NngPipeObj) {.exportc, dynlib, cdecl.} =
    if msg != nil:
      msg.pipeId = PipeId(p.id)

  # ----------------------------------------------------------------------
  # Send / Recv Sockets
  # ----------------------------------------------------------------------

  proc nng_send*(
      s: NngSocketObj, data: pointer, sz: csize_t, flags: cint
  ): cint {.exportc, dynlib, cdecl.} =
    if s.id notin gSockets:
      return cint(NNG_ECLOSED)
    let sock = gSockets[s.id]
    if sock.engineKind in {engSub0, engPull0}:
      return cint(NNG_ENOTSUP)
    var buf = newSeq[byte](sz)
    if sz > 0 and data != nil:
      copyMem(addr buf[0], data, int(sz))
    try:
      sock.send(buf)
      return 0
    except CatchableError as e:
      return toCError(e)

  proc nng_recv*(
      s: NngSocketObj, data: pointer, szp: ptr csize_t, flags: cint
  ): cint {.exportc, dynlib, cdecl.} =
    if s.id notin gSockets:
      return cint(NNG_EINVAL)
    let sock = gSockets[s.id]
    if sock.engineKind in {engPub0, engPush0}:
      return cint(NNG_ENOTSUP)
    if szp == nil:
      return cint(NNG_EINVAL)
    let nonblock = (flags and 2) != 0 # NNG_FLAG_NONBLOCK
    let recvTimeout = sock.getMs(NNG_OPT_RECVTIMEO)

    if sock.lastErr != 0:
      let err = sock.lastErr
      sock.lastErr = 0
      return err
    let start = getMonoTime().ticks div 1_000_000
    if sock.engineKind == engSurveyor0 and not sock.raw:
      if sock.recvQueue.len == 0 and not sock.inSurvey(0):
        return cint(NNG_ESTATE)
    if sock.engineKind == engReq0 and not sock.raw:
      if sock.recvQueue.len == 0 and not hasReqInFlight(sock, 0):
        return cint(NNG_ESTATE)
    while sock.recvQueue.len == 0:
      tickSockets()
      servicePendingSends()
      if sock.lastErr != 0:
        let err = sock.lastErr
        sock.lastErr = 0
        return err
      if nonblock:
        return cint(NNG_EAGAIN)
      if sock.engineKind == engSurveyor0 and not sock.raw:
        if not sock.inSurvey(0):
          return cint(NNG_ETIMEDOUT)
      if recvTimeout >= 0:
        let now = getMonoTime().ticks div 1_000_000
        if (now - start) >= recvTimeout:
          return cint(NNG_ETIMEDOUT)
      try:
        poll(10)
      except CatchableError:
        sleep(5)

    if sock.recvQueue.len == 0:
      return cint(NNG_ETIMEDOUT)

    let msg = sock.recvMsg()
    let copyLen = min(int(szp[]), msg.body.len)
    if copyLen > 0 and data != nil:
      copyMem(data, unsafeAddr msg.body[0], copyLen)
    szp[] = csize_t(msg.body.len)
    return 0

  proc nng_sendmsg*(
      s: NngSocketObj, msg: ptr NngMsgObj, flags: cint
  ): cint {.exportc, dynlib, cdecl.} =
    if s.id notin gSockets or msg == nil:
      return cint(NNG_EINVAL)
    let sock = gSockets[s.id]
    if sock.engineKind in {engSub0, engPull0}:
      return cint(NNG_ENOTSUP)
    var spMsg = SpMsg(header: msg.header, body: msg.body, pipeId: msg.pipeId)
    try:
      sock.sendMsg(spMsg)
      nng_msg_free(msg)
      return 0
    except CatchableError as e:
      return toCError(e)

  proc nng_recvmsg*(
      s: NngSocketObj, msgp: ptr ptr NngMsgObj, flags: cint
  ): cint {.exportc, dynlib, cdecl.} =
    if s.id notin gSockets:
      return cint(NNG_EINVAL)
    let sock = gSockets[s.id]
    if sock.engineKind in {engPub0, engPush0}:
      return cint(NNG_ENOTSUP)
    if msgp == nil:
      return cint(NNG_EINVAL)
    let nonblock = (flags and 2) != 0
    let recvTimeout = sock.getMs(NNG_OPT_RECVTIMEO)

    if sock.lastErr != 0:
      let err = sock.lastErr
      sock.lastErr = 0
      return err
    let start = getMonoTime().ticks div 1_000_000
    if sock.engineKind == engSurveyor0 and not sock.raw:
      if sock.recvQueue.len == 0 and not sock.inSurvey(0):
        return cint(NNG_ESTATE)
    if sock.engineKind == engReq0 and not sock.raw:
      if sock.recvQueue.len == 0 and not hasReqInFlight(sock, 0):
        return cint(NNG_ESTATE)
    while sock.recvQueue.len == 0:
      tickSockets()
      servicePendingSends()
      if sock.lastErr != 0:
        let err = sock.lastErr
        sock.lastErr = 0
        return err
      if nonblock:
        return cint(NNG_EAGAIN)
      if sock.engineKind == engSurveyor0 and not sock.raw:
        if not sock.inSurvey(0):
          return cint(NNG_ETIMEDOUT)
      if recvTimeout >= 0:
        let now = getMonoTime().ticks div 1_000_000
        if (now - start) >= recvTimeout:
          return cint(NNG_ETIMEDOUT)
      try:
        poll(10)
      except CatchableError:
        sleep(5)

    if sock.recvQueue.len == 0:
      return cint(NNG_ETIMEDOUT)

    try:
      let spMsg = sock.recvMsg()
      let m = createShared(NngMsgObj)
      m.header = spMsg.header
      m.body = spMsg.body
      m.pipeId = spMsg.pipeId
      msgp[] = m
      return 0
    except CatchableError as e:
      return toCError(e)

  # ----------------------------------------------------------------------
  # Contexts (nng_ctx)
  # ----------------------------------------------------------------------

  proc regCtx(ctx: Context): NngCtxObj =
    let id = gNextCtxId
    inc gNextCtxId
    gContexts[id] = ctx
    gClosedCtxs.excl(id)
    result.id = id

  proc nng_ctx_open*(
      cp: ptr NngCtxObj, s: NngSocketObj
  ): cint {.exportc, dynlib, cdecl.} =
    if s.id notin gSockets or cp == nil:
      return cint(NNG_EINVAL)
    let sock = gSockets[s.id]
    try:
      let ctx = sock.openContext()
      cp[] = regCtx(ctx)
      return 0
    except CatchableError as e:
      return toCError(e)

  proc nng_ctx_close*(c: NngCtxObj): cint {.exportc, dynlib, cdecl.} =
    if c.id in gClosedCtxs:
      return cint(NNG_ECLOSED)
    if c.id notin gContexts:
      return cint(NNG_ECLOSED)
    let ctx = gContexts[c.id]
    closePendingSendsForCtx(c.id)
    gContexts.del(c.id)
    gClosedCtxs.incl(c.id)
    try:
      ctx.close()
      return 0
    except CatchableError as e:
      return toCError(e)

  proc nng_ctx_id*(c: NngCtxObj): cint {.exportc, dynlib, cdecl.} =
    return cint(c.id)

  proc nng_ctx_sendmsg*(
      c: NngCtxObj, msg: ptr NngMsgObj, flags: cint
  ): cint {.exportc, dynlib, cdecl.} =
    if c.id notin gContexts or msg == nil:
      return cint(NNG_EINVAL)
    let ctx = gContexts[c.id]
    if ctx.socket.engineKind in {engSub0, engPull0}:
      return cint(NNG_ENOTSUP)
    var spMsg = SpMsg(header: msg.header, body: msg.body, pipeId: msg.pipeId)
    try:
      ctx.sendMsg(spMsg)
      nng_msg_free(msg)
      return 0
    except CatchableError as e:
      return toCError(e)

  proc nng_ctx_recvmsg*(
      c: NngCtxObj, msgp: ptr ptr NngMsgObj, flags: cint
  ): cint {.exportc, dynlib, cdecl.} =
    if c.id notin gContexts or msgp == nil:
      return cint(NNG_EINVAL)
    let ctx = gContexts[c.id]
    if ctx.socket.engineKind in {engPub0, engPush0}:
      return cint(NNG_ENOTSUP)
    let nonblock = (flags and 2) != 0
    let recvTimeout = ctx.socket.getMs(NNG_OPT_RECVTIMEO)

    if ctx.lastErr != 0:
      let err = ctx.lastErr
      ctx.lastErr = 0
      return err

    # If context recvQueue is empty but socket has messages for Rep/Respondent, claim one
    if ctx.recvQueue.len == 0 and ctx.socket.recvQueue.len > 0 and
        ctx.socket.engineKind in {engRep0, engRespondent0}:
      let msg = ctx.socket.recvQueue.popFirst()
      if ctx.socket.engineKind == engRespondent0 and
          ctx.socket.respondent0Eng.reqStack.len > 0:
        ctx.socket.respondent0Eng.contexts[ctx.ctxId] =
          ctx.socket.respondent0Eng.reqStack.pop()
      elif ctx.socket.engineKind == engRep0 and ctx.socket.rep0Eng.reqStack.len > 0:
        ctx.socket.rep0Eng.contexts[ctx.ctxId] = ctx.socket.rep0Eng.reqStack.pop()
      ctx.recvQueue.addLast(msg)

    let start = getMonoTime().ticks div 1_000_000
    if ctx.socket.engineKind == engSurveyor0 and not ctx.socket.raw:
      if ctx.recvQueue.len == 0 and not ctx.socket.inSurvey(ctx.ctxId):
        return cint(NNG_ESTATE)
    if ctx.socket.engineKind == engReq0 and not ctx.socket.raw:
      if ctx.recvQueue.len == 0 and not hasReqInFlight(ctx.socket, c.id):
        return cint(NNG_ESTATE)
    while ctx.recvQueue.len == 0:
      tickSockets()
      servicePendingSends()
      if ctx.lastErr != 0:
        let err = ctx.lastErr
        ctx.lastErr = 0
        return err
      if nonblock:
        return cint(NNG_EAGAIN)
      if ctx.socket.engineKind == engSurveyor0 and not ctx.socket.raw:
        if not ctx.socket.inSurvey(ctx.ctxId):
          return cint(NNG_ETIMEDOUT)
      if recvTimeout >= 0:
        let now = getMonoTime().ticks div 1_000_000
        if (now - start) >= recvTimeout:
          return cint(NNG_ETIMEDOUT)
      try:
        poll(10)
      except CatchableError:
        sleep(5)
      if ctx.recvQueue.len == 0 and ctx.socket.recvQueue.len > 0 and
          ctx.socket.engineKind in {engRep0, engRespondent0}:
        let msg = ctx.socket.recvQueue.popFirst()
        if ctx.socket.engineKind == engRespondent0 and
            ctx.socket.respondent0Eng.reqStack.len > 0:
          ctx.socket.respondent0Eng.contexts[ctx.ctxId] =
            ctx.socket.respondent0Eng.reqStack.pop()
        elif ctx.socket.engineKind == engRep0 and ctx.socket.rep0Eng.reqStack.len > 0:
          ctx.socket.rep0Eng.contexts[ctx.ctxId] = ctx.socket.rep0Eng.reqStack.pop()
        ctx.recvQueue.addLast(msg)

    if ctx.recvQueue.len == 0:
      return cint(NNG_ETIMEDOUT)

    try:
      let spMsg = ctx.recvMsg()
      let m = createShared(NngMsgObj)
      m.header = spMsg.header
      m.body = spMsg.body
      m.pipeId = spMsg.pipeId
      msgp[] = m
      return 0
    except CatchableError as e:
      return toCError(e)

  # Context Options
  proc nng_ctx_get*(
      c: NngCtxObj, name: cstring, val: pointer, szp: ptr csize_t
  ): cint {.exportc, dynlib, cdecl.} =
    if c.id in gClosedCtxs:
      return cint(NNG_ECLOSED)
    if c.id notin gContexts or name == nil or szp == nil:
      return cint(NNG_EINVAL)
    let optName = $name
    if optName in [NNG_OPT_SUB_SUBSCRIBE, NNG_OPT_SUB_UNSUBSCRIBE]:
      return cint(NNG_EWRITEONLY)
    return cint(NNG_ENOTSUP)

  proc nng_ctx_get_int*(
      c: NngCtxObj, name: cstring, val: ptr cint
  ): cint {.exportc, dynlib, cdecl.} =
    if c.id in gClosedCtxs:
      return cint(NNG_ECLOSED)
    if c.id notin gContexts or name == nil or val == nil:
      return cint(NNG_EINVAL)
    let optName = $name
    if optName in [NNG_OPT_SENDFD, NNG_OPT_RECVFD]:
      return cint(NNG_ENOTSUP)
    let ctx = gContexts[c.id]
    try:
      val[] = cint(ctx.getInt(optName))
      return 0
    except CatchableError as e:
      return toCError(e)

  proc nng_ctx_set*(
      c: NngCtxObj, name: cstring, val: pointer, sz: csize_t
  ): cint {.exportc, dynlib, cdecl.} =
    if c.id in gClosedCtxs:
      return cint(NNG_ECLOSED)
    if c.id notin gContexts or name == nil:
      return cint(NNG_EINVAL)
    let ctx = gContexts[c.id]
    let optName = $name
    if optName == NNG_OPT_SUB_SUBSCRIBE:
      if ctx.socket.engineKind == engSub0:
        var b = newSeq[byte](sz)
        if sz > 0 and val != nil:
          copyMem(addr b[0], val, int(sz))
        try:
          ctx.subscribe(b)
          return 0
        except CatchableError as e:
          return toCError(e)
      return cint(NNG_ENOTSUP)
    elif optName == NNG_OPT_SUB_UNSUBSCRIBE:
      if ctx.socket.engineKind == engSub0:
        var b = newSeq[byte](sz)
        if sz > 0 and val != nil:
          copyMem(addr b[0], val, int(sz))
        try:
          ctx.unsubscribe(b)
          return 0
        except CatchableError as e:
          return toCError(e)
      return cint(NNG_ENOTSUP)
    try:
      if optName in ctx.options.values:
        let entry = ctx.options.values[optName]
        case entry.kind
        of optKindInt:
          if sz != csize_t(sizeof(cint)) or val == nil:
            return cint(NNG_EINVAL)
          let iv = cast[ptr cint](val)[]
          ctx.setOpt(optName, int(iv))
          return 0
        of optKindDuration:
          if sz != csize_t(sizeof(int32)) or val == nil:
            return cint(NNG_EINVAL)
          let mv = cast[ptr int32](val)[]
          ctx.setOpt(optName, int(mv))
          return 0
        else:
          return cint(NNG_ENOTSUP)
      else:
        return cint(NNG_ENOTSUP)
    except CatchableError as e:
      return toCError(e)

  # SUB0 Specific Convenience Wrappers
  proc nng_sub0_socket_subscribe*(
      s: NngSocketObj, buf: pointer, sz: csize_t
  ): cint {.exportc, dynlib, cdecl.} =
    if s.id notin gSockets:
      return cint(NNG_EINVAL)
    let sock = gSockets[s.id]
    if sock.engineKind != engSub0:
      return cint(NNG_ENOTSUP)
    var b = newSeq[byte](sz)
    if sz > 0 and buf != nil:
      copyMem(addr b[0], buf, int(sz))
    try:
      sock.sub0Eng.subscribe(b)
      return 0
    except CatchableError as e:
      return toCError(e)

  proc nng_sub0_socket_unsubscribe*(
      s: NngSocketObj, buf: pointer, sz: csize_t
  ): cint {.exportc, dynlib, cdecl.} =
    if s.id notin gSockets:
      return cint(NNG_EINVAL)
    let sock = gSockets[s.id]
    if sock.engineKind != engSub0:
      return cint(NNG_ENOTSUP)
    var b = newSeq[byte](sz)
    if sz > 0 and buf != nil:
      copyMem(addr b[0], buf, int(sz))
    try:
      sock.sub0Eng.unsubscribe(b)
      return 0
    except CatchableError as e:
      return toCError(e)

  proc nng_sub0_ctx_subscribe*(
      c: NngCtxObj, buf: pointer, sz: csize_t
  ): cint {.exportc, dynlib, cdecl.} =
    if c.id notin gContexts:
      return cint(NNG_EINVAL)
    let ctx = gContexts[c.id]
    if ctx.socket.engineKind != engSub0:
      return cint(NNG_ENOTSUP)
    var b = newSeq[byte](sz)
    if sz > 0 and buf != nil:
      copyMem(addr b[0], buf, int(sz))
    try:
      ctx.subscribe(b)
      return 0
    except CatchableError as e:
      return toCError(e)

  proc nng_sub0_ctx_unsubscribe*(
      c: NngCtxObj, buf: pointer, sz: csize_t
  ): cint {.exportc, dynlib, cdecl.} =
    if c.id notin gContexts:
      return cint(NNG_EINVAL)
    let ctx = gContexts[c.id]
    if ctx.socket.engineKind != engSub0:
      return cint(NNG_ENOTSUP)
    var b = newSeq[byte](sz)
    if sz > 0 and buf != nil:
      copyMem(addr b[0], buf, int(sz))
    try:
      ctx.unsubscribe(b)
      return 0
    except CatchableError as e:
      return toCError(e)

  # ----------------------------------------------------------------------
  # AIO (Asynchronous I/O)
  # ----------------------------------------------------------------------

  proc nng_aio_alloc*(
      aiop: ptr ptr NngAioObj, cb: proc(arg: pointer) {.cdecl.}, arg: pointer
  ): cint {.exportc, dynlib, cdecl.} =
    if aiop == nil:
      return cint(NNG_EINVAL)
    let aio = createShared(NngAioObj)
    aio.cb = cb
    aio.arg = arg
    aio.msg = nil
    aio.err = 0
    aio.timeout = -1
    aio.finished = false
    aiop[] = aio
    return 0

  proc nng_aio_free*(aio: ptr NngAioObj) {.exportc, dynlib, cdecl.} =
    if aio != nil:
      let idx = gPendingSendAios.find(aio)
      if idx >= 0:
        gPendingSendAios.delete(idx)
      if aio.msg != nil:
        nng_msg_free(aio.msg)
      freeShared(aio)

  proc nng_aio_stop*(aio: ptr NngAioObj) {.exportc, dynlib, cdecl.} =
    if aio != nil:
      let idx = gPendingSendAios.find(aio)
      if idx >= 0:
        gPendingSendAios.delete(idx)
      aio.stopped = true
      aio.err = cint(NNG_ECANCELED)
      aio.finished = true

  proc nng_aio_wait*(aio: ptr NngAioObj) {.exportc, dynlib, cdecl.} =
    if aio == nil:
      return
    let start = getMonoTime().ticks div 1_000_000
    while not aio.finished:
      servicePendingSends()
      if aio.finished:
        break
      if aio.timeout >= 0:
        let now = getMonoTime().ticks div 1_000_000
        if (now - start) >= aio.timeout:
          let idx = gPendingSendAios.find(aio)
          if idx >= 0:
            gPendingSendAios.delete(idx)
          aio.err = cint(NNG_ETIMEDOUT)
          aio.finished = true
          break
      for id, s in gSockets:
        if s.closed:
          continue
        if s.engineKind == engSurveyor0 and not s.raw:
          discard s.inSurvey(0)
          if s.pendingRecvCb != nil and not s.inSurvey(0):
            let cb = s.pendingRecvCb
            s.pendingRecvCb = nil
            cb(NNG_ETIMEDOUT, SpMsg())
        elif s.engineKind == engReq0 and not s.raw:
          let rcvTmo = s.getMs(NNG_OPT_RECVTIMEO)
          if rcvTmo >= 0 and s.pendingRecvCb != nil:
            let now = getMonoTime().ticks div 1_000_000
            if (now - start) >= rcvTmo:
              let cb = s.pendingRecvCb
              s.pendingRecvCb = nil
              cb(NNG_ETIMEDOUT, SpMsg())
              var j = 0
              while j < gPendingSendAios.len:
                let snd = gPendingSendAios[j]
                if snd.sockId == s.id and snd.ctxId == 0:
                  gPendingSendAios.delete(j)
                  nng_aio_finish(snd, cint(NNG_ECANCELED))
                else:
                  inc j
      for id, c in gContexts:
        if c.closed or c.socket.closed:
          continue
        if c.socket.engineKind == engSurveyor0 and not c.socket.raw:
          discard c.socket.inSurvey(c.ctxId)
          if c.pendingRecvCb != nil and not c.socket.inSurvey(c.ctxId):
            let cb = c.pendingRecvCb
            c.pendingRecvCb = nil
            cb(NNG_ETIMEDOUT, SpMsg())
        elif c.socket.engineKind == engReq0 and not c.socket.raw:
          let rcvTmo = c.socket.getMs(NNG_OPT_RECVTIMEO)
          if rcvTmo >= 0 and c.pendingRecvCb != nil:
            let now = getMonoTime().ticks div 1_000_000
            if (now - start) >= rcvTmo:
              let cb = c.pendingRecvCb
              c.pendingRecvCb = nil
              cb(NNG_ETIMEDOUT, SpMsg())
              var j = 0
              while j < gPendingSendAios.len:
                let snd = gPendingSendAios[j]
                if snd.sockId == c.socket.id and snd.ctxId == c.ctxId:
                  gPendingSendAios.delete(j)
                  nng_aio_finish(snd, cint(NNG_ECANCELED))
                else:
                  inc j
      try:
        poll(10)
      except CatchableError:
        sleep(5)

  proc nng_aio_cancel*(aio: ptr NngAioObj) {.exportc, dynlib, cdecl.} =
    if aio != nil:
      let idx = gPendingSendAios.find(aio)
      if idx >= 0:
        gPendingSendAios.delete(idx)
      aio.err = cint(NNG_ECANCELED)
      aio.finished = true

  proc nng_aio_abort*(aio: ptr NngAioObj, err: cint) {.exportc, dynlib, cdecl.} =
    if aio != nil:
      let idx = gPendingSendAios.find(aio)
      if idx >= 0:
        gPendingSendAios.delete(idx)
      aio.err = err
      aio.finished = true
      aio.stopped = true

  proc nng_aio_result*(aio: ptr NngAioObj): cint {.exportc, dynlib, cdecl.} =
    if aio == nil:
      return cint(NNG_EINVAL)
    return aio.err

  proc nng_aio_set_timeout*(aio: ptr NngAioObj, ms: int64) {.exportc, dynlib, cdecl.} =
    if aio != nil:
      aio.timeout = ms

  proc nng_aio_set_msg*(
      aio: ptr NngAioObj, msg: ptr NngMsgObj
  ) {.exportc, dynlib, cdecl.} =
    if aio != nil:
      aio.msg = msg

  proc nng_aio_get_msg*(aio: ptr NngAioObj): ptr NngMsgObj {.exportc, dynlib, cdecl.} =
    if aio == nil:
      return nil
    result = aio.msg

  proc nng_aio_finish*(aio: ptr NngAioObj, err: cint) {.exportc, dynlib, cdecl.} =
    if aio != nil:
      aio.err = err
      aio.finished = true
      if aio.cb != nil:
        aio.cb(aio.arg)

  proc nng_sleep_aio*(ms: int64, aio: ptr NngAioObj) {.exportc, dynlib, cdecl.} =
    if aio == nil:
      return
    sleep(int(ms))
    nng_aio_finish(aio, 0)

  proc nng_send_aio*(s: NngSocketObj, aio: ptr NngAioObj) {.exportc, dynlib, cdecl.} =
    if aio == nil:
      return
    if aio.stopped:
      nng_aio_finish(aio, cint(NNG_ECANCELED))
      return
    if s.id notin gSockets or aio.msg == nil:
      nng_aio_finish(aio, cint(NNG_EINVAL))
      return
    let sock = gSockets[s.id]
    if sock.closed:
      nng_aio_finish(aio, cint(NNG_ECLOSED))
      return
    if sock.engineKind in {engSub0, engPull0}:
      nng_aio_finish(aio, cint(NNG_ENOTSUP))
      return
    aio.sockId = s.id
    aio.ctxId = 0
    aio.startTime = getMonoTime().ticks div 1_000_000
    if sock.engineKind == engReq0:
      var i = 0
      while i < gPendingSendAios.len:
        let prev = gPendingSendAios[i]
        if prev.sockId == s.id and prev.ctxId == 0:
          gPendingSendAios.delete(i)
          nng_aio_finish(prev, cint(NNG_ECANCELED))
        else:
          inc i
    if sock.pipes.len == 0 and sock.engineKind in {
      engPush0, engPair0, engPair1, engReq0
    }:
      if aio.timeout == 0:
        nng_aio_finish(aio, cint(NNG_ETIMEDOUT))
        return
      aio.finished = false
      aio.err = 0
      gPendingSendAios.add(aio)
      return
    let res = nng_sendmsg(s, aio.msg, 0)
    aio.msg = nil
    nng_aio_finish(aio, res)

  proc nng_recv_aio*(s: NngSocketObj, aio: ptr NngAioObj) {.exportc, dynlib, cdecl.} =
    if aio != nil and aio.stopped:
      nng_aio_finish(aio, cint(NNG_ECANCELED))
      return
    if s.id notin gSockets or aio == nil:
      if aio != nil:
        nng_aio_finish(aio, cint(NNG_EINVAL))
      return
    let sock = gSockets[s.id]
    if sock.closed:
      nng_aio_finish(aio, cint(NNG_ECLOSED))
      return
    if sock.engineKind in {engPub0, engPush0}:
      nng_aio_finish(aio, cint(NNG_ENOTSUP))
      return
    if sock.pendingRecvCb != nil and sock.engineKind in {engRep0, engRespondent0}:
      nng_aio_finish(aio, cint(NNG_ESTATE))
      return
    if sock.engineKind == engSurveyor0 and not sock.raw and sock.recvQueue.len == 0 and
        not sock.inSurvey(0):
      nng_aio_finish(aio, cint(NNG_ESTATE))
      return
    if sock.engineKind == engReq0 and not sock.raw and sock.recvQueue.len == 0 and
        not hasReqInFlight(sock, 0):
      nng_aio_finish(aio, cint(NNG_ESTATE))
      return
    if sock.recvQueue.len > 0:
      var msg: ptr NngMsgObj = nil
      let res = nng_recvmsg(s, addr msg, 2)
      if res == 0:
        nng_aio_set_msg(aio, msg)
      nng_aio_finish(aio, res)
    else:
      aio.finished = false
      aio.err = 0
      sock.pendingRecvCb = proc(res: int, msg: SpMsg) =
        if res == 0:
          let m = createShared(NngMsgObj)
          m.header = msg.header
          m.body = msg.body
          m.pipeId = msg.pipeId
          nng_aio_set_msg(aio, m)
        nng_aio_finish(aio, cint(res))

  proc nng_ctx_send*(c: NngCtxObj, aio: ptr NngAioObj) {.exportc, dynlib, cdecl.} =
    if aio != nil and aio.stopped:
      nng_aio_finish(aio, cint(NNG_ECANCELED))
      return
    if c.id in gClosedCtxs:
      if aio != nil:
        nng_aio_finish(aio, cint(NNG_ECLOSED))
      return
    if c.id notin gContexts or aio == nil or aio.msg == nil:
      if aio != nil:
        nng_aio_finish(aio, cint(NNG_EINVAL))
      return
    let ctx = gContexts[c.id]
    let sock = ctx.socket
    if ctx.closed or sock.closed:
      nng_aio_finish(aio, cint(NNG_ECLOSED))
      return
    if sock.engineKind in {engSub0, engPull0}:
      nng_aio_finish(aio, cint(NNG_ENOTSUP))
      return
    if sock.engineKind == engRespondent0:
      if ctx.ctxId notin sock.respondent0Eng.contexts and
          (sock.respondent0Eng.reqStack.len == 0 or not sock.raw):
        nng_aio_finish(aio, cint(NNG_ESTATE))
        return
      if aio.timeout == 0:
        nng_aio_finish(aio, cint(NNG_ETIMEDOUT))
        return
    elif sock.engineKind == engRep0:
      if sock.inFlight > 0:
        if aio.timeout == 0:
          nng_aio_finish(aio, cint(NNG_ETIMEDOUT))
          return
        aio.finished = false
        aio.err = 0
        gPendingSendAios.add(aio)
        return
      inc sock.inFlight
    aio.sockId = sock.id
    aio.ctxId = c.id
    aio.startTime = getMonoTime().ticks div 1_000_000
    if sock.engineKind == engReq0:
      var i = 0
      while i < gPendingSendAios.len:
        let prev = gPendingSendAios[i]
        if prev.sockId == sock.id and prev.ctxId == c.id:
          gPendingSendAios.delete(i)
          nng_aio_finish(prev, cint(NNG_ECANCELED))
        else:
          inc i
    if sock.pipes.len == 0 and sock.engineKind in {
      engPush0, engPair0, engPair1, engReq0
    }:
      if aio.timeout == 0:
        nng_aio_finish(aio, cint(NNG_ETIMEDOUT))
        return
      aio.finished = false
      aio.err = 0
      gPendingSendAios.add(aio)
      return
    let res = nng_ctx_sendmsg(c, aio.msg, 0)
    aio.msg = nil
    nng_aio_finish(aio, res)

  proc nng_ctx_recv*(c: NngCtxObj, aio: ptr NngAioObj) {.exportc, dynlib, cdecl.} =
    if aio != nil and aio.stopped:
      nng_aio_finish(aio, cint(NNG_ECANCELED))
      return
    if c.id in gClosedCtxs:
      if aio != nil:
        nng_aio_finish(aio, cint(NNG_ECLOSED))
      return
    if c.id notin gContexts or aio == nil:
      if aio != nil:
        nng_aio_finish(aio, cint(NNG_EINVAL))
      return
    let ctx = gContexts[c.id]
    if ctx.closed or ctx.socket.closed:
      nng_aio_finish(aio, cint(NNG_ECLOSED))
      return
    if ctx.socket.engineKind in {engPub0, engPush0}:
      nng_aio_finish(aio, cint(NNG_ENOTSUP))
      return
    if ctx.pendingRecvCb != nil and ctx.socket.engineKind in {engRep0, engRespondent0}:
      nng_aio_finish(aio, cint(NNG_ESTATE))
      return
    if ctx.socket.engineKind == engSurveyor0 and not ctx.socket.raw and
        ctx.recvQueue.len == 0 and not ctx.socket.inSurvey(ctx.ctxId):
      nng_aio_finish(aio, cint(NNG_ESTATE))
      return
    if ctx.socket.engineKind == engReq0 and not ctx.socket.raw and ctx.recvQueue.len == 0 and
        not hasReqInFlight(ctx.socket, c.id):
      nng_aio_finish(aio, cint(NNG_ESTATE))
      return
    if ctx.recvQueue.len == 0 and ctx.socket.recvQueue.len > 0 and
        ctx.socket.engineKind in {engRep0, engRespondent0}:
      let msg = ctx.socket.recvQueue.popFirst()
      if ctx.socket.engineKind == engRespondent0 and
          ctx.socket.respondent0Eng.reqStack.len > 0:
        ctx.socket.respondent0Eng.contexts[ctx.ctxId] =
          ctx.socket.respondent0Eng.reqStack.pop()
      elif ctx.socket.engineKind == engRep0 and ctx.socket.rep0Eng.reqStack.len > 0:
        ctx.socket.rep0Eng.contexts[ctx.ctxId] = ctx.socket.rep0Eng.reqStack.pop()
      ctx.recvQueue.addLast(msg)
    if ctx.recvQueue.len > 0:
      var msg: ptr NngMsgObj = nil
      let res = nng_ctx_recvmsg(c, addr msg, 2)
      if res == 0:
        nng_aio_set_msg(aio, msg)
      nng_aio_finish(aio, res)
    else:
      aio.finished = false
      aio.err = 0
      aio.sockId = ctx.socket.id
      aio.ctxId = c.id
      aio.startTime = getMonoTime().ticks div 1_000_000
      ctx.pendingRecvCb = proc(res: int, msg: SpMsg) =
        if res == 0:
          let m = createShared(NngMsgObj)
          m.header = msg.header
          m.body = msg.body
          m.pipeId = msg.pipeId
          nng_aio_set_msg(aio, m)
        nng_aio_finish(aio, cint(res))

  # ----------------------------------------------------------------------
  # Pipes & Introspection
  # ----------------------------------------------------------------------

  proc nng_pipe_id*(p: NngPipeObj): cint {.exportc, dynlib, cdecl.} =
    return cint(p.id)

  proc nng_pipe_socket*(p: NngPipeObj): NngSocketObj {.exportc, dynlib, cdecl.} =
    if p.id in gPipes:
      let pipe = gPipes[p.id]
      let s = pipe.socket
      if s != nil:
        for id, sock in gSockets:
          if sock == s:
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
    if p.id in gPipes:
      let pipe = gPipes[p.id]
      let sock = pipe.socket
      pipe.close()
      if sock != nil:
        closePendingSendsForSocket(sock.id)
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
      of 0:
        pipeEvAddPre
      of 1:
        pipeEvAddPost
      of 2:
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
      if sock.engineKind != engSub0:
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

  proc nng_socket_raw*(
      s: NngSocketObj, val: ptr bool
  ): cint {.exportc, dynlib, cdecl.} =
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

  proc nng_socket_set_int*(
      s: NngSocketObj, name: cstring, val: cint
  ): cint {.exportc, dynlib, cdecl.} =
    if s.id notin gSockets or name == nil:
      return cint(NNG_EINVAL)
    let sock = gSockets[s.id]
    let optName = $name
    if optName in [NNG_OPT_SUB_SUBSCRIBE, NNG_OPT_SUB_UNSUBSCRIBE]:
      if sock.engineKind != engSub0:
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
      if sock.engineKind != engSub0:
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

  proc nng_device*(
      s1: NngSocketObj, s2: NngSocketObj
  ): cint {.exportc, dynlib, cdecl.} =
    var aio: ptr NngAioObj = nil
    let rv = nng_aio_alloc(addr aio, nil, nil)
    if rv != 0:
      return rv
    nng_device_aio(aio, s1, s2)
    nng_aio_wait(aio)
    result = nng_aio_result(aio)
    nng_aio_free(aio)

  # ----------------------------------------------------------------------
  # TLS Configuration
  # ----------------------------------------------------------------------

  proc nng_tls_config_alloc*(
      cfgp: ptr pointer, mode: cint
  ): cint {.exportc, dynlib, cdecl.} =
    if cfgp == nil:
      return cint(NNG_EINVAL)
    let tlsMode = if mode == 1: tlsModeServer else: tlsModeClient
    let cfg = newTlsConfig(tlsMode)
    let id = gNextTlsCfgId
    inc gNextTlsCfgId
    gTlsConfigs[id] = cfg
    cfgp[] = cast[pointer](cfg)
    return 0

  proc nng_tls_config_free*(cfg: pointer) {.exportc, dynlib, cdecl.} =
    if cfg != nil:
      for id, c in gTlsConfigs:
        if cast[pointer](c) == cfg:
          gTlsConfigs.del(id)
          break

  proc nng_tls_config_server_name*(
      cfg: pointer, name: cstring
  ): cint {.exportc, dynlib, cdecl.} =
    if cfg == nil or name == nil:
      return cint(NNG_EINVAL)
    let c = cast[TlsConfig](cfg)
    c.setServerName($name)
    return 0

  proc nng_tls_config_auth_mode*(
      cfg: pointer, mode: cint
  ): cint {.exportc, dynlib, cdecl.} =
    if cfg == nil:
      return cint(NNG_EINVAL)
    let c = cast[TlsConfig](cfg)
    let auth =
      case mode
      of 0:
        tlsAuthModeNone
      of 1:
        tlsAuthModeOptional
      of 2:
        tlsAuthModeRequired
      else:
        return cint(NNG_EINVAL)
    c.setAuthMode(auth)
    return 0

  proc nng_tls_config_ca_chain*(
      cfg: pointer, chain: cstring, crl: cstring
  ): cint {.exportc, dynlib, cdecl.} =
    if cfg == nil or chain == nil:
      return cint(NNG_EINVAL)
    let c = cast[TlsConfig](cfg)
    c.setCaChain($chain)
    return 0

  proc nng_tls_config_ca_file*(
      cfg: pointer, path: cstring
  ): cint {.exportc, dynlib, cdecl.} =
    if cfg == nil or path == nil:
      return cint(NNG_EINVAL)
    let c = cast[TlsConfig](cfg)
    c.setCaFile($path)
    return 0

  proc nng_tls_config_own_cert*(
      cfg: pointer, cert: cstring, key: cstring, pass: cstring
  ): cint {.exportc, dynlib, cdecl.} =
    if cfg == nil or cert == nil or key == nil:
      return cint(NNG_EINVAL)
    let c = cast[TlsConfig](cfg)
    c.setOwnCert($cert, $key)
    return 0

  proc nng_tls_config_cert_key_file*(
      cfg: pointer, cert: cstring, key: cstring
  ): cint {.exportc, dynlib, cdecl.} =
    if cfg == nil or cert == nil or key == nil:
      return cint(NNG_EINVAL)
    let c = cast[TlsConfig](cfg)
    c.setCertKeyFile($cert, $key)
    return 0

  proc nng_tls_config_version*(
      cfg: pointer, minV: cint, maxV: cint
  ): cint {.exportc, dynlib, cdecl.} =
    if cfg == nil:
      return cint(NNG_EINVAL)
    let c = cast[TlsConfig](cfg)
    let minTls =
      case minV
      of 0x303: tlsVersion12
      of 0x304: tlsVersion13
      else: tlsVersion12
    let maxTls =
      case maxV
      of 0x303: tlsVersion12
      of 0x304: tlsVersion13
      else: tlsVersion13
    c.setVersion(minTls, maxTls)
    return 0

  proc nng_tls_engine_name*(): cstring {.exportc, dynlib, cdecl.} =
    return "openssl"

  # ----------------------------------------------------------------------
  # POSIX Mutex / Condition Variable / Thread Stubs for Upstream Test Runner
  # ----------------------------------------------------------------------

  proc nng_mtx_alloc*(mtxp: ptr pointer): cint {.exportc, dynlib, cdecl.} =
    if mtxp == nil:
      return cint(NNG_EINVAL)
    let m = cast[ptr Pthread_mutex](allocShared0(128))
    if pthread_mutex_init(m, nil) != 0:
      deallocShared(m)
      return cint(NNG_ENOMEM)
    mtxp[] = cast[pointer](m)
    return 0

  proc nng_mtx_free*(mtx: pointer) {.exportc, dynlib, cdecl.} =
    if mtx != nil:
      let m = cast[ptr Pthread_mutex](mtx)
      discard pthread_mutex_destroy(m)
      deallocShared(m)

  proc nng_mtx_lock*(mtx: pointer) {.exportc, dynlib, cdecl.} =
    if mtx != nil:
      discard pthread_mutex_lock(cast[ptr Pthread_mutex](mtx))

  proc nng_mtx_unlock*(mtx: pointer) {.exportc, dynlib, cdecl.} =
    if mtx != nil:
      discard pthread_mutex_unlock(cast[ptr Pthread_mutex](mtx))

  proc nng_cv_alloc*(cvp: ptr pointer, mtx: pointer): cint {.exportc, dynlib, cdecl.} =
    if cvp == nil or mtx == nil:
      return cint(NNG_EINVAL)
    let cv = createShared(NngCvObj)
    cv.c = cast[ptr Pthread_cond](allocShared0(128))
    if pthread_cond_init(cv.c, nil) != 0:
      deallocShared(cv.c)
      freeShared(cv)
      return cint(NNG_ENOMEM)
    cv.mtx = cast[ptr Pthread_mutex](mtx)
    cvp[] = cast[pointer](cv)
    return 0

  proc nng_cv_free*(cv: pointer) {.exportc, dynlib, cdecl.} =
    if cv != nil:
      let c = cast[ptr NngCvObj](cv)
      if c.c != nil:
        discard pthread_cond_destroy(c.c)
        deallocShared(c.c)
      freeShared(c)

  proc nng_cv_wait*(cv: pointer) {.exportc, dynlib, cdecl.} =
    if cv != nil:
      let c = cast[ptr NngCvObj](cv)
      if c.c != nil and c.mtx != nil:
        discard pthread_cond_wait(c.c, c.mtx)

  proc nng_cv_until*(cv: pointer, whenMs: uint64): cint {.exportc, dynlib, cdecl.} =
    if cv == nil:
      return cint(NNG_EINVAL)
    let c = cast[ptr NngCvObj](cv)
    if c.c == nil or c.mtx == nil:
      return cint(NNG_EINVAL)
    var ts: Timespec
    ts.tv_sec = posix.Time(whenMs div 1000'u64)
    ts.tv_nsec = clong((whenMs mod 1000'u64) * 1_000_000'u64)
    let rc = pthread_cond_timedwait(c.c, c.mtx, addr ts)
    if rc == ETIMEDOUT:
      return cint(NNG_ETIMEDOUT)
    elif rc != 0:
      return cint(NNG_EINVAL)
    return 0

  proc nng_cv_wake*(cv: pointer) {.exportc, dynlib, cdecl.} =
    if cv != nil:
      let c = cast[ptr NngCvObj](cv)
      if c.c != nil:
        discard pthread_cond_broadcast(c.c)

  proc nng_cv_wake1*(cv: pointer) {.exportc, dynlib, cdecl.} =
    if cv != nil:
      let c = cast[ptr NngCvObj](cv)
      if c.c != nil:
        discard pthread_cond_signal(c.c)

  proc threadEntry(arg: pointer) {.thread.} =
    {.cast(gcsafe).}:
      let t = cast[ptr NngThreadObj](arg)
      if t != nil and t.fn != nil:
        t.fn(t.arg)
      freeShared(t)

  proc nng_thread_create*(
      thrp: ptr pointer, fn: proc(arg: pointer) {.cdecl.}, arg: pointer
  ): cint {.exportc, dynlib, cdecl.} =
    if thrp == nil or fn == nil:
      return cint(NNG_EINVAL)
    let t = createShared(NngThreadObj)
    t.fn = fn
    t.arg = arg
    createThread(t.thr, threadEntry, cast[pointer](t))
    thrp[] = cast[pointer](t)
    return 0

  proc nng_thread_destroy*(thr: pointer) {.exportc, dynlib, cdecl.} =
    if thr != nil:
      let t = cast[ptr NngThreadObj](thr)
      joinThread(t.thr)
