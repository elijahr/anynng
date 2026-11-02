type
  NngSocketObj* {.exportc: "nng_socket_s", bycopy.} = object
    id*: uint32

  NngDialerObj* {.exportc: "nng_dialer_s", bycopy.} = object
    id*: uint32

  NngListenerObj* {.exportc: "nng_listener_s", bycopy.} = object
    id*: uint32

  NngCtxObj* {.exportc: "nng_ctx_s", bycopy.} = object
    id*: uint32

  NngPipeObj* {.exportc: "nng_pipe_s", bycopy.} = object
    id*: uint32

  NngMsgObj* {.exportc: "nng_msg".} = object

  NngMsgImpl* = object
    header*: seq[byte]
    body*: seq[byte]
    pipeId*: PipeId

  NngIov* {.exportc: "nng_iov", bycopy.} = object
    iov_buf*: pointer
    iov_len*: csize_t

  NngAioObj* {.exportc: "nng_aio".} = object
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
    skipCbPtr*: ptr bool
    wakeTime*: int64
    devS1*, devS2*: uint32
    iov*: ptr NngIov
    niov*: cuint

  NngPipeNotifyCbCdecl* = proc(pipe: NngPipeObj, ev: cint, arg: pointer) {.cdecl.}

  PipeNotifyWrapper = ref object
    cb: NngPipeNotifyCbCdecl
    arg: pointer

  NngMtxObj* {.exportc: "nng_mtx".} = object
    m*: ptr Pthread_mutex

  NngCvObj* {.exportc: "nng_cv".} = object
    c*: ptr Pthread_cond
    mtx*: ptr Pthread_mutex

  NngThreadObj* {.exportc: "nng_thread".} = object
    fn*: proc(arg: pointer) {.cdecl.}
    arg*: pointer
    thr*: Thread[pointer]

  NngUrlObj* {.exportc: "nng_url".} = object
    u_scheme*: cstring
    u_userinfo*: cstring
    u_hostname*: cstring
    u_port*: uint32
    u_path*: cstring
    u_query*: cstring
    u_fragment*: cstring
    u_buffer*: pointer
    u_bufsz*: csize_t
    u_static*: array[128, char]

  NngUdpObj* {.exportc: "nng_udp".} = object
    fd*: cint

  NngInitParamsObj* {.exportc: "nng_init_params", bycopy.} = object
    num_task_threads*: int16
    max_task_threads*: int16
    num_expire_threads*: int16
    max_expire_threads*: int16
    num_poller_threads*: int16
    max_poller_threads*: int16
    num_resolver_threads*: int16
    malloc_fn*: proc(sz: csize_t): pointer {.cdecl.}
    calloc_fn*: proc(num: csize_t, sz: csize_t): pointer {.cdecl.}
    free_fn*: proc(p: pointer, sz: csize_t) {.cdecl.}

  NngTlsConfigObj* {.exportc: "nng_tls_config".} = object
  NngTlsCertObj* {.exportc: "nng_tls_cert_s".} = object
  NngIdMapObj* {.exportc: "nng_id_map_s".} = object
  NngStatObj* {.exportc: "nng_stat".} = object
  NngStreamObj* {.exportc: "nng_stream".} = object
  NngStreamDialerObj* {.exportc: "nng_stream_dialer".} = object
  NngStreamListenerObj* {.exportc: "nng_stream_listener".} = object

  NngLogger* = proc(level: cint, facility: cint, msgid: cstring, msg: cstring) {.cdecl.}

  NngSockaddrInproc* {.exportc: "nng_sockaddr_inproc", bycopy.} = object
    sa_family*: uint16
    sa_name*: array[128, char]

  NngSockaddrPath* {.exportc: "nng_sockaddr_path", bycopy.} = object
    sa_family*: uint16
    sa_path*: array[128, char]

  NngSockaddrIpc* {.exportc: "nng_sockaddr_ipc", bycopy.} = object
    sa_family*: uint16
    sa_path*: array[128, char]

  NngSockaddrIn6* {.exportc: "nng_sockaddr_in6", bycopy.} = object
    sa_family*: uint16
    sa_port*: uint16
    sa_scope*: uint32
    sa_addr*: array[16, uint8]

  NngSockaddrIn* {.exportc: "nng_sockaddr_in", bycopy.} = object
    sa_family*: uint16
    sa_port*: uint16
    sa_addr*: uint32

  NngSockaddrAbstract* {.exportc: "nng_sockaddr_abstract", bycopy.} = object
    sa_family*: uint16
    sa_len*: uint16
    sa_name*: array[107, uint8]

  NngSockaddrStorage* {.exportc: "nng_sockaddr_storage", bycopy.} = object
    sa_family*: uint16
    sa_pad*: array[16, uint64]

  NngSockaddr* {.exportc: "nng_sockaddr", union, bycopy.} = object
    s_family*: uint16
    s_ipc*: NngSockaddrIpc
    s_inproc*: NngSockaddrInproc
    s_in6*: NngSockaddrIn6
    s_in*: NngSockaddrIn
    s_abstract*: NngSockaddrAbstract
    s_storage*: NngSockaddrStorage

  NngSockaddrFamily* {.exportc: "nng_sockaddr_family".} = enum
    NNG_AF_UNSPEC = 0
    NNG_AF_INPROC = 1
    NNG_AF_IPC = 2
    NNG_AF_INET = 3
    NNG_AF_INET6 = 4
    NNG_AF_ABSTRACT = 5

template impl*(m: ptr NngMsgObj): ptr NngMsgImpl =
  cast[ptr NngMsgImpl](m)

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
  gDeviceSockets = initHashSet[uint32]()
  gClosedSockets = initHashSet[uint32]()
  gPendingSleepAios: seq[ptr NngAioObj] = @[]

proc cleanupAio*(aio: ptr NngAioObj) =
  if aio != nil:
    if aio.devS1 != 0:
      gDeviceSockets.excl(aio.devS1)
      aio.devS1 = 0
    if aio.devS2 != 0:
      gDeviceSockets.excl(aio.devS2)
      aio.devS2 = 0
    let sIdx = gPendingSleepAios.find(aio)
    if sIdx >= 0:
      gPendingSleepAios.delete(sIdx)
    if aio.sockId in gSockets:
      let s = gSockets[aio.sockId]
      if aio.ctxId != 0 and aio.ctxId in gContexts:
        let c = gContexts[aio.ctxId]
        c.pendingRecvCb = nil
      elif aio.ctxId == 0:
        s.pendingRecvCb = nil

proc nng_aio_skip_callback*(
    aio: ptr NngAioObj, skippedp: ptr bool
) {.exportc, dynlib, cdecl.} =
  if skippedp != nil:
    skippedp[] = false

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
  let now = getMonoTime().ticks div 1_000_000

  var readySleepAios: seq[ptr NngAioObj] = @[]
  var sIdx = 0
  while sIdx < gPendingSleepAios.len:
    let sa = gPendingSleepAios[sIdx]
    if sa.finished or sa.stopped:
      gPendingSleepAios.delete(sIdx)
    elif now >= sa.wakeTime:
      gPendingSleepAios.delete(sIdx)
      readySleepAios.add(sa)
    else:
      inc sIdx
  for sa in readySleepAios:
    nng_aio_finish(sa, 0)

  var i = 0
  while i < gPendingSendAios.len:
    let a = gPendingSendAios[i]
    if a.finished or a.stopped:
      gPendingSendAios.delete(i)
      continue
    if a.sockId notin gSockets or gSockets[a.sockId].closed:
      gPendingSendAios.delete(i)
      nng_aio_finish(a, cint(NNG_ECLOSED))
      continue
    if a.ctxId != 0 and
        (a.ctxId in gClosedCtxs or a.ctxId notin gContexts or gContexts[a.ctxId].closed):
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
