# ----------------------------------------------------------------------
# POSIX Mutex / Condition Variable / Thread Stubs for Upstream Test Runner
# ----------------------------------------------------------------------

proc nng_mtx_alloc*(mtxp: ptr ptr NngMtxObj): cint {.exportc, dynlib, cdecl.} =
  if mtxp == nil:
    return cint(NNG_EINVAL)
  let m = createShared(NngMtxObj)
  m.m = cast[ptr Pthread_mutex](allocShared0(128))
  if pthread_mutex_init(m.m, nil) != 0:
    deallocShared(m.m)
    freeShared(m)
    return cint(NNG_ENOMEM)
  mtxp[] = m
  return 0

proc nng_mtx_free*(mtx: ptr NngMtxObj) {.exportc, dynlib, cdecl.} =
  if mtx != nil:
    if mtx.m != nil:
      discard pthread_mutex_destroy(mtx.m)
      deallocShared(mtx.m)
    freeShared(mtx)

proc nng_mtx_lock*(mtx: ptr NngMtxObj) {.exportc, dynlib, cdecl.} =
  if mtx != nil and mtx.m != nil:
    discard pthread_mutex_lock(mtx.m)

proc nng_mtx_unlock*(mtx: ptr NngMtxObj) {.exportc, dynlib, cdecl.} =
  if mtx != nil and mtx.m != nil:
    discard pthread_mutex_unlock(mtx.m)

proc nng_cv_alloc*(
    cvp: ptr ptr NngCvObj, mtx: ptr NngMtxObj
): cint {.exportc, dynlib, cdecl.} =
  if cvp == nil or mtx == nil:
    return cint(NNG_EINVAL)
  let cv = createShared(NngCvObj)
  cv.c = cast[ptr Pthread_cond](allocShared0(128))
  if pthread_cond_init(cv.c, nil) != 0:
    deallocShared(cv.c)
    freeShared(cv)
    return cint(NNG_ENOMEM)
  cv.mtx = mtx.m
  cvp[] = cv
  return 0

proc nng_cv_free*(cv: ptr NngCvObj) {.exportc, dynlib, cdecl.} =
  if cv != nil:
    if cv.c != nil:
      discard pthread_cond_destroy(cv.c)
      deallocShared(cv.c)
    freeShared(cv)

proc nng_cv_wait*(cv: ptr NngCvObj) {.exportc, dynlib, cdecl.} =
  if cv != nil and cv.c != nil and cv.mtx != nil:
    discard pthread_cond_wait(cv.c, cv.mtx)

proc nng_cv_until*(cv: ptr NngCvObj, whenMs: uint64): cint {.exportc, dynlib, cdecl.} =
  if cv == nil or cv.c == nil or cv.mtx == nil:
    return cint(NNG_EINVAL)
  var ts: Timespec
  ts.tv_sec = posix.Time(whenMs div 1000'u64)
  ts.tv_nsec = clong((whenMs mod 1000'u64) * 1_000_000'u64)
  let rc = pthread_cond_timedwait(cv.c, cv.mtx, addr ts)
  if rc == ETIMEDOUT:
    return cint(NNG_ETIMEDOUT)
  elif rc != 0:
    return cint(NNG_EINVAL)
  return 0

proc nng_cv_wake*(cv: ptr NngCvObj) {.exportc, dynlib, cdecl.} =
  if cv != nil and cv.c != nil:
    discard pthread_cond_broadcast(cv.c)

proc nng_cv_wake1*(cv: ptr NngCvObj) {.exportc, dynlib, cdecl.} =
  if cv != nil and cv.c != nil:
    discard pthread_cond_signal(cv.c)

proc threadEntry(arg: pointer) {.thread.} =
  {.cast(gcsafe).}:
    let t = cast[ptr NngThreadObj](arg)
    if t != nil and t.fn != nil:
      t.fn(t.arg)
    freeShared(t)

proc nng_thread_create*(
    thrp: ptr ptr NngThreadObj, fn: proc(arg: pointer) {.cdecl.}, arg: pointer
): cint {.exportc, dynlib, cdecl.} =
  if thrp == nil or fn == nil:
    return cint(NNG_EINVAL)
  let t = createShared(NngThreadObj)
  t.fn = fn
  t.arg = arg
  createThread(t.thr, threadEntry, cast[pointer](t))
  thrp[] = t
  return 0

proc nng_thread_set_name*(
    thr: ptr NngThreadObj, name: cstring
) {.exportc, dynlib, cdecl.} =
  discard

proc nng_thread_destroy*(thr: ptr NngThreadObj) {.exportc, dynlib, cdecl.} =
  if thr != nil:
    joinThread(thr.thr)
