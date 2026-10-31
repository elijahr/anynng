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
