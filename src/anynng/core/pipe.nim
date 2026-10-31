# anynng - Pure-Nim multi-target (C, JS, WASM) and multi-transport NNG / Scalability Protocols
# Licensed under MIT License.

import std/[tables, deques]
import ./[types, options, runtime_types, socket]
import ../transports/inproc

proc attachCustomPipe*(s: Socket, pipe: Pipe) =
  pipe.sockPtr = cast[pointer](s)

  # NNG_PIPE_EV_ADD_PRE hook
  if pipeEvAddPre in s.pipeCallbacks:
    var rejected = false
    let origClose = pipe.closeImpl
    pipe.closeImpl = proc() =
      rejected = true
      if origClose != nil:
        origClose()

    s.pipeCallbacks[pipeEvAddPre](pipe, pipeEvAddPre)
    pipe.closeImpl = origClose
    if rejected:
      pipe.close()
      return

  s.pipes[pipe.pipeId] = pipe
  let pid = pipe.pipeId
  let ev = EngineEvent(kind: evPipeAdded, pipeId: pid)
  let actions = s.stepEngine(ev)
  s.executeActions(actions)
  s.updateSendFd()
  if s.lastErr != 0:
    let err = s.lastErr
    s.lastErr = 0
    raise newNngError(err, "Socket send failed: " & $err)

  # Flush send queue if any
  while s.sendQueue.len > 0 and s.pipes.len > 0:
    let msg = s.sendQueue.popFirst()
    let evSend =
      EngineEvent(kind: evUserSend, ctxId: 0, msg: msg, nowMs: currentNowMs())
    let sendActions = s.stepEngine(evSend)
    s.executeActions(sendActions)

  # NNG_PIPE_EV_ADD_POST hook
  if pipeEvAddPost in s.pipeCallbacks:
    s.pipeCallbacks[pipeEvAddPost](pipe, pipeEvAddPost)

  if gOnPipeHook != nil:
    gOnPipeHook()

proc detachCustomPipe*(s: Socket, pipeId: PipeId) =
  if pipeId in s.pipes:
    let p = s.pipes[pipeId]
    s.pipes.del(pipeId)
    s.inFlight = 0
    let ev = EngineEvent(kind: evPipeRemoved, pipeId: pipeId)
    let actions = s.stepEngine(ev)
    s.executeActions(actions)
    s.updateSendFd()
    s.updateRecvFd()

    # NNG_PIPE_EV_REM_POST hook
    if pipeEvRemPost in s.pipeCallbacks:
      s.pipeCallbacks[pipeEvRemPost](p, pipeEvRemPost)

proc feedPipeFrame*(s: Socket, pipeId: PipeId, frame: openArray[byte]) =
  if s.closed:
    return
  let maxRecv = s.options.getInt(NNG_OPT_RECVBUF)
  var hasWaiting = s.pendingRecvCb != nil or s.onRecvCb != nil
  if not hasWaiting and s.contexts.len > 0:
    for ctx in s.contexts.values:
      if not ctx.closed and (ctx.pendingRecvCb != nil or ctx.onRecvCb != nil):
        hasWaiting = true
        break
  if not hasWaiting and maxRecv > 0 and s.recvQueue.len >= maxRecv:
    if s.engineKind == engSub0 and
        (s.options.getBool(NNG_OPT_SUB_PREFNEW) or s.contexts.len > 0):
      discard
    elif s.engineKind in {engPub0, engSub0} or
        (s.engineKind == engPair1 and s.pair1Eng.polyamorous):
      return
    elif s.raw and s.engineKind in {engReq0, engRep0, engSurveyor0, engRespondent0}:
      if s.recvQueue.len < maxRecv + 20:
        discard
      elif s.engineKind == engSurveyor0:
        return
      else:
        raise newNngError(NNG_ETIMEDOUT, "Receive buffer full")
    else:
      raise newNngError(NNG_ETIMEDOUT, "Receive buffer full")
  let ev = EngineEvent(
    kind: evPipeMessage, pipeId: pipeId, rawFrame: @frame, nowMs: currentNowMs()
  )
  let actions = s.stepEngine(ev)
  s.executeActions(actions)

proc attachEndpoint*(
    s: Socket, ep: InprocEndpoint, dialerPtr: pointer = nil, listenerPtr: pointer = nil
) =
  let pid = ep.pipeId
  ep.onMessage = proc(rawFrame: seq[byte]) =
    s.feedPipeFrame(pid, rawFrame)

  ep.onClose = proc() =
    s.detachCustomPipe(pid)

  let pipe = newPipe(
    pid,
    s.proto,
    proc(data: openArray[byte]) =
      try:
        ep.send(data)
      except NngError as e:
        if (s.raw and s.engineKind in {engRep0, engRespondent0, engSurveyor0}) or
            (s.engineKind in {engPub0, engSurveyor0, engBus0}):
          discard
        else:
          raise e
      inc s.inFlight
      s.updateSendFd(),
    proc() =
      ep.close(),
  )
  pipe.sockPtr = cast[pointer](s)
  pipe.dialerPtr = dialerPtr
  pipe.listenerPtr = listenerPtr
  pipe.drainImpl = proc() =
    if ep.peer != nil and ep.peer.pipe != nil:
      let peerPipe = ep.peer.pipe
      if peerPipe.sockPtr != nil:
        let peerSock = cast[Socket](peerPipe.sockPtr)
        if peerSock.inFlight > 0:
          dec peerSock.inFlight
        peerSock.updateSendFd()
        if gOnPipeHook != nil:
          gOnPipeHook()
  ep.pipe = pipe
  s.attachCustomPipe(pipe)
  if ep.recvBuf.len > 0:
    let pending = ep.recvBuf
    ep.recvBuf = @[]
    for f in pending:
      s.feedPipeFrame(pid, f)

# Pipe Introspection Methods
proc id*(p: Pipe): PipeId =
  p.pipeId

proc socket*(p: Pipe): Socket =
  (if p == nil or p.sockPtr == nil: nil
  else: cast[Socket](p.sockPtr))

proc dialer*(p: Pipe): Dialer =
  (if p == nil or p.dialerPtr == nil: nil
  else: cast[Dialer](p.dialerPtr))

proc listener*(p: Pipe): Listener =
  (if p == nil or p.listenerPtr == nil: nil
  else: cast[Listener](p.listenerPtr))

proc getBool*(p: Pipe, name: string): bool =
  case name
  of NNG_OPT_TLS_VERIFIED:
    p.tlsVerified
  else:
    raise newNngError(NNG_ENOTSUP, "Option " & name & " not supported on pipe")

proc getString*(p: Pipe, name: string): string =
  case name
  of NNG_OPT_TLS_PEER_CN:
    p.tlsPeerCn
  of NNG_OPT_LOCADDR:
    p.locAddr
  of NNG_OPT_REMADDR:
    p.remAddr
  else:
    raise newNngError(NNG_ENOTSUP, "Option " & name & " not supported on pipe")
