# pubsub0.nim - PUBv0 and SUBv0 Protocol Engines
# Licensed under MIT License.

import ../types
import ../options

type Pub0Engine* = object
  pipes*: seq[PipeId]
  raw*: bool

func initPub0Engine*(raw: bool = false): Pub0Engine =
  result.pipes = @[]
  result.raw = raw

proc step*(eng: var Pub0Engine, ev: EngineEvent): seq[EngineAction] =
  case ev.kind
  of evPipeAdded:
    if ev.pipeId notin eng.pipes:
      eng.pipes.add(ev.pipeId)
  of evPipeRemoved:
    var idx = -1
    for i, p in eng.pipes:
      if p == ev.pipeId:
        idx = i
        break
    if idx >= 0:
      eng.pipes.delete(idx)
  of evUserSend:
    # Broadcast to all connected pipes
    let frame = ev.msg.header & ev.msg.body
    for p in eng.pipes:
      result.add(EngineAction(kind: actSendPipe, pipeId: p, rawFrame: frame))
    result.add(EngineAction(kind: actFinishSend, ctxId: ev.ctxId))
  of evPipeMessage:
    # CRITICAL INVARIANT (DEF-002, pub0_pipe_recv_cb in nng/pub.c):
    # PUB0 sockets never accept inbound data; any message from a pipe closes the pipe immediately!
    result.add(EngineAction(kind: actClosePipe, pipeId: ev.pipeId, err: spErrProto))
  of evUserCancel, evTimerTick:
    discard

type Sub0Engine* = object
  pipes*: seq[PipeId]
  subscriptions*: seq[seq[byte]]
  raw*: bool
  hasContexts*: bool

func initSub0Engine*(raw: bool = false): Sub0Engine =
  result.pipes = @[]
  result.subscriptions = @[]
  result.raw = raw
  result.hasContexts = false

proc subscribe*(eng: var Sub0Engine, topic: openArray[byte]) =
  let t = @topic
  if t notin eng.subscriptions:
    eng.subscriptions.add(t)

proc unsubscribe*(eng: var Sub0Engine, topic: openArray[byte]) =
  let t = @topic
  var idx = -1
  for i, s in eng.subscriptions:
    if s == t:
      idx = i
      break
  if idx >= 0:
    eng.subscriptions.delete(idx)
  else:
    raise newNngError(NNG_ENOENT, "Subscription not found")

proc matchesTopic*(eng: Sub0Engine, frame: openArray[byte]): bool =
  # WARN-02: Default is drop-all until at least one topic is registered.
  if eng.subscriptions.len == 0:
    return false

  for sub in eng.subscriptions:
    if sub.len == 0:
      # Empty subscription "" matches everything (wildcard)
      return true
    if frame.len >= sub.len:
      var matches = true
      for i in 0 ..< sub.len:
        if frame[i] != sub[i]:
          matches = false
          break
      if matches:
        return true
  return false

proc step*(eng: var Sub0Engine, ev: EngineEvent): seq[EngineAction] =
  case ev.kind
  of evPipeAdded:
    if ev.pipeId notin eng.pipes:
      eng.pipes.add(ev.pipeId)
  of evPipeRemoved:
    var idx = -1
    for i, p in eng.pipes:
      if p == ev.pipeId:
        idx = i
        break
    if idx >= 0:
      eng.pipes.delete(idx)
  of evUserSend:
    # SUB0 sockets cannot transmit to PUB0
    result.add(EngineAction(kind: actFailContext, ctxId: ev.ctxId, err: spErrProto))
  of evPipeMessage:
    if eng.matchesTopic(ev.rawFrame) or eng.hasContexts:
      let msg = initSpMsg(ev.rawFrame)
      result.add(
        EngineAction(kind: actDeliverRecv, ctxId: 0, msg: msg, pipeId: ev.pipeId)
      )
  of evUserCancel, evTimerTick:
    discard
