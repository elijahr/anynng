# bus0.nim - BUSv0 Protocol Engine (Full-Mesh Broadcast)
# Licensed under MIT License.

import ../types

type Bus0Engine* = object
  pipes*: seq[PipeId]
  raw*: bool

func initBus0Engine*(raw: bool = false): Bus0Engine =
  result.pipes = @[]
  result.raw = raw

proc step*(eng: var Bus0Engine, ev: EngineEvent): seq[EngineAction] =
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
    # Full mesh: broadcast outgoing message to ALL connected pipes
    let frame = ev.msg.header & ev.msg.body
    for p in eng.pipes:
      if ev.msg.pipeId == 0 or p != ev.msg.pipeId:
        result.add(EngineAction(kind: actSendPipe, pipeId: p, rawFrame: frame))
    result.add(EngineAction(kind: actFinishSend, ctxId: ev.ctxId))
  of evPipeMessage:
    # CRIT-04 & nng/bus.c:249: Cooked BUS0 delivers strictly to local receive!
    # Incoming frames are NEVER forwarded or echoed across other pipes.
    let msg = initSpMsg(ev.rawFrame)
    result.add(
      EngineAction(kind: actDeliverRecv, ctxId: 0, msg: msg, pipeId: ev.pipeId)
    )
  of evUserCancel, evTimerTick:
    discard
