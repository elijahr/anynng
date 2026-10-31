# pipeline0.nim - PUSHv0 and PULLv0 Protocol Engines (One-Way Pipeline)
# Licensed under MIT License.

import ../types

type Push0Engine* = object
  pipes*: seq[PipeId]
  currentPipeIdx*: int
  raw*: bool

func initPush0Engine*(raw: bool = false): Push0Engine =
  result.pipes = @[]
  result.currentPipeIdx = 0
  result.raw = raw

proc step*(eng: var Push0Engine, ev: EngineEvent): seq[EngineAction] =
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
      if eng.currentPipeIdx >= eng.pipes.len:
        eng.currentPipeIdx = 0
  of evUserSend:
    if eng.pipes.len == 0:
      result.add(
        EngineAction(kind: actFailContext, ctxId: ev.ctxId, err: spErrTimedOut)
      )
    else:
      let targetPipe = eng.pipes[eng.currentPipeIdx]
      eng.currentPipeIdx = (eng.currentPipeIdx + 1) mod eng.pipes.len
      let frame = ev.msg.header & ev.msg.body
      result.add(EngineAction(kind: actSendPipe, pipeId: targetPipe, rawFrame: frame))
      result.add(EngineAction(kind: actFinishSend, ctxId: ev.ctxId))
  of evPipeMessage:
    # PUSH sockets never receive; incoming data closes pipe
    result.add(EngineAction(kind: actClosePipe, pipeId: ev.pipeId, err: spErrProto))
  of evUserCancel, evTimerTick:
    discard

type Pull0Engine* = object
  pipes*: seq[PipeId]
  raw*: bool

func initPull0Engine*(raw: bool = false): Pull0Engine =
  result.pipes = @[]
  result.raw = raw

proc step*(eng: var Pull0Engine, ev: EngineEvent): seq[EngineAction] =
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
    # PULL sockets cannot send
    result.add(EngineAction(kind: actFailContext, ctxId: ev.ctxId, err: spErrProto))
  of evPipeMessage:
    let msg = initSpMsg(ev.rawFrame)
    result.add(
      EngineAction(kind: actDeliverRecv, ctxId: 0, msg: msg, pipeId: ev.pipeId)
    )
  of evUserCancel, evTimerTick:
    discard
