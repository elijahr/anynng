# pair0.nim - PAIRv0 Protocol Engine (1:1 bidirectional pipe, 0-byte header)
# Licensed under MIT License.

import ../types

type Pair0Engine* = object
  pipeId*: PipeId
  hasPipe*: bool
  raw*: bool

func initPair0Engine*(raw: bool = false): Pair0Engine =
  result.pipeId = 0
  result.hasPipe = false
  result.raw = raw

proc step*(eng: var Pair0Engine, ev: EngineEvent): seq[EngineAction] =
  case ev.kind
  of evPipeAdded:
    if not eng.hasPipe:
      eng.pipeId = ev.pipeId
      eng.hasPipe = true
    else:
      # PAIR0 only accepts a single pipe; reject extra pipes
      result.add(EngineAction(kind: actClosePipe, pipeId: ev.pipeId, err: spErrProto))
  of evPipeRemoved:
    if eng.hasPipe and eng.pipeId == ev.pipeId:
      eng.hasPipe = false
      eng.pipeId = 0
  of evUserSend:
    if not eng.hasPipe:
      result.add(
        EngineAction(kind: actFailContext, ctxId: ev.ctxId, err: spErrTimedOut)
      )
    else:
      var frame = ev.msg.body
      result.add(EngineAction(kind: actSendPipe, pipeId: eng.pipeId, rawFrame: frame))
      result.add(EngineAction(kind: actFinishSend, ctxId: ev.ctxId))
  of evPipeMessage:
    if eng.hasPipe and eng.pipeId == ev.pipeId:
      let msg = initSpMsg(ev.rawFrame)
      result.add(
        EngineAction(kind: actDeliverRecv, ctxId: 0, msg: msg, pipeId: ev.pipeId)
      )
  of evUserCancel, evTimerTick:
    discard
