# pair1.nim - PAIRv1 Protocol Engine (4-byte hop count header, monogamous & polyamorous)
# Licensed under MIT License.

import ../types
import ../endian

const DefaultMaxTtl* = 8'u32

type Pair1Engine* = object
  polyamorous*: bool
  raw*: bool
  maxTtl*: uint32
  pipes*: seq[PipeId]

func initPair1Engine*(
    polyamorous: bool = false, raw: bool = false, maxTtl: uint32 = DefaultMaxTtl
): Pair1Engine =
  result.polyamorous = polyamorous
  result.raw = raw
  result.maxTtl = maxTtl
  result.pipes = @[]

proc step*(eng: var Pair1Engine, ev: EngineEvent): seq[EngineAction] =
  case ev.kind
  of evPipeAdded:
    if not eng.polyamorous and eng.pipes.len >= 1:
      # Monogamous mode only accepts 1 pipe
      result.add(EngineAction(kind: actClosePipe, pipeId: ev.pipeId, err: spErrProto))
    else:
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
    if eng.pipes.len == 0:
      result.add(EngineAction(kind: actFailContext, ctxId: ev.ctxId, err: spErrClosed))
    else:
      let targetPipe =
        if ev.pipeId != 0:
          ev.pipeId
        else:
          eng.pipes[0]
      var frame: seq[byte]
      if eng.raw:
        frame = ev.msg.header & ev.msg.body
      else:
        # Cooked send: prepend 4-byte hop count with value 1
        frame = newSeq[byte](4 + ev.msg.body.len)
        putBe32(frame, 0, 1'u32)
        for i in 0 ..< ev.msg.body.len:
          frame[4 + i] = ev.msg.body[i]

      result.add(EngineAction(kind: actSendPipe, pipeId: targetPipe, rawFrame: frame))
      result.add(EngineAction(kind: actFinishSend, ctxId: ev.ctxId))
  of evPipeMessage:
    if ev.rawFrame.len < 4:
      # Malformed: < 4 bytes terminates pipe (HIGH-02, nng/pair.c:339)
      result.add(EngineAction(kind: actClosePipe, pipeId: ev.pipeId, err: spErrProto))
      return

    let hopCount = getBe32(ev.rawFrame, 0)
    if hopCount > 0xff'u32:
      # Malformed: hop count > 255 terminates pipe (HIGH-02, nng/pair.c:339)
      result.add(EngineAction(kind: actClosePipe, pipeId: ev.pipeId, err: spErrProto))
      return

    if hopCount > eng.maxTtl:
      # TTL exceeded: drop message silently, keep pipe open (HIGH-02)
      return

    var msg: SpMsg
    if eng.raw:
      msg.header = ev.rawFrame[0 ..< 4]
      msg.body = ev.rawFrame[4 .. ^1]
    else:
      # Cooked mode: strip 4-byte hop count header
      msg.body = ev.rawFrame[4 .. ^1]

    result.add(
      EngineAction(kind: actDeliverRecv, ctxId: 0, msg: msg, pipeId: ev.pipeId)
    )
  of evUserCancel, evTimerTick:
    discard
