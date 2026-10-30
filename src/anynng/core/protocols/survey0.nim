# survey0.nim - SURVEYORv0 and RESPONDENTv0 Protocol Engines
# Licensed under MIT License.

import std/tables
import ../types
import ../endian

const
  DefaultSurveyTime* = 1000'i64 # 1 second (NNG_OPT_SURVEYOR_SURVEYTIME)
  DefaultMaxTtl* = 8'u32

type Surveyor0Engine* = object
  pipes*: seq[PipeId]
  nextSurveyId*: uint32
  currentSurveyId*: uint32
  inSurvey*: bool
  surveyDeadlineMs*: int64
  surveyTimeMs*: int64
  activeCtxId*: uint32
  raw*: bool

func initSurveyor0Engine*(
    raw: bool = false, surveyTimeMs: int64 = DefaultSurveyTime
): Surveyor0Engine =
  result.pipes = @[]
  result.nextSurveyId = 1'u32
  result.currentSurveyId = 0'u32
  result.inSurvey = false
  result.surveyDeadlineMs = 0'i64
  result.surveyTimeMs = surveyTimeMs
  result.activeCtxId = 0'u32
  result.raw = raw

proc nextId*(eng: var Surveyor0Engine): uint32 =
  let id = eng.nextSurveyId
  inc eng.nextSurveyId
  if eng.nextSurveyId > 0x7fffffff'u32:
    eng.nextSurveyId = 1'u32
  result = id or 0x80000000'u32

proc step*(eng: var Surveyor0Engine, ev: EngineEvent): seq[EngineAction] =
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
    if eng.pipes.len == 0:
      result.add(EngineAction(kind: actFailContext, ctxId: ev.ctxId, err: spErrClosed))
      return

    eng.currentSurveyId = eng.nextId()
    eng.inSurvey = true
    eng.surveyDeadlineMs = ev.nowMs + eng.surveyTimeMs
    eng.activeCtxId = ev.ctxId

    var frame: seq[byte]
    if eng.raw:
      frame = ev.msg.header & ev.msg.body
    else:
      frame = newSeq[byte](4 + ev.msg.body.len)
      putBe32(frame, 0, eng.currentSurveyId)
      for i in 0 ..< ev.msg.body.len:
        frame[4 + i] = ev.msg.body[i]

    # Broadcast survey to all connected pipes
    for p in eng.pipes:
      result.add(EngineAction(kind: actSendPipe, pipeId: p, rawFrame: frame))
    result.add(EngineAction(kind: actFinishSend, ctxId: ev.ctxId))
  of evPipeMessage:
    if not eng.inSurvey:
      # Responses outside of an active survey are dropped silently
      return

    if ev.rawFrame.len < 4:
      result.add(EngineAction(kind: actClosePipe, pipeId: ev.pipeId, err: spErrProto))
      return

    let id = getBe32(ev.rawFrame, 0)
    if id != eng.currentSurveyId:
      # Response for an old, mismatched, or expired survey; drop silently
      return

    var msg: SpMsg
    if eng.raw:
      msg.header = ev.rawFrame[0 ..< 4]
      msg.body = ev.rawFrame[4 .. ^1]
    else:
      msg.body = ev.rawFrame[4 .. ^1]

    result.add(
      EngineAction(
        kind: actDeliverRecv, ctxId: eng.activeCtxId, msg: msg, pipeId: ev.pipeId
      )
    )
  of evTimerTick:
    # WARN-03: Deadline expiration cleanly terminates the survey window
    if eng.inSurvey and ev.nowMs >= eng.surveyDeadlineMs:
      eng.inSurvey = false
      result.add(
        EngineAction(kind: actFailContext, ctxId: eng.activeCtxId, err: spErrTimedOut)
      )
  of evUserCancel:
    if eng.inSurvey and ev.ctxId == eng.activeCtxId:
      eng.inSurvey = false

type
  RespondentContextState* = object
    pipeId*: PipeId
    header*: seq[byte]

  Respondent0Engine* = object
    pipes*: seq[PipeId]
    contexts*: Table[uint32, RespondentContextState]
    maxTtl*: uint32
    raw*: bool

func initRespondent0Engine*(
    raw: bool = false, maxTtl: uint32 = DefaultMaxTtl
): Respondent0Engine =
  result.pipes = @[]
  result.contexts = initTable[uint32, RespondentContextState]()
  result.maxTtl = maxTtl
  result.raw = raw

proc step*(eng: var Respondent0Engine, ev: EngineEvent): seq[EngineAction] =
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
  of evPipeMessage:
    if ev.rawFrame.len < 4:
      result.add(EngineAction(kind: actClosePipe, pipeId: ev.pipeId, err: spErrProto))
      return

    # Parse backtrace stack until word with bit 31 set
    var offset = 0
    var foundBottom = false
    var hops = 0'u32

    while offset + 4 <= ev.rawFrame.len:
      let word = getBe32(ev.rawFrame, offset)
      offset += 4
      inc hops
      if (word and 0x80000000'u32) != 0:
        foundBottom = true
        break
      if hops > eng.maxTtl:
        break

    if not foundBottom:
      result.add(EngineAction(kind: actClosePipe, pipeId: ev.pipeId, err: spErrProto))
      return

    let header = ev.rawFrame[0 ..< offset]
    let body = ev.rawFrame[offset .. ^1]

    eng.contexts[ev.ctxId] = RespondentContextState(pipeId: ev.pipeId, header: header)

    var msg: SpMsg
    if eng.raw:
      msg.header = header
      msg.body = body
    else:
      msg.body = body

    result.add(
      EngineAction(kind: actDeliverRecv, ctxId: ev.ctxId, msg: msg, pipeId: ev.pipeId)
    )
  of evUserSend:
    var targetCtxId = ev.ctxId
    if targetCtxId notin eng.contexts:
      if eng.contexts.len > 0:
        for k in eng.contexts.keys:
          targetCtxId = k
          break
      else:
        result.add(
          EngineAction(kind: actFailContext, ctxId: ev.ctxId, err: spErrClosed)
        )
        return

    let state = eng.contexts[targetCtxId]
    eng.contexts.del(targetCtxId)

    var frame: seq[byte]
    if eng.raw:
      frame = ev.msg.header & ev.msg.body
    else:
      frame = state.header & ev.msg.body

    result.add(EngineAction(kind: actSendPipe, pipeId: state.pipeId, rawFrame: frame))
    result.add(EngineAction(kind: actFinishSend, ctxId: ev.ctxId))
  of evUserCancel:
    eng.contexts.del(ev.ctxId)
  of evTimerTick:
    discard
