# survey0.nim - SURVEYORv0 and RESPONDENTv0 Protocol Engines
# Licensed under MIT License.

import std/tables
import ../types
import ../endian

const
  DefaultSurveyTime* = 1000'i64 # 1 second (NNG_OPT_SURVEYOR_SURVEYTIME)
  DefaultMaxTtl* = 8'u32

type
  SurveyorContextState* = object
    surveyId*: uint32
    deadlineMs*: int64
    inSurvey*: bool

  Surveyor0Engine* = object
    pipes*: seq[PipeId]
    nextSurveyId*: uint32
    contexts*: Table[uint32, SurveyorContextState]
    surveyToCtx*: Table[uint32, uint32]
    surveyTimeMs*: int64
    raw*: bool

func initSurveyor0Engine*(
    raw: bool = false, surveyTimeMs: int64 = DefaultSurveyTime
): Surveyor0Engine =
  result.pipes = @[]
  result.nextSurveyId = 1'u32
  result.contexts = initTable[uint32, SurveyorContextState]()
  result.surveyToCtx = initTable[uint32, uint32]()
  result.surveyTimeMs = surveyTimeMs
  result.raw = raw

proc nextId*(eng: var Surveyor0Engine): uint32 =
  let id = eng.nextSurveyId
  inc eng.nextSurveyId
  if eng.nextSurveyId > 0x7fffffff'u32:
    eng.nextSurveyId = 1'u32
  result = id or 0x80000000'u32

proc inSurvey*(eng: Surveyor0Engine, ctxId: uint32 = 0): bool =
  if ctxId in eng.contexts:
    eng.contexts[ctxId].inSurvey
  else:
    false

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
    if ev.ctxId in eng.contexts:
      let oldSId = eng.contexts[ev.ctxId].surveyId
      eng.surveyToCtx.del(oldSId)

    let sId = eng.nextId()
    let deadline = ev.nowMs + eng.surveyTimeMs
    eng.contexts[ev.ctxId] =
      SurveyorContextState(surveyId: sId, deadlineMs: deadline, inSurvey: true)
    eng.surveyToCtx[sId] = ev.ctxId

    var frame: seq[byte]
    if eng.raw:
      frame = ev.msg.header & ev.msg.body
    else:
      frame = newSeq[byte](4 + ev.msg.body.len)
      putBe32(frame, 0, sId)
      for i in 0 ..< ev.msg.body.len:
        frame[4 + i] = ev.msg.body[i]

    # Broadcast survey to all connected pipes (broadcasts to 0 pipes if none)
    for p in eng.pipes:
      result.add(EngineAction(kind: actSendPipe, pipeId: p, rawFrame: frame))
    result.add(EngineAction(kind: actFinishSend, ctxId: ev.ctxId))
  of evPipeMessage:
    if ev.rawFrame.len < 4:
      result.add(EngineAction(kind: actClosePipe, pipeId: ev.pipeId, err: spErrProto))
      return

    if eng.raw:
      var offset = 0
      var foundEnd = false
      var hops = 0
      while offset + 4 <= ev.rawFrame.len:
        let w = getBe32(ev.rawFrame, offset)
        offset += 4
        inc hops
        if hops > 8:
          return
        if (w and 0x80000000'u32) != 0:
          foundEnd = true
          break
      if not foundEnd:
        result.add(EngineAction(kind: actClosePipe, pipeId: ev.pipeId, err: spErrProto))
        return
      var msg: SpMsg
      msg.header = ev.rawFrame[0 ..< offset]
      msg.body = ev.rawFrame[offset .. ^1]
      msg.pipeId = ev.pipeId
      result.add(
        EngineAction(kind: actDeliverRecv, ctxId: 0, msg: msg, pipeId: ev.pipeId)
      )
      return

    let id = getBe32(ev.rawFrame, 0)
    if id notin eng.surveyToCtx:
      # Response for an old, mismatched, or expired survey; drop silently
      return

    let targetCtxId = eng.surveyToCtx[id]
    if targetCtxId notin eng.contexts or not eng.contexts[targetCtxId].inSurvey:
      return

    if ev.nowMs >= eng.contexts[targetCtxId].deadlineMs:
      eng.contexts[targetCtxId].inSurvey = false
      eng.surveyToCtx.del(id)
      return

    var msg: SpMsg
    if eng.raw:
      msg.header = ev.rawFrame[0 ..< 4]
      msg.body = ev.rawFrame[4 .. ^1]
    else:
      msg.body = ev.rawFrame[4 .. ^1]

    result.add(
      EngineAction(
        kind: actDeliverRecv, ctxId: targetCtxId, msg: msg, pipeId: ev.pipeId
      )
    )
  of evTimerTick:
    var expiredSurveys: seq[uint32] = @[]
    for ctxId, state in eng.contexts:
      if state.inSurvey and ev.nowMs >= state.deadlineMs:
        expiredSurveys.add(ctxId)
    for ctxId in expiredSurveys:
      let sId = eng.contexts[ctxId].surveyId
      eng.contexts[ctxId].inSurvey = false
      eng.surveyToCtx.del(sId)
      result.add(EngineAction(kind: actFailContext, ctxId: ctxId, err: spErrTimedOut))
  of evUserCancel:
    if ev.ctxId in eng.contexts:
      let sId = eng.contexts[ev.ctxId].surveyId
      eng.contexts[ev.ctxId].inSurvey = false
      eng.surveyToCtx.del(sId)

type
  RespondentContextState* = object
    pipeId*: PipeId
    header*: seq[byte]

  Respondent0Engine* = object
    pipes*: seq[PipeId]
    contexts*: Table[uint32, RespondentContextState]
    reqStack*: seq[RespondentContextState]
    maxTtl*: uint32
    raw*: bool

func initRespondent0Engine*(
    raw: bool = false, maxTtl: uint32 = DefaultMaxTtl
): Respondent0Engine =
  result.pipes = @[]
  result.contexts = initTable[uint32, RespondentContextState]()
  result.reqStack = @[]
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
      if hops > eng.maxTtl:
        return
      if (word and 0x80000000'u32) != 0:
        foundBottom = true
        break

    if not foundBottom:
      result.add(EngineAction(kind: actClosePipe, pipeId: ev.pipeId, err: spErrProto))
      return

    let header = ev.rawFrame[0 ..< offset]
    let body = ev.rawFrame[offset .. ^1]

    let repState = RespondentContextState(pipeId: ev.pipeId, header: header)
    if ev.ctxId != 0:
      eng.contexts[ev.ctxId] = repState
    else:
      eng.reqStack.add(repState)

    var msg: SpMsg
    if eng.raw:
      var rawHeader = newSeq[byte](4 + header.len)
      putBe32(rawHeader, 0, uint32(ev.pipeId))
      for i in 0 ..< header.len:
        rawHeader[4 + i] = header[i]
      msg.header = rawHeader
      msg.body = body
    else:
      msg.header = header
      msg.body = body
    msg.pipeId = ev.pipeId

    result.add(
      EngineAction(kind: actDeliverRecv, ctxId: ev.ctxId, msg: msg, pipeId: ev.pipeId)
    )
  of evUserSend:
    if eng.raw:
      if ev.msg.header.len < 8:
        result.add(EngineAction(kind: actFinishSend, ctxId: ev.ctxId))
        return
      let targetPipe = PipeId(getBe32(ev.msg.header, 0))
      var offset = 4
      var hasBottom = false
      while offset + 4 <= ev.msg.header.len:
        let w = getBe32(ev.msg.header, offset)
        offset += 4
        if (w and 0x80000000'u32) != 0:
          hasBottom = true
          break
      if not hasBottom:
        result.add(EngineAction(kind: actFinishSend, ctxId: ev.ctxId))
        return
      let frame = ev.msg.header[4 .. ^1] & ev.msg.body
      if targetPipe in eng.pipes:
        result.add(EngineAction(kind: actSendPipe, pipeId: targetPipe, rawFrame: frame))
      result.add(EngineAction(kind: actFinishSend, ctxId: ev.ctxId))
      return

    var state: RespondentContextState
    var found = false

    if ev.ctxId != 0 and ev.ctxId in eng.contexts:
      state = eng.contexts[ev.ctxId]
      eng.contexts.del(ev.ctxId)
      found = true
    elif eng.reqStack.len > 0:
      var foundIdx = -1
      if ev.msg.header.len >= 4:
        for idx in countdown(eng.reqStack.len - 1, 0):
          if eng.reqStack[idx].header == ev.msg.header:
            foundIdx = idx
            break
      if foundIdx >= 0:
        state = eng.reqStack[foundIdx]
        eng.reqStack.delete(foundIdx)
      else:
        state = eng.reqStack[0]
        eng.reqStack.delete(0)
      found = true
    elif eng.contexts.len > 0:
      for k in eng.contexts.keys:
        state = eng.contexts[k]
        eng.contexts.del(k)
        found = true
        break

    if not found:
      result.add(EngineAction(kind: actFailContext, ctxId: ev.ctxId, err: spErrState))
      return

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
