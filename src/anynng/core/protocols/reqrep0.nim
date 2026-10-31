# reqrep0.nim - REQv0 and REPv0 Protocol Engines with Backtrace Stack & Multiplexing
# Licensed under MIT License.

import std/tables
import ../types
import ../endian

const
  DefaultReqResendTime* = 60000'i64 # 60 seconds (NNG_OPT_REQ_RESENDTIME)
  DefaultMaxTtl* = 8'u32

type
  PendingReq* = object
    ctxId*: uint32
    pipeId*: PipeId
    reqId*: uint32
    frame*: seq[byte]
    sentAtMs*: int64
    resendIntervalMs*: int64

  Req0Engine* = object
    pipes*: seq[PipeId]
    currentPipeIdx*: int
    nextReqId*: uint32
    pending*: Table[uint32, PendingReq]
    ctxToReqId*: Table[uint32, uint32]
    resendTimeMs*: int64
    raw*: bool

func initReq0Engine*(
    raw: bool = false, resendTimeMs: int64 = DefaultReqResendTime
): Req0Engine =
  result.pipes = @[]
  result.currentPipeIdx = 0
  result.nextReqId = 1'u32
  result.pending = initTable[uint32, PendingReq]()
  result.ctxToReqId = initTable[uint32, uint32]()
  result.resendTimeMs = resendTimeMs
  result.raw = raw

proc nextRequestId*(eng: var Req0Engine): uint32 =
  let id = eng.nextReqId
  inc eng.nextReqId
  if eng.nextReqId > 0x7fffffff'u32:
    eng.nextReqId = 1'u32
  # Bit 31 must be SET on all REQ request IDs (sp-websocket-mapping-01 / nng/req.c)
  result = id or 0x80000000'u32

proc step*(eng: var Req0Engine, ev: EngineEvent): seq[EngineAction] =
  case ev.kind
  of evPipeAdded:
    if ev.pipeId notin eng.pipes:
      eng.pipes.add(ev.pipeId)
      for reqId, preq in eng.pending:
        if preq.pipeId notin eng.pipes or preq.pipeId == 0:
          eng.pending[reqId].pipeId = ev.pipeId
          result.add(
            EngineAction(kind: actSendPipe, pipeId: ev.pipeId, rawFrame: preq.frame)
          )
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
    var toDelete: seq[uint32] = @[]
    for reqId, preq in eng.pending:
      if preq.pipeId == ev.pipeId:
        if eng.resendTimeMs <= 0:
          result.add(
            EngineAction(kind: actFailContext, ctxId: preq.ctxId, err: spErrConnReset)
          )
          toDelete.add(reqId)
        elif eng.pipes.len > 0:
          eng.pending[reqId].pipeId = eng.pipes[0]
          result.add(
            EngineAction(kind: actSendPipe, pipeId: eng.pipes[0], rawFrame: preq.frame)
          )
    for r in toDelete:
      if eng.pending[r].ctxId in eng.ctxToReqId:
        eng.ctxToReqId.del(eng.pending[r].ctxId)
      eng.pending.del(r)
  of evUserSend:
    if not eng.raw and ev.ctxId in eng.ctxToReqId:
      let oldReqId = eng.ctxToReqId[ev.ctxId]
      eng.pending.del(oldReqId)
      eng.ctxToReqId.del(ev.ctxId)
      result.add(
        EngineAction(kind: actFailContext, ctxId: ev.ctxId, err: spErrCanceled)
      )

    let targetPipe =
      if eng.pipes.len > 0:
        eng.pipes[eng.currentPipeIdx]
      else:
        0'u32
    if eng.pipes.len > 0:
      eng.currentPipeIdx = (eng.currentPipeIdx + 1) mod eng.pipes.len

    if eng.raw:
      let frame = ev.msg.header & ev.msg.body
      if targetPipe != 0:
        result.add(EngineAction(kind: actSendPipe, pipeId: targetPipe, rawFrame: frame))
      result.add(EngineAction(kind: actFinishSend, ctxId: ev.ctxId))
      return

    if ev.ctxId in eng.ctxToReqId:
      let oldReqId = eng.ctxToReqId[ev.ctxId]
      eng.pending.del(oldReqId)
      eng.ctxToReqId.del(ev.ctxId)

    let reqId = eng.nextRequestId()
    var frame = newSeq[byte](4 + ev.msg.body.len)
    putBe32(frame, 0, reqId)
    for i in 0 ..< ev.msg.body.len:
      frame[4 + i] = ev.msg.body[i]

    var preq = PendingReq(
      ctxId: ev.ctxId,
      pipeId: targetPipe,
      reqId: reqId,
      frame: frame,
      sentAtMs: ev.nowMs,
      resendIntervalMs: eng.resendTimeMs,
    )
    eng.pending[reqId] = preq
    eng.ctxToReqId[ev.ctxId] = reqId

    if targetPipe != 0:
      result.add(EngineAction(kind: actSendPipe, pipeId: targetPipe, rawFrame: frame))
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
        let word = getBe32(ev.rawFrame, offset)
        offset += 4
        inc hops
        if (word and 0x80000000'u32) != 0:
          foundEnd = true
          break
        if hops > 8:
          return
      if not foundEnd:
        result.add(EngineAction(kind: actClosePipe, pipeId: ev.pipeId, err: spErrProto))
        return
      var msg: SpMsg
      msg.header = ev.rawFrame[0 ..< offset]
      msg.body = ev.rawFrame[offset .. ^1]
      result.add(
        EngineAction(kind: actDeliverRecv, ctxId: 0, msg: msg, pipeId: ev.pipeId)
      )
      return

    let reqId = getBe32(ev.rawFrame, 0)
    if (reqId and 0x80000000'u32) == 0:
      # Malformed: REQ request ID must have MSB set
      result.add(EngineAction(kind: actClosePipe, pipeId: ev.pipeId, err: spErrProto))
      return

    if reqId in eng.pending:
      let preq = eng.pending[reqId]
      eng.pending.del(reqId)
      eng.ctxToReqId.del(preq.ctxId)

      var msg: SpMsg
      msg.body = ev.rawFrame[4 .. ^1]

      result.add(
        EngineAction(
          kind: actDeliverRecv, ctxId: preq.ctxId, msg: msg, pipeId: ev.pipeId
        )
      )
    else:
      # HIGH-04 & nng/req.c:386: Unmatched or expired reqId is dropped silently without dropping pipe
      discard
  of evUserCancel:
    if ev.ctxId in eng.ctxToReqId:
      let reqId = eng.ctxToReqId[ev.ctxId]
      eng.ctxToReqId.del(ev.ctxId)
      eng.pending.del(reqId)
  of evTimerTick:
    # Resend any pending requests whose timer has expired
    for reqId, preq in eng.pending:
      if preq.resendIntervalMs > 0 and
          (ev.nowMs - preq.sentAtMs) >= preq.resendIntervalMs:
        eng.pending[reqId].sentAtMs = ev.nowMs
        if eng.pipes.len > 0:
          let pipe =
            if preq.pipeId in eng.pipes:
              preq.pipeId
            else:
              eng.pipes[0]
          result.add(
            EngineAction(kind: actSendPipe, pipeId: pipe, rawFrame: preq.frame)
          )

type
  RepContextState* = object
    pipeId*: PipeId
    header*: seq[byte]

  Rep0Engine* = object
    pipes*: seq[PipeId]
    contexts*: Table[uint32, RepContextState]
    reqStack*: seq[RepContextState]
    maxTtl*: uint32
    raw*: bool

func initRep0Engine*(raw: bool = false, maxTtl: uint32 = DefaultMaxTtl): Rep0Engine =
  result.pipes = @[]
  result.contexts = initTable[uint32, RepContextState]()
  result.reqStack = @[]
  result.maxTtl = maxTtl
  result.raw = raw

proc step*(eng: var Rep0Engine, ev: EngineEvent): seq[EngineAction] =
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

    # Parse backtrace stack: sequence of 4-byte words until one with bit 31 set
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
        # Exceeded TTL without finding bottom marker
        return

    if not foundBottom:
      # Malformed: no request ID with bit 31 set found
      result.add(EngineAction(kind: actClosePipe, pipeId: ev.pipeId, err: spErrProto))
      return

    let header = ev.rawFrame[0 ..< offset]
    let body = ev.rawFrame[offset .. ^1]

    let repState = RepContextState(pipeId: ev.pipeId, header: header)
    if ev.ctxId != 0:
      eng.contexts[ev.ctxId] = repState
    else:
      eng.reqStack.add(repState)

    var msg: SpMsg
    if eng.raw:
      var pidBytes = newSeq[byte](4)
      putBe32(pidBytes, 0, uint32(ev.pipeId))
      msg.header = pidBytes & header
      msg.body = body
    else:
      msg.header = header
      msg.body = body
    msg.pipeId = ev.pipeId

    result.add(
      EngineAction(kind: actDeliverRecv, ctxId: ev.ctxId, msg: msg, pipeId: ev.pipeId)
    )
  of evUserSend:
    var state: RepContextState
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
      # Prepend the stored backtrace stack
      frame = state.header & ev.msg.body

    result.add(EngineAction(kind: actSendPipe, pipeId: state.pipeId, rawFrame: frame))
    result.add(EngineAction(kind: actFinishSend, ctxId: ev.ctxId))
  of evUserCancel:
    eng.contexts.del(ev.ctxId)
  of evTimerTick:
    discard
