# t_core_protocols.nim - Sans-I/O Unit Tests for All 11 SP Protocol Roles
# Licensed under MIT License.

import std/unittest
import anynng/core/types
import anynng/core/endian
import anynng/core/protocols/[pair0, pair1, pubsub0, reqrep0, pipeline0, survey0, bus0]

suite "PAIR0 Protocol Engine":
  test "Single pipe accepted, extra pipe rejected with actClosePipe":
    var eng = initPair0Engine()
    let a1 = eng.step(EngineEvent(kind: evPipeAdded, pipeId: 1))
    check a1.len == 0
    check eng.hasPipe
    check eng.pipeId == 1

    # Second pipe rejected
    let a2 = eng.step(EngineEvent(kind: evPipeAdded, pipeId: 2))
    check a2.len == 1
    check a2[0].kind == actClosePipe
    check a2[0].pipeId == 2
    check a2[0].err == spErrProto

  test "Send delivers raw frame, recv delivers body":
    var eng = initPair0Engine()
    discard eng.step(EngineEvent(kind: evPipeAdded, pipeId: 10))

    let sendActions = eng.step(
      EngineEvent(kind: evUserSend, ctxId: 1, msg: initSpMsg(@[byte(1), byte(2)]))
    )
    check sendActions.len == 2
    check sendActions[0].kind == actSendPipe
    check sendActions[0].pipeId == 10
    check sendActions[0].rawFrame == @[byte(1), byte(2)]
    check sendActions[1].kind == actFinishSend
    check sendActions[1].ctxId == 1

    let recvActions = eng.step(
      EngineEvent(kind: evPipeMessage, pipeId: 10, rawFrame: @[byte(3), byte(4)])
    )
    check recvActions.len == 1
    check recvActions[0].kind == actDeliverRecv
    check recvActions[0].msg.body == @[byte(3), byte(4)]

suite "PAIR1 Protocol Engine":
  test "Cooked send prepends hop 1, recv strips hop count (HIGH-02)":
    var eng = initPair1Engine()
    discard eng.step(EngineEvent(kind: evPipeAdded, pipeId: 1))

    let sendActions = eng.step(
      EngineEvent(kind: evUserSend, ctxId: 1, msg: initSpMsg(@[byte('p'), byte('1')]))
    )
    check sendActions.len == 2
    check sendActions[0].kind == actSendPipe
    check sendActions[0].rawFrame.len == 6
    check getBe32(sendActions[0].rawFrame, 0) == 1'u32
    check sendActions[0].rawFrame[4 .. ^1] == @[byte('p'), byte('1')]

    # Valid receive with hop count 1
    var inFrame = newSeq[byte](4 + 2)
    putBe32(inFrame, 0, 1'u32)
    inFrame[4] = byte('r')
    inFrame[5] = byte('1')
    let recvActions =
      eng.step(EngineEvent(kind: evPipeMessage, pipeId: 1, rawFrame: inFrame))
    check recvActions.len == 1
    check recvActions[0].kind == actDeliverRecv
    check recvActions[0].msg.body == @[byte('r'), byte('1')]

  test "Hop count 0 is tolerated on recv per HIGH-02":
    var eng = initPair1Engine()
    discard eng.step(EngineEvent(kind: evPipeAdded, pipeId: 1))

    var inFrame = newSeq[byte](4 + 1)
    putBe32(inFrame, 0, 0'u32)
    inFrame[4] = byte('z')
    let recvActions =
      eng.step(EngineEvent(kind: evPipeMessage, pipeId: 1, rawFrame: inFrame))
    check recvActions.len == 1
    check recvActions[0].msg.body == @[byte('z')]

  test "Malformed frame (< 4 bytes or hop > 255) closes pipe per HIGH-02":
    var eng = initPair1Engine()
    discard eng.step(EngineEvent(kind: evPipeAdded, pipeId: 1))

    # < 4 bytes closes pipe
    let a1 = eng.step(
      EngineEvent(kind: evPipeMessage, pipeId: 1, rawFrame: @[byte(1), byte(2)])
    )
    check a1.len == 1
    check a1[0].kind == actClosePipe
    check a1[0].err == spErrProto

    # Hop > 255 closes pipe
    var badHop = newSeq[byte](4)
    putBe32(badHop, 0, 256'u32)
    let a2 = eng.step(EngineEvent(kind: evPipeMessage, pipeId: 1, rawFrame: badHop))
    check a2.len == 1
    check a2[0].kind == actClosePipe
    check a2[0].err == spErrProto

  test "Hop count > maxTtl drops message silently keeping pipe open":
    var eng = initPair1Engine(maxTtl = 4'u32)
    discard eng.step(EngineEvent(kind: evPipeAdded, pipeId: 1))

    var ttlFrame = newSeq[byte](5)
    putBe32(ttlFrame, 0, 5'u32) # Exceeds TTL 4
    ttlFrame[4] = byte('x')
    let a = eng.step(EngineEvent(kind: evPipeMessage, pipeId: 1, rawFrame: ttlFrame))
    check a.len == 0 # Dropped silently, no actClosePipe!

suite "PUB0 and SUB0 Protocol Engines":
  test "PUB0 broadcasts to all pipes":
    var pub = initPub0Engine()
    discard pub.step(EngineEvent(kind: evPipeAdded, pipeId: 10))
    discard pub.step(EngineEvent(kind: evPipeAdded, pipeId: 20))

    let actions = pub.step(
      EngineEvent(
        kind: evUserSend,
        ctxId: 1,
        msg: initSpMsg(@[byte('n'), byte('e'), byte('w'), byte('s')]),
      )
    )
    check actions.len == 3
    check actions[0].kind == actSendPipe
    check actions[0].pipeId == 10
    check actions[1].kind == actSendPipe
    check actions[1].pipeId == 20
    check actions[2].kind == actFinishSend

  test "PUB0 closes pipe if inbound message arrives (DEF-002, pub.c)":
    var pub = initPub0Engine()
    discard pub.step(EngineEvent(kind: evPipeAdded, pipeId: 10))

    let actions =
      pub.step(EngineEvent(kind: evPipeMessage, pipeId: 10, rawFrame: @[byte(1)]))
    check actions.len == 1
    check actions[0].kind == actClosePipe
    check actions[0].err == spErrProto

  test "SUB0 drop-all default, wildcard, and prefix matching (WARN-02)":
    var sub = initSub0Engine()
    discard sub.step(EngineEvent(kind: evPipeAdded, pipeId: 1))

    let weatherMsg = @[byte('w'), byte('x'), byte(':'), byte('7'), byte('2')]
    let sportsMsg = @[byte('s'), byte('p'), byte(':'), byte('1'), byte('0')]

    # 1. Default: drop-all until subscribed (WARN-02)
    let a1 = sub.step(EngineEvent(kind: evPipeMessage, pipeId: 1, rawFrame: weatherMsg))
    check a1.len == 0

    # 2. Subscribe to "wx"
    sub.subscribe(@[byte('w'), byte('x')])
    let a2 = sub.step(EngineEvent(kind: evPipeMessage, pipeId: 1, rawFrame: weatherMsg))
    check a2.len == 1
    check a2[0].kind == actDeliverRecv
    check a2[0].msg.body == weatherMsg

    # Sports message doesn't match "wx"
    let a3 = sub.step(EngineEvent(kind: evPipeMessage, pipeId: 1, rawFrame: sportsMsg))
    check a3.len == 0

    # 3. Empty subscription matches all (wildcard)
    sub.subscribe(@[])
    let a4 = sub.step(EngineEvent(kind: evPipeMessage, pipeId: 1, rawFrame: sportsMsg))
    check a4.len == 1
    check a4[0].kind == actDeliverRecv

  test "SUB0 rejects send":
    var sub = initSub0Engine()
    let a =
      sub.step(EngineEvent(kind: evUserSend, ctxId: 1, msg: initSpMsg(@[byte(1)])))
    check a.len == 1
    check a[0].kind == actFailContext
    check a[0].err == spErrProto

suite "REQ0 and REP0 Protocol Engines":
  test "REQ0 bit 31 set on reqId and context multiplexing":
    var req = initReq0Engine()
    discard req.step(EngineEvent(kind: evPipeAdded, pipeId: 100))

    let s1 = req.step(
      EngineEvent(kind: evUserSend, ctxId: 1, msg: initSpMsg(@[byte('q'), byte('1')]))
    )
    check s1.len == 2
    check s1[0].kind == actSendPipe
    let reqId1 = getBe32(s1[0].rawFrame, 0)
    check (reqId1 and 0x80000000'u32) != 0'u32
    check s1[0].rawFrame[4 .. ^1] == @[byte('q'), byte('1')]

    let s2 = req.step(
      EngineEvent(kind: evUserSend, ctxId: 2, msg: initSpMsg(@[byte('q'), byte('2')]))
    )
    let reqId2 = getBe32(s2[0].rawFrame, 0)
    check reqId2 != reqId1

    # Out-of-order replies deliver to correct ctxId
    var rep2 = newSeq[byte](4 + 2)
    putBe32(rep2, 0, reqId2)
    rep2[4] = byte('a')
    rep2[5] = byte('2')
    let r2 = req.step(EngineEvent(kind: evPipeMessage, pipeId: 100, rawFrame: rep2))
    check r2.len == 1
    check r2[0].ctxId == 2
    check r2[0].msg.body == @[byte('a'), byte('2')]

    var rep1 = newSeq[byte](4 + 2)
    putBe32(rep1, 0, reqId1)
    rep1[4] = byte('a')
    rep1[5] = byte('1')
    let r1 = req.step(EngineEvent(kind: evPipeMessage, pipeId: 100, rawFrame: rep1))
    check r1.len == 1
    check r1[0].ctxId == 1
    check r1[0].msg.body == @[byte('a'), byte('1')]

  test "REQ0 drops unmatched or expired reqId silently (HIGH-04, req.c:386)":
    var req = initReq0Engine()
    discard req.step(EngineEvent(kind: evPipeAdded, pipeId: 100))

    var unknownRep = newSeq[byte](4 + 1)
    putBe32(unknownRep, 0, 0x80009999'u32) # Unknown reqId
    let a =
      req.step(EngineEvent(kind: evPipeMessage, pipeId: 100, rawFrame: unknownRep))
    check a.len == 0 # Dropped silently, no actClosePipe!

  test "REP0 echoes backtrace stack":
    var rep = initRep0Engine()
    discard rep.step(EngineEvent(kind: evPipeAdded, pipeId: 50))

    # Inbound request with 8-byte backtrace (hop word + reqId with bit 31 set)
    var reqFrame = newSeq[byte](8 + 3)
    putBe32(reqFrame, 0, 0x00000001'u32) # intermediary hop
    putBe32(reqFrame, 4, 0x80000042'u32) # bottom request ID
    reqFrame[8] = byte('r')
    reqFrame[9] = byte('e')
    reqFrame[10] = byte('q')

    let recvActions = rep.step(
      EngineEvent(kind: evPipeMessage, pipeId: 50, ctxId: 1, rawFrame: reqFrame)
    )
    check recvActions.len == 1
    check recvActions[0].msg.body == @[byte('r'), byte('e'), byte('q')]

    # Reply echos the exact 8-byte backtrace header
    let sendActions = rep.step(
      EngineEvent(kind: evUserSend, ctxId: 1, msg: initSpMsg(@[byte('o'), byte('k')]))
    )
    check sendActions.len == 2
    check sendActions[0].kind == actSendPipe
    check sendActions[0].pipeId == 50
    check sendActions[0].rawFrame.len == 8 + 2
    check sendActions[0].rawFrame[0 ..< 8] == reqFrame[0 ..< 8]
    check sendActions[0].rawFrame[8 .. ^1] == @[byte('o'), byte('k')]

suite "PUSH0 and PULL0 Protocol Engines":
  test "PUSH round-robins across ready pipes":
    var push = initPush0Engine()
    discard push.step(EngineEvent(kind: evPipeAdded, pipeId: 1))
    discard push.step(EngineEvent(kind: evPipeAdded, pipeId: 2))

    let a1 =
      push.step(EngineEvent(kind: evUserSend, ctxId: 1, msg: initSpMsg(@[byte(1)])))
    check a1[0].pipeId == 1
    let a2 =
      push.step(EngineEvent(kind: evUserSend, ctxId: 2, msg: initSpMsg(@[byte(2)])))
    check a2[0].pipeId == 2
    let a3 =
      push.step(EngineEvent(kind: evUserSend, ctxId: 3, msg: initSpMsg(@[byte(3)])))
    check a3[0].pipeId == 1

  test "PUSH rejects inbound frames; PULL rejects send":
    var push = initPush0Engine()
    discard push.step(EngineEvent(kind: evPipeAdded, pipeId: 1))
    let pClose =
      push.step(EngineEvent(kind: evPipeMessage, pipeId: 1, rawFrame: @[byte(1)]))
    check pClose.len == 1
    check pClose[0].kind == actClosePipe

    var pull = initPull0Engine()
    let pSend =
      pull.step(EngineEvent(kind: evUserSend, ctxId: 1, msg: initSpMsg(@[byte(1)])))
    check pSend.len == 1
    check pSend[0].kind == actFailContext
    check pSend[0].err == spErrProto

suite "SURVEYOR0 and RESPONDENT0 Protocol Engines":
  test "Surveyor broadcasts survey and receives multi-responses":
    var s = initSurveyor0Engine(surveyTimeMs = 500'i64)
    discard s.step(EngineEvent(kind: evPipeAdded, pipeId: 10))
    discard s.step(EngineEvent(kind: evPipeAdded, pipeId: 20))

    let sSend = s.step(
      EngineEvent(
        kind: evUserSend,
        ctxId: 1,
        msg: initSpMsg(@[byte('p'), byte('o'), byte('l'), byte('l')]),
        nowMs: 1000'i64,
      )
    )
    check sSend.len == 3
    check sSend[0].kind == actSendPipe
    check sSend[1].kind == actSendPipe
    let surveyId = getBe32(sSend[0].rawFrame, 0)
    check (surveyId and 0x80000000'u32) != 0'u32

    # Multiple respondents reply with matching survey ID
    var resp1 = newSeq[byte](4 + 2)
    putBe32(resp1, 0, surveyId)
    resp1[4] = byte('r')
    resp1[5] = byte('1')
    let r1 = s.step(EngineEvent(kind: evPipeMessage, pipeId: 10, rawFrame: resp1))
    check r1.len == 1
    check r1[0].msg.body == @[byte('r'), byte('1')]

    var resp2 = newSeq[byte](4 + 2)
    putBe32(resp2, 0, surveyId)
    resp2[4] = byte('r')
    resp2[5] = byte('2')
    let r2 = s.step(EngineEvent(kind: evPipeMessage, pipeId: 20, rawFrame: resp2))
    check r2.len == 1
    check r2[0].msg.body == @[byte('r'), byte('2')]

    # Timer tick past deadline cleanly completes survey window (WARN-03)
    let tEnd = s.step(EngineEvent(kind: evTimerTick, nowMs: 1501'i64))
    check tEnd.len == 1
    check tEnd[0].kind == actFailContext
    check tEnd[0].err == spErrTimedOut

suite "BUS0 Protocol Engine":
  test "Full-mesh broadcast on send; incoming delivered strictly to local recv (CRIT-04)":
    var bus = initBus0Engine()
    discard bus.step(EngineEvent(kind: evPipeAdded, pipeId: 1))
    discard bus.step(EngineEvent(kind: evPipeAdded, pipeId: 2))
    discard bus.step(EngineEvent(kind: evPipeAdded, pipeId: 3))

    # Local user send broadcasts to all connected pipes
    let sActions = bus.step(
      EngineEvent(
        kind: evUserSend,
        ctxId: 1,
        msg: initSpMsg(@[byte('m'), byte('e'), byte('s'), byte('h')]),
      )
    )
    check sActions.len == 4
    check sActions[0].pipeId == 1
    check sActions[1].pipeId == 2
    check sActions[2].pipeId == 3
    check sActions[3].kind == actFinishSend

    # CRIT-04: Inbound message on pipe 1 is delivered strictly to local recv; NEVER rebroadcast across pipes 2 and 3!
    let rActions = bus.step(
      EngineEvent(kind: evPipeMessage, pipeId: 1, rawFrame: @[byte('h'), byte('i')])
    )
    check rActions.len == 1
    check rActions[0].kind == actDeliverRecv
    check rActions[0].msg.body == @[byte('h'), byte('i')]
