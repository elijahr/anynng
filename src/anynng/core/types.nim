# types.nim - Core Scalability Protocol types and Sans-I/O Engine Contract
# Licensed under MIT License.

type SpProto* = enum
  spPair0 = 0x10
  spPair1 = 0x11
  spPub0 = 0x20
  spSub0 = 0x21
  spReq0 = 0x30
  spRep0 = 0x31
  spPush0 = 0x50
  spPull0 = 0x51
  spSurveyor0 = 0x62
  spRespondent0 = 0x63
  spBus0 = 0x70

func toSpProto*(val: uint16): (bool, SpProto) =
  case val
  of uint16(spPair0):
    (true, spPair0)
  of uint16(spPair1):
    (true, spPair1)
  of uint16(spPub0):
    (true, spPub0)
  of uint16(spSub0):
    (true, spSub0)
  of uint16(spReq0):
    (true, spReq0)
  of uint16(spRep0):
    (true, spRep0)
  of uint16(spPush0):
    (true, spPush0)
  of uint16(spPull0):
    (true, spPull0)
  of uint16(spSurveyor0):
    (true, spSurveyor0)
  of uint16(spRespondent0):
    (true, spRespondent0)
  of uint16(spBus0):
    (true, spBus0)
  else:
    (false, spPair0)

type
  SpOption* = enum
    optRaw
    optRecvTimeout
    optSendTimeout
    optMaxTtl
    optReconnectMin
    optReconnectMax
    optPair1Poly
    optSubSubscribe
    optSubUnsubscribe
    optSubPrefNew
    optReqResendTime
    optSurveyorSurveyTime
    optRecvMax

  SpError* = enum
    spOk = 0
    spErrProto = 1
    spErrTimedOut = 2
    spErrMsgSize = 3
    spErrClosed = 4
    spErrNoMatch = 5
    spErrBadUrl = 6
    spErrNotSupported = 7
    spErrCanceled = 8
    spErrState = 9
    spErrConnReset = 10

  SpTimeoutError* = object of CatchableError

  PipeId* = uint64

  PipeEvent* = enum
    pipeEvAddPre = 0
    pipeEvAddPost = 1
    pipeEvRemPost = 2

  Pipe* = ref object
    pipeId*: PipeId
    proto*: SpProto
    sendImpl*: proc(data: openArray[byte]) {.closure.}
    closeImpl*: proc() {.closure.}
    drainImpl*: proc() {.closure.}
    sockPtr*: pointer
    dialerPtr*: pointer
    listenerPtr*: pointer
    tlsVerified*: bool
    tlsPeerCn*: string
    locAddr*: string
    remAddr*: string
    schemeStr*: string

  PipeNotifyCb* = proc(pipe: Pipe, ev: PipeEvent) {.closure.}

proc send*(p: Pipe, data: openArray[byte]) =
  if p != nil and p.sendImpl != nil:
    p.sendImpl(data)

proc close*(p: Pipe) =
  if p != nil and p.closeImpl != nil:
    p.closeImpl()

proc drain*(p: Pipe) =
  if p != nil and p.drainImpl != nil:
    p.drainImpl()

proc newPipe*(
    pipeId: PipeId,
    proto: SpProto,
    sendCb: proc(data: openArray[byte]) {.closure.},
    closeCb: proc() {.closure.},
): Pipe =
  Pipe(
    pipeId: pipeId,
    proto: proto,
    sendImpl: sendCb,
    closeImpl: closeCb,
    sockPtr: nil,
    dialerPtr: nil,
    listenerPtr: nil,
    tlsVerified: false,
    tlsPeerCn: "",
    locAddr: "",
    remAddr: "",
  )

var gNextPipeId: PipeId = 1'u64

proc allocPipeId*(): PipeId =
  result = gNextPipeId
  inc gNextPipeId

type
  SpMsg* = object
    header*: seq[byte]
    body*: seq[byte]
    pipeId*: PipeId

  EngineEventKind* = enum
    evPipeAdded
    evPipeRemoved
    evPipeMessage
    evTimerTick
    evUserSend
    evUserCancel

  EngineEvent* = object
    kind*: EngineEventKind
    pipeId*: PipeId
    ctxId*: uint32
    rawFrame*: seq[byte]
    msg*: SpMsg
    nowMs*: int64

  EngineActionKind* = enum
    actSendPipe
    actClosePipe
    actDeliverRecv
    actFinishSend
    actFailContext

  EngineAction* = object
    kind*: EngineActionKind
    pipeId*: PipeId
    ctxId*: uint32
    rawFrame*: seq[byte]
    msg*: SpMsg
    err*: SpError

func initSpMsg*(
    body: openArray[byte] = @[], header: openArray[byte] = @[], pipeId: PipeId = 0
): SpMsg =
  result.header = @header
  result.body = @body
  result.pipeId = pipeId

func `==`*(a, b: SpMsg): bool =
  a.header == b.header and a.body == b.body

func protoName*(p: SpProto): string =
  case p
  of spPair0: "pair"
  of spPair1: "pair1"
  of spPub0: "pub"
  of spSub0: "sub"
  of spReq0: "req"
  of spRep0: "rep"
  of spPush0: "push"
  of spPull0: "pull"
  of spSurveyor0: "surveyor"
  of spRespondent0: "respondent"
  of spBus0: "bus"

func peerProto*(p: SpProto): SpProto =
  case p
  of spPair0: spPair0
  of spPair1: spPair1
  of spPub0: spSub0
  of spSub0: spPub0
  of spReq0: spRep0
  of spRep0: spReq0
  of spPush0: spPull0
  of spPull0: spPush0
  of spSurveyor0: spRespondent0
  of spRespondent0: spSurveyor0
  of spBus0: spBus0
