# anynng - Pure-Nim multi-target (C, JS, WASM) and multi-transport NNG / Scalability Protocols
# Licensed under MIT License.

import std/[tables, deques]
import ./[types, options, tls]
import ./protocols/[pair0, pair1, pubsub0, reqrep0, pipeline0, survey0, bus0]

when not defined(js):
  import std/monotimes
else:
  import std/times

const projectVersion* = "0.0.1"

proc currentNowMs*(): int64 =
  when defined(js):
    int64(jsNow())
  else:
    getMonoTime().ticks div 1_000_000

type
  EngineVariantKind* = enum
    engPair0
    engPair1
    engPub0
    engSub0
    engReq0
    engRep0
    engPush0
    engPull0
    engSurveyor0
    engRespondent0
    engBus0

  Socket* = ref object
    id*: uint32
    proto*: SpProto
    engineKind*: EngineVariantKind
    pair0Eng*: Pair0Engine
    pair1Eng*: Pair1Engine
    pub0Eng*: Pub0Engine
    sub0Eng*: Sub0Engine
    req0Eng*: Req0Engine
    rep0Eng*: Rep0Engine
    push0Eng*: Push0Engine
    pull0Eng*: Pull0Engine
    surveyor0Eng*: Surveyor0Engine
    respondent0Eng*: Respondent0Engine
    bus0Eng*: Bus0Engine
    pipes*: Table[PipeId, Pipe]
    recvQueue*: Deque[SpMsg]
    sendQueue*: Deque[SpMsg]
    inFlight*: int
    lastErr*: cint
    onRecvCb*: proc(msg: SpMsg) {.closure.}
    pendingRecvCb*: proc(res: int, msg: SpMsg) {.closure.}
    deviceHook*: proc(msg: SpMsg) {.closure.}
    closed*: bool
    raw*: bool
    options*: OptionStore
    pipeCallbacks*: Table[PipeEvent, PipeNotifyCb]
    contexts*: Table[uint32, Context]
    when not defined(js):
      recvFds*: array[2, cint]
      sendFds*: array[2, cint]
      recvFdReadable*: bool
      sendFdWritable*: bool

  Dialer* = ref object
    socket*: Socket
    url*: string
    options*: OptionStore
    tlsCfg*: TlsConfig
    closed*: bool
    started*: bool
    closeImpl*: proc() {.closure.}

  Listener* = ref object
    socket*: Socket
    url*: string
    boundPort*: int
    options*: OptionStore
    tlsCfg*: TlsConfig
    closed*: bool
    started*: bool
    closeImpl*: proc() {.closure.}

  Context* = ref object
    socket*: Socket
    ctxId*: uint32
    options*: OptionStore
    closed*: bool
    lastErr*: cint
    recvQueue*: Deque[SpMsg]
    onRecvCb*: proc(msg: SpMsg) {.closure.}
    pendingRecvCb*: proc(res: int, msg: SpMsg) {.closure.}
    sub0Topics*: seq[seq[byte]]

func matchesTopicList*(topics: seq[seq[byte]], frame: openArray[byte]): bool =
  if topics.len == 0:
    return false
  for sub in topics:
    if sub.len == 0:
      return true
    if frame.len >= sub.len:
      if equalMem(unsafeAddr frame[0], unsafeAddr sub[0], sub.len):
        return true
  return false

var gNextCtxId*: uint32 = 1'u32
var gOnPipeHook*: proc() {.closure.}
var gPendingAioCountProc*: proc(sockId: uint32): int {.closure.}
var gCloseContext*: proc(ctx: Context) {.closure.}

proc allocCtxId*(): uint32 =
  result = gNextCtxId
  inc gNextCtxId
