# wasm_bridge.nim - WebAssembly & TypeScript Foreign Function Interface Bridge
# Exports C-ABI / WASM functions for SP protocol state machines and wire framing.
# Coordinates with JavaScript WebSocket transport (js_ws) for browser/Node clients.
# Licensed under MIT License.

import std/tables
import ./core/types
import ./core/framing/ws_framing
import ./core/framing/stream_framing
import ./core/protocols/[pair0, pair1, pubsub0, reqrep0, pipeline0, survey0, bus0]

template pArr(p: pointer or ptr byte): ptr UncheckedArray[byte] =
  cast[ptr UncheckedArray[byte]](p)

func readBe16(p: ptr byte, offset: int = 0): uint16 {.inline.} =
  (uint16(pArr(p)[offset]) shl 8) or uint16(pArr(p)[offset + 1])

func writeBe16(p: ptr byte, offset: int, val: uint16) {.inline.} =
  pArr(p)[offset] = byte((val shr 8) and 0xff'u16)
  pArr(p)[offset + 1] = byte(val and 0xff'u16)

func readBe32(p: ptr byte, offset: int = 0): uint32 {.inline.} =
  (uint32(pArr(p)[offset]) shl 24) or (uint32(pArr(p)[offset + 1]) shl 16) or
    (uint32(pArr(p)[offset + 2]) shl 8) or uint32(pArr(p)[offset + 3])

func writeBe32(p: ptr byte, offset: int, val: uint32) {.inline.} =
  pArr(p)[offset + 0] = byte((val shr 24) and 0xff'u32)
  pArr(p)[offset + 1] = byte((val shr 16) and 0xff'u32)
  pArr(p)[offset + 2] = byte((val shr 8) and 0xff'u32)
  pArr(p)[offset + 3] = byte(val and 0xff'u32)

func readBe64(p: ptr byte, offset: int = 0): uint64 {.inline.} =
  (uint64(pArr(p)[offset]) shl 56) or (uint64(pArr(p)[offset + 1]) shl 48) or
    (uint64(pArr(p)[offset + 2]) shl 40) or (uint64(pArr(p)[offset + 3]) shl 32) or
    (uint64(pArr(p)[offset + 4]) shl 24) or (uint64(pArr(p)[offset + 5]) shl 16) or
    (uint64(pArr(p)[offset + 6]) shl 8) or uint64(pArr(p)[offset + 7])

func writeBe64(p: ptr byte, offset: int, val: uint64) {.inline.} =
  pArr(p)[offset + 0] = byte((val shr 56) and 0xff'u64)
  pArr(p)[offset + 1] = byte((val shr 48) and 0xff'u64)
  pArr(p)[offset + 2] = byte((val shr 40) and 0xff'u64)
  pArr(p)[offset + 3] = byte((val shr 32) and 0xff'u64)
  pArr(p)[offset + 4] = byte((val shr 24) and 0xff'u64)
  pArr(p)[offset + 5] = byte((val shr 16) and 0xff'u64)
  pArr(p)[offset + 6] = byte((val shr 8) and 0xff'u64)
  pArr(p)[offset + 7] = byte(val and 0xff'u64)

func toSpProto(val: int32): SpProto =
  case val
  of 0x10: spPair0
  of 0x11: spPair1
  of 0x20: spPub0
  of 0x21: spSub0
  of 0x30: spReq0
  of 0x31: spRep0
  of 0x50: spPush0
  of 0x51: spPull0
  of 0x62: spSurveyor0
  of 0x63: spRespondent0
  of 0x70: spBus0
  else: spPair0

{.push exportc, cdecl.}

proc anynng_wasm_version*(): int32 =
  ## Returns the anynng WASM bridge ABI version.
  1'i32

proc anynng_alloc*(size: int32): pointer =
  ## Allocates linear memory buffer for zero-copy data passing from JS/TS.
  if size <= 0:
    return nil
  alloc0(size)

proc anynng_free*(p: pointer) =
  ## Frees linear memory buffer.
  if p != nil:
    dealloc(p)

proc anynng_ws_dialer_subprotocol*(
    proto: int32, outBuf: cstring, outCap: int32
): int32 =
  ## Returns dialer Sec-WebSocket-Protocol name into outBuf.
  let p = toSpProto(proto)
  let sub = dialerSubprotocol(p)
  if outBuf != nil and outCap > sub.len:
    copyMem(outBuf, cstring(sub), sub.len + 1)
    return int32(sub.len)
  return int32(sub.len)

proc anynng_ws_listener_subprotocol*(
    proto: int32, outBuf: cstring, outCap: int32
): int32 =
  ## Returns listener Sec-WebSocket-Protocol name into outBuf.
  let p = toSpProto(proto)
  let sub = listenerSubprotocol(p)
  if outBuf != nil and outCap > sub.len:
    copyMem(outBuf, cstring(sub), sub.len + 1)
    return int32(sub.len)
  return int32(sub.len)

# ----------------------------------------------------------------------
# REQ0 / REP0 Protocol Framing
# ----------------------------------------------------------------------

proc anynng_req0_encode*(
    reqId: uint32, bodyPtr: ptr byte, bodyLen: int32, outPtr: ptr byte
): int32 =
  ## Encodes REQ0 message: prepends 4-byte BE (reqId | 0x80000000) header.
  if outPtr == nil:
    return -1
  let cookedReqId = reqId or 0x80000000'u32
  writeBe32(outPtr, 0, cookedReqId)
  if bodyLen > 0 and bodyPtr != nil:
    let dest = cast[ptr byte](cast[uint](outPtr) + 4)
    copyMem(dest, bodyPtr, bodyLen)
  return 4 + bodyLen

proc anynng_req0_decode*(
    framePtr: ptr byte,
    frameLen: int32,
    outReqId: ptr uint32,
    outBodyOffset: ptr int32,
    outBodyLen: ptr int32,
): int32 =
  ## Decodes REQ0/REP0 frame: extracts 31-bit request ID and body offset/len.
  if framePtr == nil or frameLen < 4:
    return -1
  let h = readBe32(framePtr, 0)
  if outReqId != nil:
    outReqId[] = h and 0x7fffffff'u32
  if outBodyOffset != nil:
    outBodyOffset[] = 4
  if outBodyLen != nil:
    outBodyLen[] = frameLen - 4
  return 0

proc anynng_rep0_encode*(
    reqId: uint32, bodyPtr: ptr byte, bodyLen: int32, outPtr: ptr byte
): int32 =
  ## Encodes REP0 reply message: restores 4-byte BE (reqId | 0x80000000) backtrace.
  if outPtr == nil:
    return -1
  let cookedReqId = reqId or 0x80000000'u32
  writeBe32(outPtr, 0, cookedReqId)
  if bodyLen > 0 and bodyPtr != nil:
    let dest = cast[ptr byte](cast[uint](outPtr) + 4)
    copyMem(dest, bodyPtr, bodyLen)
  return 4 + bodyLen

# ----------------------------------------------------------------------
# PAIR1 Protocol Framing
# ----------------------------------------------------------------------

proc anynng_pair1_encode*(
    hops: uint32, bodyPtr: ptr byte, bodyLen: int32, outPtr: ptr byte
): int32 =
  ## Encodes PAIR1 frame: prepends 4-byte BE hop count.
  if outPtr == nil:
    return -1
  writeBe32(outPtr, 0, hops)
  if bodyLen > 0 and bodyPtr != nil:
    let dest = cast[ptr byte](cast[uint](outPtr) + 4)
    copyMem(dest, bodyPtr, bodyLen)
  return 4 + bodyLen

proc anynng_pair1_decode*(
    framePtr: ptr byte,
    frameLen: int32,
    outHops: ptr uint32,
    outBodyOffset: ptr int32,
    outBodyLen: ptr int32,
): int32 =
  ## Decodes PAIR1 frame: extracts hop count and body.
  if framePtr == nil or frameLen < 4:
    return -1
  let hops = readBe32(framePtr, 0)
  if outHops != nil:
    outHops[] = hops
  if outBodyOffset != nil:
    outBodyOffset[] = 4
  if outBodyLen != nil:
    outBodyLen[] = frameLen - 4
  return 0

# ----------------------------------------------------------------------
# PUB0 / SUB0 Topic Filtering
# ----------------------------------------------------------------------

proc anynng_sub0_matches*(
    topicPtr: ptr byte, topicLen: int32, msgPtr: ptr byte, msgLen: int32
): int32 =
  ## Returns 1 if msg begins with topic prefix; 0 otherwise. Empty topic matches all.
  if topicLen <= 0:
    return 1
  if msgLen < topicLen or msgPtr == nil or topicPtr == nil:
    return 0
  if equalMem(topicPtr, msgPtr, topicLen):
    return 1
  return 0

# ----------------------------------------------------------------------
# SURVEYOR0 / RESPONDENT0 Protocol Framing
# ----------------------------------------------------------------------

proc anynng_survey0_encode*(
    surveyId: uint32, bodyPtr: ptr byte, bodyLen: int32, outPtr: ptr byte
): int32 =
  ## Encodes SURVEYOR0 survey message: prepends 4-byte BE (surveyId | 0x80000000).
  if outPtr == nil:
    return -1
  let cookedId = surveyId or 0x80000000'u32
  writeBe32(outPtr, 0, cookedId)
  if bodyLen > 0 and bodyPtr != nil:
    let dest = cast[ptr byte](cast[uint](outPtr) + 4)
    copyMem(dest, bodyPtr, bodyLen)
  return 4 + bodyLen

proc anynng_survey0_decode*(
    framePtr: ptr byte,
    frameLen: int32,
    outSurveyId: ptr uint32,
    outBodyOffset: ptr int32,
    outBodyLen: ptr int32,
): int32 =
  ## Decodes SURVEYOR0/RESPONDENT0 frame: extracts surveyId and body.
  if framePtr == nil or frameLen < 4:
    return -1
  let h = readBe32(framePtr, 0)
  if outSurveyId != nil:
    outSurveyId[] = h and 0x7fffffff'u32
  if outBodyOffset != nil:
    outBodyOffset[] = 4
  if outBodyLen != nil:
    outBodyLen[] = frameLen - 4
  return 0

proc anynng_respondent0_encode*(
    surveyId: uint32, bodyPtr: ptr byte, bodyLen: int32, outPtr: ptr byte
): int32 =
  ## Encodes RESPONDENT0 response: preserves surveyId header.
  anynng_survey0_encode(surveyId, bodyPtr, bodyLen, outPtr)

# ----------------------------------------------------------------------
# BUS0 Protocol Framing
# ----------------------------------------------------------------------

proc anynng_bus0_encode*(
    hops: uint32, bodyPtr: ptr byte, bodyLen: int32, outPtr: ptr byte
): int32 =
  ## Encodes BUS0 frame: prepends 4-byte BE hop count.
  if outPtr == nil:
    return -1
  writeBe32(outPtr, 0, hops)
  if bodyLen > 0 and bodyPtr != nil:
    let dest = cast[ptr byte](cast[uint](outPtr) + 4)
    copyMem(dest, bodyPtr, bodyLen)
  return 4 + bodyLen

proc anynng_bus0_decode*(
    framePtr: ptr byte,
    frameLen: int32,
    outHops: ptr uint32,
    outBodyOffset: ptr int32,
    outBodyLen: ptr int32,
): int32 =
  ## Decodes BUS0 frame: extracts hop count and body.
  if framePtr == nil or frameLen < 4:
    return -1
  let hops = readBe32(framePtr, 0)
  if outHops != nil:
    outHops[] = hops
  if outBodyOffset != nil:
    outBodyOffset[] = 4
  if outBodyLen != nil:
    outBodyLen[] = frameLen - 4
  return 0

# ----------------------------------------------------------------------
# Stream Wire Framing (TCP / IPC)
# ----------------------------------------------------------------------

proc anynng_stream_encode_greeting*(proto: int32, outPtr: ptr byte): int32 =
  ## Writes 8-byte SP stream greeting into outPtr: \0 S P \0 [proto BE16] \0 \0
  if outPtr == nil:
    return -1
  let p = toSpProto(proto)
  var g = encodeGreeting(p)
  copyMem(outPtr, addr g[0], 8)
  return 8

proc anynng_stream_decode_greeting*(
    greetingPtr: ptr byte, outProto: ptr uint16
): int32 =
  ## Decodes 8-byte SP stream greeting. Returns 0 on success, -1 on protocol error.
  if greetingPtr == nil:
    return -1
  if pArr(greetingPtr)[0] != 0x00'u8 or pArr(greetingPtr)[1] != byte('S') or
      pArr(greetingPtr)[2] != byte('P') or pArr(greetingPtr)[3] != 0x00'u8:
    return -1
  let p = readBe16(greetingPtr, 4)
  if outProto != nil:
    outProto[] = p
  return 0

proc anynng_stream_encode_frame*(
    bodyPtr: ptr byte, bodyLen: int32, outPtr: ptr byte
): int32 =
  ## Encodes stream frame with 8-byte BE length prefix.
  if outPtr == nil:
    return -1
  writeBe64(outPtr, 0, uint64(bodyLen))
  if bodyLen > 0 and bodyPtr != nil:
    let dest = cast[ptr byte](cast[uint](outPtr) + 8)
    copyMem(dest, bodyPtr, bodyLen)
  return 8 + bodyLen

proc anynng_stream_decode_frame_len*(headerPtr: ptr byte, outLen: ptr uint64): int32 =
  ## Decodes 8-byte BE frame length from headerPtr.
  if headerPtr == nil:
    return -1
  let l = readBe64(headerPtr, 0)
  if outLen != nil:
    outLen[] = l
  return 0

# ----------------------------------------------------------------------
# UDP Datagram Framing (NNG v2 udp_sp_msg)
# ----------------------------------------------------------------------

proc anynng_udp_encode_header*(
    packetType: uint8, hops: uint8, flowId: uint16, seqNum: uint32, outPtr: ptr byte
): int32 =
  ## Encodes 8-byte UDP SP header.
  if outPtr == nil:
    return -1
  pArr(outPtr)[0] = packetType
  pArr(outPtr)[1] = hops
  writeBe16(outPtr, 2, flowId)
  writeBe32(outPtr, 4, seqNum)
  return 8

proc anynng_udp_decode_header*(
    headerPtr: ptr byte,
    outType: ptr uint8,
    outHops: ptr uint8,
    outFlow: ptr uint16,
    outSeq: ptr uint32,
): int32 =
  ## Decodes 8-byte UDP SP header.
  if headerPtr == nil:
    return -1
  if outType != nil:
    outType[] = pArr(headerPtr)[0]
  if outHops != nil:
    outHops[] = pArr(headerPtr)[1]
  if outFlow != nil:
    outFlow[] = readBe16(headerPtr, 2)
  if outSeq != nil:
    outSeq[] = readBe32(headerPtr, 4)
  return 0

# ----------------------------------------------------------------------
# Option & Context & Pipe Notification Support
# ----------------------------------------------------------------------

var gWasmNextCtxId: int32 = 2'i32

proc anynng_ctx_create*(): int32 =
  ## Allocates a sequential Context ID for context multiplexing.
  result = gWasmNextCtxId
  inc gWasmNextCtxId

proc anynng_ctx_close*(ctxId: int32): int32 =
  ## Closes a Context ID.
  0'i32

proc anynng_pipe_event_name*(ev: int32, outBuf: cstring, outCap: int32): int32 =
  ## Returns pipe event name string ("add_pre", "add_post", "rem_post").
  var name: string
  case ev
  of 1:
    name = "add_pre"
  of 2:
    name = "add_post"
  of 3:
    name = "rem_post"
  else:
    name = "unknown"
  if outBuf != nil and outCap > name.len:
    copyMem(outBuf, cstring(name), name.len + 1)
    return int32(name.len)
  return int32(name.len)

# ----------------------------------------------------------------------
# Canonical ProtocolEngine State Machine Handle Table & C-ABI
# ----------------------------------------------------------------------

type
  EngineKind* = enum
    ekPair0
    ekPair1
    ekPub0
    ekSub0
    ekReq0
    ekRep0
    ekPush0
    ekPull0
    ekSurveyor0
    ekRespondent0
    ekBus0

  WasmEngine* = ref object
    kind*: EngineKind
    raw*: bool
    maxTtl*: int
    polyamorous*: bool
    recvMaxSize*: int
    resendTimeMs*: int64
    surveyTimeMs*: int64
    reconnectTimeMinMs*: int
    reconnectTimeMaxMs*: int
    pair0*: Pair0Engine
    pair1*: Pair1Engine
    pub0*: Pub0Engine
    sub0*: Sub0Engine
    req0*: Req0Engine
    rep0*: Rep0Engine
    push0*: Push0Engine
    pull0*: Pull0Engine
    surveyor0*: Surveyor0Engine
    respondent0*: Respondent0Engine
    bus0*: Bus0Engine

var gEngines = initTable[int32, WasmEngine]()
var gNextEngineHandle: int32 = 1
var gStepResultBuf: seq[byte] = @[]

proc anynng_engine_create*(
    proto: int32,
    raw: int32,
    maxTtl: int32,
    poly: int32,
    resendMs: int64,
    surveyMs: int64,
): int32 =
  let sp = toSpProto(proto)
  let isRaw = raw != 0
  let isPoly = poly != 0
  let ttl =
    if maxTtl > 0:
      uint32(maxTtl)
    else:
      8'u32
  let resend = if resendMs > 0: resendMs else: 60000'i64
  let survey = if surveyMs > 0: surveyMs else: 1000'i64

  var eng = WasmEngine(
    raw: isRaw,
    maxTtl: int(ttl),
    polyamorous: isPoly,
    recvMaxSize: 1024 * 1024,
    resendTimeMs: resend,
    surveyTimeMs: survey,
    reconnectTimeMinMs: 100,
    reconnectTimeMaxMs: 10000,
  )

  case sp
  of spPair0:
    eng.kind = ekPair0
    eng.pair0 = initPair0Engine(isRaw)
  of spPair1:
    eng.kind = ekPair1
    eng.pair1 = initPair1Engine(isPoly, isRaw, ttl)
  of spPub0:
    eng.kind = ekPub0
    eng.pub0 = initPub0Engine(isRaw)
  of spSub0:
    eng.kind = ekSub0
    eng.sub0 = initSub0Engine(isRaw)
  of spReq0:
    eng.kind = ekReq0
    eng.req0 = initReq0Engine(isRaw, resend)
  of spRep0:
    eng.kind = ekRep0
    eng.rep0 = initRep0Engine(isRaw, ttl)
  of spPush0:
    eng.kind = ekPush0
    eng.push0 = initPush0Engine(isRaw)
  of spPull0:
    eng.kind = ekPull0
    eng.pull0 = initPull0Engine(isRaw)
  of spSurveyor0:
    eng.kind = ekSurveyor0
    eng.surveyor0 = initSurveyor0Engine(isRaw, survey)
  of spRespondent0:
    eng.kind = ekRespondent0
    eng.respondent0 = initRespondent0Engine(isRaw, ttl)
  of spBus0:
    eng.kind = ekBus0
    eng.bus0 = initBus0Engine(isRaw)

  let handle = gNextEngineHandle
  inc gNextEngineHandle
  gEngines[handle] = eng
  return handle

proc anynng_engine_destroy*(handle: int32): int32 =
  if handle in gEngines:
    gEngines.del(handle)
    return 0
  return -1

proc anynng_engine_subscribe*(
    handle: int32, topicPtr: ptr byte, topicLen: int32
): int32 =
  if handle notin gEngines:
    return -1
  let eng = gEngines[handle]
  if eng.kind != ekSub0:
    return -1
  var topic: seq[byte] = @[]
  if topicLen > 0 and topicPtr != nil:
    topic = newSeq[byte](topicLen)
    copyMem(addr topic[0], topicPtr, topicLen)
  eng.sub0.subscribe(topic)
  return 0

proc anynng_engine_unsubscribe*(
    handle: int32, topicPtr: ptr byte, topicLen: int32
): int32 =
  if handle notin gEngines:
    return -1
  let eng = gEngines[handle]
  if eng.kind != ekSub0:
    return -1
  var topic: seq[byte] = @[]
  if topicLen > 0 and topicPtr != nil:
    topic = newSeq[byte](topicLen)
    copyMem(addr topic[0], topicPtr, topicLen)
  eng.sub0.unsubscribe(topic)
  return 0

proc anynng_engine_set_opt_int*(handle: int32, opt: int32, val: int64): int32 =
  if handle notin gEngines:
    return -1
  let eng = gEngines[handle]
  case opt
  of 1:
    eng.maxTtl = int(val)
    if eng.kind == ekPair1:
      eng.pair1.maxTtl = uint32(val)
    elif eng.kind == ekRep0:
      eng.rep0.maxTtl = uint32(val)
    elif eng.kind == ekRespondent0:
      eng.respondent0.maxTtl = uint32(val)
  of 2:
    eng.raw = val != 0
    if eng.kind == ekPair0:
      eng.pair0.raw = eng.raw
    elif eng.kind == ekPair1:
      eng.pair1.raw = eng.raw
    elif eng.kind == ekPub0:
      eng.pub0.raw = eng.raw
    elif eng.kind == ekSub0:
      eng.sub0.raw = eng.raw
    elif eng.kind == ekReq0:
      eng.req0.raw = eng.raw
    elif eng.kind == ekRep0:
      eng.rep0.raw = eng.raw
    elif eng.kind == ekPush0:
      eng.push0.raw = eng.raw
    elif eng.kind == ekPull0:
      eng.pull0.raw = eng.raw
    elif eng.kind == ekSurveyor0:
      eng.surveyor0.raw = eng.raw
    elif eng.kind == ekRespondent0:
      eng.respondent0.raw = eng.raw
    elif eng.kind == ekBus0:
      eng.bus0.raw = eng.raw
  of 3:
    eng.polyamorous = val != 0
    if eng.kind == ekPair1:
      eng.pair1.polyamorous = eng.polyamorous
  of 4:
    eng.recvMaxSize = int(val)
  of 5:
    eng.resendTimeMs = val
    if eng.kind == ekReq0:
      eng.req0.resendTimeMs = val
  of 6:
    eng.surveyTimeMs = val
    if eng.kind == ekSurveyor0:
      eng.surveyor0.surveyTimeMs = val
  of 7:
    eng.reconnectTimeMinMs = int(val)
  of 8:
    eng.reconnectTimeMaxMs = int(val)
  else:
    return -1
  return 0

proc anynng_engine_get_opt_int*(handle: int32, opt: int32): int64 =
  if handle notin gEngines:
    return -1
  let eng = gEngines[handle]
  case opt
  of 1:
    return int64(eng.maxTtl)
  of 2:
    return if eng.raw: 1'i64 else: 0'i64
  of 3:
    return if eng.polyamorous: 1'i64 else: 0'i64
  of 4:
    return int64(eng.recvMaxSize)
  of 5:
    return eng.resendTimeMs
  of 6:
    return eng.surveyTimeMs
  of 7:
    return int64(eng.reconnectTimeMinMs)
  of 8:
    return int64(eng.reconnectTimeMaxMs)
  else:
    return -1

proc anynng_engine_set_opt*(handle: int32, name: cstring, val: int64): int32 =
  if name == nil:
    return -1
  let s = $name
  case s
  of "maxTtl", "ttl-max":
    return anynng_engine_set_opt_int(handle, 1, val)
  of "raw":
    return anynng_engine_set_opt_int(handle, 2, val)
  of "polyamorous", "pair1:polyamorous":
    return anynng_engine_set_opt_int(handle, 3, val)
  of "recvMaxSize", "recv-max-size":
    return anynng_engine_set_opt_int(handle, 4, val)
  of "resendTime", "resendTimeMs", "req:resend-time":
    return anynng_engine_set_opt_int(handle, 5, val)
  of "surveyTime", "surveyTimeMs", "surveyor:survey-time":
    return anynng_engine_set_opt_int(handle, 6, val)
  of "reconnectTimeMin", "reconnectTimeMinMs", "reconnect-time-min":
    return anynng_engine_set_opt_int(handle, 7, val)
  of "reconnectTimeMax", "reconnectTimeMaxMs", "reconnect-time-max":
    return anynng_engine_set_opt_int(handle, 8, val)
  else:
    return -1

proc anynng_engine_get_opt*(handle: int32, name: cstring): int64 =
  if name == nil:
    return -1
  let s = $name
  case s
  of "maxTtl", "ttl-max":
    return anynng_engine_get_opt_int(handle, 1)
  of "raw":
    return anynng_engine_get_opt_int(handle, 2)
  of "polyamorous", "pair1:polyamorous":
    return anynng_engine_get_opt_int(handle, 3)
  of "recvMaxSize", "recv-max-size":
    return anynng_engine_get_opt_int(handle, 4)
  of "resendTime", "resendTimeMs", "req:resend-time":
    return anynng_engine_get_opt_int(handle, 5)
  of "surveyTime", "surveyTimeMs", "surveyor:survey-time":
    return anynng_engine_get_opt_int(handle, 6)
  of "reconnectTimeMin", "reconnectTimeMinMs", "reconnect-time-min":
    return anynng_engine_get_opt_int(handle, 7)
  of "reconnectTimeMax", "reconnectTimeMaxMs", "reconnect-time-max":
    return anynng_engine_get_opt_int(handle, 8)
  else:
    return -1

proc anynng_engine_step*(
    handle: int32,
    evKind: int32,
    pipeId: uint64,
    ctxId: uint32,
    inPtr: ptr byte,
    inLen: int32,
    nowMs: int64,
    outPtr: ptr byte,
    outCap: int32,
): int32 =
  if handle notin gEngines:
    return -1
  let eng = gEngines[handle]

  var ev: EngineEvent
  case evKind
  of 0:
    ev.kind = evPipeAdded
  of 1:
    ev.kind = evPipeRemoved
  of 2:
    ev.kind = evPipeMessage
  of 3:
    ev.kind = evTimerTick
  of 4:
    ev.kind = evUserSend
  of 5:
    ev.kind = evUserCancel
  else:
    return -1

  ev.pipeId = pipeId
  ev.ctxId = ctxId
  ev.nowMs = nowMs

  if inLen > 0 and inPtr != nil:
    if ev.kind == evPipeMessage:
      ev.rawFrame = newSeq[byte](inLen)
      copyMem(addr ev.rawFrame[0], inPtr, inLen)
    elif ev.kind == evUserSend:
      var body = newSeq[byte](inLen)
      copyMem(addr body[0], inPtr, inLen)
      ev.msg = initSpMsg(body)
  elif ev.kind == evUserSend:
    ev.msg = initSpMsg(@[])

  var actions: seq[EngineAction]
  case eng.kind
  of ekPair0:
    actions = eng.pair0.step(ev)
  of ekPair1:
    actions = eng.pair1.step(ev)
  of ekPub0:
    actions = eng.pub0.step(ev)
  of ekSub0:
    actions = eng.sub0.step(ev)
  of ekReq0:
    actions = eng.req0.step(ev)
  of ekRep0:
    actions = eng.rep0.step(ev)
  of ekPush0:
    actions = eng.push0.step(ev)
  of ekPull0:
    actions = eng.pull0.step(ev)
  of ekSurveyor0:
    actions = eng.surveyor0.step(ev)
  of ekRespondent0:
    actions = eng.respondent0.step(ev)
  of ekBus0:
    if ev.kind == evUserSend and not eng.raw:
      var frame = newSeq[byte](4 + ev.msg.body.len)
      for i in 0 ..< ev.msg.body.len:
        frame[4 + i] = ev.msg.body[i]
      for p in eng.bus0.pipes:
        actions.add(EngineAction(kind: actSendPipe, pipeId: p, rawFrame: frame))
      actions.add(EngineAction(kind: actFinishSend, ctxId: ev.ctxId))
    elif ev.kind == evPipeMessage and not eng.raw:
      if ev.rawFrame.len >= 4:
        let hops = readBe32(addr ev.rawFrame[0], 0)
        if hops <= uint32(eng.maxTtl):
          let msg = initSpMsg(ev.rawFrame[4 .. ^1], ev.rawFrame[0 ..< 4])
          actions.add(
            EngineAction(kind: actDeliverRecv, ctxId: 0, msg: msg, pipeId: ev.pipeId)
          )
    else:
      actions = eng.bus0.step(ev)

  var totalLen = 4
  for a in actions:
    let payloadLen =
      case a.kind
      of actSendPipe: a.rawFrame.len
      of actDeliverRecv: a.msg.body.len
      else: 0
    totalLen += 21 + payloadLen

  gStepResultBuf.setLen(totalLen)
  writeBe32(addr gStepResultBuf[0], 0, uint32(actions.len))
  var offset = 4
  for a in actions:
    gStepResultBuf[offset] = byte(ord(a.kind))
    writeBe64(addr gStepResultBuf[0], offset + 1, a.pipeId)
    writeBe32(addr gStepResultBuf[0], offset + 9, a.ctxId)
    writeBe32(addr gStepResultBuf[0], offset + 13, uint32(ord(a.err)))
    let payloadLen =
      case a.kind
      of actSendPipe: a.rawFrame.len
      of actDeliverRecv: a.msg.body.len
      else: 0
    writeBe32(addr gStepResultBuf[0], offset + 17, uint32(payloadLen))
    if payloadLen > 0:
      let srcPtr =
        case a.kind
        of actSendPipe:
          addr a.rawFrame[0]
        of actDeliverRecv:
          addr a.msg.body[0]
        else:
          nil
      if srcPtr != nil:
        copyMem(addr gStepResultBuf[offset + 21], srcPtr, payloadLen)
    offset += 21 + payloadLen

  if outPtr != nil and outCap >= totalLen:
    copyMem(outPtr, addr gStepResultBuf[0], totalLen)
    return int32(totalLen)

  return int32(totalLen)

proc anynng_engine_get_actions*(outPtr: ptr byte, outCap: int32): int32 =
  let totalLen = gStepResultBuf.len
  if outPtr != nil and outCap >= totalLen and totalLen > 0:
    copyMem(outPtr, addr gStepResultBuf[0], totalLen)
    return int32(totalLen)
  return int32(totalLen)

{.pop.}
