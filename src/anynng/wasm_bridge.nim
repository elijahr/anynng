# wasm_bridge.nim - WebAssembly & TypeScript Foreign Function Interface Bridge
# Exports C-ABI / WASM functions for SP protocol state machines and wire framing.
# Coordinates with JavaScript WebSocket transport (js_ws) for browser/Node clients.
# Licensed under MIT License.

import ./core/types
import ./core/framing/ws_framing
import ./core/framing/stream_framing

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

{.pop.}
