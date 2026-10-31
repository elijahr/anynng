# stream_framing.nim - SP Stream Framing for TCP and IPC (Unix Domain Sockets)
# Licensed under MIT License.

import ../types
import ../endian
import std/strutils

const
  DefaultRecvMax* = 1048576'u64 # 1 MiB default (matching NNG_OPT_RECVMAXSZ)
  StreamGreetingLen* = 8
  StreamMsgTypeInBand* = 0x01'u8
  StreamHeaderLen* = 9 # 1-byte message type (0x01) + 8-byte BE uint64 length

func encodeGreeting*(proto: SpProto): array[8, byte] =
  ## Encodes standard SP 8-byte stream greeting.
  ## Per nng/tcp.c and nng/ipc.c:604, byte 0 is strictly 0x00 (CRIT-01).
  result[0] = 0x00'u8
  result[1] = byte('S')
  result[2] = byte('P')
  result[3] = 0x00'u8
  putBe16(result, 4, uint16(proto))
  result[6] = 0x00'u8
  result[7] = 0x00'u8

type VerifyGreetingResult* = object
  ok*: bool
  val*: SpProto
  err*: SpError

func verifyGreeting*(
    greeting: openArray[byte], expectedPeerProto: SpProto
): VerifyGreetingResult =
  if greeting.len < StreamGreetingLen:
    return VerifyGreetingResult(ok: false, val: spPair0, err: spErrProto)

  # Check magic [0x00, 'S', 'P', 0x00]
  if greeting[0] != 0x00'u8 or greeting[1] != byte('S') or greeting[2] != byte('P') or
      greeting[3] != 0x00'u8:
    return VerifyGreetingResult(ok: false, val: spPair0, err: spErrProto)

  # Check reserved [0x00, 0x00]
  if greeting[6] != 0x00'u8 or greeting[7] != 0x00'u8:
    return VerifyGreetingResult(ok: false, val: spPair0, err: spErrProto)

  let peerProtoVal = getBe16(greeting, 4)
  if peerProtoVal != uint16(expectedPeerProto):
    return VerifyGreetingResult(ok: false, val: spPair0, err: spErrProto)

  let (valid, proto) = toSpProto(peerProtoVal)
  if not valid:
    return VerifyGreetingResult(ok: false, val: spPair0, err: spErrProto)

  return VerifyGreetingResult(ok: true, val: proto, err: spOk)

func encodeStreamFrame*(data: openArray[byte], hasTypeByte: bool = false): seq[byte] =
  ## Encodes a raw byte slice with an 8-byte Big-Endian uint64 length prefix.
  ## If hasTypeByte is true (IPC per sp-ipc-mapping), a 0x01 in-band byte precedes the length.
  let totalLen = uint64(data.len)
  let hdrLen = if hasTypeByte: 9 else: 8
  result = newSeq[byte](hdrLen + int(totalLen))
  if hasTypeByte:
    result[0] = StreamMsgTypeInBand
    putBe64(result, 1, totalLen)
  else:
    putBe64(result, 0, totalLen)
  if data.len > 0:
    for i in 0 ..< data.len:
      result[hdrLen + i] = data[i]

func encodeStreamFrame*(msg: SpMsg, hasTypeByte: bool = false): seq[byte] =
  ## Encodes an SP message with an 8-byte Big-Endian uint64 length prefix.
  ## If hasTypeByte is true (IPC per sp-ipc-mapping), a 0x01 in-band byte precedes the length.
  let totalLen = uint64(msg.header.len + msg.body.len)
  let hdrLen = if hasTypeByte: 9 else: 8
  result = newSeq[byte](hdrLen + int(totalLen))
  if hasTypeByte:
    result[0] = StreamMsgTypeInBand
    putBe64(result, 1, totalLen)
  else:
    putBe64(result, 0, totalLen)
  if msg.header.len > 0:
    for i in 0 ..< msg.header.len:
      result[hdrLen + i] = msg.header[i]
  if msg.body.len > 0:
    for i in 0 ..< msg.body.len:
      result[hdrLen + msg.header.len + i] = msg.body[i]

type StreamDecoder* = object
  buf*: seq[byte]
  recvMax*: uint64
  hasTypeByte*: bool

func initStreamDecoder*(
    recvMax: uint64 = DefaultRecvMax, hasTypeByte: bool = false
): StreamDecoder =
  result.buf = @[]
  result.recvMax = recvMax
  result.hasTypeByte = hasTypeByte

proc feed*(
    dec: var StreamDecoder, chunk: openArray[byte], onMsg: proc(frame: seq[byte])
) =
  if chunk.len == 0:
    return
  let oldLen = dec.buf.len
  dec.buf.setLen(oldLen + chunk.len)
  for i in 0 ..< chunk.len:
    dec.buf[oldLen + i] = chunk[i]

  let hdrLen = if dec.hasTypeByte: 9 else: 8
  while dec.buf.len >= hdrLen:
    if dec.hasTypeByte:
      let msgType = dec.buf[0]
      if msgType != StreamMsgTypeInBand:
        dec.buf.setLen(0)
        raise
          newException(ValueError, "Invalid stream message type: 0x" & msgType.toHex(2))

    let lenOffset = if dec.hasTypeByte: 1 else: 0
    let msgLen = getBe64(dec.buf, lenOffset)
    if msgLen > dec.recvMax:
      # Exceeded max receive size (HIGH-01)
      dec.buf.setLen(0)
      raise newException(
        ValueError,
        "Stream frame length (" & $msgLen & ") exceeds recvMax (" & $dec.recvMax & ")",
      )

    let frameLen = int(msgLen)
    if dec.buf.len < hdrLen + frameLen:
      # Incomplete frame, wait for more data
      break

    var frame = newSeq[byte](frameLen)
    for i in 0 ..< frameLen:
      frame[i] = dec.buf[hdrLen + i]

    # Shift remaining buffer
    let remaining = dec.buf.len - (hdrLen + frameLen)
    if remaining > 0:
      for i in 0 ..< remaining:
        dec.buf[i] = dec.buf[hdrLen + frameLen + i]
      dec.buf.setLen(remaining)
    else:
      dec.buf.setLen(0)

    onMsg(frame)
