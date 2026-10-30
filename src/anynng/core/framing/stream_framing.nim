# stream_framing.nim - SP Stream Framing for TCP and IPC (Unix Domain Sockets)
# Licensed under MIT License.

import ../types
import ../endian

const
  DefaultRecvMax* = 1048576'u64 # 1 MiB default (matching NNG_OPT_RECVMAXSZ)
  StreamGreetingLen* = 8

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

func toSpProto*(val: uint16): (bool, SpProto) =
  case val
  of 0x10:
    (true, spPair0)
  of 0x11:
    (true, spPair1)
  of 0x20:
    (true, spPub0)
  of 0x21:
    (true, spSub0)
  of 0x30:
    (true, spReq0)
  of 0x31:
    (true, spRep0)
  of 0x50:
    (true, spPush0)
  of 0x51:
    (true, spPull0)
  of 0x62:
    (true, spSurveyor0)
  of 0x63:
    (true, spRespondent0)
  of 0x70:
    (true, spBus0)
  else:
    (false, spPair0)

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

func encodeStreamFrame*(msg: SpMsg): seq[byte] =
  ## Encodes an SP message with an 8-byte Big-Endian uint64 length prefix.
  let totalLen = uint64(msg.header.len + msg.body.len)
  result = newSeq[byte](8 + int(totalLen))
  putBe64(result, 0, totalLen)
  if msg.header.len > 0:
    for i in 0 ..< msg.header.len:
      result[8 + i] = msg.header[i]
  if msg.body.len > 0:
    for i in 0 ..< msg.body.len:
      result[8 + msg.header.len + i] = msg.body[i]

type StreamDecoder* = object
  buf*: seq[byte]
  recvMax*: uint64

func initStreamDecoder*(recvMax: uint64 = DefaultRecvMax): StreamDecoder =
  result.buf = @[]
  result.recvMax = recvMax

proc feed*(
    dec: var StreamDecoder, chunk: openArray[byte], onMsg: proc(frame: seq[byte])
) =
  if chunk.len == 0:
    return
  let oldLen = dec.buf.len
  dec.buf.setLen(oldLen + chunk.len)
  for i in 0 ..< chunk.len:
    dec.buf[oldLen + i] = chunk[i]

  while dec.buf.len >= 8:
    let msgLen = getBe64(dec.buf, 0)
    if msgLen > dec.recvMax:
      # Exceeded max receive size (HIGH-01)
      dec.buf.setLen(0)
      raise newException(
        ValueError,
        "Stream frame length (" & $msgLen & ") exceeds recvMax (" & $dec.recvMax & ")",
      )

    let frameLen = int(msgLen)
    if dec.buf.len < 8 + frameLen:
      # Incomplete frame, wait for more data
      break

    var frame = newSeq[byte](frameLen)
    for i in 0 ..< frameLen:
      frame[i] = dec.buf[8 + i]

    # Shift remaining buffer
    let remaining = dec.buf.len - (8 + frameLen)
    if remaining > 0:
      for i in 0 ..< remaining:
        dec.buf[i] = dec.buf[8 + frameLen + i]
      dec.buf.setLen(remaining)
    else:
      dec.buf.setLen(0)

    onMsg(frame)
