# udp_framing.nim - NNG v2 8-byte udp_sp_msg Datagram Framing Codec
# Licensed under MIT License.

import ../types
import ../endian

const
  UdpSpVer* = 0x01'u8
  UdpOpCreq* = 0x01'u8
  UdpOpCack* = 0x02'u8
  UdpOpData* = 0x03'u8
  UdpOpDisc* = 0x04'u8
  UdpOpMesh* = 0x05'u8

type UdpSpHeader* = object
  ver*: uint8
  opCode*: uint8
  protoType*: uint16 # Serialized as Little-Endian per nng/udp.c:494 (CRIT-03)
  param0*: uint16 # Serialized as Little-Endian
  param1*: uint16 # Serialized as Little-Endian

func encodeUdpSpHeader*(hdr: UdpSpHeader): array[8, byte] =
  result[0] = hdr.ver
  result[1] = hdr.opCode
  putLe16(result, 2, hdr.protoType)
  putLe16(result, 4, hdr.param0)
  putLe16(result, 6, hdr.param1)

func decodeUdpSpHeader*(buf: openArray[byte]): UdpSpHeader =
  if buf.len < 8:
    raise newException(
      ValueError, "UDP packet too small for udp_sp_msg header: " & $buf.len & " bytes"
    )
  result.ver = buf[0]
  result.opCode = buf[1]
  result.protoType = getLe16(buf, 2)
  result.param0 = getLe16(buf, 4)
  result.param1 = getLe16(buf, 6)

func encodeUdpDataFrame*(proto: SpProto, msg: SpMsg): seq[byte] =
  ## Encodes an SP message into an 8-byte UDP DATA packet.
  let payloadLen = msg.header.len + msg.body.len
  result = newSeq[byte](8 + payloadLen)
  var hdr: UdpSpHeader
  hdr.ver = UdpSpVer
  hdr.opCode = UdpOpData
  hdr.protoType = uint16(proto)
  hdr.param0 = uint16(payloadLen and 0xffff)
  hdr.param1 = 0'u16
  let hdrBytes = encodeUdpSpHeader(hdr)
  for i in 0 ..< 8:
    result[i] = hdrBytes[i]
  if msg.header.len > 0:
    for i in 0 ..< msg.header.len:
      result[8 + i] = msg.header[i]
  if msg.body.len > 0:
    for i in 0 ..< msg.body.len:
      result[8 + msg.header.len + i] = msg.body[i]

func decodeUdpDataFrame*(buf: openArray[byte]): (UdpSpHeader, seq[byte]) =
  let hdr = decodeUdpSpHeader(buf)
  var payload: seq[byte] = @[]
  if buf.len > 8:
    payload = newSeq[byte](buf.len - 8)
    for i in 0 ..< payload.len:
      payload[i] = buf[8 + i]
  return (hdr, payload)
