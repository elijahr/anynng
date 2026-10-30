# t_core_framing.nim - Sans-I/O Unit Tests for Wire Framing Codecs
# Licensed under MIT License.

import std/unittest
import anynng/core/types
import anynng/core/endian
import anynng/core/url
import anynng/core/framing/[ws_framing, stream_framing, udp_framing]

suite "Core Endian Codec":
  test "Big-Endian 16/32/64 bit encode and decode":
    var buf = newSeq[byte](16)
    putBe16(buf, 0, 0x1234'u16)
    check getBe16(buf, 0) == 0x1234'u16
    check buf[0] == 0x12'u8
    check buf[1] == 0x34'u8

    putBe32(buf, 2, 0x80001234'u32)
    check getBe32(buf, 2) == 0x80001234'u32
    check buf[2] == 0x80'u8
    check buf[3] == 0x00'u8
    check buf[4] == 0x12'u8
    check buf[5] == 0x34'u8

    putBe64(buf, 6, 0x0102030405060708'u64)
    check getBe64(buf, 6) == 0x0102030405060708'u64
    check buf[6] == 0x01'u8
    check buf[13] == 0x08'u8

  test "Little-Endian 16/32 bit encode and decode (CRIT-03)":
    var buf = newSeq[byte](8)
    putLe16(buf, 0, 0x1234'u16)
    check getLe16(buf, 0) == 0x1234'u16
    check buf[0] == 0x34'u8
    check buf[1] == 0x12'u8

    putLe32(buf, 2, 0x12345678'u32)
    check getLe32(buf, 2) == 0x12345678'u32
    check buf[2] == 0x78'u8
    check buf[3] == 0x56'u8
    check buf[4] == 0x34'u8
    check buf[5] == 0x12'u8

suite "WebSocket Framing Codec":
  test "Subprotocol lookup asymmetry: dialer sends peer, listener matches self (CRIT-02)":
    # REQ / REP
    check dialerSubprotocol(spReq0) == "rep.sp.nanomsg.org"
    check listenerSubprotocol(spReq0) == "req.sp.nanomsg.org"
    check dialerSubprotocol(spRep0) == "req.sp.nanomsg.org"
    check listenerSubprotocol(spRep0) == "rep.sp.nanomsg.org"

    # PUB / SUB
    check dialerSubprotocol(spPub0) == "sub.sp.nanomsg.org"
    check listenerSubprotocol(spPub0) == "pub.sp.nanomsg.org"
    check dialerSubprotocol(spSub0) == "pub.sp.nanomsg.org"
    check listenerSubprotocol(spSub0) == "sub.sp.nanomsg.org"

    # PAIR
    check dialerSubprotocol(spPair0) == "pair.sp.nanomsg.org"
    check listenerSubprotocol(spPair0) == "pair.sp.nanomsg.org"
    check dialerSubprotocol(spPair1) == "pair1.sp.nanomsg.org"
    check listenerSubprotocol(spPair1) == "pair1.sp.nanomsg.org"

    # PUSH / PULL
    check dialerSubprotocol(spPush0) == "pull.sp.nanomsg.org"
    check listenerSubprotocol(spPush0) == "push.sp.nanomsg.org"
    check dialerSubprotocol(spPull0) == "push.sp.nanomsg.org"
    check listenerSubprotocol(spPull0) == "pull.sp.nanomsg.org"

    # SURVEY
    check dialerSubprotocol(spSurveyor0) == "respondent.sp.nanomsg.org"
    check listenerSubprotocol(spSurveyor0) == "surveyor.sp.nanomsg.org"
    check dialerSubprotocol(spRespondent0) == "surveyor.sp.nanomsg.org"
    check listenerSubprotocol(spRespondent0) == "respondent.sp.nanomsg.org"

    # BUS
    check dialerSubprotocol(spBus0) == "bus.sp.nanomsg.org"
    check listenerSubprotocol(spBus0) == "bus.sp.nanomsg.org"

  test "packWsBinary packs header and body":
    let msg = initSpMsg(@[byte(10), byte(20)], @[byte(1), byte(2)])
    let packed = packWsBinary(msg)
    check packed == @[byte(1), byte(2), byte(10), byte(20)]

  test "validateWsOpcode accepts binary and control frames, rejects text":
    check validateWsOpcode(0x2'u8) # Binary
    check validateWsOpcode(0x8'u8) # Close
    check validateWsOpcode(0x9'u8) # Ping
    check validateWsOpcode(0xA'u8) # Pong
    check not validateWsOpcode(0x1'u8) # Text - rejected with 1003

suite "Stream Framing Codec (TCP / IPC)":
  test "encodeGreeting has byte 0 strictly 0x00 per CRIT-01 and nng/ipc.c:604":
    let g = encodeGreeting(spReq0)
    check g[0] == 0x00'u8
    check g[1] == byte('S')
    check g[2] == byte('P')
    check g[3] == 0x00'u8
    check getBe16(g, 4) == uint16(spReq0)
    check g[6] == 0x00'u8
    check g[7] == 0x00'u8

  test "verifyGreeting validates matching peer and rejects corrupt bytes":
    let g = encodeGreeting(spReq0) # Sender is REQ0
    # Expected peer for REP0 is REQ0:
    let res = verifyGreeting(g, spReq0)
    check res.ok
    check res.val == spReq0

    # Negative control: wrong byte 0 (e.g. 0x01)
    var bad0 = g
    bad0[0] = 0x01'u8
    check not verifyGreeting(bad0, spReq0).ok

    # Negative control: wrong magic
    var badMagic = g
    badMagic[1] = byte('X')
    check not verifyGreeting(badMagic, spReq0).ok

    # Negative control: protocol mismatch
    check not verifyGreeting(g, spPub0).ok

  test "encodeStreamFrame and StreamDecoder roundtrip":
    let msg = initSpMsg(@[byte('h'), byte('e'), byte('l'), byte('l'), byte('o')])
    let encoded = encodeStreamFrame(msg)
    check getBe64(encoded, 0) == 5'u64
    check encoded[8 .. ^1] == msg.body

    var dec = initStreamDecoder()
    var delivered: seq[seq[byte]] = @[]
    dec.feed(
      encoded,
      proc(f: seq[byte]) =
        delivered.add(f),
    )
    check delivered.len == 1
    check delivered[0] == msg.body

  test "StreamDecoder splits across chunks and handles multiple frames":
    var dec = initStreamDecoder()
    var delivered: seq[seq[byte]] = @[]

    let frame1 = encodeStreamFrame(initSpMsg(@[byte(1), byte(2), byte(3)]))
    let frame2 = encodeStreamFrame(initSpMsg(@[byte(4), byte(5)]))
    let combined = frame1 & frame2

    # Feed in arbitrary slice chunks
    dec.feed(
      combined[0 ..< 5],
      proc(f: seq[byte]) =
        delivered.add(f),
    )
    check delivered.len == 0
    dec.feed(
      combined[5 ..< 15],
      proc(f: seq[byte]) =
        delivered.add(f),
    )
    check delivered.len == 1
    check delivered[0] == @[byte(1), byte(2), byte(3)]

    dec.feed(
      combined[15 .. ^1],
      proc(f: seq[byte]) =
        delivered.add(f),
    )
    check delivered.len == 2
    check delivered[1] == @[byte(4), byte(5)]

  test "StreamDecoder enforces recvMax guard per HIGH-01":
    var dec = initStreamDecoder(recvMax = 64'u64)
    var bigFrame = newSeq[byte](8 + 100)
    putBe64(bigFrame, 0, 100'u64) # Exceeds 64-byte limit

    expect(ValueError):
      dec.feed(
        bigFrame,
        proc(f: seq[byte]) =
          discard,
      )

suite "NNG v2 UDP Datagram Framing Codec":
  test "UDP header uses Little-Endian for us_type and us_params (CRIT-03)":
    var hdr: UdpSpHeader
    hdr.ver = UdpSpVer
    hdr.opCode = UdpOpData
    hdr.protoType = 0x30'u16 # spReq0
    hdr.param0 = 0x0123'u16
    hdr.param1 = 0x0456'u16

    let bytes = encodeUdpSpHeader(hdr)
    check bytes[0] == UdpSpVer
    check bytes[1] == UdpOpData

    # Check Little-Endian byte representation
    check bytes[2] == 0x30'u8
    check bytes[3] == 0x00'u8
    check bytes[4] == 0x23'u8
    check bytes[5] == 0x01'u8
    check bytes[6] == 0x56'u8
    check bytes[7] == 0x04'u8

    let decoded = decodeUdpSpHeader(bytes)
    check decoded.ver == hdr.ver
    check decoded.opCode == hdr.opCode
    check decoded.protoType == hdr.protoType
    check decoded.param0 == hdr.param0
    check decoded.param1 == hdr.param1

  test "UDP data frame encode and decode":
    let msg = initSpMsg(@[byte('d'), byte('a'), byte('t'), byte('a')])
    let packet = encodeUdpDataFrame(spReq0, msg)
    let (hdr, payload) = decodeUdpDataFrame(packet)
    check hdr.ver == UdpSpVer
    check hdr.opCode == UdpOpData
    check hdr.protoType == uint16(spReq0)
    check payload == msg.body

suite "URL Parser":
  test "Parses inproc, ipc, tcp, udp, ws URLs":
    let u1 = parseSpUrl("inproc://my_channel")
    check u1.kind == tranInproc
    check u1.path == "my_channel"

    let u2 = parseSpUrl("ipc:///tmp/anynng.ipc")
    check u2.kind == tranIpc
    check u2.path == "/tmp/anynng.ipc"

    let u3 = parseSpUrl("tcp://127.0.0.1:5555")
    check u3.kind == tranTcp
    check u3.host == "127.0.0.1"
    check u3.port == 5555

    let u4 = parseSpUrl("udp://127.0.0.1:6666")
    check u4.kind == tranUdp
    check u4.host == "127.0.0.1"
    check u4.port == 6666

    let u5 = parseSpUrl("ws://localhost:8080/sp")
    check u5.kind == tranWs
    check u5.host == "localhost"
    check u5.port == 8080
    check u5.path == "/sp"
    check not u5.isSecure

    let u6 = parseSpUrl("wss://secure.example.com/sp")
    check u6.kind == tranWs
    check u6.host == "secure.example.com"
    check u6.port == 443
    check u6.isSecure
