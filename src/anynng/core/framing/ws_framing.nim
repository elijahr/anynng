# ws_framing.nim - SP-over-WebSocket framing & Sec-WebSocket-Protocol lookup
# Licensed under MIT License.

import ../types

const
  WsCloseNormal* = 1000'u16
  WsCloseProtocolErr* = 1002'u16
  WsCloseUnsuppFormat* = 1003'u16

func dialerSubprotocol*(proto: SpProto): string =
  ## Returns the Sec-WebSocket-Protocol sent by SP dialers (clients).
  ## Per sp-websocket-mapping-01 §2 and nng/websocket.c:561, the dialer MUST send its PEER's protocol name!
  case proto
  of spPair0: "pair.sp.nanomsg.org"
  of spPair1: "pair1.sp.nanomsg.org"
  of spPub0: "sub.sp.nanomsg.org"
  of spSub0: "pub.sp.nanomsg.org"
  of spReq0: "rep.sp.nanomsg.org"
  of spRep0: "req.sp.nanomsg.org"
  of spPush0: "pull.sp.nanomsg.org"
  of spPull0: "push.sp.nanomsg.org"
  of spSurveyor0: "respondent.sp.nanomsg.org"
  of spRespondent0: "surveyor.sp.nanomsg.org"
  of spBus0: "bus.sp.nanomsg.org"

func listenerSubprotocol*(proto: SpProto): string =
  ## Returns the Sec-WebSocket-Protocol expected by SP listeners (servers).
  ## Per nng/websocket.c:592, the listener matches its OWN (self) protocol name!
  case proto
  of spPair0: "pair.sp.nanomsg.org"
  of spPair1: "pair1.sp.nanomsg.org"
  of spPub0: "pub.sp.nanomsg.org"
  of spSub0: "sub.sp.nanomsg.org"
  of spReq0: "req.sp.nanomsg.org"
  of spRep0: "rep.sp.nanomsg.org"
  of spPush0: "push.sp.nanomsg.org"
  of spPull0: "pull.sp.nanomsg.org"
  of spSurveyor0: "surveyor.sp.nanomsg.org"
  of spRespondent0: "respondent.sp.nanomsg.org"
  of spBus0: "bus.sp.nanomsg.org"

func packWsBinary*(msg: SpMsg): seq[byte] =
  ## Packs an SP message (header || body) into a single WebSocket binary frame payload.
  result = newSeq[byte](msg.header.len + msg.body.len)
  if msg.header.len > 0:
    for i in 0 ..< msg.header.len:
      result[i] = msg.header[i]
  if msg.body.len > 0:
    for i in 0 ..< msg.body.len:
      result[msg.header.len + i] = msg.body[i]

func validateWsOpcode*(opcode: byte): bool =
  ## SP sockets operate strictly in binary message mode (opcode 0x2).
  ## Opcode 0x1 (text) is rejected with close code 1003.
  opcode == 0x2 or opcode == 0x8 or opcode == 0x9 or opcode == 0xA
