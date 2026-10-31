# t_native_transports.nim - Live native loopback tests for TCP, IPC, UDP, and WS
# Licensed under MIT License.

when not defined(js):
  import std/[unittest, os, net, strutils, asyncdispatch, asyncnet, posix]
  import anynng
  import anynng/core/framing/ws_framing

  suite "Native TCP Transport (tcp://)":
    test "PAIR0 TCP loopback point-to-point":
      let url = "tcp://127.0.0.1:15511"
      let lSock = openPair0()
      let listener = lSock.listen(url)

      let dSock = openPair0()
      dSock.dial(url)

      dSock.send("hello over tcp from dialer")
      let msg1 = lSock.recvStr()
      check msg1 == "hello over tcp from dialer"

      lSock.send("reply over tcp from listener")
      let msg2 = dSock.recvStr()
      check msg2 == "reply over tcp from listener"

      dSock.close()
      listener.close()

    test "REQ0 and REP0 TCP request-reply":
      let url = "tcp://127.0.0.1:15512"
      let repSock = openRep0()
      let listener = repSock.listen(url)

      let reqSock = openReq0()
      reqSock.dial(url)

      reqSock.send("ping tcp")
      let req = repSock.recvStr()
      check req == "ping tcp"

      repSock.send("pong tcp")
      let rep = reqSock.recvStr()
      check rep == "pong tcp"

      reqSock.close()
      listener.close()

    test "PUB0 and SUB0 TCP broadcast with topic filtering (WARN-02)":
      let url = "tcp://127.0.0.1:15513"
      let pubSock = openPub0()
      let listener = pubSock.listen(url)

      let subSock = openSub0()
      subSock.subscribe("weather.")
      subSock.dial(url)

      # Allow connection to settle
      sleep(50)

      pubSock.send("sports.football: goal!")
      pubSock.send("weather.rain: precipitation 80%")
      pubSock.send("weather.sun: clear skies")

      let msg1 = subSock.recvStr()
      check msg1 == "weather.rain: precipitation 80%"

      let msg2 = subSock.recvStr()
      check msg2 == "weather.sun: clear skies"

      subSock.close()
      listener.close()

  suite "Native IPC Transport (ipc://)":
    test "PAIR0 IPC loopback via Unix domain socket":
      let path = "/tmp/anynng_test_ipc_loopback.sock"
      let url = "ipc://" & path
      let lSock = openPair0()
      let listener = lSock.listen(url)

      let dSock = openPair0()
      dSock.dial(url)

      dSock.send("ipc payload alpha")
      let msg1 = lSock.recvStr()
      check msg1 == "ipc payload alpha"

      lSock.send("ipc payload beta")
      let msg2 = dSock.recvStr()
      check msg2 == "ipc payload beta"

      dSock.close()
      listener.close()
      var st: Stat
      check posix.stat(cstring(path), st) != 0

    test "REQ0 and REP0 IPC request-reply":
      let path = "/tmp/anynng_test_ipc_reqrep.sock"
      let url = "ipc://" & path
      let repSock = openRep0()
      let listener = repSock.listen(url)

      let reqSock = openReq0()
      reqSock.dial(url)

      reqSock.send("ping ipc")
      let req = repSock.recvStr()
      check req == "ping ipc"

      repSock.send("pong ipc")
      let rep = reqSock.recvStr()
      check rep == "pong ipc"

      reqSock.close()
      listener.close()
      var st: Stat
      check posix.stat(cstring(path), st) != 0

  suite "Native NNG v2 UDP Transport (udp://)":
    test "PUSH0 and PULL0 UDP datagram transport":
      let url = "udp://127.0.0.1:15514"
      let pullSock = openPull0()
      let listener = pullSock.listen(url)

      let pushSock = openPush0()
      pushSock.dial(url)

      pushSock.send("udp pipeline packet 1")
      pushSock.send("udp pipeline packet 2")

      let msg1 = pullSock.recvStr()
      check msg1 == "udp pipeline packet 1"

      let msg2 = pullSock.recvStr()
      check msg2 == "udp pipeline packet 2"

      pushSock.close()
      listener.close()

  suite "Native RFC 6455 WebSocket Transport (ws://)":
    test "PAIR0 WebSocket loopback with subprotocol matching":
      let url = "ws://127.0.0.1:15515/ws"
      let lSock = openPair0()
      let listener = lSock.listen(url)

      let dSock = openPair0()
      dSock.dial(url)

      dSock.send("websocket payload dialer")
      let msg1 = lSock.recvStr()
      check msg1 == "websocket payload dialer"

      lSock.send("websocket payload listener")
      let msg2 = dSock.recvStr()
      check msg2 == "websocket payload listener"

      dSock.close()
      listener.close()

    test "REQ0 and REP0 WebSocket request-reply":
      let url = "ws://127.0.0.1:15517/ws"
      let repSock = openRep0()
      let listener = repSock.listen(url)

      let reqSock = openReq0()
      reqSock.dial(url)

      reqSock.send("ping over ws")
      let req = repSock.recvStr()
      check req == "ping over ws"

      repSock.send("pong over ws")
      let rep = reqSock.recvStr()
      check rep == "pong over ws"

      reqSock.close()
      listener.close()

    test "WebSocket rejects text frame opcode 0x1 with close code 1003 (CRIT-02)":
      let url = "ws://127.0.0.1:15516/ws"
      let lSock = openPair0()
      let listener = lSock.listen(url)

      proc runRawClient(): Future[void] {.async.} =
        let client = newAsyncSocket(buffered = false)
        await client.connect("127.0.0.1", Port(15516))

        let req =
          "GET /ws HTTP/1.1\r\n" & "Host: 127.0.0.1:15516\r\n" & "Upgrade: websocket\r\n" &
          "Connection: Upgrade\r\n" & "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n" &
          "Sec-WebSocket-Version: 13\r\n" &
          "Sec-WebSocket-Protocol: pair.sp.nanomsg.org\r\n\r\n"
        await client.send(req)

        # Read 101 Switching Protocols
        let resp = await client.recv(256)
        check resp.contains("101 Switching Protocols")

        # Send unmasked text frame: opcode 0x1, length 4, "text"
        # byte 0: 0x81 (FIN=1, opcode=1)
        # byte 1: 0x04 (MASK=0, len=4)
        # "text"
        let textFrame = "\x81\x04text"
        await client.send(textFrame)

        # Server must respond with Close frame opcode 0x8 and code 1003 (0x03EB)
        let closeHdr = await client.recv(2)
        check closeHdr.len == 2
        let b0 = byte(closeHdr[0])
        check (b0 and 0x0f'u8) == 0x8'u8 # Close frame opcode

        let closePayload = await client.recv(2)
        check closePayload.len == 2
        let closeCode =
          (uint16(byte(closePayload[0])) shl 8) or uint16(byte(closePayload[1]))
        check closeCode == WsCloseUnsuppFormat # 1003

        client.close()

      waitFor runRawClient()
      listener.close()
