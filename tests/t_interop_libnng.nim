# t_interop_libnng.nim - Full live interop test suite against C libnng 1.12.4
# Licensed under MIT License.

when not defined(js):
  import std/[unittest, os, asyncdispatch, posix]
  import anynng

  const LibNng = "/opt/homebrew/opt/nng/lib/libnng.dylib"
  const NNG_FLAG_NONBLOCK = 2'i32

  type NngSocket = object
    id: uint32

  proc nng_bus0_open(s: ptr NngSocket): cint {.importc, dynlib: LibNng.}
  proc nng_pair0_open(s: ptr NngSocket): cint {.importc, dynlib: LibNng.}
  proc nng_pull0_open(s: ptr NngSocket): cint {.importc, dynlib: LibNng.}
  proc nng_push0_open(s: ptr NngSocket): cint {.importc, dynlib: LibNng.}
  proc nng_pub0_open(s: ptr NngSocket): cint {.importc, dynlib: LibNng.}
  proc nng_sub0_open(s: ptr NngSocket): cint {.importc, dynlib: LibNng.}
  proc nng_sub0_socket_subscribe(
    s: NngSocket, topic: pointer, sz: csize_t
  ): cint {.importc, dynlib: LibNng.}

  proc nng_rep0_open(s: ptr NngSocket): cint {.importc, dynlib: LibNng.}
  proc nng_req0_open(s: ptr NngSocket): cint {.importc, dynlib: LibNng.}
  proc nng_respondent0_open(s: ptr NngSocket): cint {.importc, dynlib: LibNng.}
  proc nng_surveyor0_open(s: ptr NngSocket): cint {.importc, dynlib: LibNng.}

  proc nng_listen(
    s: NngSocket, url: cstring, lp: pointer, flags: cint
  ): cint {.importc, dynlib: LibNng.}

  proc nng_dial(
    s: NngSocket, url: cstring, dp: pointer, flags: cint
  ): cint {.importc, dynlib: LibNng.}

  proc nng_send(
    s: NngSocket, data: pointer, size: csize_t, flags: cint
  ): cint {.importc, dynlib: LibNng.}

  proc nng_recv(
    s: NngSocket, data: pointer, size: ptr csize_t, flags: cint
  ): cint {.importc, dynlib: LibNng.}

  proc nng_socket_set_ms(
    s: NngSocket, opt: cstring, val: int32
  ): cint {.importc, dynlib: LibNng.}

  proc nng_close(s: NngSocket): cint {.importc, dynlib: LibNng.}
  proc nng_version(): cstring {.importc, dynlib: LibNng.}

  proc flushEvents(count: int = 5, ms: int = 10) =
    for _ in 0 ..< count:
      try:
        poll(ms)
      except ValueError:
        break

  suite "libnng C Interop - TCP (tcp://)":
    test "PAIR0: anynng dialer -> libnng listener":
      echo "[libnng Interop] Linked against libnng version: ", nng_version()
      let url = "tcp://127.0.0.1:16901"
      var cSock: NngSocket
      check nng_pair0_open(addr cSock) == 0
      check nng_socket_set_ms(cSock, "recv-timeout", 2000) == 0
      check nng_listen(cSock, cstring(url), nil, 0) == 0

      let anySock = openPair0()
      anySock.dial(url)
      flushEvents()

      anySock.send("ping from anynng over tcp")
      flushEvents()

      var buf = newString(256)
      var sz: csize_t = 256
      check nng_recv(cSock, addr buf[0], addr sz, 0) == 0
      buf.setLen(sz)
      check buf == "ping from anynng over tcp"

      let reply = "pong from libnng over tcp"
      check nng_send(cSock, cstring(reply), csize_t(reply.len), 0) == 0
      let rep = anySock.recvStr()
      check rep == reply

      anySock.close()
      discard nng_close(cSock)

    test "PAIR0: libnng dialer -> anynng listener":
      let url = "tcp://127.0.0.1:16902"
      let anySock = openPair0()
      let listener = anySock.listen(url)

      var cSock: NngSocket
      check nng_pair0_open(addr cSock) == 0
      check nng_socket_set_ms(cSock, "recv-timeout", 2000) == 0
      check nng_dial(cSock, cstring(url), nil, NNG_FLAG_NONBLOCK) == 0

      var s1: cint = -1
      let msg = "ping from libnng dialer"
      for _ in 0 ..< 20:
        poll(10)
        s1 = nng_send(cSock, cstring(msg), csize_t(msg.len), NNG_FLAG_NONBLOCK)
        if s1 == 0:
          break
      check s1 == 0

      let recvMsg = anySock.recvStr()
      check recvMsg == msg

      let reply = "pong from anynng listener"
      anySock.send(reply)
      poll(10)

      var buf = newString(256)
      var sz: csize_t = 256
      check nng_recv(cSock, addr buf[0], addr sz, 0) == 0
      buf.setLen(sz)
      check buf == reply

      anySock.close()
      listener.close()
      discard nng_close(cSock)

    test "REQ0 / REP0: anynng REQ dialer -> libnng REP listener":
      let url = "tcp://127.0.0.1:16903"
      var repSock: NngSocket
      check nng_rep0_open(addr repSock) == 0
      check nng_socket_set_ms(repSock, "recv-timeout", 2000) == 0
      check nng_listen(repSock, cstring(url), nil, 0) == 0

      let reqSock = openReq0()
      reqSock.dial(url)
      flushEvents()

      reqSock.send("request: compute-hash")
      flushEvents()

      var buf = newString(256)
      var sz: csize_t = 256
      check nng_recv(repSock, addr buf[0], addr sz, 0) == 0
      buf.setLen(sz)
      check buf == "request: compute-hash"

      let reply = "response: hash=0xDEADBEEF"
      check nng_send(repSock, cstring(reply), csize_t(reply.len), 0) == 0
      let rep = reqSock.recvStr()
      check rep == reply

      reqSock.close()
      discard nng_close(repSock)

    test "REQ0 / REP0: libnng REQ dialer -> anynng REP listener":
      let url = "tcp://127.0.0.1:16904"
      let repSock = openRep0()
      let listener = repSock.listen(url)

      var reqSock: NngSocket
      check nng_req0_open(addr reqSock) == 0
      check nng_socket_set_ms(reqSock, "recv-timeout", 2000) == 0
      check nng_dial(reqSock, cstring(url), nil, NNG_FLAG_NONBLOCK) == 0

      var s1: cint = -1
      let reqMsg = "request: libnng-job"
      for _ in 0 ..< 20:
        poll(10)
        s1 = nng_send(reqSock, cstring(reqMsg), csize_t(reqMsg.len), NNG_FLAG_NONBLOCK)
        if s1 == 0:
          break
      check s1 == 0

      let recvReq = repSock.recvStr()
      check recvReq == reqMsg

      let repMsg = "response: anynng-done"
      repSock.send(repMsg)
      poll(10)

      var buf = newString(256)
      var sz: csize_t = 256
      check nng_recv(reqSock, addr buf[0], addr sz, 0) == 0
      buf.setLen(sz)
      check buf == repMsg

      repSock.close()
      listener.close()
      discard nng_close(reqSock)

    test "PUB0 / SUB0: anynng PUB listener -> libnng SUB dialer with topic filter":
      let url = "tcp://127.0.0.1:16905"
      let pubSock = openPub0()
      let listener = pubSock.listen(url)

      var subSock: NngSocket
      check nng_sub0_open(addr subSock) == 0
      check nng_socket_set_ms(subSock, "recv-timeout", 2000) == 0
      let topic = "market."
      check nng_sub0_socket_subscribe(subSock, cstring(topic), csize_t(topic.len)) == 0
      check nng_dial(subSock, cstring(url), nil, NNG_FLAG_NONBLOCK) == 0

      # Wait for connection to establish
      for _ in 0 ..< 15:
        poll(10)

      # Send filtered and matching messages
      pubSock.send("weather.rain: high")
      pubSock.send("market.stocks: AAPL +2%")
      pubSock.send("market.crypto: BTC +5%")
      poll(10)

      var buf = newString(256)
      var sz: csize_t = 256
      check nng_recv(subSock, addr buf[0], addr sz, 0) == 0
      buf.setLen(sz)
      check buf == "market.stocks: AAPL +2%"

      sz = 256
      check nng_recv(subSock, addr buf[0], addr sz, 0) == 0
      buf.setLen(sz)
      check buf == "market.crypto: BTC +5%"

      pubSock.close()
      listener.close()
      discard nng_close(subSock)

    test "PUB0 / SUB0: libnng PUB listener -> anynng SUB dialer with topic filter":
      let url = "tcp://127.0.0.1:16915"
      var pubSock: NngSocket
      check nng_pub0_open(addr pubSock) == 0
      check nng_listen(pubSock, cstring(url), nil, 0) == 0

      let subSock = openSub0()
      subSock.subscribe("news.")
      subSock.dial(url)
      flushEvents(count = 10)

      # Send filtered and matching messages from libnng
      let m1 = "sports.scores: 3-1"
      let m2 = "news.headline: breaking"
      discard nng_send(pubSock, cstring(m1), csize_t(m1.len), 0)
      discard nng_send(pubSock, cstring(m2), csize_t(m2.len), 0)

      let got = subSock.recvStr()
      check got == m2

      subSock.close()
      discard nng_close(pubSock)

    test "PUSH0 / PULL0: anynng PUSH dialer -> libnng PULL listener":
      let url = "tcp://127.0.0.1:16906"
      var pullSock: NngSocket
      check nng_pull0_open(addr pullSock) == 0
      check nng_socket_set_ms(pullSock, "recv-timeout", 2000) == 0
      check nng_listen(pullSock, cstring(url), nil, 0) == 0

      let pushSock = openPush0()
      pushSock.dial(url)
      flushEvents()

      pushSock.send("pipeline-packet-1")
      pushSock.send("pipeline-packet-2")
      flushEvents()

      var buf = newString(256)
      var sz: csize_t = 256
      check nng_recv(pullSock, addr buf[0], addr sz, 0) == 0
      buf.setLen(sz)
      check buf == "pipeline-packet-1"

      sz = 256
      check nng_recv(pullSock, addr buf[0], addr sz, 0) == 0
      buf.setLen(sz)
      check buf == "pipeline-packet-2"

      pushSock.close()
      discard nng_close(pullSock)

    test "PUSH0 / PULL0: libnng PUSH dialer -> anynng PULL listener":
      let url = "tcp://127.0.0.1:16916"
      let pullSock = openPull0()
      let listener = pullSock.listen(url)

      var pushSock: NngSocket
      check nng_push0_open(addr pushSock) == 0
      check nng_dial(pushSock, cstring(url), nil, NNG_FLAG_NONBLOCK) == 0

      var s1: cint = -1
      let pkt = "pipeline-packet-from-libnng"
      for _ in 0 ..< 20:
        poll(10)
        s1 = nng_send(pushSock, cstring(pkt), csize_t(pkt.len), NNG_FLAG_NONBLOCK)
        if s1 == 0:
          break
      check s1 == 0

      let got = pullSock.recvStr()
      check got == pkt

      pullSock.close()
      listener.close()
      discard nng_close(pushSock)

    test "SURVEYOR0 / RESPONDENT0: anynng SURVEYOR dialer -> libnng RESPONDENT listener":
      let url = "tcp://127.0.0.1:16907"
      var respSock: NngSocket
      check nng_respondent0_open(addr respSock) == 0
      check nng_socket_set_ms(respSock, "recv-timeout", 2000) == 0
      check nng_listen(respSock, cstring(url), nil, 0) == 0

      let survSock = openSurveyor0()
      survSock.dial(url)
      flushEvents()

      survSock.send("survey: service-status")
      flushEvents()

      var buf = newString(256)
      var sz: csize_t = 256
      check nng_recv(respSock, addr buf[0], addr sz, 0) == 0
      buf.setLen(sz)
      check buf == "survey: service-status"

      let rep = "service: ok"
      check nng_send(respSock, cstring(rep), csize_t(rep.len), 0) == 0
      let ans = survSock.recvStr()
      check ans == rep

      survSock.close()
      discard nng_close(respSock)

    test "SURVEYOR0 / RESPONDENT0: libnng SURVEYOR dialer -> anynng RESPONDENT listener":
      let url = "tcp://127.0.0.1:16917"
      let respSock = openRespondent0()
      let listener = respSock.listen(url)

      var survSock: NngSocket
      check nng_surveyor0_open(addr survSock) == 0
      check nng_socket_set_ms(survSock, "recv-timeout", 2000) == 0
      check nng_dial(survSock, cstring(url), nil, NNG_FLAG_NONBLOCK) == 0

      # Wait for surveyor dialer connection to establish and handshake
      flushEvents(count = 10)
      sleep(50)
      flushEvents(count = 5)

      let surveyQuery = "survey: cluster-health"
      check nng_send(survSock, cstring(surveyQuery), csize_t(surveyQuery.len), 0) == 0

      let got = respSock.recvStr()
      check got == surveyQuery

      let surveyReply = "health: 100%"
      respSock.send(surveyReply)
      poll(10)

      var buf = newString(256)
      var sz: csize_t = 256
      check nng_recv(survSock, addr buf[0], addr sz, 0) == 0
      buf.setLen(sz)
      check buf == surveyReply

      respSock.close()
      listener.close()
      discard nng_close(survSock)

    test "BUS0: anynng BUS dialer <-> libnng BUS listener":
      let url = "tcp://127.0.0.1:16908"
      var cBus: NngSocket
      check nng_bus0_open(addr cBus) == 0
      check nng_socket_set_ms(cBus, "recv-timeout", 2000) == 0
      check nng_listen(cBus, cstring(url), nil, 0) == 0

      let anyBus = openBus0()
      anyBus.dial(url)
      flushEvents()

      anyBus.send("bus msg from anynng")
      flushEvents()

      var buf = newString(256)
      var sz: csize_t = 256
      check nng_recv(cBus, addr buf[0], addr sz, 0) == 0
      buf.setLen(sz)
      check buf == "bus msg from anynng"

      let reply = "bus msg from libnng"
      check nng_send(cBus, cstring(reply), csize_t(reply.len), 0) == 0
      let rep = anyBus.recvStr()
      check rep == reply

      anyBus.close()
      discard nng_close(cBus)

  suite "libnng C Interop - IPC (ipc://)":
    test "PAIR0: anynng dialer -> libnng listener":
      let path = "/tmp/anynng_interop_ipc1.sock"
      let url = "ipc://" & path
      discard unlink(cstring(path))

      var cSock: NngSocket
      check nng_pair0_open(addr cSock) == 0
      check nng_socket_set_ms(cSock, "recv-timeout", 2000) == 0
      check nng_listen(cSock, cstring(url), nil, 0) == 0

      let anySock = openPair0()
      anySock.dial(url)
      flushEvents()

      anySock.send("hello over ipc to libnng")
      flushEvents()

      var buf = newString(256)
      var sz: csize_t = 256
      check nng_recv(cSock, addr buf[0], addr sz, 0) == 0
      buf.setLen(sz)
      check buf == "hello over ipc to libnng"

      let reply = "reply over ipc from libnng"
      check nng_send(cSock, cstring(reply), csize_t(reply.len), 0) == 0
      let rep = anySock.recvStr()
      check rep == reply

      anySock.close()
      discard nng_close(cSock)
      discard unlink(cstring(path))

    test "PAIR0: libnng dialer -> anynng listener":
      let path = "/tmp/anynng_interop_ipc2.sock"
      let url = "ipc://" & path
      discard unlink(cstring(path))

      let anySock = openPair0()
      let listener = anySock.listen(url)

      var cSock: NngSocket
      check nng_pair0_open(addr cSock) == 0
      check nng_socket_set_ms(cSock, "recv-timeout", 2000) == 0
      check nng_dial(cSock, cstring(url), nil, NNG_FLAG_NONBLOCK) == 0

      var s1: cint = -1
      let msg = "libnng ipc dialer payload"
      for _ in 0 ..< 20:
        poll(10)
        s1 = nng_send(cSock, cstring(msg), csize_t(msg.len), NNG_FLAG_NONBLOCK)
        if s1 == 0:
          break
      check s1 == 0

      let recvMsg = anySock.recvStr()
      check recvMsg == msg

      let reply = "anynng ipc listener response"
      anySock.send(reply)
      poll(10)

      var buf = newString(256)
      var sz: csize_t = 256
      check nng_recv(cSock, addr buf[0], addr sz, 0) == 0
      buf.setLen(sz)
      check buf == reply

      anySock.close()
      listener.close()
      discard nng_close(cSock)
      var st: Stat
      check posix.stat(cstring(path), st) != 0

    test "REQ0 / REP0: anynng REQ dialer -> libnng REP listener":
      let path = "/tmp/anynng_interop_ipc3.sock"
      let url = "ipc://" & path
      discard unlink(cstring(path))

      var repSock: NngSocket
      check nng_rep0_open(addr repSock) == 0
      check nng_socket_set_ms(repSock, "recv-timeout", 2000) == 0
      check nng_listen(repSock, cstring(url), nil, 0) == 0

      let reqSock = openReq0()
      reqSock.dial(url)
      flushEvents()

      reqSock.send("ipc request")
      flushEvents()

      var buf = newString(256)
      var sz: csize_t = 256
      check nng_recv(repSock, addr buf[0], addr sz, 0) == 0
      buf.setLen(sz)
      check buf == "ipc request"

      let reply = "ipc response"
      check nng_send(repSock, cstring(reply), csize_t(reply.len), 0) == 0
      let rep = reqSock.recvStr()
      check rep == reply

      reqSock.close()
      discard nng_close(repSock)
      discard unlink(cstring(path))

    test "REQ0 / REP0: libnng REQ dialer -> anynng REP listener":
      let path = "/tmp/anynng_interop_ipc3_rev.sock"
      let url = "ipc://" & path
      discard unlink(cstring(path))

      let repSock = openRep0()
      let listener = repSock.listen(url)

      var reqSock: NngSocket
      check nng_req0_open(addr reqSock) == 0
      check nng_socket_set_ms(reqSock, "recv-timeout", 2000) == 0
      check nng_dial(reqSock, cstring(url), nil, NNG_FLAG_NONBLOCK) == 0

      var s1: cint = -1
      let reqMsg = "ipc req query from libnng"
      for _ in 0 ..< 20:
        poll(10)
        s1 = nng_send(reqSock, cstring(reqMsg), csize_t(reqMsg.len), NNG_FLAG_NONBLOCK)
        if s1 == 0:
          break
      check s1 == 0

      let recvReq = repSock.recvStr()
      check recvReq == reqMsg

      let repMsg = "ipc rep answer from anynng"
      repSock.send(repMsg)
      poll(10)

      var buf = newString(256)
      var sz: csize_t = 256
      check nng_recv(reqSock, addr buf[0], addr sz, 0) == 0
      buf.setLen(sz)
      check buf == repMsg

      repSock.close()
      listener.close()
      discard nng_close(reqSock)
      discard unlink(cstring(path))

    test "PUSH0 / PULL0: anynng PUSH dialer -> libnng PULL listener":
      let path = "/tmp/anynng_interop_ipc4.sock"
      let url = "ipc://" & path
      discard unlink(cstring(path))

      var pullSock: NngSocket
      check nng_pull0_open(addr pullSock) == 0
      check nng_socket_set_ms(pullSock, "recv-timeout", 2000) == 0
      check nng_listen(pullSock, cstring(url), nil, 0) == 0

      let pushSock = openPush0()
      pushSock.dial(url)
      flushEvents()

      pushSock.send("ipc pipeline message")
      flushEvents()

      var buf = newString(256)
      var sz: csize_t = 256
      check nng_recv(pullSock, addr buf[0], addr sz, 0) == 0
      buf.setLen(sz)
      check buf == "ipc pipeline message"

      pushSock.close()
      discard nng_close(pullSock)

    test "PUSH0 / PULL0: libnng PUSH dialer -> anynng PULL listener":
      let path = "/tmp/anynng_interop_ipc4_rev.sock"
      let url = "ipc://" & path
      discard unlink(cstring(path))

      let pullSock = openPull0()
      let listener = pullSock.listen(url)

      var pushSock: NngSocket
      check nng_push0_open(addr pushSock) == 0
      check nng_socket_set_ms(pushSock, "recv-timeout", 2000) == 0
      check nng_dial(pushSock, cstring(url), nil, NNG_FLAG_NONBLOCK) == 0

      var s1: cint = -1
      let pkt = "pipeline-packet-from-libnng-over-ipc"
      for _ in 0 ..< 20:
        poll(10)
        s1 = nng_send(pushSock, cstring(pkt), csize_t(pkt.len), NNG_FLAG_NONBLOCK)
        if s1 == 0:
          break
      check s1 == 0

      let got = pullSock.recvStr()
      check got == pkt

      pullSock.close()
      listener.close()
      discard nng_close(pushSock)
      discard unlink(cstring(path))

  suite "libnng C Interop - RFC 6455 WebSocket (ws://)":
    test "PAIR0: anynng dialer -> libnng listener":
      let url = "ws://127.0.0.1:16911/ws"
      var cSock: NngSocket
      check nng_pair0_open(addr cSock) == 0
      check nng_socket_set_ms(cSock, "recv-timeout", 2000) == 0
      check nng_listen(cSock, cstring(url), nil, 0) == 0

      let anySock = openPair0()
      anySock.dial(url)
      flushEvents()

      anySock.send("ping over ws to libnng")
      flushEvents()

      var buf = newString(256)
      var sz: csize_t = 256
      check nng_recv(cSock, addr buf[0], addr sz, 0) == 0
      buf.setLen(sz)
      check buf == "ping over ws to libnng"

      let reply = "pong over ws from libnng"
      check nng_send(cSock, cstring(reply), csize_t(reply.len), 0) == 0
      let rep = anySock.recvStr()
      check rep == reply

      anySock.close()
      discard nng_close(cSock)

    test "PAIR0: libnng dialer -> anynng listener":
      let url = "ws://127.0.0.1:16912/ws"
      let anySock = openPair0()
      let listener = anySock.listen(url)

      var cSock: NngSocket
      check nng_pair0_open(addr cSock) == 0
      check nng_socket_set_ms(cSock, "recv-timeout", 2000) == 0
      check nng_dial(cSock, cstring(url), nil, NNG_FLAG_NONBLOCK) == 0

      var s1: cint = -1
      let msg = "libnng ws dialer payload"
      for _ in 0 ..< 50:
        poll(10)
        s1 = nng_send(cSock, cstring(msg), csize_t(msg.len), NNG_FLAG_NONBLOCK)
        if s1 == 0:
          break
        sleep(20)
      check s1 == 0

      let recvMsg = anySock.recvStr()
      check recvMsg == msg

      let reply = "anynng ws listener response"
      anySock.send(reply)
      poll(10)

      var buf = newString(256)
      var sz: csize_t = 256
      check nng_recv(cSock, addr buf[0], addr sz, 0) == 0
      buf.setLen(sz)
      check buf == reply

      anySock.close()
      listener.close()
      discard nng_close(cSock)

    test "REQ0 / REP0: anynng REQ dialer -> libnng REP listener":
      let url = "ws://127.0.0.1:16913/ws"
      var repSock: NngSocket
      check nng_rep0_open(addr repSock) == 0
      check nng_socket_set_ms(repSock, "recv-timeout", 2000) == 0
      check nng_listen(repSock, cstring(url), nil, 0) == 0

      let reqSock = openReq0()
      reqSock.dial(url)
      flushEvents()

      reqSock.send("ws req query")
      flushEvents()

      var buf = newString(256)
      var sz: csize_t = 256
      check nng_recv(repSock, addr buf[0], addr sz, 0) == 0
      buf.setLen(sz)
      check buf == "ws req query"

      let reply = "ws rep answer"
      check nng_send(repSock, cstring(reply), csize_t(reply.len), 0) == 0
      let rep = reqSock.recvStr()
      check rep == reply

      reqSock.close()
      discard nng_close(repSock)

    test "REQ0 / REP0: libnng REQ dialer -> anynng REP listener":
      let url = "ws://127.0.0.1:16914/ws"
      let repSock = openRep0()
      let listener = repSock.listen(url)

      var reqSock: NngSocket
      check nng_req0_open(addr reqSock) == 0
      check nng_socket_set_ms(reqSock, "recv-timeout", 2000) == 0
      check nng_dial(reqSock, cstring(url), nil, NNG_FLAG_NONBLOCK) == 0

      var s1: cint = -1
      let reqMsg = "ws req query from libnng"
      for _ in 0 ..< 50:
        poll(10)
        s1 = nng_send(reqSock, cstring(reqMsg), csize_t(reqMsg.len), NNG_FLAG_NONBLOCK)
        if s1 == 0:
          break
        sleep(20)
      check s1 == 0

      let recvReq = repSock.recvStr()
      check recvReq == reqMsg

      let repMsg = "ws rep answer from anynng"
      repSock.send(repMsg)
      poll(10)

      var buf = newString(256)
      var sz: csize_t = 256
      check nng_recv(reqSock, addr buf[0], addr sz, 0) == 0
      buf.setLen(sz)
      check buf == repMsg

      repSock.close()
      listener.close()
      discard nng_close(reqSock)
