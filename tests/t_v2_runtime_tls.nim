# t_v2_runtime_tls.nim - Full NNG Core Runtime & Native OpenSSL 3 TLS Integration Suite
# Licensed under MIT License.

when not defined(js):
  import std/[unittest, os, net, strutils, asyncdispatch, asyncnet]
  import anynng

  proc bytesToStr(b: openArray[byte]): string =
    result = newString(b.len)
    for i in 0 ..< b.len:
      result[i] = char(b[i])

  suite "NNG Strongly-Typed Option Registry & Error Semantics":
    test "Valid get and set on Socket options":
      let sock = openReq0()
      sock.setOpt(NNG_OPT_MAXTTL, 16)
      check sock.getInt(NNG_OPT_MAXTTL) == 16

      sock.setOpt(NNG_OPT_RECVTIMEO, 2500'i64)
      check sock.getMs(NNG_OPT_RECVTIMEO) == 2500'i64

      sock.setOpt(NNG_OPT_TCP_NODELAY, false)
      check sock.getBool(NNG_OPT_TCP_NODELAY) == false

      sock.close()

    test "Error NNG_EBADTYPE (30) on mismatched option type":
      let sock = openPair0()
      var caught = false
      try:
        discard sock.getInt(NNG_OPT_PROTONAME)
      except NngError as e:
        check e.errCode == NNG_EBADTYPE
        caught = true
      check caught
      sock.close()

    test "Error NNG_EREADONLY (24) on attempt to set read-only option":
      let sock = openPair0()
      var caught = false
      try:
        sock.setOpt(NNG_OPT_PROTO, 99)
      except NngError as e:
        check e.errCode == NNG_EREADONLY
        caught = true
      check caught
      sock.close()

    test "Error NNG_EWRITEONLY (25) on attempt to get write-only option":
      let sock = openSub0()
      var caught = false
      try:
        discard sock.getString(NNG_OPT_SUB_SUBSCRIBE)
      except NngError as e:
        check e.errCode == NNG_EWRITEONLY
        caught = true
      check caught
      sock.close()

    test "Error NNG_ENOTSUP (9) on unsupported option":
      let sock = openPair0()
      var caught = false
      try:
        discard sock.getInt("non-existent-option-xyz")
      except NngError as e:
        check e.errCode == NNG_ENOTSUP
        caught = true
      check caught
      sock.close()

    test "Error NNG_EINVAL (3) on out-of-bounds option value":
      let sock = openPair0()
      var caught = false
      try:
        sock.setOpt(NNG_OPT_MAXTTL, 999) # Out of range 1..255
      except NngError as e:
        check e.errCode == NNG_EINVAL
        caught = true
      check caught
      sock.close()

  suite "Two-Step Dialer & Listener Lifecycle and Port-0 Binding":
    test "Listener binds port 0 and populates NNG_OPT_TCP_BOUND_PORT":
      let repSock = openRep0()
      let listener = repSock.newListener("tcp://127.0.0.1:0")
      listener.setOpt(NNG_OPT_TCP_NODELAY, true)
      listener.start()

      let boundPort = listener.getInt(NNG_OPT_TCP_BOUND_PORT)
      check boundPort > 0
      check listener.getSockAddr(NNG_OPT_LOCADDR) == "127.0.0.1:" & $boundPort

      let reqSock = openReq0()
      let dialer = reqSock.newDialer("tcp://127.0.0.1:" & $boundPort)
      dialer.setOpt(NNG_OPT_TCP_NODELAY, true)
      dialer.start()

      reqSock.send("two-step ping")
      check repSock.recvStr() == "two-step ping"
      repSock.send("two-step pong")
      check reqSock.recvStr() == "two-step pong"

      dialer.close()
      reqSock.close()
      listener.close()
      repSock.close()

  suite "First-Class Pipe Introspection & Event Hooks":
    test "pipeNotify fires ADD_PRE, ADD_POST, and REM_POST":
      var eventsFired: seq[PipeEvent] = @[]
      var observedPipeId: PipeId = 0

      let lSock = openPair0()
      lSock.pipeNotify(
        pipeEvAddPre,
        proc(p: Pipe, ev: PipeEvent) =
          eventsFired.add(ev),
      )
      lSock.pipeNotify(
        pipeEvAddPost,
        proc(p: Pipe, ev: PipeEvent) =
          eventsFired.add(ev)
          observedPipeId = p.id
          check p.socket == lSock
        ,
      )
      lSock.pipeNotify(
        pipeEvRemPost,
        proc(p: Pipe, ev: PipeEvent) =
          eventsFired.add(ev),
      )

      let listener = lSock.listen("tcp://127.0.0.1:0")
      let port = listener.getInt(NNG_OPT_TCP_BOUND_PORT)

      let dSock = openPair0()
      dSock.dial("tcp://127.0.0.1:" & $port)

      dSock.send("check pipe hooks")
      check lSock.recvStr() == "check pipe hooks"

      check eventsFired.contains(pipeEvAddPre)
      check eventsFired.contains(pipeEvAddPost)
      check observedPipeId > 0

      dSock.close()
      # Give dispatcher a moment to process disconnect
      for _ in 0 ..< 5:
        asyncdispatch.poll(10)
      check eventsFired.contains(pipeEvRemPost)

      listener.close()
      lSock.close()

    test "Pipe rejection in ADD_PRE hook drops connection":
      let lSock = openPair0()
      lSock.pipeNotify(
        pipeEvAddPre,
        proc(p: Pipe, ev: PipeEvent) =
          p.close() # Rejection
        ,
      )

      let listener = lSock.listen("tcp://127.0.0.1:0")
      let port = listener.getInt(NNG_OPT_TCP_BOUND_PORT)

      let dSock = openPair0()
      try:
        dSock.dial("tcp://127.0.0.1:" & $port)
      except CatchableError:
        discard

      # No pipes should be active on lSock
      check not lSock.hasRecv()

      listener.close()
      lSock.close()
      dSock.close()

  suite "Concurrent Contexts (nng_ctx)":
    test "Multiple contexts on single REQ0 socket multiplex cleanly":
      let repSock = openRep0()
      let listener = repSock.listen("tcp://127.0.0.1:0")
      let port = listener.getInt(NNG_OPT_TCP_BOUND_PORT)

      let reqSock = openReq0()
      reqSock.dial("tcp://127.0.0.1:" & $port)

      let ctx1 = reqSock.openContext()
      let ctx2 = reqSock.openContext()
      let ctx3 = reqSock.openContext()

      ctx1.send("req-from-ctx1")
      ctx2.send("req-from-ctx2")
      ctx3.send("req-from-ctx3")

      # Server receives all 3 requests
      let msg1 = repSock.recvMsg()
      let msg2 = repSock.recvMsg()
      let msg3 = repSock.recvMsg()

      check bytesToStr(msg1.body) == "req-from-ctx1"
      check bytesToStr(msg2.body) == "req-from-ctx2"
      check bytesToStr(msg3.body) == "req-from-ctx3"

      # Server replies in reverse order
      repSock.send("rep-to-ctx3")
      repSock.send("rep-to-ctx2")
      repSock.send("rep-to-ctx1")

      # Each context receives only its own reply
      check ctx3.recvStr() == "rep-to-ctx3"
      check ctx2.recvStr() == "rep-to-ctx2"
      check ctx1.recvStr() == "rep-to-ctx1"

      ctx1.close()
      ctx2.close()
      ctx3.close()
      reqSock.close()
      listener.close()
      repSock.close()

  suite "Raw Mode Sockets & Device Forwarding":
    test "device forwarder bridges two raw sockets":
      let devIn = openPair0(raw = true)
      let devOut = openPair0(raw = true)

      let l1 = devIn.listen("inproc://dev_in")
      let l2 = devOut.listen("inproc://dev_out")

      let client = openPair0()
      client.dial("inproc://dev_in")

      let server = openPair0()
      server.dial("inproc://dev_out")

      var devTask = device(devIn, devOut)

      client.send("device-forwarded-payload")
      # Wait a tick for device forwarding loop
      for _ in 0 ..< 5:
        asyncdispatch.poll(10)
      check server.recvStr() == "device-forwarded-payload"

      server.send("device-reply-payload")
      for _ in 0 ..< 5:
        asyncdispatch.poll(10)
      check client.recvStr() == "device-reply-payload"

      client.close()
      server.close()
      l1.close()
      l2.close()
      devIn.close()
      devOut.close()

    test "device rejects cooked sockets with NNG_EINVAL":
      let s1 = openPair0(raw = false)
      let s2 = openPair0(raw = false)
      var caught = false
      try:
        waitFor device(s1, s2)
      except NngError as e:
        check e.errCode == NNG_EINVAL
        caught = true
      check caught
      s1.close()
      s2.close()

  suite "Native OpenSSL 3 TLS over TCP (tls+tcp://)":
    test "PAIR0 tls+tcp:// encrypted loopback":
      let certPath = "/tmp/anynng_test_cert.pem"
      let keyPath = "/tmp/anynng_test_key.pem"

      if not fileExists(certPath) or not fileExists(keyPath):
        # Generate self-signed cert if missing
        discard os.execShellCmd(
          "openssl req -x509 -newkey rsa:2048 -nodes -keyout " & keyPath & " -out " &
            certPath & " -days 365 -subj '/CN=localhost'"
        )

      let srvCfg = newTlsConfig(tlsModeServer)
      srvCfg.setCertKeyFile(certPath, keyPath)

      let lSock = openPair0()
      let listener = lSock.newListener("tls+tcp://127.0.0.1:0")
      listener.setOpt(NNG_OPT_TLS_CONFIG, srvCfg)
      listener.start()

      let port = listener.getInt(NNG_OPT_TCP_BOUND_PORT)
      check port > 0

      let clientCfg = newTlsConfig(tlsModeClient)
      clientCfg.setAuthMode(tlsAuthModeNone) # Self-signed test cert

      let dSock = openPair0()
      let dialer = dSock.newDialer("tls+tcp://127.0.0.1:" & $port)
      dialer.setOpt(NNG_OPT_TLS_CONFIG, clientCfg)
      dialer.start()

      dSock.send("hello over tls+tcp")
      check lSock.recvStr() == "hello over tls+tcp"

      lSock.send("reply over tls+tcp")
      check dSock.recvStr() == "reply over tls+tcp"

      dialer.close()
      dSock.close()
      listener.close()
      lSock.close()

  suite "Native RFC 6455 WebSocket Secure (wss://)":
    test "PAIR0 wss:// encrypted loopback":
      let certPath = "/tmp/anynng_test_cert.pem"
      let keyPath = "/tmp/anynng_test_key.pem"

      let srvCfg = newTlsConfig(tlsModeServer)
      srvCfg.setCertKeyFile(certPath, keyPath)

      let lSock = openPair0()
      let listener = lSock.newListener("wss://127.0.0.1:0/test")
      listener.setOpt(NNG_OPT_TLS_CONFIG, srvCfg)
      listener.start()

      let port = listener.getInt(NNG_OPT_TCP_BOUND_PORT)
      check port > 0

      let clientCfg = newTlsConfig(tlsModeClient)
      clientCfg.setAuthMode(tlsAuthModeNone)

      let dSock = openPair0()
      let dialer = dSock.newDialer("wss://127.0.0.1:" & $port & "/test")
      dialer.setOpt(NNG_OPT_TLS_CONFIG, clientCfg)
      dialer.start()

      dSock.send("hello over wss")
      check lSock.recvStr() == "hello over wss"

      lSock.send("reply over wss")
      check dSock.recvStr() == "reply over wss"

      dialer.close()
      dSock.close()
      listener.close()
      lSock.close()
