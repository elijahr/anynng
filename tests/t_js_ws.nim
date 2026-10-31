# tests/t_js_ws.nim - Integration Test Suite for anynng Browser & Node.js WebSocket Transport
# Licensed under MIT License.

when not defined(js):
  echo "Skipping t_js_ws.nim under C backend (run via: nim js -d:nodejs tests/t_js_ws.nim)"
else:
  import std/asyncjs
  import std/jsffi
  import anynng/core/types
  import anynng/transports/js_ws

  proc toStr(b: seq[byte]): string =
    result = newString(b.len)
    for i in 0 ..< b.len:
      result[i] = char(b[i])

  proc toBytes(s: string): seq[byte] =
    result = newSeq[byte](s.len)
    for i in 0 ..< s.len:
      result[i] = byte(s[i])

  proc startMockServer(): Future[JsObject] =
    let p = newPromise[JsObject](proc(resolve: proc(val: JsObject)) =
      {.emit: """
      import('./helpers/mock_ws_server.mjs').then(mod => {
        return mod.createSpWsServer(0);
      }).then(server => {
        `resolve`(`server`);
      });
      """.}
    )
    return p

  proc startMockWssServer(): Future[JsObject] =
    let p = newPromise[JsObject](proc(resolve: proc(val: JsObject)) =
      {.emit: """
      process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';
      import('./helpers/mock_ws_server.mjs').then(mod => {
        return mod.createSpWssServer(0);
      }).then(server => {
        `resolve`(`server`);
      });
      """.}
    )
    return p

  proc runTests() {.async.} =
    echo "[TEST START] anynng JavaScript WebSocket Transport (Node v26 & Browser)"

    let srv = await startMockServer()
    var srvUrl: cstring
    {.emit: "`srvUrl` = `srv`.url;".}
    echo "Mock SP WebSocket Server listening at: ", srvUrl

    # Test 1: PAIR0 Socket
    block:
      echo "--- Test 1: PAIR0 raw loopback ---"
      let sock = await dialJsWs($srvUrl, spPair0)
      await sock.send(toBytes("hello-pair0"))
      let resp = await sock.recv()
      let msg = toStr(resp)
      echo "PAIR0 received: ", msg
      assert msg == "hello-pair0", "Expected 'hello-pair0', got: " & msg
      await sock.close()
      echo "PAIR0 test PASSED."

    # Test 2: REQ0 Socket
    block:
      echo "--- Test 2: REQ0 request/reply backtrace ---"
      let sock = await dialJsWs($srvUrl, spReq0)
      await sock.send(toBytes("ping-req0"))
      let resp = await sock.recv()
      let msg = toStr(resp)
      echo "REQ0 received reply: ", msg
      assert msg == "rep:ping-req0", "Expected 'rep:ping-req0', got: " & msg
      await sock.close()
      echo "REQ0 test PASSED."

    # Test 3: PAIR1 Socket (Hop count handling)
    block:
      echo "--- Test 3: PAIR1 hop header loopback ---"
      let sock = await dialJsWs($srvUrl, spPair1)
      await sock.send(toBytes("data-pair1"))
      let resp = await sock.recv()
      let msg = toStr(resp)
      echo "PAIR1 received: ", msg
      assert msg == "data-pair1", "Expected 'data-pair1', got: " & msg
      await sock.close()
      echo "PAIR1 test PASSED."

    # Test 4: SURVEYOR0 Socket
    block:
      echo "--- Test 4: SURVEYOR0 survey broadcast ---"
      let sock = await dialJsWs($srvUrl, spSurveyor0)
      await sock.send(toBytes("vote-survey0"))
      let resp = await sock.recv()
      let msg = toStr(resp)
      echo "SURVEYOR0 received: ", msg
      assert msg == "resp:vote-survey0", "Expected 'resp:vote-survey0', got: " & msg
      await sock.close()
      echo "SURVEYOR0 test PASSED."

    # Test 5: pipeNotify hook and ADD_PRE rejection
    block:
      echo "--- Test 5: pipeNotify hook lifecycle & rejection ---"
      var events: seq[int] = @[]
      let hook1: PipeNotifyHook = proc(pId: PipeId, ev: PipeEvent): bool =
        events.add(int(ev))
        return true

      let sock = await dialJsWs($srvUrl, spPair0, pipeNotifyHook = hook1)
      assert events.len == 2, "Expected 2 events (addPre, addPost), got: " & $events.len
      assert events[0] == int(pipeEvAddPre)
      assert events[1] == int(pipeEvAddPost)
      await sock.close()

      # ADD_PRE rejection
      var rejectEvents: seq[int] = @[]
      let hookReject: PipeNotifyHook = proc(pId: PipeId, ev: PipeEvent): bool =
        rejectEvents.add(int(ev))
        if ev == pipeEvAddPre:
          return false
        return true

      let sockReject = await dialJsWs($srvUrl, spPair0, pipeNotifyHook = hookReject)
      assert sockReject.isOpen == false, "Expected rejected socket to not be open"
      assert rejectEvents.contains(int(pipeEvAddPre)), "Expected addPre"
      assert rejectEvents.contains(int(pipeEvRemPost)), "Expected remPost on rejection"
      echo "pipeNotify test PASSED."

    # Test 6: Context Multiplexing
    block:
      echo "--- Test 6: Context concurrent multiplexing ---"
      let sock = await dialJsWs($srvUrl, spReq0)
      let ctx1 = sock.openContext()
      let ctx2 = sock.openContext()
      assert ctx1.id != ctx2.id

      await ctx1.send(toBytes("ping-ctx1"))
      await ctx2.send(toBytes("ping-ctx2"))

      let resp1 = await ctx1.recv()
      let resp2 = await ctx2.recv()

      assert toStr(resp1) == "rep:ping-ctx1", "Ctx1 mismatch: " & toStr(resp1)
      assert toStr(resp2) == "rep:ping-ctx2", "Ctx2 mismatch: " & toStr(resp2)

      ctx1.close()
      ctx2.close()
      await sock.close()
      echo "Context multiplexing test PASSED."

    # Test 7: Secure TLS WebSocket (wss://)
    block:
      echo "--- Test 7: Secure TLS WebSocket loopback (wss://) ---"
      let wssSrv = await startMockWssServer()
      var wssUrl: cstring
      {.emit: "`wssUrl` = `wssSrv`.url;".}
      echo "Mock SP WSS Server listening at: ", wssUrl

      let sock = await dialJsWs($wssUrl, spPair0)
      await sock.send(toBytes("secure-tls-payload"))
      let resp = await sock.recv()
      let msg = toStr(resp)
      assert msg == "secure-tls-payload", "Expected 'secure-tls-payload', got: " & msg
      await sock.close()
      {.emit: "`wssSrv`.close();".}
      echo "Secure WSS test PASSED."

    # Test 8: Two-step JsDialer Lifecycle
    block:
      echo "--- Test 8: Two-step JsDialer ---"
      let sock = newJsWsSocket(spPair0)
      let dialer = sock.newJsDialer($srvUrl, reconnectMin = 50, reconnectMax = 500)
      await dialer.start()

      await sock.send(toBytes("dialer-payload"))
      let resp = await sock.recv()
      assert toStr(resp) == "dialer-payload"

      dialer.close()
      echo "JsDialer test PASSED."

    # Test 9: Socket Options
    block:
      echo "--- Test 9: Socket Options (maxTtl, polyamorous, recvMaxSize, raw) ---"
      let sock = newJsWsSocket(spPair1)
      sock.setOpt("maxTtl", 16)
      assert sock.getOptInt("maxTtl") == 16
      sock.setOpt("recvMaxSize", 32768)
      assert sock.getOptInt("recvMaxSize") == 32768
      sock.setOptBool("polyamorous", true)
      assert sock.getOptBool("polyamorous") == true
      sock.setOptBool("raw", true)
      assert sock.getOptBool("raw") == true
      echo "Socket Options test PASSED."

    # Test 10: Raw Mode Loopback
    block:
      echo "--- Test 10: Raw Mode Loopback ---"
      let sock = await dialJsWs($srvUrl, spPair0, raw = true)
      await sock.send(toBytes("raw-data-stream"))
      let resp = await sock.recv()
      assert toStr(resp) == "raw-data-stream"
      await sock.close()
      echo "Raw Mode test PASSED."

    # Close server
    {.emit: "`srv`.close();".}

    echo "======================================================"
    echo "[TEST SUCCESS] All anynng JS WebSocket tests passed 100%!"
    echo "======================================================"

  discard runTests()
