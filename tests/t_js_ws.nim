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

    # Close server
    {.emit: "`srv`.close();".}

    echo "======================================================"
    echo "[TEST SUCCESS] All anynng JS WebSocket tests passed 100%!"
    echo "======================================================"

  discard runTests()
