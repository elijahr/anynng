# anynng - Pure-Nim multi-target (C, JS, WASM) and multi-transport NNG / Scalability Protocols
# Licensed under MIT License.

import std/asyncdispatch
import ./[options, runtime_types, socket]

# Raw Mode Device Forwarding
proc device*(s1, s2: Socket): Future[void] {.async.} =
  let target2 = if s2 != nil and s2 != s1: s2 else: s1
  if not s1.raw or (s2 != nil and not s2.raw):
    raise newNngError(NNG_EINVAL, "device requires raw mode sockets")

  while not s1.closed and (target2 == s1 or not target2.closed):
    # Forward s1 -> target2 if message ready
    if s1.hasRecv():
      let msg = s1.recvMsg(0)
      target2.sendMsg(msg)
    # Forward target2 -> s1 if message ready
    if target2 != s1 and target2.hasRecv():
      let msg = target2.recvMsg(0)
      s1.sendMsg(msg)
    await sleepAsync(1)
