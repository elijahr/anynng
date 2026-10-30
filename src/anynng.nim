# anynng - Pure-Nim multi-target (C, JS, WASM) and multi-transport (WS, TCP, IPC, UDP, Inproc) NNG / Scalability Protocols implementation

const projectVersion* = "0.0.1"

const greetingMsg* = "hello from anynng"

proc hello*(): string =
  ## Returns standard project greeting string.
  greetingMsg

proc add*(a, b: int): int {.exportc: "add", cdecl.} =
  ## Adds two integers (exported to WebAssembly).
  a + b
