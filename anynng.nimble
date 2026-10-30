# Package

version = "0.0.1"
author = "Axiomantic Maintainers"
description = "Pure-Nim multi-target (C, JS, WASM) and multi-transport (WS, TCP, IPC, UDP, Inproc) NNG / Scalability Protocols implementation"
license = "MIT"
srcDir = "src"
installExt = @["nim"]

# Dependencies

requires "nim >= 2.0.0"

# Tasks

task test, "Run test suite":
  for file in listFiles("tests"):
    if (file.startsWith("tests/test") or file.startsWith("tests/t")) and
        file.endsWith(".nim") and not file.endsWith("config.nims"):
      exec "nim r --hints:off " & file

task docgen, "Generate API documentation for Starlight":
  exec "nim c -r tools/docgen.nim"
