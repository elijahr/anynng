# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial project scaffolding via Polyform.
- Add Phase 3 Dialectical Pump architecture specification, NNG v1.12.4 wire protocol constants, DevEx verification findings, nimts WASM bridge integration, polyform template enhancement roadmap, and 0-defect adversarial audit report.
- Implement Pure-Nim Sans-I/O Core, wire framing codecs (WS, TCP/IPC, UDP), all 11 SP protocol roles, inproc:// transport, and dual-backend (nim c and nim js) test suite.
- **Browser/Node.js WebSocket Transport & WASM Bridge:** Implemented pure-Nim js_ws.nim, C-ABI wasm_bridge.nim, compile_wasm.sh with clang/wasm-ld/wasi-libc, full TypeScript SP socket classes (Req0, Rep0, Pub0, Sub0, Pair0, Pair1, Push0, Pull0, Surveyor0, Respondent0, Bus0), and comprehensive Vitest and Node v26 E2E integration test suites.
- **Native Transports & C libnng 1.12.4 Interop:** Implemented native TCP, Unix domain socket IPC, NNG v2 UDP, and RFC 6455 WebSocket transports with live C libnng 1.12.4 interop test suite across all 7 protocol families and bidirectional communication.
