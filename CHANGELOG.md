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
- **Season v0.2.0 Full NNG Parity & Upstream nuts.h Spec Harness Architecture:** Formulated complete architecture and specification parity for TLS transports (`tls+tcp://`, `wss://`), `TlsConfig` parity with `nng_tls_config`, strongly-typed Option Registry (`getopt`/`setopt`) with exact NNG error taxonomy (`NNG_EBADTYPE`, `NNG_EREADONLY`, `NNG_EWRITEONLY`, `NNG_ENOTSUP`, `NNG_EINVAL`), first-class `Dialer` & `Listener` two-step lifecycle with ephemeral port-0 binding (`NNG_OPT_TCP_BOUND_PORT`), first-class `Pipe` introspection & `nng_pipe_notify` hooks (`ADD_PRE`, `ADD_POST`, `REM_POST`), concurrent `Context` (`nng_ctx`) multiplexing, raw sockets & `device(s1, s2)` forwarding, and C ABI shim (`src/anynng/c_api.nim` + `tests/upstream_shim/nuts.h` + `just test-nng-spec`) running unmodified upstream `nanomsg/nng` v1.12.4 test suites from `.cache/nng-upstream`. All 8 audit findings (`CRIT-06..08`, `HIGH-05..07`, `WARN-05..06`) remediated with 0 open defects.
- **Browser/Node.js JS & WASM Parity (Task 3.2):** Implemented wss:// support, two-step JsDialer with auto-reconnect backoff, pipeNotify callbacks (ADD_PRE rejection), Context concurrent multiplexing, REQ0 resend & SURVEYOR0 deadline timers, PAIR1 polyamorous/TTL options, and full TypeScript wrappers.
- **Core Runtime & Native OpenSSL 3 TLS (Task 3.1):** Full NNG core runtime parity, strongly-typed option registry, two-step dialer/listener with port-0 dynamic binding, pipeNotify event hooks, concurrent contexts (nng_ctx), raw socket device forwarding, and native OpenSSL 3 TLS transports (tls+tcp://, wss://).
- feat(c-abi): <nng/nng.h> C-ABI export layer (c_api.nim), nuts.h test harness, and upstream nanomsg/nng v1.12.4 specification test suite (just test-nng-spec)
- Canonical WASM ProtocolEngine state machine export, de-duplication of TypeScript protocol state machines, template carcass purge, and Deque O(1) queue refactor (Task 5.1).
- feat(nuts): 100% pass across all 12 upstream nanomsg/nng v1.12.4 NUTS protocol specification test suites (pair0, pair1, pair1_poly, req, rep, pub, sub, push, pull, survey, respond, bus)
- feat: upgrade C-ABI and protocol engines to nanomsg/nng v2.1.0 compatibility with 18/18 upstream NUTS protocol test suites passing and per-suite execution timeout
- refactor: decompose monolithic src/anynng.nim and src/anynng/c_api.nim into single-responsibility domain submodules
- refactor: purge all NNG v1 legacy symbols and wire pure NNG v2.1.0 APIs & upstream libnng interop
- feat(abi): integrate HeaderKit v2 pipeline and align C-ABI signatures against upstream NNG v2.1.0 (pyproject.toml, headerkit.toml with diff/cffi/pyi targets, typed pointer parameter exports, duration width alignment, zero mangled parameter suffixes via NimJsonDocEnricher, and just verify-abi test target)
- fix(cabi): align nng_sockaddr union layout & sub-structs, canonical handle typedefs, expanded nng_init_params, nng_iov / nng_aio_set_iov, purge internal seq[byte] leakage from public C headers, and update HeaderKit ABI verifier with holistic struct/union layout coverage (Task task-anynng-shard4-abi-remediation)
