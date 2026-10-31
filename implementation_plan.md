# Implementation Plan: `anynng` (Multi-Target Pure-Nim NNG & Polyform JS/WASM Enhancements)

## 1. Swarm Roster & Role Mapping
| Worker Name | Persona | Role | Assigned Subsystems |
| :--- | :--- | :--- | :--- |
| `anynng-architect` | Golem-Spire | Staff Systems Architect | Layer 1 Sans-I/O Core (`types`, `endian`, `url`, `framing/*`, `protocols/*`), `inproc://`, Native `tcp://`/`ipc://`/`udp://`/`ws://` transports, and C `libnng` 1.12.4 interop suite |
| `anynng-implementer` | Escapement-Forge | DevEx & Implementation Lead | `polyform` `has_nim_js` & `nimts` template enhancements, `nim js` WebSocket transport (`js_ws.nim`), `nimts` WASM bridge (`wasm_bridge.nim`), TypeScript socket classes (`ts/*`), and Vitest / Node JS interop suite |
| `anynng-auditor` | Cedar-Gauge | Verification & Adversarial Auditor | Independent Key 3 Five-Pillar Adversarial Code Audit on all strands prior to `vine weave` |

---

## 2. Distributed Locking & Concurrency Schedule
Before modifying any shared entrypoint file across concurrent waves, the designated worker must obtain an atomic lease:
- **Lock Target**: `file:src/anynng.nim`
  - *Wave 1 Owner*: `anynng-architect` (`task-core-engine`)
  - *Wave 2 Owners*: `anynng-architect` (`task-native-transports-interop`) then `anynng-implementer` (`task-browser-js-wasm-nimts`)
  - *Command*: `rhizo lock file:src/anynng.nim 900 --fencing`
- **Lock Target**: `file:justfile`
  - *Command*: `rhizo lock file:justfile 600 --fencing`

---

## 3. Vine Strand Lifecycle & Verification Matrix
For all development tracks:
1. **Provision Strand**:
   ```bash
   vine new <task_id> --branch strand/<task_id>
   ```
2. **Execute Work Inside Strand**:
   - Implement Provider + live Consumer call-sites + Replacement Purge of template stubs.
   - Run local unit tests (`nimble test`, `nim js -d:nodejs -r ...`, `npx vitest run`).
   - Record changelog entry via `vine changelog "feat: ..."`.
3. **Three-Key Gate Verification**:
   - **Worker**: Runs `vine submit --author <worker>` (evaluates Key 1 mechanical merge-tree + Key 2 live compiler/test suite + changelog).
   - **Independent Auditor (`@anynng-auditor`)**: Inspects Five Pillars under Presumption of Guilt and runs `vine audit approve --auditor anynng-auditor` (Key 3 ratification, stamps cryptographic `audit_token`).
4. **Trunk Weaving**:
   - **Orchestrator (`@anynng-orchestrator`)**: Fast-forwards ratified branch into `main` via `vine weave` and completes task via `rhizo task complete <id> --skip-gate`.

---

## 4. Phase-by-Phase Task Checklist

### Wave 1: Sans-I/O Protocol Core (`anynng`) & Multi-Target Template Facets (`polyform`)

- [x] **Task 1.1: Pure-Nim Sans-I/O Core, Wire Framing Codecs, All 7 SP Protocol Families & `inproc://` Transport** (`anynng-architect`) — *WOVEN (`b5342ce`, Audit Token: `7931ab03...`)*
  - *Task ID*: `task-core-engine`
  - *Strand*: `strand/task-core-engine` (in `/Users/eek/Development/anynng`)
  - *Lock*: `rhizo lock file:src/anynng.nim 900 --fencing`
  - *Provider*:
    - `src/anynng/core/types.nim` (`SpProto`, `SpOption`, `SpError`, `PipeId`, `SpMsg`, `EngineEvent` including `evUserSend`/`evUserCancel`, `EngineAction` including `actFinishSend`)
    - `src/anynng/core/endian.nim` (`putBe16/32/64`, `getBe16/32/64`, `putLe16`, `getLe16`)
    - `src/anynng/core/url.nim` (URI parser for `ws://`, `wss://`, `tcp://`, `ipc://`, `udp://`, `inproc://`)
    - `src/anynng/core/framing/ws_framing.nim` (`dialerSubprotocol`, `listenerSubprotocol`, binary frame pack/unpack, text opcode `0x1` rejection `1003`)
    - `src/anynng/core/framing/stream_framing.nim` (8-byte `\0SP\0` greeting with byte 0 = `0x00`, 64-bit BE length prefix with `recvMax` 1 MiB guard)
    - `src/anynng/core/framing/udp_framing.nim` (NNG v2 8-byte `udp_sp_msg` with Little-Endian `us_type` and `us_params`)
    - `src/anynng/core/protocols/{pair0,pair1,pubsub0,reqrep0,pipeline0,survey0,bus0}.nim` (All 11 SP protocol roles with exact NNG 1.12.4 wire tolerances)
    - `src/anynng/transports/inproc.nim` (Cross-target in-memory pipe transport working on `nim c` and `nim js`)
  - *Consumer / Call-Site*:
    - `src/anynng.nim` (Unified `Socket`, `Context`, `Dialer`, `Listener`, `dial`, `listen`, `send`, `recv`, `subscribe`, `unsubscribe` wired to core protocols and `inproc://`)
    - `tests/t_core_framing.nim`, `tests/t_core_protocols.nim`, and `tests/test_anynng.nim` (wired into `anynng.nimble` test task, running on both `nim c -r` and `nim js -d:nodejs -r`)
  - *Replacement Deletion*: Delete template placeholder `hello()` and `add(a, b)` from `src/anynng.nim` and `tests/test_anynng.nim`.
  - *Holistic Verification*: `nim c -r tests/test_anynng.nim` and `nim js -d:nodejs -r tests/test_anynng.nim` pass 100% of framing, protocol state machine, negative control, and live `inproc://` socket tests.
  - *Weave*: `vine submit --author anynng-architect` -> Key 3 Audit by `anynng-auditor` -> `vine weave`.

- [x] **Task 1.2: Polyform Template Enhancements for `has_nim_js` & First-Class `nimts` Bridge** (`anynng-implementer`) — *WOVEN (`4cb1a94`, Audit Token: `ce65a3a2...`)*
  - *Task ID*: `task-polyform-js-wasm`
  - *Strand*: `strand/task-polyform-js-wasm` (in `/Users/eek/Development/polyform`)
  - *Provider*:
    - `/Users/eek/Development/polyform/copier.yml` (Add `has_nim_js` boolean facet and `"nimts"` choice in `ts_bridge_strategy`)
    - `/Users/eek/Development/polyform/template/justfile.jinja` (Add `test-js` target running `nim js -d:nodejs -r` when `has_nim_js` is enabled, and `nimts` build hooks)
    - `/Users/eek/Development/polyform/template/scripts/compile_wasm.sh.jinja` & `template/package.json.jinja` (Support `nimts compile` when `ts_bridge_strategy == 'nimts'`)
    - `/Users/eek/Development/polyform/template/.github/workflows/ci.yml.jinja` (Add `test-js` step when `has_nim_js` is enabled)
  - *Consumer / Call-Site*:
    - `/Users/eek/Development/polyform/tests/` template generation verification test suite (`just test` in `/Users/eek/Development/polyform`)
  - *Holistic Verification*: Run `polyform` test suite to verify generated projects with `has_nim_js=true` and `ts_bridge_strategy=nimts` scaffold and pass checks cleanly.
  - *Weave*: `vine submit --author anynng-implementer` -> Key 3 Audit by `anynng-auditor` -> `vine weave`.

---

### Wave 2: Native Transports (`tcp://`, `ipc://`, `udp://`, `ws://`), Browser/Node JS & `nimts` WASM Bridge, and Live C `libnng` Interop

- [x] **Task 2.1: Native Transports (`tcp://`, `ipc://`, `udp://`, `ws://`) & Live C `libnng` 1.12.4 Interop Suite** (`anynng-architect`) — *WOVEN (`873623a`, Audit Token: `9d8ad337...`)*
  - *Dependency*: Task 1.1 (`task-core-engine`)
  - *Task ID*: `task-native-transports-interop`
  - *Strand*: `strand/task-native-transports-interop`
  - *Lock*: `rhizo lock file:src/anynng.nim 900 --fencing`
  - *Provider*:
    - `src/anynng/transports/native_tcp.nim` (`tcp://`, `tcp4://`, `tcp6://` async dialer & listener over `std/asyncnet` + `stream_framing`)
    - `src/anynng/transports/native_ipc.nim` (`ipc://` Unix domain socket async dialer & listener + `stream_framing`)
    - `src/anynng/transports/native_udp.nim` (`udp://`, `udp4://`, `udp6://` async datagram transport + `udp_framing` `CREQ`/`CACK`/`DATA`/`DISC`)
    - `src/anynng/transports/native_ws.nim` (`ws://` native RFC 6455 HTTP/1.1 Upgrade dialer & listener with `Sec-WebSocket-Protocol` asymmetry, 4-byte random client masking, `PING`/`PONG`/`CLOSE`, and text frame `1003` close)
  - *Consumer / Call-Site*:
    - Wire `tcp://`, `ipc://`, `udp://`, and `ws://` into `src/anynng.nim` (`dial` and `listen` dispatchers)
    - `tests/t_native_transports.nim` (Live native loopback tests across `tcp://`, `ipc://`, `udp://`, and `ws://`)
    - `tests/t_interop_libnng.nim` (Live C `libnng` 1.12.4 FFI interoperability test suite verifying `anynng` <-> `libnng` over `tcp://`, `ipc://`, and `ws://` across `PAIR0`, `PAIR1`, `REQ0`/`REP0`, `PUB0`/`SUB0`, `PUSH0`/`PULL0`, `SURVEYOR0`/`RESPONDENT0`, and `BUS0`)
  - *Holistic Verification*: `nimble test` runs `t_native_transports.nim` and `t_interop_libnng.nim` against `/opt/homebrew/opt/nng/lib/libnng.dylib` with 0 failures.
  - *Weave*: `vine submit --author anynng-architect` -> Key 3 Audit by `anynng-auditor` -> `vine weave`.

- [x] **Task 2.2: Browser/Node `nim js` WebSocket Transport, `nimts` WASM Bridge, and TypeScript Socket Package (`ts/*`)** (`anynng-implementer`) — *WOVEN (`e49fb0c`, Audit Token: `fa35664d...`)*
  - *Dependency*: Task 1.1 (`task-core-engine`)
  - *Task ID*: `task-browser-js-wasm-nimts`
  - *Strand*: `strand/task-browser-js-wasm-nimts`
  - *Provider*:
    - `src/anynng/transports/js_ws.nim` (`when defined(js):` native `globalThis.WebSocket` dialer with `binaryType = "arraybuffer"`, single-token `dialerSubprotocol`, and `std/asyncjs` `Future` integration)
    - `src/anynng/wasm_bridge.nim` (WASM C-ABI / `nimts`-compatible exports for SP framing & protocol state machines)
    - `scripts/compile_wasm.sh` (Compiles `src/anynng/wasm_bridge.nim` to `dist/anynng.wasm` and `src/anynng.nim` to JS)
    - `ts/types.ts`, `ts/runtime.ts`, `ts/index.ts` (Typed TypeScript classes: `Req0Socket`, `Rep0Socket`, `Pub0Socket`, `Sub0Socket`, `Pair0Socket`, `Pair1Socket`, `Push0Socket`, `Pull0Socket`, `Surveyor0Socket`, `Respondent0Socket`, `Bus0Socket` over WebSocket + WASM framing codec)
  - *Consumer / Call-Site*:
    - Wire `js_ws.nim` into `src/anynng.nim` under `when defined(js):`
    - `tests/t_js_ws.nim` (End-to-end `nim js -d:nodejs` WebSocket client test talking to a live `ws://` server)
    - `tests/test_ts_bridge.ts` (Vitest suite testing WASM codec exports AND live WebSocket `Req0Socket`/`Rep0Socket`, `Pub0Socket`/`Sub0Socket`, `Pair1Socket`, and `Bus0Socket` exchanges)
  - *Replacement Deletion*: Delete dummy fallback WASM bytecode in `scripts/compile_wasm.sh` and delete placeholder `add(a, b)` / `hello()` stubs in `ts/runtime.ts`, `ts/index.ts`, `ts/types.ts`, and `tests/test_ts_bridge.ts`.
  - *Holistic Verification*: `just test` (`nimble test`, `nim js -d:nodejs -r tests/t_js_ws.nim`, and `npx vitest run`) passes 100%.
  - *Weave*: `vine submit --author anynng-implementer` -> Key 3 Audit by `anynng-auditor` -> `vine weave`.

---

### Season `v0.2.0` — Wave 3: Full NNG Core Runtime, Options/Contexts/Timers/Devices, Native TLS (`tls+tcp://` & `wss://`), and JS/WASM Parity

- [x] **Task 3.1: Full NNG Core Runtime (`Options`, `Dialer`/`Listener` Port-0 & Auto-Reconnect, `Pipe`/`pipeNotify`, `Context`, Protocol Timers, `device`) & Native OpenSSL 3 TLS (`tls+tcp://`, `wss://`, `TlsConfig`)** (`anynng-architect`) — *WOVEN (`0f6e8e3`, Audit Token `a9d8373a...`)*
  - *Dependency*: Season `v0.2.0` Phase 3 (`task-v2-dialectic-parity` — WOVEN `60f5c7f`)
  - *Task ID*: `task-v2-core-runtime-tls`
  - *Strand*: `strand/task-v2-core-runtime-tls`
  - *Lock*: `rhizo lock file:src/anynng.nim 900 --fencing`
  - *Provider*:
    - `src/anynng/core/options.nim` (Strongly-typed `OptionDescriptor` & `OptionStore` across `Socket`, `Dialer`, `Listener`, `Pipe`, `Context` enforcing exact NNG error semantics: `NNG_EBADTYPE=30`, `NNG_EREADONLY=24`, `NNG_EWRITEONLY=25`, `NNG_ENOTSUP=9`, `NNG_EINVAL=3`)
    - `src/anynng/core/tls.nim` (`TlsConfig` matching `nng_tls_config`: `TlsMode`, `TlsAuthMode`, `TlsVersion`, `ownCert`, `caChain`, `certKeyFile`, `caFile`, `serverName` SNI)
    - `src/anynng/transports/native_tls.nim` (`tls+tcp://`, `tls+tcp4://`, `tls+tcp6://` async OpenSSL 3 dialer & listener + `NNG_OPT_TLS_VERIFIED` and `NNG_OPT_TLS_PEER_CN`)
    - Upgrade `src/anynng/transports/native_{tcp,ipc,udp,ws}.nim` & `src/anynng.nim`:
      - Add `wss://` (`wss4://`, `wss6://`) support to `native_ws.nim` over `TlsConfig`
      - Two-step `Dialer` (`newDialer`, `setOpt`/`getOpt`, `start`, `close`, background auto-reconnect with `reconnect-time-min`/`reconnect-time-max` and `NNG_FLAG_NONBLOCK`) and `Listener` (`newListener`, `setOpt`/`getOpt`, `start`, `close`, ephemeral port `0` binding with `getsockname()` populating `NNG_OPT_TCP_BOUND_PORT` / `NNG_OPT_LOCADDR`)
      - First-class `Pipe` introspection (`id`, `socket`, `dialer`, `listener`, `close`, `msg.pipeId`) and `pipeNotify` (`NNG_PIPE_EV_ADD_PRE`, `ADD_POST`, `REM_POST` where closing a pipe in `ADD_PRE` rejects the connection)
      - Concurrent `Context` (`openContext`, `ctx.send`, `ctx.recv`, `ctx.setOpt`, `ctx.getOpt`, `ctx.close`) on `REQ0`, `REP0`, `SURVEYOR0`, `RESPONDENT0`, `SUB0`
      - Protocol timers (`req:resend-time`, `surveyor:survey-time`), bounded `SENDBUF`/`RECVBUF` backpressure, and raw-mode `device(s1, s2)` forwarder
  - *Consumer / Call-Site*:
    - Wire into `src/anynng.nim` (`dial`, `listen`, `newDialer`, `newListener`, `openContext`, `pipeNotify`, `device`, typed `getOpt`/`setOpt`)
    - `tests/t_v2_runtime_tls.nim` (Comprehensive test suite for typed options + negative controls, two-step `Dialer`/`Listener` port-0 binding & auto-reconnect, `pipeNotify` `ADD_PRE` rejection, `Context` multiplexing, `REQ0` resend & `SURVEYOR0` survey timers, `device(s1, s2)` raw proxy, and native `tls+tcp://` + `wss://` TLS handshakes)
    - Expand `tests/t_interop_libnng.nim` with live `tls+tcp://` and `wss://` interoperability tests against Homebrew C `libnng` 1.12.4
  - *Holistic Verification*: `nimble test` and `nim c -r -d:ssl -d:test_c_interop tests/t_interop_libnng.nim` pass 100%.
  - *Weave*: `vine submit --author anynng-architect` -> Key 3 Audit by `anynng-auditor` -> `vine weave`.

- [x] **Task 3.2: JS (`nim js`) & WASM/TypeScript (`nimts` `ts/*`) Full Parity (`wss://`, `inproc://`, Options, `Context`, Timers, `pipeNotify`, Auto-Reconnect)** (`anynng-implementer`) — *WOVEN (`1d8f1de`, Audit Token `cfaf2d6a...`)*
  - *Dependency*: Season `v0.2.0` Phase 3 (`task-v2-dialectic-parity` — WOVEN `60f5c7f`)
  - *Task ID*: `task-v2-js-wasm-parity`
  - *Strand*: `strand/task-v2-js-wasm-parity`
  - *Provider*:
    - `src/anynng/transports/js_ws.nim` (Add `wss://` support, two-step `JsDialer` with auto-reconnect backoff, `pipeNotify` callbacks `onPipeAddPre`/`onPipeAddPost`/`onPipeRemPost`, `Context` multiplexing `openContext()`, `REQ0` resend timer, `SURVEYOR0` survey timer, `PAIR1` `polyamorous` & `maxTtl` options, and `raw` mode support)
    - `src/anynng/wasm_bridge.nim`, `ts/types.ts`, `ts/runtime.ts`, `ts/index.ts` (Expose full option getters/setters `maxTtl`, `polyamorous`, `resendTime`, `surveyTime`, `recvMaxSize`, `raw`, `Context` class via `socket.openContext()`, `pipeNotify` hooks, `Dialer` auto-reconnect, and `wss://` support in TypeScript)
  - *Consumer / Call-Site*:
    - Expand `tests/t_js_ws.nim` (`nim js -d:nodejs -r`) and `tests/test_ts_bridge.ts` (Vitest) with `wss://`, `Context` concurrent multiplexing, `pipeNotify` `ADD_PRE` rejection, `REQ0` resend timer, `SURVEYOR0` deadline timer, `PAIR1` polyamorous/TTL options, and negative controls
  - *Holistic Verification*: `nim js -d:nodejs -r tests/t_js_ws.nim` and `npx vitest run` pass 100%.
  - *Weave*: `vine submit --author anynng-implementer` -> Key 3 Audit by `anynng-auditor` -> `vine weave`.

---

### Season `v0.2.0` — Wave 4: `<nng/nng.h>` C-ABI Library (`c_api.nim`) & Upstream `nanomsg/nng` v1.12.4 `nuts.h` Specification Test Runner

- [x] **Task 4.1: `<nng/nng.h>` C-ABI Export Layer (`src/anynng/c_api.nim`), `tests/upstream_shim/nuts.h`, and Automated Upstream `nanomsg/nng` v1.12.4 Spec Test Suite (`just test-nng-spec`)** (`anynng-architect`) — *WOVEN (`a587913`, Audit Token `8fb66155...`)*
  - *Dependency*: Task 3.1 (`task-v2-core-runtime-tls`) and Task 3.2 (`task-v2-js-wasm-parity`)
  - *Task ID*: `task-v2-upstream-nuts-spec`
  - *Strand*: `strand/task-v2-upstream-nuts-spec`
  - *Lock*: `rhizo lock file:justfile 600 --fencing`
  - *Provider*:
    - `src/anynng/c_api.nim` (`{.exportc, dynlib, cdecl.}` implementation of `<nng/nng.h>`, protocol openers `nng_*_open` / `nng_*_open_raw` / `nng_pair1_open_poly`, `nng_listen*`, `nng_dial*`, `nng_send*`, `nng_recv*`, `nng_msg_*`, `nng_aio_*`, `nng_ctx_*`, `nng_pipe_*`, `nng_socket_get/set*`, `nng_dialer_get/set*`, `nng_listener_get/set*`, `nng_device`, `nng_tls_config_*`, `nng_strerror`, `nng_clock`, `nng_msleep`, `nng_random`, `nng_thread_*`, `nng_mtx_*`, `nng_cv_*`)
    - `tests/upstream_shim/nuts.h` (Bridges unmodified upstream `nanomsg/nng` v1.12.4 `*_test.c` test files and `acutest.h` to `libanynng`)
    - `scripts/run_upstream_nng_tests.sh` (Shallow-clones `https://github.com/nanomsg/nng.git` tag `v1.12.4` into git-ignored `.cache/nng-upstream`, builds `libanynng` from `src/anynng/c_api.nim`, compiles and executes upstream's C specification test suites `pair0_test.c`, `pair1_test.c`, `req_test.c`, `rep_test.c`, `pub_test.c`, `sub_test.c`, `push_test.c`, `pull_test.c`, `survey_test.c`, `respond_test.c`, `bus_test.c`, `inproc_test.c`, `tcp_test.c`, `ipc_test.c`, `ws_test.c` directly against `anynng` without copying a single upstream test file into the repo)
  - *Consumer / Call-Site*:
    - Wire `test-nng-spec` into `justfile` (`just test-nng-spec` and `just test`) and `anynng.nimble`
  - *Holistic Verification*: `just test-nng-spec` and `just test` execute all upstream `nanomsg/nng` v1.12.4 C test suites against `anynng` with 0 failures.
  - *Weave*: `vine submit --author anynng-architect` -> Key 3 Audit by `anynng-auditor` -> `vine weave`.

---

## 5. Dynamic Progress Tracking & Emergent Design Addendum Protocol
- As each task is woven via `vine weave`, `@anynng-orchestrator` updates `- [ ]` to `- [x]` in `implementation_plan.md`.
- Any deviation from `design.md` requires submitting `docs/addenda/addendum_<topic>.md` to `@anynng-orchestrator` before coding.
