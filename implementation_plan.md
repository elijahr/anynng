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

## 5. Dynamic Progress Tracking & Emergent Design Addendum Protocol
- As each task is woven via `vine weave`, `@anynng-orchestrator` updates `- [ ]` to `- [x]` in `implementation_plan.md`.
- Any deviation from `design.md` requires submitting `docs/addenda/addendum_<topic>.md` to `@anynng-orchestrator` before coding.
