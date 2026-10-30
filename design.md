# `anynng` — Architecture & System Design Document

> [!NOTE]
> **Project Workspace:** `/Users/eek/Development/anynng`
> **Targets:** `nim js` (Browser / Node / Deno / Bun), `nim c` (Native Linux / macOS / Windows / Embedded), `nim wasm` (`--os:any` / Emscripten)
> **Wire Compatibility:** 100% interoperable with C `libnng` v1 (`1.12.4` installed locally) and v2 (`main`), Go `mangos` v3, and Rust `nng-core`.

---

## 1. Architecture Overview

`anynng` is structured around a strict **3-Layer Sans-I/O Architecture** so that 100% of the Scalability Protocols (SP) wire framing, transport framing codecs, and protocol state machines are written once in pure target-agnostic Nim (`seq[byte]`, `uint32`, deterministic state machines) with zero OS, POSIX, or JS FFI imports in the core:

```mermaid
flowchart TB
    subgraph Layer3["Layer 3: High-Level Async Socket API (anynng.nim & ts/index.ts)"]
        API["Socket / Context / Dialer / Listener API\ndial(url), listen(url), send(bytes), recv(), subscribe(topic)"]
    end

    subgraph Layer1["Layer 1: Pure-Nim Sans-I/O Core (src/anynng/core/) — 100% Shared Across C / JS / WASM"]
        Proto["SP Protocol State Machines (protocols/)\n• Pair0 / Pair1 (hop header, poly mode)\n• Req0 / Rep0 (31-bit reqId + 0x80000000 backtrace, retry, ctx multiplexing)\n• Pub0 / Sub0 (prefix filter table)\n• Push0 / Pull0 (round-robin load balance / fair-queue)\n• Surveyor0 / Respondent0 (surveyId broadcast + deadline timer)\n• Bus0 (full mesh: local send broadcasts to all; rx delivers strictly to local recv)"]
        Framing["Transport Wire Framing Codecs (framing/)\n• ws_codec: Sec-WebSocket-Protocol (dialer=peer, listener=self) + 1:1 binary msg framing\n• stream_codec (tcp/ipc): 8-byte '\\0SP\\0' greeting (byte0=0x00) + 64-bit BE length (recvMax bounded)\n• udp_codec (NNG v2): 8-byte Little-Endian udp_sp_msg (CREQ/CACK/DATA/DISC/MESH)"]
    end

    subgraph Layer2["Layer 2: Target-Specific I/O Adapters (src/anynng/transports/)"]
        JSIO["when defined(js):\n• ws_js.nim (Browser/Node globalThis.WebSocket + std/asyncjs)\n• inproc_js.nim (In-tab/Worker memory pipes)"]
        WasmIO["when defined(wasm) or defined(emscripten):\n• ws_wasm.nim (Host JS WebSocket imports / emscripten_websocket)\n• ts/runtime.ts (nimts zero-copy memory views)"]
        NativeIO["when not defined(js) and not defined(wasm):\n• tcp_native.nim (std/asyncnet TCP dialer & listener)\n• ipc_native.nim (Unix domain sockets)\n• udp_native.nim (Async UDP datagram sockets)\n• ws_native.nim (RFC 6455 HTTP/1.1 Upgrade + frame masking)\n• inproc_native.nim (Zero-copy in-memory pipes)"]
    end

    API --> Proto
    Proto --> Framing
    Framing --> JSIO
    Framing --> WasmIO
    Framing --> NativeIO
```

---

## 2. Module & Component Breakdown

```text
/Users/eek/Development/anynng/
├── anynng.nimble                     # Zero external dependencies required for core + JS + TCP/IPC/UDP/WS!
├── src/
│   ├── anynng.nim                    # Unified public API (works on nim c, nim js, nim wasm)
│   └── anynng/
│       ├── core/                     # 100% Pure Sans-I/O Nim (no OS/JS/net imports)
│       │   ├── types.nim             # SpProto, SpRole, SpError, PipeId, SpMsg (header + body), EngineEvent, EngineAction
│       │   ├── endian.nim            # Big-Endian and Little-Endian uint16 / uint32 / uint64 encode/decode helpers
│       │   ├── url.nim               # URI parser for ws://, wss://, tcp://, ipc://, udp://, inproc://
│       │   ├── framing/
│       │   │   ├── ws_framing.nim    # Subprotocol lookup (dialerSubprotocol / listenerSubprotocol) & binary frame pack/unpack
│       │   │   ├── stream_framing.nim# Incremental stream parser for TCP/IPC (\0SP\0 greeting with byte0=0x00 + 8-byte BE length prefix with recvMax bound)
│       │   │   └── udp_framing.nim   # NNG v2 8-byte udp_sp_msg packet codec with Little-Endian fields (nng/udp.c:494)
│       │   └── protocols/
│       │       ├── pair0.nim         # PAIRv0 (0-byte header, 1:1 pipe)
│       │       ├── pair1.nim         # PAIRv1 (4-byte hop count header, cooked + polyamorous pipe addressing, tolerant hop 0)
│       │       ├── pubsub0.nim       # PUB0 (fan-out broadcast; closes pipe if rx) & SUB0 (drop-all default until subscribe, empty prefix wildcard)
│       │       ├── reqrep0.nim       # REQ0 (31-bit reqId | 0x80000000, context map, resend timer, drop unknown reply silently) & REP0 (backtrace stack)
│       │       ├── pipeline0.nim     # PUSH0 (round-robin across ready pipes) & PULL0 (fair-queued recv)
│       │       ├── survey0.nim       # SURVEYOR0 (broadcast surveyId | 0x80000000, deadline timer termination) & RESPONDENT0
│       │       └── bus0.nim          # BUS0 (full-mesh broadcast: send to all pipes; incoming delivered strictly to local recv)
│       └── transports/               # Target-specific runtime I/O drivers
│           ├── inproc.nim            # Cross-target in-memory pipe transport (inproc://)
│           ├── js_ws.nim             # [nim js] Browser/Node WebSocket dialer (binaryType="arraybuffer")
│           ├── wasm_ws.nim           # [nim wasm] Emscripten / Host-imported WebSocket dialer
│           ├── native_tcp.nim        # [nim c] Async TCP dialer & listener (tcp://, tcp4://, tcp6://)
│           ├── native_ipc.nim        # [nim c] Async Unix domain socket dialer & listener (ipc://)
│           ├── native_udp.nim        # [nim c] Async UDP datagram endpoint (udp://, udp4://, udp6://)
│           └── native_ws.nim         # [nim c] Native RFC 6455 WebSocket dialer (masked) & listener (unmasked)
├── ts/                               # First-class TypeScript package bindings
│   ├── index.ts                      # Typed Socket classes (Req0Socket, Rep0Socket, Pub0Socket, Sub0Socket, Pair1Socket, Bus0Socket)
│   ├── runtime.ts                    # WebAssembly runtime loader & zero-copy Uint8Array views (nimts)
│   └── types.ts                      # Protocol types and configuration options
└── tests/
    ├── t_core_framing.nim            # Sans-I/O unit tests for all wire codecs (runs on both nim c and nim js!)
    ├── t_core_protocols.nim          # Sans-I/O state machine tests for all 11 protocol roles + malformed frame handling
    ├── t_native_transports.nim       # Native inproc://, tcp://, ipc://, udp://, ws:// loopback tests
    ├── t_interop_libnng.nim          # Live cross-language interop against C libnng 1.12.4 (TCP, IPC, WS, and Node JS client!)
    └── test_ts_bridge.ts             # TypeScript & Vitest integration tests for npm package

---

## 3. Data Flow & State Machine Contract

### 3.1 The Sans-I/O Engine Contract (`types.nim`)
Every protocol state machine implements a uniform, deterministic interface driven by events and emitting explicit actions:

- **Inbound Events (`EngineEvent`):**
  1. `evPipeAdded(pipeId: PipeId)` — Transport finished handshake (`\0SP\0` on TCP/IPC, `101 Switching Protocols` on WS, or `CACK` on UDP).
  2. `evPipeRemoved(pipeId: PipeId)` — Transport disconnected.
  3. `evPipeMessage(pipeId: PipeId, rawFrame: seq[byte])` — Complete SP frame (`header || body`) arrived from `pipeId`.
  4. `evTimerTick(nowMs: int64)` — Periodic or scheduled wakeup for `REQ0` retry or `SURVEYOR0` deadline expiration.
  5. `evUserSend(ctxId: uint32, msg: SpMsg)` — User application initiating a send on context `ctxId`.
  6. `evUserCancel(ctxId: uint32)` — User application aborting or timing out a pending request/operation.
- **Outbound Actions (`EngineAction`):**
  1. `actSendPipe(pipeId: PipeId, rawFrame: seq[byte])` — Instructs the transport to transmit `rawFrame` on `pipeId`.
  2. `actClosePipe(pipeId: PipeId, reason: SpError)` — Instructs the transport to immediately drop `pipeId` (e.g., when a malformed header arrives or a `SUB0` peer illegally transmits data to a `PUB0` socket).
  3. `actDeliverRecv(ctxId: uint32, msg: SpMsg, pipeId: PipeId)` — Resolves a pending `recv()` / request `Future` with `msg`.
  4. `actFinishSend(ctxId: uint32)` — Resolves a pending async `send()` `Future` on context `ctxId`.
  5. `actFailContext(ctxId: uint32, err: SpError)` — Fails a pending `Future` (e.g., `spErrTimedOut` when a survey deadline expires).

### 3.2 Native WebSocket (`native_ws.nim`) Without External Dependencies
Because NNG's `ws://` transport only requires standard RFC 6455 HTTP/1.1 upgrade (`Sec-WebSocket-Key` + SHA-1 magic GUID `258EAFA5-E914-47DA-95CA-C5AB0DC85B11` via Nim's built-in `std/sha1` and `std/base64`) plus `Sec-WebSocket-Protocol` negotiation (`dialerSubprotocol` sends peer proto name, `listenerSubprotocol` matches self proto name) and binary frame masking (`opcode = 0x2`, `PING`/`PONG` `0x9`/`0xA`, `CLOSE` `0x8`), `native_ws.nim` can be implemented directly on top of `std/asyncnet` with **zero third-party Nimble dependencies**!
- **Frame Masking Compliance (RFC 6455 §5.3)**: `native_ws.nim` dialers generate a cryptographically random 4-byte masking key for all client-to-server frames, while `native_ws.nim` listeners emit unmasked frames. Server closes with code `1002` if client frame is unmasked; client closes with code `1002` if server frame is masked.

---

## 4. Error Handling & Protocol Compliance Rules

| Condition | Exact NNG Wire Behavior Implemented by `anynng` |
| :--- | :--- |
| **`REQ0` / `REP0` / `SURVEY` frame `< 4` bytes or missing `0x80` high-bit marker within `maxTtl` (default 8) hops** | If `< 4` bytes before `0x80` marker: emit `actClosePipe(pipeId, spErrProto)` (closes connection immediately). If hops exceed `maxTtl`: drop frame silently (`stat_ttl_drop`), keep pipe open. |
| **`REQ0` receives reply with unknown or expired `reqId`** | Drop reply silently (`stat_rx_nomatch` per `nng/req.c:386`) and keep pipe open. |
| **`PAIR1` frame `< 4` bytes or hop count `> 255`** | Emit `actClosePipe(pipeId, spErrProto)` per `nng/pair.c:339`. If `hopCount > maxTtl` (default 8): drop frame silently, keep pipe open. Hop count `0` is tolerated on recv. |
| **`PUB0` receives any data frame from `SUB0`** | Emit `actClosePipe(pipeId, spErrProto)` (matches `pub0_pipe_recv_cb` in `nng`). |
| **`SUB0` default topic filtering** | Default is drop-all until at least one topic is registered via `subscribe()`. Subscribing to empty string `""` acts as wildcard match for all topics. |
| **`SURVEYOR0` survey timeout** | Survey timeout cleanly completes survey collection window; async response iterator terminates without raising fatal network error. |
| **`Stream` (`tcp://` / `ipc://`) message length > `recvMax`** | Emit `actClosePipe(pipeId, spErrMsgSize)` (enforces `NNG_OPT_RECVMAXSZ`, default 1MB to prevent memory exhaustion). |
| **`WS` receives Text Frame (`opcode = 0x1`) in SP mode** | Send WebSocket Close frame `1003` (`WS_CLOSE_UNSUPP_FORMAT`) and close pipe. |
| **`WS` server receives unmasked frame or client receives masked frame** | Send WebSocket Close frame `1002` (`WS_CLOSE_PROTOCOL_ERR`) and close pipe. |
| **`TCP`/`IPC` receives invalid `\0SP\0` magic or mismatched `peer_id`** | Immediately close stream during negotiation (`spErrProto`). Byte 0 of greeting must be strictly `0x00`. |

---

## 5. Verification & Live Interop Testing Strategy

Because this machine has **Nim 2.2.12**, **Node v26.12.1** (with native browser-standard `globalThis.WebSocket`), and **C `libnng` 1.12.4** (`/opt/homebrew/opt/nng`) installed, we can verify `anynng` across four tiers with zero mocks:

1. **Tier 1 — Dual-Backend Sans-I/O Unit Tests (`nim c -r` AND `nim js -r` via Node):**
   - Run `tests/t_core_framing.nim` and `tests/t_core_protocols.nim` on **both** the C backend and the JS backend to prove 100% byte-identical behavior across `PAIR0`, `PAIR1`, `PUB0`/`SUB0`, `REQ0`/`REP0`, `PUSH0`/`PULL0`, `SURVEYOR0`/`RESPONDENT0`, and `BUS0`.
2. **Tier 2 — Pure-Nim Native Loopback Tests (`nim c -r tests/t_native_transports.nim`):**
   - Test `anynng` dialers talking to `anynng` listeners over `inproc://`, `tcp://127.0.0.1:*`, `ipc:///tmp/anynng_*.ipc`, `udp://127.0.0.1:*`, and `ws://127.0.0.1:*/sp`.
3. **Tier 3 — Cross-Target `nim js` (Browser WebSocket) $\leftrightarrow$ `nim c` (`ws://` Server) Interop:**
   - Spawn an `anynng` native `ws://` server (`REP0`, `PUB0`, `PAIR1`, `BUS0`) and connect to it from a compiled `nim js` client running in Node's native `WebSocket` engine (`node tests/js_client_test.js`).
4. **Tier 4 — Ground-Truth C `libnng` 1.12.4 Interoperability (`tests/t_interop_libnng.nim`):**
   - Link a tiny test harness against `/opt/homebrew/opt/nng/lib/libnng.dylib` and verify live wire exchanges between:
     - **Browser/JS `anynng` (`nim js`) $\leftrightarrow$ C `libnng` (`ws://` listener)** across `REQ0`/`REP0`, `PUB0`/`SUB0`, `PAIR0`, `PAIR1`, `PUSH0`/`PULL0`, `SURVEYOR0`/`RESPONDENT0`, and `BUS0`.
     - **Native `anynng` (`nim c`) $\leftrightarrow$ C `libnng`** over `tcp://`, `ipc://`, and `ws://` (both as dialer and as listener!).

---

## 6. Universal Public API Ergonomics (`src/anynng.nim`)

The public API in `src/anynng.nim` provides a unified, ergonomic, and target-agnostic interface across `nim c`, `nim js`, and `nim wasm`:

```nim
import std/[asyncdispatch, tables]
when defined(js):
  import std/asyncjs

type
  SpProto* = enum
    spPair0 = 0x10,
    spPair1 = 0x11,
    spPub0  = 0x20,
    spSub0  = 0x21,
    spReq0  = 0x30,
    spRep0  = 0x31,
    spPush0 = 0x50,
    spPull0 = 0x51,
    spSurveyor0   = 0x62,
    spRespondent0 = 0x63,
    spBus0  = 0x70

  SpOption* = enum
    optRaw,
    optRecvTimeout,
    optSendTimeout,
    optMaxTtl,
    optReconnectMin,
    optReconnectMax,
    optPair1Poly,
    optSubSubscribe,
    optSubUnsubscribe,
    optReqResendTime,
    optSurveyorSurveyTime

  Socket* = ref object
    proto*: SpProto
    raw*: bool
    maxTtl*: int
    recvTimeoutMs*: int
    sendTimeoutMs*: int

  Dialer* = ref object
    url*: string
    socket*: Socket

  Listener* = ref object
    url*: string
    socket*: Socket

  Context* = ref object
    socket*: Socket
    ctxId*: uint32

# Ergonomic Async Socket Operations
proc dial*(url: string, proto: SpProto): Future[Socket] {.async.}
proc listen*(url: string, proto: SpProto): Future[Listener] {.async.}
proc send*(s: Socket, data: openArray[byte]): Future[void] {.async.}
proc recv*(s: Socket): Future[seq[byte]] {.async.}
proc close*(s: Socket): Future[void] {.async.}

# Protocol-Specific Convenience Procs
proc subscribe*(s: Socket, topic: openArray[byte])
proc unsubscribe*(s: Socket, topic: openArray[byte])
proc newContext*(s: Socket): Context
proc send*(ctx: Context, data: openArray[byte]): Future[void] {.async.}
proc recv*(ctx: Context): Future[seq[byte]] {.async.}
proc close*(ctx: Context)
```

---

## 7. WebAssembly & TypeScript Export Bridge (`nimts`)

To allow browser and Node.js TypeScript/JavaScript projects to use `anynng` without requiring a full Nim toolchain, `src/anynng/core/` integrates with `nimts` (`/Users/eek/Development/nimts`):

1. **`{.exportts.}` Macro Layer**:
   ```nim
   import nimts

   setNimTsModule("anynng")

   proc wasmCreateEngine*(protoId: int): int {.exportts.}
   proc wasmFeedFrame*(engineId: int, framePtr: NimTsView[byte]): int {.exportts.}
   proc wasmEncodeReq*(reqId: uint32, payload: NimTsView[byte]): NimTsView[byte] {.exportts.}
   proc wasmEncodePair1*(hopCount: uint32, payload: NimTsView[byte]): NimTsView[byte] {.exportts.}
   ```
2. **TypeScript Runtime Packaging (`ts/index.ts`, `ts/types.ts`)**:
   Emits idiomatic TypeScript classes wrapping the WASM exports alongside the native WebSocket transport:
   ```typescript
   import { AnynngWasmRuntime } from './runtime';

   export interface SocketOptions {
     recvTimeoutMs?: number;
     sendTimeoutMs?: number;
     maxTtl?: number;
     resendTimeMs?: number;
     surveyTimeMs?: number;
   }

   export abstract class BaseSocket {
     protected ws?: WebSocket;
     abstract send(payload: Uint8Array): Promise<void>;
     abstract recv(): Promise<Uint8Array>;
     abstract close(): void;
   }

   export class Req0Socket extends BaseSocket {
     constructor(private url: string, private opts?: SocketOptions) { super(); }
     async connect(): Promise<void> { /* ws connect with dialerSubprotocol('req0') */ }
     async request(payload: Uint8Array): Promise<Uint8Array> { /* send with 0x80000000 reqId */ }
     async send(payload: Uint8Array): Promise<void> { /* ... */ }
     async recv(): Promise<Uint8Array> { /* ... */ }
     close(): void { this.ws?.close(); }
   }

   export class Rep0Socket extends BaseSocket {
     constructor(private url: string, private opts?: SocketOptions) { super(); }
     async reply(payload: Uint8Array): Promise<void> { /* echo backtrace stack */ }
     async send(payload: Uint8Array): Promise<void> { /* ... */ }
     async recv(): Promise<Uint8Array> { /* ... */ }
     close(): void { this.ws?.close(); }
   }

   export class Pub0Socket extends BaseSocket {
     constructor(private url: string, private opts?: SocketOptions) { super(); }
     async publish(topic: string | Uint8Array, body: Uint8Array): Promise<void> { /* prefix + body */ }
     async send(payload: Uint8Array): Promise<void> { /* ... */ }
     async recv(): Promise<Uint8Array> { throw new Error('PUB0 cannot receive'); }
     close(): void { this.ws?.close(); }
   }

   export class Sub0Socket extends BaseSocket {
     constructor(private url: string, private opts?: SocketOptions) { super(); }
     subscribe(topic: string | Uint8Array): void { /* client-side prefix filter */ }
     unsubscribe(topic: string | Uint8Array): void { /* remove prefix filter */ }
     async send(payload: Uint8Array): Promise<void> { throw new Error('SUB0 cannot send to PUB0'); }
     async recv(): Promise<Uint8Array> { /* ... */ }
     close(): void { this.ws?.close(); }
   }

   export class Pair1Socket extends BaseSocket {
     constructor(private url: string, private opts?: SocketOptions) { super(); }
     async send(payload: Uint8Array): Promise<void> { /* prepend hop 1 */ }
     async recv(): Promise<Uint8Array> { /* strip hop */ }
     close(): void { this.ws?.close(); }
   }

   export class Bus0Socket extends BaseSocket {
     constructor(private url: string, private opts?: SocketOptions) { super(); }
     async send(payload: Uint8Array): Promise<void> { /* broadcast to all */ }
     async recv(): Promise<Uint8Array> { /* local recv */ }
     close(): void { this.ws?.close(); }
   }
   ```

---

## 8. Polyform Template Enhancements Specification

Based on empirical inspection of `/Users/eek/Development/polyform/copier.yml` and `/Users/eek/Development/polyform/template/`, the template must be enhanced with the following multi-target capabilities:

1. **`has_nim_js` (or `has_js`) Facet**:
   - In `copier.yml`:
     ```yaml
     has_nim_js:
       type: bool
       help: "Include Nim JavaScript compilation support (Node.js/browser targets, just test-js)?"
       default: "{{ has_nim and (has_typescript or flavor in ['nim_ts_wasm', 'custom']) }}"
       when: "{{ has_nim }}"
     ```
   - In `justfile`:
     ```just
     # Run JavaScript backend tests under Node.js
     test-js:
         nim js -d:nodejs -r tests/test_js.nim
     ```
   - In `.github/workflows/ci.yml`:
     - Adds a `test-js` matrix job executing `nim js -d:nodejs -r tests/test_js.nim`.

2. **`nimts` First-Class Integration in `ts_bridge_strategy`**:
   - In `copier.yml`:
     ```yaml
     ts_bridge_strategy:
       type: str
       help: "TypeScript bridge integration strategy"
       choices:
         "nimts (Zero-overhead WebAssembly bridge + TypeScript AST/types)": "nimts"
         "WebAssembly (WASM compiler, browser/Node runtime)": "wasm"
         "N-API (Node native addon bindings)": "napi"
         "None (Independent parallel TypeScript package)": "none"
       when: "{{ has_nim and has_typescript }}"
       default: "nimts"
     ```
   - Generates `@axiomantic/nimts` devDependency in `package.json`, automated `compile_wasm.sh` using `nimts compile`, and Vite plugin hooks.

---

## 9. Adversarial Invariant Matrix & Quality Gates

| Invariant | Enforcement Mechanism | Verification |
| :--- | :--- | :--- |
| **Zero Mock Testing** | Every protocol test runs against real serialization buffers; live tests run against Homebrew C `libnng` 1.12.4. | `tests/t_interop_libnng.nim` via `-d:test_c_interop` |
| **Strict Subprotocol Asymmetry** | Dialers send peer's protocol name; NNG HTTP upgrade parser matches exact string. | Verified against `websocket.c` `wstran_dialer_init()` |
| **Single Subprotocol Token** | Reject array of subprotocols in client WebSocket instantiation to avoid NNG 400 Bad Request. | Unit tested in `tests/t_core_framing.nim` |
| **Binary Message Framing** | Text frames strictly prohibited; trigger close code 1003. | Checked in `ws_framing.nim` |
| **Strict TTL / Hop Validation** | Frames with hop count > 255 or invalid format trigger immediate pipe termination. | Checked in `pair1.nim` and `reqrep0.nim` |
| **Client-Side SUB0 Filtering** | Reject any outgoing data frame from SUB0 to PUB0 to prevent pipe teardown. | Enforced in `pubsub0.nim` state machine |
