/**
 * anynng - Pure-Nim multi-target (C, JS, WASM) and multi-transport (WS, TCP, IPC, UDP, Inproc) NNG / Scalability Protocols implementation
 * TypeScript + WebAssembly Entrypoint
 * Licensed under MIT License.
 */

export * from "./types.js";
export * from "./runtime.js";
import { AnynngBridge } from "./runtime.js";
import {
  EngineEventKind,
  EngineActionKind,
  SP_PROTO_IDS,
  type SocketOptions,
  type SpProtoName,
  type PipeNotifyHook,
  type ContextOptions,
  type DialerOptions,
  type EngineAction,
} from "./types.js";

let sharedBridgePromise: Promise<AnynngBridge> | null = null;

export function getSharedBridge(): Promise<AnynngBridge> {
  if (!sharedBridgePromise) {
    sharedBridgePromise = createAnynng();
  }
  return sharedBridgePromise;
}

export function dialerSubprotocol(proto: SpProtoName): string {
  switch (proto) {
    case "pair0":
      return "pair.sp.nanomsg.org";
    case "pair1":
      return "pair1.sp.nanomsg.org";
    case "pub0":
      return "sub.sp.nanomsg.org";
    case "sub0":
      return "pub.sp.nanomsg.org";
    case "req0":
      return "rep.sp.nanomsg.org";
    case "rep0":
      return "req.sp.nanomsg.org";
    case "push0":
      return "pull.sp.nanomsg.org";
    case "pull0":
      return "push.sp.nanomsg.org";
    case "surveyor0":
      return "respondent.sp.nanomsg.org";
    case "respondent0":
      return "surveyor.sp.nanomsg.org";
    case "bus0":
      return "bus.sp.nanomsg.org";
  }
}

export function listenerSubprotocol(proto: SpProtoName): string {
  switch (proto) {
    case "pair0":
      return "pair.sp.nanomsg.org";
    case "pair1":
      return "pair1.sp.nanomsg.org";
    case "pub0":
      return "pub.sp.nanomsg.org";
    case "sub0":
      return "sub.sp.nanomsg.org";
    case "req0":
      return "req.sp.nanomsg.org";
    case "rep0":
      return "rep.sp.nanomsg.org";
    case "push0":
      return "push.sp.nanomsg.org";
    case "pull0":
      return "pull.sp.nanomsg.org";
    case "surveyor0":
      return "surveyor.sp.nanomsg.org";
    case "respondent0":
      return "respondent.sp.nanomsg.org";
    case "bus0":
      return "bus.sp.nanomsg.org";
  }
}

interface Waiter {
  resolve: (data: Uint8Array) => void;
  reject: (err: Error) => void;
  timer?: ReturnType<typeof setTimeout>;
}

// ----------------------------------------------------------------------
// Context Class (Concurrent Multiplexing)
// ----------------------------------------------------------------------

export class Context {
  protected recvQueue: Uint8Array[] = [];
  protected recvWaiters: Waiter[] = [];
  public isClosed = false;

  constructor(
    public readonly id: number,
    public readonly socket: BaseSocket,
    public readonly opts?: ContextOptions,
  ) {}

  async send(payload: Uint8Array): Promise<void> {
    if (this.isClosed) throw new Error("Context is closed");
    await this.socket.contextSend(this, payload);
  }

  async recv(timeoutMs?: number): Promise<Uint8Array> {
    if (this.isClosed) throw new Error("Context is closed");
    if (this.recvQueue.length > 0) return this.recvQueue.shift()!;
    return new Promise<Uint8Array>((resolve, reject) => {
      const waiter: Waiter = { resolve, reject };
      const timeout = timeoutMs ?? this.opts?.recvTimeoutMs;
      if (timeout && timeout > 0) {
        waiter.timer = setTimeout(() => {
          const idx = this.recvWaiters.indexOf(waiter);
          if (idx !== -1) {
            this.recvWaiters.splice(idx, 1);
            reject(new Error("Context receive timeout"));
          }
        }, timeout);
      }
      this.recvWaiters.push(waiter);
    });
  }

  onMessage(data: Uint8Array): void {
    if (this.recvWaiters.length > 0) {
      const waiter = this.recvWaiters.shift()!;
      if (waiter.timer) clearTimeout(waiter.timer);
      waiter.resolve(data);
    } else {
      this.recvQueue.push(data);
    }
  }

  close(): void {
    this.isClosed = true;
    for (const waiter of this.recvWaiters) {
      if (waiter.timer) clearTimeout(waiter.timer);
      waiter.reject(new Error("Context closed"));
    }
    this.recvWaiters = [];
    this.recvQueue = [];
    this.socket.closeContext(this.id);
  }
}

// ----------------------------------------------------------------------
// Dialer Class (Two-Step Lifecycle & Auto-Reconnect)
// ----------------------------------------------------------------------

export class Dialer {
  private active = false;
  private currentBackoffMs: number;
  private reconnectTimer?: ReturnType<typeof setTimeout>;

  constructor(
    public readonly socket: BaseSocket,
    public readonly url: string,
    public readonly opts?: DialerOptions,
  ) {
    this.currentBackoffMs = opts?.reconnectTimeMinMs ?? 100;
  }

  async start(): Promise<void> {
    this.active = true;
    await this.socket.connectTo(this.url);
  }

  handleClose(): void {
    if (!this.active || this.opts?.autoReconnect === false) return;
    const max = this.opts?.reconnectTimeMaxMs ?? 10000;
    const delay = this.currentBackoffMs;
    this.currentBackoffMs = Math.min(this.currentBackoffMs * 2, max);
    this.reconnectTimer = setTimeout(async () => {
      if (this.active) {
        try {
          await this.start();
        } catch {
          // Reconnect cycle continues on close/failure
        }
      }
    }, delay);
  }

  close(): void {
    this.active = false;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }
    this.socket.close();
  }
}

// ----------------------------------------------------------------------
// Base Socket Class
// ----------------------------------------------------------------------

export abstract class BaseSocket {
  protected ws?: WebSocket;
  protected bridge?: AnynngBridge;
  protected engineHandle = 0;
  protected recvQueue: Uint8Array[] = [];
  protected recvWaiters: Waiter[] = [];
  protected isClosed = false;
  protected pipeNotifyHook?: PipeNotifyHook;
  protected currentPipeId = 1;
  protected currentPipeIdVal = 0;
  protected contexts = new Map<number, Context>();
  protected nextCtxId = 2;
  protected activeDialers: Dialer[] = [];

  // Options Store
  protected options: Map<string, unknown> = new Map();

  constructor(
    public url: string,
    public readonly opts?: SocketOptions,
    public readonly proto: SpProtoName = "pair0",
  ) {
    this.options.set("maxTtl", opts?.maxTtl ?? 8);
    this.options.set("polyamorous", opts?.polyamorous ?? false);
    this.options.set("recvMaxSize", opts?.recvMaxSize ?? 1024 * 1024);
    this.options.set("raw", opts?.raw ?? false);
    this.options.set("resendTime", opts?.resendTimeMs ?? 0);
    this.options.set("surveyTime", opts?.surveyTimeMs ?? 1000);
    this.options.set("reconnectTimeMin", opts?.reconnectTimeMinMs ?? 100);
    this.options.set("reconnectTimeMax", opts?.reconnectTimeMaxMs ?? 10000);
  }

  get dialerSubprotocol(): string {
    return dialerSubprotocol(this.proto);
  }

  pipeNotify(hook: PipeNotifyHook): void {
    this.pipeNotifyHook = hook;
  }

  setOpt(name: string, val: unknown): void {
    this.options.set(name, val);
    if (this.bridge && this.engineHandle !== 0) {
      if (typeof val === "number" || typeof val === "boolean") {
        this.bridge.engineSetOpt(this.engineHandle, name, val);
      }
    }
  }

  getOpt(name: string): unknown {
    return this.options.get(name);
  }

  openContext(opts?: ContextOptions): Context {
    const id = this.nextCtxId++;
    const ctx = new Context(id, this, opts);
    this.contexts.set(id, ctx);
    return ctx;
  }

  closeContext(ctxId: number): void {
    this.contexts.delete(ctxId);
    if (this.bridge && this.engineHandle !== 0) {
      this.bridge.engineStep(
        this.engineHandle,
        EngineEventKind.evUserCancel,
        0,
        ctxId,
      );
    }
  }

  newDialer(url?: string, opts?: DialerOptions): Dialer {
    const dialer = new Dialer(this, url ?? this.url, opts);
    this.activeDialers.push(dialer);
    return dialer;
  }

  async getBridge(): Promise<AnynngBridge> {
    if (!this.bridge) {
      this.bridge = await getSharedBridge();
    }
    return this.bridge;
  }

  async ensureEngine(): Promise<AnynngBridge> {
    const bridge = await this.getBridge();
    if (this.engineHandle === 0) {
      const protoId = SP_PROTO_IDS[this.proto];
      const raw = (this.getOpt("raw") as boolean) ?? false;
      const maxTtl = (this.getOpt("maxTtl") as number) ?? 8;
      const poly = (this.getOpt("polyamorous") as boolean) ?? false;
      const resendMs = (this.getOpt("resendTime") as number) ?? 0;
      const surveyMs = (this.getOpt("surveyTime") as number) ?? 1000;
      this.engineHandle = bridge.engineCreate(
        protoId,
        raw,
        maxTtl,
        poly,
        resendMs,
        surveyMs,
      );
    }
    return bridge;
  }

  async connectTo(targetUrl: string, bridge?: AnynngBridge): Promise<void> {
    this.url = targetUrl;
    await this.connect(bridge);
  }

  async connect(bridge?: AnynngBridge): Promise<void> {
    if (bridge) {
      this.bridge = bridge;
    } else if (!this.bridge) {
      this.bridge = await getSharedBridge();
    }
    await this.ensureEngine();

    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      return;
    }

    return new Promise<void>((resolve, reject) => {
      const subprotocol = this.dialerSubprotocol;
      const ws = new globalThis.WebSocket(this.url, subprotocol);
      ws.binaryType = "arraybuffer";
      this.ws = ws;

      const pipeId = this.currentPipeId++;
      this.currentPipeIdVal = pipeId;

      const onOpen = () => {
        cleanup();
        if (this.pipeNotifyHook) {
          const accepted = this.pipeNotifyHook(pipeId, "add_pre");
          if (accepted === false) {
            ws.close();
            if (this.pipeNotifyHook) {
              this.pipeNotifyHook(pipeId, "rem_post");
            }
            reject(new Error("Connection rejected by pipeNotify ADD_PRE"));
            return;
          }
        }
        const actions = this.bridge!.engineStep(
          this.engineHandle,
          EngineEventKind.evPipeAdded,
          pipeId,
        );
        this.dispatchActions(actions);
        if (this.pipeNotifyHook) {
          this.pipeNotifyHook(pipeId, "add_post");
        }
        resolve();
      };

      const onError = (evt: unknown) => {
        cleanup();
        const err = evt as { message?: string };
        reject(
          new Error(`WebSocket connection failed: ${err?.message ?? "error"}`),
        );
      };

      const cleanup = () => {
        ws.removeEventListener("open", onOpen);
        ws.removeEventListener("error", onError);
      };

      ws.addEventListener("open", onOpen);
      ws.addEventListener("error", onError);

      ws.addEventListener("message", (evt: MessageEvent) => {
        let data: Uint8Array;
        if (evt.data instanceof ArrayBuffer) {
          data = new Uint8Array(evt.data);
        } else if (ArrayBuffer.isView(evt.data)) {
          data = new Uint8Array(
            evt.data.buffer,
            evt.data.byteOffset,
            evt.data.byteLength,
          );
        } else if (typeof evt.data === "string") {
          data = new TextEncoder().encode(evt.data);
        } else {
          return;
        }

        const maxSz = (this.getOpt("recvMaxSize") as number) ?? 1024 * 1024;
        if (data.length > maxSz) {
          return;
        }

        if (this.getOpt("raw") === true) {
          this.onRawMessage(data);
          return;
        }

        const actions = this.bridge!.engineStep(
          this.engineHandle,
          EngineEventKind.evPipeMessage,
          this.currentPipeIdVal,
          0,
          data,
          Date.now(),
        );
        this.dispatchActions(actions);
      });

      ws.addEventListener("close", () => {
        if (this.bridge && this.engineHandle !== 0) {
          const actions = this.bridge.engineStep(
            this.engineHandle,
            EngineEventKind.evPipeRemoved,
            this.currentPipeIdVal,
            0,
            new Uint8Array(0),
            Date.now(),
          );
          this.dispatchActions(actions);
        }
        if (this.pipeNotifyHook) {
          this.pipeNotifyHook(pipeId, "rem_post");
        }
        this.onClose();
        for (const dialer of this.activeDialers) {
          dialer.handleClose();
        }
      });
    });
  }

  protected dispatchActions(actions: EngineAction[]): void {
    for (const a of actions) {
      switch (a.kind) {
        case EngineActionKind.actSendPipe:
          if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(a.data);
          }
          break;
        case EngineActionKind.actDeliverRecv:
          if (a.ctxId > 1) {
            const ctx = this.contexts.get(a.ctxId);
            if (ctx) {
              ctx.onMessage(a.data);
              break;
            }
          }
          this.onRawMessage(a.data);
          break;
        case EngineActionKind.actClosePipe:
          if (this.ws) {
            try {
              this.ws.close();
            } catch {
              // ignore
            }
          }
          break;
        case EngineActionKind.actFinishSend:
        case EngineActionKind.actFailContext:
          break;
      }
    }
  }

  protected async ensureConnected(): Promise<void> {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      await this.connect();
    }
  }

  protected onRawMessage(data: Uint8Array): void {
    if (this.recvWaiters.length > 0) {
      const waiter = this.recvWaiters.shift()!;
      if (waiter.timer) clearTimeout(waiter.timer);
      waiter.resolve(data);
    } else {
      this.recvQueue.push(data);
    }
  }

  protected onClose(): void {
    this.isClosed = true;
    for (const waiter of this.recvWaiters) {
      if (waiter.timer) clearTimeout(waiter.timer);
      waiter.reject(new Error("Socket closed"));
    }
    this.recvWaiters = [];
  }

  protected async rawSend(data: Uint8Array): Promise<void> {
    await this.ensureConnected();
    if (this.isClosed || !this.ws || this.ws.readyState !== WebSocket.OPEN) {
      throw new Error("Socket is not open");
    }
    this.ws.send(data);
  }

  protected async rawRecv(timeoutMs?: number): Promise<Uint8Array> {
    await this.ensureConnected();
    if (this.recvQueue.length > 0) {
      return this.recvQueue.shift()!;
    }
    if (this.isClosed) {
      throw new Error("Socket is closed");
    }

    return new Promise<Uint8Array>((resolve, reject) => {
      const waiter: Waiter = { resolve, reject };
      const timeout = timeoutMs ?? this.opts?.recvTimeoutMs;
      if (timeout && timeout > 0) {
        waiter.timer = setTimeout(() => {
          const idx = this.recvWaiters.indexOf(waiter);
          if (idx !== -1) {
            this.recvWaiters.splice(idx, 1);
            reject(new Error("Receive timeout"));
          }
        }, timeout);
      }
      this.recvWaiters.push(waiter);
    });
  }

  async contextSend(ctx: Context, payload: Uint8Array): Promise<void> {
    await this.ensureConnected();
    if (this.getOpt("raw") === true) {
      await this.rawSend(payload);
      return;
    }
    const bridge = await this.ensureEngine();
    const actions = bridge.engineStep(
      this.engineHandle,
      EngineEventKind.evUserSend,
      0,
      ctx.id,
      payload,
      Date.now(),
    );
    this.dispatchActions(actions);
  }

  async send(payload: Uint8Array): Promise<void> {
    await this.ensureConnected();
    if (this.getOpt("raw") === true) {
      await this.rawSend(payload);
      return;
    }
    const bridge = await this.ensureEngine();
    const actions = bridge.engineStep(
      this.engineHandle,
      EngineEventKind.evUserSend,
      0,
      1,
      payload,
      Date.now(),
    );
    this.dispatchActions(actions);
  }

  async recv(timeoutMs?: number): Promise<Uint8Array> {
    return await this.rawRecv(timeoutMs);
  }

  close(): void {
    this.isClosed = true;
    for (const waiter of this.recvWaiters) {
      if (waiter.timer) clearTimeout(waiter.timer);
      waiter.reject(new Error("Socket closed"));
    }
    this.recvWaiters = [];
    this.recvQueue = [];
    for (const ctx of this.contexts.values()) {
      ctx.close();
    }
    this.contexts.clear();
    if (this.bridge && this.engineHandle !== 0) {
      this.bridge.engineDestroy(this.engineHandle);
      this.engineHandle = 0;
    }
    if (this.ws) {
      try {
        this.ws.close();
      } catch {
        // ignore
      }
      this.ws = undefined;
    }
  }
}

// ----------------------------------------------------------------------
// SP Socket Implementations
// ----------------------------------------------------------------------

export class Pair0Socket extends BaseSocket {
  constructor(url: string, opts?: SocketOptions) {
    super(url, opts, "pair0");
  }
}

export class Pair1Socket extends BaseSocket {
  constructor(url: string, opts?: SocketOptions) {
    super(url, opts, "pair1");
  }

  override async send(payload: Uint8Array, hops = 0): Promise<void> {
    if (this.getOpt("raw") === true) {
      await this.rawSend(payload);
      return;
    }
    await this.ensureConnected();
    const bridge = await this.ensureEngine();
    let data = payload;
    if (hops > 0) {
      data = bridge.pair1Encode(hops, payload);
    }
    const actions = bridge.engineStep(
      this.engineHandle,
      EngineEventKind.evUserSend,
      0,
      1,
      data,
      Date.now(),
    );
    this.dispatchActions(actions);
  }
}

export class Pub0Socket extends BaseSocket {
  constructor(url: string, opts?: SocketOptions) {
    super(url, opts, "pub0");
  }

  override async recv(): Promise<Uint8Array> {
    throw new Error("PUB0 cannot receive");
  }
}

export class Sub0Socket extends BaseSocket {
  private pendingSubscriptions: (string | Uint8Array)[] = [];

  constructor(url: string, opts?: SocketOptions) {
    super(url, opts, "sub0");
  }

  override async connect(bridge?: AnynngBridge): Promise<void> {
    await super.connect(bridge);
    for (const topic of this.pendingSubscriptions) {
      this.bridge!.engineSubscribe(this.engineHandle, topic);
    }
    this.pendingSubscriptions = [];
  }

  subscribe(topic: string | Uint8Array): void {
    if (this.bridge && this.engineHandle !== 0) {
      this.bridge.engineSubscribe(this.engineHandle, topic);
    } else {
      this.pendingSubscriptions.push(topic);
    }
  }

  unsubscribe(topic: string | Uint8Array): void {
    if (this.bridge && this.engineHandle !== 0) {
      this.bridge.engineUnsubscribe(this.engineHandle, topic);
    }
  }

  override async send(_payload: Uint8Array): Promise<void> {
    throw new Error("SUB0 cannot send");
  }
}

export class Req0Socket extends BaseSocket {
  private resendTimer?: ReturnType<typeof setInterval>;

  constructor(url: string, opts?: SocketOptions) {
    super(url, opts, "req0");
  }

  override async connect(bridge?: AnynngBridge): Promise<void> {
    await super.connect(bridge);
    const resendMs = (this.getOpt("resendTime") as number) ?? 0;
    if (resendMs > 0 && !this.resendTimer) {
      this.resendTimer = setInterval(() => {
        if (!this.isClosed && this.bridge && this.engineHandle !== 0) {
          const acts = this.bridge.engineStep(
            this.engineHandle,
            EngineEventKind.evTimerTick,
            0,
            0,
            new Uint8Array(0),
            Date.now(),
          );
          this.dispatchActions(acts);
        }
      }, Math.max(10, Math.floor(resendMs / 2)));
    }
  }

  override close(): void {
    if (this.resendTimer) {
      clearInterval(this.resendTimer);
      this.resendTimer = undefined;
    }
    super.close();
  }
}

export class Rep0Socket extends BaseSocket {
  constructor(url: string, opts?: SocketOptions) {
    super(url, opts, "rep0");
  }
}

export class Push0Socket extends BaseSocket {
  constructor(url: string, opts?: SocketOptions) {
    super(url, opts, "push0");
  }

  override async recv(): Promise<Uint8Array> {
    throw new Error("PUSH0 cannot receive");
  }
}

export class Pull0Socket extends BaseSocket {
  constructor(url: string, opts?: SocketOptions) {
    super(url, opts, "pull0");
  }

  override async send(_payload: Uint8Array): Promise<void> {
    throw new Error("PULL0 cannot send");
  }
}

export class Surveyor0Socket extends BaseSocket {
  constructor(url: string, opts?: SocketOptions) {
    super(url, opts, "surveyor0");
  }

  override async recv(timeoutMs?: number): Promise<Uint8Array> {
    const surveyTime = (this.getOpt("surveyTime") as number) ?? 1000;
    const timeout = timeoutMs ?? surveyTime;
    return await this.rawRecv(timeout);
  }
}

export class Respondent0Socket extends BaseSocket {
  constructor(url: string, opts?: SocketOptions) {
    super(url, opts, "respondent0");
  }
}

export class Bus0Socket extends BaseSocket {
  constructor(url: string, opts?: SocketOptions) {
    super(url, opts, "bus0");
  }
}

export async function createAnynng(
  options?: import("./types.js").BridgeOptions,
): Promise<AnynngBridge> {
  const bridge = new AnynngBridge();
  return await bridge.init(options);
}

export default createAnynng;
