/**
 * anynng - Pure-Nim multi-target (C, JS, WASM) and multi-transport (WS, TCP, IPC, UDP, Inproc) NNG / Scalability Protocols implementation
 * TypeScript + WebAssembly Entrypoint
 * Licensed under MIT License.
 */

export * from "./types.js";
export * from "./runtime.js";
import { AnynngBridge } from "./runtime.js";
import type {
  SocketOptions,
  SpProtoName,
  PipeNotifyHook,
  ContextOptions,
  DialerOptions,
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
  protected recvQueue: Uint8Array[] = [];
  protected recvWaiters: Waiter[] = [];
  protected isClosed = false;
  protected pipeNotifyHook?: PipeNotifyHook;
  protected currentPipeId = 1;
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

    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      return;
    }

    return new Promise<void>((resolve, reject) => {
      const subprotocol = this.dialerSubprotocol;
      const ws = new globalThis.WebSocket(this.url, subprotocol);
      ws.binaryType = "arraybuffer";
      this.ws = ws;

      const pipeId = this.currentPipeId++;

      const onOpen = () => {
        cleanup();
        if (this.pipeNotifyHook) {
          const accepted = this.pipeNotifyHook(pipeId, "add_pre");
          if (accepted === false) {
            // Rejected by ADD_PRE! Close immediately without accepting pipe
            ws.close();
            if (this.pipeNotifyHook) {
              this.pipeNotifyHook(pipeId, "rem_post");
            }
            reject(new Error("Connection rejected by pipeNotify ADD_PRE"));
            return;
          }
        }
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
          return; // Drop message exceeding bounded buffer limit
        }

        this.onRawMessage(data);
      });

      ws.addEventListener("close", () => {
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

  // Hook for context-specific send (overridden in Req0Socket, Rep0Socket, etc.)
  async contextSend(_ctx: Context, payload: Uint8Array): Promise<void> {
    await this.send(payload);
  }

  abstract send(payload: Uint8Array): Promise<void>;
  abstract recv(): Promise<Uint8Array>;

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

  async send(payload: Uint8Array): Promise<void> {
    await this.rawSend(payload);
  }

  async recv(): Promise<Uint8Array> {
    return await this.rawRecv();
  }
}

export class Pair1Socket extends BaseSocket {
  constructor(url: string, opts?: SocketOptions) {
    super(url, opts, "pair1");
  }

  async send(payload: Uint8Array, hops = 0): Promise<void> {
    if (this.getOpt("raw") === true) {
      await this.rawSend(payload);
      return;
    }
    const bridge = await this.getBridge();
    const frame = bridge.pair1Encode(hops, payload);
    await this.rawSend(frame);
  }

  async recv(): Promise<Uint8Array> {
    const raw = await this.rawRecv();
    if (this.getOpt("raw") === true) {
      return raw;
    }
    const bridge = await this.getBridge();
    const decoded = bridge.pair1Decode(raw);
    const maxTtl = (this.getOpt("maxTtl") as number) ?? 8;
    if (decoded.hops > maxTtl) {
      throw new Error(`PAIR1 TTL exceeded: hops=${decoded.hops} max=${maxTtl}`);
    }
    return decoded.body;
  }
}

export class Pub0Socket extends BaseSocket {
  constructor(url: string, opts?: SocketOptions) {
    super(url, opts, "pub0");
  }

  async send(payload: Uint8Array): Promise<void> {
    await this.rawSend(payload);
  }

  async recv(): Promise<Uint8Array> {
    throw new Error("PUB0 cannot receive");
  }
}

export class Sub0Socket extends BaseSocket {
  private subscriptions: Uint8Array[] = [];

  constructor(url: string, opts?: SocketOptions) {
    super(url, opts, "sub0");
  }

  subscribe(topic: string | Uint8Array): void {
    const bytes =
      typeof topic === "string" ? new TextEncoder().encode(topic) : topic;
    this.subscriptions.push(bytes);
  }

  unsubscribe(topic: string | Uint8Array): void {
    const bytes =
      typeof topic === "string" ? new TextEncoder().encode(topic) : topic;
    this.subscriptions = this.subscriptions.filter(
      (sub) =>
        sub.length !== bytes.length ||
        !sub.every((val, idx) => val === bytes[idx]),
    );
  }

  async send(_payload: Uint8Array): Promise<void> {
    throw new Error("SUB0 cannot send");
  }

  async recv(): Promise<Uint8Array> {
    const bridge = await this.getBridge();
    while (true) {
      const raw = await this.rawRecv();
      if (this.subscriptions.length === 0) {
        return raw;
      }
      for (const sub of this.subscriptions) {
        if (bridge.sub0Matches(sub, raw)) {
          return raw;
        }
      }
      // Non-matching topic dropped, continue loop
    }
  }
}

interface PendingReq {
  reqId: number;
  ctx?: Context;
  timer?: ReturnType<typeof setInterval>;
  payload: Uint8Array;
}

export class Req0Socket extends BaseSocket {
  private nextReqId = 1;
  private pendingReqs = new Map<number, PendingReq>();

  constructor(url: string, opts?: SocketOptions) {
    super(url, opts, "req0");
  }

  override async contextSend(ctx: Context, payload: Uint8Array): Promise<void> {
    const bridge = await this.getBridge();
    const reqId = this.nextReqId++ & 0x7fffffff;
    const frame = bridge.req0Encode(reqId, payload);

    const pending: PendingReq = { reqId, ctx, payload };
    const resendMs =
      ctx.opts?.resendTimeMs ?? (this.getOpt("resendTime") as number) ?? 0;
    if (resendMs > 0) {
      pending.timer = setInterval(async () => {
        if (!this.isClosed && !ctx.isClosed && this.pendingReqs.has(reqId)) {
          try {
            await this.rawSend(frame);
          } catch {
            // Ignore send error on resend
          }
        }
      }, resendMs);
    }
    this.pendingReqs.set(reqId, pending);
    await this.rawSend(frame);
  }

  async send(payload: Uint8Array): Promise<void> {
    if (this.getOpt("raw") === true) {
      await this.rawSend(payload);
      return;
    }
    const bridge = await this.getBridge();
    const reqId = this.nextReqId++ & 0x7fffffff;
    const frame = bridge.req0Encode(reqId, payload);

    const pending: PendingReq = { reqId, payload };
    const resendMs = (this.getOpt("resendTime") as number) ?? 0;
    if (resendMs > 0) {
      pending.timer = setInterval(async () => {
        if (!this.isClosed && this.pendingReqs.has(reqId)) {
          try {
            await this.rawSend(frame);
          } catch {
            // Ignore send error on resend
          }
        }
      }, resendMs);
    }
    this.pendingReqs.set(reqId, pending);
    await this.rawSend(frame);
  }

  protected override onRawMessage(data: Uint8Array): void {
    if (this.getOpt("raw") === true) {
      super.onRawMessage(data);
      return;
    }
    if (!this.bridge) {
      super.onRawMessage(data);
      return;
    }

    try {
      const decoded = this.bridge.req0Decode(data);
      const pending = this.pendingReqs.get(decoded.reqId);
      if (pending) {
        if (pending.timer) clearInterval(pending.timer);
        this.pendingReqs.delete(decoded.reqId);
        if (pending.ctx) {
          pending.ctx.onMessage(decoded.body);
          return;
        }
        // Deliver to socket
        super.onRawMessage(decoded.body);
        return;
      }
    } catch {
      // Decode error or dropped
    }
  }

  async recv(): Promise<Uint8Array> {
    return await this.rawRecv();
  }

  override close(): void {
    for (const pending of this.pendingReqs.values()) {
      if (pending.timer) clearInterval(pending.timer);
    }
    this.pendingReqs.clear();
    super.close();
  }
}

export class Rep0Socket extends BaseSocket {
  private lastReqId?: number;

  constructor(url: string, opts?: SocketOptions) {
    super(url, opts, "rep0");
  }

  async recv(): Promise<Uint8Array> {
    const raw = await this.rawRecv();
    if (this.getOpt("raw") === true) {
      return raw;
    }
    const bridge = await this.getBridge();
    const decoded = bridge.req0Decode(raw);
    this.lastReqId = decoded.reqId;
    return decoded.body;
  }

  async send(payload: Uint8Array): Promise<void> {
    if (this.getOpt("raw") === true) {
      await this.rawSend(payload);
      return;
    }
    if (this.lastReqId === undefined) {
      throw new Error("Cannot send REP0 reply without preceding request");
    }
    const bridge = await this.getBridge();
    const frame = bridge.rep0Encode(this.lastReqId, payload);
    this.lastReqId = undefined;
    await this.rawSend(frame);
  }
}

export class Push0Socket extends BaseSocket {
  constructor(url: string, opts?: SocketOptions) {
    super(url, opts, "push0");
  }

  async send(payload: Uint8Array): Promise<void> {
    await this.rawSend(payload);
  }

  async recv(): Promise<Uint8Array> {
    throw new Error("PUSH0 cannot receive");
  }
}

export class Pull0Socket extends BaseSocket {
  constructor(url: string, opts?: SocketOptions) {
    super(url, opts, "pull0");
  }

  async send(_payload: Uint8Array): Promise<void> {
    throw new Error("PULL0 cannot send");
  }

  async recv(): Promise<Uint8Array> {
    return await this.rawRecv();
  }
}

export class Surveyor0Socket extends BaseSocket {
  private nextSurveyId = 1;
  private currentSurveyId?: number;
  private surveyDeadline = 0;

  constructor(url: string, opts?: SocketOptions) {
    super(url, opts, "surveyor0");
  }

  async send(payload: Uint8Array): Promise<void> {
    const bridge = await this.getBridge();
    this.currentSurveyId = this.nextSurveyId++ & 0x7fffffff;
    const duration = (this.getOpt("surveyTime") as number) ?? 1000;
    this.surveyDeadline = Date.now() + duration;
    const frame = bridge.survey0Encode(this.currentSurveyId, payload);
    await this.rawSend(frame);
  }

  async recv(): Promise<Uint8Array> {
    if (this.currentSurveyId === undefined) {
      throw new Error("Cannot receive on SURVEYOR0 without active survey");
    }
    const remaining = this.surveyDeadline - Date.now();
    if (remaining <= 0) {
      this.currentSurveyId = undefined;
      throw new Error("Survey timed out");
    }
    const bridge = await this.getBridge();
    while (true) {
      const raw = await this.rawRecv(remaining);
      const decoded = bridge.survey0Decode(raw);
      if (decoded.surveyId === this.currentSurveyId) {
        return decoded.body;
      }
      // Non-matching survey response dropped
    }
  }
}

export class Respondent0Socket extends BaseSocket {
  private lastSurveyId?: number;

  constructor(url: string, opts?: SocketOptions) {
    super(url, opts, "respondent0");
  }

  async recv(): Promise<Uint8Array> {
    const raw = await this.rawRecv();
    const bridge = await this.getBridge();
    const decoded = bridge.survey0Decode(raw);
    this.lastSurveyId = decoded.surveyId;
    return decoded.body;
  }

  async send(payload: Uint8Array): Promise<void> {
    if (this.lastSurveyId === undefined) {
      throw new Error("Cannot send RESPONDENT0 without active survey");
    }
    const bridge = await this.getBridge();
    const frame = bridge.respondent0Encode(this.lastSurveyId, payload);
    this.lastSurveyId = undefined;
    await this.rawSend(frame);
  }
}

export class Bus0Socket extends BaseSocket {
  constructor(url: string, opts?: SocketOptions) {
    super(url, opts, "bus0");
  }

  async send(payload: Uint8Array, hops = 0): Promise<void> {
    const bridge = await this.getBridge();
    const frame = bridge.bus0Encode(hops, payload);
    await this.rawSend(frame);
  }

  async recv(): Promise<Uint8Array> {
    const raw = await this.rawRecv();
    const bridge = await this.getBridge();
    const decoded = bridge.bus0Decode(raw);
    const maxTtl = (this.getOpt("maxTtl") as number) ?? 8;
    if (decoded.hops > maxTtl) {
      throw new Error(`BUS0 TTL exceeded: hops=${decoded.hops} max=${maxTtl}`);
    }
    return decoded.body;
  }
}

export async function createAnynng(
  options?: import("./types.js").BridgeOptions,
): Promise<AnynngBridge> {
  const bridge = new AnynngBridge();
  return await bridge.init(options);
}

export default createAnynng;
