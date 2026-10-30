/**
 * anynng - Pure-Nim multi-target (C, JS, WASM) and multi-transport (WS, TCP, IPC, UDP, Inproc) NNG / Scalability Protocols implementation
 * TypeScript + WebAssembly Entrypoint
 */

export * from "./types.js";
export * from "./runtime.js";
import { AnynngBridge } from "./runtime.js";
import type { SocketOptions, SpProtoName } from "./types.js";

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

export abstract class BaseSocket {
  protected ws?: WebSocket;
  abstract send(payload: Uint8Array): Promise<void>;
  abstract recv(): Promise<Uint8Array>;
  abstract close(): void;
}

export class Req0Socket extends BaseSocket {
  constructor(
    public readonly url: string,
    public readonly opts?: SocketOptions,
  ) {
    super();
  }
  async send(_payload: Uint8Array): Promise<void> {}
  async recv(): Promise<Uint8Array> {
    return new Uint8Array();
  }
  close(): void {
    this.ws?.close();
  }
}

export class Rep0Socket extends BaseSocket {
  constructor(
    public readonly url: string,
    public readonly opts?: SocketOptions,
  ) {
    super();
  }
  async send(_payload: Uint8Array): Promise<void> {}
  async recv(): Promise<Uint8Array> {
    return new Uint8Array();
  }
  close(): void {
    this.ws?.close();
  }
}

export class Pub0Socket extends BaseSocket {
  constructor(
    public readonly url: string,
    public readonly opts?: SocketOptions,
  ) {
    super();
  }
  async send(_payload: Uint8Array): Promise<void> {}
  async recv(): Promise<Uint8Array> {
    throw new Error("PUB0 cannot receive");
  }
  close(): void {
    this.ws?.close();
  }
}

export class Sub0Socket extends BaseSocket {
  private subscriptions: Uint8Array[] = [];
  constructor(
    public readonly url: string,
    public readonly opts?: SocketOptions,
  ) {
    super();
  }
  subscribe(topic: string | Uint8Array): void {
    const bytes =
      typeof topic === "string" ? new TextEncoder().encode(topic) : topic;
    this.subscriptions.push(bytes);
  }
  async send(_payload: Uint8Array): Promise<void> {
    throw new Error("SUB0 cannot send");
  }
  async recv(): Promise<Uint8Array> {
    return new Uint8Array();
  }
  close(): void {
    this.ws?.close();
  }
}

export class Pair1Socket extends BaseSocket {
  constructor(
    public readonly url: string,
    public readonly opts?: SocketOptions,
  ) {
    super();
  }
  async send(_payload: Uint8Array): Promise<void> {}
  async recv(): Promise<Uint8Array> {
    return new Uint8Array();
  }
  close(): void {
    this.ws?.close();
  }
}

export class Bus0Socket extends BaseSocket {
  constructor(
    public readonly url: string,
    public readonly opts?: SocketOptions,
  ) {
    super();
  }
  async send(_payload: Uint8Array): Promise<void> {}
  async recv(): Promise<Uint8Array> {
    return new Uint8Array();
  }
  close(): void {
    this.ws?.close();
  }
}

export async function createAnynng(): Promise<AnynngBridge> {
  const bridge = new AnynngBridge();
  return await bridge.init();
}

export default createAnynng;
