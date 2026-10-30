/**
 * Types and interfaces for anynng WASM and TypeScript library.
 */

export interface WasmModuleExports {
  memory?: WebAssembly.Memory;
  hello?: () => number;
  add?: (a: number, b: number) => number;
  [key: string]: unknown;
}

export interface BridgeOptions {
  wasmPath?: string;
  wasmBinary?: ArrayBufferView | ArrayBuffer;
}

export interface AnynngInstance {
  readonly ready: boolean;
  hello(): string;
  add(a: number, b: number): number;
}

export type SpProtoName =
  | "pair0"
  | "pair1"
  | "pub0"
  | "sub0"
  | "req0"
  | "rep0"
  | "push0"
  | "pull0"
  | "surveyor0"
  | "respondent0"
  | "bus0";

export interface SocketOptions {
  recvTimeoutMs?: number;
  sendTimeoutMs?: number;
  maxTtl?: number;
  resendTimeMs?: number;
  surveyTimeMs?: number;
}
