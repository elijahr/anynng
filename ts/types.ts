/**
 * ts/types.ts - Types and interfaces for anynng WASM and TypeScript library.
 * Licensed under MIT License.
 */

export interface WasmModuleExports {
  memory: WebAssembly.Memory;
  NimMain?: () => void;
  anynng_wasm_version: () => number;
  anynng_alloc: (size: number) => number;
  anynng_free: (ptr: number) => void;
  anynng_ws_dialer_subprotocol: (
    proto: number,
    outBuf: number,
    outCap: number,
  ) => number;
  anynng_ws_listener_subprotocol: (
    proto: number,
    outBuf: number,
    outCap: number,
  ) => number;
  anynng_req0_encode: (
    reqId: number,
    bodyPtr: number,
    bodyLen: number,
    outPtr: number,
  ) => number;
  anynng_req0_decode: (
    framePtr: number,
    frameLen: number,
    outReqId: number,
    outBodyOffset: number,
    outBodyLen: number,
  ) => number;
  anynng_rep0_encode: (
    reqId: number,
    bodyPtr: number,
    bodyLen: number,
    outPtr: number,
  ) => number;
  anynng_pair1_encode: (
    hops: number,
    bodyPtr: number,
    bodyLen: number,
    outPtr: number,
  ) => number;
  anynng_pair1_decode: (
    framePtr: number,
    frameLen: number,
    outHops: number,
    outBodyOffset: number,
    outBodyLen: number,
  ) => number;
  anynng_sub0_matches: (
    topicPtr: number,
    topicLen: number,
    msgPtr: number,
    msgLen: number,
  ) => number;
  anynng_survey0_encode: (
    surveyId: number,
    bodyPtr: number,
    bodyLen: number,
    outPtr: number,
  ) => number;
  anynng_survey0_decode: (
    framePtr: number,
    frameLen: number,
    outSurveyId: number,
    outBodyOffset: number,
    outBodyLen: number,
  ) => number;
  anynng_respondent0_encode: (
    surveyId: number,
    bodyPtr: number,
    bodyLen: number,
    outPtr: number,
  ) => number;
  anynng_bus0_encode: (
    hops: number,
    bodyPtr: number,
    bodyLen: number,
    outPtr: number,
  ) => number;
  anynng_bus0_decode: (
    framePtr: number,
    frameLen: number,
    outHops: number,
    outBodyOffset: number,
    outBodyLen: number,
  ) => number;
  anynng_stream_encode_greeting: (proto: number, outPtr: number) => number;
  anynng_stream_decode_greeting: (
    greetingPtr: number,
    outProto: number,
  ) => number;
  anynng_stream_encode_frame: (
    bodyPtr: number,
    bodyLen: number,
    outPtr: number,
  ) => number;
  anynng_stream_decode_frame_len: (headerPtr: number, outLen: number) => number;
  anynng_udp_encode_header: (
    packetType: number,
    hops: number,
    flowId: number,
    seqNum: number,
    outPtr: number,
  ) => number;
  anynng_udp_decode_header: (
    headerPtr: number,
    outType: number,
    outHops: number,
    outFlow: number,
    outSeq: number,
  ) => number;
  anynng_ctx_create?: () => number;
  anynng_ctx_close?: (ctxId: number) => number;
  anynng_pipe_event_name?: (
    ev: number,
    outBuf: number,
    outCap: number,
  ) => number;
  anynng_engine_create: (
    proto: number,
    raw: number,
    maxTtl: number,
    poly: number,
    resendMs: bigint,
    surveyMs: bigint,
  ) => number;
  anynng_engine_destroy: (handle: number) => number;
  anynng_engine_subscribe: (
    handle: number,
    topicPtr: number,
    topicLen: number,
  ) => number;
  anynng_engine_unsubscribe: (
    handle: number,
    topicPtr: number,
    topicLen: number,
  ) => number;
  anynng_engine_set_opt_int: (
    handle: number,
    opt: number,
    val: bigint,
  ) => number;
  anynng_engine_get_opt_int: (handle: number, opt: number) => bigint;
  anynng_engine_set_opt: (
    handle: number,
    namePtr: number,
    val: bigint,
  ) => number;
  anynng_engine_get_opt: (handle: number, namePtr: number) => bigint;
  anynng_engine_step: (
    handle: number,
    evKind: number,
    pipeId: bigint,
    ctxId: number,
    inPtr: number,
    inLen: number,
    nowMs: bigint,
    outPtr: number,
    outCap: number,
  ) => number;
  anynng_engine_get_actions: (outPtr: number, outCap: number) => number;
  [key: string]: unknown;
}

export enum EngineEventKind {
  evPipeAdded = 0,
  evPipeRemoved = 1,
  evPipeMessage = 2,
  evTimerTick = 3,
  evUserSend = 4,
  evUserCancel = 5,
}

export enum EngineActionKind {
  actSendPipe = 0,
  actClosePipe = 1,
  actDeliverRecv = 2,
  actFinishSend = 3,
  actFailContext = 4,
}

export interface EngineAction {
  kind: EngineActionKind;
  pipeId: bigint;
  ctxId: number;
  err: number;
  data: Uint8Array;
}

export interface BridgeOptions {
  wasmPath?: string;
  wasmBinary?: ArrayBufferView | ArrayBuffer;
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

export const SP_PROTO_IDS: Record<SpProtoName, number> = {
  pair0: 0x10,
  pair1: 0x11,
  pub0: 0x20,
  sub0: 0x21,
  req0: 0x30,
  rep0: 0x31,
  push0: 0x50,
  pull0: 0x51,
  surveyor0: 0x62,
  respondent0: 0x63,
  bus0: 0x70,
};

export type PipeEvent = "add_pre" | "add_post" | "rem_post";

export type PipeNotifyHook = (pipeId: number, ev: PipeEvent) => boolean | void;

export interface ContextOptions {
  recvTimeoutMs?: number;
  resendTimeMs?: number;
}

export interface DialerOptions {
  reconnectTimeMinMs?: number;
  reconnectTimeMaxMs?: number;
  autoReconnect?: boolean;
}

export interface SocketOptions {
  recvTimeoutMs?: number;
  sendTimeoutMs?: number;
  maxTtl?: number;
  polyamorous?: boolean;
  recvMaxSize?: number;
  raw?: boolean;
  resendTimeMs?: number;
  surveyTimeMs?: number;
  reconnectTimeMinMs?: number;
  reconnectTimeMaxMs?: number;
  autoReconnect?: boolean;
}

export interface AnynngInstance {
  readonly ready: boolean;
  init(options?: BridgeOptions): Promise<this>;
}

export interface Req0Decoded {
  reqId: number;
  body: Uint8Array;
}

export interface Pair1Decoded {
  hops: number;
  body: Uint8Array;
}

export interface Survey0Decoded {
  surveyId: number;
  body: Uint8Array;
}

export interface Bus0Decoded {
  hops: number;
  body: Uint8Array;
}

export interface UdpHeaderDecoded {
  packetType: number;
  hops: number;
  flowId: number;
  seqNum: number;
}
