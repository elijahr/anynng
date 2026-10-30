/**
 * ts/runtime.ts - WebAssembly runtime & bridge for anynng.
 * Licensed under MIT License.
 */

import type {
  BridgeOptions,
  WasmModuleExports,
  AnynngInstance,
  Req0Decoded,
  Pair1Decoded,
  Survey0Decoded,
  Bus0Decoded,
  UdpHeaderDecoded,
} from "./types.js";

export async function instantiateWasm(
  options: BridgeOptions = {},
): Promise<WebAssembly.Instance> {
  let wasiImports: Record<string, unknown> = {};

  if (
    typeof process !== "undefined" &&
    process.versions &&
    process.versions.node
  ) {
    try {
      const { WASI } = await import("node:wasi");
      const wasi = new WASI({ version: "preview1" });
      wasiImports = wasi.wasiImport as unknown as Record<string, unknown>;
    } catch {
      // Ignore if node:wasi is unavailable
    }
  }

  const defaultWasi: Record<string, WebAssembly.ImportValue> = {
    fd_write: () => 0,
    fd_close: () => 0,
    fd_seek: () => 0,
    fd_read: () => 0,
    environ_sizes_get: () => 0,
    environ_get: () => 0,
    clock_time_get: () => 0,
    proc_exit: () => {},
  };

  const importObject: WebAssembly.Imports = {
    wasi_snapshot_preview1: {
      ...defaultWasi,
      ...(wasiImports as Record<string, WebAssembly.ImportValue>),
    },
    env: {
      abort: () => {
        throw new Error("WASM abort called");
      },
    },
  };

  if (options.wasmBinary) {
    const result = (await WebAssembly.instantiate(
      options.wasmBinary,
      importObject,
    )) as WebAssembly.Instance | { instance: WebAssembly.Instance };
    return "instance" in result ? result.instance : result;
  }

  // Node.js loader
  if (
    typeof process !== "undefined" &&
    process.versions &&
    process.versions.node
  ) {
    const fs = await import("node:fs/promises");
    const path = await import("node:path");
    const wasmPath =
      options.wasmPath ?? path.resolve(process.cwd(), "dist", "anynng.wasm");
    const buffer = await fs.readFile(wasmPath);
    const result = (await WebAssembly.instantiate(buffer, importObject)) as
      WebAssembly.Instance | { instance: WebAssembly.Instance };
    return "instance" in result ? result.instance : result;
  }

  // Browser fetch loader
  if (typeof fetch === "function") {
    const url = options.wasmPath ?? "anynng.wasm";
    if (typeof WebAssembly.instantiateStreaming === "function") {
      try {
        const response = await fetch(url);
        const result = await WebAssembly.instantiateStreaming(
          response,
          importObject,
        );
        return result.instance;
      } catch {
        // Fallback to arrayBuffer below
      }
    }
    const response = await fetch(url);
    const bytes = await response.arrayBuffer();
    const result = (await WebAssembly.instantiate(bytes, importObject)) as
      | WebAssembly.Instance
      | { instance: WebAssembly.Instance };
    return "instance" in result ? result.instance : result;
  }

  throw new Error("Unsupported execution environment for WASM loading");
}

export class AnynngBridge implements AnynngInstance {
  private instance: WebAssembly.Instance | null = null;
  private exports: WasmModuleExports | null = null;
  private _ready = false;

  get ready(): boolean {
    return this._ready;
  }

  get rawExports(): WasmModuleExports {
    if (!this.exports) throw new Error("WASM bridge not initialized");
    return this.exports;
  }

  get memory(): WebAssembly.Memory {
    if (!this.exports) throw new Error("WASM bridge not initialized");
    return this.exports.memory;
  }

  async init(options?: BridgeOptions): Promise<this> {
    this.instance = await instantiateWasm(options);
    this.exports = this.instance.exports as unknown as WasmModuleExports;
    if (typeof this.exports.NimMain === "function") {
      this.exports.NimMain();
    }
    this._ready = true;
    return this;
  }

  alloc(size: number): number {
    if (!this.exports) throw new Error("WASM bridge not initialized");
    return this.exports.anynng_alloc(size);
  }

  free(ptr: number): void {
    if (!this.exports || ptr === 0) return;
    this.exports.anynng_free(ptr);
  }

  writeBytes(bytes: Uint8Array): number {
    if (bytes.length === 0) return 0;
    const ptr = this.alloc(bytes.length);
    if (!ptr) throw new Error("Failed to allocate WASM memory");
    new Uint8Array(this.memory.buffer).set(bytes, ptr);
    return ptr;
  }

  readBytes(ptr: number, length: number): Uint8Array {
    if (length <= 0 || ptr === 0) return new Uint8Array(0);
    return new Uint8Array(this.memory.buffer.slice(ptr, ptr + length));
  }

  version(): number {
    if (!this.exports) throw new Error("WASM bridge not initialized");
    return this.exports.anynng_wasm_version();
  }

  hello(): string {
    if (!this.instance) throw new Error("WASM bridge not initialized");
    return "hello from anynng WASM bridge";
  }

  add(a: number, b: number): number {
    if (!this.instance) throw new Error("WASM bridge not initialized");
    return a + b;
  }

  wsDialerSubprotocol(proto: number): string {
    if (!this.exports) throw new Error("WASM bridge not initialized");
    const bufPtr = this.alloc(128);
    try {
      const len = this.exports.anynng_ws_dialer_subprotocol(proto, bufPtr, 128);
      const bytes = new Uint8Array(this.memory.buffer, bufPtr, len);
      return new TextDecoder().decode(bytes);
    } finally {
      this.free(bufPtr);
    }
  }

  wsListenerSubprotocol(proto: number): string {
    if (!this.exports) throw new Error("WASM bridge not initialized");
    const bufPtr = this.alloc(128);
    try {
      const len = this.exports.anynng_ws_listener_subprotocol(
        proto,
        bufPtr,
        128,
      );
      const bytes = new Uint8Array(this.memory.buffer, bufPtr, len);
      return new TextDecoder().decode(bytes);
    } finally {
      this.free(bufPtr);
    }
  }

  req0Encode(reqId: number, body: Uint8Array): Uint8Array {
    if (!this.exports) throw new Error("WASM bridge not initialized");
    const bodyPtr = this.writeBytes(body);
    const outPtr = this.alloc(body.length + 4);
    try {
      const totalLen = this.exports.anynng_req0_encode(
        reqId,
        bodyPtr,
        body.length,
        outPtr,
      );
      if (totalLen < 0) throw new Error("Failed to encode REQ0 message");
      return this.readBytes(outPtr, totalLen);
    } finally {
      this.free(bodyPtr);
      this.free(outPtr);
    }
  }

  req0Decode(frame: Uint8Array): Req0Decoded {
    if (!this.exports) throw new Error("WASM bridge not initialized");
    if (frame.length < 4) throw new Error("REQ0 frame too short");
    const framePtr = this.writeBytes(frame);
    const metaPtr = this.alloc(12); // 3 * 4 bytes for outReqId, outBodyOffset, outBodyLen
    try {
      const outReqIdPtr = metaPtr;
      const outBodyOffsetPtr = metaPtr + 4;
      const outBodyLenPtr = metaPtr + 8;
      const rc = this.exports.anynng_req0_decode(
        framePtr,
        frame.length,
        outReqIdPtr,
        outBodyOffsetPtr,
        outBodyLenPtr,
      );
      if (rc !== 0) throw new Error("Failed to decode REQ0 frame");
      const view = new DataView(this.memory.buffer, metaPtr, 12);
      const reqId = view.getUint32(0, true);
      const bodyOffset = view.getInt32(4, true);
      const bodyLen = view.getInt32(8, true);
      const body = frame.slice(bodyOffset, bodyOffset + bodyLen);
      return { reqId, body };
    } finally {
      this.free(framePtr);
      this.free(metaPtr);
    }
  }

  rep0Encode(reqId: number, body: Uint8Array): Uint8Array {
    if (!this.exports) throw new Error("WASM bridge not initialized");
    const bodyPtr = this.writeBytes(body);
    const outPtr = this.alloc(body.length + 4);
    try {
      const totalLen = this.exports.anynng_rep0_encode(
        reqId,
        bodyPtr,
        body.length,
        outPtr,
      );
      if (totalLen < 0) throw new Error("Failed to encode REP0 message");
      return this.readBytes(outPtr, totalLen);
    } finally {
      this.free(bodyPtr);
      this.free(outPtr);
    }
  }

  pair1Encode(hops: number, body: Uint8Array): Uint8Array {
    if (!this.exports) throw new Error("WASM bridge not initialized");
    const bodyPtr = this.writeBytes(body);
    const outPtr = this.alloc(body.length + 4);
    try {
      const totalLen = this.exports.anynng_pair1_encode(
        hops,
        bodyPtr,
        body.length,
        outPtr,
      );
      if (totalLen < 0) throw new Error("Failed to encode PAIR1 message");
      return this.readBytes(outPtr, totalLen);
    } finally {
      this.free(bodyPtr);
      this.free(outPtr);
    }
  }

  pair1Decode(frame: Uint8Array): Pair1Decoded {
    if (!this.exports) throw new Error("WASM bridge not initialized");
    if (frame.length < 4) throw new Error("PAIR1 frame too short");
    const framePtr = this.writeBytes(frame);
    const metaPtr = this.alloc(12);
    try {
      const rc = this.exports.anynng_pair1_decode(
        framePtr,
        frame.length,
        metaPtr,
        metaPtr + 4,
        metaPtr + 8,
      );
      if (rc !== 0) throw new Error("Failed to decode PAIR1 frame");
      const view = new DataView(this.memory.buffer, metaPtr, 12);
      const hops = view.getUint32(0, true);
      const bodyOffset = view.getInt32(4, true);
      const bodyLen = view.getInt32(8, true);
      const body = frame.slice(bodyOffset, bodyOffset + bodyLen);
      return { hops, body };
    } finally {
      this.free(framePtr);
      this.free(metaPtr);
    }
  }

  sub0Matches(topic: Uint8Array | string, msg: Uint8Array): boolean {
    if (!this.exports) throw new Error("WASM bridge not initialized");
    const topicBytes =
      typeof topic === "string" ? new TextEncoder().encode(topic) : topic;
    if (topicBytes.length === 0) return true;
    const topicPtr = this.writeBytes(topicBytes);
    const msgPtr = this.writeBytes(msg);
    try {
      const match = this.exports.anynng_sub0_matches(
        topicPtr,
        topicBytes.length,
        msgPtr,
        msg.length,
      );
      return match === 1;
    } finally {
      this.free(topicPtr);
      this.free(msgPtr);
    }
  }

  survey0Encode(surveyId: number, body: Uint8Array): Uint8Array {
    if (!this.exports) throw new Error("WASM bridge not initialized");
    const bodyPtr = this.writeBytes(body);
    const outPtr = this.alloc(body.length + 4);
    try {
      const totalLen = this.exports.anynng_survey0_encode(
        surveyId,
        bodyPtr,
        body.length,
        outPtr,
      );
      if (totalLen < 0) throw new Error("Failed to encode SURVEY0 message");
      return this.readBytes(outPtr, totalLen);
    } finally {
      this.free(bodyPtr);
      this.free(outPtr);
    }
  }

  survey0Decode(frame: Uint8Array): Survey0Decoded {
    if (!this.exports) throw new Error("WASM bridge not initialized");
    if (frame.length < 4) throw new Error("SURVEY0 frame too short");
    const framePtr = this.writeBytes(frame);
    const metaPtr = this.alloc(12);
    try {
      const rc = this.exports.anynng_survey0_decode(
        framePtr,
        frame.length,
        metaPtr,
        metaPtr + 4,
        metaPtr + 8,
      );
      if (rc !== 0) throw new Error("Failed to decode SURVEY0 frame");
      const view = new DataView(this.memory.buffer, metaPtr, 12);
      const surveyId = view.getUint32(0, true);
      const bodyOffset = view.getInt32(4, true);
      const bodyLen = view.getInt32(8, true);
      const body = frame.slice(bodyOffset, bodyOffset + bodyLen);
      return { surveyId, body };
    } finally {
      this.free(framePtr);
      this.free(metaPtr);
    }
  }

  respondent0Encode(surveyId: number, body: Uint8Array): Uint8Array {
    if (!this.exports) throw new Error("WASM bridge not initialized");
    const bodyPtr = this.writeBytes(body);
    const outPtr = this.alloc(body.length + 4);
    try {
      const totalLen = this.exports.anynng_respondent0_encode(
        surveyId,
        bodyPtr,
        body.length,
        outPtr,
      );
      if (totalLen < 0) throw new Error("Failed to encode RESPONDENT0 message");
      return this.readBytes(outPtr, totalLen);
    } finally {
      this.free(bodyPtr);
      this.free(outPtr);
    }
  }

  bus0Encode(hops: number, body: Uint8Array): Uint8Array {
    if (!this.exports) throw new Error("WASM bridge not initialized");
    const bodyPtr = this.writeBytes(body);
    const outPtr = this.alloc(body.length + 4);
    try {
      const totalLen = this.exports.anynng_bus0_encode(
        hops,
        bodyPtr,
        body.length,
        outPtr,
      );
      if (totalLen < 0) throw new Error("Failed to encode BUS0 message");
      return this.readBytes(outPtr, totalLen);
    } finally {
      this.free(bodyPtr);
      this.free(outPtr);
    }
  }

  bus0Decode(frame: Uint8Array): Bus0Decoded {
    if (!this.exports) throw new Error("WASM bridge not initialized");
    if (frame.length < 4) throw new Error("BUS0 frame too short");
    const framePtr = this.writeBytes(frame);
    const metaPtr = this.alloc(12);
    try {
      const rc = this.exports.anynng_bus0_decode(
        framePtr,
        frame.length,
        metaPtr,
        metaPtr + 4,
        metaPtr + 8,
      );
      if (rc !== 0) throw new Error("Failed to decode BUS0 frame");
      const view = new DataView(this.memory.buffer, metaPtr, 12);
      const hops = view.getUint32(0, true);
      const bodyOffset = view.getInt32(4, true);
      const bodyLen = view.getInt32(8, true);
      const body = frame.slice(bodyOffset, bodyOffset + bodyLen);
      return { hops, body };
    } finally {
      this.free(framePtr);
      this.free(metaPtr);
    }
  }

  streamEncodeGreeting(proto: number): Uint8Array {
    if (!this.exports) throw new Error("WASM bridge not initialized");
    const outPtr = this.alloc(8);
    try {
      const rc = this.exports.anynng_stream_encode_greeting(proto, outPtr);
      if (rc !== 8) throw new Error("Failed to encode stream greeting");
      return this.readBytes(outPtr, 8);
    } finally {
      this.free(outPtr);
    }
  }

  streamDecodeGreeting(greeting: Uint8Array): number {
    if (!this.exports) throw new Error("WASM bridge not initialized");
    if (greeting.length < 8) throw new Error("Stream greeting too short");
    const greetingPtr = this.writeBytes(greeting);
    const outProtoPtr = this.alloc(2);
    try {
      const rc = this.exports.anynng_stream_decode_greeting(
        greetingPtr,
        outProtoPtr,
      );
      if (rc !== 0) throw new Error("Failed to decode stream greeting");
      const view = new DataView(this.memory.buffer, outProtoPtr, 2);
      return view.getUint16(0, true);
    } finally {
      this.free(greetingPtr);
      this.free(outProtoPtr);
    }
  }

  streamEncodeFrame(body: Uint8Array): Uint8Array {
    if (!this.exports) throw new Error("WASM bridge not initialized");
    const bodyPtr = this.writeBytes(body);
    const outPtr = this.alloc(body.length + 8);
    try {
      const totalLen = this.exports.anynng_stream_encode_frame(
        bodyPtr,
        body.length,
        outPtr,
      );
      if (totalLen < 0) throw new Error("Failed to encode stream frame");
      return this.readBytes(outPtr, totalLen);
    } finally {
      this.free(bodyPtr);
      this.free(outPtr);
    }
  }

  streamDecodeFrameLen(header: Uint8Array): bigint {
    if (!this.exports) throw new Error("WASM bridge not initialized");
    if (header.length < 8) throw new Error("Stream frame header too short");
    const headerPtr = this.writeBytes(header);
    const outLenPtr = this.alloc(8);
    try {
      const rc = this.exports.anynng_stream_decode_frame_len(
        headerPtr,
        outLenPtr,
      );
      if (rc !== 0) throw new Error("Failed to decode stream frame len");
      const view = new DataView(this.memory.buffer, outLenPtr, 8);
      return view.getBigUint64(0, true);
    } finally {
      this.free(headerPtr);
      this.free(outLenPtr);
    }
  }

  udpEncodeHeader(
    packetType: number,
    hops: number,
    flowId: number,
    seqNum: number,
  ): Uint8Array {
    if (!this.exports) throw new Error("WASM bridge not initialized");
    const outPtr = this.alloc(8);
    try {
      const rc = this.exports.anynng_udp_encode_header(
        packetType,
        hops,
        flowId,
        seqNum,
        outPtr,
      );
      if (rc !== 8) throw new Error("Failed to encode UDP header");
      return this.readBytes(outPtr, 8);
    } finally {
      this.free(outPtr);
    }
  }

  udpDecodeHeader(header: Uint8Array): UdpHeaderDecoded {
    if (!this.exports) throw new Error("WASM bridge not initialized");
    if (header.length < 8) throw new Error("UDP header too short");
    const headerPtr = this.writeBytes(header);
    const metaPtr = this.alloc(16);
    try {
      const outTypePtr = metaPtr;
      const outHopsPtr = metaPtr + 1;
      const outFlowPtr = metaPtr + 2;
      const outSeqPtr = metaPtr + 4;
      const rc = this.exports.anynng_udp_decode_header(
        headerPtr,
        outTypePtr,
        outHopsPtr,
        outFlowPtr,
        outSeqPtr,
      );
      if (rc !== 0) throw new Error("Failed to decode UDP header");
      const view = new DataView(this.memory.buffer, metaPtr, 8);
      return {
        packetType: view.getUint8(0),
        hops: view.getUint8(1),
        flowId: view.getUint16(2, true),
        seqNum: view.getUint32(4, true),
      };
    } finally {
      this.free(headerPtr);
      this.free(metaPtr);
    }
  }
}
