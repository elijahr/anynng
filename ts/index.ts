/**
 * anynng - Pure-Nim multi-target (C, JS, WASM) and multi-transport (WS, TCP, IPC, UDP, Inproc) NNG / Scalability Protocols implementation
 * TypeScript + WebAssembly Entrypoint
 */

export * from './types.js';
export * from './runtime.js';
import { AnynngBridge } from './runtime.js';

export async function createAnynng(): Promise<AnynngBridge> {
  const bridge = new AnynngBridge();
  return await bridge.init();
}

export default createAnynng;
