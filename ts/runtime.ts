
import type { BridgeOptions, WasmModuleExports, AnynngInstance } from './types.js';

export async function instantiateWasm(
  options: BridgeOptions = {}
): Promise<WebAssembly.Instance> {
  const importObject: WebAssembly.Imports = {
    env: {
      abort: () => {
        throw new Error('WASM abort called');
      },
    },
  };

  if (options.wasmBinary) {
    const result = (await WebAssembly.instantiate(options.wasmBinary, importObject)) as
      | WebAssembly.Instance
      | { instance: WebAssembly.Instance };
    return 'instance' in result ? result.instance : result;
  }

  // Node.js loader
  if (typeof process !== 'undefined' && process.versions && process.versions.node) {
    const fs = await import('node:fs/promises');
    const path = await import('node:path');
    const wasmPath = options.wasmPath ?? path.resolve(process.cwd(), 'dist', 'anynng.wasm');
    const buffer = await fs.readFile(wasmPath);
    const result = (await WebAssembly.instantiate(buffer, importObject)) as
      | WebAssembly.Instance
      | { instance: WebAssembly.Instance };
    return 'instance' in result ? result.instance : result;
  }

  throw new Error('Unsupported execution environment for WASM loading');
}

export class AnynngBridge implements AnynngInstance {
  private instance: WebAssembly.Instance | null = null;
  private _ready = false;

  get ready(): boolean {
    return this._ready;
  }

  async init(options?: BridgeOptions): Promise<this> {
    this.instance = await instantiateWasm(options);
    this._ready = true;
    return this;
  }

  hello(): string {
    if (!this.instance) throw new Error('WASM bridge not initialized');
    return 'hello from anynng WASM bridge';
  }

  add(a: number, b: number): number {
    if (!this.instance) throw new Error('WASM bridge not initialized');
    const exports = this.instance.exports as unknown as WasmModuleExports;
    if (typeof exports.add === 'function') {
      return exports.add(a, b);
    }
    return a + b;
  }
}
