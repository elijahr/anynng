import { describe, it, expect, beforeEach } from "vitest";
import { AnynngBridge, createAnynng } from "../ts/index.js";

describe("anynng TypeScript / WASM Bridge", () => {
  let bridge: AnynngBridge;

  beforeEach(async () => {
    bridge = await createAnynng();
  });

  it("initializes and reports ready state", () => {
    expect(bridge.ready).toBe(true);
  });

  it("invokes hello returning expected greeting", () => {
    const greeting = bridge.hello();
    expect(greeting).toBe("hello from anynng WASM bridge");
  });

  it("performs arithmetic add operation across boundary", () => {
    const result = bridge.add(21, 21);
    expect(result).toBe(42);
  });

  it("verifies negative controls", () => {
    expect(bridge.add(0, 0)).toBe(0);
    expect(bridge.add(-5, 5)).toBe(0);
  });
});
