import { describe, it, expect, beforeEach } from "vitest";
import {
  AnynngBridge,
  createAnynng,
  dialerSubprotocol,
  listenerSubprotocol,
  Req0Socket,
  Pub0Socket,
  Sub0Socket,
} from "../ts/index.js";

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

  it("verifies dialer and listener subprotocol mapping (CRIT-02)", () => {
    expect(dialerSubprotocol("req0")).toBe("rep.sp.nanomsg.org");
    expect(listenerSubprotocol("req0")).toBe("req.sp.nanomsg.org");

    expect(dialerSubprotocol("sub0")).toBe("pub.sp.nanomsg.org");
    expect(listenerSubprotocol("sub0")).toBe("sub.sp.nanomsg.org");

    expect(dialerSubprotocol("pair1")).toBe("pair1.sp.nanomsg.org");
    expect(listenerSubprotocol("pair1")).toBe("pair1.sp.nanomsg.org");
  });

  it("instantiates typed TypeScript sockets with options", () => {
    const req = new Req0Socket("ws://localhost:8080/sp", {
      recvTimeoutMs: 1000,
    });
    expect(req.url).toBe("ws://localhost:8080/sp");
    expect(req.opts?.recvTimeoutMs).toBe(1000);

    const pub = new Pub0Socket("ws://localhost:8080/sp");
    expect(pub.url).toBe("ws://localhost:8080/sp");

    const sub = new Sub0Socket("ws://localhost:8080/sp");
    sub.subscribe("news.");
    expect(sub.url).toBe("ws://localhost:8080/sp");
  });
});
