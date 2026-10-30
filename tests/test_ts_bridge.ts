import { describe, it, expect, beforeEach, afterEach } from "vitest";
import {
  AnynngBridge,
  createAnynng,
  dialerSubprotocol,
  listenerSubprotocol,
  Pair0Socket,
  Pair1Socket,
  Pub0Socket,
  Sub0Socket,
  Req0Socket,
  Rep0Socket,
  Push0Socket,
  Pull0Socket,
  Surveyor0Socket,
  Respondent0Socket,
  Bus0Socket,
} from "../ts/index.js";
import { createSpWsServer } from "./helpers/mock_ws_server.mjs";

describe("anynng TypeScript / WASM Bridge Core", () => {
  let bridge: AnynngBridge;

  beforeEach(async () => {
    bridge = await createAnynng();
  });

  it("initializes and reports ready state", () => {
    expect(bridge.ready).toBe(true);
  });

  it("reports WASM ABI version 1", () => {
    expect(bridge.version()).toBe(1);
  });

  it("invokes hello returning expected greeting", () => {
    const greeting = bridge.hello();
    expect(greeting).toBe("hello from anynng WASM bridge");
  });

  it("performs arithmetic add operation across boundary", () => {
    const result = bridge.add(21, 21);
    expect(result).toBe(42);
    expect(bridge.add(0, 0)).toBe(0);
    expect(bridge.add(-5, 5)).toBe(0);
  });

  it("verifies dialer and listener subprotocol mapping (CRIT-02)", () => {
    expect(dialerSubprotocol("pair0")).toBe("pair.sp.nanomsg.org");
    expect(listenerSubprotocol("pair0")).toBe("pair.sp.nanomsg.org");

    expect(dialerSubprotocol("pair1")).toBe("pair1.sp.nanomsg.org");
    expect(listenerSubprotocol("pair1")).toBe("pair1.sp.nanomsg.org");

    expect(dialerSubprotocol("pub0")).toBe("sub.sp.nanomsg.org");
    expect(listenerSubprotocol("pub0")).toBe("pub.sp.nanomsg.org");

    expect(dialerSubprotocol("sub0")).toBe("pub.sp.nanomsg.org");
    expect(listenerSubprotocol("sub0")).toBe("sub.sp.nanomsg.org");

    expect(dialerSubprotocol("req0")).toBe("rep.sp.nanomsg.org");
    expect(listenerSubprotocol("req0")).toBe("req.sp.nanomsg.org");

    expect(dialerSubprotocol("rep0")).toBe("req.sp.nanomsg.org");
    expect(listenerSubprotocol("rep0")).toBe("rep.sp.nanomsg.org");

    expect(dialerSubprotocol("push0")).toBe("pull.sp.nanomsg.org");
    expect(listenerSubprotocol("push0")).toBe("push.sp.nanomsg.org");

    expect(dialerSubprotocol("pull0")).toBe("push.sp.nanomsg.org");
    expect(listenerSubprotocol("pull0")).toBe("pull.sp.nanomsg.org");

    expect(dialerSubprotocol("surveyor0")).toBe("respondent.sp.nanomsg.org");
    expect(listenerSubprotocol("surveyor0")).toBe("surveyor.sp.nanomsg.org");

    expect(dialerSubprotocol("respondent0")).toBe("surveyor.sp.nanomsg.org");
    expect(listenerSubprotocol("respondent0")).toBe("respondent.sp.nanomsg.org");

    expect(dialerSubprotocol("bus0")).toBe("bus.sp.nanomsg.org");
    expect(listenerSubprotocol("bus0")).toBe("bus.sp.nanomsg.org");
  });

  it("encodes and decodes REQ0 / REP0 framing via WASM C-ABI", () => {
    const payload = new TextEncoder().encode("req-payload-123");
    const frame = bridge.req0Encode(42, payload);
    expect(frame.length).toBe(payload.length + 4);

    const decoded = bridge.req0Decode(frame);
    expect(decoded.reqId).toBe(42);
    expect(new TextDecoder().decode(decoded.body)).toBe("req-payload-123");

    const repPayload = new TextEncoder().encode("rep-payload-456");
    const repFrame = bridge.rep0Encode(42, repPayload);
    const decodedRep = bridge.req0Decode(repFrame);
    expect(decodedRep.reqId).toBe(42);
    expect(new TextDecoder().decode(decodedRep.body)).toBe("rep-payload-456");
  });

  it("encodes and decodes PAIR1 hop headers via WASM C-ABI", () => {
    const payload = new TextEncoder().encode("pair1-body");
    const frame = bridge.pair1Encode(5, payload);
    expect(frame.length).toBe(payload.length + 4);

    const decoded = bridge.pair1Decode(frame);
    expect(decoded.hops).toBe(5);
    expect(new TextDecoder().decode(decoded.body)).toBe("pair1-body");
  });

  it("evaluates SUB0 topic prefix filtering via WASM C-ABI", () => {
    const msg1 = new TextEncoder().encode("telemetry.temperature.ch1 23.5");
    const msg2 = new TextEncoder().encode("logs.system reboot");

    expect(bridge.sub0Matches("telemetry.", msg1)).toBe(true);
    expect(bridge.sub0Matches("telemetry.", msg2)).toBe(false);
    expect(bridge.sub0Matches("", msg1)).toBe(true);
    expect(bridge.sub0Matches("", msg2)).toBe(true);
  });

  it("encodes and decodes SURVEYOR0 / RESPONDENT0 via WASM C-ABI", () => {
    const survey = new TextEncoder().encode("election-query");
    const surveyFrame = bridge.survey0Encode(101, survey);
    const decodedSurvey = bridge.survey0Decode(surveyFrame);
    expect(decodedSurvey.surveyId).toBe(101);
    expect(new TextDecoder().decode(decodedSurvey.body)).toBe("election-query");

    const resp = new TextEncoder().encode("vote-candidate-a");
    const respFrame = bridge.respondent0Encode(101, resp);
    const decodedResp = bridge.survey0Decode(respFrame);
    expect(decodedResp.surveyId).toBe(101);
    expect(new TextDecoder().decode(decodedResp.body)).toBe("vote-candidate-a");
  });

  it("encodes and decodes BUS0 mesh headers via WASM C-ABI", () => {
    const msg = new TextEncoder().encode("mesh-gossip");
    const frame = bridge.bus0Encode(2, msg);
    const decoded = bridge.bus0Decode(frame);
    expect(decoded.hops).toBe(2);
    expect(new TextDecoder().decode(decoded.body)).toBe("mesh-gossip");
  });

  it("encodes and decodes SP stream greeting and frame length via WASM", () => {
    const greeting = bridge.streamEncodeGreeting(0x30); // REQ0
    expect(greeting.length).toBe(8);
    expect(greeting[0]).toBe(0x00);
    expect(greeting[1]).toBe("S".charCodeAt(0));
    expect(greeting[2]).toBe("P".charCodeAt(0));
    expect(greeting[3]).toBe(0x00);

    const proto = bridge.streamDecodeGreeting(greeting);
    expect(proto).toBe(0x30);

    const frame = bridge.streamEncodeFrame(new TextEncoder().encode("test-frame"));
    expect(frame.length).toBe(8 + 10);
    const frameLen = bridge.streamDecodeFrameLen(frame.subarray(0, 8));
    expect(frameLen).toBe(10n);
  });

  it("encodes and decodes UDP datagram headers via WASM", () => {
    const header = bridge.udpEncodeHeader(1, 4, 0x1234, 0x56789abc);
    expect(header.length).toBe(8);
    const decoded = bridge.udpDecodeHeader(header);
    expect(decoded.packetType).toBe(1);
    expect(decoded.hops).toBe(4);
    expect(decoded.flowId).toBe(0x1234);
    expect(decoded.seqNum).toBe(0x56789abc);
  });
});

describe("anynng TypeScript WebSocket SP Sockets (E2E)", () => {
  let serverHelper: any;

  beforeEach(async () => {
    serverHelper = await createSpWsServer(0);
  });

  afterEach(async () => {
    if (serverHelper) {
      await serverHelper.close();
    }
  });

  it("performs bidirectional loopback over PAIR0 socket", async () => {
    const sock = new Pair0Socket(serverHelper.url);
    await sock.connect();

    const payload = new TextEncoder().encode("pair0-test-payload");
    await sock.send(payload);

    const reply = await sock.recv();
    expect(new TextDecoder().decode(reply)).toBe("pair0-test-payload");
    sock.close();
  });

  it("performs hop-incremented loopback over PAIR1 socket", async () => {
    const sock = new Pair1Socket(serverHelper.url);
    await sock.connect();

    const payload = new TextEncoder().encode("pair1-test-data");
    await sock.send(payload, 0);

    const reply = await sock.recv();
    expect(new TextDecoder().decode(reply)).toBe("pair1-test-data");
    sock.close();
  });

  it("performs request / reply backtrace over REQ0 socket", async () => {
    const sock = new Req0Socket(serverHelper.url, { recvTimeoutMs: 2000 });
    await sock.connect();

    const payload = new TextEncoder().encode("compute-fib-10");
    await sock.send(payload);

    const reply = await sock.recv();
    // mock_ws_server prepends "rep:" to the body
    expect(new TextDecoder().decode(reply)).toBe("rep:compute-fib-10");
    sock.close();
  });

  it("publishes and filters topics over SUB0 socket", async () => {
    const subSock = new Sub0Socket(serverHelper.url, { recvTimeoutMs: 2000 });
    await subSock.connect();
    subSock.subscribe("sports.");

    // Wait 50ms for socket registration
    await new Promise((r) => setTimeout(r, 50));

    // Broadcast non-matching and matching messages from server
    serverHelper.broadcastBinary(new TextEncoder().encode("weather.sunny"));
    serverHelper.broadcastBinary(new TextEncoder().encode("sports.baseball 5-2"));

    const received = await subSock.recv();
    expect(new TextDecoder().decode(received)).toBe("sports.baseball 5-2");
    subSock.close();
  });

  it("conducts survey broadcast over SURVEYOR0 socket", async () => {
    const surveyor = new Surveyor0Socket(serverHelper.url, {
      surveyTimeMs: 1500,
    });
    await surveyor.connect();

    await surveyor.send(new TextEncoder().encode("do-you-agree?"));
    const resp = await surveyor.recv();
    expect(new TextDecoder().decode(resp)).toBe("resp:do-you-agree?");
    surveyor.close();
  });

  it("broadcasts mesh gossip over BUS0 socket", async () => {
    const bus = new Bus0Socket(serverHelper.url, { recvTimeoutMs: 2000 });
    await bus.connect();

    await bus.send(new TextEncoder().encode("bus-node-alive"), 0);
    const reply = await bus.recv();
    expect(new TextDecoder().decode(reply)).toBe("bus-node-alive");
    bus.close();
  });
});
