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
  PipeEvent,
  EngineEventKind,
  EngineActionKind,
} from "../ts/index.js";
import {
  createSpWsServer,
  createSpWssServer,
} from "./helpers/mock_ws_server.mjs";

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

  it("creates, steps, and destroys canonical ProtocolEngine in WASM", () => {
    // Protocol ID 0x30 = REQ0
    const handle = bridge.engineCreate(0x30, false, 8, false, 5000, 1000);
    expect(handle).toBeGreaterThan(0);

    // Step evPipeAdded
    const addActs = bridge.engineStep(
      handle,
      EngineEventKind.evPipeAdded,
      100n,
    );
    expect(addActs.length).toBe(0);

    // Step evUserSend
    const payload = new TextEncoder().encode("wasm-engine-payload");
    const sendActs = bridge.engineStep(
      handle,
      EngineEventKind.evUserSend,
      0n,
      1,
      payload,
    );
    expect(sendActs.length).toBeGreaterThanOrEqual(1);
    expect(sendActs[0].kind).toBe(EngineActionKind.actSendPipe);
    expect(sendActs[0].data.length).toBe(payload.length + 4);

    // Step evPipeMessage with reply preserving the request ID
    const repFrame = new Uint8Array(sendActs[0].data);
    repFrame.set(new TextEncoder().encode("echo-"), 4);
    const recvActs = bridge.engineStep(
      handle,
      EngineEventKind.evPipeMessage,
      100n,
      0,
      repFrame,
    );
    expect(recvActs.length).toBe(1);
    expect(recvActs[0].kind).toBe(EngineActionKind.actDeliverRecv);
    expect(new TextDecoder().decode(recvActs[0].data)).toContain("echo-");

    bridge.engineDestroy(handle);
  });

  it("verifies topic subscription and filtering via WASM ProtocolEngine", () => {
    // Protocol ID 0x21 = SUB0
    const handle = bridge.engineCreate(0x21, false, 8, false, 5000, 1000);
    expect(handle).toBeGreaterThan(0);

    bridge.engineSubscribe(handle, "telemetry.cpu");

    bridge.engineStep(handle, EngineEventKind.evPipeAdded, 1n);

    // Message with non-matching topic -> dropped
    const nonMatch = new TextEncoder().encode("telemetry.mem:80%");
    const acts1 = bridge.engineStep(
      handle,
      EngineEventKind.evPipeMessage,
      1n,
      0,
      nonMatch,
    );
    expect(acts1.length).toBe(0);

    // Message with matching topic -> delivered
    const match = new TextEncoder().encode("telemetry.cpu:45%");
    const acts2 = bridge.engineStep(
      handle,
      EngineEventKind.evPipeMessage,
      1n,
      0,
      match,
    );
    expect(acts2.length).toBe(1);
    expect(acts2[0].kind).toBe(EngineActionKind.actDeliverRecv);
    expect(new TextDecoder().decode(acts2[0].data)).toBe("telemetry.cpu:45%");

    bridge.engineUnsubscribe(handle, "telemetry.cpu");
    const acts3 = bridge.engineStep(
      handle,
      EngineEventKind.evPipeMessage,
      1n,
      0,
      match,
    );
    expect(acts3.length).toBe(0);

    bridge.engineDestroy(handle);
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
    expect(listenerSubprotocol("respondent0")).toBe(
      "respondent.sp.nanomsg.org",
    );

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

    const frame = bridge.streamEncodeFrame(
      new TextEncoder().encode("test-frame"),
    );
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
    serverHelper.broadcastBinary(
      new TextEncoder().encode("sports.baseball 5-2"),
    );

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

describe("anynng Browser/Node Parity Features (Wave 3 Task 3.2)", () => {
  it("performs secure TLS loopback over wss:// with Pair0Socket", async () => {
    const origReject = process.env.NODE_TLS_REJECT_UNAUTHORIZED;
    process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";
    let wssHelper: any;
    try {
      wssHelper = await createSpWssServer(0);
      const sock = new Pair0Socket(wssHelper.url);
      await sock.connect();
      const payload = new TextEncoder().encode("secret-wss-message");
      await sock.send(payload);
      const reply = await sock.recv();
      expect(new TextDecoder().decode(reply)).toBe("secret-wss-message");
      sock.close();
    } finally {
      if (wssHelper) await wssHelper.close();
      if (origReject !== undefined) {
        process.env.NODE_TLS_REJECT_UNAUTHORIZED = origReject;
      } else {
        delete process.env.NODE_TLS_REJECT_UNAUTHORIZED;
      }
    }
  });

  it("triggers pipeNotify events on connection lifecycle and supports ADD_PRE rejection", async () => {
    const server = await createSpWsServer(0);
    try {
      // Test 1: Successful connection with hooks
      const events: { pipeId: number; ev: PipeEvent }[] = [];
      const sock1 = new Pair0Socket(server.url);
      sock1.pipeNotify((pipeId, ev) => {
        events.push({ pipeId, ev });
        return true;
      });
      await sock1.connect();
      expect(events.length).toBe(2);
      expect(events[0].ev).toBe("add_pre");
      expect(events[1].ev).toBe("add_post");

      sock1.close();
      await new Promise((r) => setTimeout(r, 50));
      expect(events.some((e) => e.ev === "rem_post")).toBe(true);

      // Test 2: ADD_PRE rejection drops pipe
      const rejectEvents: { pipeId: number; ev: PipeEvent }[] = [];
      const sock2 = new Pair0Socket(server.url);
      sock2.pipeNotify((pipeId, ev) => {
        rejectEvents.push({ pipeId, ev });
        if (ev === "add_pre") {
          return false; // Reject pipe!
        }
      });
      await expect(sock2.connect()).rejects.toThrow(
        "Connection rejected by pipeNotify ADD_PRE",
      );
      expect(rejectEvents.some((e) => e.ev === "add_pre")).toBe(true);
      expect(rejectEvents.some((e) => e.ev === "rem_post")).toBe(true);
      expect(rejectEvents.some((e) => e.ev === "add_post")).toBe(false);
    } finally {
      await server.close();
    }
  });

  it("multiplexes multiple concurrent contexts on a single Req0Socket", async () => {
    const server = await createSpWsServer(0);
    try {
      const sock = new Req0Socket(server.url);
      await sock.connect();

      const ctx1 = sock.openContext();
      const ctx2 = sock.openContext();
      const ctx3 = sock.openContext();

      expect(ctx1.id).not.toBe(ctx2.id);
      expect(ctx2.id).not.toBe(ctx3.id);

      // Send concurrent requests
      const p1 = (async () => {
        await ctx1.send(new TextEncoder().encode("req-ctx-1"));
        const res = await ctx1.recv();
        return new TextDecoder().decode(res);
      })();

      const p2 = (async () => {
        await ctx2.send(new TextEncoder().encode("req-ctx-2"));
        const res = await ctx2.recv();
        return new TextDecoder().decode(res);
      })();

      const p3 = (async () => {
        await ctx3.send(new TextEncoder().encode("req-ctx-3"));
        const res = await ctx3.recv();
        return new TextDecoder().decode(res);
      })();

      const [r1, r2, r3] = await Promise.all([p1, p2, p3]);
      expect(r1).toBe("rep:req-ctx-1");
      expect(r2).toBe("rep:req-ctx-2");
      expect(r3).toBe("rep:req-ctx-3");

      ctx1.close();
      ctx2.close();
      ctx3.close();
      expect(ctx1.isClosed).toBe(true);
      await expect(ctx1.send(new TextEncoder().encode("fail"))).rejects.toThrow(
        "Context is closed",
      );

      sock.close();
    } finally {
      await server.close();
    }
  });

  it("automatically retransmits unreplied request when resendTimeMs is configured", async () => {
    let receivedCount = 0;
    const server = await createSpWsServer(
      0,
      (socket, payload, _subproto, helper) => {
        receivedCount++;
        if (receivedCount === 1) {
          // Drop first request silently (simulate network packet drop)
          return;
        }
        // On second request (resend), reply!
        const reqId = payload.subarray(0, 4);
        const body = payload.subarray(4);
        const reply = Buffer.concat([reqId, Buffer.from("rep:"), body]);
        helper.sendBinary(socket, reply);
      },
    );

    try {
      const sock = new Req0Socket(server.url, { resendTimeMs: 150 });
      await sock.connect();

      await sock.send(new TextEncoder().encode("needs-resend"));
      const reply = await sock.recv();
      expect(new TextDecoder().decode(reply)).toBe("rep:needs-resend");
      expect(receivedCount).toBeGreaterThanOrEqual(2);

      sock.close();
    } finally {
      await server.close();
    }
  });

  it("enforces survey deadline timer (surveyTimeMs)", async () => {
    const server = await createSpWsServer(
      0,
      (socket, payload, _subproto, helper) => {
        // Delay response past deadline
        setTimeout(() => {
          const surveyId = payload.subarray(0, 4);
          const body = payload.subarray(4);
          const resp = Buffer.concat([surveyId, Buffer.from("resp:"), body]);
          helper.sendBinary(socket, resp);
        }, 300);
      },
    );

    try {
      const sock = new Surveyor0Socket(server.url, { surveyTimeMs: 100 });
      await sock.connect();

      await sock.send(new TextEncoder().encode("short-lived-survey"));
      await expect(sock.recv()).rejects.toThrow();

      sock.close();
    } finally {
      await server.close();
    }
  });

  it("correctly sets and retrieves socket options (maxTtl, polyamorous, recvMaxSize, raw)", async () => {
    const sock = new Pair1Socket("ws://127.0.0.1:9999", {
      maxTtl: 12,
      polyamorous: true,
      recvMaxSize: 65536,
      raw: false,
    });

    expect(sock.getOpt("maxTtl")).toBe(12);
    expect(sock.getOpt("polyamorous")).toBe(true);
    expect(sock.getOpt("recvMaxSize")).toBe(65536);
    expect(sock.getOpt("raw")).toBe(false);

    sock.setOpt("maxTtl", 4);
    expect(sock.getOpt("maxTtl")).toBe(4);

    sock.setOpt("polyamorous", false);
    expect(sock.getOpt("polyamorous")).toBe(false);

    sock.setOpt("raw", true);
    expect(sock.getOpt("raw")).toBe(true);
  });

  it("connects and communicates using two-step Dialer lifecycle", async () => {
    const server = await createSpWsServer(0);
    try {
      const sock = new Pair0Socket(server.url);
      const dialer = sock.newDialer(server.url, {
        reconnectTimeMinMs: 50,
        reconnectTimeMaxMs: 500,
        autoReconnect: true,
      });

      await dialer.start();

      await sock.send(new TextEncoder().encode("dialer-test"));
      const reply = await sock.recv();
      expect(new TextDecoder().decode(reply)).toBe("dialer-test");

      dialer.close();
    } finally {
      await server.close();
    }
  });
});
