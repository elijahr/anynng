# js_ws.nim - Browser and Node.js WebSocket Transport for anynng
# Pure-Nim client transport targeting JS backend (std/asyncjs + globalThis.WebSocket)
# Complements wasm_bridge for SP protocol framing in JS/WASM environments.
# Licensed under MIT License.

when defined(js):
  import std/asyncjs
  import std/jsffi
  import std/tables
  import ../core/types
  import ../core/framing/ws_framing
  import ../core/protocols/[pair0, pair1, pubsub0, reqrep0, pipeline0, survey0, bus0]

  type
    JsWebSocket* = ref object of JsObject
    JsMessageEvent* = ref object of JsObject
    JsUint8Array* = ref object of JsObject

    PipeEvent* = enum
      pipeEvAddPre = 1
      pipeEvAddPost = 2
      pipeEvRemPost = 3

    CtxId* = uint32

    PipeNotifyHook* = proc(pipeId: PipeId, ev: PipeEvent): bool {.closure.}

    JsContext* = ref object
      id*: CtxId
      socket*: JsWsSocket
      isClosed*: bool
      recvTimeoutMs*: int
      resendTimeMs*: int
      recvQueue*: seq[seq[byte]]
      recvWaiters*: seq[proc(val: seq[byte]) {.closure.}]
      pendingReqId*: uint32
      resendTimer*: JsObject
      lastSentMsg*: SpMsg

    JsWsSocket* = ref object
      url*: string
      proto*: SpProto
      ws*: JsWebSocket
      pipeId*: PipeId
      dialer*: JsDialer
      isOpen*: bool
      isClosed*: bool
      raw*: bool
      maxTtl*: int
      polyamorous*: bool
      recvMaxSize*: int
      resendTimeMs*: int
      surveyTimeMs*: int
      reconnectTimeMinMs*: int
      reconnectTimeMaxMs*: int
      pipeNotifyHook*: PipeNotifyHook
      recvQueue*: seq[seq[byte]]
      recvWaiters*: seq[proc(val: seq[byte]) {.closure.}]
      contexts*: seq[JsContext]
      nextCtxId*: CtxId
      # Protocol engines
      pair0*: Pair0Engine
      pair1*: Pair1Engine
      pub0*: Pub0Engine
      sub0*: Sub0Engine
      req0*: Req0Engine
      rep0*: Rep0Engine
      push0*: Push0Engine
      pull0*: Pull0Engine
      surveyor0*: Surveyor0Engine
      respondent0*: Respondent0Engine
      bus0*: Bus0Engine
      # Survey deadline & timers
      surveyDeadline*: float64
      reqResendTimer*: JsObject
      lastReqMsg*: SpMsg

    JsDialer* = ref object
      socket*: JsWsSocket
      url*: string
      proto*: SpProto
      active*: bool
      reconnectTimeMinMs*: int
      reconnectTimeMaxMs*: int
      currentBackoffMs*: int
      reconnectTimer*: JsObject

  var gNextPipeId: PipeId = 1'u64

  proc start*(d: JsDialer): Future[void] {.async.}
  proc connectWs*(sock: JsWsSocket, url: string): Future[void]

  proc allocPipeId(): PipeId =
    result = gNextPipeId
    inc gNextPipeId

  proc jsNow(): float64 =
    var t: float64
    {.emit: "`t` = Date.now();".}
    return t

  proc toUint8Array*(data: openArray[byte]): JsUint8Array =
    let len = data.len
    var u: JsUint8Array
    {.emit: "`u` = new Uint8Array(`len`);".}
    for i in 0 ..< len:
      let b = data[i]
      {.emit: "`u`[`i`] = `b`;".}
    result = u

  proc toSeqByte*(u: JsUint8Array): seq[byte] =
    var len: int
    {.emit: "`len` = `u`.byteLength;".}
    result = newSeq[byte](len)
    for i in 0 ..< len:
      var b: byte
      {.emit: "`b` = `u`[`i`];".}
      result[i] = b

  proc stepEngine(s: JsWsSocket, ev: EngineEvent): seq[EngineAction] =
    case s.proto
    of spPair0: s.pair0.step(ev)
    of spPair1: s.pair1.step(ev)
    of spPub0: s.pub0.step(ev)
    of spSub0: s.sub0.step(ev)
    of spReq0: s.req0.step(ev)
    of spRep0: s.rep0.step(ev)
    of spPush0: s.push0.step(ev)
    of spPull0: s.pull0.step(ev)
    of spSurveyor0: s.surveyor0.step(ev)
    of spRespondent0: s.respondent0.step(ev)
    of spBus0: s.bus0.step(ev)

  proc findContext(s: JsWsSocket, ctxId: CtxId): JsContext =
    for ctx in s.contexts:
      if ctx.id == ctxId:
        return ctx
    return nil

  proc dispatchActions(s: JsWsSocket, actions: seq[EngineAction]) =
    for action in actions:
      case action.kind
      of actSendPipe:
        let u8 = toUint8Array(action.rawFrame)
        let ws = s.ws
        if ws != nil:
          {.emit: "`ws`.send(`u8`);".}
      of actDeliverRecv:
        # Check survey deadline filter
        if s.proto == spSurveyor0 and s.surveyTimeMs > 0:
          if jsNow() > s.surveyDeadline:
            continue # Drop response received after survey deadline

        # Clear REQ0 resend timer if active
        if s.proto == spReq0 and s.reqResendTimer != nil:
          let timer = s.reqResendTimer
          {.emit: "clearInterval(`timer`);".}
          s.reqResendTimer = nil

        let targetCtx = if action.ctxId > 1: s.findContext(action.ctxId) else: nil
        if targetCtx != nil:
          if targetCtx.resendTimer != nil:
            let timer = targetCtx.resendTimer
            {.emit: "clearInterval(`timer`);".}
            targetCtx.resendTimer = nil
          if targetCtx.recvWaiters.len > 0:
            let waiter = targetCtx.recvWaiters[0]
            targetCtx.recvWaiters.delete(0)
            waiter(action.msg.body)
          else:
            targetCtx.recvQueue.add(action.msg.body)
        else:
          if s.recvWaiters.len > 0:
            let waiter = s.recvWaiters[0]
            s.recvWaiters.delete(0)
            waiter(action.msg.body)
          else:
            s.recvQueue.add(action.msg.body)
      of actClosePipe:
        let ws = s.ws
        if ws != nil:
          {.emit: "`ws`.close(1000);".}
      of actFinishSend:
        discard
      of actFailContext:
        discard

  proc initSocketEngine(sock: JsWsSocket) =
    case sock.proto
    of spPair0: sock.pair0 = initPair0Engine(sock.raw)
    of spPair1: sock.pair1 = initPair1Engine(sock.polyamorous, sock.raw, uint32(sock.maxTtl))
    of spPub0: sock.pub0 = initPub0Engine(sock.raw)
    of spSub0: sock.sub0 = initSub0Engine(sock.raw)
    of spReq0: sock.req0 = initReq0Engine(sock.raw, int64(sock.resendTimeMs))
    of spRep0: sock.rep0 = initRep0Engine(sock.raw, uint32(sock.maxTtl))
    of spPush0: sock.push0 = initPush0Engine(sock.raw)
    of spPull0: sock.pull0 = initPull0Engine(sock.raw)
    of spSurveyor0: sock.surveyor0 = initSurveyor0Engine(sock.raw, int64(sock.surveyTimeMs))
    of spRespondent0: sock.respondent0 = initRespondent0Engine(sock.raw, uint32(sock.maxTtl))
    of spBus0: sock.bus0 = initBus0Engine(sock.raw)

  proc newJsWsSocket*(proto: SpProto, raw: bool = false): JsWsSocket =
    result = JsWsSocket(
      url: "",
      proto: proto,
      ws: nil,
      pipeId: allocPipeId(),
      dialer: nil,
      isOpen: false,
      isClosed: false,
      raw: raw,
      maxTtl: 8,
      polyamorous: false,
      recvMaxSize: 1024 * 1024,
      resendTimeMs: 0,
      surveyTimeMs: 1000,
      reconnectTimeMinMs: 100,
      reconnectTimeMaxMs: 10000,
      pipeNotifyHook: nil,
      recvQueue: @[],
      recvWaiters: @[],
      contexts: @[],
      nextCtxId: 2,
      surveyDeadline: 0.0,
      reqResendTimer: nil,
      lastReqMsg: initSpMsg(@[])
    )
    result.initSocketEngine()

  proc connectWs*(sock: JsWsSocket, url: string): Future[void] =
    let jsUrl = cstring(url)
    let jsSub = cstring(dialerSubprotocol(sock.proto))
    sock.url = url
    let p = newPromise(proc(resolve: proc()) =
      var ws: JsWebSocket
      {.emit: "`ws` = new globalThis.WebSocket(`jsUrl`, `jsSub`);".}
      {.emit: "`ws`.binaryType = 'arraybuffer';".}
      sock.ws = ws

      var settled = false

      let onOpenCb = proc() =
        if settled: return
        settled = true

        let pipeId = sock.pipeId
        var allow = true
        if sock.pipeNotifyHook != nil:
          allow = sock.pipeNotifyHook(pipeId, pipeEvAddPre)

        if not allow:
          sock.isOpen = false
          let curWs = sock.ws
          if curWs != nil:
            {.emit: "`curWs`.close(1000);".}
          if sock.pipeNotifyHook != nil:
            discard sock.pipeNotifyHook(pipeId, pipeEvRemPost)
          resolve()
          return

        sock.isOpen = true
        let acts = sock.stepEngine(EngineEvent(kind: evPipeAdded, pipeId: pipeId))
        sock.dispatchActions(acts)
        if sock.pipeNotifyHook != nil:
          discard sock.pipeNotifyHook(pipeId, pipeEvAddPost)
        resolve()

      let onErrorCb = proc(err: JsObject) =
        if not settled:
          settled = true
          resolve()

      let onMessageCb = proc(ev: JsMessageEvent) =
        var u8: JsUint8Array
        {.emit: "`u8` = new Uint8Array(`ev`.data);".}
        let rawFrame = toSeqByte(u8)

        # Enforce recvMaxSize buffer boundary
        if rawFrame.len > sock.recvMaxSize:
          return

        if sock.raw:
          # Raw mode bypasses cooked protocol engine
          if sock.recvWaiters.len > 0:
            let waiter = sock.recvWaiters[0]
            sock.recvWaiters.delete(0)
            waiter(rawFrame)
          else:
            sock.recvQueue.add(rawFrame)
          return

        let acts = sock.stepEngine(EngineEvent(
          kind: evPipeMessage,
          pipeId: sock.pipeId,
          rawFrame: rawFrame
        ))
        sock.dispatchActions(acts)

      let onCloseCb = proc(ev: JsObject) =
        let wasOpen = sock.isOpen
        sock.isOpen = false
        sock.isClosed = true
        if wasOpen:
          let acts = sock.stepEngine(EngineEvent(kind: evPipeRemoved, pipeId: sock.pipeId))
          sock.dispatchActions(acts)
          if sock.pipeNotifyHook != nil:
            discard sock.pipeNotifyHook(sock.pipeId, pipeEvRemPost)

        if sock.dialer != nil and sock.dialer.active:
          let d = sock.dialer
          let delay = d.currentBackoffMs
          d.currentBackoffMs = min(d.currentBackoffMs * 2, d.reconnectTimeMaxMs)
          let dialerRef = d
          let retryCb = proc() =
            if dialerRef.active:
              discard dialerRef.start()
          var timerId: JsObject
          {.emit: "`timerId` = setTimeout(`retryCb`, `delay`);".}
          d.reconnectTimer = timerId

      {.emit: "`ws`.onopen = `onOpenCb`;".}
      {.emit: "`ws`.onerror = `onErrorCb`;".}
      {.emit: "`ws`.onmessage = `onMessageCb`;".}
      {.emit: "`ws`.onclose = `onCloseCb`;".}
    )
    return p

  proc dialJsWs*(url: string, proto: SpProto, raw: bool = false, pipeNotifyHook: PipeNotifyHook = nil): Future[JsWsSocket] {.async.} =
    let sock = newJsWsSocket(proto, raw)
    sock.pipeNotifyHook = pipeNotifyHook
    await sock.connectWs(url)
    return sock

  proc pipeNotify*(s: JsWsSocket, hook: PipeNotifyHook) =
    s.pipeNotifyHook = hook

  proc setOpt*(s: JsWsSocket, name: string, val: int) =
    case name
    of "ttl-max", "maxTtl":
      s.maxTtl = val
      if s.proto == spPair1: s.pair1.maxTtl = uint32(val)
      elif s.proto == spRep0: s.rep0.maxTtl = uint32(val)
      elif s.proto == spRespondent0: s.respondent0.maxTtl = uint32(val)
    of "recv-max-size", "recvMaxSize":
      s.recvMaxSize = val
    of "req:resend-time", "resendTime":
      s.resendTimeMs = val
      if s.proto == spReq0: s.req0.resendTimeMs = int64(val)
    of "surveyor:survey-time", "surveyTime":
      s.surveyTimeMs = val
      if s.proto == spSurveyor0: s.surveyor0.surveyTimeMs = int64(val)
    of "reconnect-time-min", "reconnectTimeMin":
      s.reconnectTimeMinMs = val
    of "reconnect-time-max", "reconnectTimeMax":
      s.reconnectTimeMaxMs = val
    else: discard

  proc getOptInt*(s: JsWsSocket, name: string): int =
    case name
    of "ttl-max", "maxTtl": return s.maxTtl
    of "recv-max-size", "recvMaxSize": return s.recvMaxSize
    of "req:resend-time", "resendTime": return s.resendTimeMs
    of "surveyor:survey-time", "surveyTime": return s.surveyTimeMs
    of "reconnect-time-min", "reconnectTimeMin": return s.reconnectTimeMinMs
    of "reconnect-time-max", "reconnectTimeMax": return s.reconnectTimeMaxMs
    else: return 0

  proc setOptBool*(s: JsWsSocket, name: string, val: bool) =
    case name
    of "pair1:polyamorous", "polyamorous":
      s.polyamorous = val
      if s.proto == spPair1: s.pair1.polyamorous = val
    of "raw":
      s.raw = val
    else: discard

  proc getOptBool*(s: JsWsSocket, name: string): bool =
    case name
    of "pair1:polyamorous", "polyamorous": return s.polyamorous
    of "raw": return s.raw
    else: return false

  proc send*(s: JsWsSocket, data: seq[byte]): Future[void] =
    let payload = data
    let p = newPromise(proc(resolve: proc()) =
      if s.isClosed:
        resolve()
        return

      if s.raw:
        let u8 = toUint8Array(payload)
        let curWs = s.ws
        if curWs != nil:
          {.emit: "`curWs`.send(`u8`);".}
        resolve()
        return

      let msg = initSpMsg(payload)
      let acts = s.stepEngine(EngineEvent(
        kind: evUserSend,
        ctxId: 1,
        msg: msg
      ))
      s.dispatchActions(acts)

      if s.proto == spSurveyor0 and s.surveyTimeMs > 0:
        s.surveyDeadline = jsNow() + float64(s.surveyTimeMs)

      if s.proto == spReq0 and s.resendTimeMs > 0:
        s.lastReqMsg = msg
        let sockRef = s
        let resendMs = s.resendTimeMs
        let timerCb = proc() =
          if not sockRef.isClosed and sockRef.req0.pending.len > 0:
            let reActs = sockRef.stepEngine(EngineEvent(
              kind: evTimerTick,
              nowMs: int64(jsNow())
            ))
            sockRef.dispatchActions(reActs)
        var timerId: JsObject
        {.emit: "`timerId` = setInterval(`timerCb`, `resendMs`);".}
        s.reqResendTimer = timerId

      resolve()
    )
    return p

  proc send*(s: JsWsSocket, data: openArray[byte]): Future[void] =
    s.send(@data)

  proc recv*(s: JsWsSocket): Future[seq[byte]] =
    let p = newPromise[seq[byte]](proc(resolve: proc(val: seq[byte])) =
      if s.recvQueue.len > 0:
        let msg = s.recvQueue[0]
        s.recvQueue.delete(0)
        resolve(msg)
      elif s.isClosed:
        resolve(@[])
      else:
        s.recvWaiters.add(proc(val: seq[byte]) =
          resolve(val)
        )
    )
    return p

  proc subscribe*(s: JsWsSocket, topic: openArray[byte]) =
    if s.proto == spSub0:
      s.sub0.subscribe(topic)

  proc unsubscribe*(s: JsWsSocket, topic: openArray[byte]) =
    if s.proto == spSub0:
      s.sub0.unsubscribe(topic)

  proc close*(s: JsWsSocket): Future[void] =
    let p = newPromise(proc(resolve: proc()) =
      s.isClosed = true
      if s.reqResendTimer != nil:
        let timer = s.reqResendTimer
        {.emit: "clearInterval(`timer`);".}
        s.reqResendTimer = nil
      for ctx in s.contexts:
        ctx.isClosed = true
        if ctx.resendTimer != nil:
          let timer = ctx.resendTimer
          {.emit: "clearInterval(`timer`);".}
          ctx.resendTimer = nil
      let ws = s.ws
      if ws != nil:
        {.emit: "`ws`.close(1000);".}
      resolve()
    )
    return p

  # ----------------------------------------------------------------------
  # Context Multiplexing
  # ----------------------------------------------------------------------

  proc openContext*(s: JsWsSocket, recvTimeoutMs: int = 0, resendTimeMs: int = 0): JsContext =
    let ctxId = s.nextCtxId
    inc s.nextCtxId
    let ctx = JsContext(
      id: ctxId,
      socket: s,
      isClosed: false,
      recvTimeoutMs: recvTimeoutMs,
      resendTimeMs: resendTimeMs,
      recvQueue: @[],
      recvWaiters: @[],
      pendingReqId: 0'u32,
      resendTimer: nil,
      lastSentMsg: initSpMsg(@[])
    )
    s.contexts.add(ctx)
    return ctx

  proc send*(ctx: JsContext, data: seq[byte]): Future[void] =
    let payload = data
    let p = newPromise(proc(resolve: proc()) =
      if ctx.isClosed or ctx.socket.isClosed:
        resolve()
        return

      let msg = initSpMsg(payload)
      let acts = ctx.socket.stepEngine(EngineEvent(
        kind: evUserSend,
        ctxId: ctx.id,
        msg: msg
      ))
      ctx.socket.dispatchActions(acts)

      if ctx.socket.proto == spReq0 and ctx.resendTimeMs > 0:
        ctx.lastSentMsg = msg
        let ctxRef = ctx
        let resendMs = ctx.resendTimeMs
        let timerCb = proc() =
          if not ctxRef.isClosed and not ctxRef.socket.isClosed:
            let reActs = ctxRef.socket.stepEngine(EngineEvent(
              kind: evUserSend,
              ctxId: ctxRef.id,
              msg: ctxRef.lastSentMsg
            ))
            ctxRef.socket.dispatchActions(reActs)
        var timerId: JsObject
        {.emit: "`timerId` = setInterval(`timerCb`, `resendMs`);".}
        ctx.resendTimer = timerId

      resolve()
    )
    return p

  proc recv*(ctx: JsContext): Future[seq[byte]] =
    let p = newPromise[seq[byte]](proc(resolve: proc(val: seq[byte])) =
      if ctx.recvQueue.len > 0:
        let msg = ctx.recvQueue[0]
        ctx.recvQueue.delete(0)
        resolve(msg)
      elif ctx.isClosed or ctx.socket.isClosed:
        resolve(@[])
      else:
        ctx.recvWaiters.add(proc(val: seq[byte]) =
          resolve(val)
        )
    )
    return p

  proc close*(ctx: JsContext) =
    ctx.isClosed = true
    if ctx.resendTimer != nil:
      let timer = ctx.resendTimer
      {.emit: "clearInterval(`timer`);".}
      ctx.resendTimer = nil

  # ----------------------------------------------------------------------
  # Dialer with Auto-Reconnect Backoff
  # ----------------------------------------------------------------------

  proc newJsDialer*(s: JsWsSocket, url: string, reconnectMin: int = 100, reconnectMax: int = 10000): JsDialer =
    result = JsDialer(
      socket: s,
      url: url,
      proto: s.proto,
      active: false,
      reconnectTimeMinMs: reconnectMin,
      reconnectTimeMaxMs: reconnectMax,
      currentBackoffMs: reconnectMin,
      reconnectTimer: nil
    )
    s.dialer = result

  proc start*(d: JsDialer): Future[void] {.async.} =
    d.active = true
    d.currentBackoffMs = d.reconnectTimeMinMs
    await d.socket.connectWs(d.url)

  proc close*(d: JsDialer) =
    d.active = false
    if d.reconnectTimer != nil:
      let timer = d.reconnectTimer
      {.emit: "clearTimeout(`timer`);".}
      d.reconnectTimer = nil
    if d.socket != nil and d.socket.ws != nil:
      let curWs = d.socket.ws
      {.emit: "`curWs`.close(1000);".}
