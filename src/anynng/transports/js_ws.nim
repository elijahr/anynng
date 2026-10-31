# js_ws.nim - Pure-Nim WebAssembly / Browser & Node.js Native WebSocket Transport
# Complements wasm_bridge for SP protocol framing in JS/WASM environments.
# Licensed under MIT License.

when defined(js):
  import std/asyncjs
  import std/jsffi
  import ../core/types
  import ../core/framing/ws_framing
  import ../core/protocols/[pair0, pair1, pubsub0, reqrep0, pipeline0, survey0, bus0]

  type
    JsWebSocket* = ref object of JsObject
    JsMessageEvent* = ref object of JsObject
    JsUint8Array* = ref object of JsObject

    JsWsSocket* = ref object
      url*: string
      proto*: SpProto
      ws*: JsWebSocket
      pipeId*: PipeId
      isOpen*: bool
      isClosed*: bool
      recvQueue*: seq[seq[byte]]
      recvWaiters*: seq[proc(val: seq[byte]) {.closure.}]
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

  var gNextPipeId: PipeId = 1'u64

  proc allocPipeId(): PipeId =
    result = gNextPipeId
    inc gNextPipeId

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
    of spPair0:
      s.pair0.step(ev)
    of spPair1:
      s.pair1.step(ev)
    of spPub0:
      s.pub0.step(ev)
    of spSub0:
      s.sub0.step(ev)
    of spReq0:
      s.req0.step(ev)
    of spRep0:
      s.rep0.step(ev)
    of spPush0:
      s.push0.step(ev)
    of spPull0:
      s.pull0.step(ev)
    of spSurveyor0:
      s.surveyor0.step(ev)
    of spRespondent0:
      s.respondent0.step(ev)
    of spBus0:
      s.bus0.step(ev)

  proc dispatchActions(s: JsWsSocket, actions: seq[EngineAction]) =
    for action in actions:
      case action.kind
      of actSendPipe:
        let u8 = toUint8Array(action.rawFrame)
        let ws = s.ws
        {.emit: "`ws`.send(`u8`);".}
      of actDeliverRecv:
        if s.recvWaiters.len > 0:
          let waiter = s.recvWaiters[0]
          s.recvWaiters.delete(0)
          waiter(action.msg.body)
        else:
          s.recvQueue.add(action.msg.body)
      of actClosePipe:
        let ws = s.ws
        {.emit: "`ws`.close(1000);".}
      of actFinishSend:
        discard
      of actFailContext:
        discard

  proc dialJsWs*(url: string, proto: SpProto): Future[JsWsSocket] =
    let jsUrl = cstring(url)
    let jsSub = cstring(dialerSubprotocol(proto))
    let p = newPromise[JsWsSocket](
      proc(resolve: proc(val: JsWsSocket)) =
        var ws: JsWebSocket
        {.emit: "`ws` = new globalThis.WebSocket(`jsUrl`, `jsSub`);".}
        {.emit: "`ws`.binaryType = 'arraybuffer';".}

        let sock = JsWsSocket(
          url: url,
          proto: proto,
          ws: ws,
          pipeId: allocPipeId(),
          isOpen: false,
          isClosed: false,
          recvQueue: @[],
          recvWaiters: @[],
        )

        case proto
        of spPair0:
          sock.pair0 = initPair0Engine()
        of spPair1:
          sock.pair1 = initPair1Engine()
        of spPub0:
          sock.pub0 = initPub0Engine()
        of spSub0:
          sock.sub0 = initSub0Engine()
        of spReq0:
          sock.req0 = initReq0Engine()
        of spRep0:
          sock.rep0 = initRep0Engine()
        of spPush0:
          sock.push0 = initPush0Engine()
        of spPull0:
          sock.pull0 = initPull0Engine()
        of spSurveyor0:
          sock.surveyor0 = initSurveyor0Engine()
        of spRespondent0:
          sock.respondent0 = initRespondent0Engine()
        of spBus0:
          sock.bus0 = initBus0Engine()

        var settled = false

        let onOpenCb = proc() =
          if settled:
            return
          settled = true
          sock.isOpen = true
          let acts =
            sock.stepEngine(EngineEvent(kind: evPipeAdded, pipeId: sock.pipeId))
          sock.dispatchActions(acts)
          resolve(sock)

        let onErrorCb = proc(err: JsObject) =
          if not settled:
            settled = true
            resolve(sock)

        let onMessageCb = proc(ev: JsMessageEvent) =
          var u8: JsUint8Array
          {.emit: "`u8` = new Uint8Array(`ev`.data);".}
          let rawFrame = toSeqByte(u8)
          let acts = sock.stepEngine(
            EngineEvent(kind: evPipeMessage, pipeId: sock.pipeId, rawFrame: rawFrame)
          )
          sock.dispatchActions(acts)

        let onCloseCb = proc(ev: JsObject) =
          sock.isOpen = false
          sock.isClosed = true
          let acts =
            sock.stepEngine(EngineEvent(kind: evPipeRemoved, pipeId: sock.pipeId))
          sock.dispatchActions(acts)

        {.emit: "`ws`.onopen = `onOpenCb`;".}
        {.emit: "`ws`.onerror = `onErrorCb`;".}
        {.emit: "`ws`.onmessage = `onMessageCb`;".}
        {.emit: "`ws`.onclose = `onCloseCb`;".}
    )
    return p

  proc send*(s: JsWsSocket, data: seq[byte]): Future[void] =
    let payload = data
    let p = newPromise(
      proc(resolve: proc()) =
        if s.isClosed:
          resolve()
          return

        let acts =
          s.stepEngine(EngineEvent(kind: evUserSend, ctxId: 1, msg: initSpMsg(payload)))
        s.dispatchActions(acts)
        resolve()
    )
    return p

  proc send*(s: JsWsSocket, data: openArray[byte]): Future[void] =
    s.send(@data)

  proc recv*(s: JsWsSocket): Future[seq[byte]] =
    let p = newPromise[seq[byte]](
      proc(resolve: proc(val: seq[byte])) =
        if s.recvQueue.len > 0:
          let msg = s.recvQueue[0]
          s.recvQueue.delete(0)
          resolve(msg)
        elif s.isClosed:
          resolve(@[])
        else:
          s.recvWaiters.add(
            proc(val: seq[byte]) =
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
    let p = newPromise(
      proc(resolve: proc()) =
        s.isClosed = true
        let ws = s.ws
        if ws != nil:
          {.emit: "`ws`.close(1000);".}
        resolve()
    )
    return p
