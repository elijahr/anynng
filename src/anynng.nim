# anynng - Pure-Nim multi-target (C, JS, WASM) and multi-transport NNG / Scalability Protocols
# Licensed under MIT License.

import std/tables
import anynng/core/[types, options, tls, endian, url]
import anynng/core/framing/[stream_framing, ws_framing, udp_framing]
import anynng/core/protocols/[pair0, pair1, pubsub0, reqrep0, pipeline0, survey0, bus0]
import anynng/transports/inproc

when not defined(js):
  import std/[asyncdispatch, monotimes, times, net, nativesockets]
  import anynng/transports/[native_tcp, native_ipc, native_udp, native_ws, native_tls]
  export native_tcp, native_ipc, native_udp, native_ws, native_tls
else:
  import anynng/transports/js_ws
  export js_ws

export types, options, tls, endian, url, stream_framing, ws_framing, udp_framing

const projectVersion* = "0.0.1"

type
  EngineVariantKind = enum
    engPair0
    engPair1
    engPub0
    engSub0
    engReq0
    engRep0
    engPush0
    engPull0
    engSurveyor0
    engRespondent0
    engBus0

  Socket* = ref object
    proto*: SpProto
    engineKind: EngineVariantKind
    pair0Eng: Pair0Engine
    pair1Eng: Pair1Engine
    pub0Eng: Pub0Engine
    sub0Eng: Sub0Engine
    req0Eng: Req0Engine
    rep0Eng: Rep0Engine
    push0Eng: Push0Engine
    pull0Eng: Pull0Engine
    surveyor0Eng: Surveyor0Engine
    respondent0Eng: Respondent0Engine
    bus0Eng: Bus0Engine
    pipes: Table[PipeId, Pipe]
    recvQueue: seq[SpMsg]
    onRecvCb: proc(msg: SpMsg) {.closure.}
    closed*: bool
    raw*: bool
    options*: OptionStore
    pipeCallbacks*: Table[PipeEvent, PipeNotifyCb]
    contexts*: Table[uint32, Context]

  Dialer* = ref object
    socket*: Socket
    url*: string
    options*: OptionStore
    tlsCfg*: TlsConfig
    closed*: bool
    started*: bool
    closeImpl*: proc() {.closure.}

  Listener* = ref object
    socket*: Socket
    url*: string
    boundPort*: int
    options*: OptionStore
    tlsCfg*: TlsConfig
    closed*: bool
    started*: bool
    closeImpl*: proc() {.closure.}

  Context* = ref object
    socket*: Socket
    ctxId*: uint32
    options*: OptionStore
    closed*: bool
    recvQueue*: seq[SpMsg]
    onRecvCb*: proc(msg: SpMsg) {.closure.}

var gNextCtxId: uint32 = 1'u32

proc allocCtxId(): uint32 =
  result = gNextCtxId
  inc gNextCtxId

# Forward Declarations
proc executeActions*(s: Socket, actions: seq[EngineAction])
proc stepEngine*(s: Socket, ev: EngineEvent): seq[EngineAction]
proc attachCustomPipe*(s: Socket, pipe: Pipe)
proc detachCustomPipe*(s: Socket, pipeId: PipeId)
proc feedPipeFrame*(s: Socket, pipeId: PipeId, frame: openArray[byte])
proc close*(s: Socket)

# Socket Implementation
proc newSocket*(proto: SpProto, raw: bool = false): Socket =
  new(result)
  result.proto = proto
  result.raw = raw
  result.closed = false
  result.recvQueue = @[]
  result.pipes = initTable[PipeId, Pipe]()
  result.options = newOptionStore(entSocket)
  result.pipeCallbacks = initTable[PipeEvent, PipeNotifyCb]()
  result.contexts = initTable[uint32, Context]()

  result.options.setIntInternal(NNG_OPT_PROTO, int(proto))
  result.options.setStringInternal(NNG_OPT_PROTONAME, protoName(proto))
  result.options.setIntInternal(NNG_OPT_PEER, int(peerProto(proto)))
  result.options.setStringInternal(NNG_OPT_PEERNAME, protoName(peerProto(proto)))
  result.options.setBoolInternal(NNG_OPT_RAW, raw)

  case proto
  of spPair0:
    result.engineKind = engPair0
    result.pair0Eng = initPair0Engine(raw)
  of spPair1:
    result.engineKind = engPair1
    result.pair1Eng = initPair1Engine(false, raw)
  of spPub0:
    result.engineKind = engPub0
    result.pub0Eng = initPub0Engine(raw)
  of spSub0:
    result.engineKind = engSub0
    result.sub0Eng = initSub0Engine(raw)
  of spReq0:
    result.engineKind = engReq0
    result.req0Eng = initReq0Engine(raw)
  of spRep0:
    result.engineKind = engRep0
    result.rep0Eng = initRep0Engine(raw)
  of spPush0:
    result.engineKind = engPush0
    result.push0Eng = initPush0Engine(raw)
  of spPull0:
    result.engineKind = engPull0
    result.pull0Eng = initPull0Engine(raw)
  of spSurveyor0:
    result.engineKind = engSurveyor0
    result.surveyor0Eng = initSurveyor0Engine(raw)
  of spRespondent0:
    result.engineKind = engRespondent0
    result.respondent0Eng = initRespondent0Engine(raw)
  of spBus0:
    result.engineKind = engBus0
    result.bus0Eng = initBus0Engine(raw)

proc pipeNotify*(s: Socket, ev: PipeEvent, cb: PipeNotifyCb) =
  s.pipeCallbacks[ev] = cb

proc pipeCount*(s: Socket): int =
  s.pipes.len

proc executeActions*(s: Socket, actions: seq[EngineAction]) =
  for act in actions:
    case act.kind
    of actSendPipe:
      if act.pipeId in s.pipes:
        s.pipes[act.pipeId].send(act.rawFrame)
    of actClosePipe:
      if act.pipeId in s.pipes:
        let p = s.pipes[act.pipeId]
        s.pipes.del(act.pipeId)
        p.close()
    of actDeliverRecv:
      var delivered = false
      if act.ctxId != 0 and act.ctxId in s.contexts:
        let ctx = s.contexts[act.ctxId]
        if not ctx.closed:
          if ctx.onRecvCb != nil:
            let cb = ctx.onRecvCb
            ctx.onRecvCb = nil
            cb(act.msg)
            delivered = true
          else:
            let maxBuf = ctx.options.getInt(NNG_OPT_RECVBUF)
            if ctx.recvQueue.len < maxBuf:
              ctx.recvQueue.add(act.msg)
              delivered = true

      if not delivered:
        if s.onRecvCb != nil:
          let cb = s.onRecvCb
          s.onRecvCb = nil
          cb(act.msg)
        else:
          let maxBuf = s.options.getInt(NNG_OPT_RECVBUF)
          if s.recvQueue.len < maxBuf:
            s.recvQueue.add(act.msg)
          elif s.engineKind == engSub0 and s.options.getBool(NNG_OPT_SUB_PREFNEW):
            s.recvQueue.delete(0)
            s.recvQueue.add(act.msg)
    of actFinishSend:
      discard
    of actFailContext:
      if act.err != spOk:
        raise newException(ValueError, "Operation rejected by protocol: " & $act.err)

proc stepEngine*(s: Socket, ev: EngineEvent): seq[EngineAction] =
  case s.engineKind
  of engPair0:
    s.pair0Eng.step(ev)
  of engPair1:
    s.pair1Eng.step(ev)
  of engPub0:
    s.pub0Eng.step(ev)
  of engSub0:
    s.sub0Eng.step(ev)
  of engReq0:
    s.req0Eng.step(ev)
  of engRep0:
    s.rep0Eng.step(ev)
  of engPush0:
    s.push0Eng.step(ev)
  of engPull0:
    s.pull0Eng.step(ev)
  of engSurveyor0:
    s.surveyor0Eng.step(ev)
  of engRespondent0:
    s.respondent0Eng.step(ev)
  of engBus0:
    s.bus0Eng.step(ev)

proc attachCustomPipe*(s: Socket, pipe: Pipe) =
  pipe.sockPtr = cast[pointer](s)

  # NNG_PIPE_EV_ADD_PRE hook
  if pipeEvAddPre in s.pipeCallbacks:
    var rejected = false
    let origClose = pipe.closeImpl
    pipe.closeImpl = proc() =
      rejected = true
      if origClose != nil:
        origClose()

    s.pipeCallbacks[pipeEvAddPre](pipe, pipeEvAddPre)
    pipe.closeImpl = origClose
    if rejected:
      pipe.close()
      return

  s.pipes[pipe.pipeId] = pipe
  let pid = pipe.pipeId
  let ev = EngineEvent(kind: evPipeAdded, pipeId: pid)
  let actions = s.stepEngine(ev)
  s.executeActions(actions)

  # NNG_PIPE_EV_ADD_POST hook
  if pipeEvAddPost in s.pipeCallbacks:
    s.pipeCallbacks[pipeEvAddPost](pipe, pipeEvAddPost)

proc detachCustomPipe*(s: Socket, pipeId: PipeId) =
  if pipeId in s.pipes:
    let p = s.pipes[pipeId]
    s.pipes.del(pipeId)
    let ev = EngineEvent(kind: evPipeRemoved, pipeId: pipeId)
    let actions = s.stepEngine(ev)
    s.executeActions(actions)

    # NNG_PIPE_EV_REM_POST hook
    if pipeEvRemPost in s.pipeCallbacks:
      s.pipeCallbacks[pipeEvRemPost](p, pipeEvRemPost)

proc feedPipeFrame*(s: Socket, pipeId: PipeId, frame: openArray[byte]) =
  if s.closed:
    return
  let ev = EngineEvent(kind: evPipeMessage, pipeId: pipeId, rawFrame: @frame)
  let actions = s.stepEngine(ev)
  s.executeActions(actions)

proc attachEndpoint(s: Socket, ep: InprocEndpoint) =
  let pid = ep.pipeId
  ep.onMessage = proc(rawFrame: seq[byte]) =
    s.feedPipeFrame(pid, rawFrame)

  ep.onClose = proc() =
    s.detachCustomPipe(pid)

  let pipe = newPipe(
    pid,
    s.proto,
    proc(data: openArray[byte]) =
      ep.send(data),
    proc() =
      ep.close(),
  )
  s.attachCustomPipe(pipe)

# Option Methods for Socket
proc getOpt*(s: Socket, name: string): OptionValue =
  if name notin s.options.values:
    raise newNngError(NNG_ENOTSUP, "Option " & name & " not supported on socket")
  s.options.values[name]

proc setOpt*(s: Socket, name: string, val: bool) =
  s.options.setBool(name, val)
  if name == NNG_OPT_PAIR1_POLY and s.engineKind == engPair1:
    s.pair1Eng.polyamorous = val

proc setOpt*(s: Socket, name: string, val: int) =
  s.options.setInt(name, val)
  if name == NNG_OPT_MAXTTL:
    # Propagate to engines
    discard

proc setOpt*(s: Socket, name: string, val: int64) =
  s.options.setMs(name, val)
  if name == NNG_OPT_REQ_RESENDTIME and s.engineKind == engReq0:
    s.req0Eng.resendTimeMs = val
  elif name == NNG_OPT_SURVEYOR_SURVEYTIME and s.engineKind == engSurveyor0:
    s.surveyor0Eng.surveyTimeMs = val

proc setOpt*(s: Socket, name: string, val: uint64) =
  s.options.setSize(name, val)

proc setOpt*(s: Socket, name: string, val: string) =
  if name == NNG_OPT_SUB_SUBSCRIBE:
    s.options.validateAccess(name, isSet = true, optKindString)
    if s.engineKind == engSub0:
      var b = newSeq[byte](val.len)
      for i in 0 ..< val.len:
        b[i] = byte(val[i])
      s.sub0Eng.subscribe(b)
  elif name == NNG_OPT_SUB_UNSUBSCRIBE:
    s.options.validateAccess(name, isSet = true, optKindString)
    if s.engineKind == engSub0:
      var b = newSeq[byte](val.len)
      for i in 0 ..< val.len:
        b[i] = byte(val[i])
      s.sub0Eng.unsubscribe(b)
  else:
    s.options.setString(name, val)

proc getBool*(s: Socket, name: string): bool =
  s.options.getBool(name)

proc getInt*(s: Socket, name: string): int =
  s.options.getInt(name)

proc getMs*(s: Socket, name: string): int64 =
  s.options.getMs(name)

proc getSize*(s: Socket, name: string): uint64 =
  s.options.getSize(name)

proc getString*(s: Socket, name: string): string =
  s.options.getString(name)

# Context Implementation
proc openContext*(s: Socket): Context =
  let cid = allocCtxId()
  new(result)
  result.socket = s
  result.ctxId = cid
  result.options = newOptionStore(entContext)
  result.closed = false
  result.recvQueue = @[]
  s.contexts[cid] = result

proc close*(ctx: Context) =
  if not ctx.closed:
    ctx.closed = true
    if ctx.socket != nil and ctx.ctxId in ctx.socket.contexts:
      ctx.socket.contexts.del(ctx.ctxId)

proc sendMsg*(ctx: Context, msg: SpMsg) =
  if ctx.closed or ctx.socket.closed:
    raise newNngError(NNG_ECLOSED, "Context or socket is closed")
  let ev = EngineEvent(kind: evUserSend, ctxId: ctx.ctxId, msg: msg)
  let actions = ctx.socket.stepEngine(ev)
  ctx.socket.executeActions(actions)

proc send*(ctx: Context, data: openArray[byte]) =
  ctx.sendMsg(initSpMsg(data))

proc send*(ctx: Context, str: string) =
  var bytes = newSeq[byte](str.len)
  for i in 0 ..< str.len:
    bytes[i] = byte(str[i])
  ctx.send(bytes)

proc recvMsg*(ctx: Context, timeoutMs: int = -1): SpMsg =
  if ctx.closed or ctx.socket.closed:
    raise newNngError(NNG_ECLOSED, "Context or socket is closed")
  let tmo =
    if timeoutMs >= 0:
      timeoutMs
    else:
      int(ctx.options.getMs(NNG_OPT_RECVTIMEO))
  when not defined(js):
    let start = getMonoTime()
    while ctx.recvQueue.len == 0 and not ctx.closed and not ctx.socket.closed:
      asyncdispatch.poll(10)
      if tmo >= 0:
        let elapsed = (getMonoTime() - start).inMilliseconds
        if elapsed > tmo:
          raise newNngError(NNG_ETIMEDOUT, "Context recvMsg timed out")
  if ctx.recvQueue.len > 0:
    result = ctx.recvQueue[0]
    ctx.recvQueue.delete(0)
  else:
    raise newNngError(NNG_ECLOSED, "Context closed or empty")

proc recv*(ctx: Context, timeoutMs: int = -1): seq[byte] =
  result = ctx.recvMsg(timeoutMs).body

proc recvStr*(ctx: Context, timeoutMs: int = -1): string =
  let body = ctx.recv(timeoutMs)
  result = newString(body.len)
  for i in 0 ..< body.len:
    result[i] = char(body[i])

proc setOpt*(ctx: Context, name: string, val: int) =
  ctx.options.setInt(name, val)

proc setOpt*(ctx: Context, name: string, val: int64) =
  ctx.options.setMs(name, val)

proc getInt*(ctx: Context, name: string): int =
  ctx.options.getInt(name)

proc getMs*(ctx: Context, name: string): int64 =
  ctx.options.getMs(name)

# Pipe Introspection Methods
proc id*(p: Pipe): PipeId =
  p.pipeId

proc socket*(p: Pipe): Socket =
  (if p == nil or p.sockPtr == nil: nil
  else: cast[Socket](p.sockPtr))

proc dialer*(p: Pipe): Dialer =
  (if p == nil or p.dialerPtr == nil: nil
  else: cast[Dialer](p.dialerPtr))

proc listener*(p: Pipe): Listener =
  (if p == nil or p.listenerPtr == nil: nil
  else: cast[Listener](p.listenerPtr))

proc getBool*(p: Pipe, name: string): bool =
  case name
  of NNG_OPT_TLS_VERIFIED:
    p.tlsVerified
  else:
    raise newNngError(NNG_ENOTSUP, "Option " & name & " not supported on pipe")

proc getString*(p: Pipe, name: string): string =
  case name
  of NNG_OPT_TLS_PEER_CN:
    p.tlsPeerCn
  of NNG_OPT_LOCADDR:
    p.locAddr
  of NNG_OPT_REMADDR:
    p.remAddr
  else:
    raise newNngError(NNG_ENOTSUP, "Option " & name & " not supported on pipe")

# Dialer Implementation
proc newDialer*(s: Socket, urlStr: string): Dialer =
  new(result)
  result.socket = s
  result.url = urlStr
  result.options = newOptionStore(entDialer)
  result.options.setStringInternal(NNG_OPT_URL, urlStr)
  result.closed = false
  result.started = false
  result.tlsCfg = nil

proc setOpt*(d: Dialer, name: string, val: int64) =
  d.options.setMs(name, val)

proc setOpt*(d: Dialer, name: string, val: bool) =
  d.options.setBool(name, val)

proc setOpt*(d: Dialer, name: string, val: TlsConfig) =
  d.options.setPtr(NNG_OPT_TLS_CONFIG, cast[pointer](val))
  d.tlsCfg = val

proc setTlsConfig*(d: Dialer, val: TlsConfig) {.inline.} =
  d.setOpt(NNG_OPT_TLS_CONFIG, val)

proc getOpt*(d: Dialer, name: string): OptionValue =
  if name notin d.options.values:
    raise newNngError(NNG_ENOTSUP, "Option " & name & " not supported on dialer")
  d.options.values[name]

proc getMs*(d: Dialer, name: string): int64 =
  d.options.getMs(name)

proc getString*(d: Dialer, name: string): string =
  d.options.getString(name)

proc start*(d: Dialer) =
  if d.closed:
    raise newNngError(NNG_ECLOSED, "Dialer is closed")
  if d.started:
    return
  d.started = true

  let parsed = parseSpUrl(d.url)
  case parsed.kind
  of tranInproc:
    let ep = connectInproc(
      parsed.raw,
      d.socket.proto,
      proc(rawFrame: seq[byte]) =
        discard,
      proc() =
        discard,
    )
    d.socket.attachEndpoint(ep)
  of tranTcp:
    when not defined(js):
      let host = if parsed.host.len > 0: parsed.host else: "127.0.0.1"
      let pipe = waitFor connectTcpAsync(
        host,
        parsed.port,
        d.socket.proto,
        proc(pid: PipeId, frame: seq[byte]) =
          d.socket.feedPipeFrame(pid, frame),
        proc(pid: PipeId) =
          d.socket.detachCustomPipe(pid),
      )
      pipe.dialerPtr = cast[pointer](d)
      d.socket.attachCustomPipe(pipe)
    else:
      raise newNngError(NNG_ENOTSUP, "TCP dialer not supported on JS")
  of tranTlsTcp:
    when not defined(js):
      let host = if parsed.host.len > 0: parsed.host else: "127.0.0.1"
      let pipe = waitFor connectTlsTcpAsync(
        host,
        parsed.port,
        d.socket.proto,
        d.tlsCfg,
        proc(pid: PipeId, frame: seq[byte]) =
          d.socket.feedPipeFrame(pid, frame),
        proc(pid: PipeId) =
          d.socket.detachCustomPipe(pid),
      )
      pipe.dialerPtr = cast[pointer](d)
      d.socket.attachCustomPipe(pipe)
    else:
      raise newNngError(NNG_ENOTSUP, "TLS TCP dialer not supported on JS")
  of tranIpc:
    when not defined(js):
      let pipe = waitFor connectIpcAsync(
        parsed.path,
        d.socket.proto,
        proc(pid: PipeId, frame: seq[byte]) =
          d.socket.feedPipeFrame(pid, frame),
        proc(pid: PipeId) =
          d.socket.detachCustomPipe(pid),
      )
      pipe.dialerPtr = cast[pointer](d)
      d.socket.attachCustomPipe(pipe)
    else:
      raise newNngError(NNG_ENOTSUP, "IPC dialer not supported on JS")
  of tranUdp:
    when not defined(js):
      let host = if parsed.host.len > 0: parsed.host else: "127.0.0.1"
      let pipe = waitFor connectUdpAsync(
        host,
        parsed.port,
        d.socket.proto,
        proc(pid: PipeId, frame: seq[byte]) =
          d.socket.feedPipeFrame(pid, frame),
        proc(pid: PipeId) =
          d.socket.detachCustomPipe(pid),
      )
      pipe.dialerPtr = cast[pointer](d)
      d.socket.attachCustomPipe(pipe)
    else:
      raise newNngError(NNG_ENOTSUP, "UDP dialer not supported on JS")
  of tranWs:
    when not defined(js):
      let host = if parsed.host.len > 0: parsed.host else: "127.0.0.1"
      let pipe = waitFor connectWsAsync(
        host,
        parsed.port,
        parsed.path,
        d.socket.proto,
        proc(pid: PipeId, frame: seq[byte]) =
          d.socket.feedPipeFrame(pid, frame),
        proc(pid: PipeId) =
          d.socket.detachCustomPipe(pid),
        tlsCfg = d.tlsCfg,
      )
      pipe.dialerPtr = cast[pointer](d)
      d.socket.attachCustomPipe(pipe)
    else:
      raise newNngError(NNG_ENOTSUP, "Use js_ws on JS target")

proc close*(d: Dialer) =
  if not d.closed:
    d.closed = true
    if d.closeImpl != nil:
      d.closeImpl()

# Listener Implementation
proc newListener*(s: Socket, urlStr: string): Listener =
  new(result)
  result.socket = s
  result.url = urlStr
  result.options = newOptionStore(entListener)
  result.options.setStringInternal(NNG_OPT_URL, urlStr)
  result.closed = false
  result.started = false
  result.boundPort = 0
  result.tlsCfg = nil

proc setOpt*(l: Listener, name: string, val: bool) =
  l.options.setBool(name, val)

proc setOpt*(l: Listener, name: string, val: TlsConfig) =
  l.options.setPtr(NNG_OPT_TLS_CONFIG, cast[pointer](val))
  l.tlsCfg = val

proc setTlsConfig*(l: Listener, val: TlsConfig) {.inline.} =
  l.setOpt(NNG_OPT_TLS_CONFIG, val)

proc getOpt*(l: Listener, name: string): OptionValue =
  if name notin l.options.values:
    raise newNngError(NNG_ENOTSUP, "Option " & name & " not supported on listener")
  l.options.values[name]

proc getInt*(l: Listener, name: string): int =
  l.options.getInt(name)

proc getString*(l: Listener, name: string): string =
  l.options.getString(name)

proc getSockAddr*(l: Listener, name: string): string =
  l.options.getSockAddr(name)

proc start*(l: Listener) =
  if l.closed:
    raise newNngError(NNG_ECLOSED, "Listener is closed")
  if l.started:
    return
  l.started = true

  let parsed = parseSpUrl(l.url)
  case parsed.kind
  of tranInproc:
    let inprocListener = registerInprocListener(
      parsed.raw,
      l.socket.proto,
      proc(ep: InprocEndpoint) =
        l.socket.attachEndpoint(ep),
    )
    l.closeImpl = proc() =
      inprocListener.close()
  of tranTcp:
    when not defined(js):
      let host = if parsed.host.len > 0: parsed.host else: "127.0.0.1"
      let tcpListener = waitFor listenTcpAsync(
        host,
        parsed.port,
        l.socket.proto,
        proc(pipe: Pipe) =
          pipe.listenerPtr = cast[pointer](l)
          l.socket.attachCustomPipe(pipe),
        proc(pid: PipeId, frame: seq[byte]) =
          l.socket.feedPipeFrame(pid, frame),
        proc(pid: PipeId) =
          l.socket.detachCustomPipe(pid),
      )
      l.boundPort = tcpListener.boundPort
      l.options.setIntInternal(NNG_OPT_TCP_BOUND_PORT, l.boundPort)
      l.options.setSockAddrInternal(NNG_OPT_LOCADDR, "127.0.0.1:" & $l.boundPort)
      l.closeImpl = proc() =
        tcpListener.close()
    else:
      raise newNngError(NNG_ENOTSUP, "TCP listener not supported on JS")
  of tranTlsTcp:
    when not defined(js):
      let host = if parsed.host.len > 0: parsed.host else: "127.0.0.1"
      let tlsListener = waitFor listenTlsTcpAsync(
        host,
        parsed.port,
        l.socket.proto,
        l.tlsCfg,
        proc(pipe: Pipe) =
          pipe.listenerPtr = cast[pointer](l)
          l.socket.attachCustomPipe(pipe),
        proc(pid: PipeId, frame: seq[byte]) =
          l.socket.feedPipeFrame(pid, frame),
        proc(pid: PipeId) =
          l.socket.detachCustomPipe(pid),
      )
      l.boundPort = tlsListener.boundPort
      l.options.setIntInternal(NNG_OPT_TCP_BOUND_PORT, l.boundPort)
      l.options.setSockAddrInternal(NNG_OPT_LOCADDR, "127.0.0.1:" & $l.boundPort)
      l.closeImpl = proc() =
        tlsListener.close()
    else:
      raise newNngError(NNG_ENOTSUP, "TLS TCP listener not supported on JS")
  of tranIpc:
    when not defined(js):
      let ipcListener = waitFor listenIpcAsync(
        parsed.path,
        l.socket.proto,
        proc(pipe: Pipe) =
          pipe.listenerPtr = cast[pointer](l)
          l.socket.attachCustomPipe(pipe),
        proc(pid: PipeId, frame: seq[byte]) =
          l.socket.feedPipeFrame(pid, frame),
        proc(pid: PipeId) =
          l.socket.detachCustomPipe(pid),
      )
      l.closeImpl = proc() =
        ipcListener.close()
    else:
      raise newNngError(NNG_ENOTSUP, "IPC listener not supported on JS")
  of tranUdp:
    when not defined(js):
      let host = if parsed.host.len > 0: parsed.host else: "127.0.0.1"
      let udpListener = waitFor listenUdpAsync(
        host,
        parsed.port,
        l.socket.proto,
        proc(pipe: Pipe) =
          pipe.listenerPtr = cast[pointer](l)
          l.socket.attachCustomPipe(pipe),
        proc(pid: PipeId, frame: seq[byte]) =
          l.socket.feedPipeFrame(pid, frame),
        proc(pid: PipeId) =
          l.socket.detachCustomPipe(pid),
      )
      l.closeImpl = proc() =
        udpListener.close()
    else:
      raise newNngError(NNG_ENOTSUP, "UDP listener not supported on JS")
  of tranWs:
    when not defined(js):
      let host = if parsed.host.len > 0: parsed.host else: "127.0.0.1"
      let wsListener = waitFor listenWsAsync(
        host,
        parsed.port,
        parsed.path,
        l.socket.proto,
        proc(pipe: Pipe) =
          pipe.listenerPtr = cast[pointer](l)
          l.socket.attachCustomPipe(pipe),
        proc(pid: PipeId, frame: seq[byte]) =
          l.socket.feedPipeFrame(pid, frame),
        proc(pid: PipeId) =
          l.socket.detachCustomPipe(pid),
        tlsCfg = l.tlsCfg,
      )
      l.boundPort = wsListener.boundPort
      l.options.setIntInternal(NNG_OPT_TCP_BOUND_PORT, l.boundPort)
      l.options.setSockAddrInternal(NNG_OPT_LOCADDR, "127.0.0.1:" & $l.boundPort)
      l.closeImpl = proc() =
        wsListener.close()
    else:
      raise newNngError(NNG_ENOTSUP, "WS listener not supported on JS")

proc close*(l: Listener) =
  if not l.closed:
    l.closed = true
    if l.closeImpl != nil:
      l.closeImpl()

# High-Level Convenience Dial & Listen
proc dial*(s: Socket, urlStr: string) =
  let d = s.newDialer(urlStr)
  d.start()

proc listen*(s: Socket, urlStr: string): Listener =
  result = s.newListener(urlStr)
  result.start()

# Sending & Receiving on Socket
proc send*(s: Socket, data: openArray[byte], ctxId: uint32 = 0) =
  if s.closed:
    raise newNngError(NNG_ECLOSED, "Socket is closed")
  let cid =
    if ctxId != 0:
      ctxId
    else:
      allocCtxId()
  let msg = initSpMsg(data)
  let ev = EngineEvent(kind: evUserSend, ctxId: cid, msg: msg)
  let actions = s.stepEngine(ev)
  s.executeActions(actions)

proc send*(s: Socket, str: string) =
  var bytes = newSeq[byte](str.len)
  for i in 0 ..< str.len:
    bytes[i] = byte(str[i])
  s.send(bytes)

proc recvMsg*(s: Socket, timeoutMs: int = -1): SpMsg =
  if s.closed:
    raise newNngError(NNG_ECLOSED, "Socket is closed")
  let tmo =
    if timeoutMs >= 0:
      timeoutMs
    else:
      int(s.options.getMs(NNG_OPT_RECVTIMEO))
  when not defined(js):
    let start = getMonoTime()
    while s.recvQueue.len == 0 and not s.closed:
      asyncdispatch.poll(10)
      if tmo >= 0:
        let elapsed = (getMonoTime() - start).inMilliseconds
        if elapsed > tmo:
          raise
            newNngError(NNG_ETIMEDOUT, "Socket recvMsg timed out after " & $tmo & "ms")
  if s.recvQueue.len > 0:
    result = s.recvQueue[0]
    s.recvQueue.delete(0)
  else:
    raise newNngError(NNG_ECLOSED, "No message ready or socket closed")

proc recv*(s: Socket, timeoutMs: int = -1): seq[byte] =
  result = s.recvMsg(timeoutMs).body

proc recvStr*(s: Socket, timeoutMs: int = -1): string =
  let body = s.recv(timeoutMs)
  result = newString(body.len)
  for i in 0 ..< body.len:
    result[i] = char(body[i])

proc poll*(s: Socket, timeoutMs: int = 0): bool =
  if s.closed:
    return false
  when not defined(js):
    asyncdispatch.poll(timeoutMs)
  return s.recvQueue.len > 0

proc hasRecv*(s: Socket): bool =
  s.recvQueue.len > 0

proc subscribe*(s: Socket, topic: openArray[byte]) =
  if s.engineKind == engSub0:
    s.sub0Eng.subscribe(topic)
  else:
    raise newNngError(NNG_ENOTSUP, "subscribe() is only supported on SUB0 sockets")

proc subscribe*(s: Socket, topic: string) =
  var bytes = newSeq[byte](topic.len)
  for i in 0 ..< topic.len:
    bytes[i] = byte(topic[i])
  s.subscribe(bytes)

proc unsubscribe*(s: Socket, topic: openArray[byte]) =
  if s.engineKind == engSub0:
    s.sub0Eng.unsubscribe(topic)
  else:
    raise newNngError(NNG_ENOTSUP, "unsubscribe() is only supported on SUB0 sockets")

proc unsubscribe*(s: Socket, topic: string) =
  var bytes = newSeq[byte](topic.len)
  for i in 0 ..< topic.len:
    bytes[i] = byte(topic[i])
  s.unsubscribe(bytes)

proc close*(s: Socket) =
  if not s.closed:
    s.closed = true
    for ctx in s.contexts.values:
      ctx.close()
    s.contexts.clear()
    var pipesToClose: seq[Pipe] = @[]
    for p in s.pipes.values:
      pipesToClose.add(p)
    s.pipes.clear()
    for p in pipesToClose:
      p.close()

# Raw Mode Device Forwarding
proc device*(s1, s2: Socket): Future[void] {.async.} =
  if not s1.raw or not s2.raw:
    raise newNngError(NNG_EINVAL, "device requires raw mode sockets")

  while not s1.closed and not s2.closed:
    # Forward s1 -> s2 if message ready
    if s1.hasRecv():
      let msg = s1.recvMsg(0)
      s2.send(msg.body)
    # Forward s2 -> s1 if message ready
    if s2.hasRecv():
      let msg = s2.recvMsg(0)
      s1.send(msg.body)
    await sleepAsync(1)

# Convenience Constructors
proc openPair0*(raw: bool = false): Socket =
  newSocket(spPair0, raw)

proc openPair1*(polyamorous: bool = false, raw: bool = false): Socket =
  result = newSocket(spPair1, raw)
  result.pair1Eng.polyamorous = polyamorous

proc openPub0*(raw: bool = false): Socket =
  newSocket(spPub0, raw)

proc openSub0*(raw: bool = false): Socket =
  newSocket(spSub0, raw)

proc openReq0*(raw: bool = false): Socket =
  newSocket(spReq0, raw)

proc openRep0*(raw: bool = false): Socket =
  newSocket(spRep0, raw)

proc openPush0*(raw: bool = false): Socket =
  newSocket(spPush0, raw)

proc openPull0*(raw: bool = false): Socket =
  newSocket(spPull0, raw)

proc openSurveyor0*(raw: bool = false): Socket =
  newSocket(spSurveyor0, raw)

proc openRespondent0*(raw: bool = false): Socket =
  newSocket(spRespondent0, raw)

proc openBus0*(raw: bool = false): Socket =
  newSocket(spBus0, raw)
