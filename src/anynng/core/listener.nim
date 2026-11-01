# anynng - Pure-Nim multi-target (C, JS, WASM) and multi-transport NNG / Scalability Protocols
# Licensed under MIT License.

import std/tables
import ./[types, options, tls, url, runtime_types, pipe]
import ../transports/inproc

when not defined(js):
  import std/asyncdispatch
  import ../transports/[native_tcp, native_ipc, native_udp, native_ws, native_tls]

proc newListener*(s: Socket, urlStr: string): Listener =
  new(result)
  result.socket = s
  result.url = urlStr
  result.options = newOptionStore(entListener)
  result.closed = false
  result.started = false
  result.boundPort = 0
  result.tlsCfg = nil

proc setOpt*(l: Listener, name: string, val: bool) =
  l.options.setBool(name, val)

proc setTls*(l: Listener, cfg: TlsConfig) =
  l.tlsCfg = cfg

proc getTls*(l: Listener): TlsConfig =
  l.tlsCfg

proc getUrl*(l: Listener): string =
  l.url

proc setTlsConfig*(l: Listener, val: TlsConfig) {.inline.} =
  l.setTls(val)

proc getOpt*(l: Listener, name: string): OptionValue =
  if name notin l.options.values:
    raise newNngError(NNG_ENOTSUP, "Option " & name & " not supported on listener")
  l.options.values[name]

proc getInt*(l: Listener, name: string): int =
  l.options.getInt(name)

proc getString*(l: Listener, name: string): string =
  l.options.getString(name)

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
        l.socket.attachEndpoint(ep, listenerPtr = cast[pointer](l)),
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
      l.options.setIntInternal(NNG_OPT_BOUND_PORT, l.boundPort)
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
      l.options.setIntInternal(NNG_OPT_BOUND_PORT, l.boundPort)
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
      l.options.setIntInternal(NNG_OPT_BOUND_PORT, l.boundPort)
      l.closeImpl = proc() =
        wsListener.close()
    else:
      raise newNngError(NNG_ENOTSUP, "WS listener not supported on JS")

proc close*(l: Listener) =
  if not l.closed:
    l.closed = true
    if l.closeImpl != nil:
      l.closeImpl()

proc listen*(s: Socket, urlStr: string): Listener =
  result = s.newListener(urlStr)
  result.start()
