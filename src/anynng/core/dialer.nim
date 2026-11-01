# anynng - Pure-Nim multi-target (C, JS, WASM) and multi-transport NNG / Scalability Protocols
# Licensed under MIT License.

import std/tables
import ./[types, options, tls, url, runtime_types, pipe]
import ../transports/inproc

when not defined(js):
  import std/asyncdispatch
  import ../transports/[native_tcp, native_ipc, native_udp, native_ws, native_tls]

proc newDialer*(s: Socket, urlStr: string): Dialer =
  new(result)
  result.socket = s
  result.url = urlStr
  result.options = newOptionStore(entDialer)
  result.closed = false
  result.started = false
  result.tlsCfg = nil

proc setOpt*(d: Dialer, name: string, val: int64) =
  d.options.setMs(name, val)

proc setOpt*(d: Dialer, name: string, val: bool) =
  d.options.setBool(name, val)

proc setTls*(d: Dialer, cfg: TlsConfig) =
  d.tlsCfg = cfg

proc getTls*(d: Dialer): TlsConfig =
  d.tlsCfg

proc getUrl*(d: Dialer): string =
  d.url

proc setTlsConfig*(d: Dialer, val: TlsConfig) {.inline.} =
  d.setTls(val)

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
    let ep = connectInproc(parsed.raw, d.socket.proto, nil, nil)
    if not ep.closed:
      d.socket.attachEndpoint(ep, dialerPtr = cast[pointer](d))
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

proc dial*(s: Socket, urlStr: string): Dialer {.discardable.} =
  result = s.newDialer(urlStr)
  result.start()
