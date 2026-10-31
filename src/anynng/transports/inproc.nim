# inproc.nim - Cross-Target In-Process Memory Pipe Transport
# Licensed under MIT License.

import std/tables
import ../core/types
import ../core/options

type
  InprocEndpoint* = ref object
    pipeId*: PipeId
    peer*: InprocEndpoint
    closed*: bool
    pipe*: Pipe
    recvBuf*: seq[seq[byte]]
    onMessage*: proc(rawFrame: seq[byte]) {.closure.}
    onClose*: proc() {.closure.}

  InprocListener* = ref object
    url*: string
    proto*: SpProto
    onConnect*: proc(ep: InprocEndpoint) {.closure.}
    closed*: bool

var
  gInprocRuntimeListeners: Table[string, InprocListener] =
    initTable[string, InprocListener]()
  gNextPipeId: PipeId = 1'u64

proc allocPipeId(): PipeId =
  result = gNextPipeId
  inc gNextPipeId

proc registerInprocListener*(
    url: string, proto: SpProto, onConnect: proc(ep: InprocEndpoint) {.closure.}
): InprocListener =
  if url in gInprocRuntimeListeners and not gInprocRuntimeListeners[url].closed:
    raise newNngError(NNG_EADDRINUSE, "Address in use: " & url)
  result = InprocListener(url: url, proto: proto, onConnect: onConnect, closed: false)
  gInprocRuntimeListeners[url] = result

proc unregisterInprocListener*(url: string) =
  gInprocRuntimeListeners.del(url)

proc connectInproc*(
    url: string,
    dialerProto: SpProto,
    onMessage: proc(rawFrame: seq[byte]) {.closure.},
    onClose: proc() {.closure.},
): InprocEndpoint =
  if url notin gInprocRuntimeListeners:
    raise newNngError(NNG_ECONNREFUSED, "No inproc listener registered at: " & url)

  let listener = gInprocRuntimeListeners[url]
  if listener.closed:
    raise newNngError(NNG_ECONNREFUSED, "Inproc listener at " & url & " is closed")

  # Verify protocol match
  if listener.proto != peerProto(dialerProto):
    raise newNngError(
      NNG_ENOTSUP,
      "Inproc protocol mismatch: dialer is " & protoName(dialerProto) & ", listener is " &
        protoName(listener.proto),
    )

  let dialerEp = InprocEndpoint(
    pipeId: allocPipeId(), closed: false, onMessage: onMessage, onClose: onClose
  )

  let listenerEp = InprocEndpoint(pipeId: allocPipeId(), closed: false)

  dialerEp.peer = listenerEp
  listenerEp.peer = dialerEp

  # Notify listener
  listener.onConnect(listenerEp)
  return dialerEp

proc send*(ep: InprocEndpoint, rawFrame: openArray[byte]) =
  if ep.closed or ep.peer == nil or ep.peer.closed:
    raise newNngError(NNG_ECLOSED, "Inproc pipe is closed")
  if ep.peer.onMessage != nil:
    ep.peer.onMessage(@rawFrame)
  else:
    ep.peer.recvBuf.add(@rawFrame)

proc close*(ep: InprocEndpoint) =
  if not ep.closed:
    ep.closed = true
    if ep.onClose != nil:
      ep.onClose()
    if ep.peer != nil and not ep.peer.closed:
      ep.peer.closed = true
      if ep.peer.onClose != nil:
        ep.peer.onClose()

proc close*(listener: InprocListener) =
  if not listener.closed:
    listener.closed = true
    gInprocRuntimeListeners.del(listener.url)
