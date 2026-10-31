# anynng - Pure-Nim multi-target (C, JS, WASM) and multi-transport NNG / Scalability Protocols
# Licensed under MIT License.

import
  anynng/core/[
    types, options, tls, endian, url, runtime_types, socket, pipe, context, dialer,
    listener, device,
  ]
import anynng/core/framing/[stream_framing, ws_framing, udp_framing]
import anynng/core/protocols/[pair0, pair1, pubsub0, reqrep0, pipeline0, survey0, bus0]
import anynng/transports/inproc

when not defined(js):
  import anynng/transports/[native_tcp, native_ipc, native_udp, native_ws, native_tls]
  export native_tcp, native_ipc, native_udp, native_ws, native_tls
else:
  import anynng/transports/js_ws
  export js_ws

export
  types, options, tls, endian, url, runtime_types, socket, pipe, context, dialer,
  listener, device
export
  stream_framing, ws_framing, udp_framing, pair0, pair1, pubsub0, reqrep0, pipeline0,
  survey0, bus0, inproc

const cApiModule* = "src/anynng/c_api.nim"
