# c_api.nim - Full C-ABI Export Layer for anynng
# Implements <nng/nng.h> C ABI enabling unmodified upstream nanomsg/nng v1.12.4 test suites
# Licensed under MIT License.

when not defined(js):
  import
    std/[tables, sets, monotimes, os, random, asyncdispatch, posix, deques, strutils]
  import ../anynng
  import ./core/[types, options, tls]

  include ./c_api/c_types
  include ./c_api/c_lifecycle
  include ./c_api/c_log
  include ./c_api/c_udp
  include ./c_api/c_socket
  include ./c_api/c_endpoint
  include ./c_api/c_url
  include ./c_api/c_msg
  include ./c_api/c_socket_io
  include ./c_api/c_ctx
  include ./c_api/c_aio
  include ./c_api/c_pipe
  include ./c_api/c_options
  include ./c_api/c_device
  include ./c_api/c_tls
  include ./c_api/c_sync
  include ./c_api/c_stats
