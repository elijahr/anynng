# url.nim - Target-agnostic SP URI parser
# Licensed under MIT License.

import std/strutils

type
  TransportKind* = enum
    tranInproc
    tranIpc
    tranTcp
    tranUdp
    tranWs

  ParsedUrl* = object
    raw*: string
    kind*: TransportKind
    scheme*: string
    host*: string
    port*: int
    path*: string
    isSecure*: bool

proc parseSpUrl*(rawUrl: string): ParsedUrl =
  result.raw = rawUrl
  let colonIdx = rawUrl.find("://")
  if colonIdx < 0:
    raise newException(ValueError, "Invalid SP URL, missing '://': " & rawUrl)

  result.scheme = rawUrl[0 ..< colonIdx].toLowerAscii()
  let rest = rawUrl[colonIdx + 3 .. ^1]

  case result.scheme
  of "inproc":
    result.kind = tranInproc
    result.path = rest
  of "ipc":
    result.kind = tranIpc
    result.path = rest
  of "tcp", "tcp4", "tcp6":
    result.kind = tranTcp
    let slashIdx = rest.find('/')
    let hostPort =
      if slashIdx >= 0:
        rest[0 ..< slashIdx]
      else:
        rest
    result.path =
      if slashIdx >= 0:
        rest[slashIdx .. ^1]
      else:
        ""
    let lastColon = hostPort.rfind(':')
    if lastColon >= 0:
      result.host = hostPort[0 ..< lastColon]
      result.port = parseInt(hostPort[lastColon + 1 .. ^1])
    else:
      result.host = hostPort
      result.port = 0
  of "udp", "udp4", "udp6":
    result.kind = tranUdp
    let lastColon = rest.rfind(':')
    if lastColon >= 0:
      result.host = rest[0 ..< lastColon]
      result.port = parseInt(rest[lastColon + 1 .. ^1])
    else:
      result.host = rest
      result.port = 0
  of "ws", "wss":
    result.kind = tranWs
    result.isSecure = (result.scheme == "wss")
    let slashIdx = rest.find('/')
    let hostPort =
      if slashIdx >= 0:
        rest[0 ..< slashIdx]
      else:
        rest
    result.path =
      if slashIdx >= 0:
        rest[slashIdx .. ^1]
      else:
        "/"
    let lastColon = hostPort.rfind(':')
    if lastColon >= 0:
      result.host = hostPort[0 ..< lastColon]
      result.port = parseInt(hostPort[lastColon + 1 .. ^1])
    else:
      result.host = hostPort
      result.port = if result.isSecure: 443 else: 80
  else:
    raise newException(ValueError, "Unsupported transport scheme: " & result.scheme)
