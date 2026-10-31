# options.nim - Strongly-typed NNG Option Registry and Error Taxonomy
# Licensed under MIT License.

import std/tables

# Exact NNG Error Code Constants matching <nng/nng.h>
const
  NNG_OK* = 0
  NNG_EINTR* = 1
  NNG_ENOMEM* = 2
  NNG_EINVAL* = 3
  NNG_EBUSY* = 4
  NNG_ETIMEDOUT* = 5
  NNG_ECONNREFUSED* = 6
  NNG_ECLOSED* = 7
  NNG_EAGAIN* = 8
  NNG_ENOTSUP* = 9
  NNG_EADDRINUSE* = 10
  NNG_ESTATE* = 11
  NNG_ENOENT* = 12
  NNG_EPROTO* = 13
  NNG_EUNREACHABLE* = 14
  NNG_EADDRINVAL* = 15
  NNG_EPERM* = 16
  NNG_EMSGSIZE* = 17
  NNG_ECONNABORTED* = 18
  NNG_ECONNRESET* = 19
  NNG_ECANCELED* = 20
  NNG_ENOFILES* = 21
  NNG_ENOSPC* = 22
  NNG_EEXIST* = 23
  NNG_EREADONLY* = 24
  NNG_EWRITEONLY* = 25
  NNG_ECRYPTO* = 26
  NNG_EPEERAUTH* = 27
  NNG_ENOARG* = 28
  NNG_EAMBIGUOUS* = 29
  NNG_EBADTYPE* = 30
  NNG_ECONNSHUT* = 31
  NNG_EINTERNAL* = 1000

func nngStrError*(err: int): string =
  case err
  of NNG_OK:
    "Success"
  of NNG_EINTR:
    "Interrupted"
  of NNG_ENOMEM:
    "Out of memory"
  of NNG_EINVAL:
    "Invalid argument"
  of NNG_EBUSY:
    "Resource busy"
  of NNG_ETIMEDOUT:
    "Timed out"
  of NNG_ECONNREFUSED:
    "Connection refused"
  of NNG_ECLOSED:
    "Object closed"
  of NNG_EAGAIN:
    "Try again"
  of NNG_ENOTSUP:
    "Not supported"
  of NNG_EADDRINUSE:
    "Address in use"
  of NNG_ESTATE:
    "Incorrect state"
  of NNG_ENOENT:
    "Entry not found"
  of NNG_EPROTO:
    "Protocol error"
  of NNG_EUNREACHABLE:
    "Destination unreachable"
  of NNG_EADDRINVAL:
    "Address invalid"
  of NNG_EPERM:
    "Permission denied"
  of NNG_EMSGSIZE:
    "Message size exceeded"
  of NNG_ECONNABORTED:
    "Connection aborted"
  of NNG_ECONNRESET:
    "Connection reset"
  of NNG_ECANCELED:
    "Operation canceled"
  of NNG_ENOFILES:
    "Out of files"
  of NNG_ENOSPC:
    "Out of space"
  of NNG_EEXIST:
    "Resource already exists"
  of NNG_EREADONLY:
    "Read-only resource"
  of NNG_EWRITEONLY:
    "Write-only resource"
  of NNG_ECRYPTO:
    "Cryptographic error"
  of NNG_EPEERAUTH:
    "Peer authentication failed"
  of NNG_ENOARG:
    "Argument required"
  of NNG_EAMBIGUOUS:
    "Ambiguous argument"
  of NNG_EBADTYPE:
    "Incorrect type"
  of NNG_ECONNSHUT:
    "Connection shut down"
  of NNG_EINTERNAL:
    "Internal error"
  else:
    "Unknown NNG error " & $err

type NngError* = object of ValueError
  errCode*: int

proc newNngError*(code: int, detail: string = ""): ref NngError =
  new(result)
  result.errCode = code
  result.msg =
    if detail.len > 0:
      nngStrError(code) & ": " & detail
    else:
      nngStrError(code)

# Universal and Protocol-Specific Option Identifiers
const
  NNG_OPT_SOCKNAME* = "socket-name"
  NNG_OPT_RAW* = "raw"
  NNG_OPT_PROTO* = "protocol"
  NNG_OPT_PROTONAME* = "protocol-name"
  NNG_OPT_PEER* = "peer"
  NNG_OPT_PEERNAME* = "peer-name"
  NNG_OPT_RECVBUF* = "recv-buffer"
  NNG_OPT_SENDBUF* = "send-buffer"
  NNG_OPT_RECVTIMEO* = "recv-timeout"
  NNG_OPT_SENDTIMEO* = "send-timeout"
  NNG_OPT_LOCADDR* = "local-address"
  NNG_OPT_REMADDR* = "remote-address"
  NNG_OPT_URL* = "url"
  NNG_OPT_MAXTTL* = "ttl-max"
  NNG_OPT_RECONNMINT* = "reconnect-time-min"
  NNG_OPT_RECONNMAXT* = "reconnect-time-max"
  NNG_OPT_TCP_BOUND_PORT* = "tcp:bound-port"
  NNG_OPT_TCP_NODELAY* = "tcp-nodelay"
  NNG_OPT_TCP_KEEPALIVE* = "tcp-keepalive"
  NNG_OPT_TLS_CONFIG* = "tls-config"
  NNG_OPT_TLS_AUTH_MODE* = "tls-authmode"
  NNG_OPT_TLS_CA_FILE* = "tls-ca-file"
  NNG_OPT_TLS_CERT_KEY_FILE* = "tls-cert-key-file"
  NNG_OPT_TLS_SERVER_NAME* = "tls-server-name"
  NNG_OPT_TLS_VERIFIED* = "tls-verified"
  NNG_OPT_TLS_PEER_CN* = "tls-peer-cn"
  NNG_OPT_TLS_PEER_ALT_NAMES* = "tls-peer-alt-names"
  NNG_OPT_PAIR1_POLY* = "pair1:polyamorous"
  NNG_OPT_SUB_SUBSCRIBE* = "sub:subscribe"
  NNG_OPT_SUB_UNSUBSCRIBE* = "sub:unsubscribe"
  NNG_OPT_SUB_PREFNEW* = "sub:prefnew"
  NNG_OPT_REQ_RESENDTIME* = "req:resend-time"
  NNG_OPT_REQ_RESENDTICK* = "req:resend-tick"
  NNG_OPT_SURVEYOR_SURVEYTIME* = "surveyor:survey-time"
  NNG_OPT_RECVMAXSZ* = "recv-size-max"

type
  OptionKind* = enum
    optKindBool
    optKindInt
    optKindDuration # ms as int64
    optKindSize # byte sizes (uint64)
    optKindString
    optKindSockAddr
    optKindPtr

  OptionAccess* = enum
    optRo # Read-only: set raises NNG_EREADONLY
    optWo # Write-only: get raises NNG_EWRITEONLY
    optRw # Read-write

  OptionTarget* = enum
    entSocket
    entDialer
    entListener
    entPipe
    entContext

  OptionTargets* = set[OptionTarget]

  OptionValue* = object
    case kind*: OptionKind
    of optKindBool: bVal*: bool
    of optKindInt: iVal*: int
    of optKindDuration: dVal*: int64
    of optKindSize: sVal*: uint64
    of optKindString: strVal*: string
    of optKindSockAddr: addrVal*: string
    of optKindPtr: pVal*: pointer

  OptionDescriptor* = object
    name*: string
    kind*: OptionKind
    access*: OptionAccess
    targets*: OptionTargets

  OptionStore* = ref object
    target*: OptionTarget
    values*: Table[string, OptionValue]

proc initOptionValue*(b: bool): OptionValue =
  OptionValue(kind: optKindBool, bVal: b)

proc initOptionValue*(i: int): OptionValue =
  OptionValue(kind: optKindInt, iVal: i)

proc initOptionValueDuration*(ms: int64): OptionValue =
  OptionValue(kind: optKindDuration, dVal: ms)

proc initOptionValueSize*(sz: uint64): OptionValue =
  OptionValue(kind: optKindSize, sVal: sz)

proc initOptionValue*(s: string): OptionValue =
  OptionValue(kind: optKindString, strVal: s)

proc initOptionValueSockAddr*(sa: string): OptionValue =
  OptionValue(kind: optKindSockAddr, addrVal: sa)

proc initOptionValuePtr*(p: pointer): OptionValue =
  OptionValue(kind: optKindPtr, pVal: p)

# Global Option Registry Descriptors
var gOptionDescriptors*: Table[string, OptionDescriptor]

proc registerOption(
    name: string, kind: OptionKind, access: OptionAccess, targets: OptionTargets
) =
  gOptionDescriptors[name] =
    OptionDescriptor(name: name, kind: kind, access: access, targets: targets)

proc initDefaultDescriptors() =
  if gOptionDescriptors.len > 0:
    return
  gOptionDescriptors = initTable[string, OptionDescriptor]()
  registerOption(NNG_OPT_SOCKNAME, optKindString, optRw, {entSocket})
  registerOption(NNG_OPT_RAW, optKindBool, optRo, {entSocket})
  registerOption(NNG_OPT_PROTO, optKindInt, optRo, {entSocket})
  registerOption(NNG_OPT_PROTONAME, optKindString, optRo, {entSocket})
  registerOption(NNG_OPT_PEER, optKindInt, optRo, {entSocket})
  registerOption(NNG_OPT_PEERNAME, optKindString, optRo, {entSocket})
  registerOption(NNG_OPT_RECVBUF, optKindInt, optRw, {entSocket, entContext})
  registerOption(NNG_OPT_SENDBUF, optKindInt, optRw, {entSocket, entContext})
  registerOption(NNG_OPT_RECVTIMEO, optKindDuration, optRw, {entSocket, entContext})
  registerOption(NNG_OPT_SENDTIMEO, optKindDuration, optRw, {entSocket, entContext})
  registerOption(NNG_OPT_LOCADDR, optKindSockAddr, optRo, {entListener, entPipe})
  registerOption(NNG_OPT_REMADDR, optKindSockAddr, optRo, {entPipe})
  registerOption(NNG_OPT_URL, optKindString, optRo, {entDialer, entListener, entPipe})
  registerOption(NNG_OPT_MAXTTL, optKindInt, optRw, {entSocket})
  registerOption(NNG_OPT_RECONNMINT, optKindDuration, optRw, {entSocket, entDialer})
  registerOption(NNG_OPT_RECONNMAXT, optKindDuration, optRw, {entSocket, entDialer})
  registerOption(NNG_OPT_TCP_BOUND_PORT, optKindInt, optRo, {entListener})
  registerOption(
    NNG_OPT_TCP_NODELAY, optKindBool, optRw, {entSocket, entDialer, entListener}
  )
  registerOption(
    NNG_OPT_TCP_KEEPALIVE, optKindBool, optRw, {entSocket, entDialer, entListener}
  )
  registerOption(NNG_OPT_TLS_CONFIG, optKindPtr, optRw, {entDialer, entListener})
  registerOption(NNG_OPT_TLS_AUTH_MODE, optKindInt, optRw, {entDialer, entListener})
  registerOption(NNG_OPT_TLS_CA_FILE, optKindString, optRw, {entDialer, entListener})
  registerOption(
    NNG_OPT_TLS_CERT_KEY_FILE, optKindString, optRw, {entDialer, entListener}
  )
  registerOption(NNG_OPT_TLS_SERVER_NAME, optKindString, optRw, {entDialer})
  registerOption(NNG_OPT_TLS_VERIFIED, optKindBool, optRo, {entPipe})
  registerOption(NNG_OPT_TLS_PEER_CN, optKindString, optRo, {entPipe})
  registerOption(NNG_OPT_TLS_PEER_ALT_NAMES, optKindString, optRo, {entPipe})
  registerOption(NNG_OPT_PAIR1_POLY, optKindBool, optRw, {entSocket})
  registerOption(NNG_OPT_SUB_SUBSCRIBE, optKindString, optWo, {entSocket, entContext})
  registerOption(NNG_OPT_SUB_UNSUBSCRIBE, optKindString, optWo, {entSocket, entContext})
  registerOption(NNG_OPT_SUB_PREFNEW, optKindBool, optRw, {entSocket, entContext})
  registerOption(
    NNG_OPT_REQ_RESENDTIME, optKindDuration, optRw, {entSocket, entContext}
  )
  registerOption(
    NNG_OPT_REQ_RESENDTICK, optKindDuration, optRw, {entSocket, entContext}
  )
  registerOption(
    NNG_OPT_SURVEYOR_SURVEYTIME, optKindDuration, optRw, {entSocket, entContext}
  )
  registerOption(
    NNG_OPT_RECVMAXSZ, optKindSize, optRw, {entSocket, entDialer, entListener}
  )

initDefaultDescriptors()

proc newOptionStore*(target: OptionTarget): OptionStore =
  new(result)
  result.target = target
  result.values = initTable[string, OptionValue]()
  # Set default values
  case target
  of entSocket, entContext:
    result.values[NNG_OPT_RECVBUF] = initOptionValue(128)
    result.values[NNG_OPT_SENDBUF] = initOptionValue(128)
    result.values[NNG_OPT_RECVTIMEO] = initOptionValueDuration(-1) # infinite
    result.values[NNG_OPT_SENDTIMEO] = initOptionValueDuration(-1) # infinite
    result.values[NNG_OPT_MAXTTL] = initOptionValue(8)
    result.values[NNG_OPT_RECONNMINT] = initOptionValueDuration(100)
    result.values[NNG_OPT_RECONNMAXT] = initOptionValueDuration(8000)
    result.values[NNG_OPT_TCP_NODELAY] = initOptionValue(true)
    result.values[NNG_OPT_REQ_RESENDTIME] = initOptionValueDuration(60000)
    result.values[NNG_OPT_SURVEYOR_SURVEYTIME] = initOptionValueDuration(1000)
    result.values[NNG_OPT_RECVMAXSZ] = initOptionValueSize(1024 * 1024'u64)
  of entDialer:
    result.values[NNG_OPT_RECONNMINT] = initOptionValueDuration(100)
    result.values[NNG_OPT_RECONNMAXT] = initOptionValueDuration(8000)
    result.values[NNG_OPT_TCP_NODELAY] = initOptionValue(true)
  of entListener:
    result.values[NNG_OPT_TCP_NODELAY] = initOptionValue(true)
    result.values[NNG_OPT_TCP_BOUND_PORT] = initOptionValue(0)
  of entPipe:
    result.values[NNG_OPT_TLS_VERIFIED] = initOptionValue(false)
    result.values[NNG_OPT_TLS_PEER_CN] = initOptionValue("")

proc validateAccess*(
    store: OptionStore, name: string, isSet: bool, expectedKind: OptionKind
) =
  if name notin gOptionDescriptors:
    raise newNngError(NNG_ENOTSUP, "Option " & name & " is not recognized")
  let desc = gOptionDescriptors[name]
  if store.target notin desc.targets:
    raise newNngError(NNG_ENOTSUP, "Option " & name & " not supported on target entity")
  if isSet:
    if desc.access == optRo:
      raise newNngError(NNG_EREADONLY, "Option " & name & " is read-only")
  else:
    if desc.access == optWo:
      raise newNngError(NNG_EWRITEONLY, "Option " & name & " is write-only")
  if desc.kind != expectedKind:
    raise newNngError(
      NNG_EBADTYPE,
      "Option " & name & " expected kind " & $desc.kind & ", got " & $expectedKind,
    )

proc setBoolInternal*(store: OptionStore, name: string, val: bool) =
  store.values[name] = initOptionValue(val)

proc setIntInternal*(store: OptionStore, name: string, val: int) =
  store.values[name] = initOptionValue(val)

proc setMsInternal*(store: OptionStore, name: string, val: int64) =
  store.values[name] = initOptionValueDuration(val)

proc setSizeInternal*(store: OptionStore, name: string, val: uint64) =
  store.values[name] = initOptionValueSize(val)

proc setStringInternal*(store: OptionStore, name: string, val: string) =
  store.values[name] = initOptionValue(val)

proc setSockAddrInternal*(store: OptionStore, name: string, val: string) =
  store.values[name] = initOptionValueSockAddr(val)

proc setBool*(store: OptionStore, name: string, val: bool) =
  store.validateAccess(name, isSet = true, optKindBool)
  store.values[name] = initOptionValue(val)

proc getBool*(store: OptionStore, name: string): bool =
  store.validateAccess(name, isSet = false, optKindBool)
  if name in store.values and store.values[name].kind == optKindBool:
    return store.values[name].bVal
  raise newNngError(NNG_ENOENT, "Option " & name & " not set")

proc setInt*(store: OptionStore, name: string, val: int) =
  store.validateAccess(name, isSet = true, optKindInt)
  if name == NNG_OPT_MAXTTL and (val < 1 or val > 255):
    raise newNngError(NNG_EINVAL, "TTL must be in range 1..255")
  if (name == NNG_OPT_RECVBUF or name == NNG_OPT_SENDBUF) and val < 0:
    raise newNngError(NNG_EINVAL, "Buffer size cannot be negative")
  store.values[name] = initOptionValue(val)

proc getInt*(store: OptionStore, name: string): int =
  store.validateAccess(name, isSet = false, optKindInt)
  if name in store.values and store.values[name].kind == optKindInt:
    return store.values[name].iVal
  raise newNngError(NNG_ENOENT, "Option " & name & " not set")

proc setMs*(store: OptionStore, name: string, val: int64) =
  store.validateAccess(name, isSet = true, optKindDuration)
  store.values[name] = initOptionValueDuration(val)

proc getMs*(store: OptionStore, name: string): int64 =
  store.validateAccess(name, isSet = false, optKindDuration)
  if name in store.values and store.values[name].kind == optKindDuration:
    return store.values[name].dVal
  raise newNngError(NNG_ENOENT, "Option " & name & " not set")

proc setSize*(store: OptionStore, name: string, val: uint64) =
  store.validateAccess(name, isSet = true, optKindSize)
  store.values[name] = initOptionValueSize(val)

proc getSize*(store: OptionStore, name: string): uint64 =
  store.validateAccess(name, isSet = false, optKindSize)
  if name in store.values and store.values[name].kind == optKindSize:
    return store.values[name].sVal
  raise newNngError(NNG_ENOENT, "Option " & name & " not set")

proc setString*(store: OptionStore, name: string, val: string) =
  store.validateAccess(name, isSet = true, optKindString)
  store.values[name] = initOptionValue(val)

proc getString*(store: OptionStore, name: string): string =
  store.validateAccess(name, isSet = false, optKindString)
  if name in store.values and store.values[name].kind == optKindString:
    return store.values[name].strVal
  raise newNngError(NNG_ENOENT, "Option " & name & " not set")

proc setPtr*(store: OptionStore, name: string, val: pointer) =
  store.validateAccess(name, isSet = true, optKindPtr)
  store.values[name] = initOptionValuePtr(val)

proc getPtr*(store: OptionStore, name: string): pointer =
  store.validateAccess(name, isSet = false, optKindPtr)
  if name in store.values and store.values[name].kind == optKindPtr:
    return store.values[name].pVal
  raise newNngError(NNG_ENOENT, "Option " & name & " not set")

proc setSockAddr*(store: OptionStore, name: string, val: string) =
  store.validateAccess(name, isSet = true, optKindSockAddr)
  store.values[name] = initOptionValueSockAddr(val)

proc getSockAddr*(store: OptionStore, name: string): string =
  store.validateAccess(name, isSet = false, optKindSockAddr)
  if name in store.values and store.values[name].kind == optKindSockAddr:
    return store.values[name].addrVal
  raise newNngError(NNG_ENOENT, "Option " & name & " not set")
