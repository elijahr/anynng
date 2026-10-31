proc nng_msg_alloc*(
    msgp: ptr ptr NngMsgObj, sz: csize_t
): cint {.exportc, dynlib, cdecl.} =
  if msgp == nil:
    return cint(NNG_EINVAL)
  let m = createShared(NngMsgObj)
  m.header = @[]
  m.body = newSeq[byte](sz)
  m.pipeId = 0
  msgp[] = m
  return 0

proc nng_msg_free*(msg: ptr NngMsgObj) {.exportc, dynlib, cdecl.} =
  if msg != nil:
    msg.header = @[]
    msg.body = @[]
    freeShared(msg)

proc nng_msg_body*(msg: ptr NngMsgObj): pointer {.exportc, dynlib, cdecl.} =
  if msg == nil or msg.body.len == 0:
    return nil
  return addr msg.body[0]

proc nng_msg_len*(msg: ptr NngMsgObj): csize_t {.exportc, dynlib, cdecl.} =
  if msg == nil:
    return 0
  return csize_t(msg.body.len)

proc nng_msg_append*(
    msg: ptr NngMsgObj, data: pointer, sz: csize_t
): cint {.exportc, dynlib, cdecl.} =
  if msg == nil:
    return cint(NNG_EINVAL)
  if sz > 0 and data != nil:
    let oldLen = msg.body.len
    msg.body.setLen(oldLen + int(sz))
    copyMem(addr msg.body[oldLen], data, int(sz))
  return 0

proc nng_msg_insert*(
    msg: ptr NngMsgObj, data: pointer, sz: csize_t
): cint {.exportc, dynlib, cdecl.} =
  if msg == nil:
    return cint(NNG_EINVAL)
  if sz > 0 and data != nil:
    var newBody = newSeq[byte](msg.body.len + int(sz))
    copyMem(addr newBody[0], data, int(sz))
    if msg.body.len > 0:
      copyMem(addr newBody[int(sz)], addr msg.body[0], msg.body.len)
    msg.body = newBody
  return 0

proc nng_msg_trim*(msg: ptr NngMsgObj, sz: csize_t): cint {.exportc, dynlib, cdecl.} =
  if msg == nil or int(sz) > msg.body.len:
    return cint(NNG_EINVAL)
  let s = int(sz)
  if s > 0:
    msg.body = msg.body[s .. ^1]
  return 0

proc nng_msg_chop*(msg: ptr NngMsgObj, sz: csize_t): cint {.exportc, dynlib, cdecl.} =
  if msg == nil or int(sz) > msg.body.len:
    return cint(NNG_EINVAL)
  msg.body.setLen(msg.body.len - int(sz))
  return 0

proc nng_msg_clear*(msg: ptr NngMsgObj) {.exportc, dynlib, cdecl.} =
  if msg != nil:
    msg.body.setLen(0)

proc nng_msg_header*(msg: ptr NngMsgObj): pointer {.exportc, dynlib, cdecl.} =
  if msg == nil or msg.header.len == 0:
    return nil
  return addr msg.header[0]

proc nng_msg_header_len*(msg: ptr NngMsgObj): csize_t {.exportc, dynlib, cdecl.} =
  if msg == nil:
    return 0
  return csize_t(msg.header.len)

proc nng_msg_header_append*(
    msg: ptr NngMsgObj, data: pointer, sz: csize_t
): cint {.exportc, dynlib, cdecl.} =
  if msg == nil:
    return cint(NNG_EINVAL)
  if sz > 0 and data != nil:
    let oldLen = msg.header.len
    msg.header.setLen(oldLen + int(sz))
    copyMem(addr msg.header[oldLen], data, int(sz))
  return 0

proc nng_msg_header_insert*(
    msg: ptr NngMsgObj, data: pointer, sz: csize_t
): cint {.exportc, dynlib, cdecl.} =
  if msg == nil:
    return cint(NNG_EINVAL)
  if sz > 0 and data != nil:
    var newHdr = newSeq[byte](msg.header.len + int(sz))
    copyMem(addr newHdr[0], data, int(sz))
    if msg.header.len > 0:
      copyMem(addr newHdr[int(sz)], addr msg.header[0], msg.header.len)
    msg.header = newHdr
  return 0

proc nng_msg_header_trim*(
    msg: ptr NngMsgObj, sz: csize_t
): cint {.exportc, dynlib, cdecl.} =
  if msg == nil or int(sz) > msg.header.len:
    return cint(NNG_EINVAL)
  let s = int(sz)
  if s > 0:
    msg.header = msg.header[s .. ^1]
  return 0

proc nng_msg_header_chop*(
    msg: ptr NngMsgObj, sz: csize_t
): cint {.exportc, dynlib, cdecl.} =
  if msg == nil or int(sz) > msg.header.len:
    return cint(NNG_EINVAL)
  msg.header.setLen(msg.header.len - int(sz))
  return 0

proc nng_msg_header_clear*(msg: ptr NngMsgObj) {.exportc, dynlib, cdecl.} =
  if msg != nil:
    msg.header.setLen(0)

proc nng_msg_dup*(
    dup: ptr ptr NngMsgObj, src: ptr NngMsgObj
): cint {.exportc, dynlib, cdecl.} =
  if dup == nil or src == nil:
    return cint(NNG_EINVAL)
  let m = createShared(NngMsgObj)
  m.header = src.header
  m.body = src.body
  m.pipeId = src.pipeId
  dup[] = m
  return 0

proc nng_msg_append_u32*(
    msg: ptr NngMsgObj, val: uint32
): cint {.exportc, dynlib, cdecl.} =
  if msg == nil:
    return cint(NNG_EINVAL)
  var beVal = [
    byte(val shr 24 and 0xFF),
    byte(val shr 16 and 0xFF),
    byte(val shr 8 and 0xFF),
    byte(val and 0xFF),
  ]
  return nng_msg_append(msg, addr beVal[0], 4)

proc nng_msg_trim_u32*(
    msg: ptr NngMsgObj, valp: ptr uint32
): cint {.exportc, dynlib, cdecl.} =
  if msg == nil or msg.body.len < 4:
    return cint(NNG_EINVAL)
  if valp != nil:
    valp[] =
      (uint32(msg.body[0]) shl 24) or (uint32(msg.body[1]) shl 16) or
      (uint32(msg.body[2]) shl 8) or uint32(msg.body[3])
  return nng_msg_trim(msg, 4)

proc nng_msg_header_append_u32*(
    msg: ptr NngMsgObj, val: uint32
): cint {.exportc, dynlib, cdecl.} =
  if msg == nil:
    return cint(NNG_EINVAL)
  var beVal = [
    byte(val shr 24 and 0xFF),
    byte(val shr 16 and 0xFF),
    byte(val shr 8 and 0xFF),
    byte(val and 0xFF),
  ]
  return nng_msg_header_append(msg, addr beVal[0], 4)

proc nng_msg_header_chop_u32*(
    msg: ptr NngMsgObj, valp: ptr uint32
): cint {.exportc, dynlib, cdecl.} =
  if msg == nil or msg.header.len < 4:
    return cint(NNG_EINVAL)
  let idx = msg.header.len - 4
  if valp != nil:
    valp[] =
      (uint32(msg.header[idx]) shl 24) or (uint32(msg.header[idx + 1]) shl 16) or
      (uint32(msg.header[idx + 2]) shl 8) or uint32(msg.header[idx + 3])
  return nng_msg_header_chop(msg, 4)

proc nng_msg_header_trim_u32*(
    msg: ptr NngMsgObj, valp: ptr uint32
): cint {.exportc, dynlib, cdecl.} =
  if msg == nil or msg.header.len < 4:
    return cint(NNG_EINVAL)
  if valp != nil:
    valp[] =
      (uint32(msg.header[0]) shl 24) or (uint32(msg.header[0 + 1]) shl 16) or
      (uint32(msg.header[0 + 2]) shl 8) or uint32(msg.header[0 + 3])
  return nng_msg_header_trim(msg, 4)

proc nng_msg_get_pipe*(msg: ptr NngMsgObj): NngPipeObj {.exportc, dynlib, cdecl.} =
  if msg == nil:
    return NngPipeObj(id: 0)
  return NngPipeObj(id: uint32(msg.pipeId))

proc nng_msg_set_pipe*(msg: ptr NngMsgObj, p: NngPipeObj) {.exportc, dynlib, cdecl.} =
  if msg != nil:
    msg.pipeId = PipeId(p.id)
