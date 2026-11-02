proc nng_msg_alloc*(
    msgp: ptr ptr NngMsgObj, sz: csize_t
): cint {.exportc, dynlib, cdecl.} =
  if msgp == nil:
    return cint(NNG_EINVAL)
  let m = createShared(NngMsgImpl)
  m.header = @[]
  m.body =
    if sz > 0:
      newSeq[byte](sz)
    else:
      @[]
  m.pipeId = 0
  msgp[] = cast[ptr NngMsgObj](m)
  return 0

proc nng_msg_free*(msg: ptr NngMsgObj) {.exportc, dynlib, cdecl.} =
  if msg != nil:
    let m = msg.impl
    m.header = @[]
    m.body = @[]
    freeShared(m)

proc nng_msg_body*(msg: ptr NngMsgObj): pointer {.exportc, dynlib, cdecl.} =
  if msg == nil:
    return nil
  let m = msg.impl
  if m.body.len == 0:
    return nil
  return addr m.body[0]

proc nng_msg_len*(msg: ptr NngMsgObj): csize_t {.exportc, dynlib, cdecl.} =
  if msg == nil:
    return 0
  let m = msg.impl
  return csize_t(m.body.len)

proc nng_msg_append*(
    msg: ptr NngMsgObj, data: pointer, sz: csize_t
): cint {.exportc, dynlib, cdecl.} =
  if msg == nil:
    return cint(NNG_EINVAL)
  if sz > 0 and data != nil:
    let m = msg.impl
    let oldLen = m.body.len
    m.body.setLen(oldLen + int(sz))
    copyMem(addr m.body[oldLen], data, int(sz))
  return 0

proc nng_msg_insert*(
    msg: ptr NngMsgObj, data: pointer, sz: csize_t
): cint {.exportc, dynlib, cdecl.} =
  if msg == nil:
    return cint(NNG_EINVAL)
  if sz > 0 and data != nil:
    let m = msg.impl
    var newBody = newSeq[byte](m.body.len + int(sz))
    copyMem(addr newBody[0], data, int(sz))
    if m.body.len > 0:
      copyMem(addr newBody[int(sz)], addr m.body[0], m.body.len)
    m.body = newBody
  return 0

proc nng_msg_trim*(msg: ptr NngMsgObj, sz: csize_t): cint {.exportc, dynlib, cdecl.} =
  if msg == nil:
    return cint(NNG_EINVAL)
  let m = msg.impl
  if int(sz) > m.body.len:
    return cint(NNG_EINVAL)
  let s = int(sz)
  if s > 0:
    m.body = m.body[s .. ^1]
  return 0

proc nng_msg_chop*(msg: ptr NngMsgObj, sz: csize_t): cint {.exportc, dynlib, cdecl.} =
  if msg == nil:
    return cint(NNG_EINVAL)
  let m = msg.impl
  if int(sz) > m.body.len:
    return cint(NNG_EINVAL)
  m.body.setLen(m.body.len - int(sz))
  return 0

proc nng_msg_clear*(msg: ptr NngMsgObj) {.exportc, dynlib, cdecl.} =
  if msg != nil:
    msg.impl.body.setLen(0)

proc nng_msg_header*(msg: ptr NngMsgObj): pointer {.exportc, dynlib, cdecl.} =
  if msg == nil:
    return nil
  let m = msg.impl
  if m.header.len == 0:
    return nil
  return addr m.header[0]

proc nng_msg_header_len*(msg: ptr NngMsgObj): csize_t {.exportc, dynlib, cdecl.} =
  if msg == nil:
    return 0
  let m = msg.impl
  return csize_t(m.header.len)

proc nng_msg_header_append*(
    msg: ptr NngMsgObj, data: pointer, sz: csize_t
): cint {.exportc, dynlib, cdecl.} =
  if msg == nil:
    return cint(NNG_EINVAL)
  if sz > 0 and data != nil:
    let m = msg.impl
    let oldLen = m.header.len
    m.header.setLen(oldLen + int(sz))
    copyMem(addr m.header[oldLen], data, int(sz))
  return 0

proc nng_msg_header_insert*(
    msg: ptr NngMsgObj, data: pointer, sz: csize_t
): cint {.exportc, dynlib, cdecl.} =
  if msg == nil:
    return cint(NNG_EINVAL)
  if sz > 0 and data != nil:
    let m = msg.impl
    var newHdr = newSeq[byte](m.header.len + int(sz))
    copyMem(addr newHdr[0], data, int(sz))
    if m.header.len > 0:
      copyMem(addr newHdr[int(sz)], addr m.header[0], m.header.len)
    m.header = newHdr
  return 0

proc nng_msg_header_trim*(
    msg: ptr NngMsgObj, sz: csize_t
): cint {.exportc, dynlib, cdecl.} =
  if msg == nil:
    return cint(NNG_EINVAL)
  let m = msg.impl
  if int(sz) > m.header.len:
    return cint(NNG_EINVAL)
  let s = int(sz)
  if s > 0:
    m.header = m.header[s .. ^1]
  return 0

proc nng_msg_header_chop*(
    msg: ptr NngMsgObj, sz: csize_t
): cint {.exportc, dynlib, cdecl.} =
  if msg == nil:
    return cint(NNG_EINVAL)
  let m = msg.impl
  if int(sz) > m.header.len:
    return cint(NNG_EINVAL)
  m.header.setLen(m.header.len - int(sz))
  return 0

proc nng_msg_header_clear*(msg: ptr NngMsgObj) {.exportc, dynlib, cdecl.} =
  if msg != nil:
    msg.impl.header.setLen(0)

proc nng_msg_dup*(
    dup: ptr ptr NngMsgObj, src: ptr NngMsgObj
): cint {.exportc, dynlib, cdecl.} =
  if dup == nil or src == nil:
    return cint(NNG_EINVAL)
  let m = createShared(NngMsgImpl)
  let s = src.impl
  m.header = s.header
  m.body = s.body
  m.pipeId = s.pipeId
  dup[] = cast[ptr NngMsgObj](m)
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
  if msg == nil:
    return cint(NNG_EINVAL)
  let m = msg.impl
  if m.body.len < 4:
    return cint(NNG_EINVAL)
  if valp != nil:
    valp[] =
      (uint32(m.body[0]) shl 24) or (uint32(m.body[1]) shl 16) or
      (uint32(m.body[2]) shl 8) or uint32(m.body[3])
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
  if msg == nil:
    return cint(NNG_EINVAL)
  let m = msg.impl
  if m.header.len < 4:
    return cint(NNG_EINVAL)
  let idx = m.header.len - 4
  if valp != nil:
    valp[] =
      (uint32(m.header[idx]) shl 24) or (uint32(m.header[idx + 1]) shl 16) or
      (uint32(m.header[idx + 2]) shl 8) or uint32(m.header[idx + 3])
  return nng_msg_header_chop(msg, 4)

proc nng_msg_header_trim_u32*(
    msg: ptr NngMsgObj, valp: ptr uint32
): cint {.exportc, dynlib, cdecl.} =
  if msg == nil:
    return cint(NNG_EINVAL)
  let m = msg.impl
  if m.header.len < 4:
    return cint(NNG_EINVAL)
  if valp != nil:
    valp[] =
      (uint32(m.header[0]) shl 24) or (uint32(m.header[0 + 1]) shl 16) or
      (uint32(m.header[0 + 2]) shl 8) or uint32(m.header[0 + 3])
  return nng_msg_header_trim(msg, 4)

proc nng_msg_get_pipe*(msg: ptr NngMsgObj): NngPipeObj {.exportc, dynlib, cdecl.} =
  if msg == nil:
    return NngPipeObj(id: 0)
  return NngPipeObj(id: uint32(msg.impl.pipeId))

proc nng_msg_set_pipe*(msg: ptr NngMsgObj, p: NngPipeObj) {.exportc, dynlib, cdecl.} =
  if msg != nil:
    msg.impl.pipeId = PipeId(p.id)
