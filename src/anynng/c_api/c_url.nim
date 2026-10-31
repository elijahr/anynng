# ----------------------------------------------------------------------
# URL API (nng_url)
# ----------------------------------------------------------------------

proc newNngUrl(
    scheme, userinfo, hostname: string, port: uint32, path, query, frag: string
): ptr NngUrlObj =
  let totalLen =
    scheme.len + 1 + userinfo.len + 1 + hostname.len + 1 + path.len + 1 + query.len + 1 +
    frag.len + 1
  let buf = allocShared0(totalLen)
  let url = cast[ptr NngUrlObj](allocShared0(sizeof(NngUrlObj)))
  url.u_buffer = buf
  url.u_bufsz = csize_t(totalLen)
  var offset = 0

  template copyStr(s: string): cstring =
    let p = cast[cstring](cast[uint](buf) + uint(offset))
    if s.len > 0:
      copyMem(p, cstring(s), s.len)
    offset += s.len + 1
    p

  url.u_scheme = copyStr(scheme)
  if userinfo.len > 0:
    url.u_userinfo = copyStr(userinfo)
  else:
    url.u_userinfo = nil

  url.u_hostname = copyStr(hostname)
  url.u_port = port
  url.u_path = copyStr(path)
  if query.len > 0:
    url.u_query = copyStr(query)
  else:
    url.u_query = nil
  if frag.len > 0:
    url.u_fragment = copyStr(frag)
  else:
    url.u_fragment = nil

  return url

proc nng_url_free*(url: ptr NngUrlObj) {.exportc, dynlib, cdecl.} =
  if url != nil:
    if url.u_buffer != nil:
      deallocShared(url.u_buffer)
    deallocShared(url)

proc nng_url_scheme*(url: ptr NngUrlObj): cstring {.exportc, dynlib, cdecl.} =
  if url != nil: url.u_scheme else: nil

proc nng_url_port*(url: ptr NngUrlObj): uint32 {.exportc, dynlib, cdecl.} =
  if url != nil: url.u_port else: 0

proc nng_url_resolve_port*(
    url: ptr NngUrlObj, port: uint32
) {.exportc, dynlib, cdecl.} =
  if url != nil and url.u_port == 0:
    url.u_port = port

proc nng_url_hostname*(url: ptr NngUrlObj): cstring {.exportc, dynlib, cdecl.} =
  if url != nil: url.u_hostname else: nil

proc nng_url_userinfo*(url: ptr NngUrlObj): cstring {.exportc, dynlib, cdecl.} =
  if url != nil: url.u_userinfo else: nil

proc nng_url_path*(url: ptr NngUrlObj): cstring {.exportc, dynlib, cdecl.} =
  if url != nil: url.u_path else: nil

proc nng_url_query*(url: ptr NngUrlObj): cstring {.exportc, dynlib, cdecl.} =
  if url != nil: url.u_query else: nil

proc nng_url_fragment*(url: ptr NngUrlObj): cstring {.exportc, dynlib, cdecl.} =
  if url != nil: url.u_fragment else: nil

proc nng_url_parse*(
    urlp: ptr ptr NngUrlObj, raw: cstring
): cint {.exportc, dynlib, cdecl.} =
  if urlp == nil or raw == nil:
    return cint(NNG_EINVAL)
  let rawStr = $raw
  let colonIdx = rawStr.find("://")
  if colonIdx < 0:
    return cint(NNG_EINVAL)
  let scheme = rawStr[0 ..< colonIdx]
  var rest = rawStr[colonIdx + 3 .. ^1]

  var userinfo = ""
  var hostname = ""
  var port: uint32 = 0
  var path = ""
  var query = ""
  var frag = ""

  let hashIdx = rest.find('#')
  if hashIdx >= 0:
    frag = rest[hashIdx + 1 .. ^1]
    rest = rest[0 ..< hashIdx]

  let qIdx = rest.find('?')
  if qIdx >= 0:
    query = rest[qIdx + 1 .. ^1]
    rest = rest[0 ..< qIdx]

  let slashIdx = rest.find('/')
  var hostpart = rest
  if slashIdx >= 0:
    hostpart = rest[0 ..< slashIdx]
    path = rest[slashIdx .. ^1]

  let atIdx = hostpart.find('@')
  if atIdx >= 0:
    userinfo = hostpart[0 ..< atIdx]
    hostpart = hostpart[atIdx + 1 .. ^1]

  if hostpart.startsWith("["):
    let closeBracket = hostpart.find(']')
    if closeBracket < 0:
      return cint(NNG_EINVAL)
    hostname = hostpart[1 ..< closeBracket]
    let afterBracket = hostpart[closeBracket + 1 .. ^1]
    if afterBracket.startsWith(":"):
      try:
        port = uint32(parseInt(afterBracket[1 .. ^1]))
      except CatchableError:
        return cint(NNG_EINVAL)
  else:
    let colonPort = hostpart.rfind(':')
    if colonPort >= 0:
      hostname = hostpart[0 ..< colonPort]
      try:
        port = uint32(parseInt(hostpart[colonPort + 1 .. ^1]))
      except CatchableError:
        return cint(NNG_EINVAL)
    else:
      hostname = hostpart

  urlp[] = newNngUrl(scheme, userinfo, hostname, port, path, query, frag)
  return 0

proc nng_url_clone*(
    dstp: ptr ptr NngUrlObj, src: ptr NngUrlObj
): cint {.exportc, dynlib, cdecl.} =
  if dstp == nil or src == nil:
    return cint(NNG_EINVAL)
  let scheme =
    if src.u_scheme != nil:
      $src.u_scheme
    else:
      ""
  let userinfo =
    if src.u_userinfo != nil:
      $src.u_userinfo
    else:
      ""
  let hostname =
    if src.u_hostname != nil:
      $src.u_hostname
    else:
      ""
  let path =
    if src.u_path != nil:
      $src.u_path
    else:
      ""
  let query =
    if src.u_query != nil:
      $src.u_query
    else:
      ""
  let frag =
    if src.u_fragment != nil:
      $src.u_fragment
    else:
      ""
  dstp[] = newNngUrl(scheme, userinfo, hostname, src.u_port, path, query, frag)
  return 0

proc urlToString(url: ptr NngUrlObj): string =
  if url == nil:
    return ""
  let scheme =
    if url.u_scheme != nil:
      $url.u_scheme
    else:
      ""
  result = scheme & "://"
  if url.u_userinfo != nil and url.u_userinfo[0] != '\0':
    result &= $url.u_userinfo & "@"
  let host =
    if url.u_hostname != nil:
      $url.u_hostname
    else:
      ""
  if host.contains(':') and not host.startsWith("["):
    result &= "[" & host & "]"
  else:
    result &= host
  if url.u_port != 0:
    result &= ":" & $url.u_port
  elif scheme.startsWith("tcp") or scheme.startsWith("tls") or scheme.startsWith("ws"):
    if host.len > 0 and (result.rfind(':') <= result.find("://") + 2):
      result &= ":0"
  if url.u_path != nil and url.u_path[0] != '\0':
    let p = $url.u_path
    if not result.endsWith("/") and not p.startsWith("/"):
      result &= "/"
    result &= p
  if url.u_query != nil and url.u_query[0] != '\0':
    result &= "?" & $url.u_query
  if url.u_fragment != nil and url.u_fragment[0] != '\0':
    result &= "#" & $url.u_fragment

proc nng_url_sprintf*(
    buf: cstring, sz: csize_t, url: ptr NngUrlObj
): cint {.exportc, dynlib, cdecl.} =
  if buf == nil or sz == 0 or url == nil:
    return cint(NNG_EINVAL)
  let s = urlToString(url)
  let copyLen = min(s.len, int(sz) - 1)
  if copyLen > 0:
    copyMem(buf, cstring(s), copyLen)
  cast[ptr UncheckedArray[char]](buf)[copyLen] = '\0'
  return cint(s.len)

proc nng_listen_url*(
    s: NngSocketObj, url: ptr NngUrlObj, lp: ptr NngListenerObj, flags: cint
): cint {.exportc, dynlib, cdecl.} =
  if url == nil:
    return cint(NNG_EINVAL)
  let addrStr = urlToString(url)
  return nng_listen(s, addrStr.cstring, lp, flags)

proc nng_dial_url*(
    s: NngSocketObj, url: ptr NngUrlObj, dp: ptr NngDialerObj, flags: cint
): cint {.exportc, dynlib, cdecl.} =
  if url == nil:
    return cint(NNG_EINVAL)
  let addrStr = urlToString(url)
  return nng_dial(s, addrStr.cstring, dp, flags)

proc nng_dialer_create_url*(
    dp: ptr NngDialerObj, s: NngSocketObj, url: ptr NngUrlObj
): cint {.exportc, dynlib, cdecl.} =
  if url == nil:
    return cint(NNG_EINVAL)
  let addrStr = urlToString(url)
  return nng_dialer_create(dp, s, addrStr.cstring)

proc nng_listener_create_url*(
    lp: ptr NngListenerObj, s: NngSocketObj, url: ptr NngUrlObj
): cint {.exportc, dynlib, cdecl.} =
  if url == nil:
    return cint(NNG_EINVAL)
  let addrStr = urlToString(url)
  return nng_listener_create(lp, s, addrStr.cstring)

proc nng_listener_get_url*(
    l: NngListenerObj, urlp: ptr ptr NngUrlObj
): cint {.exportc, dynlib, cdecl.} =
  if l.id notin gListeners or urlp == nil:
    return cint(NNG_EINVAL)
  let lis = gListeners[l.id]
  var addrStr = lis.url
  if lis.boundPort > 0:
    let colonIdx = addrStr.rfind(':')
    let schemeEnd = addrStr.find("://")
    if colonIdx > schemeEnd + 2:
      addrStr = addrStr[0 .. colonIdx] & $lis.boundPort
    else:
      addrStr &= ":" & $lis.boundPort
  var parsed: ptr NngUrlObj = nil
  let rv = nng_url_parse(addr parsed, addrStr.cstring)
  if rv != 0:
    return rv
  if lis.boundPort > 0 and parsed != nil:
    parsed.u_port = uint32(lis.boundPort)
  urlp[] = parsed
  return 0

proc nng_dialer_get_url*(
    d: NngDialerObj, urlp: ptr ptr NngUrlObj
): cint {.exportc, dynlib, cdecl.} =
  if d.id notin gDialers or urlp == nil:
    return cint(NNG_EINVAL)
  let dialer = gDialers[d.id]
  var parsed: ptr NngUrlObj = nil
  let rv = nng_url_parse(addr parsed, dialer.url.cstring)
  if rv != 0:
    return rv
  urlp[] = parsed
  return 0
