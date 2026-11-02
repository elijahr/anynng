# ----------------------------------------------------------------------
# TLS Configuration
# ----------------------------------------------------------------------

proc nng_tls_config_alloc*(
    cfgp: ptr ptr NngTlsConfigObj, mode: cint
): cint {.exportc, dynlib, cdecl.} =
  if cfgp == nil:
    return cint(NNG_EINVAL)
  let tlsMode = if mode == 1: tlsModeServer else: tlsModeClient
  let cfg = newTlsConfig(tlsMode)
  let id = gNextTlsCfgId
  inc gNextTlsCfgId
  gTlsConfigs[id] = cfg
  cfgp[] = cast[ptr NngTlsConfigObj](cfg)
  return 0

proc nng_tls_config_free*(cfg: ptr NngTlsConfigObj) {.exportc, dynlib, cdecl.} =
  if cfg != nil:
    for id, c in gTlsConfigs:
      if cast[ptr NngTlsConfigObj](c) == cfg:
        gTlsConfigs.del(id)
        break

proc nng_tls_config_server_name*(
    cfg: ptr NngTlsConfigObj, name: cstring
): cint {.exportc, dynlib, cdecl.} =
  if cfg == nil or name == nil:
    return cint(NNG_EINVAL)
  let c = cast[TlsConfig](cfg)
  c.setServerName($name)
  return 0

proc nng_tls_config_auth_mode*(
    cfg: ptr NngTlsConfigObj, mode: cint
): cint {.exportc, dynlib, cdecl.} =
  if cfg == nil:
    return cint(NNG_EINVAL)
  let c = cast[TlsConfig](cfg)
  let auth =
    case mode
    of 0:
      tlsAuthModeNone
    of 1:
      tlsAuthModeOptional
    of 2:
      tlsAuthModeRequired
    else:
      return cint(NNG_EINVAL)
  c.setAuthMode(auth)
  return 0

proc nng_tls_config_ca_chain*(
    cfg: ptr NngTlsConfigObj, chain: cstring, crl: cstring
): cint {.exportc, dynlib, cdecl.} =
  if cfg == nil or chain == nil:
    return cint(NNG_EINVAL)
  let c = cast[TlsConfig](cfg)
  c.setCaChain($chain)
  return 0

proc nng_tls_config_ca_file*(
    cfg: ptr NngTlsConfigObj, path: cstring
): cint {.exportc, dynlib, cdecl.} =
  if cfg == nil or path == nil:
    return cint(NNG_EINVAL)
  let c = cast[TlsConfig](cfg)
  c.setCaFile($path)
  return 0

proc nng_tls_config_own_cert*(
    cfg: ptr NngTlsConfigObj, cert: cstring, key: cstring, pass: cstring
): cint {.exportc, dynlib, cdecl.} =
  if cfg == nil or cert == nil or key == nil:
    return cint(NNG_EINVAL)
  let c = cast[TlsConfig](cfg)
  c.setOwnCert($cert, $key)
  return 0

proc nng_tls_config_cert_key_file*(
    cfg: ptr NngTlsConfigObj, cert: cstring, key: cstring
): cint {.exportc, dynlib, cdecl.} =
  if cfg == nil or cert == nil or key == nil:
    return cint(NNG_EINVAL)
  let c = cast[TlsConfig](cfg)
  c.setCertKeyFile($cert, $key)
  return 0

proc nng_tls_config_version*(
    cfg: ptr NngTlsConfigObj, minV: cint, maxV: cint
): cint {.exportc, dynlib, cdecl.} =
  if cfg == nil:
    return cint(NNG_EINVAL)
  let c = cast[TlsConfig](cfg)
  let minTls =
    case minV
    of 0x303: tlsVersion12
    of 0x304: tlsVersion13
    else: tlsVersion12
  let maxTls =
    case maxV
    of 0x303: tlsVersion12
    of 0x304: tlsVersion13
    else: tlsVersion13
  c.setVersion(minTls, maxTls)
  return 0

proc nng_tls_config_hold*(cfg: ptr NngTlsConfigObj) {.exportc, dynlib, cdecl.} =
  discard

proc nng_tls_engine_name*(): cstring {.exportc, dynlib, cdecl.} =
  return "openssl"

proc nng_tls_engine_description*(): cstring {.exportc, dynlib, cdecl.} =
  return "OpenSSL TLS Engine"

proc nng_tls_engine_fips_mode*(): bool {.exportc, dynlib, cdecl.} =
  return false
