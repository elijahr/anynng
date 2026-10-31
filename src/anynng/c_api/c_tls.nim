# ----------------------------------------------------------------------
# TLS Configuration
# ----------------------------------------------------------------------

proc nng_tls_config_alloc*(
    cfgp: ptr pointer, mode: cint
): cint {.exportc, dynlib, cdecl.} =
  if cfgp == nil:
    return cint(NNG_EINVAL)
  let tlsMode = if mode == 1: tlsModeServer else: tlsModeClient
  let cfg = newTlsConfig(tlsMode)
  let id = gNextTlsCfgId
  inc gNextTlsCfgId
  gTlsConfigs[id] = cfg
  cfgp[] = cast[pointer](cfg)
  return 0

proc nng_tls_config_free*(cfg: pointer) {.exportc, dynlib, cdecl.} =
  if cfg != nil:
    for id, c in gTlsConfigs:
      if cast[pointer](c) == cfg:
        gTlsConfigs.del(id)
        break

proc nng_tls_config_server_name*(
    cfg: pointer, name: cstring
): cint {.exportc, dynlib, cdecl.} =
  if cfg == nil or name == nil:
    return cint(NNG_EINVAL)
  let c = cast[TlsConfig](cfg)
  c.setServerName($name)
  return 0

proc nng_tls_config_auth_mode*(
    cfg: pointer, mode: cint
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
    cfg: pointer, chain: cstring, crl: cstring
): cint {.exportc, dynlib, cdecl.} =
  if cfg == nil or chain == nil:
    return cint(NNG_EINVAL)
  let c = cast[TlsConfig](cfg)
  c.setCaChain($chain)
  return 0

proc nng_tls_config_ca_file*(
    cfg: pointer, path: cstring
): cint {.exportc, dynlib, cdecl.} =
  if cfg == nil or path == nil:
    return cint(NNG_EINVAL)
  let c = cast[TlsConfig](cfg)
  c.setCaFile($path)
  return 0

proc nng_tls_config_own_cert*(
    cfg: pointer, cert: cstring, key: cstring, pass: cstring
): cint {.exportc, dynlib, cdecl.} =
  if cfg == nil or cert == nil or key == nil:
    return cint(NNG_EINVAL)
  let c = cast[TlsConfig](cfg)
  c.setOwnCert($cert, $key)
  return 0

proc nng_tls_config_cert_key_file*(
    cfg: pointer, cert: cstring, key: cstring
): cint {.exportc, dynlib, cdecl.} =
  if cfg == nil or cert == nil or key == nil:
    return cint(NNG_EINVAL)
  let c = cast[TlsConfig](cfg)
  c.setCertKeyFile($cert, $key)
  return 0

proc nng_tls_config_version*(
    cfg: pointer, minV: cint, maxV: cint
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

proc nng_tls_engine_name*(): cstring {.exportc, dynlib, cdecl.} =
  return "openssl"
