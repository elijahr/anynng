# tls.nim - TLS Configuration matching nng_tls_config
# Licensed under MIT License.

import std/os

type
  TlsMode* = enum
    tlsModeClient = 1
    tlsModeServer = 2

  TlsAuthMode* = enum
    tlsAuthModeNone = 0
    tlsAuthModeOptional = 1
    tlsAuthModeRequired = 2

  TlsVersion* = enum
    tlsVersion12 = 0x0303
    tlsVersion13 = 0x0304

  TlsConfig* = ref object
    mode*: TlsMode
    authMode*: TlsAuthMode
    minVersion*: TlsVersion
    maxVersion*: TlsVersion
    serverName*: string
    caChainPem*: string
    caFilePath*: string
    ownCertPem*: string
    ownKeyPem*: string
    ownCertFilePath*: string
    ownKeyFilePath*: string
    keyPassphrase*: string

proc newTlsConfig*(mode: TlsMode): TlsConfig =
  new(result)
  result.mode = mode
  result.authMode = if mode == tlsModeClient: tlsAuthModeRequired else: tlsAuthModeNone
  result.minVersion = tlsVersion12
  result.maxVersion = tlsVersion13
  result.serverName = ""
  result.caChainPem = ""
  result.caFilePath = ""
  result.ownCertPem = ""
  result.ownKeyPem = ""
  result.ownCertFilePath = ""
  result.ownKeyFilePath = ""
  result.keyPassphrase = ""

proc setServerName*(cfg: TlsConfig, name: string) {.inline.} =
  cfg.serverName = name

proc setAuthMode*(cfg: TlsConfig, auth: TlsAuthMode) {.inline.} =
  cfg.authMode = auth

proc setCaChain*(cfg: TlsConfig, certPem: string, crlPem: string = "") {.inline.} =
  cfg.caChainPem = certPem

proc setCaFile*(cfg: TlsConfig, path: string) {.inline.} =
  cfg.caFilePath = path

proc setOwnCert*(
    cfg: TlsConfig, certPem, keyPem: string, pass: string = ""
) {.inline.} =
  cfg.ownCertPem = certPem
  cfg.ownKeyPem = keyPem
  cfg.keyPassphrase = pass

proc setCertKeyFile*(
    cfg: TlsConfig, certPath: string, keyPath: string = ""
) {.inline.} =
  cfg.ownCertFilePath = certPath
  cfg.ownKeyFilePath = if keyPath.len > 0: keyPath else: certPath

proc setVersion*(cfg: TlsConfig, minV, maxV: TlsVersion) {.inline.} =
  cfg.minVersion = minV
  cfg.maxVersion = maxV

when not defined(js):
  import std/net

  proc toSslContext*(cfg: TlsConfig): SslContext =
    let verify =
      case cfg.authMode
      of tlsAuthModeNone, tlsAuthModeOptional: CVerifyNone
      of tlsAuthModeRequired: CVerifyPeer

    var certFile = cfg.ownCertFilePath
    var keyFile = cfg.ownKeyFilePath
    var caFile = cfg.caFilePath

    # If in-memory PEMs were provided, write them to ephemeral temp files for OpenSSL loader
    if cfg.ownCertPem.len > 0 and certFile.len == 0:
      let tmpCert = getTempDir() / "anynng_cert_" & $os.getCurrentProcessId() & ".pem"
      writeFile(tmpCert, cfg.ownCertPem)
      certFile = tmpCert

    if cfg.ownKeyPem.len > 0 and keyFile.len == 0:
      let tmpKey = getTempDir() / "anynng_key_" & $os.getCurrentProcessId() & ".pem"
      writeFile(tmpKey, cfg.ownKeyPem)
      keyFile = tmpKey

    if cfg.caChainPem.len > 0 and caFile.len == 0:
      let tmpCa = getTempDir() / "anynng_ca_" & $os.getCurrentProcessId() & ".pem"
      writeFile(tmpCa, cfg.caChainPem)
      caFile = tmpCa

    result = newContext(
      protVersion = protSSLv23,
      verifyMode = verify,
      certFile = certFile,
      keyFile = keyFile,
      caFile = caFile,
    )
