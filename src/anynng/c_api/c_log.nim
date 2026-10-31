proc nng_null_logger*(
    level: cint, facility: cint, msgid: cstring, msg: cstring
) {.exportc, dynlib, cdecl.} =
  discard

proc nng_stderr_logger*(
    level: cint, facility: cint, msgid: cstring, msg: cstring
) {.exportc, dynlib, cdecl.} =
  discard

proc nng_system_logger*(
    level: cint, facility: cint, msgid: cstring, msg: cstring
) {.exportc, dynlib, cdecl.} =
  discard

proc nng_log_set_facility*(facility: cint) {.exportc, dynlib, cdecl.} =
  gLogFacility = facility

proc nng_log_set_level*(level: cint) {.exportc, dynlib, cdecl.} =
  gLogLevel = level

proc nng_log_get_level*(): cint {.exportc, dynlib, cdecl.} =
  gLogLevel

proc nng_log_set_logger*(logger: NngLogger) {.exportc, dynlib, cdecl.} =
  gCurrentLogger = logger

proc nng_log_err*(msgid: cstring, msg: cstring) {.exportc, dynlib, cdecl, varargs.} =
  discard

proc nng_log_warn*(msgid: cstring, msg: cstring) {.exportc, dynlib, cdecl, varargs.} =
  discard

proc nng_log_notice*(msgid: cstring, msg: cstring) {.exportc, dynlib, cdecl, varargs.} =
  discard

proc nng_log_info*(msgid: cstring, msg: cstring) {.exportc, dynlib, cdecl, varargs.} =
  discard

proc nng_log_debug*(msgid: cstring, msg: cstring) {.exportc, dynlib, cdecl, varargs.} =
  discard

proc nng_log_auth*(
    level: cint, msgid: cstring, msg: cstring
) {.exportc, dynlib, cdecl, varargs.} =
  discard
