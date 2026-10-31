#!/usr/bin/env bash
# scripts/compile_wasm.sh - Compiles anynng/wasm_bridge.nim into dist/anynng.wasm
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

mkdir -p "${ROOT_DIR}/dist"

INPUT_NIM="${ROOT_DIR}/src/anynng/wasm_bridge.nim"
OUTPUT_WASM="${ROOT_DIR}/dist/anynng.wasm"
NIMCACHE_DIR="${ROOT_DIR}/nimcache_wasm"

rm -rf "${NIMCACHE_DIR}"
mkdir -p "${NIMCACHE_DIR}"

NIM_LIB="$(nim dump 2>&1 | grep "lib$" | head -1 || echo "")"
if [[ -z "${NIM_LIB}" || ! -d "${NIM_LIB}" ]]; then
  NIM_BIN="$(command -v nim || echo "")"
  if [[ -n "${NIM_BIN}" ]]; then
    CANDIDATE="$(dirname "$(dirname "$(readlink -f "${NIM_BIN}" 2>/dev/null || realpath "${NIM_BIN}" 2>/dev/null || echo "${NIM_BIN}")")")/lib"
    if [[ -d "${CANDIDATE}" ]]; then
      NIM_LIB="${CANDIDATE}"
    fi
  fi
fi

# Locate LLVM clang, wasm-ld, and wasi-sysroot
CLANG_BIN=""
for c in "/opt/homebrew/opt/llvm/bin/clang" "/usr/local/opt/llvm/bin/clang" "$(command -v clang || true)"; do
  if [[ -n "${c}" && -x "${c}" ]] && "${c}" --print-targets 2>&1 | grep -q wasm; then
    CLANG_BIN="${c}"
    break
  fi
done

WASM_LD_BIN=""
for l in "/opt/homebrew/bin/wasm-ld" "/usr/local/bin/wasm-ld" "$(command -v wasm-ld || true)"; do
  if [[ -n "${l}" && -x "${l}" ]]; then
    WASM_LD_BIN="${l}"
    break
  fi
done

WASI_SYSROOT=""
for s in "/opt/homebrew/share/wasi-sysroot" "/usr/local/share/wasi-sysroot" "/opt/wasi-sysroot" "/usr/share/wasi-sysroot"; do
  if [[ -n "${s}" && -d "${s}" ]]; then
    WASI_SYSROOT="${s}"
    break
  fi
done

BUILTINS_C=""
for b in "/Users/eek/Development/nimts/c_src/builtins_wasm.c" "${ROOT_DIR}/c_src/builtins_wasm.c"; do
  if [[ -f "${b}" ]]; then
    BUILTINS_C="${b}"
    break
  fi
done

COMPILED=0

if [[ -n "${CLANG_BIN}" && -n "${WASM_LD_BIN}" && -n "${WASI_SYSROOT}" ]]; then
  echo "Compiling anynng WASM module via Nim + Clang + wasm-ld..."
  nim c -c -d:emscripten -d:useMalloc -d:release --opt:speed --cpu:wasm32 \
    --noMain:on \
    -p:"${ROOT_DIR}/src" \
    --nimcache:"${NIMCACHE_DIR}" \
    "${INPUT_NIM}"

  if [[ -n "${BUILTINS_C}" ]]; then
    "${CLANG_BIN}" --target=wasm32-wasip1 --sysroot="${WASI_SYSROOT}" \
      -c "${BUILTINS_C}" -o "${NIMCACHE_DIR}/builtins_wasm.o"
  fi

  for f in "${NIMCACHE_DIR}"/*.c; do
    "${CLANG_BIN}" --target=wasm32-wasip1 --sysroot="${WASI_SYSROOT}" \
      -D_WASI_EMULATED_MMAN -D_WASI_EMULATED_SIGNAL \
      -O3 -flto -mbulk-memory -I"${NIM_LIB}" \
      -c -o "${f}.o" -- "${f}"
  done

  "${WASM_LD_BIN}" --no-entry --export-all --allow-undefined --lto-O3 \
    --initial-memory=16777216 --max-memory=2147483648 \
    --export=NimMain \
    -L"${WASI_SYSROOT}/lib/wasm32-wasip1" -lc -lwasi-emulated-mman -lwasi-emulated-signal \
    -o "${OUTPUT_WASM}" \
    "${NIMCACHE_DIR}"/*.o
  COMPILED=1
fi

if [[ "${COMPILED}" -eq 0 || ! -f "${OUTPUT_WASM}" ]]; then
  echo "Error: Failed to compile WebAssembly module. Clang/wasm-ld/wasi-sysroot required." >&2
  exit 1
fi

if [[ -d "${ROOT_DIR}/ts/wasm" ]]; then
  cp -f "${OUTPUT_WASM}" "${ROOT_DIR}/ts/wasm/anynng.wasm"
fi

echo "WASM compilation complete: ${OUTPUT_WASM} ($(wc -c < "${OUTPUT_WASM}" | tr -d ' ') bytes)"
