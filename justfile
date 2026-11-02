import? 'justfile.local'

default:
    @just --list

# 1. Dependency Setup
setup:
    npm install
    nimble install --depsOnly -y
    cd docs && npm install --legacy-peer-deps

# 2. Test Suite Execution
test:
    @just test-nim
    @just test-ts

test-nim:
    nimble test

test-ts:
    @just compile-wasm
    npx vitest run

# Upstream nanomsg/nng v1.12.4 NUTS C-ABI Specification Test Suite
test-nng-spec:
    bash scripts/run_upstream_nng_tests.sh

# HeaderKit v2 C-ABI Signature & Pipeline Verification
verify-abi:
    PYTHONPATH="${HEADERKIT_DIR:-../headerkit}" python3 scripts/verify_headerkit_abi.py

# 3. Linter Execution
lint:
    @just lint-nim
    @just lint-ts

lint-nim:
    nph --check .
    nimble check

lint-ts:
    npm run lint

# 4. Formatter Execution
fmt:
    nph .
    npm run fmt

# 5. Build Artifacts
build:
    @just compile-wasm
    npm run build

# Compile Nim to WebAssembly
compile-wasm:
    bash scripts/compile_wasm.sh

# 6. Documentation Dev Server
docs:
    nimble docgen
    cd docs && npm run dev

# 7. Documentation Production Build
docs-build:
    nimble docgen
    cd docs && npm run build

# 8. Clean Build Artifacts
clean:
    rm -rf nimcache htmldocs site docs/dist docs/.astro tools/docgen dist .venv
    find . -name '*.exe' -delete

# 9. Secret Scanning
check-secrets:
    @if command -v gitleaks >/dev/null 2>&1; then \
        gitleaks detect -v; \
    elif command -v pre-commit >/dev/null 2>&1; then \
        pre-commit run gitleaks --all-files; \
    else \
        echo "Error: neither gitleaks nor pre-commit found. Install gitleaks or pre-commit." >&2; exit 1; \
    fi

# 10. Upstream Template Synchronization
update-template:
    uvx copier update --trust
