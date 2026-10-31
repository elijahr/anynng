#!/usr/bin/env bash
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

echo "=== [anynng] Upstream nanomsg/nng v1.12.4 Specification Suite Runner ==="

# 1. Ensure upstream shallow clone is present in .cache/nng-upstream
NNG_DIR=".cache/nng-upstream"
if [ ! -d "$NNG_DIR" ]; then
    echo "Cloning nanomsg/nng v1.12.4 into $NNG_DIR..."
    mkdir -p .cache
    git clone --depth 1 --branch v1.12.4 https://github.com/nanomsg/nng.git "$NNG_DIR"
fi

# 2. Build libanynng.dylib C-ABI shared library
echo "Building libanynng.dylib from src/anynng/c_api.nim..."
mkdir -p dist
nim c --app:lib -d:release --out:dist/libanynng.dylib src/anynng/c_api.nim
if [[ "$OSTYPE" == "darwin"* ]]; then
    install_name_tool -id @rpath/libanynng.dylib dist/libanynng.dylib 2>/dev/null || true
fi

# 3. Setup compiler and include paths
NUTS_INC="-I$NNG_DIR/include -I$NNG_DIR/src -I$NNG_DIR/src/testing -Itests/upstream_shim"
mkdir -p dist/upstream_tests

TEST_FILES=(
    "$NNG_DIR/src/sp/protocol/pair0/pair0_test.c"
    "$NNG_DIR/src/sp/protocol/pair1/pair1_test.c"
    "$NNG_DIR/src/sp/protocol/pair1/pair1_poly_test.c"
    "$NNG_DIR/src/sp/protocol/reqrep0/req_test.c"
    "$NNG_DIR/src/sp/protocol/reqrep0/rep_test.c"
    "$NNG_DIR/src/sp/protocol/pubsub0/pub_test.c"
    "$NNG_DIR/src/sp/protocol/pubsub0/sub_test.c"
    "$NNG_DIR/src/sp/protocol/pipeline0/push_test.c"
    "$NNG_DIR/src/sp/protocol/pipeline0/pull_test.c"
    "$NNG_DIR/src/sp/protocol/survey0/survey_test.c"
    "$NNG_DIR/src/sp/protocol/survey0/respond_test.c"
    "$NNG_DIR/src/sp/protocol/bus0/bus_test.c"
)

TOTAL=${#TEST_FILES[@]}
PASSED_COUNT=0
FAILED_COUNT=0

echo "Compiling and executing $TOTAL upstream NUTS protocol specification test suites..."

for t in "${TEST_FILES[@]}"; do
    name=$(basename "$t" .c)
    bin_path="dist/upstream_tests/$name"

    # Compile
    clang -O2 $NUTS_INC -Ldist -lanynng -Wl,-rpath,@loader_path/../.. -Wl,-rpath,dist \
        tests/upstream_shim/nuts_stat_shim.c \
        "$NNG_DIR/src/testing/marry.c" \
        "$NNG_DIR/src/testing/util.c" \
        "$t" -o "$bin_path"

    echo "------------------------------------------------------------"
    echo "Running NUTS suite: $name"
    echo "------------------------------------------------------------"
    status=0
    "$bin_path" || status=$?
    if [ $status -eq 0 ]; then
        echo "PASS: $name"
        ((PASSED_COUNT++))
    else
        echo "FAIL (partial/upstream strictness, exit $status): $name"
        ((FAILED_COUNT++))
    fi
done

echo "============================================================"
echo "Upstream Spec Test Summary:"
echo "  Total Suites:   $TOTAL"
echo "  Passed:         $PASSED_COUNT"
echo "  With Failures:  $FAILED_COUNT"
echo "============================================================"

# Strict assertion: 100% of suites must pass with 0 failures
if [ "$FAILED_COUNT" -eq 0 ] && [ "$PASSED_COUNT" -eq "$TOTAL" ]; then
    echo "✓ Upstream NUTS C-ABI specification test harness executed cleanly with 100% PASS across all $TOTAL suites."
    exit 0
else
    echo "✗ Upstream test execution failed: $FAILED_COUNT suites had failures."
    exit 1
fi
