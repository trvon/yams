#!/usr/bin/env bash
# Verify the libyams_mobile C ABI surface.
#
# Usage: scripts/ci/check_mobile_abi.sh <path-to-libyams_mobile.{so,dylib}> [header]
#
# Checks, host build only (this says nothing about Android/iOS device loading):
#   1. The public header compiles as C11 and as C++20, so every declaration is
#      visible to plain-C / FFI consumers (Swift, Kotlin/JNI, Dart FFI).
#   2. Every function declared with YAMS_MOBILE_API is exported from the shared
#      library with an unmangled C name.
#   3. The library reports no yams_mobile_* export that the header does not declare.
set -euo pipefail

LIB_PATH="${1:-}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
HEADER="${2:-${REPO_ROOT}/include/yams/api/mobile_bindings.h}"

if [ -z "$LIB_PATH" ] || [ ! -f "$LIB_PATH" ]; then
    echo "usage: $0 <libyams_mobile shared library> [header]" >&2
    exit 2
fi
if [ ! -f "$HEADER" ]; then
    echo "mobile header not found: $HEADER" >&2
    exit 2
fi

WORK_DIR="$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/yams-mobile-abi.XXXXXX")"
trap 'rm -rf "$WORK_DIR"' EXIT

# Declared API: flatten the header and pull the identifier in front of '(' in
# each `YAMS_MOBILE_API ... ;` declaration.
tr '\n' ' ' <"$HEADER" |
    grep -oE 'YAMS_MOBILE_API[^;{#]*\(' |
    grep -oE 'yams_mobile_[A-Za-z0-9_]+[[:space:]]*\($' |
    sed -E 's/[[:space:]]*\($//' | sort -u >"$WORK_DIR/declared.txt"

declared_count=$(wc -l <"$WORK_DIR/declared.txt" | tr -d ' ')
if [ "$declared_count" -eq 0 ]; then
    echo "no YAMS_MOBILE_API declarations found in $HEADER" >&2
    exit 1
fi

# 1. Header must compile in both languages and declare every symbol in C.
INCLUDE_DIR="$(cd "$(dirname "$HEADER")/../.." && pwd)"
{
    echo '#include <yams/api/mobile_bindings.h>'
    echo 'const void* yams_mobile_abi_probe[] = {'
    sed -E 's/^(.*)$/    (const void*)\&\1,/' "$WORK_DIR/declared.txt"
    echo '};'
} >"$WORK_DIR/probe.c"
CC_BIN="${CC:-cc}"
CXX_BIN="${CXX:-c++}"
"$CC_BIN" -std=c11 -Wall -Werror -I"$INCLUDE_DIR" -c "$WORK_DIR/probe.c" -o "$WORK_DIR/probe_c.o"
cp "$WORK_DIR/probe.c" "$WORK_DIR/probe.cpp"
"$CXX_BIN" -std=c++20 -Wall -Werror -I"$INCLUDE_DIR" -c "$WORK_DIR/probe.cpp" \
    -o "$WORK_DIR/probe_cpp.o"

# Both translation units must reference the same unmangled names.
nm -u "$WORK_DIR/probe_c.o" | awk '{print $NF}' | sed -E 's/^_//' | grep '^yams_mobile_' |
    sort -u >"$WORK_DIR/c_refs.txt"
nm -u "$WORK_DIR/probe_cpp.o" | awk '{print $NF}' | sed -E 's/^_//' | grep '^yams_mobile_' |
    sort -u >"$WORK_DIR/cpp_refs.txt"
if ! diff -u "$WORK_DIR/c_refs.txt" "$WORK_DIR/cpp_refs.txt"; then
    echo "C and C++ consumers see different linkage for the mobile API (missing extern \"C\"?)" >&2
    exit 1
fi

# 2/3. Compare against the library's defined dynamic exports.
case "$(uname -s)" in
Darwin) nm -gU "$LIB_PATH" | awk '{print $NF}' | sed -E 's/^_//' ;;
*) nm -D --defined-only "$LIB_PATH" | awk '{print $NF}' ;;
esac | grep -E '^yams_mobile_' | sort -u >"$WORK_DIR/exported.txt"

status=0
comm -23 "$WORK_DIR/declared.txt" "$WORK_DIR/exported.txt" >"$WORK_DIR/missing.txt"
if [ -s "$WORK_DIR/missing.txt" ]; then
    echo "declared in $HEADER but not exported by $LIB_PATH:" >&2
    sed 's/^/  /' "$WORK_DIR/missing.txt" >&2
    status=1
fi
comm -13 "$WORK_DIR/declared.txt" "$WORK_DIR/exported.txt" >"$WORK_DIR/extra.txt"
if [ -s "$WORK_DIR/extra.txt" ]; then
    echo "exported by $LIB_PATH but not declared in $HEADER:" >&2
    sed 's/^/  /' "$WORK_DIR/extra.txt" >&2
    status=1
fi

if [ "$status" -eq 0 ]; then
    echo "mobile ABI OK: ${declared_count} functions declared, exported, and C-linkable ($LIB_PATH)"
fi
exit "$status"
