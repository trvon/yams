#!/usr/bin/env bash
# Run clang-tidy via the CMake preset in a controlled, low-noise way.
# - Forces -j1 for early fail and deterministic order
# - Turns off Unity builds to reduce overwhelming diagnostics
# - Captures full build log and extracts a concise error summary
#
# Usage:
#   scripts/dev/tidy_build.sh [preset]
#
# Default preset: yams-debug

set -euo pipefail

preset="${1:-yams-debug}"

outdir="build/${preset}"
log_full="${outdir}/tidy_full.log"
log_errors="${outdir}/tidy_errors.log"
log_checks="${outdir}/tidy_checks.txt"

mkdir -p "${outdir}"

# Ensure compile database is on and Unity build is off for clearer diagnostics
if [[ -n "${TIDY_ARGS:-}" ]]; then
  cmake --preset "${preset}" \
    -DCMAKE_UNITY_BUILD=OFF \
    -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
    -DCMAKE_C_CLANG_TIDY="${TIDY_ARGS}" \
    -DCMAKE_CXX_CLANG_TIDY="${TIDY_ARGS}"
else
  cmake --preset "${preset}" \
    -DCMAKE_UNITY_BUILD=OFF \
    -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
fi

echo "[tidy] Building preset '${preset}' with -j1 (logging to ${log_full})" >&2
set +e
cmake --build --preset "${preset}" -j1 2>&1 | tee "${log_full}"
status=${PIPESTATUS[0]}
set -e

# Extract concise error lines (path:line:col: error: message [check])
grep -E "^[^ ]*:[0-9]+:[0-9]+: error: " "${log_full}" > "${log_errors}" || true

# Summarize check occurrences (e.g. [clang-analyzer-...])
grep -o "\[[^]]\+\]" "${log_errors}" | sort | uniq -c | sort -nr > "${log_checks}" || true

echo
echo "[tidy] Error summary (first 20):"
head -n 20 "${log_errors}" || true

echo
echo "[tidy] Check counts:"
cat "${log_checks}" || true

exit "${status}"
