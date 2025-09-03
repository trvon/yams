#!/usr/bin/env bash
# scripts/dev/build_test.sh
#
# Build and test YAMS using CMake presets and Conan, mirroring editor tasks.
#
# Usage:
#   scripts/dev/build_test.sh [debug|release]
#
# Notes:
# - Always enables tests/benchmarks for the chosen preset.
# - Cleans only the chosen preset's build dir and stale CMakeUserPresets.json include.
# - Parallel test level is auto-detected; override via CTEST_PARALLEL_LEVEL.

set -euo pipefail

cfg="${1:-debug}"
case "${cfg}" in
  debug|Debug) preset="yams-debug"; build_type="Debug"; outdir="build/yams-debug";;
  release|Release) preset="yams-release"; build_type="Release"; outdir="build/yams-release";;
  *) echo "Unknown config: ${cfg}. Use 'debug' or 'release'." >&2; exit 2;;
esac

# Clean per-config artifacts and stale user presets include
rm -rf "${outdir}" || true
rm -f CMakeUserPresets.json || true

# Conan deps (tests/benchmarks enabled) — requires Conan 2.x
conan install . \
  -of "${outdir}" \
  -s build_type="${build_type}" \
  -o '&:build_tests=True' \
  -o '&:build_benchmarks=True' \
  -b missing

# Configure with tests/benchmarks enabled for the chosen preset
TIDY_ARGS="clang-tidy;-checks=bugprone-*,performance-*,modernize-*,cppcoreguidelines-*,readability-*,portability-*,clang-analyzer-*,concurrency-*;-warnings-as-errors=bugprone-*,clang-analyzer-*,concurrency-*"

cmake --preset "${preset}" \
  -DYAMS_BUILD_TESTS=ON \
  -DYAMS_BUILD_BENCHMARKS=ON \
  -DYAMS_BUILD_DOCS=OFF \
  -DCMAKE_UNITY_BUILD=OFF \
  -DCMAKE_C_CLANG_TIDY="${TIDY_ARGS}" \
  -DCMAKE_CXX_CLANG_TIDY="${TIDY_ARGS}"

# Build
cmake --build --preset "${preset}"

# Auto-detect test parallelism if not provided
if [[ -z "${CTEST_PARALLEL_LEVEL:-}" ]]; then
  if command -v nproc >/dev/null 2>&1; then
    export CTEST_PARALLEL_LEVEL="$(nproc)"
  elif [[ "$(uname -s)" == "Darwin" ]] && command -v sysctl >/dev/null 2>&1; then
    export CTEST_PARALLEL_LEVEL="$(sysctl -n hw.ncpu)"
  else
    export CTEST_PARALLEL_LEVEL=1
  fi
fi

# Test
ctest --preset "${preset}" --output-on-failure

echo "Build and tests completed for preset '${preset}'."
