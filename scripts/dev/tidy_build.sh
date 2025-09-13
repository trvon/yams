#!/usr/bin/env bash
set -euo pipefail

# tidy_build.sh
# Unified developer helper for YAMS
# - Mode 1 (default): build + (optional) test + conveniences (NEW)
# - Mode 2: clang-tidy focused run (legacy behavior via --tidy)
#
# This replaces the earlier single‑purpose clang‑tidy wrapper by adding:
#   * Optional clean of build preset
#   * Conan install (unless --no-conan)
#   * Configure + build chosen preset
#   * Mock embedding/provider mode (--mock) to bypass ONNX runtime
#   * Test selection via -R regex
#   * Summaries & colored logging
#   * Retains original clang‑tidy flow via --tidy (subcommand)
#
# Usage:
#   scripts/dev/tidy_build.sh [options]            # build + test path
#   scripts/dev/tidy_build.sh --tidy [preset]      # legacy clang-tidy mode
#
# Common Options (build+test mode):
#   -p, --preset NAME     CMake preset (default: yams-debug)
#       --release         Shortcut for --preset yams-release
#   -c, --clean           Remove build/<preset> before configure
#       --no-conan        Skip conan install
#   -r, --regex REGEX     ctest -R REGEX
#       --no-test         Skip running tests
#       --mock            Enable mock embedding/provider env (YAMS_USE_MOCK_PROVIDER=1 etc.)
#   -j, --jobs N          Parallel build jobs override
#       --                 Stop parsing; rest passed to ctest
#   -h, --help            Show help
#
# Environment overrides:
#   YAMS_TIDY_CONAN_OPTS  Extra args for conan install
#   YAMS_TIDY_CTEST_OPTS  Extra args appended to ctest invocation
#
# Exit codes:
#   0 success (or skipped tests)
#   1 failure
#   2 usage error

color() { local c="$1"; shift; printf "\033[%sm%s\033[0m" "$c" "$*"; }
log() { echo "$(color 1; color 34 [tidy]) $*"; }
warn() { echo "$(color 33 [tidy]) $*" >&2; }
err() { echo "$(color 31 [tidy]) $*" >&2; }

if [[ ${1:-} == "--tidy" ]]; then
  shift || true
  preset="${1:-yams-debug}"; shift || true
  outdir="build/${preset}"
  log "(tidy mode) preset=${preset}"
  log_full="${outdir}/tidy_full.log"
  log_errors="${outdir}/tidy_errors.log"
  log_checks="${outdir}/tidy_checks.txt"
  mkdir -p "${outdir}"
  if [[ -n "${TIDY_ARGS:-}" ]]; then
    cmake --preset "${preset}" -DCMAKE_UNITY_BUILD=OFF -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
      -DCMAKE_C_CLANG_TIDY="${TIDY_ARGS}" -DCMAKE_CXX_CLANG_TIDY="${TIDY_ARGS}"
  else
    cmake --preset "${preset}" -DCMAKE_UNITY_BUILD=OFF -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
  fi
  log "Building (serial for clearer diagnostics)"
  set +e
  cmake --build --preset "${preset}" -j1 2>&1 | tee "${log_full}"
  status=${PIPESTATUS[0]}
  set -e
  grep -E "^[^ ]*:[0-9]+:[0-9]+: error: " "${log_full}" > "${log_errors}" || true
  grep -o "\[[^]]\+\]" "${log_errors}" | sort | uniq -c | sort -nr > "${log_checks}" || true
  echo; log "Error summary (first 20):"; head -n 20 "${log_errors}" || true
  echo; log "Check counts:"; cat "${log_checks}" || true
  exit "${status}"
fi

preset="yams-debug"
regex=""
clean=0
jobs=""
run_conan=1
run_tests=1
mock=0
extra_ctest=()

usage() { grep '^# ' "$0" | sed 's/^# //'; exit 2; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    -p|--preset) preset="$2"; shift 2;;
    --release) preset="yams-release"; shift;;
    -r|--regex) regex="$2"; shift 2;;
    -c|--clean) clean=1; shift;;
    -j|--jobs) jobs="$2"; shift 2;;
    --no-conan) run_conan=0; shift;;
    --no-test) run_tests=0; shift;;
    --mock) mock=1; shift;;
    -h|--help) usage;;
    --) shift; extra_ctest+=("$@"); break;;
    *) err "Unknown option: $1"; usage;;
  esac
done

build_dir="build/${preset}"
if [[ $clean -eq 1 ]]; then
  log "Cleaning ${build_dir}"
  rm -rf "${build_dir}"
fi

if [[ $run_conan -eq 1 ]]; then
  bt="Debug"; [[ $preset == *release* ]] && bt="Release"
  log "Conan install (build_type=${bt})"
  conan install . -of "build/${preset}" -s build_type="${bt}" -b missing ${YAMS_TIDY_CONAN_OPTS:-}
fi

log "Configure (preset=${preset})"
cmake --preset "${preset}"

log "Build"
if [[ -n $jobs ]]; then
  cmake --build --preset "${preset}" -j "$jobs"
else
  cmake --build --preset "${preset}"
fi

if [[ $run_tests -eq 0 ]]; then
  log "Skipping tests (--no-test)"
  exit 0
fi

pushd "${build_dir}" >/dev/null || { err "Cannot enter ${build_dir}"; exit 1; }
ctest_args=(--output-on-failure)
[[ -n $regex ]] && ctest_args+=(-R "$regex")
[[ ${#extra_ctest[@]} -gt 0 ]] && ctest_args+=("${extra_ctest[@]}")

# Standardize test mode
export YAMS_TESTING=${YAMS_TESTING:-1}
if [[ $mock -eq 1 ]]; then
  export YAMS_USE_MOCK_PROVIDER=1
  export YAMS_SKIP_MODEL_LOADING=1
  export YAMS_PLUGIN_TRUST_ALL=1
  log "Mock embedding/provider mode enabled"
fi

log "Run tests: ctest ${ctest_args[*]} ${YAMS_TIDY_CTEST_OPTS:-}"
if ! ctest ${ctest_args[@]} ${YAMS_TIDY_CTEST_OPTS:-}; then
  err "Tests failed"
  err "Hint: re-run single test -> ctest -R <name> -V"
  exit 1
fi
log "All tests passed"
popd >/dev/null
