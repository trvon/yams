#!/usr/bin/env bash
set -euo pipefail

# tidy_build.sh
# Unified developer helper for YAMS (Meson-first)
# - Mode 1 (default): Meson build + (optional) test + conveniences
# - Mode 2: clang-tidy focused run (legacy behavior via --tidy, CMake-based)
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
#   -p, --preset NAME     Build preset: yams-debug|yams-release (default: yams-debug)
#       --release         Shortcut for --preset yams-release
#   -c, --clean           Remove build/<preset> before configure
#       --no-conan        Skip conan install
#       --no-strict       Do NOT pass -Dwerror=true and highest warning level
#       --strict          Force -Dwerror=true and warning level 3 (default)
#       --no-tests        Alias of --no-test (deprecated)
#   -r, --regex REGEX     GTest filter (mapped to --test-args --gtest_filter=REGEX)
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

# NOTE: The --tidy path remains CMake-based for now and is intended only for
# ad-hoc static analysis runs. The primary build path below uses Meson.
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
enable_tests=1
mock=0
strict=1
extra_ctest=()

usage() { grep '^# ' "$0" | sed 's/^# //'; exit 2; }

suite="smoke"  # default fast suite; use --full to run all
while [[ $# -gt 0 ]]; do
  case "$1" in
    -p|--preset) preset="$2"; shift 2;;
    --release) preset="yams-release"; shift;;
    -r|--regex) regex="$2"; shift 2;;
    -c|--clean) clean=1; shift;;
    -j|--jobs) jobs="$2"; shift 2;;
    --no-conan) run_conan=0; shift;;
    --no-test) run_tests=0; enable_tests=0; shift;;
    --no-tests) run_tests=0; enable_tests=0; shift;;
    --mock) mock=1; shift;;
    --full) suite=""; shift;;
    --with-lzma) export YAMS_ENABLE_LZMA=1; shift;;
    --no-strict) strict=0; shift;;
    --strict) strict=1; shift;;
    -h|--help) usage;;
    --) shift; extra_ctest+=("$@"); break;;
    *) err "Unknown option: $1"; usage;;
  esac
done

# Map preset to Meson build dir and build type
build_dir="build/${preset/yams-/}"
bt="Debug"; [[ $preset == *release* ]] && bt="Release"
bt_meson=$(printf '%s' "${bt}" | tr '[:upper:]' '[:lower:]')

if [[ $clean -eq 1 ]]; then
  log "Cleaning ${build_dir}"
  rm -rf "${build_dir}"
fi

if [[ $run_conan -eq 1 ]]; then
  log "Conan install (build_type=${bt})"
  conan install . -of "${build_dir}" -s build_type="${bt}" -b missing ${YAMS_TIDY_CONAN_OPTS:-}
fi

log "Configure (Meson: ${build_dir}, strict=${strict})"
# Resolve Conan Meson native toolchain file robustly across layouts.
# Preferred (current Conan 2 Meson toolchain):
#   - ${build_dir}/build-${bt_meson}/conan/conan_meson_native.ini
# Historical/alternative layouts (fallbacks):
#   - ${build_dir}/build-${bt_meson}/generators/conan_meson_native.ini
#   - ${build_dir}/build/${bt}/generators/conan_meson_native.ini
#   - ${build_dir}/build/${bt_meson}/generators/conan_meson_native.ini
#   - ${build_dir}/build-${bt_meson}/${bt}/generators/conan_meson_native.ini
# Finally: search within ${build_dir}
candidate_native_inis=(
  "${build_dir}/build-${bt_meson}/conan/conan_meson_native.ini"
  "${build_dir}/build-${bt_meson}/generators/conan_meson_native.ini"
  "${build_dir}/build/${bt}/generators/conan_meson_native.ini"
  "${build_dir}/build/${bt_meson}/generators/conan_meson_native.ini"
  "${build_dir}/build-${bt_meson}/${bt}/generators/conan_meson_native.ini"
)
native_ini=""
for cand in "${candidate_native_inis[@]}"; do
  if [[ -f "${cand}" ]]; then
    native_ini="${cand}"
    break
  fi
done
if [[ -z "${native_ini}" ]]; then
  # Last-resort search (depth-limited for speed)
  native_ini=$(find "${build_dir}" -maxdepth 5 -type f -name 'conan_meson_native.ini' 2>/dev/null | head -n1 || true)
fi
if [[ -z "${native_ini}" ]]; then
  err "Could not locate conan_meson_native.ini under ${build_dir}. Did Conan install succeed?"
  err "Try: conan install . -of '${build_dir}' -s build_type='${bt}' -b missing"
  exit 1
fi
log "Using native file: ${native_ini}"
meson_args=("${build_dir}" "--native-file" "${native_ini}" "--buildtype=${bt_meson}")
if [[ ${enable_tests} -eq 1 ]]; then
  meson_args+=("-Dbuild-tests=true")
else
  meson_args+=("-Dbuild-tests=false")
fi
# LZMA enablement (Option A): default 'auto'; allow forcing on via --with-lzma
if [[ ${YAMS_ENABLE_LZMA:-0} -eq 1 ]]; then
  meson_args+=("-Denable-lzma=true")
fi
mkdir -p "${build_dir}"
if [[ $strict -eq 1 ]]; then
  meson_args+=("-Dwerror=true")
  # Use highest Meson warning level (0-3). Project default is 2; raise to 3 here.
  meson_args+=("-Dwarning_level=3")
fi
if [[ ! -d "${build_dir}/meson-private" ]]; then
  meson setup "${meson_args[@]}"
else
  meson setup --reconfigure "${meson_args[@]}"
fi

log "Build (Meson)"
if [[ -n $jobs ]]; then
  MESON_BUILD_ARGS=("-j" "$jobs")
else
  MESON_BUILD_ARGS=()
fi
meson compile -C "${build_dir}" ${MESON_BUILD_ARGS[@]:-}

if [[ $run_tests -eq 0 ]]; then
  log "Skipping tests (--no-test)"
  exit 0
fi

# Standardize test mode
export YAMS_TESTING=${YAMS_TESTING:-1}
if [[ $mock -eq 1 ]]; then
  export YAMS_USE_MOCK_PROVIDER=1
  export YAMS_SKIP_MODEL_LOADING=1
  export YAMS_PLUGIN_TRUST_ALL=1
  log "Mock embedding/provider mode enabled"
fi

# Map regex to gtest filter via --test-args
test_args=()
if [[ -n $regex ]]; then
  test_args+=("--test-args" "--gtest_filter=${regex}")
fi

if [[ -z "${suite}" && -z "${regex}" ]]; then
  log "Test suite: full"
  run_cmd=(meson test -C "${build_dir}" ${test_args[@]:-} ${YAMS_TIDY_CTEST_OPTS:-} --print-errorlogs)
elif [[ -n "${suite}" && -z "${regex}" ]]; then
  log "Test suite: ${suite} (default). Use --full for all or -r to filter."
  run_cmd=(meson test -C "${build_dir}" --suite "${suite}" ${YAMS_TIDY_CTEST_OPTS:-} --print-errorlogs)
else
  log "Test selection via regex: ${regex}"
  run_cmd=(meson test -C "${build_dir}" ${test_args[@]:-} ${YAMS_TIDY_CTEST_OPTS:-} --print-errorlogs)
fi

log "Run: ${run_cmd[*]}"
if ! "${run_cmd[@]}"; then
  err "Tests failed"
  err "Hint: re-run single test -> meson test -C ${build_dir} --test-args --gtest_filter=<name> -v"
  exit 1
fi
log "All tests passed"
