#!/bin/bash

# Unified build script for YAMS
#
# Usage: ./setup.sh [Debug|Release|Profiling|Fuzzing] [--coverage] [--tsan] [--no-tsan]
#   build_type: Release (default), Debug, Profiling, or Fuzzing (TODO)
#   --coverage: Enable code coverage instrumentation (Debug builds only)
#   --tsan: Enable ThreadSanitizer for race detection (default for Debug builds)
#   --no-tsan: Disable ThreadSanitizer (overrides default)
#
# Environment variables (for CI/advanced use):
#   YAMS_CONAN_HOST_PROFILE  - Path to Conan host profile (bypasses auto-detection)
#   YAMS_CONAN_ARCH          - Target architecture (x86_64, armv8, etc.)
#   YAMS_EXTRA_MESON_FLAGS   - Additional Meson setup flags
#   YAMS_COMPILER            - Force compiler (clang or gcc)
#   YAMS_CPPSTD              - C++ standard (17, 20, 23)
#   YAMS_LIBCXX_HARDENING    - libc++ hardening mode (none, fast, extensive, debug)
#   YAMS_INSTALL_PREFIX      - Installation prefix (default: /usr/local or Homebrew)
#
# The script prefers Clang when available, falling back to GCC otherwise. It
# ensures Conan is given a concrete C++ standard so dependencies resolve cleanly
# and keeps Meson in sync with the generated toolchain file.

set -euo pipefail

ENABLE_COVERAGE=false
ENABLE_TSAN=""
BUILD_TYPE_INPUT=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --coverage)
      ENABLE_COVERAGE=true
      shift
      ;;
    --tsan)
      ENABLE_TSAN=true
      shift
      ;;
    --no-tsan)
      ENABLE_TSAN=false
      shift
      ;;
    Debug|Release|Profiling|Fuzzing|debug|release|profiling|fuzzing)
      if [[ -n "${BUILD_TYPE_INPUT}" ]]; then
        echo "Error: Build type specified multiple times" >&2
        exit 1
      fi
      BUILD_TYPE_INPUT="$1"
      shift
      ;;
    *)
      echo "Usage: $0 [Debug|Release|Profiling|Fuzzing] [--coverage] [--tsan] [--no-tsan]" >&2
      exit 1
      ;;
  esac
done

BUILD_TYPE_INPUT=${BUILD_TYPE_INPUT:-Release}
BUILD_TYPE_LOWER=$(echo "${BUILD_TYPE_INPUT}" | tr '[:upper:]' '[:lower:]')

case "${BUILD_TYPE_LOWER}" in
  debug)
    BUILD_TYPE="Debug"
    ;;
  release)
    BUILD_TYPE="Release"
    ;;
  profiling)
    BUILD_TYPE="Debug"  # Use Debug as base for Conan/Meson
    ENABLE_PROFILING=true
    ;;
  fuzzing)
    BUILD_TYPE="Debug"  # Use Debug as base for Conan/Meson
    ENABLE_FUZZING=true
    echo "Fuzzing build: AFL++/libFuzzer instrumentation enabled"
    ;;
  *)
    echo "Unknown build type: ${BUILD_TYPE_INPUT}. Expected Debug, Release, Profiling, or Fuzzing." >&2
    exit 1
    ;;
esac

if [[ "${ENABLE_COVERAGE}" == "true" ]] && [[ "${BUILD_TYPE}" != "Debug" ]]; then
  echo "Error: --coverage flag requires Debug build type" >&2
  exit 1
fi

# Select desired C++ standard (defaults to C++23 if compiler supports it, otherwise C++20).
# Override with YAMS_CPPSTD=20/23.
# Auto-detection: test compiler for C++23 support (constexpr containers)
if [[ -z "${YAMS_CPPSTD:-}" ]]; then
  # Test if compiler supports C++23 with constexpr containers
  if echo '#include <vector>
#if __cplusplus >= 202302L && defined(__cpp_lib_constexpr_vector) && __cpp_lib_constexpr_vector >= 201907L
#define HAS_CPP23_CONTAINERS 1
#else
#define HAS_CPP23_CONTAINERS 0
#endif
#if !HAS_CPP23_CONTAINERS
#error "No C++23 constexpr containers"
#endif' | "${CXX:-c++}" -std=c++23 -fsyntax-only -x c++ - >/dev/null 2>&1; then
    CPPSTD_INPUT="23"
    echo "Auto-detected C++23 compiler support (constexpr containers available)"
  else
    CPPSTD_INPUT="20"
    echo "C++23 not fully supported, using C++20"
  fi
else
  CPPSTD_INPUT="${YAMS_CPPSTD}"
fi

case "${CPPSTD_INPUT}" in
  17|20|23)
    CPPSTD="${CPPSTD_INPUT}"
    ;;
  c++17|c++20|c++23)
    CPPSTD="${CPPSTD_INPUT#c++}"
    ;;
  *)
    echo "Unknown C++ standard: ${CPPSTD_INPUT}. Expected 17|20|23 or c++17|c++20|c++23." >&2
    exit 1
    ;;
esac
# Value for Meson project option
MESON_CPPSTD="c++${CPPSTD}"

# libc++ hardening mode (only applies when using libc++)
# Options: none, fast, extensive, debug
# Override with YAMS_LIBCXX_HARDENING=fast|extensive|debug|none
LIBCXX_HARDENING=${YAMS_LIBCXX_HARDENING:-none}

# Check if using explicit Conan profile (CI mode)
CONAN_HOST_PROFILE=${YAMS_CONAN_HOST_PROFILE:-}
CONAN_ARCH=${YAMS_CONAN_ARCH:-}
CONAN_EXTRA_OPTIONS=${YAMS_CONAN_EXTRA_OPTIONS:-}
USE_PROFILE=false

if [[ -n "${CONAN_HOST_PROFILE}" ]]; then
  if [[ ! -f "${CONAN_HOST_PROFILE}" ]]; then
    echo "ERROR: Conan host profile not found: ${CONAN_HOST_PROFILE}" >&2
    exit 1
  fi
  USE_PROFILE=true
  echo "Using explicit Conan host profile: ${CONAN_HOST_PROFILE}"
fi

CONAN_ARGS=(-s "build_type=${BUILD_TYPE}" -b missing --update)

# Add common Conan options (sqlite3 with FTS5, etc.)
CONAN_ARGS+=(-o "sqlite3/*:fts5=True")

# Fuzzing builds: disable programs in zstd to avoid symlink issues in Docker
if [[ "${ENABLE_FUZZING:-false}" == "true" ]]; then
  CONAN_ARGS+=(-o "zstd/*:build_programs=False")
fi

# Add extra Conan options from environment (for CI)
if [[ -n "${CONAN_EXTRA_OPTIONS}" ]]; then
  # shellcheck disable=SC2086
  read -ra EXTRA_OPTS <<< "${CONAN_EXTRA_OPTIONS}"
  CONAN_ARGS+=("${EXTRA_OPTS[@]}")
  echo "Extra Conan options: ${CONAN_EXTRA_OPTIONS}"
fi

detect_version() {
  local bin="$1"
  if command -v "${bin}" >/dev/null 2>&1; then
    if "${bin}" -dumpfullversion >/dev/null 2>&1; then
      "${bin}" -dumpfullversion
    elif "${bin}" -dumpversion >/dev/null 2>&1; then
      "${bin}" -dumpversion
    else
      "${bin}" --version | head -n1 | grep -oE '[0-9]+(\.[0-9]+)*' | head -n1
    fi
  fi
}

COERCE_MAJOR() {
  echo "$1" | cut -d. -f1
}

COMPILER_OVERRIDE=${YAMS_COMPILER:-}

# When using explicit profile, skip auto-detection of compiler settings
if [[ "${USE_PROFILE}" == "true" ]]; then
  echo "Skipping compiler auto-detection (using profile: ${CONAN_HOST_PROFILE})"
  # Still set CC/CXX for Meson if not already set
  if [[ -z "${CC:-}" ]] && command -v clang >/dev/null 2>&1; then
    export CC="clang"
    export CXX="clang++"
  elif [[ -z "${CC:-}" ]] && command -v gcc >/dev/null 2>&1; then
    export CC="gcc"
    export CXX="g++"
  fi
  # Add profile to Conan args
  CONAN_ARGS+=(-pr:h "${CONAN_HOST_PROFILE}" -pr:b default)
  # Add architecture if specified
  if [[ -n "${CONAN_ARCH}" ]]; then
    CONAN_ARGS+=(-s:h "arch=${CONAN_ARCH}")
    echo "Target architecture: ${CONAN_ARCH}"
  fi
else
  # Auto-detection mode (original behavior)
  if [[ "${COMPILER_OVERRIDE}" == clang ]] || { [[ -z "${COMPILER_OVERRIDE}" ]] && command -v clang >/dev/null 2>&1 && command -v clang++ >/dev/null 2>&1; }; then
    echo "--- Using Clang toolchain ---"
    export CC="clang"
    export CXX="clang++"
    # macOS requires libc++, Linux can use libstdc++11
    if [[ "$(uname -s)" == "Darwin" ]]; then
      LIBCXX="libc++"
    else
      LIBCXX="libstdc++11"
    fi
    CLANG_VERSION=$(detect_version clang++)
    CLANG_MAJOR=$(COERCE_MAJOR "${CLANG_VERSION:-0}")
    if [[ -z "${CLANG_MAJOR}" || "${CLANG_MAJOR}" == 0 ]]; then
      echo "Unable to detect clang version." >&2
      exit 1
    fi
    if [[ "$(uname -s)" == "Darwin" ]]; then
      # On macOS use Conan's apple-clang compiler model
      CONAN_ARGS+=(
        -s "compiler=apple-clang"
        -s "compiler.version=${CLANG_MAJOR}"
        -s "compiler.libcxx=${LIBCXX}"
        -s "compiler.cppstd=${CPPSTD}"
      )
    else
      CONAN_ARGS+=(
        -s "compiler=clang"
        -s "compiler.version=${CLANG_MAJOR}"
        -s "compiler.libcxx=${LIBCXX}"
        -s "compiler.cppstd=${CPPSTD}"
      )
    fi
  else
    echo "--- Using GCC toolchain ---"
    export CC="gcc"
    export CXX="g++"
    if ! command -v g++ >/dev/null 2>&1; then
      echo "g++ not found in PATH." >&2
      exit 1
    fi
    GCC_VERSION=$(detect_version g++)
    GCC_MAJOR=$(COERCE_MAJOR "${GCC_VERSION:-0}")
    if [[ -z "${GCC_MAJOR}" || "${GCC_MAJOR}" == 0 ]]; then
      echo "Unable to detect gcc version." >&2
      exit 1
    fi
    CONAN_ARGS+=(
      -s "compiler=gcc"
      -s "compiler.version=${GCC_MAJOR}"
      -s "compiler.libcxx=libstdc++11"
      -s "compiler.cppstd=${CPPSTD}"
    )
  fi
fi

if [[ "${ENABLE_PROFILING:-false}" == "true" ]]; then
  BUILD_DIR="build/profiling"
  CONAN_SUBDIR="build-profiling"
elif [[ "${ENABLE_FUZZING:-false}" == "true" ]]; then
  BUILD_DIR="build/fuzzing"
  CONAN_SUBDIR="build-fuzzing"
elif [[ "${BUILD_TYPE}" == "Debug" ]]; then
  BUILD_DIR="builddir"
  CONAN_SUBDIR="build-debug"
else
  BUILD_DIR="build/${BUILD_TYPE_LOWER}"
  CONAN_SUBDIR="build-${BUILD_TYPE_LOWER}"
fi

# Detect install prefix based on platform
if [[ "$(uname -s)" == "Darwin" ]]; then
  # macOS: check for Homebrew installation
  if command -v brew >/dev/null 2>&1; then
    BREW_PREFIX=$(brew --prefix 2>/dev/null || echo "/usr/local")
    INSTALL_PREFIX="${BREW_PREFIX}"
  else
    INSTALL_PREFIX="/usr/local"
  fi
else
  # Linux: use standard prefix
  INSTALL_PREFIX="/usr/local"
fi

# Allow override via environment
INSTALL_PREFIX="${YAMS_INSTALL_PREFIX:-${INSTALL_PREFIX}}"

echo "Build Type:        ${BUILD_TYPE_INPUT}"
if [[ "${ENABLE_PROFILING:-false}" == "true" ]]; then
  echo "Profiling:         Tracy enabled"
fi
if [[ "${ENABLE_FUZZING:-false}" == "true" ]]; then
  echo "Fuzzing:           AFL++/libFuzzer enabled"
fi
echo "Build Dir:         ${BUILD_DIR}"
echo "Install Prefix:    ${INSTALL_PREFIX}"
echo "C++ Std:           ${MESON_CPPSTD} (Conan: ${CPPSTD})"
if [[ "${LIBCXX_HARDENING}" != "none" ]]; then
  echo "libc++ Hardening:  ${LIBCXX_HARDENING}"
fi

echo "--- Exporting custom Conan recipes... ---"
# Export custom qpdf recipe if it exists
if [[ -f "conan/qpdf/conanfile.py" ]]; then
  echo "Exporting qpdf/11.9.0 from conan/qpdf/"
  conan export conan/qpdf --name=qpdf --version=11.9.0
fi

# Export custom onnxruntime recipe if it exists
if [[ -f "conan/onnxruntime/conanfile.py" ]]; then
  echo "Exporting onnxruntime/1.23.2 from conan/onnxruntime/"
  conan export conan/onnxruntime --name=onnxruntime --version=1.23.2
fi

echo "--- Running conan install... ---"
# Add policy toolchain for legacy recipes if in Docker or CI
POLICY_TC=""
if [[ -n "${DOCKERFILE_CONF_REV:-}" ]] || [[ -n "${CI:-}" ]]; then
  POLICY_TC="/tmp/yams_policy_toolchain.cmake"
  echo 'cmake_policy(VERSION 3.5)' > "$POLICY_TC"
  CONAN_ARGS+=(-c "tools.cmake.cmaketoolchain:user_toolchain+=${POLICY_TC}")
fi

# Enable tests and benchmarks for Debug builds in Conan (needed for Catch2/gtest/benchmark dependencies)
if [[ "${BUILD_TYPE}" == "Debug" ]] || [[ "${ENABLE_PROFILING:-false}" == "true" ]] || [[ "${ENABLE_FUZZING:-false}" == "true" ]]; then
  CONAN_ARGS+=(-o build_tests=True)
  CONAN_ARGS+=(-o build_benchmarks=True)
fi

# Enable Tracy profiling for profiling builds
if [[ "${ENABLE_PROFILING:-false}" == "true" ]]; then
  echo "Tracy profiling enabled"
  CONAN_ARGS+=(-o "tracy/*:enable=True")
fi

# Handle optional feature flags from environment
if [[ "${YAMS_DISABLE_ONNX:-}" == "true" ]]; then
  echo "ONNX support disabled (YAMS_DISABLE_ONNX=true)"
  CONAN_ARGS+=(-o "yams/*:enable_onnx=False")
fi

if [[ "${YAMS_DISABLE_SYMBOL_EXTRACTION:-}" == "true" ]]; then
  echo "Symbol extraction disabled (YAMS_DISABLE_SYMBOL_EXTRACTION=true)"
  CONAN_ARGS+=(-o "yams/*:enable_symbol_extraction=False")
fi

if [[ "${YAMS_DISABLE_PDF:-}" == "true" ]]; then
  echo "PDF support disabled (YAMS_DISABLE_PDF=true)"
  CONAN_ARGS+=(-o "yams/*:enable_pdf=False")
fi

# Force building missing packages to ensure ABI compatibility
# This is especially important for C++23 with Clang + libstdc++
# Always rebuild qpdf to ensure it's built with -fPIC for plugins
CONAN_ARGS+=(--build=missing --build=qpdf/*)

# Use runtime_deploy to copy shared libraries next to executables (mainly for Windows, no-op on Unix with RPATH)
conan install . -of "${BUILD_DIR}" "${CONAN_ARGS[@]}" --deployer=runtime_deploy --deployer-folder="${BUILD_DIR}"

# Check for either native or cross file (Conan generates cross file for cross-compilation)
NATIVE_FILE="${BUILD_DIR}/${CONAN_SUBDIR}/conan/conan_meson_native.ini"
CROSS_FILE="${BUILD_DIR}/${CONAN_SUBDIR}/conan/conan_meson_cross.ini"

if [[ -f "${NATIVE_FILE}" ]]; then
  MESON_TOOLCHAIN_ARG="--native-file"
  MESON_TOOLCHAIN_FILE="${NATIVE_FILE}"
elif [[ -f "${CROSS_FILE}" ]]; then
  MESON_TOOLCHAIN_ARG="--cross-file"
  MESON_TOOLCHAIN_FILE="${CROSS_FILE}"
else
  echo "Error: Conan meson toolchain file not found" >&2
  echo "Expected either: ${NATIVE_FILE}" >&2
  echo "          or: ${CROSS_FILE}" >&2
  echo "Please check the output path from 'conan install'." >&2
  exit 1
fi

MESON_ARGS=(
  "${BUILD_DIR}"
  "--prefix" "${INSTALL_PREFIX}"
  "${MESON_TOOLCHAIN_ARG}" "${MESON_TOOLCHAIN_FILE}"
  "--buildtype" "${BUILD_TYPE_LOWER}"
)

# Detect previous configured cpp_std to decide on reconfigure vs wipe
PREV_CPPSTD=""
INTRO_OPTS_JSON="${BUILD_DIR}/meson-info/intro-buildoptions.json"
if [[ -f "${INTRO_OPTS_JSON}" ]]; then
  PREV_CPPSTD=$(awk '/"name"\s*:\s*"cpp_std"/{flag=1} flag && /"value"/{gsub(/.*"value"\s*:\s*"|".*/,"",$0); print; exit}' "${INTRO_OPTS_JSON}" || true)
fi

MESON_OPTIONS=("-Dbuild-cli=true" "-Dcpp_std=${MESON_CPPSTD}")

# Add libc++ hardening mode if specified
if [[ "${LIBCXX_HARDENING}" != "none" ]]; then
  MESON_OPTIONS+=("-Dlibcxx-hardening=${LIBCXX_HARDENING}")
fi

# Handle optional feature flags for Meson (must match Conan options)
if [[ "${YAMS_DISABLE_ONNX:-}" == "true" ]]; then
  MESON_OPTIONS+=("-Denable-onnx=disabled")
  MESON_OPTIONS+=("-Dplugin-onnx=false")
fi

if [[ "${YAMS_DISABLE_SYMBOL_EXTRACTION:-}" == "true" ]]; then
  MESON_OPTIONS+=("-Denable-symbol-extraction=false")
  MESON_OPTIONS+=("-Dplugin-symbols=false")
fi

if [[ "${YAMS_DISABLE_PDF:-}" == "true" ]]; then
  MESON_OPTIONS+=("-Dplugin-pdf=false")
fi

# ThreadSanitizer: default enabled for Debug builds, can be overridden with --tsan/--no-tsan
if [[ -z "${ENABLE_TSAN}" ]]; then
  if [[ "${BUILD_TYPE}" == "Debug" ]] || [[ "${ENABLE_PROFILING:-false}" == "true" ]] || [[ "${ENABLE_FUZZING:-false}" == "true" ]]; then
    ENABLE_TSAN=true
  else
    ENABLE_TSAN=false
  fi
fi

if [[ "${BUILD_TYPE}" == "Debug" ]] || [[ "${ENABLE_PROFILING:-false}" == "true" ]] || [[ "${ENABLE_FUZZING:-false}" == "true" ]]; then
  MESON_OPTIONS+=(
    "-Dbuild-tests=true"
    "-Denable-bench-tests=true"
  )
fi

if [[ "${ENABLE_TSAN}" == "true" ]]; then
  MESON_OPTIONS+=("-Denable-tsan=true")
  echo "ThreadSanitizer enabled (race detection)"
fi

if [[ "${ENABLE_PROFILING:-false}" == "true" ]]; then
  MESON_OPTIONS+=(
    "-Denable-profiling=true"
  )
  echo "Tracy profiling enabled for Meson build"
fi

if [[ "${ENABLE_FUZZING:-false}" == "true" ]]; then
  MESON_OPTIONS+=(
    "-Dbuild-fuzzers=true"
  )
  echo "Fuzzing targets enabled for Meson build"
fi

if [[ "${ENABLE_COVERAGE}" == "true" ]]; then
  MESON_OPTIONS+=("-Db_coverage=true")
  echo "Coverage instrumentation enabled"
fi

# Add extra Meson flags from environment (for CI)
if [[ -n "${YAMS_EXTRA_MESON_FLAGS:-}" ]]; then
  # shellcheck disable=SC2086
  read -ra EXTRA_FLAGS <<< "${YAMS_EXTRA_MESON_FLAGS}"
  MESON_OPTIONS+=("${EXTRA_FLAGS[@]}")
  echo "Extra Meson flags: ${YAMS_EXTRA_MESON_FLAGS}"
fi

echo "--- Running meson setup... ---"
if [[ -n "${PREV_CPPSTD}" ]]; then
  if [[ "${PREV_CPPSTD}" != "${MESON_CPPSTD}" ]]; then
    echo "cpp_std changed (${PREV_CPPSTD} -> ${MESON_CPPSTD}); wiping build directory configuration..."
    meson setup "${MESON_ARGS[@]}" "${MESON_OPTIONS[@]}" --wipe
  else
    meson setup "${MESON_ARGS[@]}" "${MESON_OPTIONS[@]}" --reconfigure
  fi
else
  meson setup "${MESON_ARGS[@]}" "${MESON_OPTIONS[@]}"
fi

echo
echo "--- Setup complete! ---"
echo "To compile, run: meson compile -C ${BUILD_DIR}"
