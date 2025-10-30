#!/bin/bash

# Unified build script for YAMS
#
# Usage: ./setup.sh [Debug|Release]
#   build_type: Release (default) or Debug
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

if [[ $# -gt 1 ]]; then
  echo "Usage: $0 [Debug|Release]" >&2
  exit 1
fi

BUILD_TYPE_INPUT=${1:-Release}
BUILD_TYPE_LOWER=$(echo "${BUILD_TYPE_INPUT}" | tr '[:upper:]' '[:lower:]')

case "${BUILD_TYPE_LOWER}" in
  debug)
    BUILD_TYPE="Debug"
    ;;
  release)
    BUILD_TYPE="Release"
    ;;
  *)
    echo "Unknown build type: ${BUILD_TYPE_INPUT}. Expected Debug or Release." >&2
    exit 1
    ;;

esac

# Select desired C++ standard (defaults to C++23). Override with YAMS_CPPSTD=20/23.
CPPSTD_INPUT=${YAMS_CPPSTD:-23}
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
      # NOTE: Forced includes (-include cstdint/stdint.h) have been disabled
      # as they cause abseil build failures. If specific compilation issues
      # arise, consider more targeted fixes or compiler-specific workarounds.
      # export CXXFLAGS="${CXXFLAGS:-} -include cstdint"
      # export CFLAGS="${CFLAGS:-} -include stdint.h"
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
      # Linux/other: vanilla clang
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

if [[ "${BUILD_TYPE}" == "Debug" ]]; then
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

echo "Build Type:        ${BUILD_TYPE}"
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

echo "--- Running conan install... ---"
# Add policy toolchain for legacy recipes if in Docker or CI
POLICY_TC=""
if [[ -n "${DOCKERFILE_CONF_REV:-}" ]] || [[ -n "${CI:-}" ]]; then
  POLICY_TC="/tmp/yams_policy_toolchain.cmake"
  echo 'cmake_policy(VERSION 3.5)' > "$POLICY_TC"
  CONAN_ARGS+=(-c "tools.cmake.cmaketoolchain:user_toolchain+=${POLICY_TC}")
fi

conan install . -of "${BUILD_DIR}" "${CONAN_ARGS[@]}"

NATIVE_FILE="${BUILD_DIR}/${CONAN_SUBDIR}/conan/conan_meson_native.ini"

if [[ ! -f "${NATIVE_FILE}" ]]; then
  echo "Error: Conan native file not found at ${NATIVE_FILE}" >&2
  echo "Please check the output path from 'conan install'." >&2
  exit 1
fi

MESON_ARGS=(
  "${BUILD_DIR}"
  "--prefix" "${INSTALL_PREFIX}"
  "--native-file" "${NATIVE_FILE}"
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

if [[ "${BUILD_TYPE}" == "Debug" ]]; then
  MESON_OPTIONS+=(
    "-Dbuild-tests=true"
    "-Denable-bench-tests=true"
  )
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
