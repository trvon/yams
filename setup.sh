#!/bin/bash

# Unified build script for YAMS
#
# Usage: ./setup.sh [Debug|Release]
#   build_type: Release (default) or Debug
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

CONAN_ARGS=(-s "build_type=${BUILD_TYPE}" -b missing --update)

detect_version() {
  local bin="$1"
  if command -v "${bin}" >/dev/null 2>&1; then
    if "${bin}" -dumpfullversion >/dev/null 2>&1; then
      "${bin}" -dumpfullversion
    elif "${bin}" -dumpversion >/dev/null 2>&1; then
      "${bin}" -dumpversion
    else
      "${bin}" --version | head -n1 | grep -oE '[0-9]+(\\.[0-9]+)*' | head -n1
    fi
  fi
}

COERCE_MAJOR() {
  echo "$1" | cut -d. -f1
}

COMPILER_OVERRIDE=${YAMS_COMPILER:-}

if [[ "${COMPILER_OVERRIDE}" == clang ]] || { [[ -z "${COMPILER_OVERRIDE}" ]] && command -v clang >/dev/null 2>&1 && command -v clang++ >/dev/null 2>&1; }; then
  echo "--- Using Clang toolchain ---"
  export CC="clang"
  export CXX="clang++"
  export CXXFLAGS="${CXXFLAGS:-} -include cstdint"
  export CFLAGS="${CFLAGS:-} -include stdint.h"
  CLANG_VERSION=$(detect_version clang++)
  CLANG_MAJOR=$(COERCE_MAJOR "${CLANG_VERSION:-0}")
  if [[ -z "${CLANG_MAJOR}" || "${CLANG_MAJOR}" == 0 ]]; then
    echo "Unable to detect clang version." >&2
    exit 1
  fi
  CONAN_ARGS+=(
    -s "compiler=clang"
    -s "compiler.version=${CLANG_MAJOR}"
    -s "compiler.libcxx=libstdc++11"
    -s "compiler.cppstd=20"
  )
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
    -s "compiler.cppstd=20"
  )
fi

if [[ "${BUILD_TYPE}" == "Debug" ]]; then
  BUILD_DIR="builddir"
  CONAN_SUBDIR="build-debug"
else
  BUILD_DIR="build/${BUILD_TYPE_LOWER}"
  CONAN_SUBDIR="build-${BUILD_TYPE_LOWER}"
fi

echo "Build Type: ${BUILD_TYPE}"
echo "Build Dir:  ${BUILD_DIR}"

echo "--- Running conan install... ---"
conan install . -of "${BUILD_DIR}" "${CONAN_ARGS[@]}"

NATIVE_FILE="${BUILD_DIR}/${CONAN_SUBDIR}/conan/conan_meson_native.ini"

if [[ ! -f "${NATIVE_FILE}" ]]; then
  echo "Error: Conan native file not found at ${NATIVE_FILE}" >&2
  echo "Please check the output path from 'conan install'." >&2
  exit 1
fi

MESON_ARGS=(
  "${BUILD_DIR}"
  "--prefix" "/usr/local"
  "--native-file" "${NATIVE_FILE}"
  "--buildtype" "${BUILD_TYPE_LOWER}"
)

if [[ -f "${BUILD_DIR}/meson-info/meson-info.json" ]]; then
  MESON_ARGS+=("--reconfigure")
fi

MESON_OPTIONS=("-Dbuild-cli=true")

if [[ "${BUILD_TYPE}" == "Debug" ]]; then
  MESON_OPTIONS+=(
    "-Dbuild-tests=true"
    "-Denable-bench-tests=true"
  )
fi

echo "--- Running meson setup... ---"
meson setup "${MESON_ARGS[@]}" "${MESON_OPTIONS[@]}"

echo
echo "--- Setup complete! ---"
echo "To compile, run: meson compile -C ${BUILD_DIR}"
