#!/usr/bin/env bash
# kronos/scripts/local-ci/matrix.sh
#
# Simulate the Ubuntu GitHub Actions job in a Docker container.
#
# What it does inside the container:
#   1) Configure, build, and test YAMS
#   2) Install into a local prefix (inside the mounted repo)
#   3) Configure, build, and test a minimal downstream consumer against find_package(Yams)
#
# Requirements on host:
#   - Docker available on PATH
#   - This repository checked out locally
#
# Example:
#   bash scripts/local-ci/matrix.sh
#   bash scripts/local-ci/matrix.sh --cmake-args "-DYAMS_ENABLE_COVERAGE=ON"
#
# Notes:
#   - The container image is built from docker/local-ci/ubuntu.Dockerfile (Ubuntu 24.04)
#   - macOS jobs cannot be containerized; this script targets Linux (Ubuntu) CI parity.
#   - EXTRA CMake args are passed via strings and expanded with 'eval' inside the container.
#     Only pass trusted content.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Defaults
IMAGE_DEFAULT="yams/ci-ubuntu:24.04"
DOCKERFILE_DEFAULT="${REPO_ROOT}/docker/local-ci/ubuntu.Dockerfile"
REPO_DIR_DEFAULT="${REPO_ROOT}"
GENERATOR_DEFAULT="Ninja"
BUILD_TYPE_DEFAULT="Release"
INSTALL_PREFIX_DEFAULT="/work/prefix"
JOBS_DEFAULT=""
# resolved inside container via nproc if empty
EXTRA_CMAKE_ARGS_DEFAULT=""
# e.g. '-DYAMS_USE_VCPKG=ON'
CONSUMER_CMAKE_ARGS_DEFAULT=""
# e.g. '-DCMAKE_VERBOSE_MAKEFILE=ON'
PULL_FLAG=""

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

Options:
  -r, --repo PATH                  Path to repository root (default: ${REPO_DIR_DEFAULT})
  -i, --image NAME:TAG            Docker image tag to use (default: ${IMAGE_DEFAULT})
  -f, --dockerfile PATH           Path to Dockerfile for local CI image (default: ${DOCKERFILE_DEFAULT})
      --skip-build-image          Do not build the local CI Docker image (use existing)
      --pull                      Force pulling base layers when building the image

  -g, --generator NAME            CMake generator (default: ${GENERATOR_DEFAULT})
  -b, --build-type TYPE           CMake build type (default: ${BUILD_TYPE_DEFAULT})
  -p, --prefix PATH               Install prefix inside container (default: ${INSTALL_PREFIX_DEFAULT})
  -j, --jobs N                    Parallel build jobs (default: autodetect in container)
      --cmake-args "ARGS"         Extra args for main project configure (quoted string)
      --consumer-cmake-args "ARGS" Extra args for consumer configure (quoted string)

  -h, --help                      Show this help

Examples:
  $(basename "$0")
  $(basename "$0") --cmake-args "-DYAMS_BUILD_TESTS=ON"
  $(basename "$0") -j 8 --consumer-cmake-args "-DCMAKE_VERBOSE_MAKEFILE=ON"
EOF
}

REPO_DIR="${REPO_DIR_DEFAULT}"
IMAGE="${IMAGE_DEFAULT}"
DOCKERFILE="${DOCKERFILE_DEFAULT}"
SKIP_BUILD="0"
GENERATOR="${GENERATOR_DEFAULT}"
BUILD_TYPE="${BUILD_TYPE_DEFAULT}"
INSTALL_PREFIX="${INSTALL_PREFIX_DEFAULT}"
JOBS="${JOBS_DEFAULT}"
EXTRA_CMAKE_ARGS="${EXTRA_CMAKE_ARGS_DEFAULT}"
CONSUMER_CMAKE_ARGS="${CONSUMER_CMAKE_ARGS_DEFAULT}"

# Parse args
while [[ $# -gt 0 ]]; do
  case "$1" in
    -r|--repo)
      REPO_DIR="${2:-}"; shift 2;;
    -i|--image)
      IMAGE="${2:-}"; shift 2;;
    -f|--dockerfile)
      DOCKERFILE="${2:-}"; shift 2;;
    --skip-build-image)
      SKIP_BUILD="1"; shift;;
    --pull)
      PULL_FLAG="--pull"; shift;;
    -g|--generator)
      GENERATOR="${2:-}"; shift 2;;
    -b|--build-type)
      BUILD_TYPE="${2:-}"; shift 2;;
    -p|--prefix)
      INSTALL_PREFIX="${2:-}"; shift 2;;
    -j|--jobs)
      JOBS="${2:-}"; shift 2;;
    --cmake-args)
      EXTRA_CMAKE_ARGS="${2:-}"; shift 2;;
    --consumer-cmake-args)
      CONSUMER_CMAKE_ARGS="${2:-}"; shift 2;;
    -h|--help)
      usage; exit 0;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 2;;
  esac
done

# Basic checks
if ! command -v docker >/dev/null 2>&1; then
  echo "Error: Docker not found on PATH" >&2
  exit 1
fi

if [[ ! -d "${REPO_DIR}" ]]; then
  echo "Error: Repo directory not found: ${REPO_DIR}" >&2
  exit 1
fi

if [[ "${SKIP_BUILD}" != "1" ]]; then
  if [[ ! -f "${DOCKERFILE}" ]]; then
    echo "Error: Dockerfile not found: ${DOCKERFILE}" >&2
    exit 1
  fi
  echo "==> Building local CI Docker image: ${IMAGE}"
  docker build ${PULL_FLAG} -t "${IMAGE}" -f "${DOCKERFILE}" "${REPO_DIR}"
else
  echo "==> Skipping image build. Using existing image: ${IMAGE}"
fi

# Resolve absolute repo path for mounting
REPO_ABS="$(cd "${REPO_DIR}" && pwd)"

echo "==> Running CI simulation in container"
echo "    Repo:           ${REPO_ABS}"
echo "    Image:          ${IMAGE}"
echo "    Generator:      ${GENERATOR}"
echo "    Build type:     ${BUILD_TYPE}"
echo "    Install prefix: ${INSTALL_PREFIX}"
echo "    Jobs:           ${JOBS:-'(auto)'}"
echo "    Extra CMake:    ${EXTRA_CMAKE_ARGS:-'(none)'}"
echo "    Consumer CMake: ${CONSUMER_CMAKE_ARGS:-'(none)'}"

# Run CI steps in container
# Pass configuration via environment variables to avoid quoting issues.
docker run --rm \
  -v "${REPO_ABS}:/work:rw" \
  -w /work \
  -e GEN="${GENERATOR}" \
  -e BUILD_TYPE="${BUILD_TYPE}" \
  -e INSTALL_PREFIX="${INSTALL_PREFIX}" \
  -e JOBS="${JOBS}" \
  -e EXTRA_CMAKE_ARGS="${EXTRA_CMAKE_ARGS}" \
  -e CONSUMER_EXTRA_CMAKE_ARGS="${CONSUMER_CMAKE_ARGS}" \
  "${IMAGE}" bash -lc '
    set -euxo pipefail

    # Determine jobs inside container if not provided
    if [[ -z "${JOBS:-}" ]]; then
      if command -v nproc >/dev/null 2>&1; then
        JOBS="$(nproc)"
      else
        JOBS="4"
      fi
    fi

    # 1) Clean previous artifacts
    rm -rf build consumer_build "${INSTALL_PREFIX}" || true
    mkdir -p "${INSTALL_PREFIX}"

    # 2) Configure main project
    # Use eval to allow EXTRA_CMAKE_ARGS to inject -D definitions and other flags.
    eval cmake -S . -B build -G \"${GEN}\" \
      -DCMAKE_BUILD_TYPE=\"${BUILD_TYPE}\" \
      -DYAMS_BUILD_PROFILE=dev \
      -DYAMS_BUILD_TESTS=ON \
      -DCMAKE_INSTALL_PREFIX=\"${INSTALL_PREFIX}\" \
      ${EXTRA_CMAKE_ARGS}

    # 3) Build & test main project
    cmake --build build -j "${JOBS}"
    ctest --test-dir build --output-on-failure

    # 4) Install main project (headers + libs + package config)
    cmake --install build

    # 5) Configure consumer project against installed prefix (if present)
    if [[ -d test/consumer ]]; then
      eval cmake -S test/consumer -B consumer_build -G \"${GEN}\" \
        -DCMAKE_PREFIX_PATH=\"${INSTALL_PREFIX}\" \
        ${CONSUMER_EXTRA_CMAKE_ARGS}

      # 6) Build & test consumer
      cmake --build consumer_build -j "${JOBS}"
      ctest --test-dir consumer_build --output-on-failure
    else
      echo "Skipping downstream consumer: directory 'test/consumer' not found."
    fi

    echo "Container CI steps completed successfully."
  '

echo "==> Local containerized CI completed successfully."
