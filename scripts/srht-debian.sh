#!/usr/bin/env bash
# Debian local runner for SourceHut .build.yml (trixie)
# - Mirrors the CI tasks using apt packages and Clang toolchain
# - Intended to run inside a Debian trixie container with your repo mounted at /workspace/yams
#
# Example:
#   docker run --rm -it \
#     -e GIT_REF=refs/tags/v1.2.3 \            # optional, enables packaging flow
#     -e CONAN_CPU_COUNT=2 \                    # optional, defaults to 2
#     -e CMAKE_BUILD_PARALLEL_LEVEL=2 \         # optional, defaults to 2
#     -v "$PWD":/workspace/yams \
#     -w /workspace \
#     debian:trixie bash -lc '/workspace/yams/scripts/srht-debian.sh'
#
# Artifacts:
#   - /workspace/yams/yams-*.tar.gz
#   - /workspace/yams/build/yams-release/*.deb
#   - /workspace/yams/build/yams-release/*.rpm
set -euo pipefail
set -x

export DEBIAN_FRONTEND=noninteractive

# 0) Base system and required packages (mirror .build.yml for Debian)
apt-get update -y
apt-get install -y --no-install-recommends \
  build-essential \
  cmake \
  ninja-build \
  zip \
  lld \
  clang \
  llvm \
  python3 \
  python3-venv \
  python3-pip \
  python3-numpy \
  pipx \
  git \
  ca-certificates \
  libncurses-dev \
  libtinfo-dev \
  pkg-config

# Ensure pipx-installed apps are on PATH
export PATH="${HOME}/.local/bin:${PATH}"

# Install Conan v2 via pipx, with venv fallback if needed
if ! command -v conan >/dev/null 2>&1; then
  if command -v pipx >/dev/null 2>&1; then
    pipx install 'conan<3' || true
  fi
fi
if ! command -v conan >/dev/null 2>&1; then
  python3 -m venv /opt/conan
  . /opt/conan/bin/activate
  python -m pip install --upgrade pip setuptools wheel
  python -m pip install 'conan<3'
  export PATH="/opt/conan/bin:${PATH}"
  hash -r
fi

# 1) Mirror manifest environment
#    - Persist to ~/.buildenv so subsequent shell subsessions see the variables,
#      similar to SR.ht’s behavior.
export BUILD_DIR="${BUILD_DIR:-build/yams-release}"
export STAGE_DIR="${STAGE_DIR:-stage}"
export CONAN_CPU_COUNT="${CONAN_CPU_COUNT:-2}"
export CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-2}"
: > "${HOME}/.buildenv"
{
  echo "export BUILD_DIR=${BUILD_DIR}"
  echo "export STAGE_DIR=${STAGE_DIR}"
  echo "export CONAN_CPU_COUNT=${CONAN_CPU_COUNT}"
  echo "export CMAKE_BUILD_PARALLEL_LEVEL=${CMAKE_BUILD_PARALLEL_LEVEL}"
} >> "${HOME}/.buildenv"

# 2) compute_version task
cd /workspace/yams
if [[ "${GIT_REF:-}" =~ ^refs/tags/ ]]; then
  RAW_TAG="${GIT_REF#refs/tags/}"
else
  RAW_TAG="$(git describe --tags --abbrev=0 2>/dev/null || echo 0.0.0-dev)"
fi
CLEAN_VERSION="${RAW_TAG#v}"
echo "export YAMS_VERSION=${CLEAN_VERSION}" >> "${HOME}/.buildenv"
echo "Computed YAMS_VERSION=${CLEAN_VERSION} (GIT_REF=${GIT_REF:-n/a})"

# Load the updated buildenv into current shell (SR.ht does this per task)
# shellcheck disable=SC1090
. "${HOME}/.buildenv"

# 3) setup task (prefer Clang toolchain)
export CC=clang
export CXX=clang++
echo "export CC=clang"  >> "${HOME}/.buildenv"
echo "export CXX=clang++" >> "${HOME}/.buildenv"

# Re-detect the default Conan profile with Clang toolchain in env
conan profile detect --force
# Ensure profile reflects our expectations
conan profile update settings.compiler=clang default || true
conan profile update settings.compiler.libcxx=libstdc++11 default || true
# Keep C++20
# Note: Conan v2 stores default profile at ~/.conan2/profiles/default
sed -i 's/^settings\.compiler\.cppstd=.*/settings.compiler.cppstd=20/' "${HOME}/.conan2/profiles/default" || true
echo "Conan profile configured (clang):"
conan profile show -pr default || conan profile show || true

# 4) build task
export YAMS_DISABLE_MODEL_PRELOAD=1
CLEAN_VERSION="${YAMS_VERSION}"
echo "Starting build for version ${CLEAN_VERSION}"

# Fetch dependencies and generate CMake toolchain/deps.
conan install . \
  -of "${BUILD_DIR}" \
  -pr:h=./conan/profiles/host.jinja \
  -s build_type=Release \
  -c tools.build:jobs="${CONAN_CPU_COUNT}" \
  --build=missing

# Configure with CMake (uses preset toolchain path under ${BUILD_DIR}).
cmake --preset yams-release --fresh \
  -DYAMS_VERSION="${CLEAN_VERSION}" \
  -DCURSES_NEED_NCURSES=ON \
  -DCURSES_NEED_WIDE=ON

# Build the project.
cmake --build --preset yams-release

# Install the built artifacts into the staging directory.
cmake --install "${BUILD_DIR}" --config Release

echo "Build complete."

# 5) package task (only on tag builds)
if [[ "${GIT_REF:-}" == refs/tags/* ]]; then
  cd /workspace/yams
  CLEAN_VERSION="${YAMS_VERSION}"
  echo "Packaging the release..."
  mkdir -p "${STAGE_DIR}"
  cd "${STAGE_DIR}"
  tar -czf "../yams-${CLEAN_VERSION}-linux-x86_64.tar.gz" .
  echo "Created asset: yams-${CLEAN_VERSION}-linux-x86_64.tar.gz"
else
  echo "Skipping packaging (not a tag build): GIT_REF=${GIT_REF:-n/a}"
fi

# 6) package_cpack task (only on tag builds)
if [[ "${GIT_REF:-}" == refs/tags/* ]]; then
  cd /workspace/yams
  CLEAN_VERSION="${YAMS_VERSION}"
  echo "Packaging with CPack..."
  cd "${BUILD_DIR}"

  # Ensure RPM tooling exists if we want RPM output
  if ! command -v rpmbuild >/dev/null 2>&1; then
    apt-get update -y
    apt-get install -y --no-install-recommends rpm
  fi

  # Generate DEB package
  cpack -G DEB -D CPACK_PACKAGE_VERSION="${CLEAN_VERSION}" || true
  # Generate RPM package
  cpack -G RPM -D CPACK_PACKAGE_VERSION="${CLEAN_VERSION}" || true
else
  echo "Skipping CPack packaging (not a tag build): GIT_REF=${GIT_REF:-n/a}"
fi

# Cleanup apt caches in ephemeral containers
apt-get clean
rm -rf /var/lib/apt/lists/* || true

echo "All tasks finished."
