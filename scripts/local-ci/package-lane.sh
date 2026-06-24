#!/usr/bin/env bash
# yams/scripts/local-ci/package-lane.sh
#
# One-command local lane: build the Linux packages in a cached Docker builder,
# then install + smoke-test them in clean systemd containers. This is the local
# equivalent of the release CI package job.
#
#   build  : run scripts/build-deb.sh inside docker/local-ci/debian-package.Dockerfile
#            -> produces yams-*.deb and yams-*.rpm into the build dir
#   validate: run scripts/local-ci/package-validate.sh against those artifacts
#
# Requirements on host:
#   - Docker on PATH, on a Linux host (or Docker Desktop's Linux VM).
#   - The validate phase needs privileged systemd-in-docker (cgroups). On macOS
#     Docker Desktop the build phase works; the validate phase may not boot
#     systemd -- use --build-only there and run validate on a Linux runner.
#
# Examples:
#   bash scripts/local-ci/package-lane.sh                 # build + validate (deb + rpm)
#   bash scripts/local-ci/package-lane.sh --only deb      # deb lane only
#   bash scripts/local-ci/package-lane.sh --build-only    # just produce packages
#   bash scripts/local-ci/package-lane.sh --validate-only # reuse existing packages
#   bash scripts/local-ci/package-lane.sh --no-cache      # rebuild the builder image

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

IMAGE="yams/ci-debian-package:trixie"
DOCKERFILE="${REPO_ROOT}/docker/local-ci/debian-package.Dockerfile"
BUILD_DIR="build/release-pkg"   # relative to repo root; kept separate from dev build/
ONLY="all"
DO_BUILD=1
DO_VALIDATE=1
NO_CACHE=0
VERSION=""

log()  { printf '\033[1;34m[lane]\033[0m %s\n' "$*"; }
fail() { printf '\033[1;31m[fail]\033[0m %s\n' "$*" >&2; }

usage() { sed -n '2,30p' "${BASH_SOURCE[0]}"; }

while [ "$#" -gt 0 ]; do
  case "$1" in
    --only) ONLY="$2"; shift 2 ;;
    --build-dir) BUILD_DIR="$2"; shift 2 ;;
    --version) VERSION="$2"; shift 2 ;;
    --build-only) DO_VALIDATE=0; shift ;;
    --validate-only) DO_BUILD=0; shift ;;
    --no-cache) NO_CACHE=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) fail "unknown argument: $1"; usage; exit 2 ;;
  esac
done

command -v docker >/dev/null 2>&1 || { fail "docker not found on PATH"; exit 2; }

# Resolve a version for the build (build-deb.sh packages only for tag/channel
# builds, so the build phase passes RELEASE_CHANNEL=nightly to force packaging).
if [ -z "${VERSION}" ]; then
  VERSION="$(git -C "${REPO_ROOT}" describe --tags --abbrev=7 2>/dev/null || echo 0.0.0-dev)"
fi

ABS_BUILD_DIR="${BUILD_DIR}"
case "${ABS_BUILD_DIR}" in
  /*) : ;;
  *) ABS_BUILD_DIR="${REPO_ROOT}/${BUILD_DIR}" ;;
esac

if [ "${DO_BUILD}" -eq 1 ]; then
  log "building cached builder image ${IMAGE}"
  build_args=(-f "${DOCKERFILE}" -t "${IMAGE}" "${REPO_ROOT}")
  [ "${NO_CACHE}" -eq 1 ] && build_args+=(--no-cache)
  docker build "${build_args[@]}"

  log "building packages (version=${VERSION}) into ${BUILD_DIR}"
  mkdir -p "${ABS_BUILD_DIR}"
  # Run the real build inside the container; chown artifacts back to the host
  # user so the build dir isn't left root-owned on the bind mount.
  # Persist the Conan cache across runs so dependencies aren't rebuilt from
  # source every time (the container itself is --rm). BASE_VERSION is left unset
  # so build-deb.sh derives a digit-leading version (dpkg requires that).
  docker run --rm \
    -v "${REPO_ROOT}:/workspace/yams" \
    -v yams-conan-cache:/root/.conan2 \
    -w /workspace \
    -e RELEASE_CHANNEL=nightly \
    -e GIT_REF="" \
    -e BUILD_DIR="${BUILD_DIR}" \
    -e MESON_EXTRA_ARGS="${MESON_EXTRA_ARGS:-}" \
    -e CONAN_EXTRA_OPTS="${CONAN_EXTRA_OPTS:-}" \
    -e HOST_UID="$(id -u)" \
    -e HOST_GID="$(id -g)" \
    "${IMAGE}" \
    bash -lc '/workspace/yams/scripts/build-deb.sh; rc=$?; chown -R "${HOST_UID}:${HOST_GID}" "/workspace/yams/'"${BUILD_DIR}"'" 2>/dev/null || true; exit $rc'

  log "built artifacts:"
  find "${ABS_BUILD_DIR}" -maxdepth 1 -type f \( -name 'yams-*.deb' -o -name 'yams-*.rpm' \) -print
fi

if [ "${DO_VALIDATE}" -eq 1 ]; then
  log "validating packages from ${BUILD_DIR}"
  bash "${SCRIPT_DIR}/package-validate.sh" --build-dir "${ABS_BUILD_DIR}" --only "${ONLY}"
fi

log "done"
