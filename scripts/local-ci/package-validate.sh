#!/usr/bin/env bash
# yams/scripts/local-ci/package-validate.sh
#
# Install the built Linux packages into clean systemd containers and smoke-test
# the yams-daemon service end to end:
#
#   1) Boot a clean distro container under /sbin/init (real systemd)
#   2) Install the package (.deb on Debian, .rpm on Fedora)
#   3) Assert the unit is enabled, becomes active, and the socket exists
#   4) Assert `yams --version` and `yams daemon status` work over the socket
#   5) Remove (and, for .deb, purge) and assert the service is torn down
#
# It does NOT build the packages. Build them first, e.g.:
#   bash scripts/build-deb.sh package_only <version> build/release stage
# (or via the full scripts/build-deb.sh run). The .rpm is cross-built on the
# Debian/Ubuntu builder; this script only validates installs.
#
# Requirements on host:
#   - Docker available on PATH, on a Linux host (or Docker Desktop's Linux VM)
#   - systemd-in-docker needs --privileged + cgroup access (handled below)
#
# Examples:
#   bash scripts/local-ci/package-validate.sh
#   bash scripts/local-ci/package-validate.sh --only deb
#   bash scripts/local-ci/package-validate.sh --deb path/to/yams.deb --rpm path/to/yams.rpm
#
# Notes:
#   - Substrate images come from packaging/systemd/{debian-lane,fedora-lane}.Dockerfile
#   - macOS native Docker cannot run systemd; rely on a Linux CI runner.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

BUILD_DIR_DEFAULT="${REPO_ROOT}/build/release"
DEBIAN_DOCKERFILE="${REPO_ROOT}/packaging/systemd/debian-lane.Dockerfile"
FEDORA_DOCKERFILE="${REPO_ROOT}/packaging/systemd/fedora-lane.Dockerfile"
SOCKET_PATH="/run/yams/yams-daemon.sock"

ONLY="all"
BUILD_DIR="${BUILD_DIR_DEFAULT}"
DEB_PKG=""
RPM_PKG=""

log()  { printf '\033[1;34m[validate]\033[0m %s\n' "$*"; }
ok()   { printf '\033[1;32m[ ok ]\033[0m %s\n' "$*"; }
fail() { printf '\033[1;31m[fail]\033[0m %s\n' "$*" >&2; }

usage() {
  sed -n '2,40p' "${BASH_SOURCE[0]}"
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --only) ONLY="$2"; shift 2 ;;
    --build-dir) BUILD_DIR="$2"; shift 2 ;;
    --deb) DEB_PKG="$2"; shift 2 ;;
    --rpm) RPM_PKG="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) fail "unknown argument: $1"; usage; exit 2 ;;
  esac
done

if ! command -v docker >/dev/null 2>&1; then
  fail "docker not found on PATH"
  exit 2
fi

discover_pkg() {
  local pattern="$1"
  find "${BUILD_DIR}" -maxdepth 1 -type f -name "${pattern}" 2>/dev/null | LC_ALL=C sort | tail -n1
}

[ -n "${DEB_PKG}" ] || DEB_PKG="$(discover_pkg 'yams-*.deb')"
[ -n "${RPM_PKG}" ] || RPM_PKG="$(discover_pkg 'yams-*.rpm')"

# Unique run tag so parallel invocations don't collide.
RUN_TAG="$$"

# Run a systemd container, install + smoke-test a package, then tear it down.
# Args: <name> <dockerfile> <image-tag> <pkg-path> <install-cmd> <remove-cmd> [purge-cmd]
validate_lane() {
  local name="$1" dockerfile="$2" image="$3" pkg="$4"
  local install_cmd="$5" remove_cmd="$6" purge_cmd="${7:-}"

  if [ -z "${pkg}" ] || [ ! -f "${pkg}" ]; then
    fail "${name}: package not found (looked in ${BUILD_DIR}); skip with --only or pass --deb/--rpm"
    return 1
  fi

  log "${name}: building substrate image from ${dockerfile#${REPO_ROOT}/}"
  docker build -f "${dockerfile}" -t "${image}" "${REPO_ROOT}/packaging/systemd" >/dev/null

  local container="yams-validate-${name}-${RUN_TAG}"
  docker rm -f "${container}" >/dev/null 2>&1 || true

  log "${name}: booting systemd container"
  docker run -d --name "${container}" \
    --privileged \
    --cgroupns=host \
    -v /sys/fs/cgroup:/sys/fs/cgroup:rw \
    --tmpfs /run --tmpfs /run/lock --tmpfs /tmp \
    "${image}" >/dev/null

  local rc=0
  # shellcheck disable=SC2064
  trap "docker rm -f '${container}' >/dev/null 2>&1 || true" RETURN

  # Wait for systemd to finish booting (running or degraded is acceptable).
  local state=""
  for _ in $(seq 1 60); do
    state="$(docker exec "${container}" systemctl is-system-running 2>/dev/null || true)"
    case "${state}" in
      running|degraded) break ;;
    esac
    sleep 0.5
  done
  case "${state}" in
    running|degraded) ok "${name}: systemd booted (${state})" ;;
    *) fail "${name}: systemd did not boot (state='${state}')"; return 1 ;;
  esac

  # Copy into /root (HOME), not /tmp: the container boots with --tmpfs /tmp,
  # which shadows a docker cp into that path.
  docker cp "${pkg}" "${container}:/root/$(basename "${pkg}")"

  log "${name}: installing $(basename "${pkg}")"
  if ! docker exec "${container}" bash -lc "${install_cmd}"; then
    fail "${name}: install failed"; return 1
  fi
  ok "${name}: installed"

  # Service must be enabled by the preset-driven maintainer scripts.
  if docker exec "${container}" systemctl is-enabled yams-daemon.service >/dev/null 2>&1; then
    ok "${name}: yams-daemon.service is enabled"
  else
    fail "${name}: yams-daemon.service is NOT enabled"; rc=1
  fi

  # Service must reach active state.
  local active=""
  for _ in $(seq 1 60); do
    active="$(docker exec "${container}" systemctl is-active yams-daemon.service 2>/dev/null || true)"
    [ "${active}" = "active" ] && break
    sleep 0.5
  done
  if [ "${active}" = "active" ]; then
    ok "${name}: yams-daemon.service is active"
  else
    fail "${name}: yams-daemon.service not active (state='${active}')"
    docker exec "${container}" journalctl -u yams-daemon.service --no-pager -n 50 2>/dev/null || true
    rc=1
  fi

  if docker exec "${container}" test -S "${SOCKET_PATH}"; then
    ok "${name}: socket present at ${SOCKET_PATH}"
  else
    fail "${name}: socket missing at ${SOCKET_PATH}"; rc=1
  fi

  if docker exec "${container}" yams --version >/dev/null 2>&1; then
    ok "${name}: yams --version works"
  else
    fail "${name}: yams --version failed"; rc=1
  fi

  if docker exec "${container}" env "YAMS_DAEMON_SOCKET=${SOCKET_PATH}" yams daemon status >/dev/null 2>&1; then
    ok "${name}: yams daemon status reachable over socket"
  else
    fail "${name}: yams daemon status failed over ${SOCKET_PATH}"; rc=1
  fi

  log "${name}: removing package"
  if ! docker exec "${container}" bash -lc "${remove_cmd}"; then
    fail "${name}: remove failed"; rc=1
  else
    if [ "$(docker exec "${container}" systemctl is-active yams-daemon.service 2>/dev/null || true)" = "active" ]; then
      fail "${name}: service still active after remove"; rc=1
    else
      ok "${name}: service stopped after remove"
    fi
  fi

  if [ -n "${purge_cmd}" ]; then
    log "${name}: purging package"
    if ! docker exec "${container}" bash -lc "${purge_cmd}"; then
      fail "${name}: purge failed"; rc=1
    else
      ok "${name}: purged"
    fi
  fi

  return "${rc}"
}

OVERALL=0

if [ "${ONLY}" = "all" ] || [ "${ONLY}" = "deb" ]; then
  deb_base="$(basename "${DEB_PKG:-yams.deb}")"
  # Remove the Docker base image's policy-rc.d (exit 101), which blocks service
  # auto-start during apt install. A real Debian/Ubuntu host has no such file, so
  # removing it lets the package's postinst exercise its genuine enable+start path.
  validate_lane "debian" "${DEBIAN_DOCKERFILE}" "yams/validate-debian:trixie" "${DEB_PKG}" \
    "rm -f /usr/sbin/policy-rc.d; apt-get update >/dev/null 2>&1 || true; apt-get install -y /root/${deb_base} && dpkg -s yams >/dev/null" \
    "apt-get remove -y yams" \
    "apt-get purge -y yams" || OVERALL=1
fi

if [ "${ONLY}" = "all" ] || [ "${ONLY}" = "rpm" ]; then
  rpm_base="$(basename "${RPM_PKG:-yams.rpm}")"
  validate_lane "fedora" "${FEDORA_DOCKERFILE}" "yams/validate-fedora:42" "${RPM_PKG}" \
    "dnf install -y /root/${rpm_base}" \
    "dnf remove -y yams" \
    "" || OVERALL=1
fi

if [ "${OVERALL}" -eq 0 ]; then
  ok "all package validation lanes passed"
else
  fail "one or more package validation lanes failed"
fi
exit "${OVERALL}"
