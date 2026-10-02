#!/usr/bin/env bash
# yams/scripts/local-ci/package-validate.sh
#
# Install-validate the YAMS Linux packages (.deb, .rpm, Arch .pkg.tar.zst) the
# way a user would, in clean distro containers that boot systemd as PID 1.
# Single entry point for local runs (grim, any Linux box or Docker Desktop VM
# with Docker) and the release workflow's publish gate.
#
# Per distro lane, in a fresh container:
#   install  - package manager install, no env vars, no manual steps
#   layout   - installed file list against a runtime allowlist; no headers,
#              static libs, pkg-config or CMake files; no world-writable,
#              setuid or non-root files; every ELF's DT_NEEDED resolves and
#              RPATH/RUNPATH is $ORIGIN or /usr/lib[/yams] only
#   service  - unit enabled + active, runs as the `yams` account, socket
#              group `yams` and closed to others, `systemd-analyze verify`
#              clean, `systemd-analyze security` exposure under a budget,
#              corpus in /var/lib/yams and log in /var/log/yams
#   cli      - with no YAMS_* variables: root, a `yams`-group member (files
#              in $HOME and /tmp) and a non-member: status, add, search,
#              shared corpus, no private fallback store, clear error for a
#              directory the daemon cannot read, non-member denied,
#              `yams daemon stop` defers to systemd
#   persist  - container reboot: unit back, socket perms, data still there
#   remove   - uninstall stops/disables the unit; deb purge removes state
# With --upgrade-from-<fmt>, a second container installs that older package,
# stores a document, upgrades to the new package and checks the unit restarted
# onto the new binary with the document intact.
#
# Checks never stop at the first failure: each lane prints PASS/FAIL lines and
# a summary, and the script exits non-zero if any check failed.
#
# Usage:
#   package-validate.sh [--only LANES] [--deb F] [--rpm F] [--arch F]
#                       [--upgrade-from-deb F] [--upgrade-from-rpm F]
#                       [--upgrade-from-arch F] [--build-dir DIR]
#                       [--report FILE] [--keep]
#
#   LANES: debian, ubuntu, fedora, arch, or groups: deb (debian,ubuntu),
#   rpm (fedora), linux (deb+rpm, default), all (linux+arch); comma-separated.
#   Packages not given are discovered under --build-dir (default build/release).
#
# Examples:
#   bash scripts/local-ci/package-validate.sh --only all \
#     --deb yams-0.21.0-linux-x86_64.deb --rpm yams-0.21.0-linux-x86_64.rpm \
#     --arch yams-0.21.0-1-x86_64.pkg.tar.zst \
#     --upgrade-from-deb yams-0.20.3-linux-x86_64.deb
#
# Environment:
#   ARCH_DOCKER_PLATFORM / ARCH_BASE_IMAGE   Arch lane platform and base image
#   UBUNTU_IMAGE (ubuntu:24.04) DEBIAN_IMAGE (debian:trixie-slim)
#   FEDORA_IMAGE (fedora:42)
#   YAMS_VALIDATE_MAX_EXPOSURE (3.0)         systemd-analyze security budget
#
# Containers need --privileged and a private cgroup namespace (cgroup v2) so
# systemd can run units with their sandboxing; nothing touches host cgroups.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
CHECKS="${SCRIPT_DIR}/package-validate-checks.sh"
SUBSTRATE_DIR="${REPO_ROOT}/packaging/systemd"

ONLY="linux"
BUILD_DIR="${REPO_ROOT}/build/release"
DEB_PKG="" RPM_PKG="" ARCH_PKG=""
UP_DEB="" UP_RPM="" UP_ARCH=""
REPORT=""
KEEP=0

log() { printf '\033[1;34m[validate]\033[0m %s\n' "$*"; }
okmsg() { printf '\033[1;32m[ ok ]\033[0m %s\n' "$*"; }
err() { printf '\033[1;31m[fail]\033[0m %s\n' "$*" >&2; }
usage() { sed -n '2,58p' "${BASH_SOURCE[0]}"; }

while [ "$#" -gt 0 ]; do
	case "$1" in
	--only) ONLY="$2"; shift 2 ;;
	--build-dir) BUILD_DIR="$2"; shift 2 ;;
	--deb) DEB_PKG="$2"; shift 2 ;;
	--rpm) RPM_PKG="$2"; shift 2 ;;
	--arch) ARCH_PKG="$2"; shift 2 ;;
	--upgrade-from-deb) UP_DEB="$2"; shift 2 ;;
	--upgrade-from-rpm) UP_RPM="$2"; shift 2 ;;
	--upgrade-from-arch) UP_ARCH="$2"; shift 2 ;;
	--report) REPORT="$2"; shift 2 ;;
	--keep) KEEP=1; shift ;;
	-h | --help) usage; exit 0 ;;
	*) err "unknown argument: $1"; usage; exit 2 ;;
	esac
done

# Expand lane groups into an ordered, de-duplicated lane list.
LANES=()
add_lane() { case " ${LANES[*]-} " in *" $1 "*) ;; *) LANES+=("$1") ;; esac; }
IFS=',' read -r -a requested <<<"${ONLY}"
for item in "${requested[@]}"; do
	case "${item}" in
	all) add_lane debian; add_lane ubuntu; add_lane fedora; add_lane arch ;;
	linux) add_lane debian; add_lane ubuntu; add_lane fedora ;;
	deb) add_lane debian; add_lane ubuntu ;;
	rpm) add_lane fedora ;;
	debian | ubuntu | fedora | arch) add_lane "${item}" ;;
	*) err "unsupported lane: ${item}"; exit 2 ;;
	esac
done
[ "${#LANES[@]}" -gt 0 ] || { err "no lanes selected"; exit 2; }

command -v docker >/dev/null 2>&1 || { err "docker not found on PATH"; exit 2; }
[ -f "${CHECKS}" ] || { err "missing ${CHECKS}"; exit 2; }

discover_pkg() {
	[ -d "${BUILD_DIR}" ] || return 0
	{ find "${BUILD_DIR}" -maxdepth 4 -type f -name "$1" 2>/dev/null || true; } | LC_ALL=C sort | tail -n1
}
abs() { [ -n "$1" ] && printf '%s/%s' "$(cd "$(dirname "$1")" && pwd)" "$(basename "$1")"; }

[ -n "${DEB_PKG}" ] || DEB_PKG="$(discover_pkg 'yams-*.deb')"
[ -n "${RPM_PKG}" ] || RPM_PKG="$(discover_pkg 'yams-*.rpm')"
[ -n "${ARCH_PKG}" ] || ARCH_PKG="$(discover_pkg "yams-*-${ARCH_PKG_ARCH:-x86_64}.pkg.tar.zst")"

RUN_TAG="$$"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/yams-pkg-validate.XXXXXX")"
CONTAINERS=()
cleanup() {
	if [ "${KEEP}" -eq 0 ]; then
		for c in ${CONTAINERS[@]+"${CONTAINERS[@]}"}; do docker rm -f "${c}" >/dev/null 2>&1 || true; done
	else
		log "kept containers: ${CONTAINERS[*]+${CONTAINERS[*]}}"
	fi
	rm -rf "${WORK}"
}
trap cleanup EXIT

lane_fmt() { case "$1" in debian | ubuntu) echo deb ;; fedora) echo rpm ;; arch) echo arch ;; esac; }
lane_pkg() { case "$1" in debian | ubuntu) echo "${DEB_PKG}" ;; fedora) echo "${RPM_PKG}" ;; arch) echo "${ARCH_PKG}" ;; esac; }
lane_upgrade_from() { case "$1" in debian | ubuntu) echo "${UP_DEB}" ;; fedora) echo "${UP_RPM}" ;; arch) echo "${UP_ARCH}" ;; esac; }

# Docker platform for a lane (empty = native). Bash 3.2 compatible (macOS):
# no mapfile, and empty arrays are expanded with ${a[@]+"${a[@]}"}.
lane_platform() { if [ "$1" = arch ]; then printf '%s' "${ARCH_DOCKER_PLATFORM:-linux/amd64}"; fi; }

build_substrate() { # build_substrate <lane> -> image tag on stdout
	local lane="$1" image="yams/validate-$1:local" dockerfile
	local -a args=()
	case "${lane}" in
	debian) dockerfile=debian-lane.Dockerfile; args=(--build-arg "BASE_IMAGE=${DEBIAN_IMAGE:-debian:trixie-slim}") ;;
	ubuntu) dockerfile=debian-lane.Dockerfile; args=(--build-arg "BASE_IMAGE=${UBUNTU_IMAGE:-ubuntu:24.04}") ;;
	fedora) dockerfile=fedora-lane.Dockerfile; args=(--build-arg "BASE_IMAGE=${FEDORA_IMAGE:-fedora:42}") ;;
	arch)
		dockerfile=arch-lane.Dockerfile
		if [ -n "${ARCH_BASE_IMAGE:-}" ]; then args=(--build-arg "ARCH_BASE_IMAGE=${ARCH_BASE_IMAGE}"); fi
		;;
	esac
	local plat
	plat="$(lane_platform "${lane}")"
	docker build -q ${plat:+--platform="${plat}"} ${args[@]+"${args[@]}"} \
		-f "${SUBSTRATE_DIR}/${dockerfile}" -t "${image}" "${SUBSTRATE_DIR}" >/dev/null
	printf '%s' "${image}"
}

boot() { # boot <lane> <image> <name>
	local lane="$1" image="$2" name="$3"
	local plat
	plat="$(lane_platform "${lane}")"
	docker rm -f "${name}" >/dev/null 2>&1 || true
	docker run -d --name "${name}" ${plat:+--platform="${plat}"} --privileged --cgroupns=private \
		--tmpfs /run --tmpfs /run/lock --tmpfs /tmp "${image}" >/dev/null
	CONTAINERS+=("${name}")
	wait_boot "${name}"
}

wait_boot() {
	local name="$1" state=""
	# Up to 3 minutes: a reboot on a loaded host can sit in 'starting' for a while.
	for _ in $(seq 1 360); do
		state="$(docker exec "${name}" systemctl is-system-running 2>/dev/null || true)"
		case "${state}" in running | degraded) return 0 ;; esac
		sleep 0.5
	done
	err "${name}: systemd did not boot (state='${state}')"
	docker exec "${name}" systemctl list-jobs --no-pager 2>&1 | head -n 20 >&2 || true
	docker logs "${name}" 2>&1 | tail -n 20 >&2 || true
	return 1
}

reboot() {
	docker restart "$1" >/dev/null
	wait_boot "$1"
}

stage_files() { # stage_files <container> <file>...
	local c="$1"
	shift
	docker exec "${c}" mkdir -p /var/tmp/yams-validate
	docker cp "${CHECKS}" "${c}:/var/tmp/yams-validate/checks.sh"
	for f in "$@"; do
		if [ -n "${f}" ]; then docker cp "${f}" "${c}:/var/tmp/yams-validate/$(basename "${f}")"; fi
	done
}

run_phase() { # run_phase <lane> <container> <phase> [pkg]
	local lane="$1" c="$2" phase="$3" pkg="${4:-}" out="${WORK}/$1.results"
	local arg=""
	[ -n "${pkg}" ] && arg="/var/tmp/yams-validate/$(basename "${pkg}")"
	log "${lane}: ${phase}"
	docker exec "${c}" bash /var/tmp/yams-validate/checks.sh "${phase}" "$(lane_fmt "${lane}")" "${arg}" 2>&1 |
		sed "s/^/${lane} /" | tee -a "${out}" | while IFS= read -r line; do
		case "${line}" in
		*" PASS "*) printf '  \033[32m%s\033[0m\n' "${line#* }" ;;
		*" FAIL "*) printf '  \033[31m%s\033[0m\n' "${line#* }" ;;
		*) printf '  %s\n' "${line#* }" ;;
		esac
	done
}

note_fail() { printf '%s FAIL %s %s\n' "$1" "$2" "$3" >>"${WORK}/$1.results"; err "$1: $2 $3"; }

validate_lane() {
	local lane="$1" pkg upgrade_from image
	pkg="$(abs "$(lane_pkg "${lane}")")" || true
	upgrade_from="$(abs "$(lane_upgrade_from "${lane}")")" || true
	: >"${WORK}/${lane}.results"
	if [ -z "${pkg}" ] || [ ! -f "${pkg}" ]; then
		note_fail "${lane}" package-missing "no $(lane_fmt "${lane}") package (pass it or build into ${BUILD_DIR})"
		return
	fi
	log "${lane}: validating $(basename "${pkg}")"
	if ! image="$(build_substrate "${lane}")"; then
		note_fail "${lane}" substrate "failed to build substrate image"
		return
	fi

	local c="yams-validate-${lane}-${RUN_TAG}"
	if ! boot "${lane}" "${image}" "${c}"; then
		note_fail "${lane}" boot "systemd did not boot"
		return
	fi
	stage_files "${c}" "${pkg}"
	run_phase "${lane}" "${c}" install "${pkg}"
	run_phase "${lane}" "${c}" layout
	run_phase "${lane}" "${c}" service
	run_phase "${lane}" "${c}" cli
	log "${lane}: rebooting container"
	if reboot "${c}"; then
		run_phase "${lane}" "${c}" persist
	else
		note_fail "${lane}" reboot "container did not boot again"
	fi
	run_phase "${lane}" "${c}" remove
	docker rm -f "${c}" >/dev/null 2>&1 || true

	if [ -n "${upgrade_from}" ]; then
		if [ ! -f "${upgrade_from}" ]; then
			note_fail "${lane}" upgrade-from "missing ${upgrade_from}"
			return
		fi
		local u="yams-validate-${lane}-upgrade-${RUN_TAG}"
		if ! boot "${lane}" "${image}" "${u}"; then
			note_fail "${lane}" upgrade-boot "systemd did not boot"
			return
		fi
		stage_files "${u}" "${upgrade_from}" "${pkg}"
		log "${lane}: upgrade path $(basename "${upgrade_from}") -> $(basename "${pkg}")"
		run_phase "${lane}" "${u}" install "${upgrade_from}"
		run_phase "${lane}" "${u}" seed
		run_phase "${lane}" "${u}" install "${pkg}"
		run_phase "${lane}" "${u}" upgraded
		docker rm -f "${u}" >/dev/null 2>&1 || true
	fi
}

for lane in "${LANES[@]}"; do
	validate_lane "${lane}" || note_fail "${lane}" harness "lane aborted"
done

# Summary
OVERALL=0
SUMMARY="${WORK}/summary.txt"
{
	printf '%-8s %5s %5s  %s\n' LANE PASS FAIL FAILED-CHECKS
	for lane in "${LANES[@]}"; do
		f="${WORK}/${lane}.results"
		p="$(grep -c " PASS " "${f}" 2>/dev/null || true)"
		n="$(grep -c " FAIL " "${f}" 2>/dev/null || true)"
		failed="$(grep " FAIL " "${f}" 2>/dev/null | awk '{print $3}' | paste -sd, - || true)"
		printf '%-8s %5s %5s  %s\n' "${lane}" "${p:-0}" "${n:-0}" "${failed}"
		[ "${n:-0}" -eq 0 ] || OVERALL=1
	done
} >"${SUMMARY}"
echo
cat "${SUMMARY}"
if [ -n "${REPORT}" ]; then
	{
		cat "${SUMMARY}"
		echo
		cat "${WORK}"/*.results
	} >"${REPORT}"
fi
if [ "${OVERALL}" -eq 0 ]; then okmsg "all package validation lanes passed"; else err "package validation failed"; fi
exit "${OVERALL}"
