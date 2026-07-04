#!/usr/bin/env bash
# Arch Linux package builder for YAMS.
#
# Builds yams inside an Arch Linux container (or native Arch host), produces
# a valid ALPM .pkg.tar.zst package, and optionally builds a pacman repository
# database with repo-add.
#
# Entry points:
#   provision           Install build dependencies (run once to pre-bake image)
#   build               Full build + package + repo (default)
#   package_only <ver> <build_dir> <stage_dir>  Package from pre-built stage
#
# Environment:
#   RELEASE_CHANNEL     stable, nightly, weekly (default: nightly)
#   BUILD_DIR           build output directory (default: build/release-pkg)
#   MESON_EXTRA_ARGS    extra meson configure flags
#   CONAN_EXTRA_OPTS    extra conan options
#   GIT_REF             tag/ref for version (default: auto from git describe)
#
# Artifacts:
#   - build/<dir>/yams-<pkgver>-<pkgrel>-<arch>.pkg.tar.zst
#   - build/<dir>/archrepo/os/<arch>/yams-<pkgver>-<pkgrel>-<arch>.pkg.tar.zst
#   - build/<dir>/archrepo/os/<arch>/yams.db
#   - build/<dir>/archrepo/os/<arch>/yams.files
#
# Example:
#   docker run --rm \
#     -v "$PWD:/workspace/yams" \
#     -w /workspace \
#     -e RELEASE_CHANNEL=nightly \
#     yams/ci-arch-package:latest \
#     bash -lc '/workspace/yams/scripts/build-arch-pkg.sh'
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

export PATH="${HOME}/.local/bin:/opt/venv/bin:${PATH}"

BUILD_DIR="${BUILD_DIR:-build/release-pkg}"
# Keep package builds separate from dev builds to avoid Conan cache collisions
PKG_BUILD_DIR="${BUILD_DIR}"
CHANNEL="${RELEASE_CHANNEL:-nightly}"
STAGE_DIR=""
VERSION=""
HOST_UID="${HOST_UID:-}"
HOST_GID="${HOST_GID:-}"

log() { printf '\033[1;34m[arch-pkg]\033[0m %s\n' "$*"; }
fail() {
	printf '\033[1;31m[arch-pkg]\033[0m %s\n' "$*" >&2
	exit 1
}

usage() {
	sed -n '2,28p' "${BASH_SOURCE[0]}"
}

# ── provisioning ──────────────────────────────────────────────────────────

provision_main() {
	log "Provisioning Arch build toolchain"
	if ! command -v pacman >/dev/null 2>&1; then
		fail "pacman not available — this script expects an Arch Linux environment"
	fi

	pacman-key --init 2>/dev/null || true
	pacman-key --populate archlinux 2>/dev/null || true
	pacman -Syu --noconfirm --needed
	pacman -S --noconfirm --needed \
		base-devel \
		clang \
		cmake \
		meson \
		ninja \
		python \
		python-pip \
		ccache \
		git \
		zip \
		pkgconf \
		liburing \
		libarchive \
		taglib \
		sqlite \
		openssl \
		curl \
		protobuf \
		boost \
		lld \
		fakeroot

	yes | pacman -Scc 2>/dev/null || true

	# Conan via pip (not in Arch repos at a stable version)
	if ! command -v conan >/dev/null 2>&1; then
		python -m venv /opt/venv 2>/dev/null || true
		/opt/venv/bin/pip install --no-cache-dir 'conan<3'
		/opt/venv/bin/pip install --no-cache-dir --upgrade meson ninja
	fi

	log "Arch toolchain provisioned"
}

# ── helpers ───────────────────────────────────────────────────────────────

resolve_version() {
	local v
	if [ -n "${GIT_REF:-}" ]; then
		v="${GIT_REF#refs/tags/}"
		v="${v#yams-v}"
		v="${v#v}"
	elif [ -n "${VERSION}" ]; then
		v="${VERSION}"
	else
		v="$(cd "${REPO_ROOT}" && git describe --tags --abbrev=7 2>/dev/null || echo "0.0.0")"
		v="${v#yams-v}"
		v="${v#v}"
	fi
	echo "${v}"
}

arch_label() {
	case "$(uname -m)" in
	x86_64) echo "x86_64" ;;
	aarch64 | arm64) echo "aarch64" ;;
	armv7l) echo "armv7h" ;;
	*) uname -m ;;
	esac
}

normalized_pkgver() {
	printf '%s\n' "${1//-/_}"
}

package_release() {
	printf '%s\n' "${YAMS_ARCH_PKGREL:-1}"
}

package_filename() {
	local raw_version="$1"
	local pkg_arch="$2"
	printf 'yams-%s-%s-%s.pkg.tar.zst\n' \
		"$(normalized_pkgver "${raw_version}")" \
		"$(package_release)" \
		"${pkg_arch}"
}

write_pkg_metadata() {
	local package_root="$1"
	local raw_version="$2"
	local pkg_arch="$3"
	local normalized_version pkgrel builddate install_size packager mtree_input

	normalized_version="$(normalized_pkgver "${raw_version}")"
	pkgrel="$(package_release)"
	builddate="$(date -u +%s)"
	install_size="$(du -sk "${package_root}" | awk '{print $1 * 1024}')"
	packager="${PACKAGER:-YAMS Contributors <git@trevon.dev>}"

	cat >"${package_root}/.PKGINFO" <<EOF
pkgname = yams
pkgbase = yams
pkgver = ${normalized_version}-${pkgrel}
pkgdesc = Yet Another Memory System — persistent content-addressed memory for LLMs
url = https://github.com/trvon/yams
builddate = ${builddate}
packager = ${packager}
size = ${install_size}
arch = ${pkg_arch}
license = Apache-2.0
depend = gcc-libs
depend = glibc
depend = liburing
depend = libarchive
depend = taglib
depend = sqlite
depend = openssl
depend = curl
depend = boost-libs
optdepend = onnxruntime: ONNX-based embedding acceleration
EOF

	cat >"${package_root}/.BUILDINFO" <<EOF
format = 2
pkgname = yams
pkgbase = yams
pkgver = ${normalized_version}-${pkgrel}
pkgarch = ${pkg_arch}
pkgbuild_sha256sum = SKIP
packager = ${packager}
builddate = ${builddate}
builddir = ${REPO_ROOT}
startdir = ${REPO_ROOT}
buildtool = custom-stage-packager
buildtoolver = 1
EOF

	if [ -f "${REPO_ROOT}/packaging/arch/yams.install" ]; then
		install -Dm644 "${REPO_ROOT}/packaging/arch/yams.install" "${package_root}/.INSTALL"
	fi

	if command -v bsdtar >/dev/null 2>&1; then
		mtree_input=".MTREE.input"
		(
			cd "${package_root}"
			LC_ALL=C find . -mindepth 1 ! -name '.MTREE' -print0 >"${mtree_input}"
			xargs -0 bsdtar -czf .MTREE --format=mtree \
				--options='!all,use-set,type,uid,gid,mode,time,size,md5,sha256,link' \
				<"${mtree_input}"
			rm -f "${mtree_input}"
		)
	fi
}

build_arch_package_from_stage() {
	local stage_root="$1"
	local work_dir="$2"
	local raw_version="$3"
	local pkg_arch="$4"
	local package_root pkg_file package_path

	mkdir -p "${work_dir}"
	work_dir="$(cd "${work_dir}" && pwd)"
	package_root="${work_dir}/package-root"
	pkg_file="$(package_filename "${raw_version}" "${pkg_arch}")"
	package_path="${work_dir}/${pkg_file}"

	rm -rf "${package_root}" "${package_path}"
	mkdir -p "${package_root}"
	cp -a "${stage_root}/." "${package_root}/"
	write_pkg_metadata "${package_root}" "${raw_version}" "${pkg_arch}"

	(
		cd "${package_root}"
		bsdtar -cf - . | zstd -T0 -19 -o "${package_path}"
	)

	printf '%s\n' "${package_path}"
}

# ── build ─────────────────────────────────────────────────────────────────

build_main() {
	VERSION="$(resolve_version)"
	local arch pkg_arch meson_build_dir stage_root work_dir package_path pkg_file
	arch="$(arch_label)"
	pkg_arch="${arch}"
	meson_build_dir="${YAMS_BUILD_DIR:-${PKG_BUILD_DIR}/meson-${pkg_arch}}"

	log "Building YAMS ${VERSION} for arch ${arch} (channel=${CHANNEL})"
	log "Using Arch Meson build dir: ${meson_build_dir}"

	cd "${REPO_ROOT}"

	# Configure and build with Meson + Conan (same flow as Linux CI)
	export YAMS_CONAN_HOST_PROFILE="./conan/profiles/host-linux-clang"
	export YAMS_CONAN_ARCH="${arch}"
	export YAMS_BUILD_DIR="${meson_build_dir}"
	export YAMS_ENABLE_MOBILE_BINDINGS=false
	export YAMS_EXTRA_MESON_FLAGS="-Dyams-version=${VERSION} -Dwerror=false -Dwarning_level=2 -Denable-lzma=auto ${MESON_EXTRA_ARGS:-}"
	export YAMS_INSTALL_PREFIX="/usr"
	export CC=clang
	export CXX=clang++

	bash ./setup.sh Release

	# Source Conan build env
	local conan_env=""
	for candidate in \
		"${YAMS_BUILD_DIR}/build-release/conan/conanbuild.sh" \
		"${YAMS_BUILD_DIR}/conan/conanbuild.sh"; do
		if [ -f "$candidate" ]; then
			conan_env="$candidate"
			break
		fi
	done
	if [ -n "$conan_env" ]; then
		set +u
		source "$conan_env"
		set -u
	fi

	meson compile -C "${YAMS_BUILD_DIR}"

	# Stage install. Meson interprets a relative --destdir from the build directory,
	# so keep this absolute; otherwise the package step can archive an empty
	# repo-root stage while the real payload lands under ${YAMS_BUILD_DIR}/...
	case "${PKG_BUILD_DIR}" in
	/*) stage_root="${PKG_BUILD_DIR}/stage" ;;
	*) stage_root="${REPO_ROOT}/${PKG_BUILD_DIR}/stage" ;;
	esac
	rm -rf "${stage_root}"
	meson install -C "${YAMS_BUILD_DIR}" --destdir "${stage_root}" --no-rebuild

	# Prune development payload
	bash scripts/prune-runtime-install.sh "${stage_root}/usr" 2>/dev/null || true

	# Install systemd unit + preset into stage.
	install -Dm644 packaging/systemd/yams-daemon.service \
		"${stage_root}/usr/lib/systemd/system/yams-daemon.service"
	install -Dm644 packaging/systemd/80-yams.preset \
		"${stage_root}/usr/lib/systemd/system-preset/80-yams.preset"

	log "Assembling Arch package from staged install"
	work_dir="${PKG_BUILD_DIR}/arch-work"
	rm -rf "${work_dir}"
	mkdir -p "${work_dir}"

	package_path="$(build_arch_package_from_stage "${stage_root}" "${work_dir}" "${VERSION}" "${pkg_arch}")"
	pkg_file="$(basename "${package_path}")"

	mkdir -p "${REPO_ROOT}/${PKG_BUILD_DIR}"
	cp "${package_path}" "${REPO_ROOT}/${PKG_BUILD_DIR}/"
	cd "${REPO_ROOT}"

	# Chown artifacts back to host user if running in Docker
	if [ -n "${HOST_UID:-}" ] && [ -n "${HOST_GID:-}" ] && [ "${HOST_UID}" != "0" ]; then
		chown -R "${HOST_UID}:${HOST_GID}" "${PKG_BUILD_DIR}" 2>/dev/null || true
	fi

	log "Created Arch package: ${PKG_BUILD_DIR}/${pkg_file}"

	# ── repo metadata ────────────────────────────────────────────────────
	build_repo_metadata "${pkg_arch}"

	echo "ARCH_PKG=${PKG_BUILD_DIR}/${pkg_file}" >>"${GITHUB_ENV:-/dev/null}"
}

# ── package_only ──────────────────────────────────────────────────────────

package_only() {
	VERSION="${1:-}"
	local build_dir="${2:-}"
	local stage_dir="${3:-}"
	local pkg_arch work_dir package_path pkg_file

	if [ -z "${VERSION}" ] || [ -z "${build_dir}" ] || [ -z "${stage_dir}" ]; then
		fail "usage: package_only <version> <build_dir> <stage_dir>"
	fi

	pkg_arch="$(arch_label)"
	PKG_BUILD_DIR="${build_dir}"
	work_dir="${build_dir}/.arch-package-work"
	mkdir -p "${build_dir}"

	log "Packaging staged install at ${stage_dir} -> ${build_dir}"
	package_path="$(build_arch_package_from_stage "${stage_dir}" "${work_dir}" "${VERSION}" "${pkg_arch}")"
	pkg_file="$(basename "${package_path}")"
	cp "${package_path}" "${build_dir}/${pkg_file}"
	rm -rf "${work_dir}"

	log "Created Arch package: ${build_dir}/${pkg_file}"

	build_repo_metadata "${pkg_arch}"
}

# ── repo metadata ─────────────────────────────────────────────────────────

build_repo_metadata() {
	local pkg_arch="${1:-$(arch_label)}"
	local repo_dir="${REPO_ROOT}/${PKG_BUILD_DIR}/archrepo"
	local arch_dir="${repo_dir}/os/${pkg_arch}"
	local usage_file

	mkdir -p "${arch_dir}"

	# Collect all matching .pkg.tar.zst files into the standard Arch repo layout.
	find "${REPO_ROOT}/${PKG_BUILD_DIR}" -maxdepth 1 -name "yams-*-${pkg_arch}.pkg.tar.zst" \
		-exec cp -f {} "${arch_dir}/" \;

	# Build pacman database with repo-add in the same directory pacman will fetch from.
	if command -v repo-add >/dev/null 2>&1; then
		(
			cd "${arch_dir}"
			shopt -s nullglob
			packages=(*.pkg.tar.zst)
			if [ ${#packages[@]} -eq 0 ]; then
				log "No Arch packages found under ${arch_dir}; skipping repo-add"
				exit 0
			fi
			rm -f yams.db yams.db.tar.gz yams.files yams.files.tar.gz
			repo-add -q yams.db.tar.gz "${packages[@]}"
			ln -sf yams.db.tar.gz yams.db
			ln -sf yams.files.tar.gz yams.files
		)
	else
		log "repo-add not available; skipping pacman database generation"
	fi

	usage_file="${repo_dir}/USAGE.txt"
	cat >"${usage_file}" <<'EOF'
Add Arch repo:
  sudo tee /etc/pacman.d/yams.conf <<'REPO'
  [yams]
  SigLevel = Optional TrustAll
  Server = https://repo.yamsmemory.ai/archrepo/os/$arch
  REPO

  sudo tee -a /etc/pacman.conf <<'REPO'
  Include = /etc/pacman.d/yams.conf
  REPO

  sudo pacman -Sy yams
EOF

	log "Arch repo metadata generated at ${arch_dir}"
}

# ── dispatch ──────────────────────────────────────────────────────────────

case "${1:-build}" in
provision)
	provision_main
	;;
build)
	build_main
	;;
package_only)
	shift
	package_only "$@"
	;;
-h | --help)
	usage
	exit 0
	;;
*)
	fail "unknown command: ${1:-}"
	usage
	exit 2
	;;
esac
