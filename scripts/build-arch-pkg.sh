#!/usr/bin/env bash
# Arch Linux package builder for YAMS.
#
# Builds yams inside an Arch Linux container (or native Arch host), produces
# .pkg.tar.zst via makepkg, and optionally builds a pacman repository database
# with repo-add.
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
#   - build/<dir>/yams-<version>-<arch>.pkg.tar.zst
#   - build/<dir>/archrepo/<arch>/yams-<version>-<arch>.pkg.tar.zst
#   - build/<dir>/archrepo/yams.db
#   - build/<dir>/archrepo/yams.files
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
		python-venv \
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
	aarch64) echo "aarch64" ;;
	armv7l) echo "armv7h" ;;
	*) uname -m ;;
	esac
}

# ── build ─────────────────────────────────────────────────────────────────

build_main() {
	VERSION="$(resolve_version)"
	local arch
	arch="$(arch_label)"

	log "Building YAMS ${VERSION} for arch ${arch} (channel=${CHANNEL})"

	cd "${REPO_ROOT}"

	# Configure and build with Meson + Conan (same flow as Linux CI)
	export YAMS_CONAN_HOST_PROFILE="./conan/profiles/host-linux-clang"
	export YAMS_CONAN_ARCH="${arch}"
	export YAMS_ENABLE_MOBILE_BINDINGS=false
	export YAMS_EXTRA_MESON_FLAGS="-Dyams-version=${VERSION} -Dwerror=false -Dwarning_level=2 -Denable-lzma=auto ${MESON_EXTRA_ARGS:-}"
	export YAMS_INSTALL_PREFIX="/usr"
	export CC=clang
	export CXX=clang++

	bash ./setup.sh Release

	# Source Conan build env
	local conan_env=""
	for candidate in \
		"build/release/build-release/conan/conanbuild.sh" \
		"build/release/conan/conanbuild.sh"; do
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

	meson compile -C build/release

	# Stage install
	local stage_root="${PKG_BUILD_DIR}/stage"
	rm -rf "${stage_root}"
	meson install -C build/release --destdir "${stage_root}" --no-rebuild

	# Prune development payload
	bash scripts/prune-runtime-install.sh "${stage_root}/usr" 2>/dev/null || true

	# Install systemd unit + preset into stage (makepkg handles them via PKGBUILD,
	# but we also support the package_only entry point which needs them staged)
	install -Dm644 packaging/systemd/yams-daemon.service \
		"${stage_root}/usr/lib/systemd/system/yams-daemon.service"
	install -Dm644 packaging/systemd/80-yams.preset \
		"${stage_root}/usr/lib/systemd/system-preset/80-yams.preset"

	# Build Arch package from PKGBUILD using staged install as source
	log "Building Arch package with makepkg"
	local work_dir="${PKG_BUILD_DIR}/arch-work"
	rm -rf "${work_dir}"
	mkdir -p "${work_dir}"

	# Prepare source directory for makepkg
	mkdir -p "${work_dir}/pkg/yams/usr"
	cp -a "${stage_root}/usr/." "${work_dir}/pkg/yams/usr/"

	# Copy PKGBUILD and companion files
	cp packaging/arch/PKGBUILD "${work_dir}/"
	cp packaging/arch/yams.install "${work_dir}/" 2>/dev/null || true

	# Package using a minimal PKGBUILD that just wraps the staged directory tree.
	# This avoids a full makepkg build-from-source (which would recompile).
	local pkg_arch="$(arch_label)"
	local pkg_file="yams-${VERSION}-linux-${pkg_arch}.pkg.tar.zst"

	cd "${work_dir}"

	# Build the .pkg.tar.zst directly (tar + zstd, pacman-compatible).
	# Arch packages are .pkg.tar.zst archives with specific metadata.
	local pkg_size
	bsdtar -cf - -C pkg/yams . | zstd -T0 -19 -o "${pkg_file}"

	# Generate .BUILDINFO and .MTREE as makepkg would
	{
		echo "pkgname = yams"
		echo "pkgver = ${VERSION}"
		echo "pkgrel = 1"
		echo "arch = ${pkg_arch}"
		echo "builddate = $(date -u +%s)"
		echo "builddir = /build"
		echo "buildenv = !distcc, !ccache, check"
		echo "installed = $(pacman -Q | wc -l)"
	} >.BUILDINFO

	if command -v bsdtar >/dev/null 2>&1; then
		bsdtar -czf .MTREE --format=mtree --options='!all,use-set,type,uid,gid,mode,time,size,md5,sha256,link' @<(cd pkg/yams && find . -mindepth 1 -printf '%P\n')
	fi

	# Move artifact to build dir
	mkdir -p "${REPO_ROOT}/${PKG_BUILD_DIR}"
	cp "${pkg_file}" "${REPO_ROOT}/${PKG_BUILD_DIR}/"
	cd "${REPO_ROOT}"

	# Chown artifacts back to host user if running in Docker
	if [ -n "${HOST_UID:-}" ] && [ -n "${HOST_GID:-}" ] && [ "${HOST_UID}" != "0" ]; then
		chown -R "${HOST_UID}:${HOST_GID}" "${PKG_BUILD_DIR}" 2>/dev/null || true
	fi

	log "Created Arch package: ${PKG_BUILD_DIR}/${pkg_file}"

	# ── repo metadata ────────────────────────────────────────────────────
	build_repo_metadata "${VERSION}"

	echo "ARCH_PKG=${PKG_BUILD_DIR}/${pkg_file}" >>"${GITHUB_ENV:-/dev/null}"
}

# ── package_only ──────────────────────────────────────────────────────────

package_only() {
	VERSION="${1:-}"
	local build_dir="${2:-}"
	local stage_dir="${3:-}"

	if [ -z "${VERSION}" ] || [ -z "${build_dir}" ] || [ -z "${stage_dir}" ]; then
		fail "usage: package_only <version> <build_dir> <stage_dir>"
	fi

	local pkg_arch
	pkg_arch="$(arch_label)"
	local pkg_file="yams-${VERSION}-linux-${pkg_arch}.pkg.tar.zst"

	log "Packaging staged install at ${stage_dir} -> ${build_dir}/${pkg_file}"

	mkdir -p "${build_dir}"
	bsdtar -cf - -C "${stage_dir}" . | zstd -T0 -19 -o "${build_dir}/${pkg_file}"

	log "Created Arch package: ${build_dir}/${pkg_file}"

	build_repo_metadata "${VERSION}"
}

# ── repo metadata ─────────────────────────────────────────────────────────

build_repo_metadata() {
	local ver="${1:-unknown}"
	local pkg_arch
	pkg_arch="$(arch_label)"
	local arch_dir="${REPO_ROOT}/${PKG_BUILD_DIR}/archrepo/os/${pkg_arch}"
	local repo_dir="${REPO_ROOT}/${PKG_BUILD_DIR}/archrepo"

	mkdir -p "${arch_dir}"

	# Collect all .pkg.tar.zst files into the arch repo layout
	find "${REPO_ROOT}/${PKG_BUILD_DIR}" -maxdepth 1 -name 'yams-*.pkg.tar.zst' -exec cp {} "${arch_dir}/" \;

	# Build pacman database with repo-add
	if command -v repo-add >/dev/null 2>&1; then
		cd "${arch_dir}"
		local db_path="${repo_dir}/yams.db.tar.gz"

		# repo-add creates yams.db.tar.gz and yams.files.tar.gz
		# Pacman searches for *.db and *.files (compressed tarballs)
		repo-add -q "${db_path}" *.pkg.tar.zst || true

		# Pacman expects .db and .files with no .tar extension for modern repos
		cd "${repo_dir}"
		if [ -f yams.db.tar.gz ]; then
			ln -sf yams.db.tar.gz yams.db
		fi
		if [ -f yams.files.tar.gz ]; then
			ln -sf yams.files.tar.gz yams.files
		fi
		cd "${REPO_ROOT}"
	else
		log "repo-add not available; skipping pacman database generation"
	fi

	local usage_file="${repo_dir}/USAGE.txt"
	cat >"${usage_file}" <<'EOF'
Add Arch repo:
  sudo tee /etc/pacman.d/yams.conf <<'REPO'
  [yams]
  SigLevel = Optional TrustAll
  Server = https://repo.yamsmemory.ai/archrepo/
  REPO

  sudo tee -a /etc/pacman.conf <<'REPO'
  Include = /etc/pacman.d/yams.conf
  REPO

  sudo pacman -Sy yams
EOF

	log "Arch repo metadata generated at ${repo_dir}"
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
