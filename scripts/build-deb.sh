#!/usr/bin/env bash
# Debian local runner for SourceHut .build.yml (trixie)
# - Mirrors the CI tasks using Meson + Conan toolchain
# - Produces tarball, .deb, and .rpm artifacts for tagged builds
# - Intended to run inside a Debian trixie container with your repo mounted at /workspace/yams
#
# Example:
#   docker run --rm -it \
#     -e GIT_REF=refs/tags/v1.2.3 \
#     -e CONAN_CPU_COUNT=2 \
#     -e MESON_EXTRA_ARGS="-Denable-onnx=disabled" \
#     -v "$PWD":/workspace/yams \
#     -w /workspace \
#     debian:trixie bash -lc '/workspace/yams/scripts/srht-debian.sh'
#
# Artifacts:
#   - /workspace/yams/build/release/yams-*.tar.gz
#   - /workspace/yams/build/release/yams-*.deb
#   - /workspace/yams/build/release/yams-*.rpm
set -euo pipefail
set -x

export DEBIAN_FRONTEND=noninteractive
export PATH="${HOME}/.local/bin:${PATH}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -d /workspace/yams ]; then
  REPO_ROOT="/workspace/yams"
else
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
fi

allowed_deb_ver_chars='A-Za-z0-9.+:~'
allowed_rpm_ver_chars='A-Za-z0-9._+'

normalize_arch_label() {
  local arch="$1"
  case "${arch}" in
    amd64|x86_64) echo "x86_64" ;;
    arm64|aarch64) echo "aarch64" ;;
    *) echo "${arch}" ;;
  esac
}

append_buildenv() {
  echo "$1" >> "${HOME}/.buildenv"
}

persist_env_defaults() {
  export BUILD_DIR="${BUILD_DIR:-build/release}"
  export STAGE_DIR="${STAGE_DIR:-stage}"
  local default_jobs
  if command -v nproc >/dev/null 2>&1; then
    default_jobs="$(nproc)"
  else
    default_jobs="$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 2)"
  fi
  export CONAN_CPU_COUNT="${CONAN_CPU_COUNT:-${default_jobs}}"
  : > "${HOME}/.buildenv"
  append_buildenv "export BUILD_DIR=${BUILD_DIR}"
  append_buildenv "export STAGE_DIR=${STAGE_DIR}"
  append_buildenv "export CONAN_CPU_COUNT=${CONAN_CPU_COUNT}"
  append_buildenv "export PATH=${HOME}/.local/bin:\\$PATH"
}

ensure_base_packages() {
  if ! command -v apt-get >/dev/null 2>&1; then
    echo "apt-get not available; skipping base package install" >&2
    return
  fi

  local -a apt_runner
  if [ "$(id -u)" -eq 0 ]; then
    apt_runner=(apt-get)
  elif command -v sudo >/dev/null 2>&1; then
    apt_runner=(sudo apt-get)
  else
    echo "Skipping apt-get install (requires root privileges); assuming base image already provides build dependencies" >&2
    return
  fi

  "${apt_runner[@]}" update -y
  "${apt_runner[@]}" install -y --no-install-recommends \
    build-essential \
    cmake \
    meson \
    ninja-build \
    zip \
    ccache \
    dpkg-dev \
    fakeroot \
    rpm \
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
    pkg-config \
    liburing-dev \
    libarchive-dev \
    libtag1-dev \
    libsqlite3-dev \
    libssl-dev \
    libcurl4-gnutls-dev \
    protobuf-compiler \
    libprotobuf-dev \
    libmediainfo-dev \
    mediainfo
}

install_conan() {
  if ! command -v conan >/dev/null 2>&1; then
    if command -v pipx >/dev/null 2>&1; then
      pipx install 'conan<3' --pip-args='--upgrade' || true
    fi
  fi
  if ! command -v conan >/dev/null 2>&1; then
    python3 -m pip install --user --upgrade 'conan<3' || true
    hash -r || true
  fi
  if ! command -v conan >/dev/null 2>&1; then
    python3 -m venv /opt/conan
    . /opt/conan/bin/activate
    python -m pip install --upgrade pip setuptools wheel
    python -m pip install 'conan<3'
    export PATH="/opt/conan/bin:${PATH}"
    hash -r
  fi
  if command -v conan >/dev/null 2>&1; then
    append_buildenv "export PATH=$(dirname "$(command -v conan)"):\\$PATH"
  else
    echo "Conan installation failed" >&2
    exit 1
  fi
}

compute_version() {
  cd "${REPO_ROOT}"
  local raw_tag
  if [[ "${GIT_REF:-}" =~ ^refs/tags/ ]]; then
    raw_tag="${GIT_REF#refs/tags/}"
  else
    raw_tag="$(git describe --tags --abbrev=0 2>/dev/null || echo 0.0.0-dev)"
  fi
  CLEAN_VERSION="${raw_tag#v}"
  export YAMS_VERSION="${CLEAN_VERSION}"
  append_buildenv "export YAMS_VERSION=${CLEAN_VERSION}"
  echo "Computed YAMS_VERSION=${CLEAN_VERSION} (GIT_REF=${GIT_REF:-n/a})"
}

configure_conan_profile() {
  conan --version
  conan profile detect --force || true
  conan profile show -pr default || conan profile show || true
}

run_conan_install() {
  cd "${REPO_ROOT}"
  conan install . \
    -of "${BUILD_DIR}" \
    -pr:h=./conan/profiles/host-linux-gcc \
    -pr:b=./conan/profiles/host-linux-gcc \
    -s build_type=Release \
    -c tools.build:jobs="${CONAN_CPU_COUNT}" \
    -o "sqlite3/*:fts5=True" \
    --build=missing
}

run_meson_build() {
  cd "${REPO_ROOT}"
  export YAMS_DISABLE_MODEL_PRELOAD=1
  local native_file="${BUILD_DIR}/build-release/conan/conan_meson_native.ini"
  local meson_args=()
  if [ -n "${MESON_EXTRA_ARGS:-}" ]; then
    # shellcheck disable=SC2206
    meson_args=(${MESON_EXTRA_ARGS})
  fi
  # Check if meson is already configured; use --reconfigure instead of --wipe
  # to preserve Conan-generated files in the build directory
  local setup_mode=""
  if [ -f "${BUILD_DIR}/meson-private/coredata.dat" ]; then
    setup_mode="--reconfigure"
  fi
  meson setup "${BUILD_DIR}" \
    --prefix=/usr \
    --buildtype=release \
    --native-file "${native_file}" \
    ${setup_mode} \
    "${meson_args[@]}"
  meson compile -C "${BUILD_DIR}"
  meson install -C "${BUILD_DIR}" --destdir "${STAGE_DIR}"
}

# --- Version sanitization helpers -------------------------------------------------

get_base_version() {
  # Prefer explicit env from CI; otherwise attempt to derive from git; fallback 0.0.0
  if [ -n "${BASE_VERSION:-}" ]; then
    echo "${BASE_VERSION}"
    return
  fi
  if git -C "${REPO_ROOT}" describe --tags --abbrev=0 --match='v*' >/dev/null 2>&1; then
    git -C "${REPO_ROOT}" describe --tags --abbrev=0 --match='v*' 2>/dev/null | sed 's/^v//'
    return
  fi
  echo "0.0.0"
}

sanitize_for_deb_upstream() {
  # Map arbitrary string to Debian upstream version-safe token (no hyphens)
  # Allowed: ${allowed_deb_ver_chars}; convert others to '.' and collapse repeats
  local s="$1"
  s="${s//-/.}"
  s="${s//_/.}"
  s="$(echo "$s" | sed -E "s/[^${allowed_deb_ver_chars}]+/./g" | sed -E 's/\.+/./g' | sed -E 's/^\.|\.$//g')"
  echo "$s"
}

compute_deb_version() {
  local in_ver="$1"
  local base_ver
  base_ver="$(get_base_version)"
  # Stable numeric-ish versions without hyphens are acceptable as-is
  if [[ "$in_ver" =~ ^[0-9][0-9A-Za-z.+:~]*$ ]] && [[ "$in_ver" != *-* ]]; then
    echo "$in_ver"
    return
  fi
  # Nightly: nightly-YYYYMMDD-<hash>
  if [[ "$in_ver" =~ ^nightly-([0-9]{8})-([0-9a-fA-F]{7,40})$ ]]; then
    echo "${base_ver}~nightly.${BASH_REMATCH[1]}+g${BASH_REMATCH[2],,}"
    return
  fi
  # Weekly: weekly-YYYYwWW-<hash>
  if [[ "$in_ver" =~ ^weekly-([0-9]{4}w[0-9]{2})-([0-9a-fA-F]{7,40})$ ]]; then
    echo "${base_ver}~weekly.${BASH_REMATCH[1]}+g${BASH_REMATCH[2],,}"
    return
  fi
  # Fallback: general sanitize
  local token
  token="$(sanitize_for_deb_upstream "$in_ver")"
  echo "${base_ver}~${token}"
}

sanitize_for_rpm_token() {
  # Allowed in rpm Version/Release: ${allowed_rpm_ver_chars} and '~' (for pre-release)
  local s="$1"
  s="${s//-/.}"
  s="${s//_/.}"
  s="$(echo "$s" | sed -E "s/[^${allowed_rpm_ver_chars}~]+/./g" | sed -E 's/\.+/./g' | sed -E 's/^\.|\.$//g')"
  echo "$s"
}

compute_rpm_nv() {
  # Outputs two values via echoes: rpm_version rpm_release
  local in_ver="$1"
  local base_ver
  base_ver="$(get_base_version)"
  # Stable numeric-ish with no hyphen: put all in Version and keep Release=1
  if [[ "$in_ver" =~ ^[0-9][0-9A-Za-z._+]*$ ]]; then
    echo "$in_ver"
    echo "1"
    return
  fi
  # Nightly pattern
  if [[ "$in_ver" =~ ^nightly-([0-9]{8})-([0-9a-fA-F]{7,40})$ ]]; then
    echo "$base_ver"
    echo "0.1~nightly.${BASH_REMATCH[1]}.g${BASH_REMATCH[2],,}"
    return
  fi
  # Weekly pattern
  if [[ "$in_ver" =~ ^weekly-([0-9]{4}w[0-9]{2})-([0-9a-fA-F]{7,40})$ ]]; then
    echo "$base_ver"
    echo "0.1~weekly.${BASH_REMATCH[1]}.g${BASH_REMATCH[2],,}"
    return
  fi
  # Fallback: generic sanitize in Release, keep Version as base
  local token
  token="$(sanitize_for_rpm_token "$in_ver")"
  echo "$base_ver"
  echo "0.1~${token}"
}

prepare_stage_docs() {
  local stage_root="$1"
  install -Dm644 "${REPO_ROOT}/LICENSE" "${stage_root}/usr/share/doc/yams/LICENSE"
  if [ -f "${REPO_ROOT}/README.md" ]; then
    install -Dm644 "${REPO_ROOT}/README.md" "${stage_root}/usr/share/doc/yams/README.md"
  fi
}

package_tarball() {
  local version="$1"
  local build_dir="$2"
  local stage_root="$3"
  local arch_label
  arch_label="$(normalize_arch_label "$(uname -m)")"
  local tar_name="yams-${version}-linux-${arch_label}.tar.gz"
  tar -C "${stage_root}" -czf "${build_dir}/${tar_name}" .
  echo "Created tarball: ${build_dir}/${tar_name}"
}

package_deb() {
  if ! command -v dpkg-deb >/dev/null 2>&1; then
    echo "dpkg-deb not available; skipping .deb packaging" >&2
    return
  fi
  local version="$1"
  local build_dir="$2"
  local stage_root="$3"
  local deb_arch
  deb_arch="$(dpkg --print-architecture 2>/dev/null || echo amd64)"
  local work_dir="${build_dir}/deb-work"
  rm -rf "${work_dir}"
  mkdir -p "${work_dir}"
  cp -a "${stage_root}/." "${work_dir}/"
  mkdir -p "${work_dir}/DEBIAN"
  local control="${work_dir}/DEBIAN/control"
  cat > "${control}" <<'__DEB_CONTROL__'
Package: yams
Version: __VERSION__
Section: utils
Priority: optional
Architecture: __ARCH__
Maintainer: YAMS Contributors <git@trevon.dev>
Homepage: https://git.sr.ht/~trevon/yams
Depends: @DEPENDENCIES@
Description: Yet Another Memory System - persistent memory for LLMs
 YAMS provides content-addressed storage, deduplication, and search for long-term LLM memory.
__DEB_CONTROL__
  local deb_version
  deb_version="$(compute_deb_version "$version")"
  sed -i "s|__VERSION__|${deb_version}|" "${control}"
  sed -i "s|__ARCH__|${deb_arch}|" "${control}"

  local -a binaries=()
  mapfile -d '' binaries < <(find "${work_dir}" -type f \( -perm -111 -o -name '*.so*' \) -print0) || true
  local depends_fallback="libc6 (>= 2.31), libstdc++6 (>= 12)"
  if command -v dpkg-shlibdeps >/dev/null 2>&1 && ((${#binaries[@]} > 0)); then
    set +e
    local shlib_output
    shlib_output="$(dpkg-shlibdeps -O "${binaries[@]}" 2>&1)"
    local shlib_status=$?
    set -e
    if [ "${shlib_status}" -eq 0 ] && [[ "${shlib_output}" =~ shlibs:Depends=(.*) ]]; then
      depends_fallback="${BASH_REMATCH[1]}"
    else
      echo "${shlib_output}" >&2
    fi
  fi
  sed -i "s|@DEPENDENCIES@|${depends_fallback}|" "${control}"

  find "${work_dir}" -type d -exec chmod 0755 {} +
  find "${work_dir}" -type f -exec chmod 0644 {} +
  if ((${#binaries[@]} > 0)); then
    for bin in "${binaries[@]}"; do
      chmod 0755 "${bin}"
    done
  fi

  local deb_name="yams-${version}-linux-$(normalize_arch_label "${deb_arch}")".deb
  if command -v fakeroot >/dev/null 2>&1; then
    fakeroot dpkg-deb --build "${work_dir}" "${build_dir}/${deb_name}"
  else
    dpkg-deb --build "${work_dir}" "${build_dir}/${deb_name}"
  fi
  rm -rf "${work_dir}"
  echo "Created Debian package: ${build_dir}/${deb_name}"
}

package_rpm() {
  if ! command -v rpmbuild >/dev/null 2>&1; then
    echo "rpmbuild not available; skipping .rpm packaging" >&2
    return
  fi
  local version="$1"
  local build_dir="$2"
  local stage_root="$3"
  local rpm_arch
  rpm_arch="$(rpm --eval '%{_arch}' 2>/dev/null || uname -m)"
  local rpm_root="${build_dir}/rpmbuild"
  rm -rf "${rpm_root}"
  mkdir -p "${rpm_root}/"{BUILD,RPMS,SOURCES,SPECS,SRPMS}

  # Compute rpm_version early to use for tarball name
  local rpm_version rpm_release
  local rpm_output
  rpm_output="$(compute_rpm_nv "$version")"
  rpm_version="$(echo "$rpm_output" | sed -n '1p')"
  rpm_release="$(echo "$rpm_output" | sed -n '2p')"
  rpm_release="${rpm_release:-1}"

  local source_dir="${rpm_root}/SOURCES/yams-${rpm_version}"
  mkdir -p "${source_dir}"
  cp -a "${stage_root}/." "${source_dir}/"
  tar -C "${rpm_root}/SOURCES" -czf "${rpm_root}/SOURCES/yams-${rpm_version}.tar.gz" "yams-${rpm_version}"
  rm -rf "${source_dir}"

  local rpm_filelist="${rpm_root}/SOURCES/filelist"
  (
    cd "${stage_root}"
    find . -type d -printf "%P\n" | LC_ALL=C sort | while read -r dir; do
      [ -z "${dir}" ] && continue
      echo "%dir /${dir}"
    done
    find . \( -type f -o -type l \) -printf "%P\n" | LC_ALL=C sort | while read -r file; do
      [ -z "${file}" ] && continue
      echo "/${file}"
    done
  ) > "${rpm_filelist}"

  local spec="${rpm_root}/SPECS/yams.spec"
  cat > "${spec}" <<'__RPM_SPEC__'
%global debug_package %{nil}
%global __strip /bin/true
Name: yams
Version: __VERSION__
Release: __RELEASE__%{?dist}
Summary: Yet Another Memory System
License: Apache-2.0
URL: https://git.sr.ht/~trevon/yams
Source0: %{name}-%{version}.tar.gz
BuildArch: __RPM_ARCH__

%description
Yet Another Memory System (YAMS) provides content-addressed storage with deduplication and search designed for long-term large language model memory.

%prep
%setup -q

%build
# Upstream build executed via Meson prior to packaging.

%install
rm -rf %{buildroot}
mkdir -p %{buildroot}
cp -a %{_builddir}/%{name}-%{version}/. %{buildroot}/

%files -f %{_sourcedir}/filelist
%defattr(-,root,root,-)
__RPM_SPEC__
  echo "DEBUG: version=$version, rpm_version=$rpm_version, rpm_release=$rpm_release" >&2
  sed -i "s|__VERSION__|${rpm_version}|" "${spec}"
  sed -i "s|__RELEASE__|${rpm_release}|" "${spec}"
  sed -i "s|__RPM_ARCH__|${rpm_arch}|" "${spec}"

  rpmbuild --define "_topdir ${rpm_root}" -bb "${spec}"
  local built_rpm
  built_rpm="$(find "${rpm_root}/RPMS" -type f -name 'yams-*.rpm' -print -quit)"
  if [ -n "${built_rpm}" ]; then
    local rpm_name="yams-${version}-linux-$(normalize_arch_label "${rpm_arch}")".rpm
    cp -f "${built_rpm}" "${build_dir}/${rpm_name}"
    echo "Created RPM package: ${build_dir}/${rpm_name}"
  else
    echo "rpmbuild completed but RPM artifact not found" >&2
  fi
  rm -rf "${rpm_root}"
}

package_all() {
  local version="$1"
  local build_dir="$2"
  local stage_dir="$3"
  local force="${4:-0}"

  if [[ "${force}" != "1" && "${GIT_REF:-}" != refs/tags/* ]]; then
    echo "Skipping packaging (not a tag build): GIT_REF=${GIT_REF:-n/a}"
    return
  fi

  if [[ "${build_dir}" != /* ]]; then
    build_dir="${REPO_ROOT}/${build_dir}"
  fi
  build_dir="$(readlink -f "${build_dir}")"
  local stage_root="${build_dir}/${stage_dir}"
  if [ ! -d "${stage_root}" ]; then
    echo "Stage directory ${stage_root} not found" >&2
    exit 1
  fi

  prepare_stage_docs "${stage_root}"
  package_tarball "${version}" "${build_dir}" "${stage_root}"
  package_deb "${version}" "${build_dir}" "${stage_root}"
  package_rpm "${version}" "${build_dir}" "${stage_root}"
}

cleanup_apt() {
  apt-get clean
  rm -rf /var/lib/apt/lists/* || true
}

package_only_main() {
  shift
  if [ "$#" -lt 2 ] || [ "$#" -gt 3 ]; then
    echo "Usage: $0 package_only <version> <build_dir> [stage_dir]" >&2
    exit 1
  fi
  local version="$1"
  local build_dir="$2"
  local stage_dir="${3:-stage}"
  package_all "${version}" "${build_dir}" "${stage_dir}" 1
  exit 0
}

main() {
  if [ "${1:-}" = "package_only" ]; then
    package_only_main "$@"
  fi

  ensure_base_packages
  persist_env_defaults
  install_conan
  compute_version
  configure_conan_profile
  run_conan_install
  run_meson_build
  package_all "${CLEAN_VERSION}" "${BUILD_DIR}" "${STAGE_DIR}" 0
  cleanup_apt
  echo "All tasks finished."
}

main "$@"
