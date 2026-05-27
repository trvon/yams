#!/usr/bin/env bash

set -euo pipefail

ART_DIR="artifacts"
mkdir -p "${ART_DIR}"
rm -f "${ART_DIR}/yams"*.tar.gz "${ART_DIR}/yams"*.deb "${ART_DIR}/yams"*.rpm 2>/dev/null || true

if [ -d yams ] && { [ -f yams/meson.build ] || [ -f yams/CMakeLists.txt ]; }; then
  REPO_DIR="yams"
else
  REPO_DIR="."
fi

shopt -s nullglob || true
if [[ "${GIT_REF:-}" == refs/tags/* ]]; then
  if compgen -G "${REPO_DIR}/${BUILD_DIR}/*.deb" > /dev/null; then
    for f in "${REPO_DIR}/${BUILD_DIR}"/*.deb; do cp -v "$f" "${ART_DIR}/"; done
  fi
  if compgen -G "${REPO_DIR}/${BUILD_DIR}/*.rpm" > /dev/null; then
    for f in "${REPO_DIR}/${BUILD_DIR}"/*.rpm; do cp -v "$f" "${ART_DIR}/"; done
  fi
  if ! compgen -G "${ART_DIR}/*" > /dev/null; then
    for f in "${REPO_DIR}/${BUILD_DIR}"/yams-*-linux-x86_64.tar.gz; do cp -v "$f" "${ART_DIR}/"; done
  fi
else
  for f in "${REPO_DIR}/${BUILD_DIR}"/yams-*-linux-x86_64.tar.gz; do cp -v "$f" "${ART_DIR}/"; done
fi

if compgen -G "${ART_DIR}/*.tar.gz" > /dev/null; then
  TARBALL=$(ls -1 "${ART_DIR}"/*.tar.gz | head -1)
  cp -v "$TARBALL" "${ART_DIR}/yams.tar.gz"
fi
if compgen -G "${ART_DIR}/*.deb" > /dev/null; then
  DEB=$(ls -1 "${ART_DIR}"/*.deb | head -1)
  cp -v "$DEB" "${ART_DIR}/yams.deb"
fi
if compgen -G "${ART_DIR}/*.rpm" > /dev/null; then
  RPM=$(ls -1 "${ART_DIR}"/*.rpm | head -1)
  cp -v "$RPM" "${ART_DIR}/yams.rpm"
fi

echo "Collected artifacts:" && ls -l "${ART_DIR}" || true

if [ "$(find "${ART_DIR}" -type f | wc -l | tr -d ' ')" -eq 0 ]; then
  STAGE_ROOT="${REPO_DIR}/${BUILD_DIR}/${STAGE_DIR}"
  if [ -d "${STAGE_ROOT}" ]; then
    echo "No release artifacts; creating runtime-pruned CI tarball from ${STAGE_ROOT}" >&2

    PRUNE_ROOT=""
    for candidate in "${STAGE_ROOT}/usr" "${STAGE_ROOT}/usr/local" "${STAGE_ROOT}/opt/homebrew"; do
      if [ -d "${candidate}" ]; then
        PRUNE_ROOT="${candidate}"
        break
      fi
    done
    if [ -n "${PRUNE_ROOT}" ] && [ -x "${REPO_DIR}/scripts/prune-runtime-install.sh" ]; then
      bash "${REPO_DIR}/scripts/prune-runtime-install.sh" "${PRUNE_ROOT}"
    fi

    TMP_STAGE_ROOT=$(mktemp -d)
    trap 'rm -rf "${TMP_STAGE_ROOT}"' EXIT
    PREFIX_REL="${PRUNE_ROOT#"${STAGE_ROOT}/"}"
    RUNTIME_ROOT="${TMP_STAGE_ROOT}/${PREFIX_REL}"
    mkdir -p "${RUNTIME_ROOT}"

    copy_dir_if_present() {
      local rel="$1"
      if [ -d "${PRUNE_ROOT}/${rel}" ]; then
        mkdir -p "$(dirname "${RUNTIME_ROOT}/${rel}")"
        cp -a "${PRUNE_ROOT}/${rel}" "${RUNTIME_ROOT}/${rel}"
      fi
    }

    copy_runtime_libs() {
      local libdir="$1"
      [ -d "${PRUNE_ROOT}/${libdir}" ] || return 0
      while IFS= read -r -d '' path; do
        mkdir -p "${RUNTIME_ROOT}/${libdir}"
        cp -a "$path" "${RUNTIME_ROOT}/${libdir}/"
      done < <(
        find "${PRUNE_ROOT}/${libdir}" -maxdepth 1 -type f \
          \( -name 'libyams*.so' -o -name 'libyams*.so.*' -o -name 'libyams*.dylib' \) \
          -print0
      )
    }

    copy_dir_if_present bin
    copy_dir_if_present share/yams
    copy_dir_if_present lib/yams/plugins
    copy_dir_if_present lib64/yams/plugins
    copy_runtime_libs lib
    copy_runtime_libs lib64

    tar -C "${TMP_STAGE_ROOT}" -czf "${ART_DIR}/yams.tar.gz" .
    rm -rf "${TMP_STAGE_ROOT}"
    trap - EXIT
    ls -l "${ART_DIR}" || true
  else
    echo "WARNING: Stage dir ${STAGE_ROOT} missing; no artifacts to publish" >&2
  fi
fi

for f in yams.deb yams.rpm meson-logs.tar.gz testlog.txt meson-log.txt compile_commands.json; do
  if [ ! -f "${ART_DIR}/${f}" ]; then
    echo "Not produced in this build." > "${ART_DIR}/${f}"
  fi
done

echo "collect_artifacts completed successfully"
