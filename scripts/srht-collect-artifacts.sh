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

    PREFIX_REL="${PRUNE_ROOT#"${STAGE_ROOT}/"}"
    RUNTIME_PATHS=()

    add_dir_if_present() {
      local rel="$1"
      if [ -d "${PRUNE_ROOT}/${rel}" ]; then
        RUNTIME_PATHS+=("${PREFIX_REL}/${rel}")
      fi
    }

    add_runtime_libs() {
      local libdir="$1"
      [ -d "${PRUNE_ROOT}/${libdir}" ] || return 0
      while IFS= read -r -d '' path; do
        RUNTIME_PATHS+=("${path#"${STAGE_ROOT}/"}")
      done < <(
        find "${PRUNE_ROOT}/${libdir}" -maxdepth 1 -type f \
          \( -name 'libyams*.so' -o -name 'libyams*.so.*' -o -name 'libyams*.dylib' \) \
          -print0
      )
    }

    add_dir_if_present bin
    add_dir_if_present share/yams
    add_dir_if_present lib/yams/plugins
    add_dir_if_present lib64/yams/plugins
    add_runtime_libs lib
    add_runtime_libs lib64

    if [ "${#RUNTIME_PATHS[@]}" -gt 0 ]; then
      # Archive directly from the staged install. Copying the runtime tree first can
      # exhaust constrained SourceHut workers because plugins temporarily occupy twice the space.
      tar -C "${STAGE_ROOT}" -czf "${ART_DIR}/yams.tar.gz" "${RUNTIME_PATHS[@]}"
      ls -l "${ART_DIR}" || true
    else
      echo "WARNING: No runtime files found under ${PRUNE_ROOT}" >&2
    fi
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
