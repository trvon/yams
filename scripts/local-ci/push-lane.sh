#!/usr/bin/env bash
# yams/scripts/local-ci/push-lane.sh
#
# Tiered local GitHub Actions validation for pre-push/release sanity.
# Uses act for Linux workflow parity and repo-native package scripts for package lanes.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
LOG_ROOT_DEFAULT="${REPO_ROOT}/build/local-ci/push-lane"

PROFILE="fast"
DRY_RUN=0
KEEP_GOING=0
LOG_ROOT="${LOG_ROOT_DEFAULT}"
ACT_BIN_LABEL=""
ACT_CMD=()
ARCH_ARG=""
RUN_TAG="$(date -u +%Y%m%dT%H%M%SZ)-$$"

usage() {
  cat <<'USAGE'
Usage: push-lane.sh [options]

Profiles:
  fast      Tool preflight, act sanity, Tests linux-x86_64, Release linux-hosted fast-mode
  tests     Fast lane plus targeted native Meson tests when a local builddir is available
  release   Release linux-hosted fast-mode via act
  packages  Local Docker package build; systemd install validation only on Linux
  full      tests + release + packages

Options:
  --profile NAME        fast|tests|release|packages|full (default: fast)
  --dry-run             Print commands without executing them
  --keep-going          Continue after failures and report all failed steps
  --log-root DIR        Log directory (default: build/local-ci/push-lane)
  -h, --help            Show this help

Examples:
  bash scripts/local-ci/push-lane.sh --profile fast
  bash scripts/local-ci/push-lane.sh --profile release --dry-run
  YAMS_PREPUSH_GH_LANE=1 git push
USAGE
}

log() { printf '\033[1;34m[push-lane]\033[0m %s\n' "$*"; }
ok() { printf '\033[1;32m[ ok ]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[warn]\033[0m %s\n' "$*" >&2; }
fail_msg() { printf '\033[1;31m[fail]\033[0m %s\n' "$*" >&2; }

while [ "$#" -gt 0 ]; do
  case "$1" in
    --profile) PROFILE="${2:-}"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    --keep-going) KEEP_GOING=1; shift ;;
    --log-root) LOG_ROOT="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) fail_msg "unknown argument: $1"; usage; exit 2 ;;
  esac
done

case "${PROFILE}" in
  fast|tests|release|packages|full) ;;
  *) fail_msg "invalid profile: ${PROFILE}"; echo "valid profiles: fast, tests, release, packages, full" >&2; exit 2 ;;
esac

mkdir -p "${LOG_ROOT}"
SUMMARY="${LOG_ROOT}/summary-${RUN_TAG}.md"
: > "${SUMMARY}"

FAILURES=()

record_summary() {
  printf '%s\n' "$*" >> "${SUMMARY}"
}

find_act() {
  if [ -n "${ACT_BIN_LABEL}" ]; then
    return 0
  fi
  if command -v act >/dev/null 2>&1; then
    ACT_BIN_LABEL="act"
    ACT_CMD=(act)
  elif command -v gh >/dev/null 2>&1 && gh act --help >/dev/null 2>&1; then
    ACT_BIN_LABEL="gh act"
    ACT_CMD=(gh act)
  else
    fail_msg "act not found. Install with: brew install act"
    fail_msg "Docker is also required: https://docs.docker.com/desktop/"
    exit 2
  fi
}

preflight() {
  log "preflight"
  command -v docker >/dev/null 2>&1 || { fail_msg "docker not found on PATH"; exit 2; }
  find_act
  if [ "$(uname -s)" = "Darwin" ] && [ "$(uname -m)" = "arm64" ]; then
    ARCH_ARG="--container-architecture linux/amd64"
  fi
  ok "docker: $(docker --version 2>/dev/null || echo unknown)"
  ok "act: $("${ACT_CMD[@]}" --version 2>/dev/null | head -n1 || echo "${ACT_BIN_LABEL}")"
  record_summary "# YAMS local push lane (${PROFILE})"
  record_summary ""
  record_summary "- Started: ${RUN_TAG}"
  record_summary "- Host: $(uname -s)/$(uname -m)"
  record_summary "- Act: ${ACT_BIN_LABEL} ${ARCH_ARG}"
  record_summary ""
}

run_step() {
  local name="$1"
  shift
  local log_file="${LOG_ROOT}/${RUN_TAG}-${name//[^A-Za-z0-9_.-]/_}.log"
  log "${name}"
  record_summary "## ${name}"
  record_summary ""
  record_summary '```sh'
  printf '%q ' "$@" >> "${SUMMARY}"
  record_summary ""
  record_summary '```'
  if [ "${DRY_RUN}" -eq 1 ]; then
    echo "DRY-RUN: $*" | tee "${log_file}"
    record_summary "- Result: dry-run"
    record_summary "- Log: ${log_file#"${REPO_ROOT}"/}"
    record_summary ""
    return 0
  fi
  set +e
  "$@" > >(tee "${log_file}") 2> >(tee -a "${log_file}" >&2)
  local rc=$?
  set -e
  if [ "${rc}" -eq 0 ]; then
    ok "${name}"
    record_summary "- Result: pass"
  else
    fail_msg "${name} failed (exit ${rc}); log: ${log_file}"
    record_summary "- Result: fail (${rc})"
    FAILURES+=("${name} (${rc})")
    if [ "${KEEP_GOING}" -ne 1 ]; then
      record_summary "- Log: ${log_file#"${REPO_ROOT}"/}"
      record_summary ""
      exit "${rc}"
    fi
  fi
  record_summary "- Log: ${log_file#"${REPO_ROOT}"/}"
  record_summary ""
  return 0
}

run_act_sanity() {
  run_step "act-sanity-workflow-lint" bash -lc "cd '${REPO_ROOT}' && ${ACT_BIN_LABEL} pull_request -W .github/workflows/act-sanity.yml -j workflow-lint ${ARCH_ARG} --artifact-server-path '${LOG_ROOT}/artifacts' --cache-server-path '${LOG_ROOT}/cache'"
  run_step "act-sanity-windows-script" bash -lc "cd '${REPO_ROOT}' && ${ACT_BIN_LABEL} pull_request -W .github/workflows/act-sanity.yml -j windows-script-sanity ${ARCH_ARG} --artifact-server-path '${LOG_ROOT}/artifacts' --cache-server-path '${LOG_ROOT}/cache'"
}

run_tests_act() {
  run_step "tests-linux-x86_64-act" bash -lc "cd '${REPO_ROOT}' && ${ACT_BIN_LABEL} push -W .github/workflows/tests.yml -j tests --matrix suffix:linux-x86_64 ${ARCH_ARG} --artifact-server-path '${LOG_ROOT}/artifacts' --cache-server-path '${LOG_ROOT}/cache'"
}

run_release_act() {
  run_step "release-linux-hosted-fast-act" bash -lc "cd '${REPO_ROOT}' && ${ACT_BIN_LABEL} workflow_dispatch -W .github/workflows/release.yml -j build-release --matrix os:linux-hosted --input channel=nightly --input fast_mode=true ${ARCH_ARG} --artifact-server-path '${LOG_ROOT}/artifacts' --cache-server-path '${LOG_ROOT}/cache'"
}

run_targeted_native_tests() {
  local bd="${YAMS_LOCAL_CI_BUILD_DIR:-}"
  if [ -z "${bd}" ]; then
    for candidate in builddir build/debug build/release; do
      if [ -f "${REPO_ROOT}/${candidate}/build.ninja" ]; then
        bd="${candidate}"
        break
      fi
    done
  fi
  if [ -z "${bd}" ]; then
    warn "no local Meson builddir found; skipping targeted native tests"
    record_summary "## targeted-native-tests"
    record_summary ""
    record_summary "- Result: skipped (no builddir; set YAMS_LOCAL_CI_BUILD_DIR)"
    record_summary ""
    return 0
  fi
  run_step "targeted-native-known-failures" bash -lc "cd '${REPO_ROOT}' && meson test -C '${bd}' metadata_corruption daemon_background_processing integration_smoke --print-errorlogs --timeout-multiplier 2"
}

run_package_lane() {
  if [ "$(uname -s)" = "Linux" ]; then
    run_step "package-lane-build-and-validate" bash -lc "cd '${REPO_ROOT}' && bash scripts/local-ci/package-lane.sh"
  else
    warn "non-Linux host: running package build only; systemd install validation requires Linux"
    run_step "package-lane-build-only" bash -lc "cd '${REPO_ROOT}' && bash scripts/local-ci/package-lane.sh --build-only"
  fi
}

preflight

case "${PROFILE}" in
  fast)
    run_act_sanity
    run_tests_act
    run_release_act
    ;;
  tests)
    run_act_sanity
    run_tests_act
    run_targeted_native_tests
    ;;
  release)
    run_release_act
    ;;
  packages)
    run_package_lane
    ;;
  full)
    run_act_sanity
    run_tests_act
    run_targeted_native_tests
    run_release_act
    run_package_lane
    ;;
esac

if [ "${#FAILURES[@]}" -gt 0 ]; then
  record_summary "## Failures"
  for failure in "${FAILURES[@]}"; do
    record_summary "- ${failure}"
  done
  fail_msg "${#FAILURES[@]} step(s) failed. Summary: ${SUMMARY}"
  exit 1
fi

record_summary "## Result"
record_summary "All requested local validation steps passed."
ok "summary: ${SUMMARY#"${REPO_ROOT}"/}"
