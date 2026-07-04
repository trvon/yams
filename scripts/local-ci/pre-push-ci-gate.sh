#!/usr/bin/env bash
# yams/scripts/local-ci/pre-push-ci-gate.sh
#
# Blocking pre-push CI gate. Requires Linux build+tests in smolvm and native
# macOS build+tests on Darwin hosts before push proceeds. Use --self-test to
# prove dispatch/host wiring without running the expensive build/test work.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
LOG_ROOT_DEFAULT="${REPO_ROOT}/build/local-ci/pre-push-ci-gate"

LOG_ROOT="${LOG_ROOT_DEFAULT}"
SELF_TEST=0
DRY_RUN=0
LINUX_ONLY=0
MACOS_ONLY=0
RUN_TAG="$(date -u +%Y%m%dT%H%M%SZ)-$$"

usage() {
	cat <<'USAGE'
Usage: pre-push-ci-gate.sh [options]

Runs the blocking pre-push CI gate:
  1. Linux Debug build + unit/integration tests in smolvm.
  2. Native macOS Debug build + unit/integration tests on Darwin hosts.

Options:
  --self-test    Verify gate dispatch without expensive builds.
  --dry-run      Print commands without executing them.
  --linux-only   Run only the Linux smolvm lane.
  --macos-only   Run only the native macOS lane.
  --log-root DIR Log directory (default: build/local-ci/pre-push-ci-gate)
  -h, --help     Show this help
USAGE
}

log() { printf '\033[1;34m[pre-push-ci]\033[0m %s\n' "$*"; }
ok() { printf '\033[1;32m[ ok ]\033[0m %s\n' "$*"; }
fail() { printf '\033[1;31m[fail]\033[0m %s\n' "$*" >&2; }

while [ "$#" -gt 0 ]; do
	case "$1" in
	--self-test)
		SELF_TEST=1
		shift
		;;
	--dry-run)
		DRY_RUN=1
		shift
		;;
	--linux-only)
		LINUX_ONLY=1
		shift
		;;
	--macos-only)
		MACOS_ONLY=1
		shift
		;;
	--log-root)
		LOG_ROOT="${2:-}"
		shift 2
		;;
	-h | --help)
		usage
		exit 0
		;;
	*)
		fail "unknown argument: $1"
		usage
		exit 2
		;;
	esac
done

if [ "${LINUX_ONLY}" -eq 1 ] && [ "${MACOS_ONLY}" -eq 1 ]; then
	fail "--linux-only and --macos-only are mutually exclusive"
	exit 2
fi

mkdir -p "${LOG_ROOT}"
SUMMARY="${LOG_ROOT}/summary-${RUN_TAG}.md"
: >"${SUMMARY}"
FAILURES=()

record_summary() { printf '%s\n' "$*" >>"${SUMMARY}"; }

run_step() {
	local name="$1"
	shift
	local log_file="${LOG_ROOT}/${RUN_TAG}-${name//[^A-Za-z0-9_.-]/_}.log"
	log "${name}"
	record_summary "## ${name}"
	record_summary ""
	record_summary '```sh'
	printf '%q ' "$@" >>"${SUMMARY}"
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
		fail "${name} failed (${rc}); log: ${log_file}"
		record_summary "- Result: fail (${rc})"
		FAILURES+=("${name} (${rc})")
	fi
	record_summary "- Log: ${log_file#"${REPO_ROOT}"/}"
	record_summary ""
}

record_summary "# YAMS pre-push CI gate"
record_summary ""
record_summary "- Started: ${RUN_TAG}"
record_summary "- Host: $(uname -s)/$(uname -m)"
record_summary "- Self-test: ${SELF_TEST}"
record_summary ""

linux_profile="linux-ci"
macos_profile="ci"
if [ "${SELF_TEST}" -eq 1 ]; then
	linux_profile="smoke"
	macos_profile="self-test"
fi

if [ "${MACOS_ONLY}" -ne 1 ]; then
	run_step "linux-smolvm-${linux_profile}" bash "${SCRIPT_DIR}/smolvm-lane.sh" --profile "${linux_profile}" --log-root "${LOG_ROOT}/smolvm"
fi

if [ "${LINUX_ONLY}" -ne 1 ]; then
	if [ "$(uname -s)" != "Darwin" ]; then
		fail "native macOS gate requires a Darwin host; rerun on macOS or pass --linux-only intentionally"
		FAILURES+=("native-macos-unavailable (2)")
	else
		run_step "native-macos-${macos_profile}" bash "${SCRIPT_DIR}/native-macos-lane.sh" --profile "${macos_profile}" --log-root "${LOG_ROOT}/macos"
	fi
fi

if [ "${#FAILURES[@]}" -gt 0 ]; then
	record_summary "## Failures"
	for failure in "${FAILURES[@]}"; do
		record_summary "- ${failure}"
	done
	fail "${#FAILURES[@]} step(s) failed. Summary: ${SUMMARY}"
	exit 1
fi

record_summary "## Result"
record_summary "Pre-push CI gate passed."
ok "summary: ${SUMMARY#"${REPO_ROOT}"/}"
