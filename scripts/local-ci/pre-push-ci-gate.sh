#!/usr/bin/env bash
# yams/scripts/local-ci/pre-push-ci-gate.sh
#
# Blocking pre-push CI gate. Runs host sanitizer builds (ASan+UBSan and TSan)
# before push proceeds. Use --self-test to prove dispatch/host wiring without
# running the expensive build/test work.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
LOG_ROOT_DEFAULT="${REPO_ROOT}/build/local-ci/pre-push-ci-gate"

LOG_ROOT="${LOG_ROOT_DEFAULT}"
SELF_TEST=0
DRY_RUN=0
ASAN_ONLY=0
TSAN_ONLY=0
RUN_TAG="$(date -u +%Y%m%dT%H%M%SZ)-$$"

usage() {
	cat <<'USAGE'
Usage: pre-push-ci-gate.sh [options]

Runs the blocking pre-push CI gate:
  1. Native macOS ASan+UBSan Debug build + unit/integration tests.
  2. Native macOS TSan Debug build + unit/integration tests.

Options:
  --self-test    Verify gate dispatch without expensive builds.
  --dry-run      Print commands without executing them.
  --asan-only    Run only the ASan+UBSan host lane.
  --tsan-only    Run only the TSan host lane.
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
	--asan-only)
		ASAN_ONLY=1
		shift
		;;
	--tsan-only)
		TSAN_ONLY=1
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

if [ "${ASAN_ONLY}" -eq 1 ] && [ "${TSAN_ONLY}" -eq 1 ]; then
	fail "--asan-only and --tsan-only are mutually exclusive"
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

asan_profile="asan"
tsan_profile="tsan"
if [ "${SELF_TEST}" -eq 1 ]; then
	asan_profile="self-test"
	tsan_profile="self-test"
fi

if [ "$(uname -s)" != "Darwin" ]; then
	fail "host sanitizer pre-push gate currently requires a Darwin host"
	FAILURES+=("host-sanitizer-unavailable (2)")
else
	if [ "${TSAN_ONLY}" -ne 1 ]; then
		run_step "native-macos-${asan_profile}" bash "${SCRIPT_DIR}/native-macos-lane.sh" --profile "${asan_profile}" --log-root "${LOG_ROOT}/asan"
	fi
	if [ "${ASAN_ONLY}" -ne 1 ]; then
		run_step "native-macos-${tsan_profile}" bash "${SCRIPT_DIR}/native-macos-lane.sh" --profile "${tsan_profile}" --log-root "${LOG_ROOT}/tsan"
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
