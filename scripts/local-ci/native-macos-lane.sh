#!/usr/bin/env bash
# yams/scripts/local-ci/native-macos-lane.sh
#
# Native macOS build+test lane for pre-push CI and sanitizer gates.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
LOG_ROOT_DEFAULT="${REPO_ROOT}/build/local-ci/native-macos-lane"

PROFILE="ci"
LOG_ROOT="${LOG_ROOT_DEFAULT}"
DRY_RUN=0
RUN_TAG="$(date -u +%Y%m%dT%H%M%SZ)-$$"

usage() {
	cat <<'USAGE'
Usage: native-macos-lane.sh [options]

Profiles:
  self-test  Verify the lane can run on this host without building.
  ci         Native macOS Debug build + unit/integration tests (default).
  asan       Native macOS ASan+UBSan Debug build + unit/integration tests.
  tsan       Native macOS TSan Debug build + unit/integration tests.

Options:
  --profile NAME  self-test|ci (default: ci)
  --log-root DIR  Log directory (default: build/local-ci/native-macos-lane)
  --dry-run       Print commands without executing them
  -h, --help      Show this help

Environment:
  YAMS_MACOS_BUILD_DIR        Build dir override (default depends on profile)
  YAMS_MACOS_COMPILE_TARGETS  Optional space-separated Meson compile targets
  YAMS_MACOS_TEST_ARGS        Meson test args (default: --suite unit --suite integration ...)
USAGE
}

log() { printf '\033[1;34m[macos-lane]\033[0m %s\n' "$*"; }
ok() { printf '\033[1;32m[ ok ]\033[0m %s\n' "$*"; }
fail() { printf '\033[1;31m[fail]\033[0m %s\n' "$*" >&2; }

while [ "$#" -gt 0 ]; do
	case "$1" in
	--profile)
		PROFILE="${2:-}"
		shift 2
		;;
	--log-root)
		LOG_ROOT="${2:-}"
		shift 2
		;;
	--dry-run)
		DRY_RUN=1
		shift
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

case "${PROFILE}" in
self-test | ci | asan | tsan) ;;
*)
	fail "invalid profile: ${PROFILE}"
	echo "valid profiles: self-test, ci, asan, tsan" >&2
	exit 2
	;;
esac

mkdir -p "${LOG_ROOT}"
SUMMARY="${LOG_ROOT}/summary-${RUN_TAG}.md"
LOG_FILE="${LOG_ROOT}/${RUN_TAG}-${PROFILE}.log"

run_ci() {
	cd "${REPO_ROOT}"
	if [ "$(uname -s)" != "Darwin" ]; then
		fail "native macOS lane requires a Darwin host"
		exit 2
	fi

	if [ "${PROFILE}" = "self-test" ]; then
		test -f meson.build
		test -x setup.sh
		command -v git >/dev/null
		printf '[macos] self-test host=%s arch=%s\n' "$(uname -s)" "$(uname -m)"
		return 0
	fi

	git config --global url."https://github.com/".insteadOf "git@github.com:"
	git submodule update --init --depth 1 third_party/sqlite-vec-cpp
	git submodule update --init --depth 1 third_party/simeon
	git submodule update --init --depth 1 third_party/symspell

	case "$(uname -m)" in
	arm64 | aarch64) conan_arch="armv8" ;;
	x86_64) conan_arch="x86_64" ;;
	*)
		echo "unsupported macOS arch: $(uname -m)" >&2
		exit 2
		;;
	esac

	export CI=true
	export RUNNER_OS=macOS
	export YAMS_DISABLE_FAISS=1
	export YAMS_ENABLE_MOBILE_BINDINGS=false
	export YAMS_CONAN_HOST_PROFILE="${YAMS_CONAN_HOST_PROFILE:-./conan/profiles/host-macos-apple-clang}"
	export YAMS_CONAN_ARCH="${YAMS_CONAN_ARCH:-${conan_arch}}"

	setup_args=(Debug)
	case "${PROFILE}" in
	ci)
		export ENABLE_TSAN=false
		export ENABLE_ASAN=false
		export YAMS_BUILD_DIR="${YAMS_MACOS_BUILD_DIR:-build/prepush-macos}"
		export YAMS_EXTRA_MESON_FLAGS="${YAMS_EXTRA_MESON_FLAGS:---buildtype=debug -Dbuild-tests=true -Denable-onnx=disabled -Dtest-timeout-scale=2 -Denable-dcheck=true}"
		setup_args+=(--no-tsan --no-asan)
		;;
	asan)
		export ENABLE_TSAN=false
		export ENABLE_ASAN=true
		export YAMS_BUILD_DIR="${YAMS_MACOS_BUILD_DIR:-build/prepush-asan}"
		export YAMS_EXTRA_MESON_FLAGS="${YAMS_EXTRA_MESON_FLAGS:---buildtype=debug -Dbuild-tests=true -Denable-onnx=disabled -Dtest-timeout-scale=3 -Denable-dcheck=true}"
		setup_args+=(--asan --no-tsan)
		;;
	tsan)
		export ENABLE_TSAN=true
		export ENABLE_ASAN=false
		export YAMS_BUILD_DIR="${YAMS_MACOS_BUILD_DIR:-build/prepush-tsan}"
		export YAMS_EXTRA_MESON_FLAGS="${YAMS_EXTRA_MESON_FLAGS:---buildtype=debug -Dbuild-tests=true -Denable-onnx=disabled -Dtest-timeout-scale=10 -Denable-dcheck=true}"
		setup_args+=(--tsan --no-asan)
		;;
	esac

	conan profile detect --force

	./setup.sh "${setup_args[@]}"

	CONAN_BUILD_ENV=""
	for candidate in \
		"${YAMS_BUILD_DIR}/build-debug/conan/conanbuild.sh" \
		"${YAMS_BUILD_DIR}/conan/conanbuild.sh"; do
		if [ -f "${candidate}" ]; then
			CONAN_BUILD_ENV="${candidate}"
			break
		fi
	done
	if [ -n "${CONAN_BUILD_ENV}" ]; then
		set +u
		# shellcheck source=/dev/null
		. "${CONAN_BUILD_ENV}"
		set -u
	fi

	if [ -n "${YAMS_MACOS_COMPILE_TARGETS:-}" ]; then
		# shellcheck disable=SC2086
		meson compile -C "${YAMS_BUILD_DIR}" ${YAMS_MACOS_COMPILE_TARGETS}
	else
		meson compile -C "${YAMS_BUILD_DIR}"
	fi

	export YAMS_TEST_SAFE_SINGLE_INSTANCE=1
	export YAMS_SQLITE_MINIMAL_PRAGMAS=1
	export YAMS_SQLITE_VEC_INIT_TIMEOUT_MS=1500
	export YAMS_SQLITE_BUSY_TIMEOUT_MS=1000
	export YAMS_TESTING=1

	case "${PROFILE}" in
	asan)
		default_test_args="--suite unit --suite integration --print-errorlogs --timeout-multiplier 3"
		;;
	tsan)
		default_test_args="--suite unit --suite integration --print-errorlogs --timeout-multiplier 10"
		;;
	*)
		default_test_args="--suite unit --suite integration --print-errorlogs --timeout-multiplier 2"
		;;
	esac

	# shellcheck disable=SC2086
	meson test -C "${YAMS_BUILD_DIR}" ${YAMS_MACOS_TEST_ARGS:-${default_test_args}}
}

{
	echo "# YAMS native macOS lane (${PROFILE})"
	echo
	echo "- Started: ${RUN_TAG}"
	echo "- Host: $(uname -s)/$(uname -m)"
	echo
} >"${SUMMARY}"

log "profile=${PROFILE}"
if [ "${DRY_RUN}" -eq 1 ]; then
	echo "DRY-RUN: native macOS ${PROFILE}" | tee "${LOG_FILE}"
	echo "- Result: dry-run" >>"${SUMMARY}"
	ok "summary: ${SUMMARY#"${REPO_ROOT}"/}"
	exit 0
fi

set +e
run_ci > >(tee "${LOG_FILE}") 2> >(tee -a "${LOG_FILE}" >&2)
rc=$?
set -e

if [ "${rc}" -eq 0 ]; then
	echo "- Result: pass" >>"${SUMMARY}"
	echo "- Log: ${LOG_FILE#"${REPO_ROOT}"/}" >>"${SUMMARY}"
	ok "summary: ${SUMMARY#"${REPO_ROOT}"/}"
else
	echo "- Result: fail (${rc})" >>"${SUMMARY}"
	echo "- Log: ${LOG_FILE#"${REPO_ROOT}"/}" >>"${SUMMARY}"
	fail "native macOS lane failed (${rc}); log: ${LOG_FILE}"
	exit "${rc}"
fi
