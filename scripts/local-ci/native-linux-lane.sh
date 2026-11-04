#!/usr/bin/env bash
# Native Linux sanitizer lane. Configure offline against existing system deps;
# missing dependencies fail closed rather than downloading/building packages.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
PROFILE="ci"
BUILD_DIR=""
LOG_ROOT="${REPO_ROOT}/build/local-ci/native-linux-lane"
DRY_RUN=0

usage() {
	cat <<'USAGE'
Usage: native-linux-lane.sh [options]
  --profile NAME    ci|asan|tsan|self-test (default: ci)
  --build-dir DIR   Isolated build directory (default: build/prepush-linux-PROFILE)
  --log-root DIR    Log directory
  --dry-run         Describe the lane; not sanitizer validation

New build directories use setup.sh Debug --offline --system-deps. Dependencies
must already be available. Existing directories must be native builds of this
checkout with Debug, tests, nofallback, and the requested sanitizers configured.
Both unit and integration suites are mandatory; there is no test-list override.
USAGE
}

while [ "$#" -gt 0 ]; do
	case "$1" in
	--profile | --build-dir | --log-root)
		if [ "$#" -lt 2 ] || [ -z "$2" ]; then
			echo "missing value for $1" >&2
			exit 2
		fi
		case "$1" in
		--profile) PROFILE="$2" ;;
		--build-dir) BUILD_DIR="$2" ;;
		--log-root) LOG_ROOT="$2" ;;
		esac
		shift 2
		;;
	--dry-run) DRY_RUN=1; shift ;;
	-h | --help) usage; exit 0 ;;
	*) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
	esac
 done
case "${PROFILE}" in
ci | asan | tsan | self-test) ;;
*) echo "invalid profile: ${PROFILE}" >&2; exit 2 ;;
esac
if [ "$(uname -s)" != "Linux" ]; then
	echo "native Linux lane requires a Linux host" >&2
	exit 2
fi
BUILD_DIR="${BUILD_DIR:-${REPO_ROOT}/build/prepush-linux-${PROFILE}}"
mkdir -p "${LOG_ROOT}"
RUN_TAG="$(date -u +%Y%m%dT%H%M%SZ)-$$"
SUMMARY="${LOG_ROOT}/summary-${RUN_TAG}.md"
LOG_FILE="${LOG_ROOT}/${RUN_TAG}-${PROFILE}.log"
printf '# Native Linux lane\n\n- Profile: %s\n- Host: %s/%s\n' "${PROFILE}" "$(uname -s)" "$(uname -m)" >"${SUMMARY}"
if [ "${DRY_RUN}" -eq 1 ]; then
	printf 'DRY-RUN ONLY: Linux %s; no sanitizer build/tests run\n' "${PROFILE}" | tee "${LOG_FILE}"
	printf '\n- Result: dry-run (not validation)\n' >>"${SUMMARY}"
	exit 0
fi

run_lane() {
	cd "${REPO_ROOT}"
	if [ "${PROFILE}" = "self-test" ]; then
		test -f meson.build
		test -x setup.sh
		command -v meson >/dev/null
		printf 'SELF-TEST ONLY: Linux wiring; no sanitizer build/tests run\n'
		return 0
	fi
	# No inherited sanitizer suppressions, recovery modes, or interposition.
	for name in LD_PRELOAD ASAN_OPTIONS UBSAN_OPTIONS TSAN_OPTIONS TSAN_SYMBOLIZER_PATH; do
		if [ -n "${!name:-}" ]; then
			echo "refusing sanitizer/runtime override: ${name}" >&2
			return 2
		fi
	done
	setup_args=(Debug --offline --system-deps --with-tests)
	setup_extra="${YAMS_EXTRA_MESON_FLAGS:-}"
	case "${PROFILE}" in
	asan)
		setup_args+=(--asan --no-tsan); multiplier=3; processes=4
		# Clang resolves sanitizer symbols from the executable, not each DSO.
		# Match setup.sh's TSan policy; never inject a foreign runtime library.
		setup_extra+=" -Db_lundef=false"
		;;
	tsan) setup_args+=(--tsan --no-asan); multiplier=10; processes=1 ;;
	ci) setup_args+=(--no-asan --no-tsan); multiplier=2; processes=4 ;;
	esac
	if [ ! -f "${BUILD_DIR}/meson-private/coredata.dat" ]; then
		YAMS_BUILD_DIR="${BUILD_DIR}" YAMS_MESON_WRAP_MODE=nofallback \
			YAMS_EXTRA_MESON_FLAGS="${setup_extra}" ./setup.sh "${setup_args[@]}"
	fi
	# Inspect the effective build, not flag strings or a compiler-version claim.
	python3 - "${BUILD_DIR}" "${PROFILE}" "${REPO_ROOT}" <<'PY'
import json
import subprocess
import sys
from pathlib import Path

build, profile, root = sys.argv[1:]

def info(name):
    return json.loads(subprocess.check_output(
        ["meson", "introspect", build, "--" + name], text=True))

options = {item["name"]: item["value"] for item in info("buildoptions")}
actual = options.get("b_sanitize", [])
if isinstance(actual, str):
    actual = actual.split(",") if actual not in ("", "none") else []
expected = {"asan": {"address", "undefined"}, "tsan": {"thread"}, "ci": set()}[profile]
checks = {
    "requested sanitizers": set(actual) == expected,
    "Debug build": options.get("buildtype") == "debug",
    "tests enabled": options.get("build-tests") is True,
    "no dependency fallbacks": options.get("wrap_mode") == "nofallback",
    "this source checkout": str(Path(root, "meson.build").resolve()) in
        {str(Path(p).resolve()) for p in info("buildsystem-files")},
}
machines = info("machines")
checks["native Linux build"] = all(
    machines["host"].get(key) == machines["build"].get(key)
    for key in ("system", "cpu_family", "endian")) and machines["host"].get("system") == "linux"
suites = {suite.rsplit(":", 1)[-1] for test in info("tests") for suite in test["suite"]}
checks["unit and integration suites present"] = {"unit", "integration"} <= suites
failed = [name for name, ok in checks.items() if not ok]
if failed:
    sys.exit("native Linux preflight failed: " + ", ".join(failed))
print("Native Linux preflight passed: " + profile)
PY
	# These make runtime findings fail the gate; none suppress diagnostics.
	export ASAN_OPTIONS=halt_on_error=1
	export UBSAN_OPTIONS=halt_on_error=1
	export TSAN_OPTIONS=halt_on_error=1
	export YAMS_TEST_SAFE_SINGLE_INSTANCE=1
	meson compile -C "${BUILD_DIR}" -j4
	meson test -C "${BUILD_DIR}" --suite unit --suite integration --print-errorlogs \
		--timeout-multiplier "${multiplier}" --num-processes "${processes}"
}

set +e
(
	set -euo pipefail
	run_lane
) 2>&1 | tee "${LOG_FILE}"
rc=$?
set -e
printf '\n- Exit: %s\n- Log: %s\n' "${rc}" "${LOG_FILE}" >>"${SUMMARY}"
if [ "${PROFILE}" = "self-test" ]; then
	printf -- '- Result: wiring only (not sanitizer validation)\n' >>"${SUMMARY}"
elif [ "${rc}" -eq 0 ]; then
	printf -- '- Result: pass\n' >>"${SUMMARY}"
else
	printf -- '- Result: fail\n' >>"${SUMMARY}"
fi
exit "${rc}"
