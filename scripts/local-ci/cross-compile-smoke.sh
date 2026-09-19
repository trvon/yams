#!/usr/bin/env bash
# yams/scripts/local-ci/cross-compile-smoke.sh
#
# Compile-only cross-compile smoke lane. Guards on toolchain availability: skips
# cleanly when no Windows/Linux cross compiler is installed. When one is present,
# compiles a small portability probe with -fsyntax-only to catch narrow-vs-wide
# API mismatches, missing std::jthread, and libstdc++ system_clock range
# differences before they reach GitHub CI.
#
#   --self-test   Prove dispatch and failure propagation with a fake cross compiler.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
PROBE="${SCRIPT_DIR}/portability_probe.cpp"

SELF_TEST=0
while [ "$#" -gt 0 ]; do
	case "$1" in
	--self-test)
		SELF_TEST=1
		shift
		;;
	-h | --help)
		echo "usage: cross-compile-smoke.sh [--self-test]"
		exit 0
		;;
	*)
		echo "unknown argument: $1" >&2
		exit 2
		;;
	esac
done

detect_cross_compilers() {
	local cxx
	for cxx in x86_64-w64-mingw32-g++ i686-w64-mingw32-g++ aarch64-linux-gnu-g++ x86_64-linux-gnu-g++; do
		if command -v "$cxx" >/dev/null 2>&1; then
			printf '%s\n' "$cxx"
		fi
	done
}

compile_with() {
	local cxx="$1"
	echo "[cross-smoke] compiling probe with ${cxx}"
	if "${cxx}" -std=c++20 -fsyntax-only -I "${REPO_ROOT}/include" "${PROBE}"; then
		echo "[cross-smoke] ${cxx}: ok"
		return 0
	fi
	echo "[cross-smoke] ${cxx}: compile failed" >&2
	return 1
}

scan() {
	local compilers failures=0
	compilers="$(detect_cross_compilers)"
	if [ -z "${compilers}" ]; then
		echo "[cross-smoke] no cross compiler (mingw-w64 / linux-gnu) found; skipping"
		return 0
	fi
	for cxx in ${compilers}; do
		if ! compile_with "${cxx}"; then
			failures=$((failures + 1))
		fi
	done
	return "${failures}"
}

self_test() {
	local tmpdir fake args_log
	tmpdir="$(mktemp -d)"
	args_log="${tmpdir}/args.log"
	fake="${tmpdir}/x86_64-w64-mingw32-g++"
	cat >"${fake}" <<'FAKE'
#!/usr/bin/env bash
printf '%s\n' "$@" >>"${YAMS_CROSS_SMOKE_ARGS_LOG:?}"
exit "${YAMS_CROSS_SMOKE_EXIT:-0}"
FAKE
	chmod +x "${fake}"
	export PATH="${tmpdir}:${PATH}"
	export YAMS_CROSS_SMOKE_ARGS_LOG="${args_log}"

	# Dispatch: the fake compiler is detected and receives the expected flags.
	if ! YAMS_CROSS_SMOKE_EXIT=0 scan; then
		echo "[cross-smoke] self-test: dispatch FAILED" >&2
		rm -rf "${tmpdir}"
		return 1
	fi
	for expected in "-fsyntax-only" "-std=c++20" "${REPO_ROOT}/include" "${PROBE}"; do
		if ! grep -qF -- "${expected}" "${args_log}"; then
			echo "[cross-smoke] self-test: missing expected arg ${expected}" >&2
			rm -rf "${tmpdir}"
			return 1
		fi
	done

	# Failure propagation: a failing cross compiler fails the lane.
	if YAMS_CROSS_SMOKE_EXIT=1 scan; then
		echo "[cross-smoke] self-test: failure propagation FAILED" >&2
		rm -rf "${tmpdir}"
		return 1
	fi

	rm -rf "${tmpdir}"
	echo "[cross-smoke] self-test: dispatch + failure propagation ok"
	return 0
}

if [ "${SELF_TEST}" -eq 1 ]; then
	self_test
else
	scan
fi
