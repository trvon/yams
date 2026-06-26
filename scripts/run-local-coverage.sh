#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if [[ "${YAMS_SKIP_COVERAGE_HOOK:-0}" == "1" ]]; then
	echo "[coverage] YAMS_SKIP_COVERAGE_HOOK=1; skipping local coverage gate" >&2
	exit 0
fi

case "$(uname -s)" in
Linux | Darwin) ;;
*)
	echo "[coverage] unsupported platform for tracked coverage hook; skipping" >&2
	exit 0
	;;
esac

for tool in bash git python3 gcovr; do
	if ! command -v "$tool" >/dev/null 2>&1; then
		echo "[coverage] required tool '$tool' is missing; install it or set YAMS_SKIP_COVERAGE_HOOK=1 to bypass temporarily" >&2
		exit 1
	fi
done

changed_tmp="$(mktemp)"
trap 'rm -f "$changed_tmp"' EXIT
had_input=0

collect_changed() {
	local range="$1"
	local -a diff_args=()
	if [[ -n "$range" ]]; then
		diff_args+=("$range")
	fi
	git diff --name-only --diff-filter=ACMRT "${diff_args[@]}" |
		grep -E '(\.(c|cc|cpp|cxx|h|hh|hpp)$|(^|/)(meson\.build|meson_options\.txt|setup\.sh|setup\.ps1)$)' \
			>>"$changed_tmp" || true
}

while IFS=' ' read -r local_ref local_sha remote_ref remote_sha; do
	had_input=1
	[[ -z "${local_sha:-}" ]] && continue
	if [[ "$local_sha" =~ ^0+$ ]]; then
		continue
	fi
	if [[ -n "${remote_sha:-}" && ! "$remote_sha" =~ ^0+$ ]]; then
		collect_changed "${remote_sha}..${local_sha}"
	elif git rev-parse "${local_sha}^" >/dev/null 2>&1; then
		collect_changed "${local_sha}^..${local_sha}"
	else
		collect_changed "$local_sha"
	fi
done

if [[ "$had_input" == "0" ]]; then
	if git rev-parse --abbrev-ref --symbolic-full-name '@{upstream}' >/dev/null 2>&1; then
		upstream_ref="$(git rev-parse --abbrev-ref --symbolic-full-name '@{upstream}')"
		collect_changed "${upstream_ref}..HEAD"
	elif git rev-parse HEAD^ >/dev/null 2>&1; then
		collect_changed 'HEAD^..HEAD'
	else
		collect_changed 'HEAD'
	fi
fi

changed=()
while IFS= read -r path; do
	[[ -n "$path" ]] && changed+=("$path")
done < <(sort -u "$changed_tmp")

if [[ ${#changed[@]} -eq 0 ]]; then
	echo "[coverage] no pushed C/C++ or Meson/build-script changes; skipping" >&2
	exit 0
fi

echo "[coverage] running local coverage gate for ${#changed[@]} changed file(s)" >&2
printf '  %s\n' "${changed[@]}" >&2

BUILD_DIR="${YAMS_COVERAGE_BUILD_DIR:-build/coverage}"
JOBS="${YAMS_COVERAGE_HOOK_JOBS:-4}"
TEST_ARGS=(--suite unit --print-errorlogs --timeout-multiplier 2)
if [[ "$BUILD_DIR" != "build/coverage" ]]; then
	echo "[coverage] custom build dir '$BUILD_DIR' selected; warm cache comparisons only make sense when reusing the same build dir because gcov metadata stays build-dir specific" >&2
fi
if [[ "${YAMS_COVERAGE_INCLUDE_INTEGRATION:-0}" == "1" ]]; then
	echo "[coverage] including integration suite (excluding isolated smoke)" >&2
	TEST_ARGS+=(--suite integration --no-suite isolated)
else
	echo "[coverage] unit-only fast path; set YAMS_COVERAGE_INCLUDE_INTEGRATION=1 for integration coverage" >&2
fi

export YAMS_BUILD_DIR="$BUILD_DIR"
export YAMS_ENABLE_MOBILE_BINDINGS=false
export YAMS_EXTRA_MESON_FLAGS="${YAMS_COVERAGE_MESON_FLAGS:--Dbuild-tests=true -Denable-onnx=disabled -Denable-bench-tests=false -Dbuild-benchmarks=false}"
if [[ -z "${SOURCE_DATE_EPOCH:-}" ]]; then
	if source_date_epoch="$(git log -1 --format=%ct HEAD 2>/dev/null)" && [[ -n "$source_date_epoch" ]]; then
		export SOURCE_DATE_EPOCH="$source_date_epoch"
		echo "[coverage] using SOURCE_DATE_EPOCH=$SOURCE_DATE_EPOCH for reproducible version metadata" >&2
	fi
fi

CCACHE_BIN="$(command -v ccache || true)"
C_COMPILER="$(command -v cc || true)"
CPP_COMPILER="$(command -v c++ || true)"
if command -v clang >/dev/null 2>&1 && command -v clang++ >/dev/null 2>&1; then
	C_COMPILER="$(command -v clang)"
	CPP_COMPILER="$(command -v clang++)"
fi

mkdir -p "$BUILD_DIR"
CCACHE_STATS_FILE="$BUILD_DIR/ccache-stats.txt"
GCOVR_SUMMARY_FILE="$BUILD_DIR/gcovr-summary.txt"
if [[ -n "$CCACHE_BIN" ]]; then
	export CCACHE_BASEDIR="${CCACHE_BASEDIR:-$REPO_ROOT}"
	export CCACHE_NOHASHDIR="${CCACHE_NOHASHDIR:-1}"
	echo "[coverage] ccache path normalization enabled (base_dir=$CCACHE_BASEDIR, hash_dir=off)" >&2
fi

FAST_LINKER=""
case "$(uname -s)" in
Darwin)
	if command -v ld.lld >/dev/null 2>&1; then
		FAST_LINKER="lld"
	fi
	;;
*)
	if command -v mold >/dev/null 2>&1; then
		FAST_LINKER="mold"
	elif command -v ld.lld >/dev/null 2>&1; then
		FAST_LINKER="lld"
	fi
	;;
esac

LOCAL_NATIVE_FILE="$BUILD_DIR/local-coverage-native.ini"
if [[ -d "$BUILD_DIR" && (-n "$CCACHE_BIN" || -n "$FAST_LINKER") ]]; then
	toolchain_mismatch=0
	if [[ ! -f "$BUILD_DIR/build.ninja" ]]; then
		toolchain_mismatch=1
	else
		if [[ -n "$CCACHE_BIN" ]] && ! rg -q -- '(^|[[:space:]/])ccache([[:space:]]|$)' "$BUILD_DIR/build.ninja"; then
			toolchain_mismatch=1
		fi
		if [[ -n "$FAST_LINKER" ]] && ! rg -q -- "-fuse-ld=$FAST_LINKER" "$BUILD_DIR/build.ninja"; then
			toolchain_mismatch=1
		fi
	fi
	if [[ "$toolchain_mismatch" == "1" ]]; then
		echo "[coverage] removing stale build dir so Meson can apply compiler/linker overrides at initial setup" >&2
		rm -rf "$BUILD_DIR"
	fi
fi

if [[ -n "$CCACHE_BIN" || -n "$FAST_LINKER" ]]; then
	mkdir -p "$BUILD_DIR"
	{
		echo "[binaries]"
		if [[ -n "$CCACHE_BIN" ]]; then
			printf "c = ['%s', '%s']\n" "$CCACHE_BIN" "$C_COMPILER"
			printf "cpp = ['%s', '%s']\n" "$CCACHE_BIN" "$CPP_COMPILER"
		else
			printf "c = '%s'\n" "$C_COMPILER"
			printf "cpp = '%s'\n" "$CPP_COMPILER"
		fi
		if [[ -n "$FAST_LINKER" ]]; then
			echo "[built-in options]"
			printf "c_link_args = ['-fuse-ld=%s']\n" "$FAST_LINKER"
			printf "cpp_link_args = ['-fuse-ld=%s']\n" "$FAST_LINKER"
		fi
	} >"$LOCAL_NATIVE_FILE"
	export YAMS_EXTRA_MESON_NATIVE_FILES="$LOCAL_NATIVE_FILE"
	echo "[coverage] applying compiler/linker override via initial Meson setup ($LOCAL_NATIVE_FILE)" >&2
fi

./setup.sh Debug --coverage

MESON_BIN="meson"
if [[ -x "$BUILD_DIR/mesonw" ]]; then
	MESON_BIN="$BUILD_DIR/mesonw"
fi

if [[ -n "$CCACHE_BIN" ]]; then
	ccache -z >/dev/null 2>&1 || true
fi
"$MESON_BIN" compile -C "$BUILD_DIR" -j"$JOBS"
if command -v ccache >/dev/null 2>&1; then
	echo "[coverage] ccache stats after compile:" >&2
	ccache -s | tee "$CCACHE_STATS_FILE" >&2 || true
fi
"$MESON_BIN" test -C "$BUILD_DIR" "${TEST_ARGS[@]}"

gcovr \
	--root "$REPO_ROOT" \
	--object-directory "$BUILD_DIR" \
	--exclude 'tests/.*' \
	--exclude 'third_party/.*' \
	--merge-mode-functions merge-use-line-min \
	--txt-summary | tee "$GCOVR_SUMMARY_FILE"

echo "[coverage] local coverage gate passed" >&2
