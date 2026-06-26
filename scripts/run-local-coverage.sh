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
export YAMS_BUILD_DIR="$BUILD_DIR"
export YAMS_ENABLE_MOBILE_BINDINGS=false
export YAMS_EXTRA_MESON_FLAGS="${YAMS_COVERAGE_MESON_FLAGS:--Dbuild-tests=true -Denable-onnx=disabled -Denable-bench-tests=false -Dbuild-benchmarks=false}"

./setup.sh Debug --coverage

MESON_BIN="meson"
if [[ -x "$BUILD_DIR/mesonw" ]]; then
	MESON_BIN="$BUILD_DIR/mesonw"
fi

"$MESON_BIN" compile -C "$BUILD_DIR" -j"$JOBS"
"$MESON_BIN" test -C "$BUILD_DIR" --suite unit --suite integration --print-errorlogs --timeout-multiplier 2

gcovr \
	--root "$REPO_ROOT" \
	--object-directory "$BUILD_DIR" \
	--exclude 'tests/.*' \
	--exclude 'third_party/.*' \
	--merge-mode-functions merge-use-line-min \
	--txt-summary

echo "[coverage] local coverage gate passed" >&2
