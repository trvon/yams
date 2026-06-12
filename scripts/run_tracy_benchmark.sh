#!/usr/bin/env bash
set -euo pipefail

# Run a YAMS benchmark with optional Tracy headless capture.
#
# Why this is env-driven:
# Tracy's GUI/client integration is stable, but the packaged headless capture
# tool name and flags vary by install/version (often `capture` or
# `tracy-capture`). Set YAMS_TRACY_CAPTURE_CMD and YAMS_TRACY_CAPTURE_ARGS to
# the exact capture invocation for your Tracy build.
#
# Example:
#   YAMS_TRACY_CAPTURE_CMD=/path/to/tracy/capture \
#   YAMS_TRACY_CAPTURE_ARGS='-o bench_results/write_coordinator.tracy' \
#   scripts/run_tracy_benchmark.sh write_coordinator_bench

bench_name="${1:-write_coordinator_bench}"
build_dir="${YAMS_BUILD_DIR:-build/release}"
output_dir="${YAMS_BENCH_RESULTS_DIR:-bench_results}"
mkdir -p "$output_dir"

capture_cmd="${YAMS_TRACY_CAPTURE_CMD:-}"
if [[ -z "$capture_cmd" ]]; then
	if command -v tracy-capture >/dev/null 2>&1; then
		capture_cmd="$(command -v tracy-capture)"
	elif command -v capture >/dev/null 2>&1; then
		capture_cmd="$(command -v capture)"
	fi
fi

capture_pid=""
if [[ -n "$capture_cmd" ]]; then
	capture_args="${YAMS_TRACY_CAPTURE_ARGS:-}"
	if [[ -z "$capture_args" ]]; then
		cat >&2 <<EOF
Found Tracy capture tool: $capture_cmd
Set YAMS_TRACY_CAPTURE_ARGS for your Tracy version, for example an output path
argument such as: -o $output_dir/${bench_name}.tracy
EOF
		exit 2
	fi
	echo "Starting Tracy capture: $capture_cmd $capture_args" >&2
	# shellcheck disable=SC2086 # intentional user-provided args string
	"$capture_cmd" $capture_args &
	capture_pid="$!"
	sleep "${YAMS_TRACY_CAPTURE_STARTUP_SEC:-1}"
else
	cat >&2 <<EOF
No headless Tracy capture tool found on PATH.
The benchmark will still run. To capture a trace programmatically, install/build
Tracy's capture tool and set:
  YAMS_TRACY_CAPTURE_CMD=/path/to/capture
  YAMS_TRACY_CAPTURE_ARGS='<version-specific capture args>'
Or open tracy-profiler/Tracy GUI before running this script.
EOF
fi

cleanup() {
	if [[ -n "$capture_pid" ]] && kill -0 "$capture_pid" >/dev/null 2>&1; then
		kill "$capture_pid" >/dev/null 2>&1 || true
		wait "$capture_pid" >/dev/null 2>&1 || true
	fi
}
trap cleanup EXIT

case "$bench_name" in
write_coordinator_bench)
	: "${YAMS_BENCH_OUTPUT:=$output_dir/write_coordinator_tracy.jsonl}"
	export YAMS_BENCH_OUTPUT
	: "${YAMS_BENCH_NUM_FILES:=1000}"
	: "${YAMS_BENCH_FILE_SIZE_BYTES:=4096}"
	: "${YAMS_BENCH_VERSION_ITERATIONS:=3}"
	: "${YAMS_BENCH_REPEAT:=3}"
	export YAMS_BENCH_NUM_FILES YAMS_BENCH_FILE_SIZE_BYTES YAMS_BENCH_VERSION_ITERATIONS YAMS_BENCH_REPEAT
	;;
esac

echo "Running benchmark '$bench_name' in $build_dir" >&2
meson test -C "$build_dir" "$bench_name" --print-errorlogs

echo "Benchmark complete. JSON output: ${YAMS_BENCH_OUTPUT:-<benchmark default>}" >&2
