#!/usr/bin/env bash
set -euo pipefail

# Detect Tracy capture/export tooling for benchmark automation.
# Writes a small JSON status artifact that benchmark loops can consume.

out="${1:-bench_results/tracy_capture_status.json}"
mkdir -p "$(dirname "$out")"

json_escape() {
	python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))'
}

find_tool() {
	local name
	for name in "$@"; do
		if command -v "$name" >/dev/null 2>&1; then
			command -v "$name"
			return 0
		fi
	done
	return 1
}

profiler=""
if profiler_path=$(find_tool tracy-profiler Tracy); then
	profiler="$profiler_path"
fi

capture=""
if capture_path=$(find_tool tracy-capture capture); then
	capture="$capture_path"
fi

csvexport=""
if csv_path=$(find_tool tracy-csvexport csvexport); then
	csvexport="$csv_path"
fi

status="gui-only"
if [[ -n "$capture" ]]; then
	status="headless-capture-available"
elif [[ -z "$profiler" ]]; then
	status="missing-tracy-tools"
fi

cat >"$out" <<JSON
{
  "status": "$status",
  "profiler": $(printf '%s' "$profiler" | json_escape),
  "capture": $(printf '%s' "$capture" | json_escape),
  "csvexport": $(printf '%s' "$csvexport" | json_escape),
  "headless_capture_available": $([[ -n "$capture" ]] && echo true || echo false),
  "gui_available": $([[ -n "$profiler" ]] && echo true || echo false),
  "notes": "Set YAMS_TRACY_CAPTURE_CMD and YAMS_TRACY_CAPTURE_ARGS for scripts/run_tracy_benchmark.sh when a Tracy capture tool is installed."
}
JSON

cat "$out"

if [[ -z "$capture" ]]; then
	cat >&2 <<'EOF'

No headless Tracy capture tool was found.
Validated fallback:
  1. Build benchmarks with Tracy enabled when available:
     meson setup --reconfigure build/release -Denable-profiling=true
  2. Start the GUI profiler in another terminal:
     tracy-profiler
  3. Run the benchmark wrapper:
     scripts/run_tracy_benchmark.sh write_coordinator_bench

For fully programmatic capture, install/build Tracy's capture tool and run:
  YAMS_TRACY_CAPTURE_CMD=/path/to/capture \
  YAMS_TRACY_CAPTURE_ARGS='<version-specific output args>' \
  scripts/run_tracy_benchmark.sh write_coordinator_bench
EOF
fi
