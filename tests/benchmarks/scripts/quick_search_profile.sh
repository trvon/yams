#!/bin/bash
# Quick search profile: generate corpus from existing indexed data, run benchmark with search traces
set -euo pipefail

CORPUS=${1:-200}
QUERIES=${2:-50}
DATA_DIR="${YAMS_DATA_DIR:-$HOME/.local/share/yams}"
BENCH_DIR="$(cd "$(dirname "$0")/../../.." && pwd)"
BENCH_BIN="$BENCH_DIR/build/debug/tests/benchmarks/retrieval_quality_bench"

echo "=== Search Pipeline Profile ==="
echo "  Corpus:  $CORPUS docs (synthetic)"
echo "  Queries: $QUERIES"
echo "  Trace:   /tmp/yams-traces/search_profile.trace"

cd "$BENCH_DIR"
YAMS_BENCH_CORPUS_SIZE="$CORPUS" \
	YAMS_BENCH_NUM_QUERIES="$QUERIES" \
	YAMS_BENCH_TOPK=10 \
	YAMS_BENCH_DATASET=synthetic \
	YAMS_TEST_SAFE_SINGLE_INSTANCE=1 \
	YAMS_SEARCH_STAGE_TRACE=1 \
	xcrun xctrace record \
	--template "Time Profiler" \
	--time-limit 120s \
	--output /tmp/yams-traces/search_profile.trace \
	--no-prompt \
	--launch -- "$BENCH_BIN" --benchmark_filter=BM_RetrievalQuality

echo "Done: /tmp/yams-traces/search_profile.trace"
