#!/bin/bash
# retrieval_opt_loop.sh — run retrieval_quality_bench optimization loop
# and capture weight-tuning candidates plus best-result summary.
#
# Usage:
#   ./tests/benchmarks/scripts/retrieval_opt_loop.sh [--corpus-size 100] [--queries 20]
#   Default mode prefers the live plugin/embedding path when available.
#   Pass --mock-embeddings for a synthetic fast path.
#
# Output:
#   /tmp/yams_search_opt_<timestamp>.log
#   /tmp/yams_search_opt_<timestamp>.jsonl

set -euo pipefail

CORPUS_SIZE=100
QUERIES=20
TOPK=10
DATASET=synthetic
BUILD_DIR="${YAMS_BUILD_DIR:-build/debug}"
FORCE_MOCK_EMBEDDINGS="${YAMS_BENCH_FORCE_MOCK_EMBEDDINGS:-0}"
STAMP="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="/tmp/yams_search_opt_${STAMP}.log"
JSONL_FILE="/tmp/yams_search_opt_${STAMP}.jsonl"
MANIFEST_FILE="/tmp/yams_search_opt_${STAMP}.manifest.json"

while [[ $# -gt 0 ]]; do
	case "$1" in
	--corpus-size)
		CORPUS_SIZE="$2"
		shift 2
		;;
	--queries)
		QUERIES="$2"
		shift 2
		;;
	--topk)
		TOPK="$2"
		shift 2
		;;
	--dataset)
		DATASET="$2"
		shift 2
		;;
	--build-dir)
		BUILD_DIR="$2"
		shift 2
		;;
	--real-embeddings)
		FORCE_MOCK_EMBEDDINGS=0
		shift
		;;
	--mock-embeddings)
		FORCE_MOCK_EMBEDDINGS=1
		shift
		;;
	*)
		echo "Unknown arg: $1"
		exit 1
		;;
	esac
done

BENCH_BIN="$BUILD_DIR/tests/benchmarks/retrieval_quality_bench"
if [[ ! -f "$BENCH_BIN" ]]; then
	echo "Building retrieval_quality_bench..."
	meson compile -C "$BUILD_DIR" retrieval_quality_bench
fi

echo "=== YAMS Retrieval Weight-Tuning Loop ==="
echo "  Corpus size: $CORPUS_SIZE"
echo "  Queries:     $QUERIES"
echo "  Top K:       $TOPK"
echo "  Dataset:     $DATASET"
echo "  Build dir:   $BUILD_DIR"
echo "  Embeddings:  $([[ "$FORCE_MOCK_EMBEDDINGS" == "1" ]] && echo mock || echo live)"
echo "  Log file:    $LOG_FILE"
echo "  JSONL file:  $JSONL_FILE"
echo "  Manifest:    $MANIFEST_FILE"

RUNNER_NAME="retrieval_opt_loop" \
	BENCH_BIN="$BENCH_BIN" \
	BUILD_DIR="$BUILD_DIR" \
	YAMS_BENCH_DATASET="$DATASET" \
	YAMS_BENCH_CORPUS_SIZE="$CORPUS_SIZE" \
	YAMS_BENCH_NUM_QUERIES="$QUERIES" \
	YAMS_BENCH_TOPK="$TOPK" \
	YAMS_BENCH_FORCE_MOCK_EMBEDDINGS="$FORCE_MOCK_EMBEDDINGS" \
	YAMS_BENCH_OPT_LOOP=1 \
	YAMS_SEARCH_STAGE_TRACE=1 \
	./tests/benchmarks/scripts/benchmark_mode_manifest.sh "$MANIFEST_FILE" >/dev/null

set +e
env \
	YAMS_BENCH_OPT_LOOP=1 \
	YAMS_BENCH_DEBUG_FILE="$JSONL_FILE" \
	YAMS_BENCH_CORPUS_SIZE="$CORPUS_SIZE" \
	YAMS_BENCH_NUM_QUERIES="$QUERIES" \
	YAMS_BENCH_TOPK="$TOPK" \
	YAMS_BENCH_DATASET="$DATASET" \
	YAMS_TEST_SAFE_SINGLE_INSTANCE=1 \
	YAMS_SEARCH_STAGE_TRACE=1 \
	YAMS_BENCH_FORCE_MOCK_EMBEDDINGS="$FORCE_MOCK_EMBEDDINGS" \
	"$BENCH_BIN" 2>&1 | tee "$LOG_FILE"
status=${PIPESTATUS[0]}
set -e

echo
if [[ $status -ne 0 ]]; then
	echo "Benchmark exited with status $status"
	echo "Partial artifacts preserved for inspection."
fi

echo "=== Summary ==="
grep -E "Best candidate:|Recommended env overrides:|timing_tradeoffs=|stage_tradeoffs=|stage_quality=" "$LOG_FILE" || true

if [[ $status -ne 0 ]]; then
	exit $status
fi

echo
echo "Artifacts:"
echo "  $LOG_FILE"
echo "  $JSONL_FILE"
echo "  $MANIFEST_FILE"
