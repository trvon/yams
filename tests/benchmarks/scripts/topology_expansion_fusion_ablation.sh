#!/usr/bin/env bash
set -euo pipefail

# Compatibility wrapper → xplan plan topology_expansion.
# Expansion-arm presets (full64/medoid64/cap8/cap2/rerank_only) live in
# tests/benchmarks/xplan/workers/retrieval_quality.py (EXPANSION_PRESETS).
# Canonical plan: tests/benchmarks/xplan/plans/topology_expansion.json
#
# Legacy env still honored:
#   YAMS_TOPOLOGY_EXPANSION_BIN / YAMS_TOPOLOGY_EXPANSION_OUT
#   YAMS_TOPOLOGY_EXPANSION_ARMS  (space-separated arm filter)
#   YAMS_TOPOLOGY_EXPANSION_DATASET / CORPUS_SIZE / NUM_QUERIES / ENGINE
#   YAMS_BENCH_TOPOLOGY_SOURCE / YAMS_BENCH_SEED_SEMANTIC_NEIGHBORS

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
BUILD_DIR="${YAMS_BENCH_BUILD_DIR:-${YAMS_BUILD_DIR:-$ROOT/build/release}}"
XPLAN="$ROOT/tests/benchmarks/xplan/runner.py"

if [[ -n "${YAMS_TOPOLOGY_EXPANSION_BIN:-}" ]]; then
  export YAMS_BENCH_BIN="$YAMS_TOPOLOGY_EXPANSION_BIN"
fi

export YAMS_BENCH_DATASET="${YAMS_TOPOLOGY_EXPANSION_DATASET:-${YAMS_BENCH_DATASET:-synthetic}}"
export YAMS_BENCH_DATASET_PATH="${YAMS_TOPOLOGY_EXPANSION_DATASET_PATH:-${YAMS_BENCH_DATASET_PATH:-}}"
export YAMS_BENCH_CORPUS_SIZE="${YAMS_TOPOLOGY_EXPANSION_CORPUS_SIZE:-${YAMS_BENCH_CORPUS_SIZE:-50}}"
export YAMS_BENCH_NUM_QUERIES="${YAMS_TOPOLOGY_EXPANSION_NUM_QUERIES:-${YAMS_BENCH_NUM_QUERIES:-10}}"
export YAMS_BENCH_TOPK="${YAMS_TOPOLOGY_EXPANSION_TOPK:-${YAMS_BENCH_TOPK:-10}}"
export YAMS_BENCH_TOPOLOGY_ENGINE="${YAMS_TOPOLOGY_EXPANSION_ENGINE:-${YAMS_BENCH_TOPOLOGY_ENGINE:-connected}}"

EXTRA=()
if [[ -n "${YAMS_TOPOLOGY_EXPANSION_OUT:-}" ]]; then
  EXTRA+=(--out-dir "$YAMS_TOPOLOGY_EXPANSION_OUT")
fi
if [[ -n "${YAMS_TOPOLOGY_EXPANSION_ARMS:-}" ]]; then
  for arm in $YAMS_TOPOLOGY_EXPANSION_ARMS; do
    EXTRA+=(--arm "$arm")
  done
fi

exec python3 "$XPLAN" run topology_expansion --build-dir "$BUILD_DIR" "${EXTRA[@]}" "$@"
