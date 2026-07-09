#!/usr/bin/env bash
set -euo pipefail

# Compatibility wrapper → xplan plan topology_route.
# Canonical: tests/benchmarks/xplan/plans/topology_route.json

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
BUILD_DIR="${YAMS_BENCH_BUILD_DIR:-${YAMS_BUILD_DIR:-$ROOT/build/release}}"
XPLAN="$ROOT/tests/benchmarks/xplan/runner.py"

export YAMS_BENCH_DATASET="${YAMS_TOPOLOGY_ROUTE_DATASET:-${YAMS_BENCH_DATASET:-synthetic}}"
export YAMS_BENCH_DATASET_PATH="${YAMS_TOPOLOGY_ROUTE_DATASET_PATH:-${YAMS_BENCH_DATASET_PATH:-}}"
export YAMS_BENCH_CORPUS_SIZE="${YAMS_TOPOLOGY_ROUTE_CORPUS_SIZE:-${YAMS_BENCH_CORPUS_SIZE:-50}}"
export YAMS_BENCH_NUM_QUERIES="${YAMS_TOPOLOGY_ROUTE_NUM_QUERIES:-${YAMS_BENCH_NUM_QUERIES:-10}}"
export YAMS_BENCH_TOPK="${YAMS_TOPOLOGY_ROUTE_TOPK:-${YAMS_BENCH_TOPK:-10}}"
export YAMS_BENCH_TOPOLOGY_MODE="${YAMS_TOPOLOGY_ROUTE_MODE:-${YAMS_BENCH_TOPOLOGY_MODE:-hybrid_assist}}"
export YAMS_BENCH_TOPOLOGY_ENGINE="${YAMS_TOPOLOGY_ROUTE_ENGINE:-${YAMS_BENCH_TOPOLOGY_ENGINE:-hdbscan}}"
export YAMS_BENCH_TOPOLOGY_FEATURE_SMOOTHING_HOPS="${YAMS_TOPOLOGY_ROUTE_FEATURE_SMOOTHING_HOPS:-0}"

EXTRA=()
if [[ -n "${YAMS_TOPOLOGY_ROUTE_OUT:-}" ]]; then
  EXTRA+=(--out-dir "$YAMS_TOPOLOGY_ROUTE_OUT")
fi
if [[ -n "${YAMS_TOPOLOGY_ROUTE_ARMS:-}" ]]; then
  for arm in $YAMS_TOPOLOGY_ROUTE_ARMS; do
    EXTRA+=(--arm "$arm")
  done
fi

exec python3 "$XPLAN" run topology_route --build-dir "$BUILD_DIR" "${EXTRA[@]}" "$@"
