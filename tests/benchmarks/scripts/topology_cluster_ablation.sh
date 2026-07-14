#!/usr/bin/env bash
set -euo pipefail

# Compatibility wrapper → xplan plan topology_cluster.
# Canonical: tests/benchmarks/xplan/plans/topology_cluster.json
# Worker: retrieval_quality (retrieval_quality_bench).
#
# Production-scale overrides (still env, harness-only):
#   YAMS_BENCH_DATASET / YAMS_BENCH_DATASET_PATH
#   YAMS_BENCH_CORPUS_SIZE / YAMS_BENCH_NUM_QUERIES
#   YAMS_TOPOLOGY_ABLATION_BIN

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
BUILD_DIR="${YAMS_BENCH_BUILD_DIR:-${YAMS_BUILD_DIR:-$ROOT/build/release}}"
XPLAN="$ROOT/tests/benchmarks/xplan/runner.py"

# Map legacy names onto bench env the worker reads.
export YAMS_BENCH_DATASET="${YAMS_TOPOLOGY_ABLATION_DATASET:-${YAMS_BENCH_DATASET:-synthetic}}"
export YAMS_BENCH_DATASET_PATH="${YAMS_TOPOLOGY_ABLATION_DATASET_PATH:-${YAMS_BENCH_DATASET_PATH:-}}"
export YAMS_BENCH_CORPUS_SIZE="${YAMS_TOPOLOGY_ABLATION_CORPUS_SIZE:-${YAMS_BENCH_CORPUS_SIZE:-50}}"
export YAMS_BENCH_NUM_QUERIES="${YAMS_TOPOLOGY_ABLATION_NUM_QUERIES:-${YAMS_BENCH_NUM_QUERIES:-10}}"
export YAMS_BENCH_TOPK="${YAMS_TOPOLOGY_ABLATION_TOPK:-${YAMS_BENCH_TOPK:-10}}"
export YAMS_BENCH_TOPOLOGY_MODE="${YAMS_TOPOLOGY_ABLATION_MODE:-${YAMS_BENCH_TOPOLOGY_MODE:-hybrid_assist}}"

EXTRA=()
if [[ -n "${YAMS_TOPOLOGY_ABLATION_OUT:-}" ]]; then
  EXTRA+=(--out-dir "$YAMS_TOPOLOGY_ABLATION_OUT")
fi
if [[ -n "${YAMS_TOPOLOGY_CLUSTER_ARMS:-}" ]]; then
  # Optional arm filter: space-separated names matching plan arms
  for arm in $YAMS_TOPOLOGY_CLUSTER_ARMS; do
    EXTRA+=(--arm "$arm")
  done
fi

exec python3 "$XPLAN" run topology_cluster --build-dir "$BUILD_DIR" "${EXTRA[@]}" "$@"
