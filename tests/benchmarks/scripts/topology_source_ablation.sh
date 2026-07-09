#!/usr/bin/env bash
set -euo pipefail

# Compatibility wrapper → xplan plan topology_source.
# Canonical: tests/benchmarks/xplan/plans/topology_source.json
# Worker: retrieval_quality (expansion presets + construction certificates).

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
BUILD_DIR="${YAMS_BENCH_BUILD_DIR:-${YAMS_BUILD_DIR:-$ROOT/build/release}}"
XPLAN="$ROOT/tests/benchmarks/xplan/runner.py"

if [[ -n "${YAMS_TOPOLOGY_EXPANSION_BIN:-}" ]]; then
  export YAMS_BENCH_BIN="$YAMS_TOPOLOGY_EXPANSION_BIN"
fi

export YAMS_BENCH_SEED_SEMANTIC_NEIGHBORS="${YAMS_BENCH_SEED_SEMANTIC_NEIGHBORS:-1}"
export YAMS_BENCH_CORPUS_SIZE="${YAMS_BENCH_CORPUS_SIZE:-50}"
export YAMS_BENCH_NUM_QUERIES="${YAMS_TOPOLOGY_EXPANSION_NUM_QUERIES:-${YAMS_BENCH_NUM_QUERIES:-10}}"

EXTRA=()
if [[ -n "${YAMS_TOPOLOGY_SOURCE_OUT:-}" ]]; then
  EXTRA+=(--out-dir "$YAMS_TOPOLOGY_SOURCE_OUT")
fi

# Default plan matrix: topology_source × expansion_arm
# (vector,fts5,kg) × (cap2,medoid64,rerank_only).
# Override with a custom plan if you need the old YAMS_TOPOLOGY_SOURCE_ARMS lists.

exec python3 "$XPLAN" run topology_source --build-dir "$BUILD_DIR" "${EXTRA[@]}" "$@"
