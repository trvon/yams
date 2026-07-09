#!/usr/bin/env bash
set -euo pipefail

# Compatibility wrapper: ingestion pipeline ablation now runs via xplan.
# Canonical plan: tests/benchmarks/xplan/plans/ingest_pipeline.json
# YAMS_BENCH_* toggles remain harness-only (not product configuration knobs).

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
BUILD_DIR="${YAMS_BENCH_BUILD_DIR:-${YAMS_BUILD_DIR:-$ROOT/build/release}}"
XPLAN="$ROOT/tests/benchmarks/xplan/runner.py"
PLAN="${YAMS_XPLAN_PLAN:-ingest_pipeline}"

EXTRA=()
if [[ -n "${YAMS_BENCH_OUT_DIR:-}" ]]; then
  EXTRA+=(--out-dir "$YAMS_BENCH_OUT_DIR")
fi
if [[ "${YAMS_BENCH_SKIP_BUILD:-0}" == "1" ]]; then
  # Worker respects YAMS_BENCH_SKIP_BUILD via env.
  export YAMS_BENCH_SKIP_BUILD=1
fi

# Optional corpus overrides flow through env into worker params defaults;
# the plan fixed params can be overridden by setting these before the binary runs
# (ingestion_e2e worker reads YAMS_BENCH_* from the process environment after merge).
export YAMS_BENCH_CORPUS_SIZE="${YAMS_BENCH_CORPUS_SIZE:-100}"
export YAMS_BENCH_DOC_SIZE="${YAMS_BENCH_DOC_SIZE:-1000}"
export YAMS_BENCH_POLL_INTERVAL_MS="${YAMS_BENCH_POLL_INTERVAL_MS:-100}"

exec python3 "$XPLAN" run "$PLAN" --build-dir "$BUILD_DIR" "${EXTRA[@]}" "$@"
