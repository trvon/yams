#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  tests/scripts/run_topology_scaling.sh [options]

Options:
  --output-dir <path>       Output directory (required)
  --benchmark-bin <path>    Bench binary (default: build/debug/tests/benchmarks/retrieval_quality_bench)
  --num-queries <n>         Query count per cell (default: 50)
  --topk <n>                Top-K (default: 10)
  --dataset <name>          Dataset name (default: synthetic)

Environment:
  YAMS_SCALING_NS           Space-separated corpus sizes (default: "1000 5000 25000 100000")
  YAMS_SCALING_BUNDLES      Space-separated bundle names (default: "baseline tight default wide")

Sweeps the cartesian product of (corpus N) x (frozen topology bundle).
Companion to docs/benchmarks/topology_scaling_matrix.md.
EOF
}

output_dir=""
benchmark_bin="build/debug/tests/benchmarks/retrieval_quality_bench"
num_queries="50"
topk="10"
dataset="synthetic"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-dir) output_dir="$2"; shift 2 ;;
    --benchmark-bin) benchmark_bin="$2"; shift 2 ;;
    --num-queries) num_queries="$2"; shift 2 ;;
    --topk) topk="$2"; shift 2 ;;
    --dataset) dataset="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage >&2; exit 1 ;;
  esac
done

if [[ -z "$output_dir" ]]; then
  echo "--output-dir is required" >&2; usage >&2; exit 1
fi
if [[ ! -x "$benchmark_bin" ]]; then
  echo "Benchmark binary missing: $benchmark_bin" >&2; exit 1
fi
mkdir -p "$output_dir"

scaling_ns="${YAMS_SCALING_NS:-1000 5000 25000 100000}"
bundles="${YAMS_SCALING_BUNDLES:-baseline tight default wide}"
scaling_jsonl="$output_dir/scaling.jsonl"
: > "$scaling_jsonl"

export YAMS_TEST_SAFE_SINGLE_INSTANCE="${YAMS_TEST_SAFE_SINGLE_INSTANCE:-1}"
export YAMS_ENABLE_ENV_OVERRIDES=1
export YAMS_BENCH_DATASET="$dataset"
export YAMS_BENCH_NUM_QUERIES="$num_queries"
export YAMS_BENCH_TOPK="$topk"

apply_bundle() {
  case "$1" in
    baseline)
      unset YAMS_SEARCH_ENABLE_TOPOLOGY_WEAK_ROUTING || true
      unset YAMS_SEARCH_TOPOLOGY_MAX_CLUSTERS || true
      unset YAMS_SEARCH_TOPOLOGY_MAX_DOCS || true
      ;;
    tight)
      export YAMS_SEARCH_ENABLE_TOPOLOGY_WEAK_ROUTING=1
      export YAMS_SEARCH_TOPOLOGY_MAX_CLUSTERS=1
      export YAMS_SEARCH_TOPOLOGY_MAX_DOCS=32
      ;;
    default)
      export YAMS_SEARCH_ENABLE_TOPOLOGY_WEAK_ROUTING=1
      export YAMS_SEARCH_TOPOLOGY_MAX_CLUSTERS=2
      export YAMS_SEARCH_TOPOLOGY_MAX_DOCS=64
      ;;
    wide)
      export YAMS_SEARCH_ENABLE_TOPOLOGY_WEAK_ROUTING=1
      export YAMS_SEARCH_TOPOLOGY_MAX_CLUSTERS=4
      export YAMS_SEARCH_TOPOLOGY_MAX_DOCS=128
      ;;
    *)
      echo "Unknown bundle: $1" >&2; return 1 ;;
  esac
}

run_cell() {
  local n="$1" bundle="$2"
  local raw="$output_dir/raw_n${n}_${bundle}.jsonl"
  rm -f "$raw"
  (
    apply_bundle "$bundle"
    export YAMS_BENCH_CORPUS_SIZE="$n"
    export YAMS_BENCH_OPT_LOOP=1
    export YAMS_BENCH_OPT_RESULTS_FILE="$raw"
    export YAMS_BENCH_OPT_CANDIDATE="auto_baseline"
    if ! "$benchmark_bin" >"$output_dir/log_n${n}_${bundle}.txt" 2>&1; then
      echo "  [skip] N=$n bundle=$bundle: bench exit nonzero (log: $output_dir/log_n${n}_${bundle}.txt)"
      return 0
    fi
  )
  if [[ ! -s "$raw" ]]; then
    echo "  [skip] N=$n bundle=$bundle: no records emitted"
    return 0
  fi
  jq -c \
    --arg n "$n" \
    --arg bundle "$bundle" \
    --arg schema "topology_scaling_v1" \
    'select(.hybrid != null) | {
       schema_version: $schema,
       corpus_n: ($n | tonumber),
       bundle: $bundle,
       hybrid_mrr: .hybrid.mrr,
       hybrid_ndcg_at_k: .hybrid.ndcg_at_k,
       hybrid_recall_at_k: .hybrid.recall_at_k,
       hybrid_map: .hybrid.map,
       hybrid_eval_ms: (.hybrid_eval_ms // null),
       ewma_latency_ms: (.ewma_latency_ms // null),
       num_queries: .hybrid.num_queries,
       run_id: .run_id
     }' "$raw" >>"$scaling_jsonl"
  local count
  count="$(wc -l <"$raw" | tr -d ' ')"
  echo "  [ok]   N=$n bundle=$bundle -> $count raw record(s)"
}

echo "Sweep: Ns=[$scaling_ns]  bundles=[$bundles]"
for n in $scaling_ns; do
  for bundle in $bundles; do
    run_cell "$n" "$bundle"
  done
done

cat > "$output_dir/manifest.env" <<EOF
dataset=$dataset
ns=$scaling_ns
bundles=$bundles
num_queries=$num_queries
topk=$topk
benchmark_bin=$benchmark_bin
EOF

records="$(wc -l <"$scaling_jsonl" | tr -d ' ')"
echo
echo "Wrote: $scaling_jsonl  ($records records)"
echo "Wrote: $output_dir/manifest.env"
