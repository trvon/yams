#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  tests/scripts/run_topology_retrieval_matrix.sh [options]

Options:
  --dataset <name>          Dataset name for retrieval_quality_bench (default: synthetic)
  --dataset-path <path>     Optional dataset path for BEIR-style datasets
  --output-dir <path>       Output directory for JSONL and summary (required)
  --scale <name>            smoke|standard|large (default: large)
  --corpus-size <n>         Synthetic corpus size override
  --num-queries <n>         Query count override
  --topk <n>                Top-K override
  --benchmark-bin <path>    Benchmark binary path
  --embedding-backends <list>
                            Comma-separated list of YAMS_EMBED_BACKEND values
                            (e.g. "daemon,simeon"). When provided, the full
                            matrix is run once per backend under
                            <output-dir>/<backend>/. When omitted, a single
                            run is written directly under <output-dir>.

Environment:
  YAMS_TEST_SAFE_SINGLE_INSTANCE=1 is set automatically if unset.
EOF
}

dataset="synthetic"
dataset_path=""
output_dir=""
scale="large"
corpus_size=""
num_queries=""
topk=""
benchmark_bin="build/debug/tests/benchmarks/retrieval_quality_bench"
embedding_backends=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dataset)
      dataset="$2"
      shift 2
      ;;
    --dataset-path)
      dataset_path="$2"
      shift 2
      ;;
    --output-dir)
      output_dir="$2"
      shift 2
      ;;
    --scale)
      scale="$2"
      shift 2
      ;;
    --corpus-size)
      corpus_size="$2"
      shift 2
      ;;
    --num-queries)
      num_queries="$2"
      shift 2
      ;;
    --topk)
      topk="$2"
      shift 2
      ;;
    --benchmark-bin)
      benchmark_bin="$2"
      shift 2
      ;;
    --embedding-backends)
      embedding_backends="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$output_dir" ]]; then
  echo "--output-dir is required" >&2
  usage >&2
  exit 1
fi

mkdir -p "$output_dir"

if [[ ! -x "$benchmark_bin" ]]; then
  echo "Benchmark binary not found or not executable: $benchmark_bin" >&2
  exit 1
fi

export YAMS_TEST_SAFE_SINGLE_INSTANCE="${YAMS_TEST_SAFE_SINGLE_INSTANCE:-1}"
export YAMS_ENABLE_ENV_OVERRIDES=1
export YAMS_BENCH_DATASET="$dataset"

if [[ "${YAMS_BENCH_FORCE_MOCK_EMBEDDINGS:-0}" == "1" ]]; then
  export YAMS_POST_EMBED_CONCURRENT="${YAMS_POST_EMBED_CONCURRENT:-1}"
  export YAMS_POST_EXTRACTION_CONCURRENT="${YAMS_POST_EXTRACTION_CONCURRENT:-2}"
  export YAMS_POST_KG_CONCURRENT="${YAMS_POST_KG_CONCURRENT:-1}"
fi

apply_scale_defaults() {
  case "$scale" in
    smoke)
      if [[ "$dataset" == "synthetic" ]]; then
        [[ -z "$corpus_size" ]] && corpus_size="200"
        [[ -z "$num_queries" ]] && num_queries="25"
      else
        [[ -z "$num_queries" ]] && num_queries="25"
      fi
      [[ -z "$topk" ]] && topk="10"
      ;;
    standard)
      if [[ "$dataset" == "synthetic" ]]; then
        [[ -z "$corpus_size" ]] && corpus_size="5000"
        [[ -z "$num_queries" ]] && num_queries="100"
      else
        [[ -z "$num_queries" ]] && num_queries="100"
      fi
      [[ -z "$topk" ]] && topk="10"
      ;;
    large)
      if [[ "$dataset" == "synthetic" ]]; then
        [[ -z "$corpus_size" ]] && corpus_size="20000"
        [[ -z "$num_queries" ]] && num_queries="250"
      fi
      [[ -z "$topk" ]] && topk="10"
      ;;
    *)
      echo "Invalid --scale: $scale (expected smoke|standard|large)" >&2
      exit 1
      ;;
  esac
}

apply_scale_defaults

if [[ -n "$dataset_path" ]]; then
  export YAMS_BENCH_DATASET_PATH="$dataset_path"
fi
if [[ -n "$corpus_size" ]]; then
  export YAMS_BENCH_CORPUS_SIZE="$corpus_size"
fi
if [[ -n "$num_queries" ]]; then
  export YAMS_BENCH_NUM_QUERIES="$num_queries"
fi
if [[ -n "$topk" ]]; then
  export YAMS_BENCH_TOPK="$topk"
fi

run_candidate() {
  local name="$1"
  local out_jsonl="$2"
  shift 2
  rm -f "$out_jsonl"
  (
    export YAMS_BENCH_OPT_LOOP=1
    export YAMS_BENCH_OPT_RESULTS_FILE="$out_jsonl"
    export YAMS_BENCH_OPT_CANDIDATE="$name"
    "$@"
  )
}

run_matrix() {
  local target_dir="$1"
  mkdir -p "$target_dir"
  local baseline_jsonl="$target_dir/baseline.jsonl"
  local topology_tight_jsonl="$target_dir/topology_tight.jsonl"
  local topology_default_jsonl="$target_dir/topology_default.jsonl"
  local topology_wide_jsonl="$target_dir/topology_wide.jsonl"
  local summary_json="$target_dir/summary.json"
  local delta_json="$target_dir/delta_vs_baseline.json"
  local delta_csv="$target_dir/delta_vs_baseline.csv"

  run_candidate "auto_baseline" "$baseline_jsonl" "$benchmark_bin"

  (
    export YAMS_SEARCH_ENABLE_TOPOLOGY_WEAK_ROUTING=1
    export YAMS_SEARCH_TOPOLOGY_MAX_CLUSTERS=1
    export YAMS_SEARCH_TOPOLOGY_MAX_DOCS=32
    run_candidate "auto_baseline" "$topology_tight_jsonl" "$benchmark_bin"
  )

  (
    export YAMS_SEARCH_ENABLE_TOPOLOGY_WEAK_ROUTING=1
    export YAMS_SEARCH_TOPOLOGY_MAX_CLUSTERS=2
    export YAMS_SEARCH_TOPOLOGY_MAX_DOCS=64
    run_candidate "auto_baseline" "$topology_default_jsonl" "$benchmark_bin"
  )

  (
    export YAMS_SEARCH_ENABLE_TOPOLOGY_WEAK_ROUTING=1
    export YAMS_SEARCH_TOPOLOGY_MAX_CLUSTERS=4
    export YAMS_SEARCH_TOPOLOGY_MAX_DOCS=128
    run_candidate "auto_baseline" "$topology_wide_jsonl" "$benchmark_bin"
  )

  python3 tests/scripts/summarize_retrieval_opt_jsonl.py \
    --input "$baseline_jsonl" \
    --input "$topology_tight_jsonl" \
    --input "$topology_default_jsonl" \
    --input "$topology_wide_jsonl" \
    --output "$summary_json"

  python3 tests/scripts/delta_retrieval_matrix.py \
    --input "$summary_json" \
    --baseline auto_baseline \
    --json-out "$delta_json" \
    --csv-out "$delta_csv"

  cat > "$target_dir/manifest.env" <<EOF
dataset=$dataset
dataset_path=$dataset_path
scale=$scale
corpus_size=$corpus_size
num_queries=$num_queries
topk=$topk
benchmark_bin=$benchmark_bin
embedding_backend=${YAMS_EMBED_BACKEND:-}
EOF

  echo "Wrote:"
  echo "  $baseline_jsonl"
  echo "  $topology_tight_jsonl"
  echo "  $topology_default_jsonl"
  echo "  $topology_wide_jsonl"
  echo "  $summary_json"
  echo "  $delta_json"
  echo "  $delta_csv"
  echo "  $target_dir/manifest.env"
}

if [[ -n "$embedding_backends" ]]; then
  IFS=',' read -r -a backends_arr <<< "$embedding_backends"
  for backend in "${backends_arr[@]}"; do
    backend_trimmed="${backend// /}"
    [[ -z "$backend_trimmed" ]] && continue
    echo "==> Running matrix with YAMS_EMBED_BACKEND=$backend_trimmed"
    (
      export YAMS_EMBED_BACKEND="$backend_trimmed"
      run_matrix "$output_dir/$backend_trimmed"
    )
  done
else
  run_matrix "$output_dir"
fi
