#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  tests/scripts/run_simeon_adaptivity_matrix.sh [options]

Runs a cartesian A/B sweep for the simeon adaptivity plan (Phase 0.2):
  backend ∈ {daemon, simeon}
  fusion α ∈ {0.3, 0.5, 0.7}  (vector_weight; text_weight = 1 - α)
  corpus  ∈ {synthetic, yams-docs, arxiv}

Default is a 2×2 smoke: backends={daemon,simeon}, α={0.3,0.7},
corpus=synthetic. Set YAMS_ADAPTIVITY_MATRIX_FULL=1 to run the full
18-cell matrix. Non-synthetic corpora are skipped if their dataset
path is absent (recorded as skipped in manifest).

Options:
  --output-dir <path>       Output directory for JSONL + summary (required)
  --scale <name>            smoke|standard|large (default: smoke)
  --benchmark-bin <path>    Bench binary (default: build/debug/tests/benchmarks/retrieval_quality_bench)
  --yams-docs-path <path>   Path override for yams-docs corpus slice
  --arxiv-path <path>       Path override for arXiv corpus slice
  --backends <list>         Comma-separated backends override (default auto)
  --alphas <list>           Comma-separated α values override (default auto)
  --corpora <list>          Comma-separated corpora override (default auto)
  --opt-candidate <name>    Bench OPT_CANDIDATE to execute per cell (default: auto_baseline)
  -h | --help               Show this message

Environment:
  YAMS_ADAPTIVITY_MATRIX_FULL=1   Run full 18-cell matrix (otherwise 2×2 smoke).
  YAMS_TEST_SAFE_SINGLE_INSTANCE=1 is set automatically if unset.
EOF
}

output_dir=""
scale="smoke"
benchmark_bin="build/debug/tests/benchmarks/retrieval_quality_bench"
yams_docs_path="${YAMS_BENCH_YAMS_DOCS_PATH:-}"
arxiv_path="${YAMS_BENCH_ARXIV_PATH:-}"
backends_override=""
alphas_override=""
corpora_override=""
opt_candidate="${YAMS_ADAPTIVITY_OPT_CANDIDATE:-auto_baseline}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-dir) output_dir="$2"; shift 2 ;;
    --scale) scale="$2"; shift 2 ;;
    --benchmark-bin) benchmark_bin="$2"; shift 2 ;;
    --yams-docs-path) yams_docs_path="$2"; shift 2 ;;
    --arxiv-path) arxiv_path="$2"; shift 2 ;;
    --backends) backends_override="$2"; shift 2 ;;
    --alphas) alphas_override="$2"; shift 2 ;;
    --corpora) corpora_override="$2"; shift 2 ;;
    --opt-candidate) opt_candidate="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage >&2; exit 1 ;;
  esac
done

if [[ -z "$output_dir" ]]; then
  echo "--output-dir is required" >&2
  usage >&2
  exit 1
fi

if [[ ! -x "$benchmark_bin" ]]; then
  echo "Benchmark binary not found or not executable: $benchmark_bin" >&2
  exit 1
fi

mkdir -p "$output_dir"

export YAMS_TEST_SAFE_SINGLE_INSTANCE="${YAMS_TEST_SAFE_SINGLE_INSTANCE:-1}"
export YAMS_ENABLE_ENV_OVERRIDES=1

if [[ "${YAMS_ADAPTIVITY_MATRIX_FULL:-0}" == "1" ]]; then
  default_backends="daemon,simeon"
  default_alphas="0.3,0.5,0.7"
  default_corpora="synthetic,yams-docs,arxiv"
else
  default_backends="daemon,simeon"
  default_alphas="0.3,0.7"
  default_corpora="synthetic"
fi

backends="${backends_override:-$default_backends}"
alphas="${alphas_override:-$default_alphas}"
corpora="${corpora_override:-$default_corpora}"

case "$scale" in
  smoke)    corpus_size=200;   num_queries=25;  topk=10 ;;
  standard) corpus_size=5000;  num_queries=100; topk=10 ;;
  large)    corpus_size=20000; num_queries=250; topk=10 ;;
  *)
    echo "Invalid --scale: $scale (expected smoke|standard|large)" >&2
    exit 1
    ;;
esac

export YAMS_BENCH_CORPUS_SIZE="$corpus_size"
export YAMS_BENCH_NUM_QUERIES="$num_queries"
export YAMS_BENCH_TOPK="$topk"

resolve_corpus() {
  local name="$1"
  case "$name" in
    synthetic)
      echo "dataset=synthetic"
      echo "dataset_path="
      return 0 ;;
    yams-docs)
      if [[ -z "$yams_docs_path" || ! -d "$yams_docs_path" ]]; then
        return 1
      fi
      echo "dataset=yams-docs"
      echo "dataset_path=$yams_docs_path"
      return 0 ;;
    arxiv)
      if [[ -z "$arxiv_path" || ! -d "$arxiv_path" ]]; then
        return 1
      fi
      echo "dataset=arxiv"
      echo "dataset_path=$arxiv_path"
      return 0 ;;
    *)
      return 1 ;;
  esac
}

run_cell() {
  local backend="$1"
  local alpha="$2"
  local corpus="$3"
  local dataset="$4"
  local dataset_path="$5"
  local cell_dir="$output_dir/$backend/alpha_${alpha}/$corpus"
  local cell_jsonl="$cell_dir/bench.jsonl"
  local cell_manifest="$cell_dir/manifest.env"

  mkdir -p "$cell_dir"
  rm -f "$cell_jsonl"

  local text_weight
  text_weight=$(awk -v a="$alpha" 'BEGIN { printf "%.3f", 1.0 - a }')

  (
    export YAMS_EMBED_BACKEND="$backend"
    export YAMS_BENCH_DATASET="$dataset"
    if [[ -n "$dataset_path" ]]; then
      export YAMS_BENCH_DATASET_PATH="$dataset_path"
    fi
    export YAMS_SEARCH_VECTOR_WEIGHT="$alpha"
    export YAMS_SEARCH_TEXT_WEIGHT="$text_weight"
    export YAMS_BENCH_OPT_LOOP=1
    export YAMS_BENCH_OPT_RESULTS_FILE="$cell_jsonl"
    export YAMS_BENCH_OPT_CANDIDATE="${opt_candidate}"
    "$benchmark_bin"
  )

  cat > "$cell_manifest" <<EOF
backend=$backend
fusion_alpha=$alpha
text_weight=$text_weight
corpus=$corpus
dataset=$dataset
dataset_path=$dataset_path
scale=$scale
corpus_size=$corpus_size
num_queries=$num_queries
topk=$topk
benchmark_bin=$benchmark_bin
EOF

  echo "  $cell_jsonl"
}

cells_run=0
cells_skipped=0
skip_log="$output_dir/skipped_cells.log"
: > "$skip_log"

IFS=',' read -r -a backends_arr <<< "$backends"
IFS=',' read -r -a alphas_arr <<< "$alphas"
IFS=',' read -r -a corpora_arr <<< "$corpora"

echo "==> Adaptivity matrix"
echo "    backends: ${backends_arr[*]}"
echo "    alphas:   ${alphas_arr[*]}"
echo "    corpora:  ${corpora_arr[*]}"
echo "    scale:    $scale"
echo "    output:   $output_dir"
echo

for corpus in "${corpora_arr[@]}"; do
  corpus_trimmed="${corpus// /}"
  [[ -z "$corpus_trimmed" ]] && continue

  if ! resolved=$(resolve_corpus "$corpus_trimmed"); then
    echo "    SKIP corpus=$corpus_trimmed (dataset path not available)"
    echo "corpus=$corpus_trimmed reason=dataset_unavailable" >> "$skip_log"
    cells_skipped=$((cells_skipped + ${#backends_arr[@]} * ${#alphas_arr[@]}))
    continue
  fi

  dataset=""
  dataset_path=""
  while IFS='=' read -r key value; do
    case "$key" in
      dataset) dataset="$value" ;;
      dataset_path) dataset_path="$value" ;;
    esac
  done <<< "$resolved"

  for backend in "${backends_arr[@]}"; do
    backend_trimmed="${backend// /}"
    [[ -z "$backend_trimmed" ]] && continue
    for alpha in "${alphas_arr[@]}"; do
      alpha_trimmed="${alpha// /}"
      [[ -z "$alpha_trimmed" ]] && continue

      if [[ "$backend_trimmed" == "daemon" ]] && [[ "${#alphas_arr[@]}" -gt 1 ]]; then
        if [[ "$alpha_trimmed" != "${alphas_arr[0]// /}" ]]; then
          continue
        fi
      fi

      echo "==> cell backend=$backend_trimmed alpha=$alpha_trimmed corpus=$corpus_trimmed"
      run_cell "$backend_trimmed" "$alpha_trimmed" "$corpus_trimmed" "$dataset" "$dataset_path"
      cells_run=$((cells_run + 1))
    done
  done
done

cat > "$output_dir/matrix_manifest.env" <<EOF
backends=$backends
alphas=$alphas
corpora=$corpora
scale=$scale
cells_run=$cells_run
cells_skipped=$cells_skipped
full_matrix=${YAMS_ADAPTIVITY_MATRIX_FULL:-0}
benchmark_bin=$benchmark_bin
EOF

echo
echo "Cells run:     $cells_run"
echo "Cells skipped: $cells_skipped"
echo "Manifest:      $output_dir/matrix_manifest.env"
