#!/usr/bin/env bash
set -euo pipefail

# Run ingestion pipeline ablations to separate storage/text/FTS work from
# optional KG and embedding enrichment. This is a benchmark harness only; the
# YAMS_BENCH_* toggles are not product configuration knobs.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
BUILD_DIR="${YAMS_BENCH_BUILD_DIR:-$ROOT/build/debug}"
BINARY="${YAMS_BENCH_BINARY:-$BUILD_DIR/tests/benchmarks/ingestion_e2e_bench}"
OUT_DIR="${YAMS_BENCH_OUT_DIR:-$ROOT/build/benchmarks/ingestion-pipeline-ablation/$(date +%Y%m%d-%H%M%S)}"
CORPUS_SIZE="${YAMS_BENCH_CORPUS_SIZE:-100}"
DOC_SIZE="${YAMS_BENCH_DOC_SIZE:-1000}"
POLL_INTERVAL_MS="${YAMS_BENCH_POLL_INTERVAL_MS:-100}"

mkdir -p "$OUT_DIR"

if [[ ! -x "$BINARY" && "${YAMS_BENCH_SKIP_BUILD:-0}" != "1" ]]; then
  meson compile -C "$BUILD_DIR" -j4 bench_ingestion_e2e
fi

if [[ ! -x "$BINARY" ]]; then
  echo "benchmark binary not found/executable: $BINARY" >&2
  exit 2
fi

run_variant() {
  local label="$1"
  shift
  local output="$OUT_DIR/${label}.json"
  echo "==> $label -> $output"
  env \
    YAMS_TEST_SAFE_SINGLE_INSTANCE=1 \
    YAMS_BENCH_CORPUS_SIZE="$CORPUS_SIZE" \
    YAMS_BENCH_DOC_SIZE="$DOC_SIZE" \
    YAMS_BENCH_POLL_INTERVAL_MS="$POLL_INTERVAL_MS" \
    YAMS_BENCH_OUTPUT="$output" \
    "$@" \
    "$BINARY"
}

run_variant baseline
run_variant no_kg YAMS_BENCH_DISABLE_KG=1
run_variant no_vectors YAMS_DISABLE_VECTORS=1
run_variant no_vectors_no_kg YAMS_DISABLE_VECTORS=1 YAMS_BENCH_DISABLE_KG=1
run_variant no_gliner YAMS_DISABLE_GLINER_TITLE_EXTRACTION=1

python3 - "$OUT_DIR" <<'PY'
import json
import pathlib
import sys

out = pathlib.Path(sys.argv[1])
rows = []
for path in sorted(out.glob("*.json")):
    data = json.loads(path.read_text())
    cfg = data.get("test_config", {})
    status = data.get("pipeline_status", {})
    queues = data.get("queues", {})
    post = data.get("post_ingest_phase_timings", {})
    probes = {probe.get("name"): probe for probe in data.get("search_impact", [])}
    keyword = probes.get("keyword", {})
    semantic = probes.get("semantic", {})
    graph = probes.get("graph_rerank_hybrid", {})
    rows.append({
        "variant": path.stem,
        "ms": data.get("total_duration_ms", 0),
        "docs_s": data.get("throughput_docs_per_sec", 0),
        "complete": status.get("complete"),
        "kg_enabled": cfg.get("kg_enabled"),
        "expected_kg": status.get("expected_kg"),
        "observed_kg": status.get("observed_kg"),
        "dropped": queues.get("dropped_batches"),
        "extract_ms": post.get("prepare_metadata_entry", {}).get("total_ms", 0),
        "kg_ms": post.get("kg_graph_component", {}).get("total_ms", 0),
        "dispatch_ms": post.get("dispatch_successes", {}).get("total_ms", 0),
        "embed_prepare_ms": post.get("dispatch_embed_prepare", {}).get("total_ms", 0),
        "keyword_ms": keyword.get("wall_ms", 0),
        "keyword_total": keyword.get("total_count", 0),
        "semantic_ms": semantic.get("wall_ms", 0),
        "semantic_total": semantic.get("total_count", 0),
        "semantic_skip": semantic.get("skip_reason", ""),
        "graph_ms": graph.get("wall_ms", 0),
        "graph_total": graph.get("total_count", 0),
        "graph_skip": graph.get("skip_reason", ""),
    })

print("\nSummary")
print("variant\tms\tdocs/s\tcomplete\tkg\tkg_obs/exp\tdropped\textract_ms\tkg_ms\tdispatch_ms\tembed_prep_ms\tkeyword_ms/results\tsemantic_ms/results\tgraph_ms/results")
for r in rows:
    semantic = r['semantic_skip'] or f"{r['semantic_ms']}/{r['semantic_total']}"
    graph = r['graph_skip'] or f"{r['graph_ms']}/{r['graph_total']}"
    print(
        f"{r['variant']}\t{r['ms']}\t{r['docs_s']:.2f}\t{r['complete']}\t{r['kg_enabled']}\t"
        f"{r['observed_kg']}/{r['expected_kg']}\t{r['dropped']}\t{r['extract_ms']}\t"
        f"{r['kg_ms']}\t{r['dispatch_ms']}\t{r['embed_prepare_ms']}\t"
        f"{r['keyword_ms']}/{r['keyword_total']}\t{semantic}\t{graph}"
    )
PY

echo "\nArtifacts: $OUT_DIR"
