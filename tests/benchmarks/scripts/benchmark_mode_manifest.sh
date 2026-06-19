#!/bin/bash
# benchmark_mode_manifest.sh — write a small JSON manifest describing
# benchmark fidelity / realism settings for a run.

set -euo pipefail

OUTPUT_PATH="${1:-}"
if [[ -z "$OUTPUT_PATH" ]]; then
	echo "usage: $0 <output-path>" >&2
	exit 1
fi

RUNNER_NAME="${RUNNER_NAME:-unknown}"
BENCH_BIN="${BENCH_BIN:-unknown}"
BUILD_DIR="${BUILD_DIR:-unknown}"
DATASET="${YAMS_BENCH_DATASET:-unknown}"
CORPUS_SIZE="${YAMS_BENCH_CORPUS_SIZE:-unknown}"
NUM_QUERIES="${YAMS_BENCH_NUM_QUERIES:-unknown}"
TOPK="${YAMS_BENCH_TOPK:-unknown}"
FORCE_MOCK_EMBEDDINGS="${YAMS_BENCH_FORCE_MOCK_EMBEDDINGS:-0}"
USE_WARM_DATA_DIR="${YAMS_BENCH_WARM_DATA_DIR:-0}"
PLUGIN_DIR="${YAMS_PLUGIN_DIR:-}"
SEARCH_STAGE_TRACE="${YAMS_SEARCH_STAGE_TRACE:-0}"
OPT_LOOP="${YAMS_BENCH_OPT_LOOP:-0}"
DISABLE_VECTORS="${YAMS_DISABLE_VECTORS:-0}"
MODE_LABEL="synthetic"

if [[ "$FORCE_MOCK_EMBEDDINGS" != "1" && "$DISABLE_VECTORS" != "1" ]]; then
	MODE_LABEL="live-ish"
fi
if [[ -n "$PLUGIN_DIR" && "$FORCE_MOCK_EMBEDDINGS" != "1" ]]; then
	MODE_LABEL="daemon-faithful"
fi

cat >"$OUTPUT_PATH" <<EOF
{
  "runner": "$RUNNER_NAME",
  "benchmark_binary": "$BENCH_BIN",
  "build_dir": "$BUILD_DIR",
  "mode_label": "$MODE_LABEL",
  "fidelity": {
    "real_daemon_startup": false,
    "real_plugin_discovery": $([[ -n "$PLUGIN_DIR" ]] && echo true || echo false),
    "real_embeddings": $([[ "$FORCE_MOCK_EMBEDDINGS" != "1" && "$DISABLE_VECTORS" != "1" ]] && echo true || echo false),
    "real_post_ingest_queues": false,
    "warm_data_dir_reuse": $([[ "$USE_WARM_DATA_DIR" == "1" ]] && echo true || echo false),
    "synthetic_dataset": $([[ "$DATASET" == synthetic* ]] && echo true || echo false),
    "search_stage_trace": $([[ "$SEARCH_STAGE_TRACE" == "1" ]] && echo true || echo false),
    "optimization_loop": $([[ "$OPT_LOOP" == "1" ]] && echo true || echo false)
  },
  "inputs": {
    "dataset": "$DATASET",
    "corpus_size": "$CORPUS_SIZE",
    "num_queries": "$NUM_QUERIES",
    "topk": "$TOPK"
  },
  "notes": [
    "mode_label is heuristic: daemon-faithful > live-ish > synthetic",
    "Use this manifest to compare benchmark realism before interpreting latency deltas"
  ]
}
EOF

echo "$OUTPUT_PATH"
