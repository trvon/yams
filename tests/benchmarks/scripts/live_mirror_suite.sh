#!/bin/bash
# live_mirror_suite.sh — orchestrate a more production-faithful benchmark suite:
# daemon-ish ingestion + retrieval/profile bundle with shared suite metadata.

set -euo pipefail

CORPUS_SIZE=200
QUERIES=50
TOPK=10
DATASET=synthetic
BUILD_DIR="${YAMS_BUILD_DIR:-build/debug}"
FORCE_MOCK_EMBEDDINGS="${YAMS_BENCH_FORCE_MOCK_EMBEDDINGS:-0}"
EMBED_MODEL="${YAMS_BENCH_EMBED_MODEL:-simeon-default}"
STAMP="$(date +%Y%m%d_%H%M%S)"
SUITE_DIR="/tmp/yams_live_mirror_suite_${STAMP}"
TRACE_TIME_LIMIT=300
POLL_INTERVAL_MS=25

while [[ $# -gt 0 ]]; do
	case "$1" in
	--corpus-size)
		CORPUS_SIZE="$2"
		shift 2
		;;
	--queries)
		QUERIES="$2"
		shift 2
		;;
	--topk)
		TOPK="$2"
		shift 2
		;;
	--dataset)
		DATASET="$2"
		shift 2
		;;
	--build-dir)
		BUILD_DIR="$2"
		shift 2
		;;
	--time-limit)
		TRACE_TIME_LIMIT="$2"
		shift 2
		;;
	--embed-model)
		EMBED_MODEL="$2"
		shift 2
		;;
	--mock-embeddings)
		FORCE_MOCK_EMBEDDINGS=1
		shift
		;;
	--real-embeddings)
		FORCE_MOCK_EMBEDDINGS=0
		shift
		;;
	--poll-interval-ms)
		POLL_INTERVAL_MS="$2"
		shift 2
		;;
	--steady-state)
		CORPUS_SIZE=500
		QUERIES=200
		TRACE_TIME_LIMIT=600
		POLL_INTERVAL_MS=10
		shift
		;;
	*)
		echo "Unknown arg: $1"
		exit 1
		;;
	esac
done

mkdir -p "$SUITE_DIR"
INGEST_BIN="$BUILD_DIR/tests/benchmarks/ingestion_e2e_bench"
RETRIEVAL_BIN="$BUILD_DIR/tests/benchmarks/retrieval_quality_bench"
SUITE_MANIFEST="$SUITE_DIR/suite_manifest.json"
INGEST_JSON="$SUITE_DIR/ingestion_e2e.json"
INGEST_LOG="$SUITE_DIR/ingestion_e2e.log"
RETRIEVAL_BUNDLE_DIR="$SUITE_DIR/retrieval_bundle"
SUITE_AUDIT_FILE="$SUITE_DIR/audit_findings.jsonl"
RETRIEVAL_EMBED_FLAG="--real-embeddings"
if [[ "$FORCE_MOCK_EMBEDDINGS" == "1" ]]; then
	RETRIEVAL_EMBED_FLAG="--mock-embeddings"
fi

if [[ ! -f "$INGEST_BIN" ]]; then
	meson compile -C "$BUILD_DIR" ingestion_e2e_bench
fi
if [[ ! -f "$RETRIEVAL_BIN" ]]; then
	meson compile -C "$BUILD_DIR" retrieval_quality_bench
fi

cat >"$SUITE_MANIFEST" <<EOF
{
  "suite": "live_mirror_suite",
  "mode": "$([[ "$FORCE_MOCK_EMBEDDINGS" == "1" ]] && echo synthetic || echo live-ish)",
  "corpus_size": "$CORPUS_SIZE",
  "num_queries": "$QUERIES",
  "topk": "$TOPK",
  "dataset": "$DATASET",
  "build_dir": "$BUILD_DIR",
  "steps": [
    "ingestion_e2e_bench",
    "live_benchmark_bundle(retrieval_quality_bench)"
  ],
  "notes": [
    "Suite pairs daemon-ish ingestion metrics with correlated retrieval/profile artifacts.",
    "Default live path uses simeon-default for fast built-in embeddings; ONNX should stay opt-in.",
    "Use --real-embeddings for higher-fidelity end-to-end runs; --mock-embeddings is synthetic only."
  ]
}
EOF

echo "=== YAMS Live Mirror Suite ==="
echo "  Suite dir:   $SUITE_DIR"
echo "  Corpus size: $CORPUS_SIZE"
echo "  Queries:     $QUERIES"
echo "  Poll ms:     $POLL_INTERVAL_MS"
echo "  Trace sec:   $TRACE_TIME_LIMIT"
echo "  Embeddings:  $([[ "$FORCE_MOCK_EMBEDDINGS" == "1" ]] && echo mock || echo live)"
echo "  Embed model: $EMBED_MODEL"

if [[ "$FORCE_MOCK_EMBEDDINGS" != "1" && "$EMBED_MODEL" == "simeon-default" ]]; then
	export YAMS_EMBED_BACKEND=simeon
fi

echo "[1/2] Running ingestion_e2e_bench..."
set +e
YAMS_BENCH_CORPUS_SIZE="$CORPUS_SIZE" \
	YAMS_BENCH_POLL_INTERVAL_MS="$POLL_INTERVAL_MS" \
	YAMS_BENCH_OUTPUT="$INGEST_JSON" \
	YAMS_BENCH_FORCE_MOCK_EMBEDDINGS="$FORCE_MOCK_EMBEDDINGS" \
	YAMS_DISABLE_VECTORS="$FORCE_MOCK_EMBEDDINGS" \
	YAMS_BENCH_EMBED_MODEL="$EMBED_MODEL" \
	YAMS_TEST_SAFE_SINGLE_INSTANCE=1 \
	"$INGEST_BIN" 2>&1 | tee "$INGEST_LOG"

ingest_status=${PIPESTATUS[0]}
set -e

echo "[2/2] Running retrieval bundle..."
mkdir -p "$RETRIEVAL_BUNDLE_DIR"
set +e
YAMS_BENCH_FORCE_MOCK_EMBEDDINGS="$FORCE_MOCK_EMBEDDINGS" \
	timeout $((TRACE_TIME_LIMIT + 60)) \
	./tests/benchmarks/scripts/live_benchmark_bundle.sh \
	--corpus-size "$CORPUS_SIZE" \
	--queries "$QUERIES" \
	--topk "$TOPK" \
	--dataset "$DATASET" \
	--build-dir "$BUILD_DIR" \
	--time-limit "$TRACE_TIME_LIMIT" \
	--embed-model "$EMBED_MODEL" \
	"$RETRIEVAL_EMBED_FLAG" \
	2>&1 | tee "$SUITE_DIR/retrieval_bundle.log"
retrieval_status=${PIPESTATUS[0]}
set -e

latest_bundle=$(ls -dt /tmp/yams_benchmark_bundle_* 2>/dev/null | head -1 || true)
if [[ -n "$latest_bundle" && -d "$latest_bundle" ]]; then
	rm -rf "$RETRIEVAL_BUNDLE_DIR"
	cp -R "$latest_bundle" "$RETRIEVAL_BUNDLE_DIR"
fi

SUITE_AUDIT_FILE_ENV="$SUITE_AUDIT_FILE" \
	INGEST_JSON_ENV="$INGEST_JSON" \
	RETRIEVAL_BUNDLE_DIR_ENV="$RETRIEVAL_BUNDLE_DIR" \
	python3 - <<'PYEOF'
import json
import os
from datetime import datetime, timezone
from pathlib import Path

audit_path = Path(os.environ["SUITE_AUDIT_FILE_ENV"])
ingest_path = Path(os.environ["INGEST_JSON_ENV"])
bundle_audit = Path(os.environ["RETRIEVAL_BUNDLE_DIR_ENV"]) / "audit_findings.jsonl"

def emit(severity, category, title, evidence, impact, recommendation):
    finding = {
        "event": "audit_finding",
        "runner": "live_mirror_suite",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "severity": severity,
        "category": category,
        "title": title,
        "evidence": evidence,
        "impact": impact,
        "recommendation": recommendation,
    }
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    with audit_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(finding, sort_keys=True) + "\n")

if ingest_path.exists():
    try:
        data = json.loads(ingest_path.read_text(encoding="utf-8"))
        total = float(data.get("total_duration_ms") or 0)
        stages = data.get("stages") or {}
        metadata_ms = float((stages.get("metadata_storage") or {}).get("duration_ms") or 0)
        queues = data.get("queues") or {}
        max_store = int(queues.get("max_store_document_tasks") or 0)
        max_embed = int(queues.get("max_embed_jobs") or 0)
        if total > 0 and metadata_ms / total >= 0.40:
            emit(
                "info",
                "ingestion_hotspot",
                "metadata storage dominates live-mirror ingestion time",
                f"metadata_storage={metadata_ms:.0f}ms total_duration={total:.0f}ms share={metadata_ms / total:.2%}",
                "Ingestion optimization should start in metadata/content indexing before embedding inference for this Simeon run.",
                "Inspect batchInsertContentAndIndex transaction phases and store-document metadata work before changing embedding concurrency.",
            )
        if max_store >= 100 and max_store > max_embed * 10:
            emit(
                "warning",
                "ingestion_backpressure",
                "store-document task backlog exceeds downstream embedding queue depth",
                f"max_store_document_tasks={max_store} max_embed_jobs={max_embed}",
                "Backpressure appears before or at document storage/metadata admission, not in embedding execution.",
                "Measure DocumentService::store and post-ingest commit boundaries; avoid optimizing retrieval from this ingestion evidence.",
            )
    except Exception as exc:
        emit(
            "warning",
            "harness_audit",
            "failed to derive ingestion audit findings",
            f"{type(exc).__name__}: {exc}",
            "The suite may be missing structured ingestion findings even though raw artifacts are preserved.",
            "Inspect ingestion_e2e.json manually and fix the audit derivation if this repeats.",
        )

if bundle_audit.exists():
    with bundle_audit.open("r", encoding="utf-8") as src, audit_path.open("a", encoding="utf-8") as dst:
        for line in src:
            dst.write(line)
PYEOF

echo "Suite artifacts:"
find "$SUITE_DIR" -maxdepth 2 | sort

if [[ $ingest_status -ne 0 || $retrieval_status -ne 0 ]]; then
	echo "Suite completed with nonzero status: ingestion=$ingest_status retrieval=$retrieval_status"
	exit 1
fi
