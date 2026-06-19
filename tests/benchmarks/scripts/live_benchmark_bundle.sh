#!/bin/bash
# live_benchmark_bundle.sh — run a live-ish retrieval benchmark and collect
# one correlated artifact bundle: benchmark stdout/stderr, manifest, stage-trace
# JSONL, xctrace trace, and exported profile XML.

set -euo pipefail

CORPUS_SIZE=200
QUERIES=50
TOPK=10
DATASET=synthetic
BUILD_DIR="${YAMS_BUILD_DIR:-build/debug}"
TRACE_TIME_LIMIT=300
FORCE_MOCK_EMBEDDINGS="${YAMS_BENCH_FORCE_MOCK_EMBEDDINGS:-0}"
EMBED_MODEL="${YAMS_BENCH_EMBED_MODEL:-simeon-default}"
STAMP="$(date +%Y%m%d_%H%M%S)"
BUNDLE_DIR="/tmp/yams_benchmark_bundle_${STAMP}"

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
	--steady-state)
		CORPUS_SIZE=500
		QUERIES=200
		TRACE_TIME_LIMIT=600
		shift
		;;
	*)
		echo "Unknown arg: $1"
		exit 1
		;;
	esac
done

mkdir -p "$BUNDLE_DIR"
BENCH_BIN="$BUILD_DIR/tests/benchmarks/retrieval_quality_bench"
LOG_FILE="$BUNDLE_DIR/benchmark.log"
JSONL_FILE="$BUNDLE_DIR/stage_trace.jsonl"
TRACE_OUT="$BUNDLE_DIR/profile.trace"
PROFILE_XML="$BUNDLE_DIR/profile.xml"
MANIFEST_FILE="$BUNDLE_DIR/manifest.json"
HOT_ZONES_FILE="$BUNDLE_DIR/hot_zones.txt"
AUDIT_FILE="$BUNDLE_DIR/audit_findings.jsonl"

emit_audit_finding() {
	local severity="$1"
	local category="$2"
	local title="$3"
	local evidence="$4"
	local impact="$5"
	local recommendation="$6"
	AUDIT_FILE_ENV="$AUDIT_FILE" \
		FINDING_SEVERITY="$severity" \
		FINDING_CATEGORY="$category" \
		FINDING_TITLE="$title" \
		FINDING_EVIDENCE="$evidence" \
		FINDING_IMPACT="$impact" \
		FINDING_RECOMMENDATION="$recommendation" \
		python3 - <<'PYEOF'
import json
import os
from datetime import datetime, timezone
finding = {
    "event": "audit_finding",
    "runner": "live_benchmark_bundle",
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "severity": os.environ["FINDING_SEVERITY"],
    "category": os.environ["FINDING_CATEGORY"],
    "title": os.environ["FINDING_TITLE"],
    "evidence": os.environ["FINDING_EVIDENCE"],
    "impact": os.environ["FINDING_IMPACT"],
    "recommendation": os.environ["FINDING_RECOMMENDATION"],
}
with open(os.environ["AUDIT_FILE_ENV"], "a", encoding="utf-8") as f:
    f.write(json.dumps(finding, sort_keys=True) + "\n")
PYEOF
}

if [[ ! -f "$BENCH_BIN" ]]; then
	meson compile -C "$BUILD_DIR" retrieval_quality_bench
fi

export YAMS_BENCH_CORPUS_SIZE="$CORPUS_SIZE"
export YAMS_BENCH_NUM_QUERIES="$QUERIES"
export YAMS_BENCH_TOPK="$TOPK"
export YAMS_BENCH_DATASET="$DATASET"
export YAMS_TEST_SAFE_SINGLE_INSTANCE=1
export YAMS_SEARCH_STAGE_TRACE=1
export YAMS_BENCH_DEBUG_FILE="$JSONL_FILE"
export YAMS_BENCH_FORCE_MOCK_EMBEDDINGS="$FORCE_MOCK_EMBEDDINGS"
export YAMS_BENCH_EMBED_MODEL="$EMBED_MODEL"
if [[ "$FORCE_MOCK_EMBEDDINGS" != "1" && "$EMBED_MODEL" == "simeon-default" ]]; then
	export YAMS_EMBED_BACKEND=simeon
fi

RUNNER_NAME="live_benchmark_bundle" \
	BENCH_BIN="$BENCH_BIN" \
	BUILD_DIR="$BUILD_DIR" \
	YAMS_BENCH_DATASET="$YAMS_BENCH_DATASET" \
	YAMS_BENCH_CORPUS_SIZE="$YAMS_BENCH_CORPUS_SIZE" \
	YAMS_BENCH_NUM_QUERIES="$YAMS_BENCH_NUM_QUERIES" \
	YAMS_BENCH_TOPK="$YAMS_BENCH_TOPK" \
	YAMS_BENCH_FORCE_MOCK_EMBEDDINGS="$YAMS_BENCH_FORCE_MOCK_EMBEDDINGS" \
	YAMS_SEARCH_STAGE_TRACE="$YAMS_SEARCH_STAGE_TRACE" \
	./tests/benchmarks/scripts/benchmark_mode_manifest.sh "$MANIFEST_FILE" >/dev/null

echo "=== YAMS Live Benchmark Bundle ==="
echo "  Bundle dir:  $BUNDLE_DIR"
echo "  Benchmark:   $BENCH_BIN"
echo "  Dataset:     $DATASET"
echo "  Corpus:      $CORPUS_SIZE"
echo "  Queries:     $QUERIES"
echo "  Embeddings:  $([[ "$FORCE_MOCK_EMBEDDINGS" == "1" ]] && echo mock || echo live)"
echo "  Embed model: $EMBED_MODEL"

echo "Recording xctrace + benchmark log..."
set +e
xcrun xctrace record \
	--template "Time Profiler" \
	--time-limit "${TRACE_TIME_LIMIT}s" \
	--output "$TRACE_OUT" \
	--no-prompt \
	--launch -- "$BENCH_BIN" --benchmark_filter=BM_RetrievalQuality \
	2>&1 | tee "$LOG_FILE"
status=${PIPESTATUS[0]}
set -e

profile_capture_failed=0
if [[ $status -ne 0 ]] || grep -Eq "_lockKPerf|Failed to start the recording|Recording failed with errors" "$LOG_FILE"; then
	profile_capture_failed=1
	emit_audit_finding \
		"warning" \
		"profile_capture" \
		"xctrace profile capture failed or was incomplete" \
		"xctrace_status=$status; see benchmark.log for _lockKPerf/recording errors" \
		"Hot-zone rankings from this bundle may be empty or misleading; benchmark correctness should be judged from direct benchmark output and stage_trace.jsonl." \
		"Rerun when no other xctrace/kperf session is active; the harness will run a no-profiler fallback to preserve correctness artifacts."
fi

bench_status=$status
if [[ $profile_capture_failed -eq 1 ]]; then
	echo "xctrace failed; running benchmark directly to preserve correctness/stage artifacts..." | tee -a "$LOG_FILE"
	set +e
	"$BENCH_BIN" --benchmark_filter=BM_RetrievalQuality 2>&1 | tee -a "$LOG_FILE"
	bench_status=${PIPESTATUS[0]}
	set -e
fi

if [[ ! -s "$JSONL_FILE" ]]; then
	cat >"$JSONL_FILE" <<EOF
{"event":"stage_trace_fallback","benchmark_binary":"$BENCH_BIN","dataset":"$DATASET","corpus_size":$CORPUS_SIZE,"num_queries":$QUERIES,"topk":$TOPK,"embeddings_mode":"$([[ "$FORCE_MOCK_EMBEDDINGS" == "1" ]] && echo mock || echo live)","embed_model":"$EMBED_MODEL","status":$bench_status,"note":"Structured per-query stage trace was empty; consult benchmark.log and manifest.json for this run."}
EOF
	emit_audit_finding \
		"warning" \
		"retrieval_stage_trace" \
		"retrieval benchmark emitted no structured stage trace" \
		"stage_trace.jsonl was empty after benchmark execution; fallback record was written" \
		"Per-query retrieval latency and quality findings cannot be trusted for this bundle." \
		"Inspect benchmark.log for early exit; keep optimization decisions scoped to artifacts with benchmark_summary or per-query trace events."
fi

if ! grep -Eq '"event":"benchmark_summary"|"event"[[:space:]]*:[[:space:]]*"benchmark_summary"' "$JSONL_FILE"; then
	emit_audit_finding \
		"info" \
		"retrieval_stage_trace" \
		"retrieval benchmark summary missing" \
		"stage_trace.jsonl contains $(wc -l <"$JSONL_FILE") line(s) but no benchmark_summary event" \
		"Harness produced artifacts, but retrieval-stage conclusions should be treated as incomplete evidence." \
		"Prefer rerunning after fixing benchmark completion or trace emission before choosing retrieval optimizations."
fi

if [[ -e "$TRACE_OUT" ]]; then
	set +e
	xcrun xctrace export \
		--input "$TRACE_OUT" \
		--xpath '/trace-toc/run[1]/data/table[@schema="time-profile"]' \
		--output "$PROFILE_XML"
	export_status=$?
	set -e
	if [[ $export_status -ne 0 ]]; then
		emit_audit_finding \
			"warning" \
			"profile_capture" \
			"xctrace export failed" \
			"xctrace_export_status=$export_status profile_trace=$TRACE_OUT" \
			"Hot-zone output cannot be generated from this trace, but benchmark artifacts remain valid if the direct benchmark succeeded." \
			"Rerun profiler after clearing xctrace/kperf contention or inspect the trace manually."
	elif ! python3 - <<PYEOF; then
import xml.etree.ElementTree as ET
from collections import Counter

tree = ET.parse('$PROFILE_XML')
root = tree.getroot()
samples = Counter()
total = 0
for row in root.iter('row'):
    weight_el = row.find('.//{*}weight')
    if weight_el is None:
        continue
    w = int(weight_el.text) if weight_el.text else 1000000
    total += w
    frames = row.findall('.//{*}frame')
    app = None
    for f in reversed(frames):
        name = f.get('name', '')
        binary_name = ''
        for child in f:
            if child.tag.endswith('binary'):
                binary_name = child.get('name', '')
                break
        if 'dyld' in binary_name or binary_name.startswith('lib') or binary_name == 'kernel' or 'libc++' in binary_name or 'libsqlite3' in binary_name:
            continue
        if name:
            app = name.split(' (')[0].strip()
            break
    if app:
        samples[app] += w
with open('$HOT_ZONES_FILE', 'w') as out:
    out.write(f'Top hot zones (total samples={total})\n')
    for name, w in samples.most_common(30):
        out.write(f'{(w/total*100 if total else 0):5.1f}% {w/1_000_000:8.1f}ms {name}\n')
PYEOF
		emit_audit_finding \
			"warning" \
			"profile_capture" \
			"profile XML parsing failed" \
			"profile_xml=$PROFILE_XML" \
			"Hot-zone output cannot be trusted for this bundle." \
			"Inspect profile.xml manually or rerun xctrace."
	fi
fi

if [[ -f "$HOT_ZONES_FILE" ]] && grep -q "total samples=0" "$HOT_ZONES_FILE"; then
	emit_audit_finding \
		"warning" \
		"profile_capture" \
		"profile contains zero time-profile samples" \
		"hot_zones.txt reports total samples=0" \
		"Hot-zone output is a harness artifact, not evidence of product hotspots." \
		"Use benchmark/stage artifacts for correctness; rerun profiler after clearing xctrace/kperf contention."
fi

echo "Bundle artifacts:"
find "$BUNDLE_DIR" -maxdepth 1 | sort

exit $bench_status
