#!/bin/bash
# search_profile.sh — Automated search pipeline profiling
# Builds corpus, runs search benchmarks, captures xctrace, parses hot zones.
#
# Usage:
#   ./tests/benchmarks/scripts/search_profile.sh [--corpus-size 500] [--queries 100] [--rounds 5]
#
# Requires: xcrun, python3, yams built in build/debug with -Denable-bench-tests=true
# Output: /tmp/yams-traces/search_profile_*.trace + /tmp/yams-traces/search_hot_zones.txt

set -euo pipefail

CORPUS_SIZE=500
QUERIES=100
ROUNDS=5
DATA_DIR="${YAMS_PROFILE_DATA_DIR:-/tmp/yams_search_corpus}"
TRACE_OUT="/tmp/yams-traces/search_profile_$(date +%H%M%S).trace"
BUILD_DIR="${YAMS_BUILD_DIR:-build/debug}"
TEMP_BIN=""
MANIFEST_FILE="${TRACE_OUT/.trace/_manifest.json}"

# Parse args
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
	--rounds)
		ROUNDS="$2"
		shift 2
		;;
	--data-dir)
		DATA_DIR="$2"
		shift 2
		;;
	--build-dir)
		BUILD_DIR="$2"
		shift 2
		;;
	--skip-ingest)
		SKIP_INGEST=1
		shift
		;;
	*)
		echo "Unknown arg: $1"
		exit 1
		;;
	esac
done

echo "=== YAMS Search Pipeline Profiler ==="
echo "  Corpus size: $CORPUS_SIZE"
echo "  Queries:     $QUERIES"
echo "  Rounds:      $ROUNDS"
echo "  Data dir:    $DATA_DIR"
echo "  Trace:       $TRACE_OUT"
echo "  Build dir:   $BUILD_DIR"
echo "  Manifest:    $MANIFEST_FILE"

# Step 1: Ensure benchmark binary exists
BENCH_BIN="$BUILD_DIR/tests/benchmarks/retrieval_quality_bench"
if [[ ! -f "$BENCH_BIN" ]]; then
	echo "Building retrieval_quality_bench..."
	meson compile -C "$BUILD_DIR" retrieval_quality_bench
fi

# Step 2: Run benchmark with xctrace
echo "Starting xctrace recording..."
export YAMS_BENCH_CORPUS_SIZE="$CORPUS_SIZE"
export YAMS_BENCH_NUM_QUERIES="$QUERIES"
export YAMS_BENCH_TOPK=10
export YAMS_BENCH_DATASET=synthetic
export YAMS_TEST_SAFE_SINGLE_INSTANCE=1
export YAMS_DATA_DIR="$DATA_DIR"
export YAMS_SEARCH_STAGE_TRACE=1

RUNNER_NAME="search_profile" \
	BENCH_BIN="$BENCH_BIN" \
	BUILD_DIR="$BUILD_DIR" \
	YAMS_BENCH_DATASET="$YAMS_BENCH_DATASET" \
	YAMS_BENCH_CORPUS_SIZE="$YAMS_BENCH_CORPUS_SIZE" \
	YAMS_BENCH_NUM_QUERIES="$YAMS_BENCH_NUM_QUERIES" \
	YAMS_BENCH_TOPK="$YAMS_BENCH_TOPK" \
	YAMS_SEARCH_STAGE_TRACE="$YAMS_SEARCH_STAGE_TRACE" \
	./tests/benchmarks/scripts/benchmark_mode_manifest.sh "$MANIFEST_FILE" >/dev/null

xcrun xctrace record \
	--template "Time Profiler" \
	--time-limit "$((ROUNDS * 120))s" \
	--output "$TRACE_OUT" \
	--no-prompt \
	--launch -- "$BENCH_BIN" --benchmark_filter=BM_RetrievalQuality

echo "Trace saved: $TRACE_OUT"
du -sh "$TRACE_OUT"

# Step 3: Export time-profile data
PROFILE_XML="${TRACE_OUT/.trace/_profile.xml}"
echo "Exporting time-profile to $PROFILE_XML..."
xcrun xctrace export \
	--input "$TRACE_OUT" \
	--xpath '/trace-toc/run[1]/data/table[@schema="time-profile"]' \
	--output "$PROFILE_XML"

echo "Parsing hot zones..."

# Step 4: Parse hot zones
python3 - <<PYEOF
import xml.etree.ElementTree as ET
from collections import Counter
import re, sys

tree = ET.parse('$PROFILE_XML')
root = tree.getroot()
samples = Counter()
total_samples = 0

for row in root.iter('row'):
    weight_el = row.find('.//{*}weight')
    if weight_el is None:
        continue
    w = int(weight_el.text) if weight_el.text else 1000000
    total_samples += w

    # Find deepest non-system frame
    frames = row.findall('.//{*}frame')
    app_frame = None
    for f in reversed(frames):
        name = f.get('name', '')
        binary_name = ''
        for child in f:
            if child.tag.endswith('binary'):
                binary_name = child.get('name', '')
                break
        if 'dyld' in binary_name or binary_name.startswith('lib') or \
           binary_name.startswith('libsystem') or binary_name.startswith('libobjc') or \
           binary_name == 'kernel' or 'libc++' in binary_name or 'libsqlite3' in binary_name:
            continue
        if not name:
            continue
        app_frame = name.split(' (')[0].strip()
        break
    if app_frame:
        samples[app_frame] += w

# Print report
hotspots_file = '$TRACE_OUT'.replace('.trace', '_hot_zones.txt')
with open(hotspots_file, 'w') as out:
    out.write(f"Search Pipeline Hot Zones ({total_samples/1e6:.1f}s app samples)\\n")
    out.write(f"Corpus: {0} docs, {0} queries, {0} rounds\\n\\n".format('$CORPUS_SIZE', '$QUERIES', '$ROUNDS'))
    out.write("Top 30 functions by CPU time:\\n")
    for name, w in samples.most_common(30):
        pct = w / total_samples * 100 if total_samples else 0
        ms_total = w / 1_000_000
        line = f"  {pct:5.1f}%  {ms_total:8.1f}ms  {name}\\n"
        out.write(line)
        print(line.strip())

    # Also print search-relevant functions
    search_funcs = [n for n in samples if any(k in n.lower() for k in
        ['search', 'query', 'fusion', 'rerank', 'topology', 'lexical',
         'vector', 'score', 'rank', 'semantic', 'embed', 'retriev'])]
    if search_funcs:
        out.write("\\nSearch-specific functions:\\n")
        for n in sorted(search_funcs, key=lambda x: -samples[x]):
            ms = samples[n] / 1_000_000
            out.write(f"  {ms:8.1f}ms  {n}\\n")

print(f"\\nHot zones report: {hotspots_file}")
PYEOF

echo "Done."
