#!/usr/bin/env bash
# PBI-040-4: Manual test for synchronous FTS5 indexing
# This test verifies that small document adds make grep results immediately available.
#
# Expected behavior:
# - Single file add → grep immediately finds content (no 5-10s delay)
# - Small batch add (≤10 files) → grep immediately finds content
# - Large batch add (>10 files) → grep may have delay (async indexing)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
YAMS_CLI="${YAMS_CLI:-${SCRIPT_DIR}/../../build/yams-debug/tools/yams-cli/yams-cli}"
TEST_DIR="/tmp/yams_sync_indexing_test_$$"

cleanup() {
    echo "Cleaning up..."
    rm -rf "$TEST_DIR"
    pkill -f yams-daemon || true
}
trap cleanup EXIT

echo "=== PBI-040-4: Sync FTS5 Indexing Test ==="
echo

# Setup test environment
mkdir -p "$TEST_DIR"
export YAMS_DATA_DIR="$TEST_DIR/data"
mkdir -p "$YAMS_DATA_DIR"

# Create test files
echo "Creating test documents..."
echo "The quick brown fox jumps over the lazy dog" > "$TEST_DIR/test1.txt"
echo "Lorem ipsum dolor sit amet consectetur" > "$TEST_DIR/test2.txt"
echo "YAMS is a content-addressable storage system" > "$TEST_DIR/test3.txt"

echo "Test files created in $TEST_DIR"
echo

# Test 1: Single file add + immediate grep
echo "=== Test 1: Single File Add + Immediate Grep ==="
echo "Adding single file..."
START=$(date +%s%N)
HASH=$("$YAMS_CLI" add "$TEST_DIR/test1.txt" --tags "test,sync" | grep -oE "Hash: [a-f0-9]+" | cut -d' ' -f2)
ADD_TIME=$(( ($(date +%s%N) - START) / 1000000 ))
echo "Add completed in ${ADD_TIME}ms, hash: $HASH"

echo "Grepping immediately (no delay)..."
START=$(date +%s%N)
MATCHES=$("$YAMS_CLI" grep "quick brown fox" --paths-only | wc -l)
GREP_TIME=$(( ($(date +%s%N) - START) / 1000000 ))
echo "Grep completed in ${GREP_TIME}ms, matches: $MATCHES"

if [ "$MATCHES" -gt 0 ]; then
    echo "✅ PASS: Immediate grep found content (sync indexing working)"
else
    echo "❌ FAIL: Immediate grep did not find content (sync indexing broken)"
    exit 1
fi
echo

# Test 2: Small batch add (3 files) + immediate grep
echo "=== Test 2: Small Batch Add (3 files) + Immediate Grep ==="
echo "Adding 3 files..."
START=$(date +%s%N)
"$YAMS_CLI" add "$TEST_DIR/test1.txt" "$TEST_DIR/test2.txt" "$TEST_DIR/test3.txt" --tags "test,batch" > /dev/null
ADD_TIME=$(( ($(date +%s%N) - START) / 1000000 ))
echo "Batch add completed in ${ADD_TIME}ms"

echo "Grepping immediately for 'YAMS' (no delay)..."
START=$(date +%s%N)
MATCHES=$("$YAMS_CLI" grep "YAMS" --paths-only | wc -l)
GREP_TIME=$(( ($(date +%s%N) - START) / 1000000 ))
echo "Grep completed in ${GREP_TIME}ms, matches: $MATCHES"

if [ "$MATCHES" -gt 0 ]; then
    echo "✅ PASS: Immediate grep found batch content (sync indexing working)"
else
    echo "❌ FAIL: Immediate grep did not find batch content"
    exit 1
fi
echo

# Test 3: Performance check - grep should be < 1000ms
echo "=== Test 3: Performance Check ==="
if [ "$GREP_TIME" -lt 1000 ]; then
    echo "✅ PASS: Grep performance is good (${GREP_TIME}ms < 1000ms)"
else
    echo "⚠️  WARNING: Grep took ${GREP_TIME}ms (expected < 1000ms)"
fi
echo

echo "=== All Tests Passed ==="
echo "Summary:"
echo "  - Single file add: ${ADD_TIME}ms"
echo "  - Grep latency: ${GREP_TIME}ms"
echo "  - Sync indexing: ✅ working"
