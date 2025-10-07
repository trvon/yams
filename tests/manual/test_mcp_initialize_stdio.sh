#!/bin/bash
# Manual test to verify MCP initialize response through actual stdio transport
# This simulates what a real MCP client does

set -euo pipefail

YAMS_BIN="${1:-./build/debug/src/app/yams}"

if [[ ! -f "$YAMS_BIN" ]]; then
    echo "Error: yams binary not found at $YAMS_BIN"
    echo "Usage: $0 [path/to/yams]"
    exit 1
fi

echo "Testing MCP initialize with: $YAMS_BIN"
echo ""

# Test 1: Basic initialize request
echo "=== Test 1: Initialize with protocol version 2024-11-05 ==="
RESPONSE=$(echo '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","clientInfo":{"name":"test-client","version":"1.0"}}}' | timeout 5 "$YAMS_BIN" mcp 2>&1 | grep -v "^\[" | head -1)

echo "Raw response length: ${#RESPONSE}"
if [[ -z "$RESPONSE" ]]; then
    echo "❌ FAIL: No response received (timeout or error)"
    exit 1
fi

echo "Response:"
echo "$RESPONSE" | jq '.' || echo "Failed to parse JSON: $RESPONSE"

# Check for empty result
if echo "$RESPONSE" | jq -e '.result == null or .result == {}' >/dev/null 2>&1; then
    echo "❌ FAIL: Result is null or empty!"
    exit 1
fi

# Check required fields
if ! echo "$RESPONSE" | jq -e '.result.protocolVersion' >/dev/null 2>&1; then
    echo "❌ FAIL: Missing protocolVersion!"
    exit 1
fi

if ! echo "$RESPONSE" | jq -e '.result.serverInfo' >/dev/null 2>&1; then
    echo "❌ FAIL: Missing serverInfo!"
    exit 1
fi

if ! echo "$RESPONSE" | jq -e '.result.capabilities' >/dev/null 2>&1; then
    echo "❌ FAIL: Missing capabilities!"
    exit 1
fi

echo "✅ PASS: All required fields present"
echo ""

# Test 2: Initialize without protocolVersion
echo "=== Test 2: Initialize without protocol version ==="
RESPONSE2=$(echo '{"jsonrpc":"2.0","id":2,"method":"initialize","params":{"clientInfo":{"name":"test","version":"1.0"}}}' | timeout 5 "$YAMS_BIN" mcp 2>&1 | grep -v "^\[" | head -1)

echo "Response:"
echo "$RESPONSE2" | jq '.'

if echo "$RESPONSE2" | jq -e '.result == null or .result == {}' >/dev/null 2>&1; then
    echo "❌ FAIL: Result is null or empty!"
    exit 1
fi

echo "✅ PASS: Initialize works without explicit protocol version"
echo ""

# Test 3: Initialize with empty params
echo "=== Test 3: Initialize with empty params ==="
RESPONSE3=$(echo '{"jsonrpc":"2.0","id":3,"method":"initialize","params":{}}' | timeout 5 "$YAMS_BIN" mcp 2>&1 | grep -v "^\[" | head -1)

echo "Response:"
echo "$RESPONSE3" | jq '.'

if echo "$RESPONSE3" | jq -e '.result == null or .result == {}' >/dev/null 2>&1; then
    echo "❌ FAIL: Result is null or empty!"
    exit 1
fi

echo "✅ PASS: Initialize works with empty params"
echo ""

echo "=== All tests passed! ==="
