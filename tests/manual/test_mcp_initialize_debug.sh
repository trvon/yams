#!/bin/bash
# Test MCP initialize with debug logging enabled
# This helps diagnose why some clients see empty responses

set -euo pipefail

YAMS_BIN="${1:-./build/debug/tools/yams-cli/yams-cli}"

if [[ ! -f "$YAMS_BIN" ]]; then
    echo "Error: yams binary not found at $YAMS_BIN"
    echo "Usage: $0 [path/to/yams]"
    exit 1
fi

echo "Testing MCP initialize with debug logging"
echo "Binary: $YAMS_BIN"
echo ""

# Enable debug logging via environment variable
export SPDLOG_LEVEL=debug

echo "=== Sending initialize request ==="
REQUEST='{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","clientInfo":{"name":"test-client","version":"1.0"}}}'
echo "Request: $REQUEST"
echo ""

echo "=== Server output (stdout + stderr) ==="
# Send request and capture both stdout and stderr
RESPONSE=$(echo "$REQUEST" | "$YAMS_BIN" serve 2>&1)

echo "$RESPONSE"
echo ""

echo "=== Parsing response ==="
# Try to extract just the JSON response line
JSON_LINE=$(echo "$RESPONSE" | grep -E '^\{.*"jsonrpc".*\}$' | head -1 || echo "")

if [[ -z "$JSON_LINE" ]]; then
    echo "❌ FAIL: No JSON response found in output"
    echo ""
    echo "Full output was:"
    echo "$RESPONSE"
    exit 1
fi

echo "JSON response:"
echo "$JSON_LINE" | jq '.' || echo "Failed to parse: $JSON_LINE"
echo ""

# Validate response structure
echo "=== Validation ==="
if echo "$JSON_LINE" | jq -e '.result == null or .result == {}' >/dev/null 2>&1; then
    echo "❌ FAIL: Result is null or empty!"
    echo "This is the error Goose sees: ExpectedInitResult(Some(EmptyResult(EmptyObject)))"
    exit 1
fi

if ! echo "$JSON_LINE" | jq -e '.result.protocolVersion' >/dev/null 2>&1; then
    echo "❌ FAIL: Missing protocolVersion in result"
    exit 1
fi

if ! echo "$JSON_LINE" | jq -e '.result.serverInfo' >/dev/null 2>&1; then
    echo "❌ FAIL: Missing serverInfo in result"
    exit 1
fi

if ! echo "$JSON_LINE" | jq -e '.result.capabilities' >/dev/null 2>&1; then
    echo "❌ FAIL: Missing capabilities in result"
    exit 1
fi

echo "✅ PASS: All required fields present"
echo ""

echo "=== Result details ==="
echo "$JSON_LINE" | jq '.result'
