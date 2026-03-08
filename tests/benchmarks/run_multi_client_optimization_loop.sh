#!/usr/bin/env bash
set -euo pipefail

# Runs phase 2 (throughput-first) then phase 1 (stability-first)
# and stores machine-readable artifacts for tuning/regression loops.

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
BENCH_BIN_DEFAULT="$ROOT_DIR/builddir-nosan/tests/benchmarks/multi_client_ingestion_bench"

BENCH_BIN="${YAMS_MULTI_CLIENT_BENCH_BIN:-$BENCH_BIN_DEFAULT}"
DATA_DIR_SOURCE="${YAMS_BENCH_DATA_DIR:-}"
DATA_DIR="$DATA_DIR_SOURCE"
DATA_DIR_MODE="${YAMS_BENCH_DATA_DIR_MODE:-snapshot}"
RUN_TAG="${YAMS_OPTIMIZATION_TAG:-}"
BASELINE_JSON="${YAMS_MULTI_CLIENT_BASELINE:-$ROOT_DIR/tests/benchmarks/baseline/multi_client.baseline.json}"
FAIL_ON_REGRESSION="${YAMS_FAIL_ON_REGRESSION:-0}"
TMP_WORK_DIR=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --bench-bin)
            BENCH_BIN="$2"
            shift 2
            ;;
        --data-dir)
            DATA_DIR_SOURCE="$2"
            DATA_DIR="$2"
            shift 2
            ;;
        --data-dir-mode)
            DATA_DIR_MODE="$2"
            shift 2
            ;;
        --tag)
            RUN_TAG="$2"
            shift 2
            ;;
        --baseline)
            BASELINE_JSON="$2"
            shift 2
            ;;
        --fail-on-regression)
            FAIL_ON_REGRESSION="1"
            shift
            ;;
        *)
            echo "Unknown arg: $1" >&2
            exit 2
            ;;
    esac
done

cleanup() {
    if [[ -n "$TMP_WORK_DIR" && -d "$TMP_WORK_DIR" ]]; then
        rm -rf "$TMP_WORK_DIR"
    fi
}

trap cleanup EXIT

if [[ ! -x "$BENCH_BIN" ]]; then
    echo "Benchmark binary not executable: $BENCH_BIN" >&2
    echo "Build target first (example): meson compile -C builddir-nosan bench_multi_client" >&2
    exit 1
fi

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
if [[ -n "$RUN_TAG" ]]; then
    RUN_DIR="$ROOT_DIR/bench_results/optimization_loop/${STAMP}_${RUN_TAG}"
else
    RUN_DIR="$ROOT_DIR/bench_results/optimization_loop/${STAMP}"
fi
mkdir -p "$RUN_DIR"
RUN_ID="${STAMP}${RUN_TAG:+_${RUN_TAG}}"
GIT_SHA="$(git -C "$ROOT_DIR" rev-parse --short HEAD)"
CASE_MANIFEST="$RUN_DIR/cases.manifest.jsonl"
RUN_MANIFEST="$RUN_DIR/run_manifest.json"
: > "$CASE_MANIFEST"

prepare_data_dir() {
    if [[ -z "$DATA_DIR_SOURCE" ]]; then
        DATA_DIR=""
        return
    fi

    if [[ ! -d "$DATA_DIR_SOURCE" ]]; then
        echo "Data directory does not exist: $DATA_DIR_SOURCE" >&2
        exit 1
    fi

    case "$DATA_DIR_MODE" in
        direct)
            DATA_DIR="$DATA_DIR_SOURCE"
            echo "Using data dir directly: $DATA_DIR"
            ;;
        snapshot)
            TMP_WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/yams_opt_loop_data.XXXXXX")"
            DATA_DIR="$TMP_WORK_DIR/data"
            mkdir -p "$DATA_DIR"

            echo "Creating isolated data dir snapshot from: $DATA_DIR_SOURCE"
            if command -v rsync >/dev/null 2>&1; then
                rsync -a \
                    --exclude='.yams-lock' \
                    --exclude='*.sock' \
                    --exclude='*.pid' \
                    "$DATA_DIR_SOURCE/" "$DATA_DIR/"
            else
                cp -R "$DATA_DIR_SOURCE/." "$DATA_DIR/"
                rm -f "$DATA_DIR/.yams-lock" "$DATA_DIR"/*.sock "$DATA_DIR"/*.pid
            fi
            echo "Using isolated snapshot data dir: $DATA_DIR"
            ;;
        *)
            echo "Invalid --data-dir-mode: $DATA_DIR_MODE (expected: direct|snapshot)" >&2
            exit 2
            ;;
    esac
}

prepare_data_dir

PHASE2_OUT="$RUN_DIR/phase2_throughput_first.jsonl"
PHASE1_OUT="$RUN_DIR/phase1_stability_first.jsonl"
: > "$PHASE2_OUT"
: > "$PHASE1_OUT"

write_metadata() {
    local metadata_file="$1"
    local phase_label="$2"
    local tuning_profile="$3"
    local usage_profile="$4"
    local use_mcp="$5"

    {
        echo "{"
        echo "  \"phase\": \"$phase_label\"," 
        echo "  \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"," 
        echo "  \"git_sha\": \"$GIT_SHA\"," 
        echo "  \"run_id\": \"$RUN_ID\"," 
        echo "  \"bench_bin\": \"$BENCH_BIN\"," 
        echo "  \"data_dir_source\": \"$DATA_DIR_SOURCE\"," 
        echo "  \"data_dir_effective\": \"$DATA_DIR\"," 
        echo "  \"data_dir_mode\": \"$DATA_DIR_MODE\"," 
        echo "  \"env\": {"
        echo "    \"YAMS_TUNING_PROFILE\": \"$tuning_profile\"," 
        echo "    \"YAMS_BENCH_USAGE_PROFILE\": \"$usage_profile\"," 
        echo "    \"YAMS_BENCH_USE_MCP\": \"$use_mcp\"," 
        echo "    \"YAMS_IO_THREADS\": \"${YAMS_IO_THREADS:-}\"," 
        echo "    \"YAMS_MAX_ACTIVE_CONN\": \"${YAMS_MAX_ACTIVE_CONN:-}\"," 
        echo "    \"YAMS_SERVER_MAX_INFLIGHT\": \"${YAMS_SERVER_MAX_INFLIGHT:-}\"," 
        echo "    \"YAMS_IPC_TIMEOUT_MS\": \"${YAMS_IPC_TIMEOUT_MS:-}\"," 
        echo "    \"YAMS_MAX_IDLE_TIMEOUTS\": \"${YAMS_MAX_IDLE_TIMEOUTS:-}\"," 
        echo "    \"YAMS_CONNECTION_LIFETIME_S\": \"${YAMS_CONNECTION_LIFETIME_S:-}\"," 
        echo "    \"YAMS_BENCH_OPS_PER_CLIENT\": \"${YAMS_BENCH_OPS_PER_CLIENT:-}\"," 
        echo "    \"YAMS_BENCH_OP_TIMEOUT_S\": \"${YAMS_BENCH_OP_TIMEOUT_S:-}\"," 
        echo "    \"YAMS_BENCH_SEARCH_CLIENTS\": \"${YAMS_BENCH_SEARCH_CLIENTS:-}\"," 
        echo "    \"YAMS_BENCH_LIST_CLIENTS\": \"${YAMS_BENCH_LIST_CLIENTS:-}\"," 
        echo "    \"YAMS_BENCH_GREP_CLIENTS\": \"${YAMS_BENCH_GREP_CLIENTS:-}\"," 
        echo "    \"YAMS_BENCH_STATUS_GET_CLIENTS\": \"${YAMS_BENCH_STATUS_GET_CLIENTS:-}\"," 
        echo "    \"YAMS_BENCH_LIST_INFLIGHT_CAP\": \"${YAMS_BENCH_LIST_INFLIGHT_CAP:-}\"," 
        echo "    \"YAMS_BENCH_MCP_POOL_SIZE\": \"${YAMS_BENCH_MCP_POOL_SIZE:-}\","
        echo "    \"YAMS_BENCH_MCP_SESSION_CHURN_EVERY_N\": \"${YAMS_BENCH_MCP_SESSION_CHURN_EVERY_N:-}\","
        echo "    \"YAMS_BENCH_CASE_TIMEOUT_S\": \"${YAMS_BENCH_CASE_TIMEOUT_S:-}\"," 
        echo "    \"YAMS_BENCH_CONTINUE_ON_CASE_FAILURE\": \"${YAMS_BENCH_CONTINUE_ON_CASE_FAILURE:-}\""
        echo "  }"
        echo "}"
    } > "$metadata_file"
}

run_case() {
    local output_file="$1"
    local test_filter="$2"
    local tuning_profile="$3"
    local usage_profile="$4"
    local use_mcp="$5"
    local phase_label="$6"
    local transport_label="daemon_ipc"
    if [[ "$use_mcp" == "1" ]]; then
        transport_label="mcp_query"
    fi

    local case_timeout_s="${YAMS_BENCH_CASE_TIMEOUT_S:-0}"

    if [[ "$case_timeout_s" =~ ^[0-9]+$ ]] && [[ "$case_timeout_s" -gt 0 ]]; then
        YAMS_BENCH_OUTPUT="$output_file" \
        YAMS_TUNING_PROFILE="$tuning_profile" \
        YAMS_BENCH_USAGE_PROFILE="$usage_profile" \
        YAMS_BENCH_USE_MCP="$use_mcp" \
        YAMS_BENCH_RUN_ID="$RUN_ID" \
        YAMS_BENCH_GIT_SHA="$GIT_SHA" \
        YAMS_BENCH_PHASE="$phase_label" \
        YAMS_BENCH_CASE="$test_filter" \
        YAMS_BENCH_TRANSPORT="$transport_label" \
        python3 - "$BENCH_BIN" "$test_filter" "$case_timeout_s" <<'PY'
import os
import subprocess
import sys

bench_bin = sys.argv[1]
test_filter = sys.argv[2]
timeout_s = int(sys.argv[3])

cmd = [bench_bin, "--allow-running-no-tests", test_filter]

try:
    completed = subprocess.run(cmd, env=os.environ.copy(), timeout=timeout_s)
    sys.exit(completed.returncode)
except subprocess.TimeoutExpired:
    print(
        f"Benchmark case timed out after {timeout_s}s: {test_filter}",
        file=sys.stderr,
    )
    sys.exit(124)
PY
        return $?
    fi

    YAMS_BENCH_OUTPUT="$output_file" \
    YAMS_TUNING_PROFILE="$tuning_profile" \
    YAMS_BENCH_USAGE_PROFILE="$usage_profile" \
    YAMS_BENCH_USE_MCP="$use_mcp" \
    YAMS_BENCH_RUN_ID="$RUN_ID" \
    YAMS_BENCH_GIT_SHA="$GIT_SHA" \
    YAMS_BENCH_PHASE="$phase_label" \
    YAMS_BENCH_CASE="$test_filter" \
    YAMS_BENCH_TRANSPORT="$transport_label" \
    "$BENCH_BIN" --allow-running-no-tests "$test_filter"
}

append_case_manifest() {
    local phase_name="$1"
    local output_file="$2"
    local test_filter="$3"
    local use_mcp="$4"
    local status="$5"
    local exit_code="$6"
    local usage_profile="$7"

    python3 - "$CASE_MANIFEST" "$phase_name" "$output_file" "$test_filter" "$use_mcp" \
        "$status" "$exit_code" "$usage_profile" "$RUN_ID" "$GIT_SHA" <<'PY'
import json
import sys
from datetime import datetime, timezone

manifest_path = sys.argv[1]
phase_name = sys.argv[2]
output_file = sys.argv[3]
test_filter = sys.argv[4]
use_mcp = sys.argv[5] == "1"
status = sys.argv[6]
exit_code = int(sys.argv[7])
usage_profile = sys.argv[8]
run_id = sys.argv[9]
git_sha = sys.argv[10]

entry = {
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "run_id": run_id,
    "git_sha": git_sha,
    "phase": phase_name,
    "test_filter": test_filter,
    "transport": "mcp_query" if use_mcp else "daemon_ipc",
    "usage_profile": usage_profile,
    "output_jsonl": output_file,
    "status": status,
    "exit_code": exit_code,
}

with open(manifest_path, "a", encoding="utf-8") as f:
    f.write(json.dumps(entry, sort_keys=True))
    f.write("\n")
PY
}

apply_phase2_profile() {
    # Throughput-first (tuned via sweep 20260302: IO=16 and INF=128 top cluster)
    export YAMS_TUNING_PROFILE="aggressive"
    export YAMS_IO_THREADS="16"
    export YAMS_MAX_ACTIVE_CONN="2048"
    export YAMS_SERVER_MAX_INFLIGHT="128"
    export YAMS_IPC_TIMEOUT_MS="15000"
    export YAMS_MAX_IDLE_TIMEOUTS="12"
    export YAMS_CONNECTION_LIFETIME_S="0"
}

apply_phase1_profile() {
    # Stability-first (tuned via sweep 20260302: IO=10 safe uplift, keep conservative INF)
    export YAMS_TUNING_PROFILE="balanced"
    export YAMS_IO_THREADS="10"
    export YAMS_MAX_ACTIVE_CONN="2048"
    export YAMS_SERVER_MAX_INFLIGHT="64"
    export YAMS_IPC_TIMEOUT_MS="30000"
    export YAMS_MAX_IDLE_TIMEOUTS="20"
    export YAMS_CONNECTION_LIFETIME_S="1800"
}

run_phase() {
    local phase_name="$1"
    local output_file="$2"
    local usage_profile="$3"

    local continue_on_case_failure="${YAMS_BENCH_CONTINUE_ON_CASE_FAILURE:-1}"

    run_case_checked() {
        local case_filter="$1"
        local case_transport="$2"
        local case_usage_profile="${3:-$usage_profile}"
        local rc=0
        local status="ok"
        if run_case "$output_file" "$case_filter" "$YAMS_TUNING_PROFILE" "$case_usage_profile" \
            "$case_transport" "$phase_name"; then
            rc=0
            status="ok"
        else
            rc=$?
            status="failed"
            echo "Case failed (rc=$rc): $case_filter (transport=$case_transport)" >&2
            if [[ "$continue_on_case_failure" != "1" ]]; then
                append_case_manifest "$phase_name" "$output_file" "$case_filter" "$case_transport" \
                    "$status" "$rc" "$case_usage_profile"
                exit "$rc"
            fi
        fi

        append_case_manifest "$phase_name" "$output_file" "$case_filter" "$case_transport" \
            "$status" "$rc" "$case_usage_profile"
        if [[ "$status" == "failed" && "$continue_on_case_failure" == "1" ]]; then
            return 0
        fi
        return "$rc"
    }

    echo "=== $phase_name ==="

    # Connection contention run (always available)
    run_case_checked "Multi-client ingestion: connection contention" "0"

    # Large corpus read-path run (requires existing corpus)
    if [[ -n "$DATA_DIR" && -d "$DATA_DIR" ]]; then
        export YAMS_BENCH_DATA_DIR="$DATA_DIR"

        # daemon IPC path
        run_case_checked "Multi-client ingestion: large corpus reads" "0"

        # MCP path
        run_case_checked "Multi-client ingestion: large corpus reads" "1"

        # External-agent-like churn profile (MCP path only)
        run_case_checked "Multi-client ingestion: large corpus reads" "1" "external_agent_churn"
    else
        echo "Skipping large corpus reads (set --data-dir or YAMS_BENCH_DATA_DIR)"
    fi
}

cd "$ROOT_DIR"

apply_phase2_profile
write_metadata "$RUN_DIR/phase2_throughput_first.meta.json" "phase2_throughput_first" \
    "$YAMS_TUNING_PROFILE" "extension_direct" "0_or_1"
run_phase "phase2_throughput_first" "$PHASE2_OUT" "extension_direct"

apply_phase1_profile
write_metadata "$RUN_DIR/phase1_stability_first.meta.json" "phase1_stability_first" \
    "$YAMS_TUNING_PROFILE" "mixed" "0_or_1"
run_phase "phase1_stability_first" "$PHASE1_OUT" "mixed"

python3 "$ROOT_DIR/tests/scripts/summarize_multi_client_jsonl.py" \
    --input "$PHASE2_OUT" \
    --output "$RUN_DIR/phase2_throughput_first.summary.json"

python3 "$ROOT_DIR/tests/scripts/summarize_multi_client_jsonl.py" \
    --input "$PHASE1_OUT" \
    --output "$RUN_DIR/phase1_stability_first.summary.json"

python3 "$ROOT_DIR/tests/scripts/summarize_multi_client_jsonl.py" \
    --input "$PHASE2_OUT" \
    --input "$PHASE1_OUT" \
    --output "$RUN_DIR/combined.summary.json"

python3 "$ROOT_DIR/tests/scripts/check_multi_client_regression.py" \
    "$RUN_DIR/phase1_stability_first.summary.json" \
    "$RUN_DIR/phase2_throughput_first.summary.json" \
    --output "$RUN_DIR/phase1_vs_phase2.regression.json"

# --- Baseline regression check ---
if [[ -f "$BASELINE_JSON" ]]; then
    echo ""
    echo "=== Baseline regression check ==="
    echo "  current:  $RUN_DIR/combined.summary.json"
    echo "  baseline: $BASELINE_JSON"
    FAIL_FLAG=""
    [[ "$FAIL_ON_REGRESSION" == "1" ]] && FAIL_FLAG="--fail-on-regression"
    python3 "$ROOT_DIR/tests/scripts/check_multi_client_regression.py" \
        "$RUN_DIR/combined.summary.json" \
        "$BASELINE_JSON" \
        --output "$RUN_DIR/vs_baseline.regression.json" \
        $FAIL_FLAG
    BASELINE_RC=$?
    if [[ $BASELINE_RC -ne 0 ]]; then
        echo "WARNING: Baseline regression check failed (exit $BASELINE_RC)"
    fi
else
    echo "No baseline file found at $BASELINE_JSON — skipping baseline regression check"
fi

python3 - "$RUN_MANIFEST" "$RUN_ID" "$GIT_SHA" "$RUN_DIR" "$CASE_MANIFEST" <<'PY'
import json
import pathlib
import sys
from datetime import datetime, timezone

manifest_path = pathlib.Path(sys.argv[1])
run_id = sys.argv[2]
git_sha = sys.argv[3]
run_dir = pathlib.Path(sys.argv[4])
case_manifest_path = pathlib.Path(sys.argv[5])

def _read_json(path: pathlib.Path):
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def _read_jsonl(path: pathlib.Path):
    rows = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows

artifacts = {
    "phase2_jsonl": str(run_dir / "phase2_throughput_first.jsonl"),
    "phase1_jsonl": str(run_dir / "phase1_stability_first.jsonl"),
    "phase2_summary": str(run_dir / "phase2_throughput_first.summary.json"),
    "phase1_summary": str(run_dir / "phase1_stability_first.summary.json"),
    "combined_summary": str(run_dir / "combined.summary.json"),
    "phase_compare_regression": str(run_dir / "phase1_vs_phase2.regression.json"),
    "baseline_regression": str(run_dir / "vs_baseline.regression.json"),
    "phase2_metadata": str(run_dir / "phase2_throughput_first.meta.json"),
    "phase1_metadata": str(run_dir / "phase1_stability_first.meta.json"),
    "case_manifest": str(case_manifest_path),
}

combined = _read_json(pathlib.Path(artifacts["combined_summary"])) or {}
phase_compare = _read_json(pathlib.Path(artifacts["phase_compare_regression"])) or {}
baseline_compare = _read_json(pathlib.Path(artifacts["baseline_regression"])) or {}
case_rows = _read_jsonl(case_manifest_path)

run_manifest = {
    "schema_version": "multi_client_run_manifest_v1",
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "run_id": run_id,
    "git_sha": git_sha,
    "artifacts": artifacts,
    "case_count": len(case_rows),
    "failed_cases": [row for row in case_rows if row.get("status") != "ok"],
    "summary_snapshot": {
        "record_count": combined.get("record_count", 0),
        "connection_contention_throughput_mean": (
            combined.get("connection_contention", {})
            .get("throughput_ops_per_sec", {})
            .get("mean", 0.0)
        ),
        "connection_contention_fail_rate_mean": (
            combined.get("connection_contention", {})
            .get("fail_rate", {})
            .get("mean", 0.0)
        ),
        "phase1_vs_phase2_has_regression": phase_compare.get("has_regression", False),
        "vs_baseline_has_regression": baseline_compare.get("has_regression", False),
    },
}

with manifest_path.open("w", encoding="utf-8") as f:
    json.dump(run_manifest, f, indent=2)
PY

echo ""
echo "Artifacts saved to: $RUN_DIR"
