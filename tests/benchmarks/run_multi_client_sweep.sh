#!/usr/bin/env bash
set -euo pipefail

# Grid-sweep harness for multi-client daemon parameter tuning.
#
# Round 1 (coarse): IO_THREADS x MAX_INFLIGHT x MAX_ACTIVE_CONN,
#   connection contention only (~10s each).
# Round 2 (narrow): Top-N from round 1 x IPC_TIMEOUT x CONNECTION_LIFETIME,
#   full optimization loop.
# Round 3 (validate): Top-M from round 2, repeated N times.
#
# All results go to bench_results/sweep/<timestamp>/.
# This is a local-only optimization tool, not for CI.

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
BENCH_BIN_DEFAULT="$ROOT_DIR/builddir-nosan/tests/benchmarks/multi_client_ingestion_bench"
BENCH_BIN="${YAMS_MULTI_CLIENT_BENCH_BIN:-$BENCH_BIN_DEFAULT}"
DATA_DIR="${YAMS_BENCH_DATA_DIR:-}"
BASELINE_JSON="${YAMS_MULTI_CLIENT_BASELINE:-$ROOT_DIR/tests/benchmarks/baseline/multi_client.baseline.json}"

# Which round(s) to run
ROUND="${1:-1}"

# Round 1 grid parameters (space-separated)
R1_IO_THREADS="${YAMS_SWEEP_IO_THREADS:-4 6 8 10 12 16}"
R1_MAX_INFLIGHT="${YAMS_SWEEP_MAX_INFLIGHT:-32 64 96 128 256}"
R1_MAX_ACTIVE_CONN="${YAMS_SWEEP_MAX_ACTIVE_CONN:-1024 2048 4096}"
R1_TOP_N="${YAMS_SWEEP_R1_TOP_N:-5}"

# Round 1 fixed knobs
R1_IPC_TIMEOUT_MS="15000"
R1_CONNECTION_LIFETIME_S="0"
R1_TUNING_PROFILE="aggressive"

# Round 2 secondary sweep axes
R2_IPC_TIMEOUT_MS="${YAMS_SWEEP_IPC_TIMEOUT_MS:-15000 30000}"
R2_CONNECTION_LIFETIME_S="${YAMS_SWEEP_CONNECTION_LIFETIME_S:-0 600 1800}"
R2_TOP_N="${YAMS_SWEEP_R2_TOP_N:-3}"

# Round 3 repetitions
R3_REPS="${YAMS_SWEEP_R3_REPS:-3}"

# Per-config timeout (seconds). Round 1 configs should be fast.
CASE_TIMEOUT_S="${YAMS_BENCH_CASE_TIMEOUT_S:-120}"

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
SWEEP_DIR="$ROOT_DIR/bench_results/sweep/${STAMP}"
mkdir -p "$SWEEP_DIR"

echo "=== Multi-client parameter sweep ==="
echo "  Round:     $ROUND"
echo "  Output:    $SWEEP_DIR"
echo "  Bench bin: $BENCH_BIN"
echo ""

if [[ ! -x "$BENCH_BIN" ]]; then
    echo "ERROR: Benchmark binary not found or not executable: $BENCH_BIN" >&2
    echo "Build first: meson compile -C build/debug bench_multi_client" >&2
    exit 1
fi

# ─── Helpers ──────────────────────────────────────────────────────────

run_contention_only() {
    # Run connection contention test case with given env vars.
    # Args: $1=output_jsonl
    local out="$1"
    YAMS_BENCH_OUTPUT="$out" \
    YAMS_BENCH_CASE_TIMEOUT_S="$CASE_TIMEOUT_S" \
        "$BENCH_BIN" --allow-running-no-tests \
        "Multi-client ingestion: connection contention" \
        2>&1 || true
}

summarize_jsonl() {
    # Args: $1=input_jsonl $2=output_summary_json
    python3 "$ROOT_DIR/tests/scripts/summarize_multi_client_jsonl.py" \
        --input "$1" --output "$2"
}

extract_contention_kpis() {
    # Extract throughput and fail_rate from a summary JSON. Prints: throughput fail_rate
    # Args: $1=summary_json
    python3 -c "
import json, sys
with open(sys.argv[1]) as f:
    d = json.load(f)
cc = d.get('connection_contention', {})
thr = cc.get('throughput_ops_per_sec', {}).get('mean', 0.0)
fr = cc.get('fail_rate', {}).get('mean', 0.0)
print(f'{thr} {fr}')
" "$1"
}

# ─── Round 1: Coarse grid ────────────────────────────────────────────

round1() {
    local r1_dir="$SWEEP_DIR/round1"
    mkdir -p "$r1_dir"

    local config_id=0
    local manifest="$r1_dir/manifest.tsv"
    echo -e "config_id\tio_threads\tmax_inflight\tmax_active_conn\tthroughput\tfail_rate\tstatus" > "$manifest"

    local total=0
    for io in $R1_IO_THREADS; do
        for inf in $R1_MAX_INFLIGHT; do
            for conn in $R1_MAX_ACTIVE_CONN; do
                total=$((total + 1))
            done
        done
    done

    echo "Round 1: $total configs (connection contention only)"
    echo ""

    for io in $R1_IO_THREADS; do
        for inf in $R1_MAX_INFLIGHT; do
            for conn in $R1_MAX_ACTIVE_CONN; do
                config_id=$((config_id + 1))
                local tag="c${config_id}_io${io}_inf${inf}_conn${conn}"
                local cfg_dir="$r1_dir/$tag"
                mkdir -p "$cfg_dir"

                echo -n "[$config_id/$total] IO=$io INF=$inf CONN=$conn ... "

                export YAMS_IO_THREADS="$io"
                export YAMS_SERVER_MAX_INFLIGHT="$inf"
                export YAMS_MAX_ACTIVE_CONN="$conn"
                export YAMS_TUNING_PROFILE="$R1_TUNING_PROFILE"
                export YAMS_IPC_TIMEOUT_MS="$R1_IPC_TIMEOUT_MS"
                export YAMS_CONNECTION_LIFETIME_S="$R1_CONNECTION_LIFETIME_S"

                local jsonl="$cfg_dir/contention.jsonl"
                : > "$jsonl"
                run_contention_only "$jsonl" > "$cfg_dir/bench.log" 2>&1

                local summary="$cfg_dir/summary.json"
                if summarize_jsonl "$jsonl" "$summary" 2>/dev/null; then
                    local kpis
                    kpis=$(extract_contention_kpis "$summary")
                    local thr fail_rate
                    thr=$(echo "$kpis" | awk '{print $1}')
                    fail_rate=$(echo "$kpis" | awk '{print $2}')
                    echo "throughput=${thr} fail_rate=${fail_rate}"
                    echo -e "${config_id}\t${io}\t${inf}\t${conn}\t${thr}\t${fail_rate}\tok" >> "$manifest"
                else
                    echo "FAILED (no summary)"
                    echo -e "${config_id}\t${io}\t${inf}\t${conn}\t0.0\t1.0\tfailed" >> "$manifest"
                fi

                # Save config metadata
                python3 -c "
import json, sys
json.dump({
    'config_id': $config_id,
    'io_threads': $io,
    'max_inflight': $inf,
    'max_active_conn': $conn,
    'tuning_profile': '$R1_TUNING_PROFILE',
    'ipc_timeout_ms': $R1_IPC_TIMEOUT_MS,
    'connection_lifetime_s': $R1_CONNECTION_LIFETIME_S
}, open(sys.argv[1], 'w'), indent=2)
" "$cfg_dir/config.json"
            done
        done
    done

    echo ""
    echo "Round 1 complete. Manifest: $manifest"

    # Rank: filter fail_rate==0, sort by throughput descending, take top N
    local ranked="$r1_dir/ranked.tsv"
    python3 -c "
import csv, sys

rows = []
with open(sys.argv[1], newline='') as f:
    reader = csv.DictReader(f, delimiter='\t')
    for row in reader:
        if row['status'] != 'ok':
            continue
        fr = float(row['fail_rate'])
        thr = float(row['throughput'])
        if fr > 0.001:
            continue
        rows.append(row)

rows.sort(key=lambda r: float(r['throughput']), reverse=True)
top_n = int(sys.argv[2])
winners = rows[:top_n]

with open(sys.argv[3], 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=rows[0].keys() if rows else [], delimiter='\t')
    writer.writeheader()
    writer.writerows(winners)

print(f'Top {top_n} configs (0% fail, by throughput):')
for i, r in enumerate(winners, 1):
    print(f'  {i}. IO={r[\"io_threads\"]} INF={r[\"max_inflight\"]} CONN={r[\"max_active_conn\"]} -> {float(r[\"throughput\"]):.1f} ops/s')
" "$manifest" "$R1_TOP_N" "$ranked"

    echo "Ranked: $ranked"
}

# ─── Round 2: Narrow sweep on top-N ──────────────────────────────────

round2() {
    local r1_ranked="$SWEEP_DIR/round1/ranked.tsv"
    if [[ ! -f "$r1_ranked" ]]; then
        # Try to find the latest sweep's round1 ranked
        local latest_sweep
        latest_sweep=$(ls -1d "$ROOT_DIR/bench_results/sweep/"*/round1/ranked.tsv 2>/dev/null | tail -1)
        if [[ -n "$latest_sweep" ]]; then
            r1_ranked="$latest_sweep"
            echo "Using round1 ranked from: $r1_ranked"
        else
            echo "ERROR: No round1 ranked.tsv found. Run round 1 first." >&2
            exit 1
        fi
    fi

    local r2_dir="$SWEEP_DIR/round2"
    mkdir -p "$r2_dir"

    local config_id=0
    local manifest="$r2_dir/manifest.tsv"
    echo -e "config_id\tio_threads\tmax_inflight\tmax_active_conn\tipc_timeout_ms\tconnection_lifetime_s\tthroughput\tfail_rate\tstatus" > "$manifest"

    # Read top configs from round 1
    local -a r1_io=() r1_inf=() r1_conn=()
    while IFS=$'\t' read -r _ io inf conn _ _ _; do
        r1_io+=("$io")
        r1_inf+=("$inf")
        r1_conn+=("$conn")
    done < <(tail -n +2 "$r1_ranked")

    local n_base=${#r1_io[@]}
    local n_timeout=0 n_lifetime=0
    for _ in $R2_IPC_TIMEOUT_MS; do n_timeout=$((n_timeout + 1)); done
    for _ in $R2_CONNECTION_LIFETIME_S; do n_lifetime=$((n_lifetime + 1)); done
    local total=$((n_base * n_timeout * n_lifetime))

    echo "Round 2: $n_base base configs x $n_timeout timeouts x $n_lifetime lifetimes = $total configs"
    echo "  Running connection contention directly for each config."
    echo ""

    for idx in $(seq 0 $((n_base - 1))); do
        local io="${r1_io[$idx]}"
        local inf="${r1_inf[$idx]}"
        local conn="${r1_conn[$idx]}"

        for timeout in $R2_IPC_TIMEOUT_MS; do
            for lifetime in $R2_CONNECTION_LIFETIME_S; do
                config_id=$((config_id + 1))
                local tag="c${config_id}_io${io}_inf${inf}_conn${conn}_t${timeout}_l${lifetime}"
                local cfg_dir="$r2_dir/$tag"
                mkdir -p "$cfg_dir"

                echo -n "[$config_id/$total] IO=$io INF=$inf CONN=$conn TIMEOUT=$timeout LIFETIME=$lifetime ... "

                export YAMS_IO_THREADS="$io"
                export YAMS_SERVER_MAX_INFLIGHT="$inf"
                export YAMS_MAX_ACTIVE_CONN="$conn"
                export YAMS_IPC_TIMEOUT_MS="$timeout"
                export YAMS_CONNECTION_LIFETIME_S="$lifetime"
                export YAMS_TUNING_PROFILE="aggressive"

                local jsonl="$cfg_dir/contention.jsonl"
                : > "$jsonl"
                run_contention_only "$jsonl" > "$cfg_dir/bench.log" 2>&1

                local summary="$cfg_dir/summary.json"
                if summarize_jsonl "$jsonl" "$summary" 2>/dev/null; then
                    local kpis
                    kpis=$(extract_contention_kpis "$summary")
                    local thr fail_rate
                    thr=$(echo "$kpis" | awk '{print $1}')
                    fail_rate=$(echo "$kpis" | awk '{print $2}')
                    echo "throughput=${thr} fail_rate=${fail_rate}"
                    echo -e "${config_id}\t${io}\t${inf}\t${conn}\t${timeout}\t${lifetime}\t${thr}\t${fail_rate}\tok" >> "$manifest"
                else
                    echo "FAILED (no summary)"
                    echo -e "${config_id}\t${io}\t${inf}\t${conn}\t${timeout}\t${lifetime}\t0.0\t1.0\tfailed" >> "$manifest"
                fi

                # Save config metadata
                python3 -c "
import json, sys
json.dump({
    'config_id': $config_id,
    'io_threads': $io,
    'max_inflight': $inf,
    'max_active_conn': $conn,
    'ipc_timeout_ms': $timeout,
    'connection_lifetime_s': $lifetime,
    'tuning_profile': 'aggressive'
}, open(sys.argv[1], 'w'), indent=2)
" "$cfg_dir/config.json"
            done
        done
    done

    echo ""
    echo "Round 2 complete. Manifest: $manifest"

    # Rank
    local ranked="$r2_dir/ranked.tsv"
    python3 -c "
import csv, sys

rows = []
with open(sys.argv[1], newline='') as f:
    reader = csv.DictReader(f, delimiter='\t')
    for row in reader:
        if row['status'] != 'ok':
            continue
        fr = float(row['fail_rate'])
        thr = float(row['throughput'])
        if fr > 0.001:
            continue
        rows.append(row)

rows.sort(key=lambda r: float(r['throughput']), reverse=True)
top_n = int(sys.argv[2])
winners = rows[:top_n]

with open(sys.argv[3], 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=rows[0].keys() if rows else [], delimiter='\t')
    writer.writeheader()
    writer.writerows(winners)

print(f'Top {top_n} configs (0% fail, by throughput):')
for i, r in enumerate(winners, 1):
    print(f'  {i}. IO={r[\"io_threads\"]} INF={r[\"max_inflight\"]} CONN={r[\"max_active_conn\"]} TIMEOUT={r[\"ipc_timeout_ms\"]} LIFETIME={r[\"connection_lifetime_s\"]} -> {float(r[\"throughput\"]):.1f} ops/s')
" "$manifest" "$R2_TOP_N" "$ranked"

    echo "Ranked: $ranked"
}

# ─── Round 3: Validation with repetitions ─────────────────────────────

round3() {
    local r2_ranked="$SWEEP_DIR/round2/ranked.tsv"
    if [[ ! -f "$r2_ranked" ]]; then
        local latest_sweep
        latest_sweep=$(ls -1d "$ROOT_DIR/bench_results/sweep/"*/round2/ranked.tsv 2>/dev/null | tail -1)
        if [[ -n "$latest_sweep" ]]; then
            r2_ranked="$latest_sweep"
            echo "Using round2 ranked from: $r2_ranked"
        else
            echo "ERROR: No round2 ranked.tsv found. Run round 2 first." >&2
            exit 1
        fi
    fi

    local r3_dir="$SWEEP_DIR/round3"
    mkdir -p "$r3_dir"

    local manifest="$r3_dir/manifest.tsv"
    echo -e "config_id\trep\tio_threads\tmax_inflight\tmax_active_conn\tipc_timeout_ms\tconnection_lifetime_s\tthroughput\tfail_rate\tstatus" > "$manifest"

    # Read top configs from round 2
    local -a r2_io=() r2_inf=() r2_conn=() r2_timeout=() r2_lifetime=()
    while IFS=$'\t' read -r _ io inf conn timeout lifetime _ _ _; do
        r2_io+=("$io")
        r2_inf+=("$inf")
        r2_conn+=("$conn")
        r2_timeout+=("$timeout")
        r2_lifetime+=("$lifetime")
    done < <(tail -n +2 "$r2_ranked")

    local n_configs=${#r2_io[@]}
    local total=$((n_configs * R3_REPS))

    echo "Round 3: $n_configs configs x $R3_REPS reps = $total runs"
    echo ""

    local run_num=0
    for idx in $(seq 0 $((n_configs - 1))); do
        local io="${r2_io[$idx]}"
        local inf="${r2_inf[$idx]}"
        local conn="${r2_conn[$idx]}"
        local timeout="${r2_timeout[$idx]}"
        local lifetime="${r2_lifetime[$idx]}"
        local cfg_id=$((idx + 1))

        for rep in $(seq 1 "$R3_REPS"); do
            run_num=$((run_num + 1))
            local tag="c${cfg_id}_rep${rep}_io${io}_inf${inf}_conn${conn}_t${timeout}_l${lifetime}"
            local cfg_dir="$r3_dir/$tag"
            mkdir -p "$cfg_dir"

            echo -n "[$run_num/$total] Config $cfg_id rep $rep: IO=$io INF=$inf CONN=$conn TIMEOUT=$timeout LIFETIME=$lifetime ... "

            export YAMS_IO_THREADS="$io"
            export YAMS_SERVER_MAX_INFLIGHT="$inf"
            export YAMS_MAX_ACTIVE_CONN="$conn"
            export YAMS_IPC_TIMEOUT_MS="$timeout"
            export YAMS_CONNECTION_LIFETIME_S="$lifetime"
            export YAMS_TUNING_PROFILE="aggressive"

            local jsonl="$cfg_dir/contention.jsonl"
            : > "$jsonl"
            run_contention_only "$jsonl" > "$cfg_dir/bench.log" 2>&1

            local summary="$cfg_dir/summary.json"
            if summarize_jsonl "$jsonl" "$summary" 2>/dev/null; then
                local kpis
                kpis=$(extract_contention_kpis "$summary")
                local thr fail_rate
                thr=$(echo "$kpis" | awk '{print $1}')
                fail_rate=$(echo "$kpis" | awk '{print $2}')
                echo "throughput=${thr} fail_rate=${fail_rate}"
                echo -e "${cfg_id}\t${rep}\t${io}\t${inf}\t${conn}\t${timeout}\t${lifetime}\t${thr}\t${fail_rate}\tok" >> "$manifest"
            else
                echo "FAILED (no summary)"
                echo -e "${cfg_id}\t${rep}\t${io}\t${inf}\t${conn}\t${timeout}\t${lifetime}\t0.0\t1.0\tfailed" >> "$manifest"
            fi
        done
    done

    echo ""
    echo "Round 3 complete. Manifest: $manifest"

    # Summary: mean + stdev per config across reps
    python3 -c "
import csv, statistics, sys

by_config = {}
with open(sys.argv[1], newline='') as f:
    reader = csv.DictReader(f, delimiter='\t')
    for row in reader:
        if row['status'] != 'ok':
            continue
        cid = row['config_id']
        by_config.setdefault(cid, {'rows': [], 'throughputs': [], 'fail_rates': []})
        by_config[cid]['rows'].append(row)
        by_config[cid]['throughputs'].append(float(row['throughput']))
        by_config[cid]['fail_rates'].append(float(row['fail_rate']))

print('Round 3 Validation Summary:')
print(f'{\"Config\":>8}  {\"Reps\":>4}  {\"Mean Thr\":>10}  {\"Stdev\":>8}  {\"Mean Fail\":>10}  {\"Config Details\":<40}')
for cid, data in sorted(by_config.items(), key=lambda x: statistics.fmean(x[1]['throughputs']), reverse=True):
    r = data['rows'][0]
    mean_thr = statistics.fmean(data['throughputs'])
    stdev_thr = statistics.stdev(data['throughputs']) if len(data['throughputs']) > 1 else 0.0
    mean_fr = statistics.fmean(data['fail_rates'])
    details = f'IO={r[\"io_threads\"]} INF={r[\"max_inflight\"]} CONN={r[\"max_active_conn\"]} T={r[\"ipc_timeout_ms\"]} L={r[\"connection_lifetime_s\"]}'
    print(f'{cid:>8}  {len(data[\"throughputs\"]):>4}  {mean_thr:>10.1f}  {stdev_thr:>8.1f}  {mean_fr:>10.4f}  {details}')
" "$manifest"
}

# ─── Dispatch ─────────────────────────────────────────────────────────

case "$ROUND" in
    1)
        round1
        ;;
    2)
        round2
        ;;
    3)
        round3
        ;;
    all)
        round1
        round2
        round3
        ;;
    *)
        echo "Usage: $0 {1|2|3|all}" >&2
        echo "  1   - Round 1: coarse grid (connection contention only)" >&2
        echo "  2   - Round 2: narrow sweep on top-N from round 1" >&2
        echo "  3   - Round 3: validate top-M from round 2 with repetitions" >&2
        echo "  all - Run all rounds sequentially" >&2
        echo "" >&2
        echo "Environment variables:" >&2
        echo "  YAMS_SWEEP_IO_THREADS          - Space-separated IO thread counts (default: 4 6 8 10 12 16)" >&2
        echo "  YAMS_SWEEP_MAX_INFLIGHT        - Space-separated inflight limits (default: 32 64 96 128 256)" >&2
        echo "  YAMS_SWEEP_MAX_ACTIVE_CONN     - Space-separated conn limits (default: 1024 2048 4096)" >&2
        echo "  YAMS_SWEEP_IPC_TIMEOUT_MS      - Space-separated timeouts for round 2 (default: 15000 30000)" >&2
        echo "  YAMS_SWEEP_CONNECTION_LIFETIME_S - Space-separated lifetimes for round 2 (default: 0 600 1800)" >&2
        echo "  YAMS_SWEEP_R1_TOP_N            - Top N from round 1 (default: 5)" >&2
        echo "  YAMS_SWEEP_R2_TOP_N            - Top N from round 2 (default: 3)" >&2
        echo "  YAMS_SWEEP_R3_REPS             - Repetitions per config in round 3 (default: 3)" >&2
        echo "  YAMS_MULTI_CLIENT_BENCH_BIN    - Path to benchmark binary" >&2
        echo "  YAMS_BENCH_DATA_DIR            - Data dir for large corpus reads (optional)" >&2
        echo "  YAMS_MULTI_CLIENT_BASELINE     - Baseline JSON for regression checks" >&2
        exit 2
        ;;
esac

echo ""
echo "Sweep artifacts: $SWEEP_DIR"
