#!/usr/bin/env bash
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 YAMS Contributors
#
# Honest shared-store matrix:
#   filesystem       fast shared-volume convergence arm
#   s3-persistent    slow internal MinIO durability/recovery/migration arm
#   s3-temporary     slow internal MinIO disposable-session arm

set -euo pipefail
umask 077

ARM="${1:-filesystem}"
case "$ARM" in
filesystem | s3-persistent | s3-temporary) ;;
*)
    printf 'usage: %s {filesystem|s3-persistent|s3-temporary}\n' "$0" >&2
    exit 2
    ;;
esac

HARNESS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_ID="$(date +%s)-$$-$(od -An -N3 -tx1 /dev/urandom | tr -d ' \n')"
SAFE_ARM="${ARM//[^a-zA-Z0-9]/-}"
PROJECT="yams-p2p-${SAFE_ARM}-${RUN_ID}"
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/yams-p2p-${SAFE_ARM}.XXXXXX")"
EVIDENCE_DIR="${YAMS_P2P_EVIDENCE_DIR:-$WORK_DIR/evidence}"
mkdir -p -m 700 "$EVIDENCE_DIR"

export YAMS_P2P_CONFIG_A="$WORK_DIR/config-a.toml"
export YAMS_P2P_CONFIG_B="$WORK_DIR/config-b.toml"
export YAMS_P2P_ACCESS_KEY="yams$(od -An -N8 -tx1 /dev/urandom | tr -d ' \n')"
export YAMS_P2P_SECRET_KEY="$(od -An -N24 -tx1 /dev/urandom | tr -d ' \n')"
export YAMS_P2P_BUCKET="yams-p2p-${RUN_ID//[^a-zA-Z0-9]/-}"
export COMPOSE_PROJECT_NAME="$PROJECT"

COMPOSE=(docker compose -f "$HARNESS_DIR/docker-compose.yml" --project-name "$PROJECT")
if [[ "$ARM" == s3-* ]]; then
    export COMPOSE_PROFILES=s3
    # The S3 lanes must not have any shared memory-sync or barrier volume.
    export YAMS_P2P_MEM_A=mem-a
    export YAMS_P2P_MEM_B=mem-b
fi

log() { printf '\033[1;34m[p2p:%s]\033[0m %s\n' "$ARM" "$*"; }
pass() { printf '\033[1;32m[ok]\033[0m %s\n' "$*"; }
fail() {
    printf '\033[1;31m[fail]\033[0m %s\n' "$*" >&2
    return 1
}

cleanup() {
    local status=$?
    if [[ "$status" -ne 0 ]]; then
        "${COMPOSE[@]}" ps >"$EVIDENCE_DIR/compose-ps.txt" 2>&1 || true
        "${COMPOSE[@]}" logs --no-color >"$EVIDENCE_DIR/compose.log" 2>&1 || true
        for svc in saged-a saged-b; do
            "${COMPOSE[@]}" exec -T "$svc" sh -c 'cat /data/saged.log' \
                >"$EVIDENCE_DIR/${svc}.log" 2>&1 || true
        done
        printf '[p2p:%s] failure evidence retained at %s\n' "$ARM" "$EVIDENCE_DIR" >&2
    fi
    "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
    if [[ "$status" -eq 0 ]]; then rm -rf "$WORK_DIR"; fi
    return "$status"
}
trap cleanup EXIT

write_config() {
    local file="$1" node="$2" corpus="$3" mode="$4" session="$5" secret="$6"
    local migration="${7:-false}"
    local backend=filesystem path=/mem legacy_line=""
    if [[ "$ARM" == s3-* ]]; then
        backend=s3
        path="s3://${YAMS_P2P_BUCKET}/${RUN_ID}"
    fi
    local temporary_ttl_ms=0
    if [[ "$mode" == temporary ]]; then
        temporary_ttl_ms=1500
    elif [[ "$migration" == true ]]; then
        legacy_line="allow_legacy_unbound = true"
    fi
    cat >"$file" <<EOF
[version]
config_version = 3
[daemon]
auto_load_plugins = false
auto_repair = false
[storage]
engine = "local"
[storage.s3]
url = "s3://${YAMS_P2P_BUCKET}/primary-unused"
endpoint = "http://minio:9000"
access_key = "${YAMS_P2P_ACCESS_KEY}"
secret_key = "${secret}"
region = "us-east-1"
use_path_style = true
[memory_sync]
enabled = true
node_id = "${node}"
corpus_id = "${corpus}"
corpus_epoch = 1
backend = "${backend}"
path = "${path}"
sync_interval_ms = 250
max_index_objects_per_sync = 256
max_merged_keys = 256
mode = "${mode}"
session_id = "${session}"
temporary_session_ttl_ms = ${temporary_ttl_ms}
${legacy_line}
EOF
    chmod 600 "$file"
}

reset_configs() {
    local corpus="${1:-matrix-corpus}" mode="${2:-persistent}" session="${3:-}"
    local migration="${4:-false}"
    write_config "$YAMS_P2P_CONFIG_A" 123e4567-e89b-42d3-a456-42661417400a "$corpus" "$mode" "$session" "$YAMS_P2P_SECRET_KEY" "$migration"
    write_config "$YAMS_P2P_CONFIG_B" 123e4567-e89b-42d3-a456-42661417400b "$corpus" "$mode" "$session" "$YAMS_P2P_SECRET_KEY" "$migration"
}

yams_cli() {
    local svc="$1"
    shift
    "${COMPOSE[@]}" exec -T "$svc" yams "$@"
}
yams_p2p() {
    local svc="$1"
    shift
    yams_cli "$svc" p2p "$@"
}

assert_status() {
    local svc="$1" backend="$2" min_quarantine="${3:-0}" json
    json="$(yams_p2p "$svc" status --json)"
    python3 - "$backend" "$min_quarantine" "$json" <<'PY'
import json, sys
expected_backend, minimum = sys.argv[1], int(sys.argv[2])
data = json.loads(sys.argv[3])
required = {"auth_failures", "backend", "node_id", "quarantined", "records", "started"}
assert required <= set(data), data
assert data["backend"] == expected_backend and data["started"] is True, data
assert isinstance(data["records"], int) and 0 <= data["records"] <= 256, data
assert isinstance(data["quarantined"], int) and data["quarantined"] >= minimum, data
assert isinstance(data["auth_failures"], int) and data["auth_failures"] >= 0, data
assert len(data["node_id"]) == 36, data
PY
}

assert_read() {
    local svc="$1" key="$2" expected="$3" tries="${4:-80}" json started_ms elapsed_ms
    started_ms="$(python3 -c 'import time; print(time.monotonic_ns() // 1000000)')"
    for _ in $(seq 1 "$tries"); do
        if json="$(yams_p2p "$svc" read "$key" --json 2>/dev/null)" &&
            python3 - "$key" "$expected" "$json" <<'PY'; then
import json, sys
key, expected = sys.argv[1], sys.argv[2].encode()
data = json.loads(sys.argv[3])
assert set(data) == {"key", "size", "value_hex"}, data
assert data["key"] == key and bytes.fromhex(data["value_hex"]) == expected, data
assert data["size"] == len(expected), data
PY
            elapsed_ms=$(($(python3 -c 'import time; print(time.monotonic_ns() // 1000000)') - started_ms))
            ((elapsed_ms <= tries * 250 + 1000)) || fail "convergence exceeded bounded lag"
            printf '{"service":"%s","key":"%s","lag_ms":%d,"bound_ms":%d}\n' \
                "$svc" "$key" "$elapsed_ms" "$((tries * 250 + 1000))" \
                >>"$EVIDENCE_DIR/convergence-lag.jsonl"
            return 0
        fi
        sleep 0.25
    done
    fail "${svc} did not converge key=${key} value=${expected}"
}

report_lag() {
    python3 - "$EVIDENCE_DIR/convergence-lag.jsonl" <<'PY'
import json, pathlib, sys
rows = [json.loads(line) for line in pathlib.Path(sys.argv[1]).read_text().splitlines()]
assert rows and all(0 <= row["lag_ms"] <= row["bound_ms"] for row in rows), rows
print(f"[lag] samples={len(rows)} max_ms={max(row['lag_ms'] for row in rows)} "
      f"bound_ms={max(row['bound_ms'] for row in rows)}")
PY
}

assert_missing() {
    local svc="$1" key="$2"
    if yams_p2p "$svc" read "$key" --json >"$EVIDENCE_DIR/unexpected-read.json" 2>&1; then
        fail "${svc} unexpectedly read missing key=${key}"
    fi
}

wait_ready() {
    local backend="$1"
    shift
    local services=("$@")
    if [[ ${#services[@]} -eq 0 ]]; then services=(saged-a saged-b); fi
    for svc in "${services[@]}"; do
        local ready=false
        for _ in $(seq 1 120); do
            if assert_status "$svc" "$backend" 0 >/dev/null 2>&1; then
                ready=true
                break
            fi
            sleep 0.5
        done
        if ! $ready; then
            local last_status
            last_status="$(yams_p2p "$svc" status --json 2>&1 || true)"
            fail "$svc memory-sync service did not become ready (last status: $last_status)"
        fi
    done
}

start_daemons() {
    if [[ "$ARM" == s3-* ]]; then
        "${COMPOSE[@]}" run --rm minio-init
        "${COMPOSE[@]}" up -d minio
        sleep 2
    fi
    # Always ask Compose to build so the lane validates the current checkout. Docker layer caching
    # keeps unchanged rebuilds cheap while avoiding false failures from a stale local image.
    "${COMPOSE[@]}" up --build -d saged-a saged-b
    wait_ready "${ARM%%-*}"
}

concurrent_barrier_write() {
    rm -f "$EVIDENCE_DIR"/race-*.json
    "${COMPOSE[@]}" exec -T saged-a sh -c 'touch /tmp/race-ready; while [ ! -e /tmp/race-go ]; do sleep 0.02; done; yams p2p publish race from-a --json' >"$EVIDENCE_DIR/race-a.json" &
    local pid_a=$!
    "${COMPOSE[@]}" exec -T saged-b sh -c 'touch /tmp/race-ready; while [ ! -e /tmp/race-go ]; do sleep 0.02; done; yams p2p publish race from-b --json' >"$EVIDENCE_DIR/race-b.json" &
    local pid_b=$!
    for _ in $(seq 1 100); do
        if "${COMPOSE[@]}" exec -T saged-a test -e /tmp/race-ready &&
            "${COMPOSE[@]}" exec -T saged-b test -e /tmp/race-ready; then break; fi
        sleep 0.05
    done
    "${COMPOSE[@]}" exec -T saged-a touch /tmp/race-go
    "${COMPOSE[@]}" exec -T saged-b touch /tmp/race-go
    wait "$pid_a"
    wait "$pid_b"
    local a b
    for _ in $(seq 1 80); do
        a="$(yams_p2p saged-a read race 2>/dev/null || true)"
        b="$(yams_p2p saged-b read race 2>/dev/null || true)"
        if [[ "$a" == "$b" && ("$a" == from-a || "$a" == from-b) ]]; then return 0; fi
        sleep 0.25
    done
    fail "barrier writes did not converge (a=${a}, b=${b})"
}

base_matrix() {
    local publish_output
    if ! publish_output="$(yams_p2p saged-a publish greeting hello-from-a --json 2>&1)"; then
        fail "initial publish failed: $publish_output"
    fi
    assert_read saged-b greeting hello-from-a
    yams_p2p saged-b publish response hello-from-b --json >/dev/null
    assert_read saged-a response hello-from-b
    concurrent_barrier_write
    yams_p2p saged-a publish doomed live --json >/dev/null
    assert_read saged-b doomed live
    yams_p2p saged-a delete doomed --json >/dev/null
    for _ in $(seq 1 80); do
        assert_missing saged-b doomed && break
        sleep 0.25
    done
    assert_missing saged-b doomed
}

run_filesystem() {
    reset_configs matrix-corpus persistent ""
    start_daemons
    base_matrix
    report_lag
    pass "filesystem shared-store convergence, barrier writes, and tombstones"
}

s3_fixture() {
    "${COMPOSE[@]}" exec -T \
        -e AWS_ACCESS_KEY_ID="$YAMS_P2P_ACCESS_KEY" \
        -e AWS_SECRET_ACCESS_KEY="$YAMS_P2P_SECRET_KEY" \
        -e AWS_REGION=us-east-1 saged-a \
        python3 /harness/s3_fixture.py "$@" --bucket "$YAMS_P2P_BUCKET"
}

s3_fixture_detached() {
    "${COMPOSE[@]}" run --rm --no-deps --entrypoint python3 \
        -e AWS_ACCESS_KEY_ID="$YAMS_P2P_ACCESS_KEY" \
        -e AWS_SECRET_ACCESS_KEY="$YAMS_P2P_SECRET_KEY" \
        -e AWS_REGION=us-east-1 saged-a \
        /harness/s3_fixture.py "$@" --bucket "$YAMS_P2P_BUCKET"
}

run_s3_persistent() {
    reset_configs matrix-corpus persistent ""
    start_daemons
    base_matrix

    "${COMPOSE[@]}" restart saged-b >/dev/null
    wait_ready s3
    assert_read saged-b greeting hello-from-a

    "${COMPOSE[@]}" pause minio >/dev/null
    sleep 1
    "${COMPOSE[@]}" unpause minio >/dev/null
    local recovered=false
    for _ in $(seq 1 80); do
        if yams_p2p saged-a publish recovered after-partition --json >/dev/null 2>&1; then
            recovered=true
            break
        fi
        sleep 0.25
    done
    $recovered || fail "S3 publish did not recover within 20 seconds"
    assert_read saged-b recovered after-partition

    reset_configs matrix-corpus persistent "" true
    "${COMPOSE[@]}" up -d --force-recreate saged-a saged-b >/dev/null
    wait_ready s3
    s3_fixture put-legacy --prefix "$RUN_ID" --value legacy-value
    assert_read saged-b legacy legacy-value
    s3_fixture head-migrated --prefix "$RUN_ID" --value legacy-value

    reset_configs matrix-corpus persistent ""
    "${COMPOSE[@]}" up -d --force-recreate saged-a saged-b >/dev/null
    wait_ready s3
    s3_fixture put-corrupt --prefix "$RUN_ID"
    for _ in $(seq 1 80); do
        if assert_status saged-a s3 1 >/dev/null 2>&1; then break; fi
        sleep 0.25
    done
    assert_status saged-a s3 1

    cp "$YAMS_P2P_CONFIG_B" "$WORK_DIR/config-b.good.toml"
    write_config "$YAMS_P2P_CONFIG_B" 123e4567-e89b-42d3-a456-42661417400b matrix-corpus persistent "" wrong-secret
    "${COMPOSE[@]}" up -d --force-recreate saged-b >/dev/null
    sleep 2
    if yams_p2p saged-b publish auth-probe denied --json >/dev/null 2>&1; then
        fail "wrong S3 credentials unexpectedly authorized a write"
    fi
    cp "$WORK_DIR/config-b.good.toml" "$YAMS_P2P_CONFIG_B"
    "${COMPOSE[@]}" up -d --force-recreate saged-b >/dev/null
    wait_ready s3

    write_config "$YAMS_P2P_CONFIG_B" 123e4567-e89b-42d3-a456-42661417400b isolated-corpus persistent "" "$YAMS_P2P_SECRET_KEY"
    "${COMPOSE[@]}" up -d --force-recreate saged-b >/dev/null
    sleep 2
    yams_p2p saged-a publish corpus-only visible-a --json >/dev/null
    sleep 1
    assert_missing saged-b corpus-only
    cp "$WORK_DIR/config-b.good.toml" "$YAMS_P2P_CONFIG_B"
    "${COMPOSE[@]}" up -d --force-recreate saged-b >/dev/null
    wait_ready s3
    assert_read saged-b corpus-only visible-a
    report_lag
    pass "persistent S3 durability, migration, quarantine, isolation, credentials, and recovery"
}

run_s3_temporary() {
    local session="session-${RUN_ID//[^a-zA-Z0-9]/-}"
    reset_configs matrix-corpus temporary "$session"
    start_daemons
    s3_fixture put --prefix "$RUN_ID" --key persistent-marker --value keep
    base_matrix

    write_config "$YAMS_P2P_CONFIG_B" 123e4567-e89b-42d3-a456-42661417400b matrix-corpus temporary other-session "$YAMS_P2P_SECRET_KEY"
    "${COMPOSE[@]}" up -d --force-recreate saged-b >/dev/null
    wait_ready s3
    # Both session namespaces remain active concurrently: A stays in the original
    # session while B writes through a distinct session in the same bucket/prefix.
    yams_p2p saged-a publish private-to-session session-a --json >/dev/null
    yams_p2p saged-b publish private-to-other-session session-b --json >/dev/null
    assert_read saged-a private-to-session session-a
    assert_read saged-b private-to-other-session session-b
    sleep 1
    assert_missing saged-b private-to-session
    assert_missing saged-a private-to-other-session

    write_config "$YAMS_P2P_CONFIG_B" 123e4567-e89b-42d3-a456-42661417400b matrix-corpus temporary "$session" "$YAMS_P2P_SECRET_KEY"
    "${COMPOSE[@]}" up -d --force-recreate saged-b >/dev/null
    wait_ready s3
    assert_read saged-b private-to-session session-a

    # Simulate an ungraceful crash: SIGKILL bypasses MemorySyncService::stop(), leaving
    # the session and lease behind. Starting another temporary session after the TTL
    # must collect the bounded stale namespace without touching persistent data.
    "${COMPOSE[@]}" kill -s SIGKILL saged-a saged-b >/dev/null
    sleep 2
    local collector_session="collector-${RUN_ID//[^a-zA-Z0-9]/-}"
    write_config "$YAMS_P2P_CONFIG_B" 123e4567-e89b-42d3-a456-42661417400b matrix-corpus temporary "$collector_session" "$YAMS_P2P_SECRET_KEY"
    "${COMPOSE[@]}" up -d --force-recreate saged-b >/dev/null
    wait_ready s3 saged-b
    s3_fixture_detached head --prefix "$RUN_ID" --key persistent-marker >/dev/null

    local leftovers
    leftovers="$("${COMPOSE[@]}" exec -T minio sh -c "find /data/${YAMS_P2P_BUCKET} -path '*${session}*' -print" 2>/dev/null || true)"
    [[ -z "$leftovers" ]] || fail "expired temporary namespace leaked after recovery: $leftovers"

    "${COMPOSE[@]}" stop saged-b >/dev/null
    s3_fixture_detached head --prefix "$RUN_ID" --key persistent-marker >/dev/null
    leftovers="$("${COMPOSE[@]}" exec -T minio sh -c "find /data/${YAMS_P2P_BUCKET} -path '*${collector_session}*' -print" 2>/dev/null || true)"
    [[ -z "$leftovers" ]] || fail "collector namespace leaked after explicit teardown: $leftovers"
    report_lag
    pass "temporary S3 concurrent sessions, crash expiry, isolation, and owned teardown"
}

log "starting project=${PROJECT} evidence=${EVIDENCE_DIR}"
case "$ARM" in
filesystem) run_filesystem ;;
s3-persistent) run_s3_persistent ;;
s3-temporary) run_s3_temporary ;;
esac
