# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 YAMS Contributors
#
# P2P memory-sync harness scenarios:
#   (1) filesystem<->filesystem via the shared volume — write in A, read in B,
#       and vice versa (convergence in both directions).
#   (3) causal ordering — interleaved writes, assert an identical final merged
#       index on both daemons (version-vector + LWW determinism).
# Scenarios (2)/(4)/(5) are documented in build/benchmarks/p2p-container-harness.md
# and exercised by the same helpers where applicable.

set -euo pipefail

COMPOSE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$COMPOSE_DIR/docker-compose.yml"

DOCKER="${DOCKER:-docker}"
COMPOSE="$DOCKER compose -f $COMPOSE_FILE"

log() { printf '\033[1;34m[p2p-harness]\033[0m %s\n' "$*"; }
pass() { printf '\033[1;32m[ok]\033[0m %s\n' "$*"; }
fail() {
	printf '\033[1;31m[fail]\033[0m %s\n' "$*" >&2
	exit 1
}

# Run the `yams p2p` CLI inside a container (thin wrapper over MemorySyncService).
yams_p2p() {
	local svc="$1"
	shift
	$COMPOSE exec -T "$svc" yams p2p "$@"
}

# Poll until a read in `svc` returns the expected value for `key`.
expect_value() {
	local svc="$1" key="$2" expected="$3"
	local tries="${4:-30}"
	for _ in $(seq 1 "$tries"); do
		if got="$(yams_p2p "$svc" read "$key" 2>/dev/null || true)" && [[ "$got" == "$expected" ]]; then
			return 0
		fi
		sleep 1
	done
	fail "expected '$expected' under '$key' in $svc (last read: '$got')"
}

log "building and starting containers"
$COMPOSE up --build -d
trap '$COMPOSE down -v >/dev/null 2>&1 || true' EXIT

# Wait for both daemons to be reachable.
log "waiting for daemons"
for _ in $(seq 1 60); do
	if $COMPOSE exec -T saged-a yams p2p status >/dev/null 2>&1; then
		break
	fi
	sleep 1
done

# ---------------------------------------------------------------------------
# Scenario (1): filesystem<->filesystem convergence via shared volume.
# ---------------------------------------------------------------------------
log "scenario 1: filesystem<->filesystem convergence"
yams_p2p saged-a publish greeting "hello-from-a"
expect_value saged-b greeting "hello-from-a"
yams_p2p saged-b publish greeting "hello-from-b"
expect_value saged-a greeting "hello-from-b"
pass "scenario 1: bidirectional filesystem convergence"

# ---------------------------------------------------------------------------
# Scenario (3): causal ordering — interleaved writes produce one merged index.
# ---------------------------------------------------------------------------
log "scenario 3: causal ordering"
# Interleave writes from both daemons under distinct keys.
yams_p2p saged-a publish k1 "a1"
yams_p2p saged-b publish k2 "b1"
yams_p2p saged-a publish k3 "a2"
yams_p2p saged-b publish k1 "b2" # concurrent-ish update to k1 from B
expect_value saged-a k1 "b2"
expect_value saged-b k1 "b2"

# The final merged index (key -> winner) must be identical on both daemons.
# (Explicit per-key checks: macOS bash 3.2 lacks `declare -A` associative arrays.)
for svc in saged-a saged-b; do
	expect_value "$svc" k1 "b2"
	expect_value "$svc" k2 "b1"
	expect_value "$svc" k3 "a2"
done
pass "scenario 3: identical final merged index on both daemons"

log "harness complete: scenario (1) and (3) passed"
