# P2P shared-store integration matrix

The normative trust and operating-mode contract is
[`docs/spec/p2p-memory-sync-contract.md`](../spec/p2p-memory-sync-contract.md).
These tests validate shared-store replication; they do not claim direct peer
transport, discovery, NAT traversal, relay, or cryptographic writer identity.
Schema-v4 authentication has focused Catch2/filesystem coverage, but these
container lanes intentionally remain backend-ACL mode until a dedicated signed
multi-daemon fixture provisions distinct private keys and a shared membership manifest.

## Lanes

| Lane | Backend and mode | Observable acceptance criteria | CI |
|---|---|---|---|
| `filesystem` | Shared Docker volume, persistent | Exact JSON status/read assertions, bidirectional convergence, causal tombstone propagation, and genuinely concurrent writes released by a barrier | Pull requests touching sync paths |
| `s3-persistent` | Internal-only MinIO, persistent | S3-backed convergence without a shared memory-sync volume, bounded status counts and convergence deadline, daemon restart/resume, schema-v2 migration to a bound schema-v3 sibling, corruption quarantine, causal deletion, corpus isolation, rejected credentials, and pause/recovery | Weekly and manual slow lane |
| `s3-temporary` | Internal-only MinIO, temporary | Concurrent distinct sessions, same-session rejoin, causal deletion, ownership-scoped explicit teardown, SIGKILL orphan expiry through a bounded lease collector, no remaining session or tombstone path, and survival of unrelated persistent data | Weekly and manual slow lane |

Temporary sessions refresh an ACL-protected lease after successful reconciliation.
When `temporary_session_ttl_ms` is nonzero, a newly starting temporary session
examines at most `max_index_objects_per_sync` lease records and removes an expired
session only when its complete object set fits in one equally bounded page. It
never partially deletes an oversized session. The TTL must be at least three sync
intervals. Collection therefore requires a live session startup; this is not an
always-on lifecycle service when every daemon is offline.

## Commands

Run one lane at a time through xplan:

```bash
python3 tests/benchmarks/xplan/runner.py run p2p_memory_sync_local --arm filesystem
python3 tests/benchmarks/xplan/runner.py run p2p_memory_sync_local --arm s3-persistent
python3 tests/benchmarks/xplan/runner.py run p2p_memory_sync_local --arm s3-temporary
```

Every invocation creates a unique Compose project, bucket, prefix, daemon data
volumes, and private credentials. Credentials/configuration and failure evidence
are created below a mode-`0700` `mktemp` directory with a mode-`077` umask.
Successful runs remove the directory and all Compose volumes. Failed runs retain
`compose-ps.txt`, `compose.log`, and daemon logs and print their location. MinIO
and the Ubuntu build base use pinned image digests; MinIO publishes no host port.

The harness treats command exit status as authoritative. Successful CLI calls
must exit zero; expected rejection checks (missing tombstones, isolated corpus,
and wrong credentials) explicitly require nonzero. Status/read payloads are
parsed as JSON and their complete key sets, types, backend, size, and decoded
bytes are asserted rather than matched as substrings. Every successful replicated
read records observed monotonic `lag_ms` and its deadline in
`evidence/convergence-lag.jsonl`; status assertions separately require a started
service and enforce the configured 256-record bound.

## Persistent S3 migration fixture

The S3 lane configures `memory_sync.allow_legacy_unbound=true`, an explicit
one-time migration gate. It injects a schema-v2 record through a minimal standard
library SigV4 fixture, then requires a daemon to read the value and performs an
exact authenticated `HEAD` for the deterministically derived identity-bound
schema-v3 sibling. A malformed envelope is injected separately and
must increment `p2p status --json`'s `quarantined` count without blocking unrelated
keys.

The primary local corpus remains on each daemon's `/data` volume. Only the
memory-sync backend points at `s3://...`; each S3 daemon receives a distinct
`/mem` volume, and the barrier uses per-container `/tmp` state released by the
host. Restarting a daemon preserves its own data volume while MinIO preserves
the durable sync history.

## Local validation record

On 2026-08-16, all three commands above passed twice consecutively on Docker
29.4.0 / Compose 5.1.2 (Apple arm64 host). The persistent run observed successful
restart/resume, schema migration, corruption quarantine, rejected bad credentials,
corpus isolation, and MinIO pause/recovery. The temporary run observed distinct
concurrent-session isolation, same-session rejoin, SIGKILL orphan expiry through a
bounded startup collector, explicit collector teardown, and survival of an
unrelated persistent marker. These are local MinIO results, not AWS S3 or R2
claims.

## Local execution

The matrix is intentionally local and is not registered as a dedicated CI workflow.
Run only the lane needed for the current question; xplan retains per-run evidence under
`build/benchmarks/p2p_memory_sync_local/`. A green filesystem lane must never
be presented as S3 evidence, and a green persistent lane must never be presented as
temporary teardown evidence. Like the optional xplan framework, local orchestration
artifacts need not be staged merely to preserve benchmark evidence.
