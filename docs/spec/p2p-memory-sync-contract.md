# P2P Memory-Sync Contract

Status: design contract for the hardening work tracked from
`DIFFERENTIAL_REVIEW_REPORT.md`. Unless an item is marked **implemented**, it is
an acceptance criterion, not a claim about the current daemon.

## Purpose and boundary

Memory sync reconciles a YAMS corpus through a backend namespace shared by
trusted daemon instances. The current implementation is shared-store
reconciliation over a filesystem or S3-compatible backend; it is not a direct
peer transport.

The daemon owns the periodic `MemorySyncService`. CLI `p2p publish`, `read`, and
`status` calls use the local daemon socket and do not create a second sync
worker.

Memory sync does not provide:

- peer discovery, NAT traversal, relay, or direct daemon-to-daemon transport;
- anonymous or public sharing;
- authentication merely because two nodes know the same backend URL (authentication exists only when the explicit schema-v4 writer-auth mode is enabled);
- backup, archival retention, or membership consensus;
- safe multi-writer operation until the identity, envelope, integrity, and
  tombstone criteria below are implemented.

## Actors and trust boundaries

| Actor | Authority | Required checks |
|---|---|---|
| Local CLI client | Requests user-record publish/read/status through a local daemon socket | Socket authorization, key/value limits, user namespace only |
| Daemon writer | Publishes typed records for one stable writer identity | Unique persistent writer ID, corpus binding, envelope validation |
| Shared backend | Stores immutable envelopes and blobs | Treat all returned bytes and paths as untrusted; verify before use |
| Peer daemon | Produces records visible through the shared backend | Admit only records matching the corpus/epoch and configured writer trust policy |
| Operator | Selects backend, namespace, mode, trust policy, and retention | Explicit configuration; no inferred widening of visibility |

Backend ACLs remain the default trust mechanism and are necessary in every mode.
They do not prove which daemon wrote an object. When `writer_auth_required` is
false, documentation and status output must say **backend-ACL trusted**. When it
is true and the corpus/epoch manifest validates, schema-v4 envelopes provide
authenticated writers for shared-store reconciliation; this still is not direct
or authenticated peer-to-peer transport.

## Corpus and namespace identity

Every sync instance must resolve a non-empty, stable `corpus_id` and an epoch.
The pair `(corpus_id, epoch)` defines one replication domain. Records from a
different pair must be rejected or quarantined rather than merged.

A daemon writer ID must be unique and stable across restart. `"default"`, an
empty value, or a newly generated value on every start is invalid when sync is
enabled. Equal version vectors with different payloads are a writer-identity
fork and must fail closed with health telemetry.

The logical-key namespace is split into:

- `user/<escaped-key>`: generic CLI-published values;
- reserved typed namespaces owned by adapters, including documents, content
  blobs, embeddings, topology nodes, and topology edges;
- protocol namespaces for envelopes, checkpoints, quarantine, and tombstones.

Generic publish must not write a reserved namespace. Keys must have a canonical
encoding and bounded decoded length. Empty keys, absolute paths, dot segments,
backslashes, control bytes, and any path that can escape a filesystem backend
root are invalid. The filesystem backend must enforce canonical containment
independently of caller validation.

## Envelope and integrity rules

An accepted envelope must include at least:

- schema version;
- corpus ID and epoch;
- operation ID;
- canonical logical key and record kind;
- content hash or tombstone marker;
- stable writer origin;
- version vector and hybrid timestamp.

A backend-ACL writer emits identity-bound schema version `3`. An authenticated
writer emits schema version `4`, signs canonical envelope bytes with Ed25519,
and includes `signatureAlgorithm`, `signingKeyId`, and the signature. The
signature covers schema, corpus, epoch, operation ID, logical key, record kind,
content/tombstone identity, writer origin, vector, timestamp, and key ID. During
the migration window, the decoder parses these persisted shapes:

| Shape | Identification | Normalization |
|---|---|---|
| Legacy v1 | no `schemaVersion`, vector in `version`, writer in `ts.origin` | move writer to top-level `origin`; rename `version` to `vv` |
| Intermediate | no `schemaVersion`, top-level `origin` and `vv` | treat as the v2 in-memory representation |
| Integrity v2 | `schemaVersion: 2`, top-level `origin` and `vv` | parse for diagnostics; quarantine by default because corpus identity is absent |
| ACL-bound v3 | `schemaVersion: 3`, top-level `origin`/`vv` plus corpus, operation, key, and kind fields | canonical identity-bound but unsigned representation |
| Authenticated v4 | v3 fields plus Ed25519 algorithm, signing key ID, and signature | canonical authenticated-writer representation |

Unknown explicit versions are quarantined per record, not treated as another
legacy shape. Legacy v1 and integrity v2 records are admitted only by an explicit
migration reader (`allowLegacyUnbound`) bound to one operator-selected corpus.
That reader writes an equivalent schema-v3 envelope under its content-addressed
index key before admitting the record; it leaves the verified legacy object in
place for rollback. A subsequent strict reader can therefore use the upgraded
record while quarantining the unbound predecessor. Normal daemon sync fails
closed unless `allow_legacy_unbound = true` is explicitly configured for the
migration run. Removing legacy
decode support requires a separately documented migration and compatibility
window.

Authenticated mode never upgrades or accepts an unsigned predecessor. Migration
is two-phase: first run the explicit persistent schema-v1/v2-to-v3 migration with
`writer_auth_required = false`; inspect the durable v3 siblings and quarantine
count; then disable `allow_legacy_unbound`, distribute the corpus/epoch writer
manifest, and enable `writer_auth_required`. Existing v3 history remains unsigned
and is quarantined in authenticated mode. If that history must remain live, an
operator-controlled offline migration must reissue it as new signed operations;
the daemon does not relabel another writer's history with the local private key.

### Configuration migration

Enabling memory sync now requires all of the following typed TOML fields:

```toml
[memory_sync]
enabled = true
node_id = "123e4567-e89b-42d3-a456-426614174000" # stable UUID; persist across restart
corpus_id = "team-corpus"                         # 1-128 alnum/._- characters
corpus_epoch = 1                                  # positive; bump only for an incompatible reset
max_index_objects_per_sync = 512                  # envelope requests per reconciliation
max_envelope_bytes = 65536
max_value_bytes = 16777216
max_merged_keys = 4096
max_cache_bytes = 67108864
max_tracked_identities = 8192
mode = "persistent"                               # or "temporary"
# session_id = "run-123"                          # required only for temporary mode
# temporary_session_ttl_ms = 15000                 # optional; >= 3 sync intervals
# allow_legacy_unbound = true                      # one-time persistent migration only
# writer_auth_required = true                      # require signed schema-v4 records
# writer_auth_manifest = "/etc/yams/writers.json"  # ACL-protected manifest below
```

There is no `"default"` writer fallback and no per-start UUID generation. An
operator upgrading an existing deployment must assign one unique stable UUID per
daemon and one shared `(corpus_id, corpus_epoch)` pair to intended replicas before
re-enabling sync. Reusing a writer UUID on two live daemons can create the same
`origin:counter` operation; different envelopes for that operation are quarantined
as a fork. This is causal duplicate detection under the backend-ACL trust model,
not cryptographic writer authentication unless schema-v4 auth mode is enabled.

The writer-auth manifest is scoped to exactly one corpus and epoch. Paths may be
absolute or relative to the manifest. The Ed25519 private key must be an
unencrypted PEM protected by operating-system ACLs and is local-only; public keys
form explicit membership. The daemon bounds and parses these files but does not
validate platform ownership or permission bits, so ACL enforcement remains an
operator/deployment prerequisite. Key/manifest files are bounded to 64 KiB and membership
is bounded by both the manifest limit and `max_tracked_identities`. Multiple active key IDs for one writer implement
rotation. Setting `revoked: true` rejects both new and historical envelopes from
that key on the next daemon start.

```json
{
  "schema_version": 1,
  "corpus_id": "team-corpus",
  "corpus_epoch": 1,
  "local_key": {
    "writer_id": "123e4567-e89b-42d3-a456-426614174000",
    "key_id": "writer-2026-q2",
    "private_key_path": "writer-2026-q2.pem"
  },
  "trusted_writers": [
    {
      "writer_id": "123e4567-e89b-42d3-a456-426614174000",
      "key_id": "writer-2026-q1",
      "public_key_path": "writer-2026-q1.pub",
      "revoked": false
    },
    {
      "writer_id": "123e4567-e89b-42d3-a456-426614174000",
      "key_id": "writer-2026-q2",
      "public_key_path": "writer-2026-q2.pub",
      "revoked": false
    }
  ]
}
```

Canonical replay behavior is fail closed across corpora or epochs because both
are signed. Replaying identical bytes within the same corpus/epoch is idempotent
under the existing operation-ID tracker. Reusing an operation ID for different
signed bytes remains a fork even when both signatures are valid.

Before merge or apply, a reader must verify:

1. the index-object digest against its canonical envelope bytes;
2. the blob digest against the envelope content hash;
3. origin/version-vector consistency;
4. corpus, epoch, key, record kind, and payload identity;
5. configured size/count limits;
6. when required, schema-v4 Ed25519 signature, trusted `(writer_id, key_id)` membership, non-revocation, and exact corpus/epoch scope.

Malformed, corrupt, foreign, or unauthorized records are quarantined with
bounded sanitized telemetry. They must not enter adapter stores and must not
block unrelated keys.

## Operating modes

### Persistent sync

Persistent mode uses an operator-owned durable backend namespace. Its contract
is:

- accepted history and tombstones survive daemon restart;
- a node resumes its stable writer identity and causal context;
- a restarted or temporarily offline peer converges without manual backend
  deletion;
- stopping one daemon does not delete shared state or mean that the peer left;
- deletion remains effective after restart and while a peer was offline;
- compaction/GC occurs only after the configured causal safety horizon;
- operators explicitly destroy or migrate the backend namespace.

A persistent-mode test must restart both the writer and reader processes while
retaining only the shared backend, then prove value, deletion, causal context,
and corpus isolation converge.

### Temporary sync

Temporary mode uses a unique disposable namespace/session with an explicit
expiry or teardown operation. Its contract is:

- the namespace is unique per session and cannot collide with persistent data
  or another concurrent temporary run;
- peers converge only while joined to that session;
- teardown stops workers before deleting session-owned backend state;
- teardown never removes local corpus data or another namespace;
- temporary envelopes, blobs, checkpoints, and tombstones do not become durable
  replication state after the documented expiry/cleanup boundary;
- failure cleanup is idempotent and ownership-labelled.

Deleting an arbitrary configured path is not a valid temporary-mode teardown.
The implementation must prove ownership of the disposable namespace before
removal.

When `temporary_session_ttl_ms` is enabled, the service refreshes an
ACL-protected lease after successful reconciliation. A newly starting temporary
session scans one bounded lease page and collects an expired session only if its
complete object set fits in one bounded page; oversized sessions are left intact
rather than partially deleted. The configured TTL must be at least three sync
intervals. Expiry collection therefore requires another session startup and does
not run while every daemon is offline. Explicit stop remains the immediate cleanup
boundary.

A temporary-mode test must run two isolated sessions concurrently, prove active
convergence, tear down one session, prove the other and persistent data remain,
and verify the removed session cannot resume from leaked backend state.

## Merge, deletion, and apply invariants

- Merge order must not affect the selected winner.
- Causally later operations beat earlier operations; concurrent operations use
  the documented deterministic tie-breaker.
- Equal-vector/different-payload records are forks, not ordinary concurrency.
- A missing or corrupt obsolete loser must not prevent a valid winner or an
  unrelated key from progressing.
- Deletion is a typed causally versioned tombstone, not absence from a listing.
- A tombstone must prevent resurrection by an offline peer until the causal GC
  horizon is safe.
- Concurrent value/delete operations use delete-wins semantics across every store.
  Adapter tombstones carry only the stable identity needed to delete a document,
  blob, embedding, topology node/edge, or user record; deleted content is absent.
- Persistent tombstone GC is disabled when the configured replica set is empty.
  When enabled, the replica set must be complete and stable: every configured peer
  records an acknowledgement for the exact tombstone operation, the minimum
  retention horizon must elapse, and the complete key history must fit one bounded
  listing page. GC removes historical values before the tombstone and then removes
  acknowledgement records. Missing-peer, time-only, or partial-history GC is
  prohibited because an offline peer could resurrect older state. Acknowledgement
  objects themselves remain backend-ACL trusted; schema-v4 envelope authentication
  does not sign or authenticate GC acknowledgements.
- Adapter application is idempotent and materializes the exact selected winner,
  including updates that lower a topology weight, remove edge properties, replace
  document path/status fields, or remove stale document metadata keys.
- Multi-record adapter apply parses and validates the complete winning batch before
  mutation. Repository/store calls are the transaction boundary; if a later write
  fails, the durable winning envelopes remain the retry journal and the next apply
  resumes convergence. Vector index dirty state is retained by the daemon until a
  coordinated rebuild and persistence both succeed.
- The current vector database identifies rows by `chunk_id`, while the sync wire
  identity is `(model, chunk_id)`. Therefore a winning batch containing two models
  for one chunk, or conflicting with an existing local model, is rejected with
  `NotSupported` before mutation rather than silently overwriting one model.
- Referenced content must verify and be locally available before metadata,
  vectors, or topology become visible.

## Resource and lifecycle invariants

All limits are typed configuration, not new product environment variables.
The implementation must bound:

- decoded key and value bytes;
- envelope and blob bytes;
- records/list pages processed per cycle;
- resident hydrated-blob cache;
- quarantined evidence and retained history;
- remote requests and work admitted per cycle;
- consecutive failures and retry/backoff behavior.

Reconciliation advances through deterministic index windows, selects winners
before hydrating blobs, and retains only winner blobs. Envelope fetches per call,
decoded envelope/value sizes, merged keys, cached bytes, and causal-identity
tracking are bounded by the typed fields above. A missing or corrupt obsolete
loser therefore cannot prevent hydration of a valid winner or unrelated key.

`IStorageBackend::listPage` returns at most the configured page size and an
opaque continuation cursor. S3 implements it with one native ListObjectsV2
request and its continuation token; compatibility `list` drains pages only for
legacy callers. `MemorySyncLoop` consumes exactly one page per reconciliation,
checks cancellation before each backend operation, and gates remote operations
through the daemon's `ResourceGovernor`. `IStorageBackend::requestCancel` interrupts
backends that support transport cancellation; the S3 backend wires this to libcurl's
progress callback. Apply and backfill check the service stop state between bounded
records/pages. One periodic reconciliation snapshot is reused by every adapter in
that callback, and potentially blocking publish/delete IPC work is admitted through
the bounded worker pool rather than run on the IPC executor.

Temporary mode scopes filesystem and S3 storage beneath
`.sessions/<session_id>`. The service owns only that scoped backend and removes
all of its envelopes, blobs, and tombstones during explicit idempotent `stop()`,
without touching persistent data or another session. Session IDs allow only
alphanumeric, dash, and underscore characters. Expiry and crash-orphan collection
remain lifecycle/integration work.

Shutdown disables callbacks and stops/joins memory sync before its destination
stores, `VectorIndexCoordinator`, `WorkCoordinator`, and other dependencies are
quiesced. Concurrent start/stop/status operations must be race-free and
idempotent.

## Health contract

`p2p status` distinguishes worker existence from observed sync health. Text and
machine-readable output report mode, backend type, corpus ID/epoch, writer ID,
trust mode (`backend-acl` or `authenticated-writers`), worker state, successful
and failed cycle counts, last-success age, record count, quarantine count, and a
sanitized saturating writer-authentication failure count.

Backend reachability, consecutive-failure streak, bounded last-error detail, and
backend lag/progress remain acceptance criteria. No writer, key, signature, or
OpenSSL detail is exposed.

Credentials, tokens, signed URLs, payload bytes, and private endpoints must not
appear in normal status or failure evidence.

## Acceptance matrix

| Invariant | Minimum automated evidence |
|---|---|
| Filesystem containment | Unit tests for traversal, absolute paths, separators, symlinks, and valid nested keys |
| Namespace separation | IPC/CLI tests proving generic publish cannot address typed namespaces |
| Upgrade compatibility | Persisted fixtures for each supported envelope version and mixed history |
| Content integrity | Wrong record/blob digest, truncation, and identity-mismatch tests |
| Corpus/writer isolation | Wrong corpus/epoch, duplicate writer, fork, and counter-overflow tests |
| Authenticated writers | Forged writer, invalid signature, revoked key, simultaneous rotation keys, epoch replay, bounded pre-hydration failure telemetry, and manifest migration tests |
| Failure isolation | Malformed/missing losing record plus unrelated-key forward progress |
| Bounded resources | Large-history request/cache assertions and typed-limit tests |
| Tombstones | Ordered and concurrent update/delete, offline peer, restart, retention, and GC tests |
| Exact adapter apply | Existing-row update, lower topology weight, multi-model vector, partial/rebuild failure tests |
| Lifecycle | Deterministic stop-during-work tests and a targeted TSAN lane |
| Persistent mode | Real durable MinIO/S3 restart/resume, deletion, isolation, partition/recovery arm |
| Temporary mode | Concurrent disposable sessions, expiry/teardown, no-leak and no-cross-delete arm |
| Harness honesty | Exact JSON assertions, nonzero failures preserved, unique project/namespace per run |

Lifecycle behavior is kept in the isolated `daemon_memory_sync_*` Meson registrations and the
`memory_sync_service` Catch2 suite. Sanitizer or release claims require a fresh run of the
corresponding build and must not rely on a checked-in local validation transcript.

A green shared-filesystem harness proves only shared-store convergence. It must
not be cited as S3, direct peer transport, authenticated writer, restart, or
temporary-session evidence unless the corresponding acceptance arm ran.
