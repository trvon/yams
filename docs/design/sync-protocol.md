# Distributed Corpus and P2P Sync Protocol

Status: draft design for the v0.x roadmap. This document describes the target
shape of distributed YAMS corpora; it is not an implementation contract yet.

## Purpose

YAMS is local-first: the current corpus is a content-addressed block store plus
local SQLite-backed catalogs, FTS, vector state, and Knowledge Graph state. A
distributed corpus must let several machines or containers share the same logical
corpus without sharing a live SQLite database or assuming that object-store list
operations are complete, cheap, or strongly consistent.

The design splits the system into two planes:

| Plane | Data | Rule |
|-------|------|------|
| Object plane | CAS blocks, chunk manifests, signed checkpoints | Immutable by hash; verify before use |
| Catalog plane | document metadata, paths, tags, tombstones, KG edges, vector cache manifests | Replicated by append-only ops and checkpoint roots |

## Current Implementation Map

| Area | Current source | Distributed implication |
|------|----------------|-------------------------|
| CAS blocks | `include/yams/storage/storage_engine.h`, `src/storage/storage_engine.cpp` | Hash-keyed blocks are a good replication unit, but local filesystem layout is not itself a sync protocol. |
| Content store | `include/yams/api/content_store.h`, `src/api/content_store_impl.cpp` | Document add/get/delete must publish catalog ops in addition to writing chunks. |
| Remote backends | `include/yams/storage/storage_backend.h`, `src/storage/storage_backend_factory.cpp`, `src/storage/storage_backend_engine_adapter.cpp` | S3/R2 support can host immutable objects, but list-heavy reconciliation and size scans do not scale. |
| Object plugins | `include/yams/plugins/object_storage_v1.h`, `include/yams/plugins/object_storage_iface.hpp` | Provider health, checksum, versioning, and DR hooks should feed sync readiness. |
| Metadata catalog | `include/yams/metadata/metadata_repository.h`, `src/metadata/metadata_repository.cpp` | SQLite remains node-local; replicated catalog state must be imported through repository APIs. |
| Knowledge Graph | `include/yams/metadata/knowledge_graph_store.h`, `src/metadata/knowledge_graph_store_sqlite.cpp` | KG mutations need deterministic op IDs and conflict rules. |
| Vector state | `include/yams/vector/vector_database.h`, `src/vector/vector_database.cpp`, `src/vector/sqlite_vec_backend.cpp` | Embeddings are derived from content hash and model ID; they may be synced as cache artifacts or rebuilt. |
| P2P repair hook | `include/yams/integrity/repair_manager.h`, `src/integrity/repair_manager.cpp` | `RepairStrategy::FromP2P` only fetches missing blocks today; it is not corpus sync. |
| Daemon readiness | `src/daemon/components/ServiceManager.cpp`, `include/yams/daemon/components/StateComponent.h` | Sync must only run when content store, database, metadata repo, and vector readiness gates are explicit. |

## Non-Goals

- Do not share the live SQLite metadata database across machines.
- Do not require a central coordinator for local-first operation.
- Do not treat object-store `list()` as the source of truth.
- Do not make vector indexes authoritative. They are reproducible caches unless explicitly checkpointed.
- Do not implement Byzantine consensus in v0.x. Verify hashes and signatures, but target trusted personal or team peers first.

## Corpus Identity

Each distributed corpus has stable identity and per-device writers.

| Field | Meaning |
|-------|---------|
| `corpus_id` | Random 128-bit or 256-bit identifier created at corpus initialization. |
| `site_id` | Stable identifier for one machine, container, or daemon instance. |
| `writer_key` | Signing key used to sign ops and checkpoints from one site. |
| `epoch` | Monotonic generation for destructive administrative events such as restore or re-key. |
| `checkpoint_hash` | Hash of the current signed checkpoint manifest. |

Peers trust a corpus only after an explicit join action imports the `corpus_id`,
writer public keys, and current checkpoint root. Object-store credentials do not
imply catalog trust.

## Object Namespace

Use a namespace that avoids global listing and makes pointer reads bounded.

| Prefix | Contents |
|--------|----------|
| `blocks/sha256/<aa>/<hash>` | CAS block bytes, optionally compressed by the existing content store path. |
| `manifests/chunks/<hash>.json` | Document chunk manifest keyed by manifest hash. |
| `ops/<site_id>/<seq>.json` | Signed catalog operation. |
| `checkpoints/<checkpoint_hash>.json` | Signed checkpoint manifest. |
| `heads/<site_id>.json` | Latest signed head for one writer. |
| `corpus/head.json` | Optional single-writer or compare-and-swap pointer for shared-store mode. |

The protocol reconciles from `heads/<site_id>.json` and checkpoint references.
Listing `blocks/` is never required for normal sync.

## Checkpoint Manifest

A checkpoint is a signed, content-addressed summary of catalog and object roots.

```json
{
  "version": 1,
  "corpus_id": "...",
  "epoch": 1,
  "site_id": "macbook-a",
  "sequence": 1842,
  "previous": ["sha256:..."],
  "created_at": "2026-05-24T00:00:00Z",
  "object_roots": ["sha256:..."],
  "catalog_op_ranges": [{"site_id": "macbook-a", "from": 1, "to": 1842}],
  "tombstone_root": "sha256:...",
  "kg_root": "sha256:...",
  "vector_roots": [{"model_id": "all-MiniLM-L6-v2", "root": "sha256:..."}],
  "signature": "..."
}
```

The checkpoint must be small enough to fetch on every sync round. Large catalogs
are represented by Merkleized op ranges, not one giant document list.

## Catalog Operation Model

Every user-visible mutation produces an operation with a deterministic ID.

| Operation | Required fields | Merge rule |
|-----------|-----------------|------------|
| `document.added` | document ID, content hash, chunk manifest hash, path, metadata | Add if content and manifest verify. Concurrent same-content adds merge aliases. |
| `document.removed` | document ID or path, tombstone ID, causal context | Tombstone wins over older updates. Concurrent remove/update becomes a conflict record. |
| `document.path_set` | document ID, old path, new path, causal context | Last causal writer wins when ordered; concurrent path changes are kept as aliases until resolved. |
| `tags.added` | document ID, tag set | Observed-remove set. Additions commute. |
| `tags.removed` | document ID, tag set, observed add IDs | Removes only observed additions. Concurrent unseen adds survive. |
| `metadata.set` | document ID, key, value, causal context | Register by key. Concurrent different values create a conflict value. |
| `kg.edge_added` | source, relation, target, provenance op | OR-set edge keyed by provenance. |
| `kg.edge_removed` | source, relation, target, observed edge IDs | Removes only observed edges. |
| `vector.cache_advertised` | content hash, chunk ID, model ID, vector root | Cache hint only; never authorizes document presence. |

Deletes use tombstones with retention. Garbage collection cannot remove blocks
reachable from any retained checkpoint or unresolved tombstone horizon.

## Conflict Rules

The first implementation should prefer deterministic, inspectable outcomes over
silent data loss.

| Data | Rule |
|------|------|
| CAS blocks | No conflict. Hash mismatch rejects the block. |
| Chunk manifests | No conflict by hash. Different manifests for the same path create document-version conflict metadata. |
| Paths | Causally ordered updates use the newest op. Concurrent updates preserve both as aliases and surface a conflict. |
| Tags | OR-set semantics. Concurrent add/remove does not drop unseen adds. |
| Scalar metadata | Last causal writer wins only when causally ordered. Concurrent different values are surfaced. |
| Deletes | Tombstone beats causally older updates. Concurrent delete/update is visible until user or policy resolves it. |
| KG edges | OR-set semantics with provenance IDs. |
| Vectors | Rebuild or accept only when `(content_hash, chunk_id, model_id, model_version)` matches. |

This follows the same broad lessons as Bayou-style application conflict handling,
Dynamo-style versioned reconciliation, and CRDT set/register designs: make
causality explicit and do not hide concurrent writes behind a blind timestamp.

## Sync Round

The daemon-to-daemon protocol can be implemented over the existing daemon IPC
transport family later. The shared-object-store mode uses the same messages as
logical operations.

| Step | Message | Purpose |
|------|---------|---------|
| 1 | `hello` | Exchange protocol version, corpus ID, site ID, public key, capabilities. |
| 2 | `heads` | Advertise known site heads and checkpoint hashes. |
| 3 | `want` | Request missing checkpoints, op ranges, manifests, or block hashes. |
| 4 | `have` | Return availability, sizes, checksums, and optional provider locations. |
| 5 | `fetch` | Transfer immutable objects or catalog ops. |
| 6 | `verify` | Hash and signature verification before import. |
| 7 | `apply` | Apply catalog ops through repository APIs inside local transactions. |
| 8 | `ack` | Publish new local head after import and derived index updates are durable. |

Partial failure is safe because objects are immutable and catalog ops are
idempotent by operation ID.

## Shared Object Store Mode

This is the safest first phase for multiple machines or containers.

| Constraint | Required behavior |
|------------|-------------------|
| Single writer | Only one site may update `corpus/head.json`; other sites read and repair. |
| Multi-writer | Each site writes only `heads/<site_id>.json`; readers merge heads locally. |
| Pointer updates | Use provider versioning or compare-and-swap where available. Without CAS, use per-site heads only. |
| Listing | Do not rely on full bucket listing. Fetch from known heads and manifests. |
| Credentials | Prefer read-only credentials for readers and prefix-scoped writer credentials for writers. |
| Fallback | If remote bootstrap falls back local, the daemon must mark sync degraded rather than silently forking the corpus. |

The current R2 benchmark shows non-zero failure rates under multi-client load.
Shared-store sync must therefore treat remote writes as retryable and idempotent,
and it must verify object presence before publishing a head.

## P2P Repair Mode

The existing `RepairStrategy::FromP2P` should become the block-transfer backend
for missing CAS data, not the catalog authority.

Required behavior:

| Requirement | Reason |
|-------------|--------|
| Fetch by hash only | Prevent path traversal and confused-deputy reads. |
| Verify SHA-256 before store | Matches current `RepairManager::storeIfValid` behavior. |
| Attribute repair source | Needed for audit logs and peer health. |
| Refuse unknown corpus IDs | Prevent cross-corpus block injection. |
| Rate-limit and bound block size | Prevent peer resource exhaustion. |

## Derived Indexes

FTS, vector indexes, and KG projections should be reproducible from catalog ops
and source content. The import pipeline should apply state in this order:

1. Verify and store CAS blocks.
2. Import chunk manifests.
3. Apply metadata ops and tombstones.
4. Rebuild or import FTS state.
5. Rebuild or import KG projections.
6. Rebuild or import vector caches only when model identity matches.
7. Publish local head after durable catalog and readiness checks pass.

If vector import fails, document sync can still succeed with `vectorDbReady=false`
or `vectorIndexReady=false`. Search quality degrades, but corpus correctness does
not.

## Security Model

| Control | Requirement |
|---------|-------------|
| Manifest signing | Every op, head, and checkpoint is signed by a trusted writer key. |
| Hash verification | Every block and manifest is verified before storing or applying. |
| Peer authentication | Daemon-to-daemon mode uses mutually authenticated peers. |
| Credential scope | Object-store credentials are project-scoped and prefix-scoped where possible. |
| Secret handling | Credentials never appear in catalog ops, checkpoints, or logs. |
| Encryption | Transport encryption is mandatory for P2P. Object-store encryption follows provider policy plus optional client-side encryption. |
| Revocation | Removing a writer key starts a new epoch and requires a checkpoint signed by an administrator key. |

## Readiness Gates

Sync must be disabled or degraded unless these daemon states are explicit:

| Gate | Required state |
|------|----------------|
| Content store | Initialized with the intended local or remote storage engine. |
| Database | Open, migrated, and accepting transactions. |
| Metadata repository | Ready and bound to the active connection pool. |
| KG store | Ready before applying KG ops; otherwise queue KG ops after metadata import. |
| Vector DB | Ready before importing vector caches; not required for document metadata import. |
| Object backend | Healthy, checksum-capable, and not silently in fallback-local mode for shared-store sync. |
| Repair service | Running only when required storage/catalog dependencies are ready. |

Test coverage is tracked via meson test targets.

## Observability

Expose these metrics before enabling multi-writer sync by default:

| Metric | Meaning |
|--------|---------|
| `sync_known_heads` | Number of peer heads known locally. |
| `sync_lag_seconds` | Wall-clock age of the newest unapplied trusted head. |
| `sync_missing_blocks` | Blocks wanted but not available locally. |
| `sync_conflicts_total` | Unresolved conflict records by type. |
| `sync_repair_bytes_total` | Bytes repaired from object store or peers. |
| `sync_verify_failures_total` | Hash or signature verification failures. |
| `sync_fallback_local_total` | Times configured shared storage fell back to local. |

## Phased Delivery

| Phase | Scope | Acceptance |
|-------|-------|------------|
| 1. Shared object-store safe mode | Single-writer immutable objects, per-site heads, local metadata import | Two daemons can read the same corpus through R2/S3 without sharing SQLite. |
| 2. Snapshot export/import | Signed checkpoint export/import for offline transfer | Import is idempotent and detects missing blocks before catalog mutation. |
| 3. P2P block repair | Wire `RepairStrategy::FromP2P` to authenticated block fetch | Missing block repair succeeds only after hash verification and source attribution. |
| 4. Authenticated daemon sync | `hello/heads/want/have/fetch/apply/ack` between trusted peers | Network partition and reconnect converge without data loss. |
| 5. Multi-writer conflict UX | Conflict records, tombstone retention, selective sync filters | Concurrent path/tag/metadata edits are visible and resolvable. |

## Test Plan

| Category | Cases |
|----------|-------|
| Unit | Manifest hash verification, op ID idempotency, signature rejection, OR-set tag merge, tombstone retention. |
| Integration | Two local daemons sharing object store, import after missing block, remote fallback refusal, vector-cache mismatch rebuild. |
| Fault injection | Truncated object, wrong hash, stale head, concurrent heads, unavailable object store, partial multipart upload. |
| Consistency | Partition peers, perform concurrent edits, reconnect, assert deterministic conflict records. |
| Performance | Sync round bounded by head/checkpoint/object wants, not full bucket list. |

## Research Anchors

PaperBridge searches on 2026-05-24 informed the design constraints:

| Topic | Anchor |
|-------|--------|
| Content-addressed Merkle data | Roy, Mukherjee, and Chaki, "Merkle DAG-based Distributed Data Model for Content-addressed Trust-less Verifiable Data," 2022, DOI `10.1109/ubmk55850.2022.9919573`. |
| Eventual consistency and versioned reconciliation | DeCandia et al., "Dynamo," SOSP 2007, DOI `10.1145/1294261.1294281`. |
| Application-aware conflict resolution | Petersen et al., "Bayou," 1996, DOI `10.1145/504479.504497`. |
| Disconnected operation | Kistler and Satyanarayanan, "Disconnected Operation in the Coda File System," DOI `10.1007/978-0-585-29603-6_19`. |
| CRDT merge structures | Preguica, Baquero, and Shapiro, "Conflict-Free Replicated Data Types CRDTs," 2018, DOI `10.1007/978-3-319-63962-8_185-1`. |
| Untrusted global storage | Kubiatowicz et al., "OceanStore," 2000, DOI `10.1145/356989.357007`. |
| IPFS-style Merkle indexing with CRDTs | Shi et al., "IPFS Keyword Retrieval System Based on Merkle DAG Inverted Index," 2024, DOI `10.62051/ijcsit.v2n2.16`. |
