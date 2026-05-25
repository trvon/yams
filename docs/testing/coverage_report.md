# Test Coverage Report

Test infrastructure, current measured coverage, and the storage-readiness gate
for distributed corpus work.

## Current Coverage (storage subset: src/storage/*.cpp + src/api/content_store_impl.cpp)

| Metric | Overall baseline | Storage-readiness baseline | **Current** |
|--------|:---:|:---:|:---:|
| Lines | 29.9% | 48.1% | **71.8%** |
| Functions | — | — | **78.7%** |
| Branches | 18.8% | 23.4% | **37.3%** |

### Per-file line coverage

| File | Lines | % |
|------|------|--|
| `compressed_storage_engine.cpp` | 389/486 | 80% |
| `s3_signer.cpp` | 187/224 | 83% |
| `storage_backend_factory.cpp` | 202/255 | 79% |
| `reference_counter.cpp` | 346/426 | 81% |
| `reference_db.cpp` | 178/228 | 78% |
| `garbage_collector.cpp` | 139/180 | 77% |
| `object_storage_plugin_loader.cpp` | 106/137 | 77% |
| `object_storage_adapter.cpp` | 169/223 | 75% |
| `url_backend.cpp` | 316/447 | 70% |
| `storage_backend_engine_adapter.cpp` | 88/125 | 70% |
| `storage_engine.cpp` | 353/509 | 69% |
| `content_store_impl.cpp` | 517/740 | 69% |
| `storage_runtime_resolver.cpp` | 315/551 | 57% |
| `posix_storage.cpp` | 0/72 | 0% * |

\* `posix_storage.cpp` is unreferenced dead code (no header declares its functions).

**Status**: All modules compile on Windows, Linux, and macOS.

## Test Infrastructure

| Location | Type | Description |
|----------|------|-------------|
| `tests/unit/` | Unit | Isolated component tests with mocks |
| `tests/integration/` | Integration | Component workflow tests |
| `tests/stress/` | Stress | Load and concurrency tests |
| `tests/benchmarks/` | Benchmarks | Performance measurement |

## Current Coverage Baseline

Measured on 2026-05-24 from existing `build/debug` coverage artifacts.
`build/debug` is configured with `b_coverage=true`; this is a local snapshot,
not a clean CI baseline unless the full suite is re-run immediately before
measurement.

| Surface | Lines | Functions | Branches | Command |
|---------|------:|----------:|---------:|---------|
| Repository excluding `tests/` and `third_party/` | 29.9% `83353/278872` | 33.7% `10460/31015` | 18.8% `79951/426237` | `gcovr -r . build/debug --txt-summary --exclude 'tests/.*' --exclude 'third_party/.*' --merge-mode-functions merge-use-line-min` |
| Storage-readiness surface | 48.1% `28166/58598` | 52.6% `2487/4730` | 23.4% `25081/107110` | `gcovr -r . build/debug --txt-summary --exclude 'tests/.*' --exclude 'third_party/.*' --merge-mode-functions merge-use-line-min --filter 'src/api/.*content_store.*' --filter 'src/storage/.*' --filter 'src/metadata/.*' --filter 'src/vector/.*' --filter 'src/integrity/.*' --filter 'src/daemon/components/.*'` |

`gcovr` needs `--merge-mode-functions merge-use-line-min` for this build
because inline/header functions can appear at different lines across generated
gcov files.

## Running Tests

```bash
# Run all tests
meson test -C builddir --print-errorlogs

# Run specific suite
meson test -C builddir --suite unit
meson test -C builddir --suite integration

# Generate coverage (requires gcovr)
meson configure builddir -Db_coverage=true
meson test -C builddir
gcovr -r . builddir -e tests/ --html-details coverage.html
```

## Coverage Measurement Procedure

To refresh the baseline:

1. Build with coverage enabled
2. Run full test suite
3. Generate gcovr report
4. Update this document with actual metrics

Recommended local commands:

```bash
meson setup build/coverage -Db_coverage=true -Dbuild-tests=true
meson test -C build/coverage --print-errorlogs
gcovr -r . build/coverage --txt-summary --exclude 'tests/.*' --exclude 'third_party/.*' --merge-mode-functions merge-use-line-min
```

## Storage-Readiness Gate

Distributed corpus and shared object-store work must pass a class-level storage
readiness gate. The gate is intentionally not a strict 100% line/branch target.
It requires direct behavioral tests for every class that owns or routes durable
state, plus non-regression against the measured storage-readiness baseline.

### Gate Rules

| Rule | Requirement |
|------|-------------|
| Direct coverage | Every storage-touching class in the matrix below has direct tests, not only incidental integration coverage. |
| Success path | Tests cover successful initialization and at least one successful durable operation. |
| Failure path | Tests cover initialization failure, dependency absence, bad input, or provider error. |
| Crash-ish invariant | Tests assert no process abort, no silent fallback, no partial durable mutation, and typed errors during shutdown/not-ready/corruption paths. |
| Hash/integrity invariant | CAS and repair tests reject wrong hashes and preserve existing valid data. |
| Remote invariant | Object-store paths test retryable failure, missing plugin/provider, bounded listing assumptions, and fallback policy. |
| Readiness propagation | Daemon components expose false/degraded readiness until their dependencies are actually usable. |
| Coverage baseline | Storage-readiness line and branch coverage must not regress from 48.1% lines and 23.4% branches without an explicit note in the change. |

### Storage-Touching Class Matrix

| Class or interface | Source | Existing direct tests | Required additions before distributed-ready |
|--------------------|--------|-----------------------|---------------------------------------------|
| `yams::api::IContentStore`, `ContentStore` | `include/yams/api/content_store.h`, `src/api/content_store_impl.cpp` | `tests/unit/api/content_store_catch2_test.cpp`, `tests/unit/api/raw_text_storage_catch2_test.cpp` | Basic file/stream/bytes operations, metadata, verify, injected not-ready storage propagation, explicit-null storage injection refusal, chunked add rollback for chunks/refs/metadata, file-path chunker failure handling, commit-failure cleanup of partial remote state, and remote ambiguous manifest store reconciliation are covered. Remaining: none. |
| `yams::storage::IStorageEngine`, `StorageEngine`, `AtomicFileWriter` | `include/yams/storage/storage_engine.h`, `src/storage/storage_engine.cpp` | `tests/unit/storage/storage_engine_catch2_test.cpp` | Basic CRUD, manifests, stats, verify, path traversal refusal, invalid keys across all operations, stale temp cleanup/ignore, same-key write collision atomicity, remove/read races, atomic-writer failed-rename temp cleanup, partial atomic write failure cleanup, non-regular-file removal edge handling, object storage listing, and typed remove errors for platform file-status failures are covered. Remaining: platform-specific locked-file removal behavior. |
| `CompressedStorageEngine` | `include/yams/storage/compressed_storage_engine.h`, `src/storage/compressed_storage_engine.cpp` | `tests/unit/storage/compressed_storage_engine_catch2_test.cpp`, `tests/unit/storage/compressed_storage_stats_catch2_test.cpp` | Local and generic-backend compression paths, disabled compression, policy toggles, stats, async/batch, corrupt compressed-payload rejection, incomplete header-shaped raw fallback, storage verification, compressor/decompressor registry absence, queued async shutdown, background compression scan via engine list seam, IStorageEngine list delegation, and deep scan validation of compressed output are covered. Remaining: none. |
| `IStorageBackend`, `FilesystemBackend`, `URLBackend` | `include/yams/storage/storage_backend.h`, `src/storage/url_backend.cpp` | `tests/unit/storage/storage_backend_catch2_test.cpp` | Filesystem CRUD, stats, async, key handling, concurrency, direct URL initialization failure, HTTP status taxonomy, retryable HTTP failures, non-retryable missing objects, request timeout errors, HTTP plain-text newline listing with prefix, and authenticated provider authorization header are covered. Credential-free initialization log output verified. Remaining: S3-object-listing protocol integration. |
| `StorageBackendFactory` | `include/yams/storage/storage_backend.h`, `src/storage/storage_backend_factory.cpp` | `tests/unit/storage/storage_backend_factory_catch2_test.cpp` | Local/remote/custom success, aliases, unknown type, missing explicit plugin, registered backend init failure, URL parsing, and invalid numeric params are covered. Credential-free log output confirmed. Remaining: plugin-backed S3 bootstrap with provider health failure. |
| `StorageBackendEngineAdapter` | `include/yams/storage/storage_backend_engine_adapter.h`, `src/storage/storage_backend_engine_adapter.cpp` | `tests/unit/storage/storage_backend_engine_adapter_catch2_test.cpp` | Null backend, failed store/retrieve/remove accounting, `exists()` exception conversion, local size list/retrieve failures, remote size-from-stats/list avoidance, and IStorageEngine list delegation are covered. Remaining: none (remote stats freshness is inherently asynchronous and intentionally uses cached stats for getStorageSize). |
| `IStorageBackendExtended` | `include/yams/storage/storage_backend_extended.h` | `tests/plugins/s3/s3_plugin_smoke_test.cpp` | Multipart store/retrieve readiness, checksum negotiation, and partial upload abort are covered by the plugin smoke suite. Remaining: none (all paths require a live S3 backend). |
| `IReferenceCounter`, `ReferenceCounter`, `ReferenceCounter::Transaction` | `include/yams/storage/reference_counter.h`, `src/storage/reference_counter.cpp` | `tests/unit/storage/reference_counter_catch2_test.cpp`, `tests/unit/storage/reference_counter_stress_catch2_test.cpp` | Direct commit/rollback stats, queued rollback invariants, failed commit rollback/no partial writes, corrupt restore, corrupt materialized stats reconciliation, invalid DB path, and concurrent ref updates are covered. Remaining: low-level SQLite handle failure during maintenance and true crash-recovery replay. |
| `GarbageCollector` | `include/yams/storage/reference_counter.h`, `src/storage/garbage_collector.cpp` | `tests/unit/storage/reference_counter_catch2_test.cpp` | Basic collection, dry run preservation/progress, async collection, concurrent collection guard, last-stats, scheduler stop, tombstone horizon, mixed missing-block/delete-failure preservation, and durable reference-row pruning via `ITransaction::pruneReference` are covered. Remaining: none. |
| `StorageBootstrapDecision`, `resolveStorageBootstrapDecision` | `include/yams/storage/storage_runtime_resolver.h`, `src/storage/storage_runtime_resolver.cpp` | `tests/unit/storage/storage_runtime_resolver_catch2_test.cpp` | Local success, strict S3 failure, explicit fallback, flattened keys, R2 preflight validation, and no silent strict-mode local fallback are covered. Remaining: successful plugin-backed S3 bootstrap in an integration environment. |
| `metadata::Database`, `Statement`, `CachedStatement` | `include/yams/metadata/database.h`, `src/metadata/database.cpp` | `tests/unit/metadata/database_catch2_test.cpp`, `tests/unit/metadata/metadata_schema_catch2_test.cpp` | Open/close/wal, create table, prepared statements, transactions, FTS5, query builder, migrations, concurrent access, statement cache hit/miss/release/clear, post-close operation rejection, open-nonexistent without Create failure, and cache clear on close are covered. Remaining: none. |
| `ConnectionPool`, `PooledConnection`, `ScopedConnection` | `include/yams/metadata/connection_pool.h`, `src/metadata/connection_pool.cpp` | `tests/unit/metadata/connection_pool_catch2_test.cpp` | Pool exhaustion timeout, shutdown while connection leased, read-only readiness via `sqlite3_db_readonly`, idle connection pruning, holder duration histogram and slow-holder detection, source-location tagging, and waiting-counter reset on timeout are covered. Remaining: none. |
| `IMetadataRepository`, `MetadataRepository` | `include/yams/metadata/metadata_repository.h`, `src/metadata/metadata_repository.cpp` | `tests/unit/metadata/metadata_repository_catch2_test.cpp`, `tests/unit/metadata/metadata_repository_cache_catch2_test.cpp` | Insert/get/update/delete documents, dual-pool read/write routing, session/embedding-hash read route isolation, metadata value counts, reconcile embedding status, vector hash ownership cleanup, semantic duplicate groups, FTS query/lookup, and tombstone-aware path metadata are covered. Remaining: none. |
| `KnowledgeGraphStore`, `KnowledgeGraphStore::WriteBatch` | `include/yams/metadata/knowledge_graph_store.h`, `src/metadata/knowledge_graph_store_sqlite.cpp` | `tests/unit/metadata/kg_store_catch2_test.cpp`, `tests/unit/metadata/kg_store_alias_and_entities_catch2_test.cpp` | Batch upsert idempotency, node-key lookup, addEdgesUnique dedup, semantic edge upsert with strongest-score retention, ensurePathNode/DocumentNode idempotency, topology analysis, and conflict provenance via edge semantics are covered. Remaining: batch rollback. |
| `MigrationManager`, `YamsMetadataMigrations` | `include/yams/metadata/migration.h`, `src/metadata/migration.cpp` | `tests/unit/metadata/metadata_schema_catch2_test.cpp` | Schema-version hash verification (v16, v21, v31), all built-in migrations applied, CRUD + FTS + stats + cascade post-migration schema integrity are covered. Remaining: failed-migration readiness-false and rollback verification. |
| `TreeBuilder` | `include/yams/metadata/tree_builder.h`, `src/metadata/tree_builder.cpp` | `tests/unit/metadata/tree_builder_catch2_test.cpp` | TreeNode CRUD, sort/hash/cache/serialize, TreeBuilder empty/single/multi entries, empty/nested subdirectories, exclude patterns + glob wildcards, deduplication of identical content, nonexistent directory/file errors, getTree/hasTree for missing blocks, and snapshot root stability are covered. Remaining: none. |
| `VectorDatabase` | `include/yams/vector/vector_database.h`, `src/vector/vector_database.cpp` | `tests/unit/vector/vector_smoke_catch2_test.cpp`, `tests/unit/vector/vector_dimension_validation_catch2_test.cpp` | Init, insert/search across vec0/sqlite-vec/Simeon engines, vector count, dimension validation, operations before initialize rejected, operations after close rejected are covered. Remaining: vector cache mismatch by model ID/content hash. |
| `IVectorBackend`, `SqliteVecBackend` | `include/yams/vector/vector_backend.h`, `include/yams/vector/sqlite_vec_backend.h`, `src/vector/sqlite_vec_backend.cpp` | `tests/unit/vector/sqlite_vec_backend_comprehensive_catch2_test.cpp`, `tests/unit/vector/sqlite_vec_backend_persistence_catch2_test.cpp` | In-memory and file-DB init, createTables/tablesExist, insertVector, insertVectorsBatch, getVector, getVectorsBatch, update/deleteVector, deleteVectorsByDocument, hasEmbedding, searchSimilar, schema failure readiness, WAL/checkpoint persistence, and close+reopen survival are covered. Remaining: none. |
| `FaissBackend` | `include/yams/vector/faiss_backend.h`, `src/vector/faiss_backend.cpp` | Not yet implemented | This class does not exist yet in the codebase — it is a future integration target. |
| `VectorSchemaMigration` | `include/yams/vector/vector_schema_migration.h`, `src/vector/vector_schema_migration.cpp` | `tests/unit/vector/vector_schema_migration_vec0_catch2_test.cpp` | Migrates readable vec0 V1 data into V2, preserves legacy tables when vec0 embeddings are unreadable, blocks vector ready state on failed migration. Remaining: none. |
| `EmbeddingLifecycleManager` | `include/yams/vector/embedding_lifecycle.h` | `tests/unit/daemon/embedding_lifecycle_manager_catch2_test.cpp` | Initial state, FSM transitions (Unavailable→Available→Degraded), isDegraded detection, model/plugin name setters, auto-on-add/preload flags, null-service metrics, ensureModelReadySync failure, resetWarmupState, resolvePreferredModel, detectPreloadFlag are covered. Remaining: none. |
| `RepairManager` | `include/yams/integrity/repair_manager.h`, `src/integrity/repair_manager.cpp` | `tests/unit/integrity/repair_manager_test.cpp` | Basic repair attempt, canRepair availability check, backup fetcher success flow, repair strategy ordering (backup→P2P), P2P wrong-hash rejection, and source attribution via configured strategy order are covered. Remaining: none. |
| `Verifier`, `ChunkValidator` | `include/yams/integrity/verifier.h`, `include/yams/integrity/chunk_validator.h` | `tests/unit/integrity/integrity_verifier_catch2_test.cpp`, `tests/unit/integrity/chunk_validator_test.cpp` | Valid/missing/corrupted block detection, multiple block verification, background lifecycle, pause/resume, stats and reporting, recent failures, configuration update, callbacks, scheduling strategy, verify-all, empty/invalid hash rejection, move semantics, truncated chunk detection, wrong manifest rejection, and remote read failure classification are covered. Remaining: none. |
| `ServiceManager` | `include/yams/daemon/components/ServiceManager.h`, `src/daemon/components/ServiceManager.cpp` | `tests/unit/daemon/service_manager_catch2_test.cpp`, `tests/unit/daemon/service_manager_shutdown_ownership_catch2_test.cpp`, `tests/unit/daemon/service_manager_provider_degraded_catch2_test.cpp`, `tests/unit/daemon/storage_preflight_test.cpp` | Construction, config accessors, PostIngestQueue, search engine caching, shutdown ownership and FSM stop, degraded-when-no-provider, storage preflight (healthy/missing/read-only/slow/empty paths) are covered. Remaining: none. |
| `DatabaseManager` | `include/yams/daemon/components/DatabaseManager.h`, `src/daemon/components/DatabaseManager.cpp` | `tests/unit/daemon/components/database_manager_test.cpp` | Pool init, metadata/content store initialization, and readiness configuration are covered. Remaining: repo unavailable and content store reset during shutdown. |
| `VectorSystemManager`, `VectorIndexCoordinator` | `include/yams/daemon/components/VectorSystemManager.h`, `include/yams/daemon/components/VectorIndexCoordinator.h` | `tests/unit/daemon/components/vector_system_manager_test.cpp`, `tests/unit/daemon/vector_index_coordinator_*_catch2_test.cpp` | Separate vectorDbReady and vectorIndexReady, coalesce/telemetry/bulk-scope operations for index coordinator are covered. Remaining: rebuild/import failure handling. |
| `RepairService`, `RepairServiceHost` | `include/yams/daemon/components/RepairService.h`, `include/yams/daemon/components/RepairServiceHost.h` | `tests/unit/daemon/repair_service_post_ingest_channel_catch2_test.cpp`, `tests/unit/daemon/request_handler_repair_cancel_catch2_test.cpp` | Post-ingest channel routing, deferred-when-dependencies-not-ready, and cancellation during storage read are covered. Remaining: none. |
| `PostIngestQueue` | `include/yams/daemon/components/PostIngestQueue.h` | `tests/unit/daemon/components/post_ingest_queue_unit_catch2_test.cpp`, `tests/unit/daemon/post_ingest_dynamic_cap_recovery_catch2_test.cpp` | Queue enqueue/dequeue, dynamic cap recovery, and dependency-flip readiness are covered via integration suites. Remaining: none. |
| `WriteCoordinator` | `include/yams/daemon/components/WriteCoordinator.h` | `tests/benchmarks/write_coordinator_bench.cpp` | Batch write throughput bench exercises write paths; indirect coverage via daemon daemon suites. Remaining: batch rollback, KG unavailable, shutdown while writes in flight. |
| `PluginManager` | `include/yams/daemon/components/PluginManager.h` | `tests/unit/daemon/components/plugin_manager_test.cpp`, `tests/unit/daemon/plugin_manager_config_test.cpp` | Plugin scanning, loading, configuration, and trust policy are covered. Remaining: unload during backend use. |
| `IObjectStorageBackend`, `yams_object_storage_v1`, adapter bridge | `include/yams/plugins/object_storage_iface.hpp`, `include/yams/plugins/object_storage_v1.h`, `src/storage/object_storage_adapter.cpp` | `tests/unit/storage/object_storage_adapter_catch2_test.cpp` | Adapter creation, method dispatch, null method table, provider health failure, and checksum mismatch are covered. Remaining: none. |
| `PluginBackendProxy`, plugin loader helpers | `src/storage/object_storage_plugin_loader.cpp` | `tests/unit/storage/storage_backend_factory_catch2_test.cpp` | Missing explicit plugin name returns nullptr, unknown plugin returns nullptr, empty name returns nullptr, `plugin:` prefix dispatch returns nullptr for unknown names, factory create call after registered type, and S3 plugin resolution via `tryCreateS3PluginBackend` are covered. Remaining: missing-exported-symbols and unload lifetime (require live plugins via `tests/plugins/s3/s3_plugin_smoke_test.cpp`). |
| `S3Backend` | `plugins/object_storage_s3/s3_plugin.cpp` | `tests/plugins/s3/s3_plugin_smoke_test.cpp`, `tests/unit/storage/s3_signer_catch2_test.cpp` | S3 signer (header canonicalization, signature chaining, chunked upload signing), S3 plugin smoke (upload/download/list). Remaining: MinIO multipart, induced 429/5xx retry, bounded pagination, checksum mismatch (all require live S3/MinIO). |

### Distributed Corpus Acceptance Tests

Before enabling distributed corpus sync by default, add tests for these scenarios:

| Scenario | Acceptance |
|----------|------------|
| Shared object store, single writer | Reader imports checkpoint and retrieves documents without sharing SQLite. |
| Shared object store, fallback local | Daemon reports degraded sync and does not publish a divergent head. |
| P2P block repair | Missing block is fetched by hash, verified, stored, and attributed to source peer. |
| Wrong block from peer | Hash mismatch is rejected and existing valid data is untouched. |
| Concurrent path edits | Both edits converge to a deterministic conflict record. |
| Tag add/remove race | OR-set semantics preserve unseen concurrent adds. |
| Vector model mismatch | Metadata import succeeds and vector cache is rebuilt or skipped. |
| Object-store list gap | Sync succeeds from heads/manifests without full bucket listing. |

## Known Limitations

- **Windows AF_UNIX**: Some integration tests skip due to socket limitations
- **Plugins**: Plugin tests require specific binaries to be built
- **Coverage snapshot**: Current numbers come from local `build/debug` artifacts, not a clean CI run.
- **Storage-readiness gaps**: Several storage-touching classes still have only indirect tests.

## Related Documentation

- `tests/README.md` - Test directory structure
- `tests/CATCH2_PATTERNS.md` - Test writing guidelines
- `docs/design/sync-protocol.md` - Distributed corpus/P2P sync design and readiness dependencies
