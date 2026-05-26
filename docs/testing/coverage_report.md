# Test Coverage Report

Storage-readiness coverage gate for distributed corpus work.

## Broad-Surface Coverage (106 test executables, 2026-05-25)

| Metric | Original baseline | Previous (79 tests) | **Current (106 tests)** |
|--------|:---:|:---:|:---:|
| Lines (broad) | 48.1% | 43.7% | **59.8%** |
| Functions (broad) | — | 50.3% | **68.3%** |
| Branches (broad) | 23.4% | 19.9% | **29.7%** |
| Lines (narrow) | — | 73.2% | **74.3%** |
| Functions (narrow) | — | 79.8% | **80.2%** |
| Branches (narrow) | — | 37.7% | **38.6%** |
| Total test cases | — | — | **8,346** |

### Per-Subsystem Breakdown

| Subsystem | Lines | Functions | Branches |
|-----------|------|-----------|----------|
| `src/storage/` | **74.0%** | 81.3% | 39.0% |
| `src/api/` (content_store) | **62.5%** | 24.0% | 33.1% |
| `src/metadata/` | **64.2%** | 75.3% | 32.5% |
| `src/integrity/` | **55.8%** | 80.0% | 28.1% |
| `src/vector/` | **50.3%** | 62.9% | 31.0% |
| `src/daemon/components/` | **43.9%** | 58.3% | 21.8% |

### Narrow-Surface Per-File (storage + content_store)

| File | Lines | % |
|------|------|--|
| `s3_signer.cpp` | 187/224 | 83% |
| `reference_counter.cpp` | 346/426 | 81% |
| `compressed_storage_engine.cpp` | 389/486 | 80% |
| `storage_backend_factory.cpp` | 202/255 | 79% |
| `reference_db.cpp` | 178/228 | 78% |
| `garbage_collector.cpp` | 139/180 | 77% |
| `object_storage_plugin_loader.cpp` | 106/137 | 77% |
| `object_storage_adapter.cpp` | 169/223 | 75% |
| `url_backend.cpp` | 316/447 | 70% |
| `storage_backend_engine_adapter.cpp` | 88/125 | 70% |
| `storage_engine.cpp` | 353/509 | 69% |
| `content_store_impl.cpp` | 517/740 | 69% |
| `storage_runtime_resolver.cpp` | 315/551 | 57% |
| `posix_storage.cpp` | 0/72 | 0% † |

† Unreferenced dead code (no header declarations).

## Storage-Readiness Gate

| Rule | Requirement |
|------|-------------|
| Direct coverage | Every storage-touching class has direct tests, not incidental integration coverage. |
| Success path | Tests cover init and at least one successful durable operation. |
| Failure path | Tests cover init failure, dependency absence, bad input, or provider error. |
| Crash-ish invariant | Tests assert no abort, no silent fallback, no partial durable mutation, typed errors. |
| Hash/integrity invariant | CAS and repair tests reject wrong hashes, preserve valid data. |
| Remote invariant | Object-store paths test retryable failure, missing plugin, bounded listing. |
| Readiness propagation | Daemon components expose false/degraded readiness until dependencies usable. |
| Coverage baseline | Narrow-surface must not regress from 74.3% lines / 38.6% branches. |

## Class Matrix

| Class / interface | Existing tests | Remaining |
|-------------------|----------------|-----------|
| `IContentStore`, `ContentStore` | `content_store_catch2_test.cpp` | none |
| `IStorageEngine`, `StorageEngine`, `AtomicFileWriter` | `storage_engine_catch2_test.cpp` | locked-file removal (platform-specific) |
| `CompressedStorageEngine` | `compressed_storage_engine_catch2_test.cpp` | none |
| `IStorageBackend`, `FilesystemBackend`, `URLBackend` | `storage_backend_catch2_test.cpp` | S3-object-listing integration |
| `StorageBackendFactory` | `storage_backend_factory_catch2_test.cpp` | plugin-backed S3 bootstrap health |
| `StorageBackendEngineAdapter` | `storage_backend_engine_adapter_catch2_test.cpp` | none (remote stats intentionally cached) |
| `IStorageBackendExtended` | `s3_plugin_smoke_test.cpp` | none (all paths require live S3) |
| `IReferenceCounter`, `ReferenceCounter` | `reference_counter_catch2_test.cpp` | SQLite handle failure + crash-replay |
| `GarbageCollector` | `reference_counter_catch2_test.cpp` | none |
| `StorageBootstrapDecision` | `storage_runtime_resolver_catch2_test.cpp` | plugin-backed S3 bootstrap |
| `Database`, `Statement`, `CachedStatement` | `database_catch2_test.cpp` | none |
| `ConnectionPool`, `PooledConnection` | `connection_pool_catch2_test.cpp` | none |
| `IMetadataRepository`, `MetadataRepository` | `metadata_repository_catch2_test.cpp` | none |
| `KnowledgeGraphStore`, `WriteBatch` | `kg_store_catch2_test.cpp` | batch rollback |
| `MigrationManager` | `database_catch2_test.cpp`, `metadata_schema_catch2_test.cpp` | none |
| `TreeBuilder` | `tree_builder_catch2_test.cpp` | none |
| `VectorDatabase` | `vector_smoke_catch2_test.cpp`, `vector_dimension_validation_catch2_test.cpp` | vector cache mismatch (model version) |
| `IVectorBackend`, `SqliteVecBackend` | `sqlite_vec_backend_comprehensive_catch2_test.cpp` | none |
| `FaissBackend` | — | not yet implemented |
| `VectorSchemaMigration` | `vector_schema_migration_vec0_catch2_test.cpp` | none |
| `EmbeddingLifecycleManager` | `embedding_lifecycle_manager_catch2_test.cpp` | none |
| `RepairManager` | `repair_manager_test.cpp` | none |
| `Verifier`, `ChunkValidator` | `integrity_verifier_catch2_test.cpp`, `chunk_validator_test.cpp` | none |
| `ServiceManager` | `service_manager_catch2_test.cpp`, `storage_preflight_test.cpp` | none |
| `DatabaseManager` | `database_manager_test.cpp` | repo unavailable + shutdown reset |
| `VectorSystemManager`, `VectorIndexCoordinator` | `vector_system_manager_test.cpp` | rebuild/import failure |
| `RepairService`, `RepairServiceHost` | `repair_service_post_ingest_channel_catch2_test.cpp` | none |
| `PostIngestQueue` | `post_ingest_queue_unit_catch2_test.cpp` | none |
| `WriteCoordinator` | bench only | batch rollback, KG unavailable, shutdown in flight |
| `PluginManager` | `plugin_manager_test.cpp` | unload during backend use |
| `IObjectStorageBackend`, adapter bridge | `object_storage_adapter_catch2_test.cpp` | none |
| `PluginBackendProxy`, loader helpers | `storage_backend_factory_catch2_test.cpp` | symbols + unload (needs live plugin) |
| `S3Backend` | `s3_plugin_smoke_test.cpp`, `s3_signer_catch2_test.cpp` | multipart, 429/5xx retry (needs S3/MinIO) |

### Remaining Gap Summary

| Blocker | Rows affected |
|---------|:---:|
| **S3/MinIO infrastructure** | 5 (URLBackend, StorageBackendFactory, StorageBootstrapDecision, S3Backend, IStorageBackendExtended) |
| **Crash-sim** | 1 (IReferenceCounter) |
| **Live plugin build** | 1 (PluginBackendProxy) |
| **Daemon harness** | 4 (DatabaseManager, VectorSystemManager, WriteCoordinator, KnowledgeGraphStore) |
| **Platform-specific** | 1 (StorageEngine locked-file on Windows) |
| **Model versioning** | 1 (VectorDatabase cache mismatch) |
| **Not yet implemented** | 1 (FaissBackend) |

## Distributed Corpus Acceptance Tests (pre-sync gate)

| Scenario | Acceptance |
|----------|------------|
| Shared object store, single writer | Reader imports checkpoint and retrieves documents without sharing SQLite |
| Fallback local | Daemon reports degraded sync, no divergent head |
| P2P block repair | Missing block fetched by hash, verified, stored, attributed to peer |
| Wrong block from peer | Hash mismatch rejected, existing data untouched |
| Concurrent path edits | Converge to deterministic conflict record |
| Tag add/remove race | OR-set semantics preserve unseen concurrent adds |
| Vector model mismatch | Metadata import succeeds, vector cache rebuilt or skipped |
| Object-store list gap | Sync succeeds from heads/manifests without full bucket listing |
