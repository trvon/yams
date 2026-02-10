# Changelog

All notable changes to YAMS (Yet Another Memory System) will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

- [SourceHut](https://sr.ht/~trvon/yams/): https://sr.ht/~trvon/yams/

## Archived Changelogs
- v0.7.x archive: docs/changelogs/v0.7.md
- v0.6.x archive: docs/changelogs/v0.6.md
- v0.5.x archive: docs/changelogs/v0.5.md
- v0.4.x archive: docs/changelogs/v0.4.md
- v0.3.x archive: docs/changelogs/v0.3.md
- v0.2.x archive: docs/changelogs/v0.2.md
- v0.1.x archive: docs/changelogs/v0.1.md

## [v0.8.3] - Unreleased

### Fixed
- **Stuck-doc recovery enqueue**: Fixed repair stuck-document recovery enqueueing `PostIngestTask`s to an unused InternalEventBus channel. Recovery now enqueues to `post_ingest` (consumed by PostInestQueue), so re-extraction actually runs.
- **Doctor FTS5 reindex SQLite TOOBIG**: Added best-effort truncation retry for oversized extracted text and report `truncated=<n>` in output.

- **ONNX dynamic tensor padding**: Pads input tensors to the aligned actual token length instead of the model's `max_seq_len` (512). Short texts now skip hundreds of zero-padded positions, yielding **~70-85x throughput improvement** for short inputs (93 texts/sec vs 1.6 texts/sec on CPU with `nomic-embed-text-v1.5`). Enabled by default; disable with `YAMS_ONNX_DYNAMIC_PADDING=0`. Sequence lengths are 8-byte aligned to balance SIMD efficiency with padding reduction.
- **ROCm / MIGraphX compiled-model caching**: Enable save/load of compiled MIGraphX artifacts to avoid paying multi-minute `compile_program` costs when sessions are recreated (e.g., after scale-down/eviction). Defaults to caching hashed `*.mxr` artifacts under the model directory. Configure with `YAMS_MIGRAPHX_COMPILED_PATH` (directory; if a `.mxr` path is provided, its parent directory is used), `YAMS_MIGRAPHX_SAVE_COMPILED`, and `YAMS_MIGRAPHX_LOAD_COMPILED`.
- **ONNX batch warmup and auto-tuning**: `warmupModel()` runs a 4-text batch embedding immediately after model load, pre-warming the ONNX Runtime session and measuring throughput. When `YAMS_EMBED_DOC_CAP` is unset, an auto-tuning sweep (batch sizes 4→8→16→32→64) selects the batch size with peak texts/sec as the default cap via `TuneAdvisor::setEmbedDocCap()`. Disable auto-tuning with `YAMS_EMBED_AUTOTUNE=0`.
- **ONNX session reuse**: Pool-managed sessions now show **25x latency reduction** on warm cycles (259ms cold → 10ms warm), confirming session creation cost is amortized after the first inference.
- **Hardware-adaptive pool sizing**: `TuneAdvisor::onnxSessionsPerModel()` dynamically sizes the session pool based on available CPU cores and GPU state, preventing over-subscription on constrained hardware.

### Diagnostics
- **ONNX ROCm diagnostic output**: GPU diagnostic now reports cold vs warm embedding timing to distinguish first-run compilation from steady-state inference.

#### ONNX Embedding Benchmarks (CPU-only, macOS M3 Max, `nomic-embed-text-v1.5` 768-dim)

| Benchmark | Metric | Result |
|-----------|--------|--------|
| Batch size sweep (64 texts) | Peak throughput | 55.9 texts/sec @ batch=16 |
| Dynamic padding ON vs OFF (short texts) | Speedup | ~70-85x (111 vs 1.5 texts/sec) |
| Session reuse (5 cycles) | Cold vs warm | 259ms → 10ms (25.9x) |
| Concurrent inference (4 threads) | Throughput | 83.7 texts/sec |

## [v0.8.2] - February 2, 2026

### Fixed
- MCP stdio: Improved OpenCode compatibility during handshake/tool discovery (initialize capabilities schema, strict JSON-RPC batch responses, more robust NDJSON parsing).
- MCP tools: Fixed `mcp.echo` tool `inputSchema` shape so tool schemas validate correctly.
- **FTS5 orphan detection**: Fixed bug where orphan scan used synthetic hashes (e.g., `orphan_id_12345`) that never matched actual documents. Orphans were detected but never removed because the removal query used non-existent hashes. Now passes document rowids directly via `Fts5Job.ids` and calls `removeFromIndex(docId)` which deletes by FTS5 rowid.

### Added
- **MCP grep tag filtering**: `grep` tool now accepts `tags` and `match_all_tags` parameters, aligning MCP grep with CLI `yams grep --tags` capabilities.
- **Blackboard search tools**: Three new OpenCode blackboard tools for discovering content:
  - `bb_search_tasks` — semantic search for tasks (mirrors `bb_search_findings`).
  - `bb_search` — unified cross-entity semantic search returning both findings and tasks.
  - `bb_grep` — regex/pattern search across all blackboard content with optional entity filtering.
- MCP stdio debug/compat toggles: `YAMS_MCP_HANDSHAKE_TRACE` (trace handshake events) and `YAMS_MCP_MINIMAL_TOOLS` (expose only `mcp.echo` for client compatibility debugging).

### Performance
- **Stdin routed through daemon**: `yams add -` (stdin) now sends content directly to the daemon via inline content support instead of falling back to local service initialization (~40s startup penalty eliminated). Blackboard plugin also drops unnecessary `--sync` flag for further latency reduction.
- **Adaptive sync polling**: `--sync` extraction polling now uses exponential backoff (5ms → 100ms) instead of fixed 100ms intervals. Small documents that extract via fast-track are detected on the first poll (~5ms) instead of waiting a full 100ms cycle.
- **Unified async add pipeline with parallel batching**: `yams add` with multiple files now processes up to 4 files concurrently via `addBatch()` instead of sequentially. Single shared `DaemonClient` is reused across all add operations (CLI and MCP), eliminating per-file client construction overhead.
- **`addViaDaemonAsync` coroutine**: New async entry point for all add operations. Replaces promise/future-per-attempt pattern with direct `co_await`, reducing overhead. MCP `handleStoreDocument`, `handleAddDirectory`, and download post-index all route through this single path.
- **Batch FTS5 orphan removal**: New `removeFromIndexByHashBatch()` wraps all per-hash SELECT+DELETE operations in a single transaction with cached prepared statements. Replaces N individual autocommit transactions with 1 transaction for N hashes. Eliminates prolonged DB lock contention during orphan scans (~29k orphans previously caused ~58k individual transactions), which blocked CLI requests (`yams stats`, `yams list`) and caused timeouts/segfaults.
- **`yams update` skip CLI-side name resolution for daemon path**: `yams update --name` no longer resolves names to hashes CLI-side before sending to the daemon. Eliminates ~40s `ensureStorageInitialized()` penalty and 1-4 redundant daemon round-trips. Name resolution now only occurs in the local fallback path.


## [v0.8.1] - January 31, 2026

### Added
- `yams list --metadata-values` for showing unique metadata values with counts (useful for PBI discovery).
- **Post-ingest file/directory tracking**: New metrics for tracking files and directories added/processed through the ingestion pipeline (`filesAdded`, `directoriesAdded`, `filesProcessed`, `directoriesProcessed`).
- **OpenCode Blackboard Plugin** (`external/opencode-blackboard/`): Multi-agent blackboard architecture plugin for OpenCode using YAMS as shared memory. Enables agent-to-agent communication through structured findings, task coordination, and context grouping. Requires YAMS v0.8.1+.
- **Per-stage queue depth exposure**: Real-time queue depth metrics for KG, symbol, entity, and title extraction stages accessible via daemon status.
- **Progress bars in CLI**: Visual progress bars for queue utilization, worker pool, memory pressure, and pipeline stages in `yams daemon status` and `yams status` commands.
- **Unified status UI**: `yams status` daemon-connected display now uses consistent section headers, row rendering, and status indicators matching `yams daemon status`.
- Unique PBI selection guidance in AGENTS workflow (metadata search + list values).
- **Data-dir single-instance enforcement**: Prevents multiple daemons from sharing the same data directory via flock-based `.yams-lock` file. Newer daemon requests shutdown of existing daemon and takes over, enabling seamless upgrades/restarts.

### Performance
- **Reduced status tick interval**: Governor metrics now update every 50ms (was 250ms) for more responsive CLI status output. Configurable via `YAMS_STATUS_TICK_MS`.
- **Batch snapshot info API**: New `batchGetSnapshotInfo()` method eliminates N+1 query pattern in `yams list --snapshots`. Reduces 3N queries to 1 query for N unknown snapshots.
- **Enumeration query caching**: `getSnapshots()`, `getSnapshotLabels()`, `getCollections()`, and `getAllTags()` now use a 60-second TTL cache with signal-based invalidation. Repeated calls return cached results, reducing database scans.
- **CPU-aware throttling**: ResourceGovernor now monitors CPU usage alongside memory pressure. Admission control rejects new work when CPU exceeds threshold (default 70%, configurable via `YAMS_CPU_HIGH_PCT`). Prevents CPU saturation during large batch adds.

### Fixed
- Post-ingest tuning reconciles per-stage concurrency targets to the overall budget.
- Post-ingest stage throttling now respects pause states and stage availability when computing TuneAdvisor budgets.
- Post-ingest pollers back off when a stage is paused or has a zero concurrency cap to avoid runaway CPU.
- Added a post-ingest stage snapshot log (active/paused/limits) at startup for easier tuning verification.
- Grep integration tests create the ingest directory before daemon startup to avoid missing queue paths.
- Post-ingest jobs reuse content bytes for KG/symbol/entity stages to avoid repeated content loads.
- Post-ingest KG stage no longer triggers duplicate symbol extraction when the symbol pipeline is active.
- External entity extraction reuses a single base64 payload per document across batches.
- CLI snippet formatting is now shared between search and grep for consistent output.
- `yams list` now uses the shared snippet formatter for previews.
- `yams grep` honors `--ext` filters, accepts `--cwd` with an optional path, and treats `**/*` patterns as matching direct children.
- `yams list --metadata-values` now aggregates counts in the database and respects list filters, avoiding large client-side scans.
- Added metadata aggregation indexes to speed up key/value count queries.

### Documentation
- Updated YAMS skill guide with unique PBI discovery and tagged search examples.

## [v0.8.0] - January 24, 2026

### Breaking
- **Vector database migration required**: sqlite-vec-cpp HNSW rewrite invalidates existing vector indices. After upgrading, run:
  ```bash
  yams doctor repair --embeddings   # Regenerate all embeddings
  ```
  Without this, search will fall back to FTS5-only (no semantic search).
- sqlite-vec-cpp submodule: HNSW API changes and third-party library removal (soft deletion, multi-threading, fp16 quantization, incremental persistence, pre-filtering).

### Added
- MCP `graph` tool for knowledge graph queries (parity with CLI `yams graph`).
- Graph: snapshot-scoped version nodes, `contains` edges for file→symbol, `--dead-code-report`.
- Graph prune policy (`daemon.graph_prune`) to keep latest snapshot versions.
- Download CLI: progress streaming (human/json) via DownloadService callbacks.
- Symbol-aware search ranking: definitions rank higher than usages (`YAMS_SYMBOL_WEIGHT`).
- Zig language support: functions, structs, enums, unions, fields, imports, calls.
- ColBERT MaxSim reranking when the preferred model is a ColBERT variant.
- Added support for the [mxbai-edge-colbert-v0-17m](https://huggingface.co/mixedbread-ai/mxbai-edge-colbert-v0-17m) model (embedding + MaxSim reranking, max-pooled and L2-normalized embeddings).
- Vector DB auto-rebuild on embedding dimension mismatch (`daemon.auto_rebuild_on_dim_mismatch`).
- Init now prompts for a tuning profile (efficient/balanced/aggressive) and writes `tuning.profile`.
- Search config supports a dedicated reranker model (`search.reranker_model`) with CLI helpers (`yams config search reranker`).
- **WEIGHTED_MAX fusion strategy**: Takes maximum weighted score per document instead of sum.
  Prevents "hub" documents from dominating via multi-component consensus boost. Used by
  SCIENTIFIC tuning profile for benchmark corpora.

### Performance

#### IPC & Daemon
- IPC latency reduced from ~8-28ms to ~2-5ms (connection pooling, async timers, cached config).
- Daemon startup throttling: PathTreeRepair via RepairCoordinator, Fts5Job startup delay (2s), reduced batch sizes (1000→100).

#### Ingestion & Storage
- Post-ingest throughput: dedicated worker pool, adaptive backoff, batched directory ingests.
- In-memory chunking for `storeBytes()` - avoids temp file I/O for large documents.

#### Database & Metadata
- **KGWriteQueue**: Batched, serialized writes to KnowledgeGraphStore via async writer coroutine.
  Eliminates "database is locked" errors during high-throughput ingestion by queueing KG operations
  (nodes, edges, aliases, doc entities) and committing in batches. Both symbol extraction and NL
  entity extraction now use deferred batching with nodeKey→nodeId resolution at commit time.
- Prepared statement caching for SQLite queries - reduces SQL compilation overhead on repeated operations. Cached methods: `setMetadata`, `setMetadataBatch`, `getMetadata`, `getAllMetadata`, `getContent`, `getDocument`, `getDocumentByHash`, `updateDocument`, `deleteDocument`, `insertContent`.
- `setMetadataBatch()` API for bulk metadata updates - 4x faster than individual calls.

#### Search & Retrieval
- Batch vector/KG lookups, flat_map for cache-friendly access, branch hints, memory pre-allocation.
- Concept boost post-processing now caps scan count and uses SIMD-accelerated matching with CPU
  feature auto-detect (fallback to scalar), reducing latency for large result sets.

#### Throughput Benchmarks (Debug, macOS M3 Max)

| Benchmark | Oct 2025 | Jan 2026 | Change |
|-----------|----------|----------|--------|
| Ingestion_SmallDocument | 2,771 ops/s | 2,821 ops/s | ~same |
| Ingestion_MediumDocument | 56 ops/s | 57 ops/s | ~same |
| Ingestion_E2E (100 docs) | - | 9.2 docs/s | new (KGWriteQueue) |
| Metadata_SingleUpdate | 10,537 ops/s | 17,794 ops/s | **+69%** |
| Metadata_BulkUpdate(500) | 7,823 ops/s | 50,473 ops/s | **+6.5x** |
| IPC_StreamingFramer | - | 3,732 ops/s | new |
| IPC_UnaryFramer | - | 10,088 ops/s | new |

### Experimental
- **libSQL backend**: Default database backend with concurrent write support via MVCC.
  Enables up to 4x write throughput during heavy indexing. Configure with meson option
  `database-backend` (choices: `libsql` [default], `sqlite`).

  **Installation**: If Rust toolchain is available, libsql builds automatically from source
  via the meson subproject. Otherwise falls back to SQLite.
  ```bash
  # Ensure Rust is installed (for automatic build)
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
  # Or disable libsql to use standard SQLite
  meson configure -Ddatabase-backend=sqlite
  ```
  See [libSQL](https://github.com/tursodatabase/libsql) for details.

### Documentation
- **Embedding model recommendations**: Added model comparison table to README. 384-dim models
  (e.g., `all-MiniLM-L6-v2`) recommended for best speed/quality tradeoff.

### Changed
- **Reranking**: Score-based reranking is now the default. Uses geometric mean of text and
  vector scores to boost documents with multi-component consensus. No external model needed.
  Cross-encoder model reranking is opt-in via `enableModelReranking` config option.
- Tuning profile multipliers updated: efficient 0.5x, balanced 1.0x, aggressive 1.5x.

### Fixed
- **FTS5 natural language queries**: OR fallback now correctly triggers when AND query returns
  zero results. Previously, long queries like scientific abstracts would fail because the AND
  query returned nothing and the OR fallback condition was never met.
- **ONNX multi-threading on Linux/macOS**: Removed forced single-threaded execution that was
  only needed for Windows. Non-Windows platforms now use `intra_op_threads=4` by default,
  improving inference speed for 768-dim and larger models by 2-4x.
- Hybrid search fusion: fallback to non-empty `filePath` when vector results have empty paths
  (hash→path lookup failures no longer cause result mismatches).
- TSAN race in `daemon_search()`: pass `DaemonSearchOptions` by value to avoid stack reference
  escaping to coroutine thread.
- TSAN race in `handle_streaming_request()`: check `connection_closing_` before `socket.is_open()`
  to avoid race with `handle_connection` closing the socket.
- Compression stats now persist across daemon restarts (`Storage Logical Bytes` vs
  `CAS Unique Raw Bytes` now show correct values).
- CLI rejects ambiguous subcommands (e.g., `yams search graph` → use `--query`).
- `--paths-only` search now returns results correctly.
- `yams watch` waits for daemon ready; always ignores `.git` contents.
- Expanded prune patterns for build artifacts and language caches.
- Fixed ONNX model loading deadlock on Windows (single-flight pattern, recursive mutex).
- Streaming: 30s chunk timeout, backpressure stops producer on queue overflow.
- `yams add` returns immediately; hash computed async during ingestion.
- Replaced experimental Boost.Asio APIs with stable `async_initiate` (fixes TSAN races).
- File history now records snapshot metadata for single-file adds, not just directory snapshots.
