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

## [v0.8.2] - Unreleased

### Fixed
- MCP stdio: Improved OpenCode compatibility during handshake/tool discovery (initialize capabilities schema, strict JSON-RPC batch responses, more robust NDJSON parsing).
- MCP tools: Fixed `mcp.echo` tool `inputSchema` shape so tool schemas validate correctly.

### Added
- MCP stdio debug/compat toggles: `YAMS_MCP_HANDSHAKE_TRACE` (trace handshake events) and `YAMS_MCP_MINIMAL_TOOLS` (expose only `mcp.echo` for client compatibility debugging).


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
