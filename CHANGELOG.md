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

## [v0.7.11] - Unreleased

### Breaking
- **Vector database migration required**: sqlite-vec-cpp HNSW rewrite invalidates existing vector indices. After upgrading, run:
  ```bash
  yams doctor repair --embeddings   # Regenerate all embeddings
  yams doctor repair --graph        # Rebuild knowledge graph (optional)
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

### Performance

#### IPC & Daemon
- IPC latency reduced from ~8-28ms to ~2-5ms (connection pooling, async timers, cached config).
- Daemon startup throttling: PathTreeRepair via RepairCoordinator, Fts5Job startup delay (2s), reduced batch sizes (1000→100).

#### Ingestion & Storage
- Post-ingest throughput: dedicated worker pool, adaptive backoff, batched directory ingests.
- In-memory chunking for `storeBytes()` - avoids temp file I/O for large documents.

#### Database & Metadata
- Prepared statement caching for SQLite queries - reduces SQL compilation overhead on repeated operations. Cached methods: `setMetadata`, `setMetadataBatch`, `getMetadata`, `getAllMetadata`, `getContent`, `getDocument`, `getDocumentByHash`, `updateDocument`, `deleteDocument`, `insertContent`.
- `setMetadataBatch()` API for bulk metadata updates - 4x faster than individual calls.

#### Search & Retrieval
- Batch vector/KG lookups, flat_map for cache-friendly access, branch hints, memory pre-allocation.

#### Throughput Benchmarks (Debug, macOS M3 Max)

| Benchmark | Oct 2025 | Jan 2026 | Change |
|-----------|----------|----------|--------|
| Ingestion_SmallDocument | 2,771 ops/s | 2,821 ops/s | ~same |
| Ingestion_MediumDocument | 56 ops/s | 57 ops/s | ~same |
| Metadata_SingleUpdate | 10,537 ops/s | 17,794 ops/s | **+69%** |
| Metadata_BulkUpdate(500) | 7,823 ops/s | 50,473 ops/s | **+6.5x** |
| IPC_StreamingFramer | - | 3,732 ops/s | new |
| IPC_UnaryFramer | - | 10,088 ops/s | new |

#### Retrieval Quality Benchmarks (SciFact: 5,183 docs, 300 queries)

| Metric | Score | Description |
|--------|-------|-------------|
| MRR | 0.630 | Mean Reciprocal Rank |
| Recall@10 | 0.799 | Recall at K=10 |
| MAP | 0.628 | Mean Average Precision |
| nDCG@10 | 0.669 | Normalized Discounted Cumulative Gain |

### Fixed
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
