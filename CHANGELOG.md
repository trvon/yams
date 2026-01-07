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
- IPC latency reduced from ~8-28ms to ~2-5ms (connection pooling, async timers, cached config).
- Post-ingest throughput: dedicated worker pool, adaptive backoff, batched directory ingests.
- Search optimizations: batch vector/KG lookups, flat_map for cache-friendly access, branch hints, memory pre-allocation.
- Daemon startup throttling: PathTreeRepair via RepairCoordinator, Fts5Job startup delay (2s), reduced batch sizes (1000→100).
- **New**: `setMetadataBatch()` API for bulk metadata updates - 4x faster than individual calls.
- **New**: In-memory chunking for `storeBytes()` - avoids temp file I/O for large documents.
- Benchmarks (Debug, macOS M3 Max):
  | Benchmark | Oct 2025 | Jan 2026 | Change |
  |-----------|----------|----------|--------|
  | Ingestion_SmallDocument | 2,771 ops/s | 2,821 ops/s | ~same |
  | Ingestion_MediumDocument | 56 ops/s | 57 ops/s | ~same |
  | Metadata_SingleUpdate | 10,537 ops/s | 13,966 ops/s | **+33%** |
  | Metadata_BulkUpdate(500) | 7,823 ops/s | 51,341 ops/s | **+6.5x** |
  | IPC StreamingFramer_32x10 | - | 3,732 ops/s | new |
  | IPC UnaryFramer_8KB | - | 10,088 ops/s | new |

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
