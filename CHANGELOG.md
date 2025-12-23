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

### Performance
- IPC latency reduced from ~8-28ms to ~2-5ms per request:
  - Connection pool now reuses idle connections
  - Replaced polling loop with async timer signaling
  - Request-type-aware timeouts (5s/30s/120s for fast/medium/slow ops)
  - Status retry delays reduced from 750ms to 175ms
  - Config parsing cached per-process
  - Async semaphore for connection slots
- Post-ingest throughput improvements:
  - Dedicated worker pool for binary entity extraction to avoid starving metadata/KG work
  - Post-ingest pollers drain queues with adaptive backoff instead of fixed 25-50ms sleeps
  - Queue capacity honors configured post-ingest limits
  - Post-ingest enqueue avoids long blocking backoff when queue is full
  - Directory ingest batches post-ingest enqueues to reduce per-file overhead

### Fixed
- CLI now rejects multiple top-level subcommands (e.g., `yams search graph`) and suggests
  using `--query` when a search term matches a command.
- Fixed `--paths-only` search returning no results: `pathsOnly` is now a CLI-only
  display option rather than being sent to the daemon, which was causing the search
  service to return paths in a different field that the daemon didn't read.
- Fixed ONNX model loading deadlock (EDEADLK code=36) on Windows:
  - ServiceManager now syncs `embeddingModelName_` from PluginManager during model
    provider adoption, enabling proper startup preload paths.
  - ONNX plugin uses single-flight pattern to prevent concurrent model loading.
  - Changed OnnxModelPool to use `std::recursive_mutex` to work around Windows-specific
    issue with `std::mutex`/`std::condition_variable` interaction.

### Added
- MCP `graph` tool for knowledge graph queries (parity with CLI `yams graph`).
- Snapshot-scoped graph version nodes with canonical roots and `observed_as` edges.
- Graph prune policy (configurable `daemon.graph_prune`) to keep latest snapshot versions.
- Integration coverage for snapshot versioning + pruning in EntityGraphService tests.
