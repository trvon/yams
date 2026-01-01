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
- Session watch auto-ingest now always ignores `.git` contents, even when not present
  in `.gitignore`.
- `yams watch` now waits for the daemon to report ready before enabling auto-ingest
  (configurable timeout).
- Indexing skips known build artifacts and package dependency/cache paths based on
  magic number prune categories.
- Expanded magic-number prune patterns for build output directories and language-specific
  caches (e.g., build/dist/target/.next/.pytest_cache).
- Doctor prune now advertises the git-artifacts category in CLI help/docs.
- Prune category detection now uses full file paths so git artifacts are detected.
- Doctor prune now shows a spinner while waiting for long-running daemon operations.
- Fixed ONNX model loading deadlock (EDEADLK code=36) on Windows:
  - ServiceManager now syncs `embeddingModelName_` from PluginManager during model
    provider adoption, enabling proper startup preload paths.
  - ONNX plugin uses single-flight pattern to prevent concurrent model loading.
  - Changed OnnxModelPool to use `std::recursive_mutex` to work around Windows-specific
    issue with `std::mutex`/`std::condition_variable` interaction.
- Download CLI now attempts daemon-first execution (when simple options are used) and falls back
  to local services on failure, reducing lock contention.
- Streaming responses now use a 30-second chunk timeout by default to prevent hangs when
  processing stalls. Previously disabled (0ms default).
- Streaming backpressure: when queue overflows, the producer loop now stops immediately
  instead of continuing to generate chunks that can't be sent.
- Add command now returns immediately for file paths instead of blocking on hash computation.
  Hash is computed asynchronously during ingestion.
- Replaced experimental boost::asio channel APIs with stable `async_initiate` pattern in
  DatabaseManager, ServiceManager, SearchEngineManager, and init_utils. This addresses
  TSAN data races in experimental channel implementations.
- Replaced experimental `awaitable_operators` (operator||) with stable `async_initiate`
  pattern in request_handler, SocketServer, daemon client files (asio_connection,
  asio_transport, asio_connection_pool, daemon_client). Completes removal of all
  experimental boost::asio APIs.
- Fixed `experimental::as_tuple` → stable `as_tuple` in request_handler.

### Added
- MCP `graph` tool for knowledge graph queries (parity with CLI `yams graph`).
- Snapshot-scoped graph version nodes with canonical roots and `observed_as` edges.
- Graph prune policy (configurable `daemon.graph_prune`) to keep latest snapshot versions.
- Integration coverage for snapshot versioning + pruning in EntityGraphService tests.
- Graph: file/path → symbol `contains` edges to improve dead-code analysis.
- Graph CLI: `--dead-code-report` to emit scoped isolated-node reports (src/include allowlist).
- Download CLI: progress streaming (human/json) via DownloadService callbacks.
- Benchmarks: ingestion throughput baseline config and repeatable workflow docs.
- Symbol-aware search ranking: symbol definitions now rank higher than usages in search
  results. Configurable via `YAMS_SYMBOL_WEIGHT` env var (default 0.15).
- Zig language support in symbol extractor plugin: functions, structs/enums/unions,
  fields, imports, and call expressions are now extracted from .zig files.
