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
- **IPC connection pooling**: `AsioConnectionPool::acquire()` now reuses idle connections
  instead of creating new ones for each request, saving ~1-5ms per CLI command.
- **Async response signaling**: Replaced 10ms polling loop with timer-based async notification.
  Read loop now cancels a notify timer when response arrives, eliminating 0-10ms latency floor.
- Combined IPC latency reduced from ~8-28ms to ~2-5ms per request.

### Fixed
- CLI now rejects multiple top-level subcommands (e.g., `yams search graph`) and suggests
  using `--query` when a search term matches a command.
