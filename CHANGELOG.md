# Changelog

All notable changes to YAMS (Yet Another Memory System) will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

- [SourceHut](https://sr.ht/~trvon/yams/): https://sr.ht/~trvon/yams/

## Archived Changelogs
- v0.13.x archive: docs/changelogs/v0.13.md
- v0.12.x archive: docs/changelogs/v0.12.md
- v0.10.x archive: docs/changelogs/v0.10.md
- v0.9.x archive: docs/changelogs/v0.9.md
- v0.8.x archive: docs/changelogs/v0.8.md
- v0.7.x archive: docs/changelogs/v0.7.md
- v0.6.x archive: docs/changelogs/v0.6.md
- v0.5.x archive: docs/changelogs/v0.5.md
- v0.4.x archive: docs/changelogs/v0.4.md
- v0.3.x archive: docs/changelogs/v0.3.md
- v0.2.x archive: docs/changelogs/v0.2.md
- v0.1.x archive: docs/changelogs/v0.1.md

### Fixed

* download logic improvements and ci test fixes. Stagging download fixes seperate from search features ([2d5c662](https://github.com/trvon/yams/commit/2d5c6625eb4e794c878441bb052911baf91169e0))
Full changelog: [CHANGELOG.md](https://github.com/trvon/yams/blob/v0.14.1/CHANGELOG.md)

## [0.14.0](https://github.com/trvon/yams/compare/v0.13.1...v0.14.0) (2026-05-04)

### Highlights

- Moved the search and embedding stack further toward the Simeon backend, including PHSS vector search support, topology and clustering work, and continued removal of legacy HNSW paths.
- Improved write-path coordination, repair and rebuild behavior, and daemon readiness under load.
- Added storage health checks, corruption detection, and broader instrumentation and tuning for memory-constrained systems.

### Breaking Changes

- Embeddings no longer center on ONNX Runtime by default. YAMS now prefers the lighter Simeon path, which reduces resource usage but requires AVX or NEON support.
- ONNX Runtime support was later reinstated as an alternate path, but Simeon remains the default direction in this release.

### Notable Fixes

- Fixed daemon status and readiness regressions, SocketServer loop fragility, MCP router issues, and DB optimize repair handling.
- Reduced memory pressure in repair and search paths and tightened batching for topology and rebuild work.
- Cleaned up release and packaging regressions, including build-artifact leakage and platform-specific CI issues that affected release confidence.

### Security

- Expanded Opengrep coverage and addressed storage correctness, underflow-related bugs, and repair subsystem scanning issues.
- Tightened related hook and script behavior used in repo security checks.

### Performance

- Improved idle memory reclamation, ingestion throughput, startup behavior, and compressed R2 transport behavior.

### Full Details

- See `CHANGELOG.md` in this PR for the full commit-level history.

---
This PR was generated with [Release Please](https://github.com/googleapis/release-please). See [documentation](https://github.com/googleapis/release-please#release-please).
