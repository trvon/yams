# Changelog

All notable changes to YAMS (Yet Another Memory System) will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

- [SourceHut](https://sr.ht/~trvon/yams/): https://sr.ht/~trvon/yams/

## Archived Changelogs
- v0.6.x archive: docs/changelogs/v0.6.md
- v0.5.x archive: docs/changelogs/v0.5.md
- v0.4.x archive: docs/changelogs/v0.4.md
- v0.3.x archive: docs/changelogs/v0.3.md
- v0.2.x archive: docs/changelogs/v0.2.md
- v0.1.x archive: docs/changelogs/v0.1.md

## [v0.7.3] - Unreleased

### Fixed
- **Daemon IPC:** Fixed a regression in the `grep` IPC protocol where `GrepRequest` and `GrepResponse` messages were not fully serialized, causing data loss. The protocol definitions and serializers have been updated to correctly handle all fields, including `show_diff` in requests and detailed statistics in responses.

## [v0.7.2] - 2025-10-03

### Added
- Automatic directory snapshot generation with ISO 8601 timestamp IDs and git metadata detection (commit, branch, remote). Every `yams add <directory>` now creates a timestamped snapshot stored in the `tree_snapshots` table.
- Snapshot Listing: New `yams list --snapshots` command displays all available snapshots with table and JSON output formats, showing snapshot IDs, directory paths, labels, git commits, and file counts.
-  Implemented `yams diff <snapshotA> <snapshotB>` command with tree, flat, and JSON output formats for comparing directory snapshots.
- TreeDiffer automatically detects renamed/moved files via SHA-256 hash equivalence matching, enabled by default.

### Changed
- **Snapshot Labels**: `yams add` now accepts optional `--label` flag for human-readable snapshot names.
- **Indexing Service**: Enhanced to persist snapshot metadata (snapshot_id, directory_path, git metadata, file count) to database after directory ingestion.
- **Metadata Repository**: Added `upsertTreeSnapshot()`, `listTreeSnapshots()`, and tree diff persistence methods for snapshot and change history management.
- Search: Parallelized keyword search scoring loop to significantly improve performance on multi-core systems.
- Search: Search thread pools are now configured by the central `TuningManager` to adapt to system load and tuning profiles.
- Search: Implemented structural scoring to boost relevance of results that are co-located in the same directory.
- Search: Parallelized keyword search scoring loop to significantly improve performance on multi-core systems.
- Search: Search thread pools are now configured by the central `TuningManager` to adapt to system load and tuning profiles.
- Search: Implemented structural scoring to boost relevance of results that are co-located in the same directory.
- Added FTS5 readiness fast-path check in `getByNameSmart()` to prevent 3-second blocking timeouts when search indexes are updating. 
- Added `post_ingest_queue_depth` field to status response, enabling clients to check if FTS5 indexes are ready before attempting expensive search operations.
- TUI browse command now resolves listings and fuzzy search through the shared AppContext service bundle (`TUIServices` + `IDocumentService`/`ISearchService`), with graceful fallback to metadata/content-store paths when the daemon is degraded.
- CLI Browse: Shift+R reindex dialog now performs a full extraction + index refresh through `TUIServices::reindexDocument`, providing inline success/error feedback instead of the previous placeholder flow.

### Fixed
- Daemon IPC: SocketServer now shares a live writer-budget reference with every connection and the tuning manager pushes updates through it. Multiplexed streams adjust bandwidth limits immediately when profiles or runtime heuristics change.
- Search: Corrected an issue where `yams search --include` was not being applied for hybrid searches. The include pattern is now passed to the daemon and correctly filters results.
- Fixed protobuf UTF-8 validation errors when grepping binary files or non-UTF-8 text. Changed `GrepMatch.line`, `context_before`, and `context_after` fields from `string` to `bytes` type in protobuf definition. This allows grep to handle arbitrary byte sequences including binary content, Latin-1, Windows-1252, and other legacy encodings without validation failures. (PBI-001, task 001-33)
- Daemon IPC: replaced the `io_context.run_for` polling loop with dedicated `run_one` workers so async accept completions are no longer starved during streaming requests. Added optional diagnostic thread (`YAMS_SOCKET_RUN_DIAG`) for debugging.
- CLI Browse: refuse to launch the FTXUI browser when the terminal is non-interactive, lacks TERM capabilities, or is smaller than 60x18; emit a clear resize guidance message instead of hanging or crashing.
- CLI Search: release pooled daemon clients before process teardown to prevent the `std::system_error: mutex lock failed` abort when `yams search` exits after hitting the daemon path.

## [v0.7.1] - 2025-09-29

### Changed
- GrepService: expanded candidate discovery to preselect from `req.paths` using SQL LIKE prefix scans, aligning service behavior with CLI expectations for directory patterns.
- RepairCoordinator refocus: on live `DocumentAdded` events, skip queuing when the post‑ingest
- Post‑ingest pipeline: improvements
- ServiceManager enqueue path: simplified `enqueuePostIngest` to a direct blocking enqueue. This improves predictability and throughput under high load.
- CLI Download UX: `yams download` now clearly displays the ingested content hash

### Fixed
- GrepService streaming: flushes the final partial line when scanning cold CAS streams so single-line files are matched reliably (e.g., `hello.txt`).
- Reduced GrepService log verbosity to `debug` for internal counters and match traces.
- Fixed IPC protocol regression where grep and list commands failed to properly communicate with the daemon after migration, causing incomplete results or timeouts in multi-service environments.
  - This issue impacted other tools result output 
- Guarded compression monitor global statistics with a dedicated mutex to stop concurrent tracker
  updates from crashing `unit_shard5` (validated via `meson test -C build/debug unit_shard5
  --print-errorlogs`).
- Repaired the `document_service` metadata pipeline regression so fixture-driven search tests no
  longer observe missing extracted content.
- MCP stdio transport: replaced unused static output mutex with an instance mutex to satisfy
  ODR/build on certain platforms.

## [v0.7.0] - 2025-09-25

### Highlights
- These changes reduce CPU spikes observed in profiles for large greps and remove
  blocking storage scans from interactive status paths. Post-ingest work is intentionally
  bounded; processing may take longer, but overall system responsiveness improves.
- Stability: resolved connection timeouts under multi-agent load by removing the hard
  100-connection cap and deriving a dynamic accept limit. Defaults honor
  `YAMS_MAX_ACTIVE_CONN` or compute a safe cap from CPU cores and IO concurrency.
- Throughput: added tuning profiles (efficient | balanced | aggressive). Profiles modulate
  pool growth, IO thresholds, and post-ingest workers. Default is `balanced`.
- Indexing UX: Add/ingest returns fast; post‑ingest queue handles FTS/embeddings/KG in the
  background. Path‑series versioning (Phase 1) is on by default behind an env flag.

### Added
- Tuning profiles selectable via config or env:
  - Config: `yams config set tuning.profile <efficient|balanced|aggressive>`
  - Env: `YAMS_TUNING_PROFILE=<profile>`
- Config defaults now include `[tuning] profile = "balanced"`.
- Docs: `docs/admin/tuning_profiles.md` covering profiles, envs, and observability.
- Versioning (Phase 1): path‑series lineage with `VersionOf` edges and metadata flags
  `version`, `is_latest`, `series_key`. Duplicate (same hash) re‑ingest does not create a new
  version; alternate locations and timestamps are updated.

- CLI Search: grouped multi‑version presentation (default on) with new controls.
  - Groups results by canonical path when multiple versions of the same file are returned.
  - New flags:
    - `--no-group-versions` — disable grouping and show the flat list.
    - `--versions <latest|all>` — choose best only (default: latest) or list versions per path.
    - `--versions-topk <N>` — cap versions shown per path when `--versions=all` (default: 3).
    - `--versions-sort <score|path|title>` — sort versions within a group (default: score).
    - `--no-tools` — hide per‑version tool hints.
    - `--json-grouped` — emit grouped JSON; plain `--json` remains flat and backward compatible.
  - Tool hints shown per version (when grouped):
    `yams get --hash <hash> | yams cat --hash <hash> | yams restore --hash <hash>`;
    if a local file path is resolved, a `yams diff --hash <hash> <local-path>` hint is added.
  - Environment toggles: `YAMS_NO_GROUP_VERSIONS=1` and `YAMS_NO_GROUP_TOOLS=1` to flip defaults.
  - Note: This is a presentation‑layer change; service/daemon APIs are unchanged.

### Changed
- **Build System**
  - The primary build system has been migrated from CMake to Meson. All build, test, and packaging scripts have been updated to use the new Meson-based workflow.
- Status/Stats (CLI): use daemon metrics by default and never trigger local storage scans.
  - `yams status` and `yams stats -v` now render from the same non-detailed daemon snapshot;
    removed the "scanning storage..." spinner and filesystem walks.
  - Verbose output formats the JSON fields instead of performing extra scans.
- Tools/Stats (yams-tools): `tools/yams-tools/src/commands/stats_command.cpp` refactored to
  prefer daemon-first metrics with a legacy local fallback only if daemon is unavailable.
- MCP add_directory: switched to daemon-first ingestion with a brief readiness wait to avoid
  "Content store not available" races. Removes local store preflight; maps NotInitialized to a
  clear, retryable message from the daemon.
- MCP search: path normalization + optional diff parity with CLI.
  - New request field `include_diff` adds a structured `diff` block to results when the
    `path_pattern` points to a local file; mirrors `yams search` diff behavior.
  - MCPSearch DTOs extended to round-trip `include_diff`, `diff`, and `local_input_file`.
- Daemon accept scaling: removed fixed cap; now dynamically computes `maxConnections` from
  `recommendedThreads * ioConnPerThread * 4` (min 256) unless `YAMS_MAX_ACTIVE_CONN` is set.
- Backpressure: increased default read pause to 10ms to smooth heavy load.
- Post‑ingest: preserves bounded capacity; de‑dupes inflight, indexes FTS, updates fuzzy index,
  and emits KG nodes/edges best‑effort.
- Status/Stats: JSON correctness improvements; omit misleading savings when physical size
  unknown; surface post‑ingest bus usage and document counters.
- CLI Search: grouping of multiple versions per path is enabled by default; paths‑only output
  and flat JSON remain unchanged unless `--json-grouped` is specified.

### Fixed
- Regression in metadata extraction and storage used in search and grep tools
  The async post-ingest pipeline never persisted extracted text into the metadata store. As a result, document_content stayed empty, so search, repairs, and semantic pipelines saw “Document content not found” despite vector insert logs.
- Many tuning optimizations for daemon usage
- Grep pipeline: staged KG → metadata → content with caps and budget.
  - Prefers "hot" text (metadata-extracted) and caps cold CAS reads; early path/include filters.
  - Added a global time budget (internal) to stop long content scans gracefully.
  - Capped grep worker threads to a small, background-friendly number by default (≤4).
- Grep streaming optimization: replaced per-character streambuf overflow with bulk line splitting
  (memchr-based) to eliminate the per-byte hotspot in profiles during CAS streaming.
- Post-ingest queue: bounded by configuration, not CPU heuristics.
  - Default worker threads set conservatively to 1 unless configured in `[tuning]` as
    `post_ingest_threads`. Queue capacity now honored from `post_ingest_queue_max`.
  - Added a tiny yield between tasks to reduce contention and smooth CPU.
- Addressed intermittent CLI timeouts and “Broken pipe” logs observed when many agents
  connected concurrently. Accept loop backoff now respects the higher connection cap and IO pool
  growth from the tuning manager.
- Minor unit test fixes (Result<T> value handling) to unblock CI.
