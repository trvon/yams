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

## [v0.7.0-pre] - 2025-09-21

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
