# Roadmap

Concise, implementation-aligned plan for YAMS. Pre‑1.0 (v0.x) is not stable; expect changes until v1.0.

## Scope and principles
- OSS-first: CLI, storage engine, search, and MCP (stdio) are the core.
- Managed hosting will add control plane, metrics, backups, and billing; OSS remains local/stdio only.
- Data portability and deterministic builds are requirements for GA.

## Current (v0.x)
- Storage engine
  - Content-addressed (SHA‑256), Rabin chunking, zstd/LZMA compression
  - WAL-backed metadata and FTS5 text index
- Search
  - Keyword/FTS queries; vector search available where compiled in
- CLI/MCP
  - CLI first; MCP server via stdio only (no HTTP/WebSocket in OSS)
- Tooling
  - Conan + CMake presets; Release/Debug builds; container image

## Near-term (private preview / minor releases)
- CLI and DX
  - Strengthen `yams stats` schema; stable keys for dashboards
  - Improve error messages and exit codes; consistent `--json` output
  - Incremental `yams add` behaviors: predictable include/exclude, better reporting
- Search
  - FTS5 hygiene: index only queried fields; optional post-ingest `optimize`
  - Vector search quality/latency tuning documentation and sane defaults
- Managed hosting (docs-only for OSS)
  - Early access: provisioning flow, metrics and backups expectations, usage accounting model
- Knowledge Graph Enhanced Search Tagging (v0.1 → target YAMS v0.6.0)
  - Minimal KG-assisted tagging and boosting: classification signals, bounded KG expansion, hotzone scaffolding
  - Config flags under search.enhanced.* (enabled + weights), diagnostics for classification/KG/hotzones
  - Acceptance: togglable enhancements; bounded latency; unit coverage for decay/boost logic
- Plugin System (v0.1 → target YAMS v0.6.0)
  - Foundational plugin lifecycle: discover, load, initialize, unload; minimal registry with name/version/capabilities
  - Stable, documented entry points for v0.1; safe failure behavior and basic diagnostics
  - Acceptance: reference plugin builds/loads in CI; host lists plugins; failures do not crash

## Medium-term
- Content lifecycle
  - Export/import snapshots with tags and edges preserved
  - Space reclamation and integrity verification tools
- Search quality
  - Hybrid retrieval improvements (keyword prefilter → vector re-rank)
  - Better defaults and examples per content type (code, notes, docs)
- Observability
  - Stable metrics surface via `yams stats --json` for counts, sizes, index health

## Contributing to this roadmap
- Open an issue with a short proposal (problem, approach, scope, risks)
- For larger items, include evaluation plan and acceptance criteria
- Keep changes incremental and testable
