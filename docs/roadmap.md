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
- Ops and docs
  - Admin: verified configuration, backup/restore, and tuning guides (kept concise)
  - MCP stdio: schema clarifications; tool list stability; minimal integration tests
- Managed hosting (docs-only for OSS)
  - Early access: provisioning flow, metrics and backups expectations, usage accounting model

## PBI: BERT-based tagging and knowledge graph organization
Goal: automatic semantic tags and lightweight knowledge graph edges to improve discovery and organization.

- Model and pipeline
  - Use a BERT‑family encoder (on-device or local server inference) to derive tags and relation candidates
  - Batch/offline pipeline with resumable jobs; explicit CLI entry points
  - Deterministic versioned pipeline: pin model, tokenizer, thresholds; record in metadata
- Tagging
  - Generate candidate tags per document with confidence scores
  - Store tags in metadata with provenance fields: `tag_source=bert`, `tag_confidence=<0..1>`, `tag_version=<pipeline-id>`
  - Configurable allow/deny lists; optional human-in-the-loop approval mode
- Knowledge graph (lightweight)
  - Extract relation candidates (e.g., doc→doc, doc→entity) with scores
  - Persist edges in metadata or as dedicated edge records stored in YAMS with `edge:{src,dst,type,score,version}`
  - Namespacing and scoping for multi-project isolation
- CLI
  - `yams tag --model <name> [--limit N] [--min-score S]` to append tags
  - `yams edges --model <name> [--min-score S]` to emit edges; `--dry-run` preview
  - `yams search` filters for tags: `--tag k=v` or `--tags k=v,k2=v2`
- Indexing
  - Ensure tags are indexed for fast filter queries
  - Optional materialized fields for common tag lookups
- Security and privacy
  - Local inference by default; clear opt-in for any remote inference
  - Redactable outputs; ability to remove or re-run tagging by `tag_version`
- Evaluation
  - Small labeled set for precision/recall estimation
  - Benchmarks: throughput (docs/s), latency, memory footprint; reproducible scripts
- Acceptance criteria
  - Deterministic pipeline w/ versioning and rollback
  - Measurable search improvement on labeled queries
  - Docs: configuration, limits, and operational guidance

## Medium-term
- Content lifecycle
  - Export/import snapshots with tags and edges preserved
  - Space reclamation and integrity verification tools
- Search quality
  - Hybrid retrieval improvements (keyword prefilter → vector re-rank)
  - Better defaults and examples per content type (code, notes, docs)
- Observability
  - Stable metrics surface via `yams stats --json` for counts, sizes, index health

## Managed hosting (parallel track)
- Control plane
  - Projects, tokens, usage reporting; later: SSO and audit logs
- Data plane
  - Hosted YAMS engine; backups and retention policies
- Console
  - Minimal dashboard (usage, quotas, key management)
- Billing
  - Stripe-backed; usage-based with caps (beta), SLAs (GA)
- Migration
  - Export/import tooling for moving between self-managed and hosted

## GA criteria (v1.0)
- Storage/API stability: no breaking changes to core CLI flags and metadata formats
- Deterministic builds: tagged releases with reproducible binaries
- Recovery: documented, tested backup/restore; integrity checks
- Documentation: concise, complete operator and user guidance

## Notes
- MCP server remains stdio-only in OSS; no network server in the CLI.
- Environment variable precedence: `YAMS_STORAGE` and explicit `--storage/--data-dir` flags should be consistent and documented.

## Contributing to this roadmap
- Open an issue with a short proposal (problem, approach, scope, risks)
- For larger items, include evaluation plan and acceptance criteria
- Keep changes incremental and testable