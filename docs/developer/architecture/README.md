# Developer Architecture Notes

This index collects architecture materials for YAMS. It points to system overviews, component designs, and related references. Keep documents technical, accurate, and concise.

## Core architecture

- Search system
  - Search pipeline, FTS, metadata, and query flows:
    - [Architecture: Search System](../../architecture/search_system.md)
- Vector search
  - Embedding ingestion, index layout, retrieval, and fusion strategies:
    - [Architecture: Vector Search](../../architecture/vector_search_architecture.md)

## Design notes

- Content handlers and extensibility: adapters, media types, and normalization
  - [Design: Content Handler Architecture](../../design/content-handler-architecture.md)
- Synchronization model and consistency guarantees
  - [Design: Sync Protocol](../../design/sync-protocol.md)

## APIs

- High‑level API documentation and OpenAPI
  - [API Overview](../../api/README.md)
  - [Search API](../../api/search_api.md)
  - [Vector Search API](../../api/vector_search_api.md)
  - [OpenAPI (YAML)](../../api/openapi.yaml)
  - [MCP Tools](../../api/mcp_tools.md)

## Operations and tuning (reference)

- Admin
  - [Configuration](../../admin/configuration.md)
  - [Performance Tuning](../../admin/performance_tuning.md)
  - [Vector Search Tuning](../../admin/vector_search_tuning.md)
- Operations
  - [Deployment](../../operations/deployment.md)
  - [Monitoring](../../operations/monitoring.md)
  - [Backup](../../operations/backup.md)
- Benchmarks
  - [Performance Report](../../benchmarks/performance_report.md)

## Data and engine quick facts

- Storage: content‑addressed (SHA‑256), block‑level deduplication (Rabin), compression (zstd/LZMA).
- Indexing: SQLite FTS5 for full‑text; vector index for semantic retrieval.
- Consistency: WAL for crash safety; explicit snapshot and collection semantics where applicable.
- Interfaces: CLI and API; MCP server support for tool integrations.

## Additional overviews

- Daemon and services
  - [Architecture: Daemon](../../architecture/daemon_architecture.md)
- Plugin and content extraction
  - [Architecture: Plugin](../../architecture/plugin_architecture.md)
- Disaster Recovery
  - [Architecture: DR](../../architecture/dr_architecture.md)

## Contributing to architecture docs

- Location: add new overviews under `docs/developer/architecture/`; component/feature deep‑dives under `docs/design/`.
- Scope: one topic per document; link related docs at top.
- Style: concise, diagrams optional (store sources alongside images).
- Versioning: note behavioral differences if the doc applies to specific versions. Pre‑1.0 (v0.x) may change; call out stability caveats.

If a topic is missing, open an issue proposing the outline and intended placement before submitting a PR.
