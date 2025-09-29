# Developer Architecture Notes

This index points to architecture references that mirror the live implementation. Always validate statements against the cited source modules.

## Core Architecture References

- Daemon and services — lifecycle, IPC, plugin hosting
  - [Architecture: Daemon and Services](../../architecture/daemon_architecture.md)
- Plugin pipeline — manifest parsing, trust, runtime loaders
  - [Architecture: Plugin and Content Extraction](../../architecture/plugin_architecture.md)
- Search pipeline — ingestion, ranking, hybrid fusion
  - [Architecture: Search System](../../architecture/search_system.md)
- Vector subsystem — embedding providers, storage, repair routines
  - [Architecture: Vector Search](../../architecture/vector_search_architecture.md)

## Supporting Design Notes

- Content handlers and normalization pathways
  - [Design: Content Handler Architecture](../../design/content-handler-architecture.md)
- Synchronization protocol details
  - [Design: Sync Protocol](../../design/sync-protocol.md)

## API References

- [API Overview](../../api/README.md)
- [Search API](../../api/search_api.md)
- [Vector Search API](../../api/vector_search_api.md)
- [OpenAPI Definition](../../api/openapi.yaml)
- [MCP Tools](../../api/mcp_tools.md)

## Operations and Performance Guides

- [Admin Configuration](../../admin/configuration.md)
- [Performance Tuning](../../admin/performance_tuning.md)
- [Vector Search Tuning](../../admin/vector_search_tuning.md)
- [Deployment Guide](../../operations/deployment.md)
- [Monitoring Guide](../../operations/monitoring.md)
- [Backup Procedures](../../operations/backup.md)
- [Performance Report](../../benchmarks/performance_report.md)

## Contribution Workflow

1. Update architecture docs and the traceability table (`docs/delivery/038/artifacts/architecture-traceability.md`) in the same change set.
2. Anchor new prose to specific functions, classes, or modules under `src/`.
3. Capture validation evidence (integration logs, benchmarks) in `docs/delivery/038/artifacts/` with timestamps.
4. Run `mkdocs build --strict` and archive the log before submitting changes.
5. Flag future work explicitly with issue references; avoid speculative language.

Missing topics should be proposed via issue (outline + intended source references) prior to documentation work.
