# Developer Architecture Notes

This index points to architecture references that mirror the live implementation. Always validate statements against the cited source modules.

## Core Architecture References

- Daemon and services — lifecycle, IPC, plugin hosting
  - [Architecture: Daemon and Services](../../architecture/daemon_architecture.md)
  - Plan: Daemon Simplification V2 (FSM-first) (link removed; file is outside docs)
- Plugin pipeline — manifest parsing, trust, runtime loaders
  - [Architecture: Plugin and Content Extraction](../../architecture/plugin_architecture.md)
- Search pipeline — ingestion, ranking, hybrid fusion
  - (TBD) Search System overview (link removed; page not present in docs)
- Vector subsystem — embedding providers, storage, repair routines
  - (TBD) Vector Search overview (link removed; page not present in docs)

## Supporting Design Notes

- Content handlers and normalization pathways
  - [Design: Content Handler Architecture](../../design/content-handler-architecture.md)
- Synchronization protocol details
  - [Design: Sync Protocol](../../design/sync-protocol.md)

## API References

- [API Overview](../../api/README.md)
- HTTP API topical docs and OpenAPI spec are not published in this docs site yet.
- [MCP Tools](../../api/mcp_tools.md)

## Operations and Performance Guides

- [Admin: Operations](../../admin/operations.md)
- Deployment/Monitoring/Backup pages are not published in this docs site yet.
- [Performance Report](../../benchmarks/performance_report.md)

## Contribution Workflow

1. Update architecture docs and any related traceability artifacts in the same change set.
2. Anchor new prose to specific functions, classes, or modules under `src/`.
3. Capture validation evidence (integration logs, benchmarks) alongside the change with timestamps.
4. Run `mkdocs build --strict` and archive the log before submitting changes.
5. Flag future work explicitly with issue references; avoid speculative language.

Missing topics should be proposed via issue (outline + intended source references) prior to documentation work.
