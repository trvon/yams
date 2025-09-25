# Graph Adapter v1 — API Overview

Status: draft

Purpose
- Provide a stable, read-first property-graph interface for plugins and SDKs.
- Support large graphs via cursor-based iteration; optional import/export and delta apply.

Key Concepts
- Graph: collection of nodes and edges with optional `directed` flag and stats.
- Node: `id`, `labels[]`, `properties{}` (scalar types only; complex values may be encoded as JSON strings).
- Edge: `id`, `src`, `dst`, optional `label`, optional `weight`, `properties{}`.
- Capabilities: adapter reports `read_only`, `multigraph`, `weighted`, `delta`, `provenance`, `views`, and supported import/export formats.

WIT Definition
- See `docs/spec/wit/graph_adapter_v1.wit` for the canonical interface.

Portable Formats
- GraphJSON v1 (canonical): `docs/spec/schemas/graphjson_v1.schema.json`
- Graph Delta v1: `docs/spec/schemas/graph_delta_v1.schema.json`
- Provenance v1: `docs/spec/schemas/provenance_v1.schema.json`
- Card Anchor v1: `docs/spec/schemas/card_anchor_v1.schema.json`
- Export adapters (optional): GraphML, edge-list (CSV/Parquet), Cytoscape JSON.

Example Workflows
- Enumerate graphs: `list-graphs()` => [`graph-info`]
- Read nodes/edges:
  1) `cursor = nodes-begin(graph_id, filter_json=null)`
  2) Loop `nodes-next(cursor, limit)` until `done=true`
  3) `nodes-end(cursor)`
- Export a graph: `export-graph(graph_id, "graphjson", options_json)` => bytes
- Import a graph: `import-graph("graphjson", data, options_json)` => `graph_id`
- Apply delta: `apply-delta-json(graph_id, jsonl_string)` => count

Notes
- Filters are JSON and adapter-defined (e.g., label/property predicates). Adapters should document supported filters.
- Property values are restricted to scalars for maximum interop; complex structures can be JSON-encoded strings if needed.
- For very large exports, implementations may chunk internally; the interface returns a single binary for simplicity.

