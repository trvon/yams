# External graph ingest

Callers outside YAMS (for example a citation graph) write nodes and edges into
the knowledge graph and remove them again by namespace. The same operations are
reachable from the CLI, MCP and IPC.

| Surface | Ingest | Remove |
|---------|--------|--------|
| CLI | `yams graph ingest --file <json\|-> [--json]` | `yams graph remove --ns <ns> [--relation <r>] [--dry-run] [--json]` |
| MCP `graph` tool | `action: "ingest"` | `action: "remove"`, `ns`, `relation`, `dry_run` |
| MCP composite `execute` | op `graph_ingest` | op `graph_remove` |
| IPC | `KgIngestRequest` (70) | `KgRemoveRequest` (80) |

The composite `query` tool is read-only and rejects `action: "remove"`.

## Payload

The CLI file and the MCP arguments have the same shape:

```json
{
  "nodes":   [{"node_key": "paper:doi:10.1/a", "type": "paper", "label": "A",
               "properties": {"ns": "paperbridge"}}],
  "edges":   [{"src_node_key": "paper:doi:10.1/a", "dst_node_key": "paper:doi:10.1/b",
               "relation": "cites", "weight": 1.0, "properties": {"ns": "paperbridge"}}],
  "aliases": [{"node_key": "paper:doi:10.1/a", "alias": "10.1/a", "source": "doi"}],
  "skip_existing_nodes": false,
  "skip_existing_edges": false
}
```

- Nodes upsert by `node_key`. A label, type or properties value that is sent
  replaces the stored one; an omitted or empty one keeps it. Properties are
  replaced as a whole, not merged.
- Edges upsert by (src, dst, relation). Weight becomes the max of the old and
  new weight; properties are replaced when the new weight is not lower.
- `skip_existing_*` (default `false`) leaves existing rows untouched and counts
  them as skipped.
- The CLI sends nodes, then edges, then aliases, in requests of at most 2000
  rows. Each request commits on its own; a large file is not atomic.

## Result

```json
{"action": "ingest", "success": true,
 "nodes_inserted": 2, "nodes_updated": 0, "nodes_skipped": 0,
 "edges_inserted": 1, "edges_updated": 0, "edges_skipped": 1,
 "aliases_inserted": 0, "aliases_skipped": 0,
 "unresolved_edges": [{"src_node_key": "paper:doi:10.1/a",
                       "dst_node_key": "paper:doi:10.1/missing", "relation": "cites",
                       "src_missing": false, "dst_missing": true}],
 "errors": ["Edge destination node not found: paper:doi:10.1/missing"]}
```

Counts are rows, classified inside the writer transaction: inserted is new,
updated is an existing row refreshed, skipped is an unresolved edge, a
`skip_existing` hit or a node without a key. An edge is unresolved when either
endpoint does not exist after the request's nodes are written; it is reported
and not written. A repeated row in one request counts once as inserted and then
as updated.

## Namespaces

`properties.ns` marks a row as owned by an external caller. YAMS writers do not
set it.

- Document cleanup (`deleteNodesForDocumentHash`, `deleteNodesForSourceFile`,
  `deleteEdgesForSourceFile`, document graph cleanup) and orphan repair
  (`deleteOrphanedNodes`) never remove a row with an `ns`, whatever its
  `document_hash`, `file_path` or `source_file` properties say.
- Snapshot-version pruning removes nodes carrying `snapshot_id` and
  `canonical_key`; external rows should not use those keys.
- `doc:<hash>` and `blob:<hash>` nodes are YAMS-owned. Deleting or re-indexing a
  document removes them, and every edge to them, including namespaced edges.
  Link external nodes to documents only if that is acceptable.
- Removal requires a non-empty `ns`. With `relation`, it deletes the edges of
  that relation whose `ns` matches and keeps nodes. Without it, it deletes the
  namespace's edges, its nodes, and every edge incident to those nodes. There
  is no relation-only removal. `dry_run` reports the counts and deletes nothing.

## Compatibility

- `KgIngestResponse` gained `nodes_updated`, `edges_updated` and
  `unresolved_edges` (protobuf fields 9-11). `nodes_inserted` and
  `edges_inserted` now count new rows only; older clients that read them as
  "rows accepted" see smaller numbers on re-ingest.
- `skip_existing_*` defaulted to `true` but were ignored, so every ingest
  upserted. The default is now `false` (same behaviour); `true` now skips.
- KgIngest had a protobuf schema but no binding and could not cross the socket
  transport; it now can.
- `KgRemoveRequest` is a separate message, not an ingest flag. A daemon that
  predates it rejects the message instead of ingesting.
