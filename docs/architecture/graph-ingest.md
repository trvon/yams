# External graph ingest

Callers outside YAMS (for example a citation graph) write nodes and edges into
the knowledge graph and remove them again by namespace. The same operations are
reachable from the CLI, MCP and IPC.

| Surface | Ingest | Remove |
|---------|--------|--------|
| CLI | `yams graph ingest --file <json\|-> [--json]` | `yams graph remove --ns <ns> [--relation <r>] [--dry-run] [--include-foreign-edges] [--json]` |
| MCP tool | `graph`, `action: "ingest"` (update annotation) | `graph_remove` (destructive annotation) |
| MCP composite `execute` | op `graph_ingest` | op `graph_remove` |
| IPC | `KgIngestRequest` (70) | `KgRemoveRequest` (80) |

The composite `query` tool is read-only: it rejects `graph` with
`action: "ingest"` or `"remove"`.

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

- `properties.ns`, when present, must be a non-empty string. The CLI rejects
  the payload otherwise; the daemon refuses the row (`invalid_ns`).
- `document_hash` is not supported. The CLI rejects it, MCP does not advertise
  it, and the daemon ignores the IPC field.
- Nodes upsert by `node_key`. A label or type that is sent replaces the stored
  one; an omitted one keeps it. Properties are replaced as a whole, not merged.
  Because ownership is read from the properties sent, an owner must resend
  `properties.ns` on every upsert: omitting it is an ownership change and is
  refused (`ns_change`).
- Namespaced edges (`properties.ns` set) belong to their owner: an upsert
  replaces weight and properties exactly.
- Un-namespaced edges keep the shared merge rule: weight becomes the max of the
  old and new weight, and properties are replaced only when the new weight is
  not lower. A lower-weight write changes nothing and is counted as skipped.
- `skip_existing_*` (default `false`) leaves existing rows untouched and counts
  them as skipped.
- One request is all or nothing. It runs in a savepoint, and its result is
  reported only after the transaction commits. If the op fails, the
  transaction rolls back or the request is dropped, the result is an error with
  no counts.
- The CLI sends nodes, then edges, then aliases, in requests of at most 2000
  rows. Each request commits on its own. The CLI stops at the first request that
  fails, reports the counts of the requests that committed, and exits non-zero.

## Ownership

A row with a non-empty string `properties.ns` is owned by that namespace. YAMS
writers do not set it. Ingest never changes who owns an existing row. A refused
row is skipped and listed in `ns_conflicts`, with one of three reasons:

| reason | when |
|--------|------|
| `ns_change` | The stored row has a different owner, or none. This covers no owner -> X, X -> Y and X -> no owner. |
| `invalid_ns` | `ns` is present but not a non-empty string (`""`, `5`, `true`, `{}`, `null`). |
| `reserved_key` | A `doc:` or `blob:` node was sent with an `ns`. |

So a caller cannot take over a YAMS node or another tool's node, strip its
`document_hash` or `file_path`, and later delete it with `remove --ns`.

## Result

```json
{"action": "ingest", "success": true, "chunks_committed": 2, "chunks_total": 2,
 "nodes_inserted": 2, "nodes_updated": 0, "nodes_skipped": 1,
 "edges_inserted": 1, "edges_updated": 0, "edges_skipped": 1,
 "aliases_inserted": 0, "aliases_updated": 0, "aliases_skipped": 0,
 "unresolved_edges": [{"src_node_key": "paper:doi:10.1/a",
                       "dst_node_key": "paper:doi:10.1/missing", "relation": "cites",
                       "src_missing": false, "dst_missing": true}],
 "ns_conflicts": [{"kind": "node", "node_key": "doc:abc", "existing_ns": "",
                   "requested_ns": "paperbridge", "reason": "reserved_key",
                   "src_node_key": "", "dst_node_key": "", "relation": ""}],
 "errors": ["Edge destination node not found: paper:doi:10.1/missing",
            "Namespace conflict (reserved_key) on node doc:abc"]}
```

`chunks_*` and a failure `error` appear in CLI output only. Counts are rows,
classified inside the writer transaction:

- **inserted:** a new row.
- **updated:** an existing row was written.
- **skipped:** an unresolved edge, a `skip_existing` hit, an `ns` conflict, a
  lower-weight write to an un-namespaced edge, or a node without a key.

An edge is unresolved when either endpoint does not exist after the request's
nodes are written; it is reported and not written. A repeated row in one
request counts once as inserted and then as updated.

## Cleanup

- **Never removed:** a namespaced row is never removed by document cleanup
  (`deleteNodesForDocumentHash`, `deleteNodesForSourceFile`,
  `deleteEdgesForSourceFile`, document graph cleanup), orphan repair
  (`deleteOrphanedNodes`), relation-wide derived-graph resets such as
  `semantic_neighbor`, or snapshot-version pruning. This holds whatever its
  `document_hash`, `file_path`, `source_file`, `snapshot_id` or relation say.
- **Malformed owners:** an `ns` that is not a non-empty string does not exempt
  a row. Such a row stays subject to cleanup.
- **Reserved relation:** `semantic_neighbor` belongs to YAMS. Topology
  maintenance prunes self-loop `semantic_neighbor` edges by id, whoever owns
  them, so external callers should not use that relation.
- **`doc:` and `blob:` nodes:** these are YAMS-owned. Deleting or re-indexing a
  document removes them and every edge to them, including namespaced edges.
  Link external nodes to documents only if that is acceptable.

## Removal

- **Namespace required:** removal needs a non-empty `ns`. There is no
  relation-only removal.
- **With `relation`:** deletes that relation's edges owned by `ns` and keeps
  the nodes.
- **Without `relation`:** deletes the namespace's edges and nodes, and every
  edge incident to those nodes.
  - Incident edges not owned by `ns` (another owner's, or un-namespaced) are
    reported as `foreign_edges_removed`.
  - If there are any, the removal is refused (`refused: true`) and deletes
    nothing, unless `include_foreign_edges` / `--include-foreign-edges` is set.
  - `edges_removed` includes `foreign_edges_removed`.
- **`dry_run`:** reports the same counts and deletes nothing.

Lookups by namespace use the partial indexes `idx_kg_nodes_ns` and
`idx_kg_edges_ns` (schema v48) on the guarded `properties.ns` expression.

## Compatibility

- **Ingest result fields:**
  - `KgIngestResponse` gained `nodes_updated`, `edges_updated` and
    `unresolved_edges` (protobuf 9-11), and `aliases_updated` and
    `ns_conflicts` (12-13).
  - `nodes_inserted`, `edges_inserted` and `aliases_inserted` now count new
    rows only.
- **Removal fields:** `KgRemoveRequest` gained `include_foreign_edges` (4).
  `KgRemoveResponse` gained `foreign_edges_removed` and `refused` (4-5).
- **`skip_existing_*`:** these defaulted to `true` but were ignored, so every
  ingest upserted. The default is now `false` (same behaviour); `true` now
  skips.
- **Socket transport:** KgIngest had a protobuf schema but no binding and could
  not cross the socket transport; it now can.
- **Separate removal message:** `KgRemoveRequest` is its own message, not an
  ingest flag, so a daemon that predates it rejects the message instead of
  ingesting.
- **MCP:**
  - The `graph` tool no longer removes, and the composite `query` tool no
    longer ingests.
  - Use `graph_remove` and the `execute` ops `graph_ingest` and
    `graph_remove`.
