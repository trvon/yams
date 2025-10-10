# Path Tree Builder Helper Notes

Sketch helper signatures needed for PBI 051 path-tree prototype.

- `PathTreeNode` struct (ids, parentId sentinel, pathSegment, fullPath, docCount, centroidWeight).
- Repository helpers (likely on `MetadataRepository` or a facade):
  - `std::optional<PathTreeNode> findPathTreeNode(int64_t parentId, std::string_view segment);`
  - `Result<PathTreeNode> insertPathTreeNode(int64_t parentId, std::string_view segment, std::string_view fullPath);`
  - `Result<void> incrementPathTreeDocCount(int64_t nodeId, int64_t documentId);`
  - `Result<void> accumulatePathTreeCentroid(int64_t nodeId, std::span<const float> embedding);`
- `MetadataRepository::upsertPathTreeForDocument` orchestrates the above to maintain the tree for a document (doc counts + optional centroid).
- Ensure helpers participate in existing ingestion transactions to remain atomic with document metadata writes.
- Consider locating these in a dedicated `PathTreeRepository` if `MetadataRepository` is already saturated.
- Metadata scaffolding status:
  - Interface declarations and initial implementations live in `metadata_repository.h/.cpp`; storage writes/reads now persist via migration v15.
  - Follow-up: integrate ingestion wiring and refine centroid aggregation/rehydration semantics.

- Schema support: migration v15 (`createPathTreeSchema`) creates `path_tree_nodes` and `path_tree_node_documents` tables with supporting indexes.
