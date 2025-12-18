#pragma once

#include <yams/core/types.h>

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace yams::metadata {

/**
 * KnowledgeGraphStoreConfig controls caching and behavior of the KG store.
 * The implementation should treat all caches as best-effort and thread-safe for reads.
 */
struct KnowledgeGraphStoreConfig {
    // LRU cache capacities (0 disables the cache)
    std::size_t node_cache_capacity = 10'000;
    std::size_t alias_cache_capacity = 50'000;
    std::size_t embedding_cache_capacity = 10'000;
    std::size_t neighbor_cache_capacity = 10'000;

    // Enable/disable FTS-backed alias lookup (if available)
    bool enable_alias_fts = true;

    // Use WAL mode on connections where applicable
    bool enable_wal = true;

    // Prefer exact alias match before FTS search
    bool prefer_exact_alias_first = true;

    // Default limit guards for unbounded scans
    std::size_t default_limit = 1000;
};

/**
 * Node representation (kg_nodes).
 */
struct KGNode {
    std::int64_t id = 0;                     // PRIMARY KEY (assigned by DB)
    std::string nodeKey;                     // Unique logical key
    std::optional<std::string> label;        // Human-readable name
    std::optional<std::string> type;         // Node type/category
    std::optional<std::int64_t> createdTime; // Unix seconds
    std::optional<std::int64_t> updatedTime; // Unix seconds
    std::optional<std::string> properties;   // JSON properties blob
};

/**
 * Alias representation (kg_aliases).
 */
struct KGAlias {
    std::int64_t id = 0;               // PRIMARY KEY (assigned by DB)
    std::int64_t nodeId = 0;           // REFERENCES kg_nodes(id)
    std::string alias;                 // Surface form
    std::optional<std::string> source; // Origin/system of alias
    float confidence = 1.0f;           // [0,1]
};

/**
 * Edge representation (kg_edges).
 */
struct KGEdge {
    std::int64_t id = 0;                     // PRIMARY KEY (assigned by DB)
    std::int64_t srcNodeId = 0;              // REFERENCES kg_nodes(id)
    std::int64_t dstNodeId = 0;              // REFERENCES kg_nodes(id)
    std::string relation;                    // Relation/predicate
    float weight = 1.0f;                     // Optional weight
    std::optional<std::int64_t> createdTime; // Unix seconds
    std::optional<std::string> properties;   // JSON properties blob
};

/**
 * Node embedding representation (kg_node_embeddings).
 */
struct KGNodeEmbedding {
    std::int64_t nodeId = 0;                 // PRIMARY KEY, REFERENCES kg_nodes(id)
    int dim = 0;                             // Embedding dimension
    std::vector<float> vector;               // Dense vector
    std::optional<std::string> model;        // Model identifier/name
    std::optional<std::int64_t> updatedTime; // Unix seconds
};

/**
 * Document entity annotation (doc_entities).
 */
struct DocEntity {
    std::int64_t id = 0;                     // PRIMARY KEY (assigned by DB)
    std::int64_t documentId = 0;             // REFERENCES documents(id)
    std::string entityText;                  // Surface text from document
    std::optional<std::int64_t> nodeId;      // Linked node (nullable)
    std::optional<std::int64_t> startOffset; // Byte start (optional)
    std::optional<std::int64_t> endOffset;   // Byte end (exclusive, optional)
    std::optional<float> confidence;         // [0,1]
    std::optional<std::string> extractor;    // Which extractor/linker produced this
};

/**
 * Node statistics (kg_node_stats).
 */
struct KGNodeStats {
    std::int64_t nodeId = 0;                   // PRIMARY KEY, REFERENCES kg_nodes(id)
    std::optional<std::int64_t> degree;        // Total degree
    std::optional<float> pagerank;             // Optional PageRank score
    std::optional<std::int64_t> neighborCount; // Redundant with degree in simple graphs
    std::optional<std::int64_t> lastComputed;  // Unix seconds
};

struct PathNodeDescriptor {
    std::string snapshotId;
    std::string path; // Normalized path within snapshot
    std::string rootTreeHash;
    bool isDirectory = false;
};

struct PathHistoryRecord {
    std::string snapshotId;
    std::string path;
    std::string blobHash;
    std::optional<std::int64_t> diffId;
    std::optional<std::string> changeType;
};

/**
 * Alias resolution result item, optionally carrying a score (e.g., from FTS).
 */
struct AliasResolution {
    std::int64_t nodeId = 0;
    float score = 1.0f; // 1.0 for exact, else FTS-derived or heuristic
};

/**
 * KnowledgeGraphStore defines the abstract API for reading/writing the local-first KG.
 * Implementations must be thread-safe for concurrent read access; writes should be serialized.
 */
class KnowledgeGraphStore {
public:
    virtual ~KnowledgeGraphStore() = default;

    // Configuration
    virtual void setConfig(const KnowledgeGraphStoreConfig& cfg) = 0;
    virtual const KnowledgeGraphStoreConfig& getConfig() const = 0;

    // -----------------------------------------------------------------------------
    // Nodes
    // -----------------------------------------------------------------------------

    // Insert a new node; if nodeKey exists, update mutable fields (label/type/properties).
    // Returns the node id.
    virtual Result<std::int64_t> upsertNode(const KGNode& node) = 0;

    // Bulk upsert. Returns vector of assigned/located ids by input order.
    virtual Result<std::vector<std::int64_t>> upsertNodes(const std::vector<KGNode>& nodes) = 0;

    virtual Result<std::optional<KGNode>> getNodeById(std::int64_t nodeId) = 0;
    virtual Result<std::optional<KGNode>> getNodeByKey(std::string_view nodeKey) = 0;

    // Batch node retrieval: returns nodes by IDs in a single query
    // More efficient than calling getNodeById N times for BFS hydration
    virtual Result<std::vector<KGNode>> getNodesByIds(const std::vector<std::int64_t>& nodeIds) = 0;

    // Simple scans/filters
    virtual Result<std::vector<KGNode>>
    findNodesByType(std::string_view type, std::size_t limit = 100, std::size_t offset = 0) = 0;

    // Delete node (cascades to aliases/edges/embeddings/doc_entities as per schema)
    virtual Result<void> deleteNodeById(std::int64_t nodeId) = 0;

    // -----------------------------------------------------------------------------
    // Aliases
    // -----------------------------------------------------------------------------

    // Create alias; UNIQUE(node_id, alias) enforced by schema
    virtual Result<std::int64_t> addAlias(const KGAlias& alias) = 0;

    // Bulk insert aliases (best-effort; may short-circuit on first error)
    virtual Result<void> addAliases(const std::vector<KGAlias>& aliases) = 0;

    // Exact alias resolution (case sensitivity depends on storage collation)
    virtual Result<std::vector<AliasResolution>> resolveAliasExact(std::string_view alias,
                                                                   std::size_t limit = 50) = 0;

    // FTS-assisted alias resolution when enabled; returns node ids with scores
    virtual Result<std::vector<AliasResolution>> resolveAliasFuzzy(std::string_view aliasQuery,
                                                                   std::size_t limit = 50) = 0;

    // Remove specific alias or all aliases for a node
    virtual Result<void> removeAliasById(std::int64_t aliasId) = 0;
    virtual Result<void> removeAliasesForNode(std::int64_t nodeId) = 0;

    // -----------------------------------------------------------------------------
    // Edges
    // -----------------------------------------------------------------------------

    // Insert a new edge (no implicit de-duplication beyond schema constraints).
    virtual Result<std::int64_t> addEdge(const KGEdge& edge) = 0;

    // Bulk edges
    virtual Result<void> addEdges(const std::vector<KGEdge>& edges) = 0;

    // Bulk edges with de-duplication: do not insert edges that already exist with the same
    // (src_node_id, dst_node_id, relation). Implementations should perform this efficiently
    // (e.g., INSERT ... SELECT ... WHERE NOT EXISTS) wrapped in a transaction.
    virtual Result<void> addEdgesUnique(const std::vector<KGEdge>& edges) = 0;

    // Adjacency queries
    virtual Result<std::vector<KGEdge>>
    getEdgesFrom(std::int64_t srcNodeId, std::optional<std::string_view> relation = std::nullopt,
                 std::size_t limit = 200, std::size_t offset = 0) = 0;

    virtual Result<std::vector<KGEdge>>
    getEdgesTo(std::int64_t dstNodeId, std::optional<std::string_view> relation = std::nullopt,
               std::size_t limit = 200, std::size_t offset = 0) = 0;

    // Bidirectional edges: returns both incoming and outgoing edges in a single query
    // More efficient than calling getEdgesFrom + getEdgesTo separately for BFS traversal
    virtual Result<std::vector<KGEdge>>
    getEdgesBidirectional(std::int64_t nodeId, std::optional<std::string_view> relation = std::nullopt,
                          std::size_t limit = 400) = 0;

    // For quick structural scoring: neighbor ids only (fast path)
    virtual Result<std::vector<std::int64_t>> neighbors(std::int64_t nodeId,
                                                        std::size_t maxNeighbors = 256) = 0;

    // Delete an edge
    virtual Result<void> removeEdgeById(std::int64_t edgeId) = 0;

    // -----------------------------------------------------------------------------
    // Embeddings
    // -----------------------------------------------------------------------------

    // Upsert node embedding (replace if exists)
    virtual Result<void> setNodeEmbedding(const KGNodeEmbedding& embedding) = 0;

    // Retrieve embedding (optional)
    virtual Result<std::optional<KGNodeEmbedding>> getNodeEmbedding(std::int64_t nodeId) = 0;

    // Delete embedding
    virtual Result<void> deleteNodeEmbedding(std::int64_t nodeId) = 0;

    // -----------------------------------------------------------------------------
    // Document Entities
    // -----------------------------------------------------------------------------

    // Add (or replace) document entities for a document; common pattern is:
    //   deleteDocEntitiesForDocument(docId); addDocEntities(docId, batch);
    virtual Result<void> addDocEntities(const std::vector<DocEntity>& entities) = 0;

    // Query entities by document
    // Document id resolution helpers
    virtual Result<std::optional<std::int64_t>> getDocumentIdByHash(std::string_view sha256) = 0;
    virtual Result<std::optional<std::int64_t>> getDocumentIdByPath(std::string_view file_path) = 0;
    virtual Result<std::optional<std::int64_t>> getDocumentIdByName(std::string_view file_name) = 0;
    virtual Result<std::optional<std::string>> getDocumentHashById(std::int64_t documentId) = 0;
    virtual Result<std::vector<DocEntity>> getDocEntitiesForDocument(std::int64_t documentId,
                                                                     std::size_t limit = 1000,
                                                                     std::size_t offset = 0) = 0;

    // Remove all entities for a document
    virtual Result<void> deleteDocEntitiesForDocument(std::int64_t documentId) = 0;

    // -----------------------------------------------------------------------------
    // Document/File Cleanup (for cascade on delete and re-indexing)
    // -----------------------------------------------------------------------------

    // Delete KG nodes associated with a document hash (for cascade cleanup on document deletion).
    // This deletes the doc:<hash> node and any symbol nodes with matching document_hash property.
    // Returns the count of nodes deleted.
    virtual Result<std::int64_t> deleteNodesForDocumentHash(std::string_view documentHash) = 0;

    // Delete all edges where properties.source_file matches the given path.
    // Used to clean up stale relationships when re-indexing a file.
    // Returns the count of edges deleted.
    virtual Result<std::int64_t> deleteEdgesForSourceFile(std::string_view filePath) = 0;

    // Find isolated nodes of a given type (nodes with no incoming edges of specified relation).
    // More efficient than N+1 queries - single SQL query with LEFT JOIN.
    virtual Result<std::vector<KGNode>> findIsolatedNodes(std::string_view nodeType,
                                                          std::string_view relation,
                                                          std::size_t limit = 1000) = 0;

    // -----------------------------------------------------------------------------
    // Statistics
    // -----------------------------------------------------------------------------

    virtual Result<std::optional<KGNodeStats>> getNodeStats(std::int64_t nodeId) = 0;
    virtual Result<void> setNodeStats(const KGNodeStats& stats) = 0;

    // Convenience utility to recompute and persist common stats for a node
    virtual Result<KGNodeStats> recomputeNodeStats(std::int64_t nodeId) = 0;

    // -----------------------------------------------------------------------------
    // Tree diff helpers (PBI-043)
    // -----------------------------------------------------------------------------

    virtual Result<std::int64_t> ensureBlobNode(std::string_view sha256) = 0;
    virtual Result<std::int64_t> ensurePathNode(const PathNodeDescriptor& descriptor) = 0;
    virtual Result<void> linkPathVersion(std::int64_t pathNodeId, std::int64_t blobNodeId,
                                         std::int64_t diffId) = 0;
    virtual Result<void> recordRenameEdge(std::int64_t fromPathNodeId, std::int64_t toPathNodeId,
                                          std::int64_t diffId) = 0;
    virtual Result<std::vector<PathHistoryRecord>> fetchPathHistory(std::string_view logicalPath,
                                                                    std::size_t limit = 100) = 0;

    // -----------------------------------------------------------------------------
    // Maintenance / Utilities
    // -----------------------------------------------------------------------------

    // Optimize internal indexes/tables if applicable
    virtual Result<void> optimize() = 0;

    // Clear in-memory caches (does not affect persistent data)
    virtual void clearCaches() = 0;

    // Health check (e.g., PRAGMA integrity_check in SQLite backends)
    virtual Result<void> healthCheck() = 0;
};

/**
 * Factory helpers (SQLite-backed implementation).
 *
 * These factories are declared here for convenience; their definitions live in the
 * corresponding implementation unit. They will create a new KnowledgeGraphStore
 * bound to the provided configuration and storage path/pool.
 */
class ConnectionPool; // forward declaration

// Create a SQLite-backed store using a file path. The database will be opened/configured
// internally according to the provided config.
Result<std::unique_ptr<KnowledgeGraphStore>>
makeSqliteKnowledgeGraphStore(const std::string& dbPath, const KnowledgeGraphStoreConfig& cfg = {});

// Create a SQLite-backed store that uses an existing ConnectionPool for connections.
// The store does not own the pool.
Result<std::unique_ptr<KnowledgeGraphStore>>
makeSqliteKnowledgeGraphStore(ConnectionPool& pool, const KnowledgeGraphStoreConfig& cfg = {});

} // namespace yams::metadata
