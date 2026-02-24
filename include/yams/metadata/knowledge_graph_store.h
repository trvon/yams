#pragma once

#include <yams/core/types.h>

#include <cstdint>
#include <memory>
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

struct GraphVersionPruneConfig {
    std::size_t keepLatestPerCanonical = 3;
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
 * Symbol extraction state for a document.
 * Tracks whether extraction was performed and by which extractor version,
 * enabling versioned dedupe (skip if already extracted with same version).
 */
struct SymbolExtractionState {
    std::int64_t documentId = 0;
    std::string extractorId;                        // e.g., "symbol_extractor_treesitter:v1"
    std::optional<std::string> extractorConfigHash; // hash of grammar/config versions
    std::int64_t extractedAt = 0;                   // Unix timestamp
    std::string status = "complete";                // complete|failed|pending
    std::int64_t entityCount = 0;                   // number of symbols extracted
    std::optional<std::string> errorMessage;
};

/**
 * Symbol metadata record for fast SQL-based filtering and lookup.
 * Populated alongside kg_nodes and entity_vectors during symbol extraction.
 * Provides indexed access to symbol attributes without JSON parsing.
 */
struct SymbolMetadata {
    std::int64_t symbolId = 0;                // Auto-generated primary key
    std::string documentHash;                 // Links to documents.sha256_hash
    std::string filePath;                     // Source file path
    std::string symbolName;                   // Simple symbol name (e.g., "processTask")
    std::string qualifiedName;                // Fully qualified (e.g., "yams::daemon::processTask")
    std::string kind;                         // "function", "class", "method", "variable", etc.
    std::optional<std::int32_t> startLine;    // 1-based line number
    std::optional<std::int32_t> endLine;      // 1-based line number
    std::optional<std::int32_t> startOffset;  // Byte offset from file start
    std::optional<std::int32_t> endOffset;    // Byte offset from file start
    std::optional<std::string> returnType;    // Return type for functions/methods
    std::optional<std::string> parameters;    // JSON array of parameter types
    std::optional<std::string> documentation; // Extracted docstring/comment
};

/**
 * KnowledgeGraphStore defines the abstract API for reading/writing the local-first KG.
 * Implementations must be thread-safe for concurrent read access; writes should be serialized.
 */
class KnowledgeGraphStore {
public:
    class WriteBatch {
    public:
        virtual ~WriteBatch() = default;
        virtual Result<std::int64_t> upsertNode(const KGNode& node) = 0;
        virtual Result<std::vector<std::int64_t>> upsertNodes(const std::vector<KGNode>& nodes) = 0;
        virtual Result<std::int64_t> addEdge(const KGEdge& edge) = 0;
        virtual Result<void> addEdgesUnique(const std::vector<KGEdge>& edges) = 0;
        virtual Result<void> addAliases(const std::vector<KGAlias>& aliases) = 0;
        virtual Result<void> addDocEntities(const std::vector<DocEntity>& entities) = 0;
        virtual Result<void> deleteDocEntitiesForDocument(std::int64_t documentId) = 0;
        virtual Result<std::int64_t> deleteEdgesForSourceFile(std::string_view filePath) = 0;
        virtual Result<void> upsertSymbolMetadata(const std::vector<SymbolMetadata>& symbols) = 0;
        virtual Result<void> commit() = 0;
    };

    virtual ~KnowledgeGraphStore() = default;

    // Configuration
    virtual void setConfig(const KnowledgeGraphStoreConfig& cfg) = 0;
    virtual const KnowledgeGraphStoreConfig& getConfig() const = 0;

    virtual Result<std::unique_ptr<WriteBatch>> beginWriteBatch() = 0;

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
    virtual Result<std::size_t> countNodesByType(std::string_view type) = 0;

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

    // Batch version of getEdgesTo: fetch edges for multiple destination nodes in one query
    // Returns a map from destination node ID to its incoming edges
    // Much more efficient than calling getEdgesTo N times (eliminates N+1 query problem)
    virtual Result<std::unordered_map<std::int64_t, std::vector<KGEdge>>>
    getEdgesToBatch(const std::vector<std::int64_t>& dstNodeIds,
                    std::optional<std::string_view> relation = std::nullopt,
                    std::size_t limitPerNode = 10) = 0;

    // Bidirectional edges: returns both incoming and outgoing edges in a single query
    // More efficient than calling getEdgesFrom + getEdgesTo separately for BFS traversal
    virtual Result<std::vector<KGEdge>>
    getEdgesBidirectional(std::int64_t nodeId,
                          std::optional<std::string_view> relation = std::nullopt,
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
    // Symbol Extraction State (versioned dedupe)
    // -----------------------------------------------------------------------------

    // Get symbol extraction state for a document by its hash.
    // Returns nullopt if no extraction has been recorded.
    virtual Result<std::optional<SymbolExtractionState>>
    getSymbolExtractionState(std::string_view documentHash) = 0;

    // Upsert symbol extraction state. Uses document hash to resolve document_id internally.
    // If extraction already recorded, updates the row; otherwise inserts.
    virtual Result<void> upsertSymbolExtractionState(std::string_view documentHash,
                                                     const SymbolExtractionState& state) = 0;

    // -----------------------------------------------------------------------------
    // Symbol Metadata (fast SQL-indexed symbol lookup)
    // -----------------------------------------------------------------------------

    // Upsert symbol metadata records for a document. Replaces existing records for the document.
    // Called after symbol extraction to populate the indexed symbol_metadata table.
    virtual Result<void> upsertSymbolMetadata(const std::vector<SymbolMetadata>& symbols) = 0;

    // Delete all symbol metadata for a document hash (for re-extraction or document deletion).
    virtual Result<std::int64_t> deleteSymbolMetadataForDocument(std::string_view documentHash) = 0;

    // Query symbol metadata by various criteria. Returns matching symbols.
    // Useful for pre-filtering before expensive vector search.
    virtual Result<std::vector<SymbolMetadata>>
    querySymbolMetadata(std::optional<std::string_view> filePath = std::nullopt,
                        std::optional<std::string_view> kind = std::nullopt,
                        std::optional<std::string_view> namePattern = std::nullopt,
                        std::size_t limit = 100, std::size_t offset = 0) = 0;

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

    // Delete edges that reference missing nodes. Returns the count of edges deleted.
    virtual Result<std::int64_t> deleteOrphanedEdges() = 0;

    // Delete doc_entities rows referencing missing documents. Returns the count deleted.
    virtual Result<std::int64_t> deleteOrphanedDocEntities() = 0;

    // Find isolated nodes of a given type (nodes with no incoming edges of specified relation).
    // More efficient than N+1 queries - single SQL query with LEFT JOIN.
    virtual Result<std::vector<KGNode>> findIsolatedNodes(std::string_view nodeType,
                                                          std::string_view relation,
                                                          std::size_t limit = 1000) = 0;

    // Get all distinct node types with their counts, ordered by count descending.
    // Used for node type discovery in CLI (e.g., `yams graph --list-types`).
    virtual Result<std::vector<std::pair<std::string, std::size_t>>> getNodeTypeCounts() = 0;

    // Get all distinct relation types with their counts, ordered by count descending.
    // Used for relation type discovery in CLI (e.g., `yams graph --relations`).
    virtual Result<std::vector<std::pair<std::string, std::size_t>>> getRelationTypeCounts() = 0;

    // Search nodes by label using LIKE pattern matching.
    // Pattern uses SQL LIKE wildcards: % for any sequence, _ for single char.
    // Returns nodes with labels matching the pattern.
    virtual Result<std::vector<KGNode>> searchNodesByLabel(std::string_view pattern,
                                                           std::size_t limit = 100,
                                                           std::size_t offset = 0) = 0;

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

    // Prune versioned nodes (nodes with properties.snapshot_id) to keep only the
    // latest N versions per canonical key. Returns number of nodes deleted.
    virtual Result<std::int64_t> pruneVersionNodes(const GraphVersionPruneConfig& cfg) = 0;

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
