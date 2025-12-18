#pragma once

#include <yams/core/types.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace yams::app::services {

/**
 * GraphQueryService: Unified service for graph connectivity queries.
 *
 * PBI-009: Provides reusable APIs for:
 * - Resolving documents/snapshots/paths into KG node IDs
 * - Traversing relations (same_content, renamed_from, directory containment, symbol references)
 * - Returning hydrated metadata with snapshot context
 *
 * Design principles:
 * - Depth-limited traversal (default=1, max=4) to prevent expensive queries
 * - Pagination support for large result sets
 * - Graceful degradation when KG is unavailable
 * - Batch operations where possible to reduce MetadataRepository overhead
 */

// ===========================
// Request/Response DTOs
// ===========================

enum class GraphRelationType {
    All,             // All relation types
    SameContent,     // Documents with identical content (blob edges)
    RenamedFrom,     // Rename/move history
    RenamedTo,       // Forward rename edges
    DirectoryChild,  // Directory containment
    SymbolReference, // Symbol/entity cross-references
    PathVersion,     // Path node to blob version edges
};

struct GraphNodeDescriptor {
    std::int64_t nodeId{0};
    std::string nodeKey;
    std::optional<std::string> label;
    std::optional<std::string> type;
};

struct GraphNodeMetadata {
    GraphNodeDescriptor node;

    // Document metadata (when node represents a document/blob)
    std::optional<std::string> documentHash;
    std::optional<std::string> documentPath;
    std::optional<std::int64_t> documentId;
    std::vector<std::string> tags;
    std::unordered_map<std::string, std::string> metadata;

    // Snapshot context (when applicable)
    std::optional<std::string> snapshotId;
    std::optional<std::string> snapshotLabel;
    std::optional<std::int64_t> snapshotTime;

    // Path node specifics (for tree diff nodes)
    std::optional<std::string> normalizedPath;
    std::optional<bool> isDirectory;
    std::optional<std::string> rootTreeHash;
};

struct GraphEdgeDescriptor {
    std::int64_t edgeId{0};
    std::int64_t srcNodeId{0};
    std::int64_t dstNodeId{0};
    std::string relation;
    float weight{1.0f};
    std::optional<std::string> properties; // JSON properties blob
};

struct GraphConnectedNode {
    GraphNodeMetadata nodeMetadata;
    std::vector<GraphEdgeDescriptor> connectingEdges; // Edges that led to this node
    int distance{0};                                  // Distance from query origin (BFS depth)
    double relevanceScore{0.0};                       // Optional scoring for ranking
};

struct GraphQueryRequest {
    // Target identification (exactly one required)
    std::optional<std::string> documentHash; // Full or partial hash
    std::optional<std::string> documentName; // Document name/path
    std::optional<std::string> snapshotId;   // Snapshot identifier
    std::optional<std::int64_t> nodeId;      // Direct KG node ID

    // Traversal options
    std::vector<GraphRelationType> relationFilters; // Empty = all relations
    int maxDepth{1};                                // BFS depth limit (1-4)
    std::size_t maxResults{200};                    // Total result cap
    std::size_t maxResultsPerDepth{100};            // Per-depth cap
    bool reverseTraversal{false};                   // Traverse incoming edges instead of outgoing

    // Snapshot context (scope results to specific snapshot)
    std::optional<std::string> scopeToSnapshot;

    // Pagination
    std::size_t offset{0};
    std::size_t limit{100};

    // Output control
    bool includeEdgeProperties{false}; // Include full edge property JSON
    bool includeNodeProperties{false}; // Include full node property JSON
    bool hydrateFully{true};           // Fetch full document metadata (slower)
};

struct GraphQueryResponse {
    // Origin node
    GraphNodeMetadata originNode;

    // Connected nodes (grouped by distance)
    std::unordered_map<int, std::vector<GraphConnectedNode>> nodesByDistance;

    // Flattened results for simple iteration
    std::vector<GraphConnectedNode> allConnectedNodes;

    // Statistics
    std::size_t totalNodesFound{0};
    std::size_t totalEdgesTraversed{0};
    bool truncated{false};       // True if results were capped
    int maxDepthReached{0};      // Actual max depth reached
    std::int64_t queryTimeMs{0}; // Execution time

    // Error/warning info
    bool kgAvailable{true};
    std::optional<std::string> warning;
};

// Snapshot listing support
struct SnapshotSummary {
    std::string snapshotId;
    std::string snapshotLabel;
    std::int64_t timestamp{0};
    std::uint64_t fileCount{0};
    std::uint64_t totalBytes{0};
    std::optional<std::int64_t> lastModified;

    // Top connected nodes (directories, renamed files)
    std::vector<std::string> topDirectories;
    std::vector<std::string> topRenamedFiles;
    std::size_t totalRenames{0};
};

struct ListSnapshotsRequest {
    std::size_t limit{50};
    std::size_t offset{0};
    bool includeSummary{true};     // Include file counts, top nodes
    std::size_t topNodesLimit{10}; // Number of top nodes to return
};

struct ListSnapshotsResponse {
    std::vector<SnapshotSummary> snapshots;
    std::size_t totalSnapshots{0};
    bool hasMore{false};
};

// Path history support (rename chains)
struct PathHistoryEntry {
    std::string path;
    std::string snapshotId;
    std::optional<std::string> blobHash;
    std::optional<std::string> changeType; // "added", "modified", "renamed", "deleted"
    std::optional<std::int64_t> diffId;
    std::optional<std::int64_t> timestamp;
};

struct PathHistoryRequest {
    std::string path; // Logical path to query
    std::size_t limit{100};
};

struct PathHistoryResponse {
    std::string queryPath;
    std::vector<PathHistoryEntry> history;
    bool hasMore{false};
};

// ===========================
// Service Interface
// ===========================

class IGraphQueryService {
public:
    virtual ~IGraphQueryService() = default;

    /**
     * Perform a graph connectivity query.
     * Returns connected nodes within the specified depth and relation filters.
     */
    virtual Result<GraphQueryResponse> query(const GraphQueryRequest& req) = 0;

    /**
     * List snapshots with optional summaries.
     */
    virtual Result<ListSnapshotsResponse> listSnapshots(const ListSnapshotsRequest& req) = 0;

    /**
     * Fetch rename/version history for a specific path.
     */
    virtual Result<PathHistoryResponse> getPathHistory(const PathHistoryRequest& req) = 0;

    /**
     * Resolve a document hash/name/snapshot to a KG node ID.
     * Returns nullopt if resolution fails or KG is unavailable.
     */
    virtual Result<std::optional<std::int64_t>>
    resolveToNodeId(const std::string& hashOrNameOrSnapshot) = 0;
};

/**
 * Factory function to create the graph query service.
 * Returns nullptr if required dependencies (KG store, metadata repo) are unavailable.
 */
std::shared_ptr<IGraphQueryService>
makeGraphQueryService(std::shared_ptr<metadata::KnowledgeGraphStore> kgStore,
                      std::shared_ptr<metadata::MetadataRepository> metadataRepo);

} // namespace yams::app::services
