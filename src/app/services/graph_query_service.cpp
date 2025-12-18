#include <yams/app/services/graph_query_service.hpp>

#include <algorithm>
#include <chrono>
#include <queue>
#include <unordered_set>

#include <spdlog/spdlog.h>

namespace yams::app::services {

namespace {

// Helper: Convert GraphRelationType enum to KG relation string
std::vector<std::string> relationTypeToStrings(GraphRelationType type) {
    switch (type) {
        case GraphRelationType::All:
            return {}; // Empty = no filter
        case GraphRelationType::SameContent:
            return {"same_content", "blob_version"};
        case GraphRelationType::RenamedFrom:
            return {"renamed_from", "moved_from"};
        case GraphRelationType::RenamedTo:
            return {"renamed_to", "moved_to"};
        case GraphRelationType::DirectoryChild:
            return {"contains", "directory_child"};
        case GraphRelationType::SymbolReference:
            return {"symbol_reference", "entity_reference"};
        case GraphRelationType::PathVersion:
            return {"path_version", "blob_at_path"};
        default:
            return {};
    }
}

// Helper: Check if an edge matches the relation filters
bool matchesRelationFilter(const metadata::KGEdge& edge,
                           const std::vector<GraphRelationType>& filters) {
    if (filters.empty()) {
        return true; // No filter = all edges match
    }

    for (const auto& filterType : filters) {
        auto allowedRelations = relationTypeToStrings(filterType);
        if (allowedRelations.empty()) {
            return true; // "All" type matches everything
        }

        for (const auto& allowed : allowedRelations) {
            if (edge.relation == allowed) {
                return true;
            }
        }
    }
    return false;
}

} // anonymous namespace

// ===========================
// GraphQueryService Implementation
// ===========================

class GraphQueryService : public IGraphQueryService {
public:
    GraphQueryService(std::shared_ptr<metadata::KnowledgeGraphStore> kgStore,
                      std::shared_ptr<metadata::MetadataRepository> metadataRepo)
        : kgStore_(std::move(kgStore)), metadataRepo_(std::move(metadataRepo)) {}

    Result<GraphQueryResponse> query(const GraphQueryRequest& req) override {
        auto start = std::chrono::steady_clock::now();

        GraphQueryResponse response;
        response.kgAvailable = (kgStore_ != nullptr);

        if (!kgStore_ || !metadataRepo_) {
            response.warning = "Knowledge graph store not available";
            return response;
        }

        // Step 1: Resolve origin node
        auto originNodeIdResult = resolveOriginNode(req);
        if (!originNodeIdResult) {
            return Result<GraphQueryResponse>(originNodeIdResult.error());
        }

        if (!originNodeIdResult.value().has_value()) {
            response.warning = "Could not resolve origin node";
            return response;
        }

        std::int64_t originNodeId = originNodeIdResult.value().value();

        // Step 2: Hydrate origin node metadata
        auto originMetadataResult = hydrateNodeMetadata(originNodeId, req.hydrateFully);
        if (!originMetadataResult) {
            return Result<GraphQueryResponse>(originMetadataResult.error());
        }
        response.originNode = std::move(originMetadataResult.value());

        // Step 3: Perform BFS traversal
        auto traversalResult = performBFSTraversal(originNodeId, req, response);
        if (!traversalResult) {
            return Result<GraphQueryResponse>(traversalResult.error());
        }

        // Step 4: Calculate metrics
        auto end = std::chrono::steady_clock::now();
        response.queryTimeMs =
            std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

        return response;
    }

    Result<ListSnapshotsResponse> listSnapshots(const ListSnapshotsRequest& req) override {
        ListSnapshotsResponse response;

        if (!metadataRepo_) {
            return Result<ListSnapshotsResponse>(
                Error(ErrorCode::InvalidState, "Metadata repository not available"));
        }

        // Query metadata repository for snapshots
        // Note: This assumes MetadataRepository has snapshot listing support
        // If not, we'll need to add that capability

        // Placeholder implementation - will be completed when MetadataRepository snapshot API is
        // confirmed
        response.totalSnapshots = 0;
        response.hasMore = false;

        spdlog::warn("GraphQueryService::listSnapshots: Not yet fully implemented (PBI-009)");

        return response;
    }

    Result<PathHistoryResponse> getPathHistory(const PathHistoryRequest& req) override {
        PathHistoryResponse response;
        response.queryPath = req.path;

        if (!kgStore_) {
            return Result<PathHistoryResponse>(
                Error(ErrorCode::InvalidState, "Knowledge graph store not available"));
        }

        // Use KG store's fetchPathHistory helper
        auto historyResult = kgStore_->fetchPathHistory(req.path, req.limit);
        if (!historyResult) {
            return Result<PathHistoryResponse>(historyResult.error());
        }

        // Convert KG history records to service DTOs
        for (const auto& record : historyResult.value()) {
            PathHistoryEntry entry;
            entry.path = record.path;
            entry.snapshotId = record.snapshotId;
            entry.blobHash = record.blobHash;
            entry.changeType = record.changeType;
            if (record.diffId.has_value()) {
                entry.diffId = record.diffId.value();
            }
            // Timestamp would need to be extracted from snapshot metadata
            response.history.push_back(std::move(entry));
        }

        response.hasMore = (response.history.size() >= req.limit);

        return response;
    }

    Result<std::optional<std::int64_t>>
    resolveToNodeId(const std::string& hashOrNameOrSnapshot) override {
        if (!kgStore_ || !metadataRepo_) {
            return std::optional<std::int64_t>{}; // Graceful failure
        }

        // Try document hash first
        auto docIdResult = kgStore_->getDocumentIdByHash(hashOrNameOrSnapshot);
        if (docIdResult && docIdResult.value().has_value()) {
            // Found document, now find its blob node
            std::string nodeKey = "blob:" + hashOrNameOrSnapshot;
            auto nodeResult = kgStore_->getNodeByKey(nodeKey);
            if (nodeResult && nodeResult.value().has_value()) {
                return std::optional<std::int64_t>{nodeResult.value()->id};
            }
        }

        // Try document name/path
        auto nameDocIdResult = kgStore_->getDocumentIdByPath(hashOrNameOrSnapshot);
        if (!nameDocIdResult) {
            nameDocIdResult = kgStore_->getDocumentIdByName(hashOrNameOrSnapshot);
        }

        if (nameDocIdResult && nameDocIdResult.value().has_value()) {
            // Get document hash then blob node
            auto hashResult = kgStore_->getDocumentHashById(nameDocIdResult.value().value());
            if (hashResult && hashResult.value().has_value()) {
                std::string nodeKey = "blob:" + hashResult.value().value();
                auto nodeResult = kgStore_->getNodeByKey(nodeKey);
                if (nodeResult && nodeResult.value().has_value()) {
                    return std::optional<std::int64_t>{nodeResult.value()->id};
                }
            }
        }

        // Try as snapshot ID (node key would be "snapshot:ID")
        std::string snapshotNodeKey = "snapshot:" + hashOrNameOrSnapshot;
        auto snapshotNodeResult = kgStore_->getNodeByKey(snapshotNodeKey);
        if (snapshotNodeResult && snapshotNodeResult.value().has_value()) {
            return std::optional<std::int64_t>{snapshotNodeResult.value()->id};
        }

        return std::optional<std::int64_t>{}; // Not found
    }

private:
    // Resolve the origin node from request parameters
    Result<std::optional<std::int64_t>> resolveOriginNode(const GraphQueryRequest& req) {
        // Direct node ID takes precedence
        if (req.nodeId.has_value()) {
            return req.nodeId;
        }

        // Document hash
        if (req.documentHash.has_value()) {
            return resolveToNodeId(req.documentHash.value());
        }

        // Document name
        if (req.documentName.has_value()) {
            return resolveToNodeId(req.documentName.value());
        }

        // Snapshot ID
        if (req.snapshotId.has_value()) {
            return resolveToNodeId(req.snapshotId.value());
        }

        return Result<std::optional<std::int64_t>>(Error(
            ErrorCode::InvalidArgument,
            "No valid origin specified (need nodeId, documentHash, documentName, or snapshotId)"));
    }

    // Hydrate full metadata for a KG node
    Result<GraphNodeMetadata> hydrateNodeMetadata(std::int64_t nodeId, bool hydrateFully) {
        GraphNodeMetadata metadata;

        // Get base node info
        auto nodeResult = kgStore_->getNodeById(nodeId);
        if (!nodeResult) {
            return Result<GraphNodeMetadata>(nodeResult.error());
        }

        if (!nodeResult.value().has_value()) {
            return Result<GraphNodeMetadata>(
                Error(ErrorCode::NotFound, "Node not found: " + std::to_string(nodeId)));
        }

        const auto& node = nodeResult.value().value();
        metadata.node.nodeId = node.id;
        metadata.node.nodeKey = node.nodeKey;
        metadata.node.label = node.label;
        metadata.node.type = node.type;

        if (!hydrateFully) {
            return metadata;
        }

        // Extract node type-specific metadata
        if (node.nodeKey.starts_with("blob:")) {
            // Blob node - extract hash and find document(s)
            std::string hash = node.nodeKey.substr(5); // Remove "blob:" prefix
            metadata.documentHash = hash;

            auto docIdResult = kgStore_->getDocumentIdByHash(hash);
            if (docIdResult && docIdResult.value().has_value()) {
                metadata.documentId = docIdResult.value().value();

                // Get document metadata from repository
                if (metadataRepo_) {
                    auto docResult = metadataRepo_->getDocumentByHash(hash);
                    if (docResult && docResult.value().has_value()) {
                        const auto& doc = docResult.value().value();
                        metadata.documentPath = doc.filePath;

                        // Tags are stored separately in metadata, not in DocumentInfo
                        // For now, leave tags empty - they would need to be fetched via
                        // a separate query if MetadataRepository exposes that API
                        // TODO: Add tag fetching when API is available
                    }
                }
            }
        } else if (node.nodeKey.starts_with("path:")) {
            // Path node - extract snapshot and path info
            // Format: "path:snapshotId:normalized_path"
            auto firstColon = node.nodeKey.find(':', 5);
            if (firstColon != std::string::npos) {
                metadata.snapshotId = node.nodeKey.substr(5, firstColon - 5);
                metadata.normalizedPath = node.nodeKey.substr(firstColon + 1);
            }

            // Check if it's a directory from properties
            if (node.properties.has_value()) {
                // Parse JSON to extract isDirectory (simplified - would use JSON library in
                // production)
                if (node.properties.value().find("\"isDirectory\":true") != std::string::npos) {
                    metadata.isDirectory = true;
                } else {
                    metadata.isDirectory = false;
                }
            }
        } else if (node.nodeKey.starts_with("snapshot:")) {
            // Snapshot node
            metadata.snapshotId = node.nodeKey.substr(9);
            if (node.label.has_value()) {
                metadata.snapshotLabel = node.label.value();
            }
        }

        return metadata;
    }

    // Edge cache type: nodeId -> vector of all edges (both incoming and outgoing)
    using EdgeCache = std::unordered_map<std::int64_t, std::vector<metadata::KGEdge>>;

    // Collect neighbors and cache edges for later reuse
    void collectNeighborsWithCache(std::int64_t nodeId, int distance, int maxDepth,
                                   const std::vector<GraphRelationType>& relationFilters,
                                   std::unordered_set<std::int64_t>& visited,
                                   std::queue<std::pair<std::int64_t, int>>& queue,
                                   EdgeCache& edgeCache, bool reverseTraversal = false) {
        // Use bidirectional query to get all edges in one call (optimization: reduces 2 queries to 1)
        auto allEdges = kgStore_->getEdgesBidirectional(nodeId);
        if (!allEdges) {
            return;
        }

        // Cache edges for this node (used later for connectingEdges)
        edgeCache[nodeId] = allEdges.value();

        // Normal traversal: follow outgoing edges (A calls B -> traverse to B)
        // Reverse traversal: follow incoming edges (A calls B -> traverse to A)
        for (const auto& edge : allEdges.value()) {
            if (!matchesRelationFilter(edge, relationFilters))
                continue;

            std::int64_t neighborId;
            if (!reverseTraversal) {
                // Follow outgoing: nodeId is src, neighbor is dst
                if (edge.srcNodeId != nodeId)
                    continue;
                neighborId = edge.dstNodeId;
            } else {
                // Follow incoming: nodeId is dst, neighbor is src
                if (edge.dstNodeId != nodeId)
                    continue;
                neighborId = edge.srcNodeId;
            }

            if (visited.find(neighborId) == visited.end() && distance + 1 <= maxDepth) {
                queue.push({neighborId, distance + 1});
                visited.insert(neighborId);
            }
        }
    }

    // Legacy collectNeighbors for backward compatibility (without cache)
    void collectNeighbors(std::int64_t nodeId, int distance, int maxDepth,
                          const std::vector<GraphRelationType>& relationFilters,
                          std::unordered_set<std::int64_t>& visited,
                          std::queue<std::pair<std::int64_t, int>>& queue,
                          bool reverseTraversal = false) {
        EdgeCache dummyCache;
        collectNeighborsWithCache(nodeId, distance, maxDepth, relationFilters, visited, queue,
                                  dummyCache, reverseTraversal);
    }

    Result<void> performBFSTraversal(std::int64_t originNodeId, const GraphQueryRequest& req,
                                     GraphQueryResponse& response) {
        std::queue<std::pair<std::int64_t, int>> queue;
        std::unordered_set<std::int64_t> visited;
        EdgeCache edgeCache; // Cache edges to avoid duplicate queries

        queue.push({originNodeId, 0});
        visited.insert(originNodeId);

        std::size_t totalNodes = 0;
        std::size_t totalEdges = 0;

        while (!queue.empty() && totalNodes < req.maxResults) {
            auto [currentNodeId, distance] = queue.front();
            queue.pop();

            if (distance == 0) {
                collectNeighborsWithCache(currentNodeId, distance, req.maxDepth, req.relationFilters,
                                          visited, queue, edgeCache, req.reverseTraversal);
                continue;
            }

            auto nodeMetadataResult = hydrateNodeMetadata(currentNodeId, req.hydrateFully);
            if (!nodeMetadataResult) {
                spdlog::warn("Failed to hydrate node {}: {}", currentNodeId,
                             nodeMetadataResult.error().message);
                continue;
            }

            GraphConnectedNode connectedNode;
            connectedNode.nodeMetadata = std::move(nodeMetadataResult.value());
            connectedNode.distance = distance;

            // Use cached edges if available, otherwise fetch with bidirectional query
            auto cacheIt = edgeCache.find(currentNodeId);
            std::vector<metadata::KGEdge> nodeEdges;
            if (cacheIt != edgeCache.end()) {
                nodeEdges = cacheIt->second;
            } else {
                // Fetch edges using single bidirectional query (optimization: 1 query instead of 2)
                auto allEdges = kgStore_->getEdgesBidirectional(currentNodeId);
                if (allEdges) {
                    nodeEdges = std::move(allEdges.value());
                    edgeCache[currentNodeId] = nodeEdges;
                }
            }

            // Build connecting edges from cached/fetched edges
            for (const auto& edge : nodeEdges) {
                // Check if edge connects to a visited node
                bool connects = false;
                if (edge.srcNodeId == currentNodeId && visited.find(edge.dstNodeId) != visited.end()) {
                    connects = true;
                } else if (edge.dstNodeId == currentNodeId &&
                           visited.find(edge.srcNodeId) != visited.end()) {
                    connects = true;
                }

                if (connects) {
                    GraphEdgeDescriptor edgeDesc;
                    edgeDesc.edgeId = edge.id;
                    edgeDesc.srcNodeId = edge.srcNodeId;
                    edgeDesc.dstNodeId = edge.dstNodeId;
                    edgeDesc.relation = edge.relation;
                    edgeDesc.weight = edge.weight;
                    if (req.includeEdgeProperties)
                        edgeDesc.properties = edge.properties;
                    connectedNode.connectingEdges.push_back(std::move(edgeDesc));
                    totalEdges++;
                }
            }

            response.nodesByDistance[distance].push_back(connectedNode);
            response.allConnectedNodes.push_back(std::move(connectedNode));
            totalNodes++;

            if (response.nodesByDistance[distance].size() >= req.maxResultsPerDepth) {
                response.truncated = true;
                continue;
            }

            collectNeighborsWithCache(currentNodeId, distance, req.maxDepth, req.relationFilters,
                                      visited, queue, edgeCache, req.reverseTraversal);

            response.maxDepthReached = std::max(response.maxDepthReached, distance);
        }

        response.totalNodesFound = totalNodes;
        response.totalEdgesTraversed = totalEdges;

        if (totalNodes >= req.maxResults)
            response.truncated = true;

        if (req.offset > 0 || req.limit < response.allConnectedNodes.size()) {
            std::size_t start = std::min(req.offset, response.allConnectedNodes.size());
            std::size_t end = std::min(start + req.limit, response.allConnectedNodes.size());

            std::vector<GraphConnectedNode> paginatedResults;
            paginatedResults.reserve(end - start);
            for (std::size_t i = start; i < end; ++i) {
                paginatedResults.push_back(std::move(response.allConnectedNodes[i]));
            }
            response.allConnectedNodes = std::move(paginatedResults);
        }

        return Result<void>();
    }

    std::shared_ptr<metadata::KnowledgeGraphStore> kgStore_;
    std::shared_ptr<metadata::MetadataRepository> metadataRepo_;
};

// ===========================
// Factory Function
// ===========================

std::shared_ptr<IGraphQueryService>
makeGraphQueryService(std::shared_ptr<metadata::KnowledgeGraphStore> kgStore,
                      std::shared_ptr<metadata::MetadataRepository> metadataRepo) {
    if (!kgStore || !metadataRepo) {
        spdlog::warn("GraphQueryService: Cannot create service without KG store and metadata repo");
        return nullptr;
    }

    return std::make_shared<GraphQueryService>(std::move(kgStore), std::move(metadataRepo));
}

} // namespace yams::app::services
