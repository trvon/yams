#include <yams/app/services/graph_query_service.hpp>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <queue>
#include <unordered_map>
#include <unordered_set>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

namespace yams::app::services {

namespace {

struct NormalizedRelationFilter {
    bool allowAll{true};
    std::unordered_set<std::string> allowed;
};

std::string normalizeRelationName(std::string relation) {
    auto trimLeft = std::find_if_not(relation.begin(), relation.end(),
                                     [](unsigned char c) { return std::isspace(c) != 0; });
    auto trimRight = std::find_if_not(relation.rbegin(), relation.rend(), [](unsigned char c) {
                         return std::isspace(c) != 0;
                     }).base();

    if (trimLeft >= trimRight) {
        return {};
    }

    std::string normalized(trimLeft, trimRight);
    std::transform(normalized.begin(), normalized.end(), normalized.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    std::replace(normalized.begin(), normalized.end(), '-', '_');
    std::replace(normalized.begin(), normalized.end(), ' ', '_');

    // Canonical aliases accepted from CLI/API callers.
    if (normalized == "call")
        return "calls";
    if (normalized == "include")
        return "includes";
    if (normalized == "inherit")
        return "inherits";
    if (normalized == "implement")
        return "implements";
    if (normalized == "reference")
        return "references";
    if (normalized == "rename_to")
        return "renamed_to";
    if (normalized == "rename_from")
        return "renamed_from";
    if (normalized == "move_to")
        return "moved_to";
    if (normalized == "move_from")
        return "moved_from";
    if (normalized == "version")
        return "has_version";
    if (normalized == "blob_version")
        return "has_version";

    return normalized;
}

// Helper: Convert GraphRelationType enum to KG relation strings
std::vector<std::string> relationTypeToStrings(GraphRelationType type) {
    switch (type) {
        case GraphRelationType::All:
            return {}; // Empty = no filter
        case GraphRelationType::SameContent:
            return {"same_content", "blob_version", "has_blob", "blob_at_path"};
        case GraphRelationType::RenamedFrom:
            return {"renamed_from", "moved_from", "renamed_to"};
        case GraphRelationType::RenamedTo:
            return {"renamed_to", "moved_to", "renamed_from"};
        case GraphRelationType::DirectoryChild:
            return {"contains", "directory_child", "scoped_by"};
        case GraphRelationType::SymbolReference:
            return {"symbol_reference", "entity_reference", "references",  "calls",
                    "includes",         "inherits",         "implements",  "defined_in",
                    "located_in",       "scoped_by",        "observed_as", "mentioned_in",
                    "co_mentioned_with"};
        case GraphRelationType::PathVersion:
            return {"has_version", "path_version", "blob_at_path", "observed_as"};
        default:
            return {};
    }
}

NormalizedRelationFilter buildRelationFilter(const std::vector<GraphRelationType>& filters,
                                             const std::vector<std::string>& relationNames) {
    NormalizedRelationFilter out;

    if (!relationNames.empty()) {
        out.allowAll = false;
        for (const auto& relationName : relationNames) {
            std::string normalized = normalizeRelationName(relationName);
            if (!normalized.empty()) {
                out.allowed.insert(std::move(normalized));
            }
        }
        return out;
    }

    if (filters.empty()) {
        out.allowAll = true;
        return out;
    }

    for (const auto& filterType : filters) {
        auto allowedRelations = relationTypeToStrings(filterType);
        if (allowedRelations.empty()) {
            out.allowAll = true;
            out.allowed.clear();
            return out;
        }

        out.allowAll = false;
        for (auto allowed : allowedRelations) {
            allowed = normalizeRelationName(std::move(allowed));
            if (!allowed.empty()) {
                out.allowed.insert(std::move(allowed));
            }
        }
    }

    return out;
}

// Helper: Check if an edge matches the normalized relation filter.
bool matchesRelationFilter(const metadata::KGEdge& edge, const NormalizedRelationFilter& filter) {
    if (filter.allowAll) {
        return true;
    }
    const std::string normalized = normalizeRelationName(edge.relation);
    return filter.allowed.find(normalized) != filter.allowed.end();
}

int relationSignalWeight(std::string_view relation) {
    const std::string normalized = normalizeRelationName(std::string(relation));

    if (normalized == "calls" || normalized == "references" || normalized == "inherits" ||
        normalized == "implements" || normalized == "mentioned_in") {
        return 100;
    }

    if (normalized == "includes" || normalized == "defined_in" || normalized == "located_in" ||
        normalized == "observed_as" || normalized == "co_mentioned_with" ||
        normalized == "symbol_reference" || normalized == "entity_reference") {
        return 75;
    }

    if (normalized == "has_version" || normalized == "path_version" ||
        normalized == "blob_at_path" || normalized == "renamed_from" ||
        normalized == "renamed_to" || normalized == "moved_from" || normalized == "moved_to" ||
        normalized == "same_content") {
        return 45;
    }

    if (normalized == "contains" || normalized == "has_blob" || normalized == "scoped_by" ||
        normalized == "directory_child" || normalized == "has_tag") {
        return 15;
    }

    return 30;
}

double extractEdgeConfidence(const metadata::KGEdge& edge) {
    if (edge.properties.has_value()) {
        try {
            const auto props = nlohmann::json::parse(edge.properties.value());
            if (props.contains("confidence") && props["confidence"].is_number()) {
                return std::clamp(props["confidence"].get<double>(), 0.0, 1.0);
            }
            if (props.contains("provenance") && props["provenance"].is_object()) {
                const auto& provenance = props["provenance"];
                if (provenance.contains("confidence") && provenance["confidence"].is_number()) {
                    return std::clamp(provenance["confidence"].get<double>(), 0.0, 1.0);
                }
            }
        } catch (...) {
        }
    }

    return std::clamp(static_cast<double>(edge.weight), 0.0, 1.0);
}

double relationPriorityScore(const metadata::KGEdge& edge) {
    const int signal = relationSignalWeight(edge.relation);
    const double confidence = extractEdgeConfidence(edge);
    return static_cast<double>(signal) + (confidence * 10.0) +
           (static_cast<double>(edge.weight) * 0.5);
}

double computeNodeRelevance(const metadata::KGEdge& edge, int distance) {
    const double base = relationPriorityScore(edge);
    const double depthPenalty = static_cast<double>(std::max(distance - 1, 0)) * 2.0;
    return base - depthPenalty;
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
        const auto relationFilter = buildRelationFilter(req.relationFilters, req.relationNames);
        auto traversalResult = performBFSTraversal(originNodeId, req, relationFilter, response);
        if (!traversalResult) {
            return Result<GraphQueryResponse>(traversalResult.error());
        }

        // Step 4: Calculate metrics
        auto end = std::chrono::steady_clock::now();
        response.queryTimeMs =
            std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

        return response;
    }

    Result<ListSnapshotsResponse>
    listSnapshots([[maybe_unused]] const ListSnapshotsRequest& req) override {
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

    Result<void> performBFSTraversal(std::int64_t originNodeId, const GraphQueryRequest& req,
                                     const NormalizedRelationFilter& relationFilter,
                                     GraphQueryResponse& response) {
        struct TraversalParent {
            std::int64_t parentNodeId{0};
            metadata::KGEdge edge;
        };

        auto edgePreference = [](const metadata::KGEdge& lhs, const metadata::KGEdge& rhs) {
            const double lhsScore = relationPriorityScore(lhs);
            const double rhsScore = relationPriorityScore(rhs);
            if (lhsScore != rhsScore) {
                return lhsScore > rhsScore;
            }

            const std::string lhsRelation = normalizeRelationName(lhs.relation);
            const std::string rhsRelation = normalizeRelationName(rhs.relation);
            if (lhsRelation != rhsRelation) {
                return lhsRelation < rhsRelation;
            }

            if (lhs.id != rhs.id) {
                return lhs.id < rhs.id;
            }

            if (lhs.srcNodeId != rhs.srcNodeId) {
                return lhs.srcNodeId < rhs.srcNodeId;
            }
            return lhs.dstNodeId < rhs.dstNodeId;
        };

        auto isBetterParent = [&](const metadata::KGEdge& candidate,
                                  const metadata::KGEdge& existing) -> bool {
            return edgePreference(candidate, existing);
        };

        auto getCachedEdges =
            [&](std::int64_t nodeId,
                EdgeCache& cache) -> Result<const std::vector<metadata::KGEdge>*> {
            auto cacheIt = cache.find(nodeId);
            if (cacheIt == cache.end()) {
                auto edgeRes = kgStore_->getEdgesBidirectional(nodeId);
                if (!edgeRes) {
                    return Result<const std::vector<metadata::KGEdge>*>(edgeRes.error());
                }
                cacheIt = cache.emplace(nodeId, std::move(edgeRes.value())).first;
            }
            return &cacheIt->second;
        };

        auto collectTraversalEdges = [&](std::int64_t nodeId,
                                         const std::vector<metadata::KGEdge>& allEdges,
                                         bool reverseTraversal) {
            std::vector<metadata::KGEdge> traversalEdges;
            traversalEdges.reserve(allEdges.size());

            for (const auto& edge : allEdges) {
                if (!matchesRelationFilter(edge, relationFilter)) {
                    continue;
                }

                if (!reverseTraversal) {
                    if (edge.srcNodeId != nodeId) {
                        continue;
                    }
                } else {
                    if (edge.dstNodeId != nodeId) {
                        continue;
                    }
                }

                traversalEdges.push_back(edge);
            }

            std::sort(traversalEdges.begin(), traversalEdges.end(), edgePreference);
            return traversalEdges;
        };

        std::queue<std::pair<std::int64_t, int>> queue;
        std::unordered_set<std::int64_t> visited;
        std::unordered_map<std::int64_t, int> distanceByNode;
        std::unordered_map<std::int64_t, TraversalParent> parentByNode;
        EdgeCache edgeCache; // Cache edges to avoid duplicate queries

        queue.push({originNodeId, 0});
        visited.insert(originNodeId);
        distanceByNode.emplace(originNodeId, 0);

        std::size_t totalNodes = 0;
        std::size_t totalEdges = 0;

        while (!queue.empty() && totalNodes < req.maxResults) {
            auto [currentNodeId, distance] = queue.front();
            queue.pop();

            const auto allEdgesRes = getCachedEdges(currentNodeId, edgeCache);
            if (!allEdgesRes) {
                spdlog::warn("Failed to fetch edges for node {}: {}", currentNodeId,
                             allEdgesRes.error().message);
                continue;
            }

            const auto& allEdges = *allEdgesRes.value();
            const auto traversalEdges =
                collectTraversalEdges(currentNodeId, allEdges, req.reverseTraversal);

            if (distance < req.maxDepth) {
                for (const auto& edge : traversalEdges) {
                    const std::int64_t neighborId =
                        req.reverseTraversal ? edge.srcNodeId : edge.dstNodeId;
                    const int neighborDistance = distance + 1;

                    auto depthIt = distanceByNode.find(neighborId);
                    const bool seenBefore = (depthIt != distanceByNode.end());

                    if (!seenBefore) {
                        distanceByNode.emplace(neighborId, neighborDistance);
                        parentByNode[neighborId] = TraversalParent{currentNodeId, edge};
                        queue.push({neighborId, neighborDistance});
                        visited.insert(neighborId);
                        continue;
                    }

                    // If there are multiple paths at the same BFS depth, prefer the
                    // higher-signal relation for attribution.
                    if (depthIt->second == neighborDistance) {
                        auto parentIt = parentByNode.find(neighborId);
                        if (parentIt == parentByNode.end() ||
                            isBetterParent(edge, parentIt->second.edge)) {
                            parentByNode[neighborId] = TraversalParent{currentNodeId, edge};
                        }
                    }
                }
            }

            if (distance == 0) {
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

            auto parentIt = parentByNode.find(currentNodeId);
            if (parentIt != parentByNode.end()) {
                const auto& parentEdge = parentIt->second.edge;
                GraphEdgeDescriptor edgeDesc;
                edgeDesc.edgeId = parentEdge.id;
                edgeDesc.srcNodeId = parentEdge.srcNodeId;
                edgeDesc.dstNodeId = parentEdge.dstNodeId;
                edgeDesc.relation = parentEdge.relation;
                edgeDesc.weight = parentEdge.weight;
                if (req.includeEdgeProperties) {
                    edgeDesc.properties = parentEdge.properties;
                }
                connectedNode.connectingEdges.push_back(std::move(edgeDesc));
                connectedNode.relevanceScore = computeNodeRelevance(parentEdge, distance);
                totalEdges += 1;
            }

            auto& depthBucket = response.nodesByDistance[distance];
            if (depthBucket.size() >= req.maxResultsPerDepth) {
                response.truncated = true;
                continue;
            }
            depthBucket.push_back(std::move(connectedNode));
            totalNodes++;

            response.maxDepthReached = std::max(response.maxDepthReached, distance);
        }

        auto nodeOrder = [](const GraphConnectedNode& lhs, const GraphConnectedNode& rhs) {
            if (lhs.relevanceScore != rhs.relevanceScore) {
                return lhs.relevanceScore > rhs.relevanceScore;
            }
            if (lhs.nodeMetadata.node.nodeKey != rhs.nodeMetadata.node.nodeKey) {
                return lhs.nodeMetadata.node.nodeKey < rhs.nodeMetadata.node.nodeKey;
            }
            return lhs.nodeMetadata.node.nodeId < rhs.nodeMetadata.node.nodeId;
        };

        std::vector<int> distances;
        distances.reserve(response.nodesByDistance.size());
        for (const auto& [distance, _] : response.nodesByDistance) {
            distances.push_back(distance);
        }
        std::sort(distances.begin(), distances.end());

        response.allConnectedNodes.clear();
        for (int distance : distances) {
            auto bucketIt = response.nodesByDistance.find(distance);
            if (bucketIt == response.nodesByDistance.end()) {
                continue;
            }
            auto& bucket = bucketIt->second;
            std::sort(bucket.begin(), bucket.end(), nodeOrder);
            for (const auto& node : bucket) {
                response.allConnectedNodes.push_back(node);
            }
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
