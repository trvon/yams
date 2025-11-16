// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 YAMS Contributors

#include <spdlog/spdlog.h>
#include <yams/app/services/graph_query_service.hpp>
#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/metadata/knowledge_graph_store.h>

#include <queue>
#include <unordered_set>

namespace yams::daemon {

using namespace yams::app::services;
using namespace yams::metadata;

boost::asio::awaitable<Response>
RequestDispatcher::handleGraphQueryRequest(const GraphQueryRequest& req) {
    spdlog::debug(
        "GraphQuery request: docHash='{}', docName='{}', snapId='{}', nodeId={}, depth={}",
        req.documentHash, req.documentName, req.snapshotId, req.nodeId, req.maxDepth);

    auto metaRepo = serviceManager_ ? serviceManager_->getMetadataRepo() : nullptr;
    if (!metaRepo) {
        co_return ErrorResponse{.code = ErrorCode::InternalError,
                                .message = "Metadata repository unavailable"};
    }

    // Access KnowledgeGraphStore
    auto kgStore = metaRepo->getKnowledgeGraphStore();
    if (!kgStore) {
        GraphQueryResponse resp;
        resp.kgAvailable = false;
        resp.warning = "Knowledge graph not available";
        spdlog::warn("GraphQuery: KG store not available");
        co_return resp;
    }

    // Determine origin node ID
    int64_t originNodeId = req.nodeId;

    if (originNodeId < 0) {
        // Need to resolve from document hash, name, or snapshot
        if (!req.documentHash.empty()) {
            auto docIdResult = kgStore->getDocumentIdByHash(req.documentHash);
            if (!docIdResult || !docIdResult.value()) {
                co_return ErrorResponse{.code = ErrorCode::NotFound,
                                        .message = "Document not found: " + req.documentHash};
            }
            // In this implementation, we'll use document ID as node ID directly
            // This is a simplified approach - in a full implementation, you might need
            // to look up the corresponding KG node via a mapping table
            originNodeId = docIdResult.value().value();
        } else if (!req.documentName.empty()) {
            auto docIdResult = kgStore->getDocumentIdByName(req.documentName);
            if (!docIdResult || !docIdResult.value()) {
                co_return ErrorResponse{.code = ErrorCode::NotFound,
                                        .message = "Document not found: " + req.documentName};
            }
            originNodeId = docIdResult.value().value();
        } else {
            co_return ErrorResponse{.code = ErrorCode::InvalidArgument,
                                    .message =
                                        "Must specify documentHash, documentName, or nodeId"};
        }
    }

    // Get origin node
    auto originNodeResult = kgStore->getNodeById(originNodeId);
    if (!originNodeResult || !originNodeResult.value()) {
        co_return ErrorResponse{.code = ErrorCode::NotFound,
                                .message = "Origin node not found in knowledge graph"};
    }

    GraphQueryResponse resp;
    resp.kgAvailable = true;

    // Populate origin node
    const auto& originNode = originNodeResult.value().value();
    resp.originNode.nodeId = originNode.id;
    resp.originNode.nodeKey = originNode.nodeKey;
    resp.originNode.label = originNode.label.value_or("");
    resp.originNode.type = originNode.type.value_or("");
    resp.originNode.distance = 0;

    // BFS traversal to find connected nodes
    std::vector<int64_t> visited;
    std::queue<std::pair<int64_t, int32_t>> queue; // <nodeId, depth>
    std::unordered_set<int64_t> visitedSet;

    queue.push({originNodeId, 0});
    visitedSet.insert(originNodeId);

    uint64_t edgesTraversed = 0;
    int32_t maxDepthReached = 0;

    while (!queue.empty() && visited.size() < req.maxResults) {
        auto [currentNodeId, depth] = queue.front();
        queue.pop();

        if (depth > req.maxDepth) {
            continue;
        }

        maxDepthReached = std::max(maxDepthReached, depth);

        // Get outgoing edges
        auto edgesResult =
            kgStore->getEdgesFrom(currentNodeId, std::nullopt, req.maxResultsPerDepth, 0);
        if (edgesResult) {
            for (const auto& edge : edgesResult.value()) {
                edgesTraversed++;

                // Filter by relation if specified
                if (!req.relationFilters.empty()) {
                    bool matchesFilter = false;
                    for (const auto& filter : req.relationFilters) {
                        if (edge.relation == filter) {
                            matchesFilter = true;
                            break;
                        }
                    }
                    if (!matchesFilter) {
                        continue;
                    }
                }

                if (visitedSet.find(edge.dstNodeId) == visitedSet.end()) {
                    visitedSet.insert(edge.dstNodeId);
                    visited.push_back(edge.dstNodeId);

                    if (depth + 1 <= req.maxDepth) {
                        queue.push({edge.dstNodeId, depth + 1});
                    }
                }
            }
        }
    }

    resp.totalNodesFound = visited.size();
    resp.totalEdgesTraversed = edgesTraversed;
    resp.truncated = (visited.size() >= req.maxResults);
    resp.maxDepthReached = maxDepthReached;

    // Populate connected nodes (with pagination)
    const size_t startIdx = req.offset;
    const size_t endIdx = std::min(startIdx + req.limit, visited.size());

    resp.connectedNodes.reserve(endIdx - startIdx);
    for (size_t i = startIdx; i < endIdx; ++i) {
        int64_t nodeId = visited[i];

        auto nodeResult = kgStore->getNodeById(nodeId);
        if (nodeResult && nodeResult.value()) {
            const auto& node = nodeResult.value().value();

            GraphNode graphNode;
            graphNode.nodeId = node.id;
            graphNode.nodeKey = node.nodeKey;
            graphNode.label = node.label.value_or("");
            graphNode.type = node.type.value_or("");

            // Try to get document info if this is a document node
            if (auto hashResult = kgStore->getDocumentHashById(nodeId);
                hashResult && hashResult.value()) {
                graphNode.documentHash = hashResult.value().value();
            }

            graphNode.distance = 1; // Simplified - would need to track actual distance

            resp.connectedNodes.push_back(std::move(graphNode));
        }
    }

    spdlog::debug("GraphQuery: returning {} connected nodes, totalFound={}, truncated={}",
                  resp.connectedNodes.size(), resp.totalNodesFound, resp.truncated);
    co_return resp;
}

boost::asio::awaitable<Response>
RequestDispatcher::handleGraphPathHistoryRequest(const GraphPathHistoryRequest& req) {
    spdlog::debug("GraphPathHistory request: path='{}'", req.path);

    auto metaRepo = serviceManager_ ? serviceManager_->getMetadataRepo() : nullptr;
    if (!metaRepo) {
        co_return ErrorResponse{.code = ErrorCode::InternalError,
                                .message = "Metadata repository unavailable"};
    }

    if (req.path.empty()) {
        co_return ErrorResponse{.code = ErrorCode::InvalidArgument,
                                .message = "Path is required for path history query"};
    }

    // Access KnowledgeGraphStore
    auto kgStore = metaRepo->getKnowledgeGraphStore();
    if (!kgStore) {
        GraphPathHistoryResponse resp;
        resp.queryPath = req.path;
        // Empty history when KG not available
        spdlog::warn("GraphPathHistory: KG store not available");
        co_return resp;
    }

    // Query path history using the KG store's tree diff functionality
    auto historyResult = kgStore->fetchPathHistory(req.path, req.limit);
    if (!historyResult) {
        co_return ErrorResponse{.code = historyResult.error().code,
                                .message = historyResult.error().message};
    }

    // Build response
    GraphPathHistoryResponse resp;
    resp.queryPath = req.path;
    resp.history.reserve(historyResult.value().size());

    for (const auto& record : historyResult.value()) {
        PathHistoryEntry entry;
        entry.path = record.path;
        entry.snapshotId = record.snapshotId;
        entry.blobHash = record.blobHash;
        entry.changeType = record.changeType.value_or("unknown");

        resp.history.push_back(std::move(entry));
    }

    resp.hasMore = (resp.history.size() >= req.limit);

    spdlog::debug("GraphPathHistory: returning {} history entries for path '{}'",
                  resp.history.size(), req.path);
    co_return resp;
}

} // namespace yams::daemon
