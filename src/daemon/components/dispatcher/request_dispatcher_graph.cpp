// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 YAMS Contributors

#include <spdlog/spdlog.h>
#include <yams/app/services/graph_query_service.hpp>
#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/metadata/knowledge_graph_store.h>

namespace yams::daemon {

using namespace yams::app::services;
using namespace yams::metadata;

boost::asio::awaitable<Response>
RequestDispatcher::handleGraphQueryRequest(const GraphQueryRequest& req) {
    spdlog::debug(
        "GraphQuery request: docHash='{}', docName='{}', snapId='{}', nodeId={}, depth={}, "
        "listByType={}, nodeType='{}', nodeKey='{}'",
        req.documentHash, req.documentName, req.snapshotId, req.nodeId, req.maxDepth,
        req.listByType, req.nodeType, req.nodeKey);

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

    // PBI-093: Handle listByType mode - list nodes by type without traversal
    if (req.listByType) {
        co_return co_await handleGraphQueryListByType(req, kgStore.get());
    }

    // Handle isolated mode - find nodes with no incoming edges (optimized single query)
    if (req.isolatedMode) {
        co_return co_await handleGraphQueryIsolatedMode(req, kgStore.get());
    }

    // PBI-093: Handle nodeKey lookup - resolve key to nodeId first
    int64_t originNodeId = req.nodeId;
    if (originNodeId < 0 && !req.nodeKey.empty()) {
        auto nodeResult = kgStore->getNodeByKey(req.nodeKey);
        if (!nodeResult || !nodeResult.value()) {
            co_return ErrorResponse{.code = ErrorCode::NotFound,
                                    .message = "Node not found: " + req.nodeKey};
        }
        originNodeId = nodeResult.value()->id;
    }

    auto graphService = makeGraphQueryService(kgStore, metaRepo);
    if (!graphService) {
        co_return ErrorResponse{.code = ErrorCode::InternalError,
                                .message = "Failed to create graph query service"};
    }

    app::services::GraphQueryRequest svcReq;
    svcReq.nodeId = (originNodeId >= 0) ? std::make_optional(originNodeId) : std::nullopt;
    svcReq.documentHash = req.documentHash.empty() ? std::nullopt : std::make_optional(req.documentHash);
    svcReq.documentName = req.documentName.empty() ? std::nullopt : std::make_optional(req.documentName);
    svcReq.snapshotId = req.snapshotId.empty() ? std::nullopt : std::make_optional(req.snapshotId);
    svcReq.maxDepth = req.maxDepth;
    svcReq.maxResults = req.maxResults;
    svcReq.maxResultsPerDepth = req.maxResultsPerDepth;
    svcReq.offset = req.offset;
    svcReq.limit = req.limit;
    svcReq.hydrateFully = req.includeNodeProperties;
    svcReq.includeEdgeProperties = req.includeEdgeProperties;
    svcReq.reverseTraversal = req.reverseTraversal;

    auto result = graphService->query(svcReq);
    if (!result) {
        co_return ErrorResponse{.code = result.error().code, .message = result.error().message};
    }

    const auto& svcResp = result.value();

    GraphQueryResponse resp;
    resp.kgAvailable = svcResp.kgAvailable;
    resp.warning = svcResp.warning.value_or("");
    resp.totalNodesFound = svcResp.totalNodesFound;
    resp.totalEdgesTraversed = svcResp.totalEdgesTraversed;
    resp.truncated = svcResp.truncated;
    resp.maxDepthReached = svcResp.maxDepthReached;

    resp.originNode.nodeId = svcResp.originNode.node.nodeId;
    resp.originNode.nodeKey = svcResp.originNode.node.nodeKey;
    resp.originNode.label = svcResp.originNode.node.label.value_or("");
    resp.originNode.type = svcResp.originNode.node.type.value_or("");
    resp.originNode.distance = 0;

    resp.connectedNodes.reserve(svcResp.allConnectedNodes.size());
    for (const auto& cn : svcResp.allConnectedNodes) {
        GraphNode graphNode;
        graphNode.nodeId = cn.nodeMetadata.node.nodeId;
        graphNode.nodeKey = cn.nodeMetadata.node.nodeKey;
        graphNode.label = cn.nodeMetadata.node.label.value_or("");
        graphNode.type = cn.nodeMetadata.node.type.value_or("");
        graphNode.documentHash = cn.nodeMetadata.documentHash.value_or("");
        graphNode.distance = cn.distance;
        resp.connectedNodes.push_back(std::move(graphNode));
    }

    spdlog::debug("GraphQuery: returning {} connected nodes, totalFound={}, truncated={}",
                  resp.connectedNodes.size(), resp.totalNodesFound, resp.truncated);
    co_return resp;
}

// PBI-093: Helper for listByType mode - list KG nodes by type without traversal
boost::asio::awaitable<Response>
RequestDispatcher::handleGraphQueryListByType(const GraphQueryRequest& req,
                                              KnowledgeGraphStore* kgStore) {
    spdlog::debug("GraphQuery listByType: type='{}', limit={}, offset={}", req.nodeType, req.limit,
                  req.offset);

    if (req.nodeType.empty()) {
        co_return ErrorResponse{.code = ErrorCode::InvalidArgument,
                                .message = "nodeType is required for listByType mode"};
    }

    // Query nodes by type with pagination
    auto nodesResult = kgStore->findNodesByType(req.nodeType, req.limit, req.offset);
    if (!nodesResult) {
        co_return ErrorResponse{.code = nodesResult.error().code,
                                .message = nodesResult.error().message};
    }

    GraphQueryResponse resp;
    resp.kgAvailable = true;
    resp.totalNodesFound = nodesResult.value().size();
    resp.truncated = (nodesResult.value().size() >= req.limit);
    resp.maxDepthReached = 0; // No traversal in listByType mode

    // No origin node in listByType mode - use empty placeholder
    resp.originNode.nodeId = -1;
    resp.originNode.nodeKey = "";
    resp.originNode.label = "listByType:" + req.nodeType;
    resp.originNode.type = "query";
    resp.originNode.distance = 0;

    // Convert KGNodes to GraphNodes
    resp.connectedNodes.reserve(nodesResult.value().size());
    for (const auto& node : nodesResult.value()) {
        GraphNode graphNode;
        graphNode.nodeId = node.id;
        graphNode.nodeKey = node.nodeKey;
        graphNode.label = node.label.value_or("");
        graphNode.type = node.type.value_or("");
        graphNode.distance = 0;

        // Include properties if requested
        if (req.includeNodeProperties && node.properties) {
            graphNode.properties = node.properties.value();
        }

        resp.connectedNodes.push_back(std::move(graphNode));
    }

    spdlog::debug("GraphQuery listByType: returning {} nodes of type '{}'",
                  resp.connectedNodes.size(), req.nodeType);
    co_return resp;
}

boost::asio::awaitable<Response>
RequestDispatcher::handleGraphQueryIsolatedMode(const GraphQueryRequest& req,
                                                KnowledgeGraphStore* kgStore) {
    std::string nodeType = req.nodeType.empty() ? "function" : req.nodeType;
    std::string relation = req.isolatedRelation.empty() ? "calls" : req.isolatedRelation;

    spdlog::debug("GraphQuery isolatedMode: type='{}', relation='{}', limit={}", nodeType, relation,
                  req.limit);

    // Use the optimized single-query method
    auto nodesResult = kgStore->findIsolatedNodes(nodeType, relation, req.limit);
    if (!nodesResult) {
        co_return ErrorResponse{.code = nodesResult.error().code,
                                .message = nodesResult.error().message};
    }

    GraphQueryResponse resp;
    resp.kgAvailable = true;
    resp.totalNodesFound = nodesResult.value().size();
    resp.truncated = (nodesResult.value().size() >= req.limit);
    resp.maxDepthReached = 0;

    // No origin node in isolated mode
    resp.originNode.nodeId = -1;
    resp.originNode.nodeKey = "";
    resp.originNode.label = "isolated:" + nodeType + ":" + relation;
    resp.originNode.type = "query";
    resp.originNode.distance = 0;

    resp.connectedNodes.reserve(nodesResult.value().size());
    for (const auto& node : nodesResult.value()) {
        GraphNode graphNode;
        graphNode.nodeId = node.id;
        graphNode.nodeKey = node.nodeKey;
        graphNode.label = node.label.value_or("");
        graphNode.type = node.type.value_or("");
        graphNode.distance = 0;

        if (req.includeNodeProperties && node.properties) {
            graphNode.properties = node.properties.value();
        }

        resp.connectedNodes.push_back(std::move(graphNode));
    }

    spdlog::debug("GraphQuery isolatedMode: found {} isolated {} nodes (no incoming {} edges)",
                  resp.connectedNodes.size(), nodeType, relation);
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

boost::asio::awaitable<Response>
RequestDispatcher::handleKgIngestRequest(const KgIngestRequest& req) {
    spdlog::debug("KgIngest request: {} nodes, {} edges, {} aliases, docHash='{}'",
                  req.nodes.size(), req.edges.size(), req.aliases.size(), req.documentHash);

    auto metaRepo = serviceManager_ ? serviceManager_->getMetadataRepo() : nullptr;
    if (!metaRepo) {
        co_return ErrorResponse{.code = ErrorCode::InternalError,
                                .message = "Metadata repository unavailable"};
    }

    auto kgStore = metaRepo->getKnowledgeGraphStore();
    if (!kgStore) {
        co_return ErrorResponse{.code = ErrorCode::InternalError,
                                .message = "Knowledge graph store unavailable"};
    }

    KgIngestResponse resp;

    // Phase 1: Ingest nodes
    // Build a map of nodeKey -> nodeId for edge resolution
    std::unordered_map<std::string, int64_t> nodeKeyToId;

    if (!req.nodes.empty()) {
        std::vector<KGNode> kgNodes;
        kgNodes.reserve(req.nodes.size());

        for (const auto& ingestNode : req.nodes) {
            KGNode node;
            node.nodeKey = ingestNode.nodeKey;
            node.label =
                ingestNode.label.empty() ? std::nullopt : std::make_optional(ingestNode.label);
            node.type =
                ingestNode.type.empty() ? std::nullopt : std::make_optional(ingestNode.type);
            node.properties = ingestNode.properties.empty()
                                  ? std::nullopt
                                  : std::make_optional(ingestNode.properties);
            kgNodes.push_back(std::move(node));
        }

        auto upsertResult = kgStore->upsertNodes(kgNodes);
        if (!upsertResult) {
            resp.errors.push_back("Node upsert failed: " + upsertResult.error().message);
            resp.success = false;
        } else {
            const auto& nodeIds = upsertResult.value();
            for (size_t i = 0; i < nodeIds.size() && i < req.nodes.size(); ++i) {
                nodeKeyToId[req.nodes[i].nodeKey] = nodeIds[i];
                if (nodeIds[i] > 0) {
                    resp.nodesInserted++;
                } else {
                    resp.nodesSkipped++;
                }
            }
        }
    }

    // Also populate nodeKeyToId for any existing nodes referenced in edges
    // that weren't in the nodes list
    for (const auto& edge : req.edges) {
        if (nodeKeyToId.find(edge.srcNodeKey) == nodeKeyToId.end()) {
            auto nodeRes = kgStore->getNodeByKey(edge.srcNodeKey);
            if (nodeRes && nodeRes.value()) {
                nodeKeyToId[edge.srcNodeKey] = nodeRes.value()->id;
            }
        }
        if (nodeKeyToId.find(edge.dstNodeKey) == nodeKeyToId.end()) {
            auto nodeRes = kgStore->getNodeByKey(edge.dstNodeKey);
            if (nodeRes && nodeRes.value()) {
                nodeKeyToId[edge.dstNodeKey] = nodeRes.value()->id;
            }
        }
    }

    // Phase 2: Ingest edges
    if (!req.edges.empty()) {
        std::vector<KGEdge> kgEdges;
        kgEdges.reserve(req.edges.size());

        for (const auto& ingestEdge : req.edges) {
            // Resolve node keys to IDs
            auto srcIt = nodeKeyToId.find(ingestEdge.srcNodeKey);
            auto dstIt = nodeKeyToId.find(ingestEdge.dstNodeKey);

            if (srcIt == nodeKeyToId.end()) {
                resp.errors.push_back("Edge source node not found: " + ingestEdge.srcNodeKey);
                resp.edgesSkipped++;
                continue;
            }
            if (dstIt == nodeKeyToId.end()) {
                resp.errors.push_back("Edge destination node not found: " + ingestEdge.dstNodeKey);
                resp.edgesSkipped++;
                continue;
            }

            KGEdge edge;
            edge.srcNodeId = srcIt->second;
            edge.dstNodeId = dstIt->second;
            edge.relation = ingestEdge.relation;
            edge.weight = ingestEdge.weight;
            edge.properties = ingestEdge.properties.empty()
                                  ? std::nullopt
                                  : std::make_optional(ingestEdge.properties);
            kgEdges.push_back(std::move(edge));
        }

        if (!kgEdges.empty()) {
            // Use addEdgesUnique for de-duplication
            auto addResult = req.skipExistingEdges ? kgStore->addEdgesUnique(kgEdges)
                                                   : kgStore->addEdges(kgEdges);
            if (!addResult) {
                resp.errors.push_back("Edge insert failed: " + addResult.error().message);
                resp.success = false;
            } else {
                resp.edgesInserted += kgEdges.size();
            }
        }
    }

    // Phase 3: Ingest aliases
    if (!req.aliases.empty()) {
        std::vector<KGAlias> kgAliases;
        kgAliases.reserve(req.aliases.size());

        for (const auto& ingestAlias : req.aliases) {
            auto nodeIt = nodeKeyToId.find(ingestAlias.nodeKey);
            if (nodeIt == nodeKeyToId.end()) {
                // Try to look up the node
                auto nodeRes = kgStore->getNodeByKey(ingestAlias.nodeKey);
                if (nodeRes && nodeRes.value()) {
                    nodeKeyToId[ingestAlias.nodeKey] = nodeRes.value()->id;
                    nodeIt = nodeKeyToId.find(ingestAlias.nodeKey);
                } else {
                    resp.errors.push_back("Alias node not found: " + ingestAlias.nodeKey);
                    resp.aliasesSkipped++;
                    continue;
                }
            }

            KGAlias alias;
            alias.nodeId = nodeIt->second;
            alias.alias = ingestAlias.alias;
            alias.source =
                ingestAlias.source.empty() ? std::nullopt : std::make_optional(ingestAlias.source);
            alias.confidence = ingestAlias.confidence;
            kgAliases.push_back(std::move(alias));
        }

        if (!kgAliases.empty()) {
            auto addResult = kgStore->addAliases(kgAliases);
            if (!addResult) {
                resp.errors.push_back("Alias insert failed: " + addResult.error().message);
                // Don't mark as full failure for aliases
            } else {
                resp.aliasesInserted += kgAliases.size();
            }
        }
    }

    spdlog::info("KgIngest completed: {} nodes inserted, {} skipped; {} edges inserted, {} "
                 "skipped; {} aliases inserted, {} skipped; {} errors",
                 resp.nodesInserted, resp.nodesSkipped, resp.edgesInserted, resp.edgesSkipped,
                 resp.aliasesInserted, resp.aliasesSkipped, resp.errors.size());

    co_return resp;
}

} // namespace yams::daemon
