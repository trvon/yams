// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (c) 2025 YAMS Contributors

#include <spdlog/spdlog.h>
#include <algorithm>
#include <cctype>
#include <unordered_set>
#include <yams/app/services/graph_query_service.hpp>
#include <yams/daemon/components/dispatch_response.hpp>
#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/metadata/knowledge_graph_store.h>

namespace yams::daemon {

namespace {

std::string canonicalizeRelationName(std::string value) {
    auto trimLeft = std::find_if_not(value.begin(), value.end(),
                                     [](unsigned char c) { return std::isspace(c) != 0; });
    auto trimRight = std::find_if_not(value.rbegin(), value.rend(), [](unsigned char c) {
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

    return normalized;
}

std::vector<std::string> canonicalizeRelationFilters(const std::vector<std::string>& input) {
    std::vector<std::string> out;
    out.reserve(input.size());
    for (const auto& relation : input) {
        auto canonical = canonicalizeRelationName(relation);
        if (!canonical.empty()) {
            out.push_back(std::move(canonical));
        }
    }
    return out;
}

} // namespace

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
        co_return dispatch::makeErrorResponse(ErrorCode::InternalError,
                                              "Metadata repository unavailable");
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

    // yams-66h: Handle listTypes mode - list available node types with counts
    if (req.listTypes) {
        co_return co_await handleGraphQueryListTypes(req, kgStore.get());
    }

    // Handle listRelations mode - list relation types with counts
    if (req.listRelations) {
        co_return co_await handleGraphQueryListRelations(req, kgStore.get());
    }

    // Handle searchMode - search nodes by label pattern
    if (req.searchMode) {
        co_return co_await handleGraphQuerySearchMode(req, kgStore.get());
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
            co_return dispatch::makeErrorResponse(ErrorCode::NotFound,
                                                  "Node not found: " + req.nodeKey);
        }
        originNodeId = nodeResult.value()->id;
    }

    auto graphService = makeGraphQueryService(kgStore, metaRepo);
    if (!graphService) {
        co_return dispatch::makeErrorResponse(ErrorCode::InternalError,
                                              "Failed to create graph query service");
    }

    app::services::GraphQueryRequest svcReq;
    svcReq.nodeId = (originNodeId >= 0) ? std::make_optional(originNodeId) : std::nullopt;
    svcReq.documentHash =
        req.documentHash.empty() ? std::nullopt : std::make_optional(req.documentHash);
    svcReq.documentName =
        req.documentName.empty() ? std::nullopt : std::make_optional(req.documentName);
    svcReq.snapshotId = req.snapshotId.empty() ? std::nullopt : std::make_optional(req.snapshotId);
    svcReq.maxDepth = req.maxDepth;
    svcReq.maxResults = req.maxResults;
    svcReq.maxResultsPerDepth = req.maxResultsPerDepth;
    svcReq.offset = req.offset;
    svcReq.limit = req.limit;
    svcReq.hydrateFully = req.includeNodeProperties;
    svcReq.includeEdgeProperties = req.includeEdgeProperties;
    svcReq.reverseTraversal = req.reverseTraversal;
    svcReq.relationNames = canonicalizeRelationFilters(req.relationFilters);

    auto result = graphService->query(svcReq);
    if (!result) {
        co_return dispatch::makeErrorResponse(result.error().code, result.error().message);
    }

    const auto& svcResp = result.value();

    GraphQueryResponse resp = dispatch::GraphQueryResponseMapper::fromServiceResponse(svcResp);

    resp.connectedNodes.reserve(svcResp.allConnectedNodes.size());
    std::unordered_set<int64_t> seenEdges;
    for (const auto& cn : svcResp.allConnectedNodes) {
        dispatch::GraphQueryResponseMapper::mapConnectedNodesAndEdges(cn, resp.connectedNodes,
                                                                      resp.edges, seenEdges);
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
        co_return dispatch::makeErrorResponse(ErrorCode::InvalidArgument,
                                              "nodeType is required for listByType mode");
    }

    // Query nodes by type with pagination
    auto nodesResult = kgStore->findNodesByType(req.nodeType, req.limit, req.offset);
    if (!nodesResult) {
        co_return dispatch::makeErrorResponse(nodesResult.error().code,
                                              nodesResult.error().message);
    }

    GraphQueryResponse resp;
    resp.kgAvailable = true;
    auto totalCount = kgStore->countNodesByType(req.nodeType);
    if (!totalCount) {
        co_return dispatch::makeErrorResponse(totalCount.error().code, totalCount.error().message);
    }
    resp.totalNodesFound = static_cast<uint64_t>(totalCount.value());
    resp.truncated = (nodesResult.value().size() >= req.limit);
    resp.maxDepthReached = 0;

    dispatch::GraphQueryResponseMapper::setOriginNode(resp, -1, "", "listByType:" + req.nodeType,
                                                      "query");

    resp.connectedNodes =
        dispatch::KGNodeMapper::mapKGNodes(nodesResult.value(), req.includeNodeProperties);

    spdlog::debug("GraphQuery listByType: returning {} nodes of type '{}'",
                  resp.connectedNodes.size(), req.nodeType);
    co_return resp;
}

boost::asio::awaitable<Response>
RequestDispatcher::handleGraphQueryIsolatedMode(const GraphQueryRequest& req,
                                                KnowledgeGraphStore* kgStore) {
    std::string nodeType = req.nodeType.empty() ? "function" : req.nodeType;
    std::string relation =
        req.isolatedRelation.empty() ? "calls" : canonicalizeRelationName(req.isolatedRelation);

    spdlog::debug("GraphQuery isolatedMode: type='{}', relation='{}', limit={}", nodeType, relation,
                  req.limit);

    // Use the optimized single-query method
    auto nodesResult = kgStore->findIsolatedNodes(nodeType, relation, req.limit);
    if (!nodesResult) {
        co_return dispatch::makeErrorResponse(nodesResult.error().code,
                                              nodesResult.error().message);
    }

    GraphQueryResponse resp;
    resp.kgAvailable = true;
    resp.totalNodesFound = nodesResult.value().size();
    resp.truncated = (nodesResult.value().size() >= req.limit);
    resp.maxDepthReached = 0;

    dispatch::GraphQueryResponseMapper::setOriginNode(
        resp, -1, "", "isolated:" + nodeType + ":" + relation, "query");

    resp.connectedNodes =
        dispatch::KGNodeMapper::mapKGNodes(nodesResult.value(), req.includeNodeProperties);

    spdlog::debug("GraphQuery isolatedMode: found {} isolated {} nodes (no incoming {} edges)",
                  resp.connectedNodes.size(), nodeType, relation);
    co_return resp;
}

// yams-66h: Helper for listTypes mode - list available node types with counts
boost::asio::awaitable<Response>
RequestDispatcher::handleGraphQueryListTypes(const GraphQueryRequest& req,
                                             KnowledgeGraphStore* kgStore) {
    spdlog::debug("GraphQuery listTypes: fetching node type counts");
    (void)req; // Unused for now, but may be used for future filtering

    auto countsResult = kgStore->getNodeTypeCounts();
    if (!countsResult) {
        co_return dispatch::makeErrorResponse(countsResult.error().code,
                                              countsResult.error().message);
    }

    GraphQueryResponse resp;
    resp.kgAvailable = true;
    resp.totalNodesFound = countsResult.value().size();
    resp.truncated = false;
    resp.maxDepthReached = 0;

    // No origin node in listTypes mode
    resp.originNode.nodeId = -1;
    resp.originNode.nodeKey = "";
    resp.originNode.label = "listTypes";
    resp.originNode.type = "query";
    resp.originNode.distance = 0;

    // Populate nodeTypeCounts
    resp.nodeTypeCounts.reserve(countsResult.value().size());
    for (const auto& [type, count] : countsResult.value()) {
        resp.nodeTypeCounts.emplace_back(type, static_cast<uint64_t>(count));
    }

    spdlog::debug("GraphQuery listTypes: found {} distinct node types", resp.nodeTypeCounts.size());
    co_return resp;
}

// yams-kt5t: Helper for listRelations mode - list relation types with counts
boost::asio::awaitable<Response>
RequestDispatcher::handleGraphQueryListRelations(const GraphQueryRequest& req,
                                                 KnowledgeGraphStore* kgStore) {
    spdlog::debug("GraphQuery listRelations: fetching relation type counts");
    (void)req; // Unused for now, but may be used for future filtering

    auto countsResult = kgStore->getRelationTypeCounts();
    if (!countsResult) {
        co_return dispatch::makeErrorResponse(countsResult.error().code,
                                              countsResult.error().message);
    }

    GraphQueryResponse resp;
    resp.kgAvailable = true;
    resp.totalNodesFound = 0; // Not applicable for relation listing
    resp.truncated = false;
    resp.maxDepthReached = 0;

    // No origin node in listRelations mode
    resp.originNode.nodeId = -1;
    resp.originNode.nodeKey = "";
    resp.originNode.label = "listRelations";
    resp.originNode.type = "query";
    resp.originNode.distance = 0;

    // Populate relationTypeCounts
    resp.relationTypeCounts.reserve(countsResult.value().size());
    for (const auto& [relation, count] : countsResult.value()) {
        resp.relationTypeCounts.emplace_back(relation, static_cast<uint64_t>(count));
    }

    spdlog::debug("GraphQuery listRelations: found {} distinct relation types",
                  resp.relationTypeCounts.size());
    co_return resp;
}

// yams-kt5t: Helper for search mode - search nodes by label pattern
boost::asio::awaitable<Response>
RequestDispatcher::handleGraphQuerySearchMode(const GraphQueryRequest& req,
                                              KnowledgeGraphStore* kgStore) {
    spdlog::debug("GraphQuery searchMode: pattern='{}', limit={}, offset={}", req.searchPattern,
                  req.limit, req.offset);

    if (req.searchPattern.empty()) {
        co_return dispatch::makeErrorResponse(ErrorCode::InvalidArgument,
                                              "searchPattern is required for search mode");
    }

    auto nodesResult = kgStore->searchNodesByLabel(req.searchPattern, req.limit, req.offset);
    if (!nodesResult) {
        co_return dispatch::makeErrorResponse(nodesResult.error().code,
                                              nodesResult.error().message);
    }

    GraphQueryResponse resp;
    resp.kgAvailable = true;
    resp.totalNodesFound = nodesResult.value().size();
    resp.truncated = (nodesResult.value().size() >= req.limit);
    resp.maxDepthReached = 0;

    // No origin node in search mode
    resp.originNode.nodeId = -1;
    resp.originNode.nodeKey = "";
    resp.originNode.label = "search:" + req.searchPattern;
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

    spdlog::debug("GraphQuery searchMode: found {} nodes matching pattern '{}'",
                  resp.connectedNodes.size(), req.searchPattern);
    co_return resp;
}

boost::asio::awaitable<Response>
RequestDispatcher::handleGraphPathHistoryRequest(const GraphPathHistoryRequest& req) {
    spdlog::debug("GraphPathHistory request: path='{}'", req.path);

    auto metaRepo = serviceManager_ ? serviceManager_->getMetadataRepo() : nullptr;
    if (!metaRepo) {
        co_return dispatch::makeErrorResponse(ErrorCode::InternalError,
                                              "Metadata repository unavailable");
    }

    if (req.path.empty()) {
        co_return dispatch::makeErrorResponse(ErrorCode::InvalidArgument,
                                              "Path is required for path history query");
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
        co_return dispatch::makeErrorResponse(historyResult.error().code,
                                              historyResult.error().message);
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
        co_return dispatch::makeErrorResponse(ErrorCode::InternalError,
                                              "Metadata repository unavailable");
    }

    auto kgStore = metaRepo->getKnowledgeGraphStore();
    if (!kgStore) {
        co_return dispatch::makeErrorResponse(ErrorCode::InternalError,
                                              "Knowledge graph store unavailable");
    }

    KgIngestResponse resp;

    auto* coord = serviceManager_ ? serviceManager_->getWriteCoordinator() : nullptr;
    if (coord) {
        auto wb = std::make_unique<WriteBatch>();
        wb->source = "RPC::ingestGraph";

        std::unordered_set<std::string> knownNodeKeys;
        knownNodeKeys.reserve(req.nodes.size() + req.edges.size() * 2 + req.aliases.size());
        for (const auto& n : req.nodes) {
            if (!n.nodeKey.empty()) {
                knownNodeKeys.insert(n.nodeKey);
            }
        }
        auto nodeExists = [&](const std::string& nodeKey) -> bool {
            if (nodeKey.empty()) {
                return false;
            }
            if (knownNodeKeys.contains(nodeKey)) {
                return true;
            }
            auto nodeRes = kgStore->getNodeByKey(nodeKey);
            if (nodeRes && nodeRes.value().has_value()) {
                knownNodeKeys.insert(nodeKey);
                return true;
            }
            return false;
        };

        if (!req.nodes.empty()) {
            std::vector<KGNode> nodes;
            nodes.reserve(req.nodes.size());
            for (const auto& n : req.nodes) {
                KGNode node;
                node.nodeKey = n.nodeKey;
                node.label = n.label.empty() ? std::nullopt : std::make_optional(n.label);
                node.type = n.type.empty() ? std::nullopt : std::make_optional(n.type);
                node.properties =
                    n.properties.empty() ? std::nullopt : std::make_optional(n.properties);
                nodes.push_back(std::move(node));
            }
            wb->ops.emplace_back(UpsertNodesOp{std::move(nodes)});
        }

        if (!req.edges.empty()) {
            std::vector<DeferredEdgeOp> deferred;
            deferred.reserve(req.edges.size());
            for (const auto& edge : req.edges) {
                const bool srcOk = nodeExists(edge.srcNodeKey);
                const bool dstOk = nodeExists(edge.dstNodeKey);
                if (!srcOk || !dstOk) {
                    ++resp.edgesSkipped;
                    if (!srcOk) {
                        resp.errors.push_back("Edge source node not found: " + edge.srcNodeKey);
                    }
                    if (!dstOk) {
                        resp.errors.push_back("Edge destination node not found: " +
                                              edge.dstNodeKey);
                    }
                    continue;
                }
                DeferredEdgeOp op;
                op.srcNodeKey = edge.srcNodeKey;
                op.dstNodeKey = edge.dstNodeKey;
                op.relation = edge.relation;
                op.weight = edge.weight;
                op.properties =
                    edge.properties.empty() ? std::nullopt : std::make_optional(edge.properties);
                deferred.push_back(std::move(op));
            }
            wb->ops.emplace_back(AddDeferredEdgesOp{std::move(deferred)});
        }

        if (!req.aliases.empty()) {
            std::vector<KGAlias> aliases;
            aliases.reserve(req.aliases.size());
            for (const auto& a : req.aliases) {
                if (!nodeExists(a.nodeKey)) {
                    ++resp.aliasesSkipped;
                    resp.errors.push_back("Alias node not found: " + a.nodeKey);
                    continue;
                }
                KGAlias alias;
                alias.nodeId = 0;
                alias.alias = a.alias;
                std::string realSource = a.source.empty() ? std::string{} : a.source;
                alias.source = realSource + "|" + a.nodeKey;
                alias.confidence = a.confidence;
                aliases.push_back(std::move(alias));
            }
            wb->ops.emplace_back(AddAliasesOp{std::move(aliases)});
        }

        coord->enqueue(std::move(wb));
        auto fr = coord->flush();
        if (!fr) {
            resp.success = false;
            const auto validEdges = req.edges.size() - resp.edgesSkipped;
            const auto validAliases = req.aliases.size() - resp.aliasesSkipped;
            if (!req.nodes.empty()) {
                resp.errors.push_back("Node upsert failed: " + fr.error().message);
            } else if (validEdges > 0) {
                resp.errors.push_back("Edge insert failed: " + fr.error().message);
            } else if (validAliases > 0) {
                resp.success = true;
                resp.errors.push_back("Alias insert failed: " + fr.error().message);
            } else {
                resp.errors.push_back("Flush failed: " + fr.error().message);
            }
        } else {
            resp.success = true;
            resp.nodesInserted = static_cast<uint32_t>(req.nodes.size());
            resp.edgesInserted = static_cast<uint32_t>(req.edges.size() - resp.edgesSkipped);
            resp.aliasesInserted = static_cast<uint32_t>(req.aliases.size() - resp.aliasesSkipped);
        }
        spdlog::info("KgIngest (via WriteCoordinator) completed: {} nodes, {} edges, {} aliases",
                     resp.nodesInserted, resp.edgesInserted, resp.aliasesInserted);
        co_return resp;
    }

    resp.success = false;
    resp.errors.push_back("WriteCoordinator unavailable");
    co_return resp;
}

} // namespace yams::daemon
