// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (c) 2025 YAMS Contributors

#include <spdlog/spdlog.h>
#include <algorithm>
#include <cctype>
#include <chrono>
#include <future>
#include <limits>
#include <unordered_set>
#include <yams/app/services/graph_context_service.hpp>
#include <yams/app/services/graph_query_service.hpp>
#include <yams/app/services/graph_scope_service.hpp>
#include <yams/core/assert.hpp>
#include <yams/daemon/components/dispatch_response.hpp>
#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/metadata/kg_relation_summary.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/profiling.h>

namespace yams::daemon {

namespace {

std::vector<std::string> canonicalizeRelationFilters(const std::vector<std::string>& input) {
    std::vector<std::string> out;
    out.reserve(input.size());
    for (const auto& relation : input) {
        auto canonical = metadata::normalizeRelationName(relation);
        if (!canonical.empty()) {
            out.push_back(std::move(canonical));
        }
    }
    return out;
}

std::string snippetModeToString(app::services::GraphContextSnippetMode mode) {
    switch (mode) {
        case app::services::GraphContextSnippetMode::Full:
            return "full";
        case app::services::GraphContextSnippetMode::Omitted:
            return "omitted";
    }
    YAMS_UNREACHABLE("unknown graph context snippet mode in dispatcher");
}

GraphExploreSymbol mapGraphExploreSymbol(const app::services::GraphContextSymbol& in) {
    GraphExploreSymbol out;
    out.nodeKey = in.nodeKey;
    out.label = in.label;
    out.qualifiedName = in.qualifiedName;
    out.kind = in.kind;
    out.filePath = in.filePath;
    out.startLine = in.startLine;
    out.endLine = in.endLine;
    out.score = in.score;
    out.exactMatch = in.exactMatch;
    out.generatedOrCache = in.generatedOrCache;
    out.testFile = in.testFile;
    return out;
}

GraphExploreResponse mapGraphExploreResponse(const app::services::GraphExploreResponse& in) {
    GraphExploreResponse out;
    out.query = in.query;
    out.totalSymbolsConsidered = in.totalSymbolsConsidered;
    out.totalFilesConsidered = in.totalFilesConsidered;
    out.emittedChars = in.emittedChars;
    out.snippetRenderMicros = static_cast<std::uint64_t>(in.snippetRenderMicros);
    out.snippetsRendered = static_cast<std::uint64_t>(in.snippetsRendered);
    out.kgAvailable = in.kgAvailable;
    out.truncated = in.truncated;
    out.warnings = in.warnings;

    out.entrySymbols.reserve(in.entrySymbols.size());
    for (const auto& symbol : in.entrySymbols) {
        out.entrySymbols.push_back(mapGraphExploreSymbol(symbol));
    }

    out.files.reserve(in.files.size());
    for (const auto& file : in.files) {
        GraphExploreSnippet snippet;
        snippet.filePath = file.filePath;
        snippet.language = file.language;
        snippet.mode = snippetModeToString(file.mode);
        snippet.startLine = file.startLine;
        snippet.endLine = file.endLine;
        snippet.heading = file.heading;
        snippet.content = file.content;
        snippet.truncated = file.truncated;
        snippet.symbols.reserve(file.symbols.size());
        for (const auto& symbol : file.symbols) {
            snippet.symbols.push_back(mapGraphExploreSymbol(symbol));
        }
        out.files.push_back(std::move(snippet));
    }

    out.relationships.reserve(in.relationships.size());
    for (const auto& relation : in.relationships) {
        GraphExploreRelation outRelation;
        outRelation.relation = relation.relation;
        outRelation.sourceNodeKey = relation.sourceNodeKey;
        outRelation.sourceLabel = relation.sourceLabel;
        outRelation.targetNodeKey = relation.targetNodeKey;
        outRelation.targetLabel = relation.targetLabel;
        outRelation.weight = relation.weight;
        outRelation.confidence = relation.confidence;
        outRelation.provenance = relation.provenance.value_or("");
        out.relationships.push_back(std::move(outRelation));
    }

    return out;
}

GraphExploreSnippet mapGraphSnippet(const app::services::GraphContextSnippet& in) {
    GraphExploreSnippet snippet;
    snippet.filePath = in.filePath;
    snippet.language = in.language;
    snippet.mode = snippetModeToString(in.mode);
    snippet.startLine = in.startLine;
    snippet.endLine = in.endLine;
    snippet.heading = in.heading;
    snippet.content = in.content;
    snippet.truncated = in.truncated;
    snippet.symbols.reserve(in.symbols.size());
    for (const auto& symbol : in.symbols) {
        snippet.symbols.push_back(mapGraphExploreSymbol(symbol));
    }
    return snippet;
}

GraphExploreRelation mapGraphRelation(const app::services::GraphContextRelation& in) {
    GraphExploreRelation out;
    out.relation = in.relation;
    out.sourceNodeKey = in.sourceNodeKey;
    out.sourceLabel = in.sourceLabel;
    out.targetNodeKey = in.targetNodeKey;
    out.targetLabel = in.targetLabel;
    out.weight = in.weight;
    out.confidence = in.confidence;
    out.provenance = in.provenance.value_or("");
    return out;
}

GraphSymbolLookupResponse
mapGraphSymbolLookupResponse(const app::services::GraphSymbolLookupResponse& in) {
    GraphSymbolLookupResponse out;
    out.symbol = in.symbol;
    out.ambiguous = in.ambiguous;
    out.truncated = in.truncated;
    out.snippetRenderMicros = static_cast<std::uint64_t>(in.snippetRenderMicros);
    out.snippetsRendered = static_cast<std::uint64_t>(in.snippetsRendered);
    out.warnings = in.warnings;
    out.matches.reserve(in.matches.size());
    for (const auto& match : in.matches) {
        out.matches.push_back(mapGraphExploreSymbol(match));
    }
    out.snippets.reserve(in.snippets.size());
    for (const auto& snippet : in.snippets) {
        out.snippets.push_back(mapGraphSnippet(snippet));
    }
    out.trail.reserve(in.trail.size());
    for (const auto& relation : in.trail) {
        out.trail.push_back(mapGraphRelation(relation));
    }
    return out;
}

GraphTraceResponse mapGraphTraceResponse(const app::services::GraphTraceResponse& in) {
    GraphTraceResponse out;
    out.from = in.from;
    out.to = in.to;
    out.found = in.found;
    out.truncated = in.truncated;
    out.snippetRenderMicros = static_cast<std::uint64_t>(in.snippetRenderMicros);
    out.snippetsRendered = static_cast<std::uint64_t>(in.snippetsRendered);
    out.warnings = in.warnings;
    out.path.reserve(in.path.size());
    for (const auto& relation : in.path) {
        out.path.push_back(mapGraphRelation(relation));
    }
    out.snippets.reserve(in.snippets.size());
    for (const auto& snippet : in.snippets) {
        out.snippets.push_back(mapGraphSnippet(snippet));
    }
    return out;
}

} // namespace

using namespace yams::app::services;
using namespace yams::metadata;

namespace {

// Wait for the writer to settle a graph op. A broken promise (op dropped unapplied) and a
// timeout both mean the outcome is unknown to this request and are reported as errors.
// `settledByWriter` is true only when the writer published the result, i.e. when it is safe to
// read the completion's other fields (the promise orders those writes before the read).
template <typename T> struct GraphCompletionWait {
    Result<T> result;
    bool settledByWriter = false;
};

template <typename T>
GraphCompletionWait<T> awaitGraphCompletion(std::future<Result<T>>& settled,
                                            std::chrono::milliseconds timeout) {
    if (settled.wait_for(timeout) != std::future_status::ready) {
        return {Error{ErrorCode::Timeout, "graph write did not finish within " +
                                              std::to_string(timeout.count()) +
                                              " ms; its outcome is unknown"},
                false};
    }
    try {
        return {settled.get(), true};
    } catch (const std::future_error&) {
        return {Error{ErrorCode::InternalError, "graph write was dropped before it was applied"},
                false};
    }
}

} // namespace

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
        // NOLINTNEXTLINE(bugprone-unchecked-optional-access): checked above
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

boost::asio::awaitable<Response>
RequestDispatcher::handleGraphExploreRequest(const GraphExploreRequest& req) {
    spdlog::debug("GraphExplore request: query='{}', maxFiles={}, includeCode={}, includeTests={}",
                  req.query, req.maxFiles, req.includeCode, req.includeTests);

    auto metaRepo = serviceManager_ ? serviceManager_->getMetadataRepo() : nullptr;
    if (!metaRepo) {
        co_return dispatch::makeErrorResponse(ErrorCode::InternalError,
                                              "Metadata repository unavailable");
    }

    auto kgStore = metaRepo->getKnowledgeGraphStore();
    if (!kgStore) {
        GraphExploreResponse resp;
        resp.query = req.query;
        resp.kgAvailable = false;
        resp.warnings.push_back("Knowledge graph not available");
        co_return resp;
    }

    auto graphService = app::services::makeGraphContextService(kgStore, metaRepo);
    if (!graphService) {
        co_return dispatch::makeErrorResponse(ErrorCode::InternalError,
                                              "Failed to create graph context service");
    }

    app::services::GraphExploreRequest serviceReq;
    serviceReq.query = req.query;
    serviceReq.scopePathPrefix = req.scopePathPrefix;
    serviceReq.budget.maxFiles = static_cast<std::size_t>(req.maxFiles);
    serviceReq.budget.maxSymbols = static_cast<std::size_t>(req.maxSymbols);
    serviceReq.budget.maxTotalChars = static_cast<std::size_t>(req.maxTotalChars);
    serviceReq.budget.maxCharsPerFile = static_cast<std::size_t>(req.maxCharsPerFile);
    serviceReq.budget.maxSnippetLines = static_cast<std::size_t>(req.maxSnippetLines);
    serviceReq.budget.includeLineNumbers = req.includeLineNumbers;
    serviceReq.budget.includeRelationships = req.includeRelationships;
    serviceReq.includeCode = req.includeCode;
    serviceReq.includeTests = req.includeTests;

    auto result = graphService->explore(serviceReq);
    if (!result) {
        co_return dispatch::makeErrorResponse(result.error().code, result.error().message);
    }

    auto response = mapGraphExploreResponse(result.value());
    spdlog::debug("GraphExplore: returning {} files, {} symbols, truncated={}",
                  response.files.size(), response.entrySymbols.size(), response.truncated);
    co_return response;
}

boost::asio::awaitable<Response>
RequestDispatcher::handleGraphSymbolLookupRequest(const GraphSymbolLookupRequest& req) {
    YAMS_ZONE_SCOPED_N("dispatch::graphSymbolLookup");
    auto metaRepo = serviceManager_ ? serviceManager_->getMetadataRepo() : nullptr;
    if (!metaRepo) {
        co_return dispatch::makeErrorResponse(ErrorCode::InternalError,
                                              "Metadata repository unavailable");
    }
    auto kgStore = metaRepo->getKnowledgeGraphStore();
    if (!kgStore) {
        GraphSymbolLookupResponse resp;
        resp.symbol = req.symbol;
        resp.warnings.push_back("Knowledge graph not available");
        co_return resp;
    }
    auto graphService = app::services::makeGraphContextService(kgStore, metaRepo);
    if (!graphService) {
        co_return dispatch::makeErrorResponse(ErrorCode::InternalError,
                                              "Failed to create graph context service");
    }
    app::services::GraphSymbolLookupRequest serviceReq;
    serviceReq.symbol = req.symbol;
    if (req.hasFile) {
        serviceReq.file = req.file;
    }
    if (req.hasLine) {
        serviceReq.line = req.line;
    }
    serviceReq.budget.maxFiles = static_cast<std::size_t>(req.maxFiles);
    serviceReq.budget.maxSymbols = static_cast<std::size_t>(req.maxSymbols);
    serviceReq.budget.maxTotalChars = static_cast<std::size_t>(req.maxTotalChars);
    serviceReq.budget.maxCharsPerFile = static_cast<std::size_t>(req.maxCharsPerFile);
    serviceReq.budget.maxSnippetLines = static_cast<std::size_t>(req.maxSnippetLines);
    serviceReq.budget.includeLineNumbers = req.includeLineNumbers;
    serviceReq.includeCode = req.includeCode;

    auto result = graphService->lookupSymbol(serviceReq);
    if (!result) {
        co_return dispatch::makeErrorResponse(result.error().code, result.error().message);
    }
    co_return mapGraphSymbolLookupResponse(result.value());
}

boost::asio::awaitable<Response>
RequestDispatcher::handleGraphTraceRequest(const GraphTraceRequest& req) {
    YAMS_ZONE_SCOPED_N("dispatch::graphTrace");
    auto metaRepo = serviceManager_ ? serviceManager_->getMetadataRepo() : nullptr;
    if (!metaRepo) {
        co_return dispatch::makeErrorResponse(ErrorCode::InternalError,
                                              "Metadata repository unavailable");
    }
    auto kgStore = metaRepo->getKnowledgeGraphStore();
    if (!kgStore) {
        GraphTraceResponse resp;
        resp.from = req.from;
        resp.to = req.to;
        resp.warnings.push_back("Knowledge graph not available");
        co_return resp;
    }
    auto graphService = app::services::makeGraphContextService(kgStore, metaRepo);
    if (!graphService) {
        co_return dispatch::makeErrorResponse(ErrorCode::InternalError,
                                              "Failed to create graph context service");
    }
    app::services::GraphTraceRequest serviceReq;
    serviceReq.from = req.from;
    serviceReq.to = req.to;
    serviceReq.maxDepth = static_cast<std::size_t>(req.maxDepth);
    serviceReq.budget.maxFiles = static_cast<std::size_t>(req.maxFiles);
    serviceReq.budget.maxSymbols = static_cast<std::size_t>(req.maxSymbols);
    serviceReq.budget.maxTotalChars = static_cast<std::size_t>(req.maxTotalChars);
    serviceReq.budget.maxCharsPerFile = static_cast<std::size_t>(req.maxCharsPerFile);
    serviceReq.budget.maxSnippetLines = static_cast<std::size_t>(req.maxSnippetLines);
    serviceReq.budget.includeLineNumbers = req.includeLineNumbers;

    auto result = graphService->trace(serviceReq);
    if (!result) {
        co_return dispatch::makeErrorResponse(result.error().code, result.error().message);
    }
    co_return mapGraphTraceResponse(result.value());
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

    GraphQueryResponse resp;
    resp.kgAvailable = true;
    const auto scopedRanges = req.scopePathPrefix.empty()
                                  ? std::vector<metadata::KGPathRange>{}
                                  : app::services::buildGraphCwdScopePathRanges(
                                        std::filesystem::path(req.scopePathPrefix));
    auto totalCount = scopedRanges.empty()
                          ? kgStore->countNodesByType(req.nodeType)
                          : kgStore->countNodesByTypeInPathRanges(req.nodeType, scopedRanges);
    if (!totalCount) {
        co_return dispatch::makeErrorResponse(totalCount.error().code, totalCount.error().message);
    }
    if (static_cast<std::uint64_t>(totalCount.value()) >
        static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
        co_return dispatch::makeErrorResponse(ErrorCode::InvalidData,
                                              "Invalid graph node count for listByType");
    }
    const auto totalNodeCount = static_cast<std::size_t>(totalCount.value());
    std::vector<metadata::KGNode> nodes;
    if (scopedRanges.empty()) {
        auto nodesResult = kgStore->findNodesByType(req.nodeType, req.limit, req.offset);
        if (!nodesResult) {
            co_return dispatch::makeErrorResponse(nodesResult.error().code,
                                                  nodesResult.error().message);
        }
        nodes = std::move(nodesResult.value());
        resp.totalNodesFound = static_cast<uint64_t>(totalNodeCount);
        const auto returnedThrough = static_cast<std::uint64_t>(req.offset) + nodes.size();
        resp.truncated = returnedThrough < resp.totalNodesFound;
    } else {
        auto nodesResult =
            kgStore->findNodesByTypeInPathRanges(req.nodeType, scopedRanges, req.limit, req.offset);
        if (!nodesResult) {
            co_return dispatch::makeErrorResponse(nodesResult.error().code,
                                                  nodesResult.error().message);
        }
        nodes = std::move(nodesResult.value());
        resp.totalNodesFound = static_cast<uint64_t>(totalNodeCount);
        const auto returnedThrough = static_cast<std::uint64_t>(req.offset) + nodes.size();
        resp.truncated = returnedThrough < resp.totalNodesFound;
    }
    resp.maxDepthReached = 0;

    dispatch::GraphQueryResponseMapper::setOriginNode(resp, -1, "", "listByType:" + req.nodeType,
                                                      "query");

    resp.connectedNodes = dispatch::KGNodeMapper::mapKGNodes(nodes, req.includeNodeProperties);

    spdlog::debug("GraphQuery listByType: returning {} nodes of type '{}'",
                  resp.connectedNodes.size(), req.nodeType);
    co_return resp;
}

boost::asio::awaitable<Response>
RequestDispatcher::handleGraphQueryIsolatedMode(const GraphQueryRequest& req,
                                                KnowledgeGraphStore* kgStore) {
    // The pre-v0.20 default (function nodes without incoming calls edges) queried the removed
    // code-symbol graph. Default to documents instead: with semantic_neighbor below, isolated
    // means a document that no other document lists as an embedding neighbour.
    const std::string nodeType = req.nodeType.empty() ? std::string{"document"} : req.nodeType;
    // Default to semantic_neighbor: "isolated" then means a node with no embedding neighbours,
    // the orphan signal that still exists now that code-symbol calls edges are gone.
    std::string relation = req.isolatedRelation.empty()
                               ? "semantic_neighbor"
                               : metadata::normalizeRelationName(req.isolatedRelation);

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
    if (!coord) {
        resp.success = false;
        resp.errors.push_back("WriteCoordinator unavailable");
        co_return resp;
    }

    // One op classifies and writes every row inside the writer's transaction, so the counts
    // below are rows inserted/updated/skipped, not request sizes.
    IngestGraphOp op;
    op.skipExistingNodes = req.skipExistingNodes;
    op.skipExistingEdges = req.skipExistingEdges;
    op.ns = req.ns;
    op.nodes.reserve(req.nodes.size());
    for (const auto& n : req.nodes) {
        KGNode node;
        node.nodeKey = n.nodeKey;
        node.label = n.label.empty() ? std::nullopt : std::make_optional(n.label);
        node.type = n.type.empty() ? std::nullopt : std::make_optional(n.type);
        node.properties = n.properties.empty() ? std::nullopt : std::make_optional(n.properties);
        op.nodes.push_back(std::move(node));
    }
    op.edges.reserve(req.edges.size());
    for (const auto& edge : req.edges) {
        DeferredEdgeOp deferred;
        deferred.srcNodeKey = edge.srcNodeKey;
        deferred.dstNodeKey = edge.dstNodeKey;
        deferred.relation = edge.relation;
        deferred.weight = edge.weight;
        deferred.properties =
            edge.properties.empty() ? std::nullopt : std::make_optional(edge.properties);
        op.edges.push_back(std::move(deferred));
    }
    op.aliases.reserve(req.aliases.size());
    for (const auto& a : req.aliases) {
        op.aliases.push_back(GraphIngestAlias{a.nodeKey, a.alias, a.source, a.confidence});
    }
    auto completion = std::make_shared<GraphIngestCompletion>();
    auto settled = completion->promise.get_future();
    op.completion = completion;

    auto wb = std::make_unique<WriteBatch>();
    wb->source = "RPC::ingestGraph";
    wb->ops.emplace_back(std::move(op));
    coord->enqueue(std::move(wb));
    // The writer settles this op only after its transaction committed (or failed): no shared
    // flush state, no reading a result the writer is still producing.
    auto waited = awaitGraphCompletion(settled, coord->graphOpResultTimeout());
    auto& result = waited.result;
    if (!result) {
        // Nothing from this request is durable; report no rows.
        resp.success = false;
        const auto& message = result.error().message;
        // The stage is only meaningful (and only safe to read) once the writer settled the op.
        const auto stage =
            waited.settledByWriter ? completion->failedStage : GraphIngestCompletion::Stage::None;
        switch (stage) {
            case GraphIngestCompletion::Stage::Nodes:
                resp.errors.push_back("Node upsert failed: " + message);
                break;
            case GraphIngestCompletion::Stage::Edges:
                resp.errors.push_back("Edge insert failed: " + message);
                break;
            case GraphIngestCompletion::Stage::Aliases:
                resp.errors.push_back("Alias insert failed: " + message);
                break;
            case GraphIngestCompletion::Stage::None:
                resp.errors.push_back("Graph ingest not committed: " + message);
                break;
        }
        co_return resp;
    }

    const auto& outcome = result.value();
    resp.success = true;
    resp.nodesInserted = outcome.nodesInserted;
    resp.nodesUpdated = outcome.nodesUpdated;
    resp.nodesSkipped = outcome.nodesSkipped;
    resp.edgesInserted = outcome.edgesInserted;
    resp.edgesUpdated = outcome.edgesUpdated;
    resp.edgesSkipped = outcome.edgesSkipped;
    resp.aliasesInserted = outcome.aliasesInserted;
    resp.aliasesUpdated = outcome.aliasesUpdated;
    resp.aliasesSkipped = outcome.aliasesSkipped;
    resp.unresolvedEdges.reserve(outcome.unresolvedEdges.size());
    for (const auto& edge : outcome.unresolvedEdges) {
        if (edge.srcMissing) {
            resp.errors.push_back("Edge source node not found: " + edge.srcNodeKey);
        }
        if (edge.dstMissing) {
            resp.errors.push_back("Edge destination node not found: " + edge.dstNodeKey);
        }
        resp.unresolvedEdges.push_back(KgIngestUnresolvedEdge{
            edge.srcNodeKey, edge.dstNodeKey, edge.relation, edge.srcMissing, edge.dstMissing});
    }
    for (const auto& key : outcome.unresolvedAliasKeys) {
        resp.errors.push_back("Alias node not found: " + key);
    }
    resp.nsConflicts.reserve(outcome.nsConflicts.size());
    for (const auto& c : outcome.nsConflicts) {
        const auto target = c.kind == "node"
                                ? c.nodeKey
                                : c.srcNodeKey + " -[" + c.relation + "]-> " + c.dstNodeKey;
        resp.errors.push_back("Namespace conflict (" + c.reason + ") on " + c.kind + " " + target);
        resp.nsConflicts.push_back(KgIngestNsConflict{c.kind, c.nodeKey, c.srcNodeKey, c.dstNodeKey,
                                                      c.relation, c.existingNs, c.requestedNs,
                                                      c.reason});
    }
    spdlog::info("KgIngest committed: nodes +{} ~{} ={}, edges +{} ~{} ={} ({} unresolved), "
                 "aliases +{} ~{} ={}, {} ns conflicts",
                 resp.nodesInserted, resp.nodesUpdated, resp.nodesSkipped, resp.edgesInserted,
                 resp.edgesUpdated, resp.edgesSkipped, resp.unresolvedEdges.size(),
                 resp.aliasesInserted, resp.aliasesUpdated, resp.aliasesSkipped,
                 resp.nsConflicts.size());
    co_return resp;
}

boost::asio::awaitable<Response>
RequestDispatcher::handleKgRemoveRequest(const KgRemoveRequest& req) {
    if (req.ns.empty()) {
        co_return dispatch::makeErrorResponse(
            ErrorCode::InvalidArgument,
            "Graph removal requires a namespace (properties.ns); relation-only removal is not "
            "supported");
    }
    auto* coord = serviceManager_ ? serviceManager_->getWriteCoordinator() : nullptr;
    if (!coord) {
        co_return dispatch::makeErrorResponse(ErrorCode::NotInitialized,
                                              "WriteCoordinator unavailable");
    }

    DeleteGraphNamespaceOp op;
    op.ns = req.ns;
    if (!req.relation.empty()) {
        op.relation = req.relation;
    }
    op.dryRun = req.dryRun;
    op.includeForeignEdges = req.includeForeignEdges;
    auto completion = std::make_shared<GraphRemoveCompletion>();
    auto settled = completion->promise.get_future();
    op.completion = completion;

    auto wb = std::make_unique<WriteBatch>();
    wb->source = "RPC::removeGraphNamespace";
    wb->ops.emplace_back(std::move(op));
    coord->enqueue(std::move(wb));
    auto waited = awaitGraphCompletion(settled, coord->graphOpResultTimeout());
    auto& result = waited.result;
    if (!result) {
        co_return dispatch::makeErrorResponse(result.error().code,
                                              "Graph removal failed: " + result.error().message);
    }

    const auto& counts = result.value();
    KgRemoveResponse resp;
    resp.nodesRemoved = static_cast<uint64_t>(std::max<std::int64_t>(0, counts.nodesDeleted));
    resp.edgesRemoved = static_cast<uint64_t>(std::max<std::int64_t>(0, counts.edgesDeleted));
    resp.foreignEdgesRemoved =
        static_cast<uint64_t>(std::max<std::int64_t>(0, counts.foreignEdges));
    resp.refused = counts.refused;
    resp.dryRun = req.dryRun;
    spdlog::info("KgRemove ns='{}' relation='{}' dry_run={} refused={}: {} nodes, {} edges "
                 "({} foreign)",
                 req.ns, req.relation, req.dryRun, resp.refused, resp.nodesRemoved,
                 resp.edgesRemoved, resp.foreignEdgesRemoved);
    co_return resp;
}

} // namespace yams::daemon
