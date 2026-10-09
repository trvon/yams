#include <yams/cli/graph_query_execution.h>

#include <yams/cli/graph_helpers.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/metadata/kg_relation_summary.h>

namespace yams::cli {

namespace {

std::vector<std::string> canonicalRelationFilters(const std::string& relationFilter) {
    if (relationFilter.empty()) {
        return {};
    }

    std::vector<std::string> filters;
    auto canonicalRelation = yams::metadata::normalizeRelationName(relationFilter);
    if (!canonicalRelation.empty()) {
        filters.push_back(std::move(canonicalRelation));
    }
    return filters;
}

yams::daemon::GraphQueryRequest makeTraversalRequest(const GraphTraversalQueryOptions& options) {
    yams::daemon::GraphQueryRequest req;
    req.maxDepth = options.depth;
    req.maxResults = static_cast<std::uint32_t>(options.limit);
    req.maxResultsPerDepth = 100;
    req.offset = static_cast<std::uint32_t>(options.offset);
    req.limit = static_cast<std::uint32_t>(options.limit);
    req.includeNodeProperties = options.verbose;
    req.includeEdgeProperties = options.verbose;
    req.relationFilters = canonicalRelationFilters(options.relationFilter);
    return req;
}

} // namespace

boost::asio::awaitable<Result<yams::daemon::GraphQueryResponse>>
executeGraphListTypesQuery(yams::daemon::DaemonClient& client) {
    yams::daemon::GraphQueryRequest req;
    req.listTypes = true;
    co_return co_await client.call(req);
}

boost::asio::awaitable<Result<yams::daemon::GraphQueryResponse>>
executeGraphListRelationsQuery(yams::daemon::DaemonClient& client) {
    yams::daemon::GraphQueryRequest req;
    req.listRelations = true;
    co_return co_await client.call(req);
}

boost::asio::awaitable<Result<yams::daemon::GraphQueryResponse>>
executeGraphSearchQuery(yams::daemon::DaemonClient& client,
                        const GraphSearchQueryOptions& options) {
    yams::daemon::GraphQueryRequest req;
    req.searchMode = true;
    req.searchPattern = options.pattern;
    req.limit = static_cast<std::uint32_t>(options.limit);
    req.offset = static_cast<std::uint32_t>(options.offset);
    req.includeNodeProperties = options.verbose;
    co_return co_await client.call(req);
}

boost::asio::awaitable<Result<yams::daemon::GraphQueryResponse>>
executeGraphListByTypeQuery(yams::daemon::DaemonClient& client,
                            const GraphListByTypeQueryOptions& options) {
    yams::daemon::GraphQueryRequest req;
    req.listByType = true;
    req.nodeType = options.nodeType;
    req.limit = static_cast<std::uint32_t>(options.limit);
    req.offset = static_cast<std::uint32_t>(options.offset);
    req.includeNodeProperties = options.verbose;
    req.scopePathPrefix = options.scopePathPrefix;
    co_return co_await client.call(req);
}

boost::asio::awaitable<Result<yams::daemon::GraphQueryResponse>>
executeGraphTraversalByNode(yams::daemon::DaemonClient& client,
                            const GraphTraversalQueryOptions& options, const std::string& nodeKey,
                            std::optional<std::int64_t> nodeId) {
    auto req = makeTraversalRequest(options);
    req.nodeKey = nodeKey;
    req.nodeId = nodeId.value_or(-1);
    co_return co_await client.call(req);
}

boost::asio::awaitable<Result<yams::daemon::GetResponse>>
executeDocumentGraphLookup(yams::daemon::DaemonClient& client,
                           const DocumentGraphLookupOptions& options) {
    yams::daemon::GetRequest req;
    req.hash = options.hash;
    req.metadataOnly = true;
    req.showGraph = true;
    req.graphDepth = options.depth;
    req.verbose = options.verbose;
    if (options.name.empty()) {
        co_return co_await client.get(req);
    }

    // The daemon may run in another directory, so send a relative path as the absolute path
    // ingestion stored, then fall back to the name as given (file name or suffix match).
    req.byName = true;
    const auto candidates = buildGraphDocumentNameCandidates(options.name, options.cwd);
    Result<yams::daemon::GetResponse> result =
        Error{ErrorCode::NotFound, "Document not found with name: " + options.name};
    for (const auto& candidate : candidates) {
        req.name = candidate;
        result = co_await client.get(req);
        // Retry only when the name did not resolve; a resolved document's errors are final.
        if (result || result.error().code != ErrorCode::NotFound ||
            result.error().message.find("Document not found with name") == std::string::npos) {
            break;
        }
    }
    co_return result;
}

} // namespace yams::cli
