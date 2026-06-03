#include <yams/cli/graph_query_execution.h>

#include <yams/cli/graph_helpers.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/metadata/kg_relation_summary.h>
#include <yams/metadata/path_utils.h>

#include <spdlog/spdlog.h>

namespace yams::cli {

namespace {

std::string buildPathFileNodeKey(const std::string& path) {
    try {
        auto derived = yams::metadata::computePathDerivedValues(path);
        if (!derived.normalizedPath.empty()) {
            return "path:file:" + derived.normalizedPath;
        }
    } catch (const std::exception& e) {
        spdlog::trace("graph: path-derived node key fallback for '{}': {}", path, e.what());
    } catch (...) {
        spdlog::trace("graph: path-derived node key fallback for '{}'", path);
    }
    return "path:file:" + path;
}

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

boost::asio::awaitable<Result<std::optional<yams::daemon::GraphQueryResponse>>>
executeGraphTraversalByNameCandidates(yams::daemon::DaemonClient& client,
                                      const GraphTraversalQueryOptions& options,
                                      const std::string& name, const std::filesystem::path& cwd) {
    const auto candidates = build_graph_file_node_candidates(name, cwd);
    for (const auto& candidate : candidates) {
        auto req = makeTraversalRequest(options);
        req.nodeKey = buildPathFileNodeKey(candidate);

        auto result = co_await client.call(req);
        if (result && result.value().kgAvailable && result.value().originNode.nodeId > 0) {
            co_return std::optional<yams::daemon::GraphQueryResponse>{std::move(result.value())};
        }
    }

    co_return std::optional<yams::daemon::GraphQueryResponse>{std::nullopt};
}

boost::asio::awaitable<Result<yams::daemon::GetResponse>>
executeDocumentGraphLookup(yams::daemon::DaemonClient& client,
                           const DocumentGraphLookupOptions& options) {
    yams::daemon::GetRequest req;
    req.hash = options.hash;
    req.name = options.name;
    req.byName = !options.name.empty();
    req.metadataOnly = true;
    req.showGraph = true;
    req.graphDepth = options.depth;
    req.verbose = options.verbose;
    co_return co_await client.get(req);
}

} // namespace yams::cli
