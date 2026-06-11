#pragma once

#include <yams/core/types.h>
#include <yams/daemon/ipc/ipc_protocol_responses.h>

#include <boost/asio/awaitable.hpp>

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>

namespace yams::daemon {
class DaemonClient;
}

namespace yams::cli {

struct GraphTraversalQueryOptions {
    int depth{1};
    std::size_t limit{100};
    std::size_t offset{0};
    bool verbose{false};
    std::string relationFilter;
};

struct GraphSearchQueryOptions {
    std::string pattern;
    std::size_t limit{100};
    std::size_t offset{0};
    bool verbose{false};
};

struct GraphListByTypeQueryOptions {
    std::string nodeType;
    std::size_t limit{100};
    std::size_t offset{0};
    bool verbose{false};
};

struct DocumentGraphLookupOptions {
    std::string hash;
    std::string name;
    int depth{1};
    bool verbose{false};
};

boost::asio::awaitable<Result<yams::daemon::GraphQueryResponse>>
executeGraphListTypesQuery(yams::daemon::DaemonClient& client);

boost::asio::awaitable<Result<yams::daemon::GraphQueryResponse>>
executeGraphListRelationsQuery(yams::daemon::DaemonClient& client);

boost::asio::awaitable<Result<yams::daemon::GraphQueryResponse>>
executeGraphSearchQuery(yams::daemon::DaemonClient& client, const GraphSearchQueryOptions& options);

boost::asio::awaitable<Result<yams::daemon::GraphQueryResponse>>
executeGraphListByTypeQuery(yams::daemon::DaemonClient& client,
                            const GraphListByTypeQueryOptions& options);

boost::asio::awaitable<Result<yams::daemon::GraphQueryResponse>>
executeGraphTraversalByNode(yams::daemon::DaemonClient& client,
                            const GraphTraversalQueryOptions& options, const std::string& nodeKey,
                            std::optional<std::int64_t> nodeId = std::nullopt);

boost::asio::awaitable<Result<std::optional<yams::daemon::GraphQueryResponse>>>
executeGraphTraversalByNameCandidates(yams::daemon::DaemonClient& client,
                                      const GraphTraversalQueryOptions& options,
                                      const std::string& name, const std::filesystem::path& cwd);

boost::asio::awaitable<Result<yams::daemon::GetResponse>>
executeDocumentGraphLookup(yams::daemon::DaemonClient& client,
                           const DocumentGraphLookupOptions& options);

} // namespace yams::cli
