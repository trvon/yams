#pragma once

#include <chrono>
#include <string>
#include <vector>

#include <boost/asio/awaitable.hpp>

#include <yams/core/types.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>

namespace yams::cli::search_runner {

struct DaemonSearchOptions {
    std::string query;
    std::size_t limit{20};
    std::string searchType{"hybrid"};

    bool fuzzy{false};
    bool literalText{false};
    double similarity{0.7};

    bool showLineNumbers{false};
    bool symbolRank{true};

    // Server-side filters
    std::vector<std::string> pathPatterns;
    std::vector<std::string> tags;
    bool matchAllTags{false};
    std::string extension;
    std::string mimeType;
    std::string fileType;
    bool textOnly{false};
    bool binaryOnly{false};

    // Search timeout sent to daemon
    std::chrono::milliseconds timeout{std::chrono::seconds(5)};
};

struct DaemonSearchResult {
    yams::daemon::SearchResponse response;

    // Lightweight observability that doesn't depend on daemon IPC fields
    bool usedStreaming{false};
    bool usedFuzzyRetry{false};
    bool usedLiteralTextRetry{false};
    int attempts{0};
};

// Runs daemon search using the same retry behavior as the CLI:
// - tries streamingSearch when enabled
// - retries with fuzzy when 0 results and fuzzy==false
// - retries with literalText when parse-like errors and literalText==false
boost::asio::awaitable<Result<DaemonSearchResult>> daemon_search(yams::daemon::DaemonClient& client,
                                                                 const DaemonSearchOptions& opts,
                                                                 bool enableStreaming);

} // namespace yams::cli::search_runner
