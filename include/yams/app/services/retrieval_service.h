#pragma once

#include <filesystem>
#include <functional>
#include <optional>

#include <yams/core/types.h>
// Ensure boost::asio symbols used by daemon_client.h are declared in TU including this header
#include <boost/asio/awaitable.hpp>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>

namespace yams::app::services {

struct RetrievalOptions {
    // Optional explicit daemon socket path (overrides default resolver)
    std::optional<std::filesystem::path> socketPath;
    std::optional<std::filesystem::path> explicitDataDir; // only when user overrides
    bool enableStreaming{true};
    bool progressiveOutput{false};
    bool singleUseConnections{false};
    std::size_t maxChunkSize{512 * 1024};
    int headerTimeoutMs{30000};
    int bodyTimeoutMs{120000};
    int requestTimeoutMs{30000};
};

class RetrievalService {
public:
    RetrievalService() = default;

    Result<yams::daemon::GetResponse> get(const yams::daemon::GetRequest& req,
                                          const RetrievalOptions& opts) const;

    Result<yams::daemon::GrepResponse> grep(const yams::daemon::GrepRequest& req,
                                            const RetrievalOptions& opts) const;

    Result<yams::daemon::ListResponse> list(const yams::daemon::ListRequest& req,
                                            const RetrievalOptions& opts) const;

    Result<void> getToStdout(const yams::daemon::GetInitRequest& req,
                             const RetrievalOptions& opts) const;

    Result<void> getToFile(const yams::daemon::GetInitRequest& req,
                           const std::filesystem::path& outputPath,
                           const RetrievalOptions& opts) const;

    struct ChunkedGetResult {
        yams::daemon::GetInitResponse init;
        std::string content; // potentially truncated to cap
    };

    Result<ChunkedGetResult> getChunkedBuffer(const yams::daemon::GetInitRequest& req,
                                              std::size_t capBytes,
                                              const RetrievalOptions& opts) const;

    // Smart name resolution and retrieval: tries resolver->session list->hybrid search, then get
    Result<yams::daemon::GetResponse>
    getByNameSmart(const std::string& name, bool oldest, bool includeContent, bool useSession,
                   const std::string& sessionName, const RetrievalOptions& opts,
                   std::function<Result<std::string>(const std::string&)> resolver = {}) const;
};

} // namespace yams::app::services
