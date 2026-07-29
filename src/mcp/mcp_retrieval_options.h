#pragma once

#include <yams/app/services/retrieval_service.h>
#include <yams/daemon/client/daemon_client.h>

namespace yams::mcp::detail {

inline app::services::RetrievalOptions
makeMcpRetrievalOptions(const daemon::ClientConfig& daemonClientConfig) {
    app::services::RetrievalOptions options;
    options.socketPath = daemonClientConfig.socketPath;
    options.requestTimeoutMs = 15000;
    options.headerTimeoutMs = 10000;
    options.bodyTimeoutMs = 60000;
    return options;
}

} // namespace yams::mcp::detail
