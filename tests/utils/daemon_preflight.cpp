#include "daemon_preflight.h"

#include <yams/cli/cli_sync.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>

using namespace std::chrono_literals;

namespace yams::test {

bool wait_for_daemon_ready(yams::daemon::DaemonClient& client, std::chrono::milliseconds max_wait,
                           std::string* last_status) {
    const auto deadline = std::chrono::steady_clock::now() + max_wait;
    while (std::chrono::steady_clock::now() < deadline) {
        auto st = yams::cli::run_sync(client.status(), 500ms);
        if (st) {
            const auto& s = st.value();
            if (last_status)
                *last_status = s.overallStatus;
            // Consider both Ready and Degraded as reachable (provider checks later)
            if (s.ready || s.overallStatus == "Ready" || s.overallStatus == "ready" ||
                s.overallStatus == "Degraded" || s.overallStatus == "degraded") {
                return true;
            }
        }
        std::this_thread::sleep_for(100ms);
    }
    return false;
}

bool ensure_embeddings_provider(yams::daemon::DaemonClient& client,
                                std::chrono::milliseconds max_wait, const std::string& model_name,
                                std::string* reason) {
    // Wait for daemon reachable first
    std::string status;
    if (!wait_for_daemon_ready(client, max_wait, &status)) {
        if (reason)
            *reason = "Daemon not ready in time (status=" + status + ")";
        return false;
    }

    // Try a tiny embedding to assert provider usability
    yams::daemon::GenerateEmbeddingRequest req;
    req.text = "_healthcheck_";
    req.modelName = model_name.empty() ? std::string{"all-MiniLM-L6-v2"} : model_name;
    req.normalize = true;

    auto res = yams::cli::run_sync(client.generateEmbedding(req), 2s);
    if (!res) {
        if (reason)
            *reason = std::string{"GenerateEmbedding failed: "} + res.error().message;
        return false;
    }
    if (res.value().embedding.empty()) {
        if (reason)
            *reason = "Embedding provider returned empty vector";
        return false;
    }
    return true;
}

} // namespace yams::test
