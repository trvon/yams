// Test preflight helpers for daemon/provider readiness
#pragma once

#include <chrono>
#include <string>

namespace yams {
namespace daemon {
class DaemonClient;
struct StatusResponse;
struct GenerateEmbeddingRequest;
} // namespace daemon
} // namespace yams

namespace yams::test {

// Polls daemon status until overall ready/degraded within max_wait.
// Returns true on reachable status; fills last status string.
bool wait_for_daemon_ready(yams::daemon::DaemonClient& client, std::chrono::milliseconds max_wait,
                           std::string* last_status);

// Ensures an embeddings provider is usable by attempting a trivial embedding
// with a short timeout. Returns true if embedding succeeds.
bool ensure_embeddings_provider(yams::daemon::DaemonClient& client,
                                std::chrono::milliseconds max_wait, const std::string& model_name,
                                std::string* reason);

} // namespace yams::test
