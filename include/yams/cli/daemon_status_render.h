#pragma once

#include <filesystem>
#include <functional>
#include <ostream>

namespace yams::daemon {
struct StatusResponse;
struct MemorySyncResponse;
} // namespace yams::daemon

namespace yams::cli {

// Inputs to `yams daemon status` that come from the CLI process rather than from the daemon reply.
struct DaemonStatusRenderContext {
    // Data directory this CLI/config expects the daemon to serve; empty when unknown.
    std::filesystem::path configuredDataDir;
    // --socket / YAMS_DAEMON_SOCKET was given, so a differing data directory is expected.
    bool explicitCustomSocket{false};
    // Best-effort memory-sync status; nullptr when the fetch failed. Rendered only when started.
    const yams::daemon::MemorySyncResponse* memorySync{nullptr};
};

// Pure StatusResponse -> text renderers for `yams daemon status` (brief) and `-d` (detailed).
// No RPCs and no process state: everything a test can drive through the arguments.
void renderDaemonStatusBrief(const yams::daemon::StatusResponse& status,
                             const DaemonStatusRenderContext& ctx, std::ostream& os);
void renderDaemonStatusDetailed(const yams::daemon::StatusResponse& status,
                                const DaemonStatusRenderContext& ctx, std::ostream& os);
void renderMemorySyncSection(const yams::daemon::MemorySyncResponse* sync, std::ostream& os);

// Render primary status and then invoke a best-effort optional-section producer.
void renderDaemonStatusWithOptionalSection(
    const yams::daemon::StatusResponse& status, const DaemonStatusRenderContext& ctx, bool detailed,
    std::ostream& os, const std::function<void(std::ostream&)>& optionalSection);

} // namespace yams::cli
