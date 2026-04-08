#pragma once

#include <filesystem>
#include <optional>

namespace yams::daemon::client {

// Best-effort discovery of a live daemon socket by inspecting running yams-daemon processes.
// This is used when the configured/default socket path is not healthy, such as when a packaged
// system service is running on a different socket than the per-user default.
std::optional<std::filesystem::path>
discoverLiveDaemonSocket(const std::filesystem::path& preferredSocket = {},
                         const std::filesystem::path& pidFilePath = {},
                         bool allowAnyDaemonFallback = true);

} // namespace yams::daemon::client
