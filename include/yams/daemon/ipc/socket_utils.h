#pragma once

#include <filesystem>

namespace yams::daemon::socket_utils {

// Compatibility entry points backed by config::resolve_runtime_paths(). Both resolve the same
// immutable policy: explicit/environment socket, daemon.socket_path, then platform default.
// Conflicting socket aliases throw std::invalid_argument rather than selecting a daemon silently.
std::filesystem::path resolve_socket_path();
std::filesystem::path resolve_socket_path_config_first();

// Derive the proxy/control socket path from the main daemon socket path.
// Example: /tmp/yams-daemon.sock -> /tmp/yams-daemon.proxy.sock
std::filesystem::path derive_proxy_socket_path(const std::filesystem::path& mainSocketPath);

} // namespace yams::daemon::socket_utils
