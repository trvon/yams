#pragma once

#include <filesystem>

namespace yams::daemon::socket_utils {

// Resolve the AF_UNIX socket path using environment first (YAMS_DAEMON_SOCKET),
// then XDG_RUNTIME_DIR, then a per-user /tmp fallback for non-root, or /var/run for root.
std::filesystem::path resolve_socket_path();

// Resolve with config-first semantics: try YAMS_DAEMON_SOCKET, then read
// $XDG_CONFIG_HOME/yams/config.toml or $HOME/.config/yams/config.toml for
// daemon.socket_path. If not found, fall back to resolve_socket_path().
std::filesystem::path resolve_socket_path_config_first();

// Derive the proxy/control socket path from the main daemon socket path.
// Example: /tmp/yams-daemon.sock -> /tmp/yams-daemon.proxy.sock
std::filesystem::path derive_proxy_socket_path(const std::filesystem::path& mainSocketPath);

} // namespace yams::daemon::socket_utils
