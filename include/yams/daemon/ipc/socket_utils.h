#pragma once

#include <cstdint>
#include <filesystem>
#include <optional>

namespace yams::daemon::socket_utils {

// Client entry points backed by config::resolve_runtime_paths() and
// config::select_client_socket_path(). Both resolve the same policy: explicit/environment socket,
// daemon.socket_path, the per-user platform default, and the packaged system daemon socket when
// no per-user daemon or data directory is configured. Conflicting socket aliases throw
// std::invalid_argument rather than selecting a daemon silently.
std::filesystem::path resolve_socket_path();
std::filesystem::path resolve_socket_path_config_first();

// Socket a daemon started by this user would bind: the same policy without system daemon
// discovery. Use it when writing a per-user service unit or spawning a daemon.
std::filesystem::path resolve_own_daemon_socket_path();

// True when the path is the packaged system service socket (/run/yams/yams-daemon.sock).
bool is_system_daemon_socket(const std::filesystem::path& socketPath);

// UID of the process listening on a Unix socket (SO_PEERCRED / getpeereid), or nullopt when
// nothing accepts the connection or the platform cannot tell. Opens one short connection.
std::optional<std::uint32_t> socket_peer_uid(const std::filesystem::path& socketPath);

// True when the daemon behind the socket runs as a different user than this process, so it
// cannot be assumed to read this user's files (the packaged system service, for example).
bool daemon_runs_as_other_user(const std::filesystem::path& socketPath);

// Derive the proxy/control socket path from the main daemon socket path.
// Example: /tmp/yams-daemon.sock -> /tmp/yams-daemon.proxy.sock
std::filesystem::path derive_proxy_socket_path(const std::filesystem::path& mainSocketPath);

} // namespace yams::daemon::socket_utils
