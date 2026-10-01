#include <yams/config/config_helpers.h>
#include <yams/daemon/ipc/socket_utils.h>

#include <cstring>
#include <filesystem>
#include <stdexcept>
#include <utility>

#ifndef _WIN32
#include <unistd.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#endif

namespace yams::daemon::socket_utils {
namespace {

yams::config::ResolvedRuntimePaths resolveRuntimePathsOrThrow() {
    auto resolved = yams::config::resolve_runtime_paths();
    if (!resolved) {
        throw std::invalid_argument(resolved.error().message);
    }
    return std::move(resolved.value());
}

std::filesystem::path resolveClientSocketPath() {
    return yams::config::select_client_socket_path(resolveRuntimePathsOrThrow(),
                                                   yams::config::default_daemon_socket_probe(),
                                                   yams::config::system_daemon_socket_path());
}

} // namespace

std::filesystem::path resolve_socket_path() {
    return resolveClientSocketPath();
}

std::filesystem::path resolve_socket_path_config_first() {
    return resolveClientSocketPath();
}

std::filesystem::path resolve_own_daemon_socket_path() {
    return resolveRuntimePathsOrThrow().socketPath.value;
}

bool is_system_daemon_socket(const std::filesystem::path& socketPath) {
    const auto systemSocket = yams::config::system_daemon_socket_path();
    return !systemSocket.empty() && !socketPath.empty() &&
           socketPath.lexically_normal() == systemSocket;
}

std::optional<std::uint32_t> socket_peer_uid(const std::filesystem::path& socketPath) {
#ifdef _WIN32
    (void)socketPath;
    return std::nullopt;
#else
    const auto native = socketPath.string();
    sockaddr_un addr{};
    if (native.empty() || native.size() >= sizeof(addr.sun_path)) {
        return std::nullopt;
    }
    addr.sun_family = AF_UNIX;
    std::memcpy(addr.sun_path, native.c_str(), native.size() + 1);

    const int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        return std::nullopt;
    }
    std::optional<std::uint32_t> uid;
    if (::connect(fd, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) == 0) {
#if defined(__linux__)
        ucred cred{};
        socklen_t len = sizeof(cred);
        if (::getsockopt(fd, SOL_SOCKET, SO_PEERCRED, &cred, &len) == 0) {
            uid = static_cast<std::uint32_t>(cred.uid);
        }
#else
        uid_t peerUid = 0;
        gid_t peerGid = 0;
        if (::getpeereid(fd, &peerUid, &peerGid) == 0) {
            uid = static_cast<std::uint32_t>(peerUid);
        }
#endif
    }
    ::close(fd);
    return uid;
#endif
}

bool daemon_runs_as_other_user(const std::filesystem::path& socketPath) {
#ifdef _WIN32
    (void)socketPath;
    return false;
#else
    const auto peer = socket_peer_uid(socketPath);
    return peer.has_value() && *peer != static_cast<std::uint32_t>(::geteuid());
#endif
}

std::filesystem::path derive_proxy_socket_path(const std::filesystem::path& mainSocketPath) {
    if (mainSocketPath.empty()) {
        return {};
    }

    auto base = mainSocketPath.stem().string();
    if (base.empty()) {
        base = mainSocketPath.filename().string();
    }
    if (base.empty()) {
        base = "yams-daemon";
    }
    return mainSocketPath.parent_path() / (base + ".proxy.sock");
}

} // namespace yams::daemon::socket_utils
