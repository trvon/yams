#include <yams/config/config_helpers.h>
#include <yams/daemon/ipc/socket_utils.h>

#include <filesystem>
#include <stdexcept>

namespace yams::daemon::socket_utils {
namespace {

std::filesystem::path resolveConfiguredSocketPath() {
    auto resolved = yams::config::resolve_runtime_paths();
    if (!resolved) {
        throw std::invalid_argument(resolved.error().message);
    }
    return resolved.value().socketPath.value;
}

} // namespace

std::filesystem::path resolve_socket_path() {
    return resolveConfiguredSocketPath();
}

std::filesystem::path resolve_socket_path_config_first() {
    return resolveConfiguredSocketPath();
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
