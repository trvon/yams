#include <yams/daemon/client/sandbox_detection.h>

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <filesystem>
#include <string>

#include <yams/config/config_helpers.h>
#include <yams/daemon/client/daemon_client.h>

namespace yams::daemon {
namespace {

std::string normalize(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return value;
}

bool is_true(std::string_view raw) {
    const std::string value = normalize(std::string(raw));
    return value == "1" || value == "true" || value == "on" || value == "yes";
}

bool is_false(std::string_view raw) {
    const std::string value = normalize(std::string(raw));
    return value == "0" || value == "false" || value == "off" || value == "no";
}

bool is_auto(std::string_view raw) {
    return normalize(std::string(raw)) == "auto";
}

bool in_container() {
    std::error_code ec;
    if (std::filesystem::exists("/.dockerenv", ec)) {
        return true;
    }
    if (const char* container = std::getenv("container"); container && *container) {
        return true;
    }
    return false;
}

} // namespace

ClientTransportMode resolve_transport_mode(const ClientConfig& config) {
    if (config.transportMode == ClientTransportMode::InProcess ||
        config.transportMode == ClientTransportMode::Socket) {
        return config.transportMode;
    }

    bool autoProbe = false;

    if (const char* env = std::getenv("YAMS_EMBEDDED"); env && *env) {
        if (is_true(env)) {
            return ClientTransportMode::InProcess;
        }
        if (is_false(env)) {
            return ClientTransportMode::Socket;
        }
        if (is_auto(env)) {
            autoProbe = true;
        }
    }

    if (!autoProbe) {
        const auto daemonMode = yams::config::resolve_daemon_mode_from_config();
        if (!daemonMode.empty()) {
            if (is_true(daemonMode) || normalize(daemonMode) == "embedded" ||
                normalize(daemonMode) == "in_process") {
                return ClientTransportMode::InProcess;
            }
            if (is_false(daemonMode) || normalize(daemonMode) == "socket" ||
                normalize(daemonMode) == "daemon") {
                return ClientTransportMode::Socket;
            }
            if (is_auto(daemonMode)) {
                autoProbe = true;
            }
        }
    }

    if (!autoProbe) {
        return ClientTransportMode::Socket;
    }

    if (const char* inDaemon = std::getenv("YAMS_IN_DAEMON"); inDaemon && *inDaemon) {
        return ClientTransportMode::Socket;
    }

    if (in_container()) {
        return ClientTransportMode::InProcess;
    }

    const auto socketPath = config.socketPath.empty() ? DaemonClient::resolveSocketPathConfigFirst()
                                                      : config.socketPath;
    std::error_code ec;
    if (socketPath.empty() || !std::filesystem::exists(socketPath, ec)) {
        return ClientTransportMode::InProcess;
    }

    return ClientTransportMode::Socket;
}

} // namespace yams::daemon
