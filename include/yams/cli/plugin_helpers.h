#pragma once

#include <iostream>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>
#include <yams/cli/daemon_helpers.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>

namespace yams::cli {

namespace plugin {

using StatusResult = yams::Result<yams::daemon::StatusResponse>;

inline void
print_plugin_not_ready_hint(const std::optional<yams::daemon::StatusResponse>& statusOpt) {
    std::cout << "Plugin subsystem is not ready yet; the daemon is still initializing plugins."
              << '\n';
    if (statusOpt) {
        const auto& st = *statusOpt;
        auto it = st.readinessStates.find("plugins");
        if (it != st.readinessStates.end()) {
            std::cout << "  readiness.plugins=" << (it->second ? "true" : "false") << '\n';
        }
        if (!st.lifecycleState.empty()) {
            std::cout << "  lifecycle=" << st.lifecycleState << '\n';
        }
        if (!st.lastError.empty()) {
            std::cout << "  last_error=" << st.lastError << '\n';
        }
    }
    std::cout << "Inspect 'yams daemon status -d' for detailed plugin state.\n";
}

template <typename FetchStatusFn>
inline bool handle_plugin_rpc_error(const Error& err, const FetchStatusFn& fetchStatus,
                                    const std::string& actionLabel) {
    if (yams::cli::is_transport_failure(err)) {
        std::cout << actionLabel << " failed: " << err.message << '\n';
        return false;
    }
    if (err.code == ErrorCode::InvalidState) {
        std::optional<yams::daemon::StatusResponse> statusOpt;
        auto status = fetchStatus();
        if (status)
            statusOpt = status.value();
        print_plugin_not_ready_hint(statusOpt);
        return false;
    }
    std::cout << actionLabel << " failed: " << err.message << '\n';
    return false;
}

inline std::map<std::string, std::vector<std::string>>
parse_plugins_json_interfaces(const std::string& jsonStr) {
    std::map<std::string, std::vector<std::string>> ifaceMap;
    try {
        nlohmann::json pj = nlohmann::json::parse(jsonStr, nullptr, false);
        if (!pj.is_discarded()) {
            for (const auto& rec : pj) {
                auto name = rec.value("name", std::string{});
                if (!name.empty() && rec.contains("interfaces")) {
                    std::vector<std::string> v;
                    for (const auto& s : rec["interfaces"]) {
                        if (s.is_string())
                            v.push_back(s.get<std::string>());
                    }
                    if (!v.empty())
                        ifaceMap[name] = std::move(v);
                }
            }
        }
    } catch (...) {
    }
    return ifaceMap;
}

inline std::map<std::string, std::string> parse_plugins_json_types(const std::string& jsonStr) {
    std::map<std::string, std::string> typeMap;
    try {
        nlohmann::json pj = nlohmann::json::parse(jsonStr, nullptr, false);
        if (!pj.is_discarded()) {
            for (const auto& rec : pj) {
                auto name = rec.value("name", std::string{});
                if (!name.empty() && rec.contains("type")) {
                    typeMap[name] = rec["type"].get<std::string>();
                }
            }
        }
    } catch (...) {
    }
    return typeMap;
}

inline void print_interfaces(const std::vector<std::string>& interfaces,
                             const char* /*prefix*/ = "  ") {
    for (size_t i = 0; i < interfaces.size(); ++i) {
        if (i)
            std::cout << ", ";
        std::cout << interfaces[i];
    }
    std::cout << "\n";
}

inline void print_provider_info(const yams::daemon::StatusResponse::ProviderInfo& p, bool verbose,
                                const std::map<std::string, std::vector<std::string>>& ifaceMap) {
    std::cout << "  - " << p.name;
    if (verbose) {
        if (p.isProvider)
            std::cout << " [provider]";
        if (!p.ready)
            std::cout << " [not-ready]";
        if (p.degraded)
            std::cout << " [degraded]";
        if (p.modelsLoaded > 0)
            std::cout << " models=" << p.modelsLoaded;
        if (!p.error.empty())
            std::cout << " error=\"" << p.error << "\"";
        auto itf = ifaceMap.find(p.name);
        if (itf != ifaceMap.end() && !itf->second.empty()) {
            std::cout << " interfaces=";
            print_interfaces(itf->second, "");
        }
    }
    std::cout << "\n";
}

inline void print_plugin_list(const yams::daemon::StatusResponse& status,
                              const std::map<std::string, std::vector<std::string>>& ifaceMap,
                              bool verbose) {
    std::cout << "Loaded plugins (" << status.providers.size() << "):\n";
    for (const auto& p : status.providers) {
        print_provider_info(p, verbose, ifaceMap);
    }
}

inline void
print_skipped_plugins(const std::vector<yams::daemon::StatusResponse::PluginSkipInfo>& skipped) {
    if (!skipped.empty()) {
        std::cout << "\nSkipped plugins (" << skipped.size() << "):\n";
        for (const auto& sp : skipped) {
            std::cout << "  - " << sp.path << ": " << sp.reason << "\n";
        }
    }
}

inline void print_loaded_plugins_hint(
    const std::vector<yams::daemon::StatusResponse::ProviderInfo>& providers) {
    if (!providers.empty()) {
        std::cout << "Loaded plugins: ";
        bool first = true;
        for (const auto& p : providers) {
            if (!first)
                std::cout << ", ";
            std::cout << p.name;
            first = false;
        }
        std::cout << "\n";
    }
}

} // namespace plugin

} // namespace yams::cli
