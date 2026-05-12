#pragma once
#include <filesystem>
#include <optional>
#include <set>
#include <string>
#include <vector>
#include <yams/daemon/ipc/ipc_protocol.h>

namespace yams::cli {
class YamsCLI;
}

namespace yams::cli::doctor {

struct TrustedRootCheck {
    std::filesystem::path path;
    std::vector<std::string> issues;
};

struct PluginTrust {
    static std::set<std::filesystem::path> readTrusted();
    static bool resolveStrictPluginDirMode();
    static std::vector<std::filesystem::path>
    dedupeRoots(const std::vector<std::filesystem::path>& roots);
    static std::vector<std::filesystem::path> getDefaultPluginRoots(bool strictMode);
    static std::optional<std::vector<std::filesystem::path>>
    fetchTrustedRootsFromDaemon(YamsCLI* cli);
    static std::vector<TrustedRootCheck>
    assessTrustedRoots(const std::vector<std::filesystem::path>& trustedRoots, bool strictMode,
                       const std::vector<std::filesystem::path>& defaultRoots);
    static bool isTrustedPath(const std::filesystem::path& p,
                              const std::set<std::filesystem::path>& roots);
    static std::optional<std::filesystem::path> resolveByName(const std::string& name);
};

} // namespace yams::cli::doctor
