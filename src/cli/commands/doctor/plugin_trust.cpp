#include <yams/cli/doctor/plugin_trust.h>
#include <yams/cli/daemon_helpers.h>
#include <yams/cli/plugin_util.h>
#include <yams/cli/result_helpers.h>
#include <yams/cli/yams_cli.h>
#include <yams/config/config_helpers.h>
#include <yams/daemon/client/daemon_client.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <system_error>

namespace yams::cli::doctor {

namespace {

bool parseBoolValue(std::string value, bool fallback) {
    if (value.empty()) {
        return fallback;
    }
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return value == "1" || value == "true" || value == "yes" || value == "on";
}

} // namespace

std::set<std::filesystem::path> PluginTrust::readTrusted() {
    return plugin::readTrustedRoots();
}

bool PluginTrust::resolveStrictPluginDirMode() {
    bool strictMode = false;
    try {
        auto cfg = yams::config::parse_simple_toml(yams::config::get_config_path());
        if (auto it = cfg.find("daemon.plugin_dir_strict"); it != cfg.end()) {
            strictMode = parseBoolValue(it->second, strictMode);
        }
    } catch (...) {
    }
    if (const char* envStrict = std::getenv("YAMS_PLUGIN_DIR_STRICT")) {
        strictMode = parseBoolValue(envStrict, strictMode);
    }
    return strictMode;
}

std::vector<std::filesystem::path>
PluginTrust::dedupeRoots(const std::vector<std::filesystem::path>& roots) {
    std::set<std::string> seen;
    std::vector<std::filesystem::path> unique;
    unique.reserve(roots.size());
    for (const auto& p : roots) {
        auto key = p.lexically_normal().string();
        if (seen.insert(key).second) {
            unique.push_back(p);
        }
    }
    return unique;
}

std::vector<std::filesystem::path> PluginTrust::getDefaultPluginRoots(bool strictMode) {
    if (strictMode) {
        return {};
    }
    return dedupeRoots(plugin::getPluginSearchDirs());
}

std::optional<std::vector<std::filesystem::path>>
PluginTrust::fetchTrustedRootsFromDaemon(YamsCLI* cli) {
    using namespace yams::daemon;
    try {
        ClientConfig cfg;
        if (cli && cli->hasExplicitDataDir()) {
            cfg.dataDir = cli->getDataPath();
        }
        cfg.requestTimeout = std::chrono::milliseconds(5000);
        auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(cfg);
        if (!leaseRes) {
            return std::nullopt;
        }
        auto leaseHandle = std::move(leaseRes.value());
        auto& client = **leaseHandle;
        PluginTrustListRequest req;
        auto res = yams::cli::run_result<PluginTrustListResponse>(client.call(req),
                                                                  std::chrono::milliseconds(5000));
        if (!res) {
            return std::nullopt;
        }
        std::vector<std::filesystem::path> out;
        out.reserve(res.value().paths.size());
        for (const auto& p : res.value().paths) {
            out.emplace_back(p);
        }
        return out;
    } catch (...) {
        return std::nullopt;
    }
}

std::vector<TrustedRootCheck>
PluginTrust::assessTrustedRoots(const std::vector<std::filesystem::path>& trustedRoots,
                                bool strictMode,
                                const std::vector<std::filesystem::path>& defaultRoots) {
    namespace fs = std::filesystem;
    std::vector<TrustedRootCheck> checks;
    checks.reserve(trustedRoots.size());

    std::set<fs::path> defaultRootSet(defaultRoots.begin(), defaultRoots.end());
    std::error_code tempEc;
    fs::path tempRoot = fs::temp_directory_path(tempEc);
    std::set<fs::path> tempSet;
    if (!tempEc) {
        tempSet.insert(tempRoot);
    }

    for (const auto& root : trustedRoots) {
        TrustedRootCheck check;
        check.path = root;

        std::error_code ec;
        auto canonical = fs::weakly_canonical(root, ec);
        if (ec) {
            canonical = root.lexically_normal();
        }

        if (!fs::exists(root, ec) || ec) {
            check.issues.push_back("missing");
        }

        if (!tempSet.empty() && plugin::isPathTrusted(canonical, tempSet)) {
            check.issues.push_back("temporary-path");
        }

        {
            auto normalized = canonical.lexically_normal().string();
            std::transform(normalized.begin(), normalized.end(), normalized.begin(),
                           [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
            std::replace(normalized.begin(), normalized.end(), '\\', '/');
            if (normalized.find("/build/") != std::string::npos ||
                normalized.find("/builddir") != std::string::npos ||
                normalized.find("/cmake-build") != std::string::npos) {
                check.issues.push_back("build-artifact-path");
            }
        }

        if (!strictMode && !defaultRootSet.empty() &&
            plugin::isPathTrusted(canonical, defaultRootSet)) {
            check.issues.push_back("covered-by-default-root");
        }

        checks.push_back(std::move(check));
    }

    return checks;
}

bool PluginTrust::isTrustedPath(const std::filesystem::path& p,
                                const std::set<std::filesystem::path>& roots) {
    return plugin::isPathTrusted(p, roots);
}

std::optional<std::filesystem::path> PluginTrust::resolveByName(const std::string& name) {
    namespace fs = std::filesystem;
    std::error_code ec;

    for (const auto& dir : plugin::getPluginSearchDirs()) {
        if (!fs::exists(dir, ec) || !fs::is_directory(dir, ec)) {
            continue;
        }

        for (const auto& entry : fs::directory_iterator(dir, ec)) {
            if (!entry.is_regular_file(ec)) {
                continue;
            }

            const auto& path = entry.path();
            if (!plugin::hasPluginExtension(path)) {
                continue;
            }

            const std::string stem = path.stem().string();
            const std::string filename = path.filename().string();
            if (stem == name || filename == name || filename.find(name) != std::string::npos) {
                return path;
            }
        }
    }

    return std::nullopt;
}

} // namespace yams::cli::doctor
