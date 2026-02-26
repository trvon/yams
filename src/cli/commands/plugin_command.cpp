#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <future>
#include <iostream>
#include <map>
#include <optional>
#include <set>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <yams/cli/command.h>
#include <yams/cli/daemon_helpers.h>
#include <yams/cli/plugin_helpers.h>
#include <yams/cli/result_helpers.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/cli/yams_cli.h>
#include <yams/config/config_helpers.h>
#include <yams/core/types.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/plugins/plugin_installer.hpp>
#include <yams/plugins/plugin_repo_client.hpp>

namespace yams::cli {

static auto dedupePluginRoots(std::vector<std::filesystem::path> roots)
    -> std::vector<std::filesystem::path> {
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

class PluginCommand : public ICommand {
public:
    std::string getName() const override { return "plugin"; }
    std::string getDescription() const override {
        return "Manage plugins (list/scan/info/load/unload/trust)";
    }

    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;
        auto register_subs = [this](CLI::App* plugin) {
            plugin->require_subcommand(1);

            // plugin search (catalog)
            std::string indexPath;
            auto* search =
                plugin->add_subcommand("search", "Search plugin catalog (local index.json)");
            search->add_option("--index", indexPath,
                               "Path to catalog JSON (default: docs/plugins/index.json)");
            search->callback([&indexPath]() {
                namespace fs = std::filesystem;
                fs::path idx =
                    indexPath.empty() ? fs::path("docs/plugins/index.json") : fs::path(indexPath);
                if (!fs::exists(idx)) {
                    std::cout << "Catalog not found: " << idx << "\n";
                    return;
                }
                try {
                    std::ifstream in(idx);
                    nlohmann::json j = nlohmann::json::parse(in);
                    auto arr = j.value("plugins", nlohmann::json::array());
                    std::cout << "Plugins (" << arr.size() << "):\n";
                    for (auto& it : arr) {
                        std::string name = it.value("name", "");
                        std::string transport = it.value("transport", "");
                        std::string repo = it.value("repo", "");
                        auto ifaces = it.value("interfaces", std::vector<std::string>{});
                        std::cout << "  - " << name << " [" << transport << "]";
                        if (!ifaces.empty()) {
                            std::cout << " interfaces=";
                            for (size_t i = 0; i < ifaces.size(); ++i) {
                                if (i)
                                    std::cout << ",";
                                std::cout << ifaces[i];
                            }
                        }
                        if (!repo.empty())
                            std::cout << " repo=" << repo;
                        std::cout << "\n";
                    }
                } catch (const std::exception& e) {
                    std::cout << "Failed to read catalog: " << e.what() << "\n";
                }
            });

            // plugin list
            auto* list = plugin->add_subcommand("list", "List loaded plugins and their interfaces");
            list->add_flag("-v,--verbose", verboseList_, "Show skipped plugins and reasons");
            list->callback([this]() { listPlugins(); });

            // plugin scan [--dir DIR] [TARGET]
            auto* scan =
                plugin->add_subcommand("scan", "Scan a directory or file for plugins (no init)");
            scan->add_option("--dir", scanDir_, "Directory to scan (default: search paths)");
            scan->add_option("target", scanTarget_, "Specific plugin file to scan (optional)");
            scan->callback([this]() { scanPlugins(scanDir_, scanTarget_); });

            // plugin info NAME
            auto* info = plugin->add_subcommand("info", "Show manifest/health for a plugin");
            info->add_option("name", infoName_, "Plugin name")->required();
            info->callback([this]() { showPluginInfo(infoName_); });

            // plugin health [NAME]
            auto* health = plugin->add_subcommand("health", "Show health status of loaded plugins");
            health->add_option("name", healthName_, "Plugin name (optional, shows all if omitted)");
            health->callback([this]() { showPluginHealth(healthName_); });

            // plugin load PATH|NAME [--config FILE] [--dry-run]
            auto* load = plugin->add_subcommand("load", "Load a plugin by path or name");
            load->add_option("path_or_name", loadArg_, "Plugin path or registered name")
                ->required();
            load->add_option("--config", configFile_,
                             "Configuration JSON/TOML for init (optional)");
            load->add_flag("--dry-run", dryRun_, "Scan only; do not initialize");
            load->callback([this]() { loadPlugin(loadArg_, configFile_, dryRun_); });

            // plugin unload NAME
            auto* unload = plugin->add_subcommand("unload", "Unload a plugin by name");
            unload->add_option("name", unloadName_, "Plugin name")->required();
            unload->callback([this]() { unloadPlugin(unloadName_); });

            // plugin trust (add/list/remove/reset/status)
            auto* trust = plugin->add_subcommand("trust", "Manage plugin trust policy");
            trust->require_subcommand(1);

            trust->add_subcommand("add", "Trust a plugin directory or file")
                ->add_option("path", trustPath_, "Path to trust")
                ->required();
            trust->get_subcommand("add")->callback([this]() { trustAdd(trustPath_); });

            trust->add_subcommand("list", "List trusted plugin paths")
                ->add_flag("--details", trustDetails_,
                           "Show trust file, defaults, strict mode, and effective roots");
            trust->get_subcommand("list")->callback([this]() { trustList(trustDetails_); });

            trust->add_subcommand("remove", "Remove a trusted plugin path")
                ->add_option("path", untrustPath_, "Path to remove")
                ->required();
            trust->get_subcommand("remove")->callback([this]() { trustRemove(untrustPath_); });

            trust->add_subcommand("reset", "Remove all persisted trusted plugin paths")
                ->callback([this]() { trustReset(); });

            trust->add_subcommand("status", "Show trust/discovery defaults and overrides")
                ->callback([this]() { trustStatus(); });

            // plugin install
            auto* install =
                plugin->add_subcommand("install", "Install plugin from repository or URL");
            install->add_option("source", installSrc_, "Plugin name[@version] or URL")->required();
            install->add_option("--dir", installDir_,
                                "Installation directory (default: ~/.local/lib/yams/plugins)");
            install->add_flag("--force,-f", installForce_, "Force reinstall if already installed");
            install->add_flag("--no-trust", installNoTrust_, "Don't add to trust list");
            install->add_flag("--no-load", installNoLoad_, "Don't load plugin after install");
            install->add_flag("--dry-run", installDryRun_, "Preview only, don't install");
            install->callback([this]() { installPlugin(); });

            // plugin repo (remote repository commands)
            auto* repo = plugin->add_subcommand("repo", "Query plugin repository");
            repo->require_subcommand(1);

            // plugin repo list
            auto* repoList = repo->add_subcommand("list", "List available plugins in repository");
            repoList->add_option("--filter", repoFilter_, "Filter plugins by name or interface");
            repoList->callback([this]() { repoListPlugins(); });

            // plugin repo info
            auto* repoInfo = repo->add_subcommand("info", "Show plugin details from repository");
            repoInfo->add_option("name", repoPluginName_, "Plugin name")->required();
            repoInfo->add_option("--version", repoVersion_, "Specific version (default: latest)");
            repoInfo->callback([this]() { repoShowInfo(); });

            // plugin repo versions
            auto* repoVersions = repo->add_subcommand("versions", "List available versions");
            repoVersions->add_option("name", repoPluginName_, "Plugin name")->required();
            repoVersions->callback([this]() { repoShowVersions(); });

            // plugin uninstall
            auto* uninstall = plugin->add_subcommand("uninstall", "Uninstall a plugin");
            uninstall->add_option("name", uninstallName_, "Plugin name")->required();
            uninstall->add_flag("--keep-trust", uninstallKeepTrust_, "Keep in trust list");
            uninstall->callback([this]() { uninstallPlugin(); });

            // plugin update
            auto* update = plugin->add_subcommand("update", "Update plugins to latest version");
            update->add_option("name", updateName_, "Plugin name (empty = check all)");
            update->add_flag("--all,-a", updateAll_, "Update all installed plugins");
            update->callback([this]() { updatePlugins(); });

            // plugin enable/disable/verify (stubs)
            plugin->add_subcommand("enable", "Enable a previously loaded plugin (stub)")
                ->add_option("name", nameToggle_, "Plugin name")
                ->required();
            plugin->get_subcommand("enable")->callback(
                [this]() { std::cout << "yams plugin enable: TODO name=" << nameToggle_ << "\n"; });

            plugin->add_subcommand("disable", "Disable a plugin (stub)")
                ->add_option("name", nameToggle_, "Plugin name")
                ->required();
            plugin->get_subcommand("disable")->callback([this]() {
                std::cout << "yams plugin disable: TODO name=" << nameToggle_ << "\n";
            });

            plugin->add_subcommand("verify", "Verify plugin signature/hash (stub)")
                ->add_option("name", nameToggle_, "Plugin name")
                ->required();
            plugin->get_subcommand("verify")->callback(
                [this]() { std::cout << "yams plugin verify: TODO name=" << nameToggle_ << "\n"; });
        };

        auto* plugin = app.add_subcommand(getName(), getDescription());
        register_subs(plugin);
        // Alias: "plugins" behaves the same as "plugin"
        auto* plugins = app.add_subcommand("plugins", getDescription());
        register_subs(plugins);
    }

    Result<void> execute() override { return Result<void>(); }

private:
    void listPlugins();
    void showPluginInfo(const std::string& name);
    void showPluginHealth(const std::string& name);
    void scanPlugins(const std::string& dir, const std::string& target);
    void loadPlugin(const std::string& arg, const std::string& cfg, bool dryRun);
    void unloadPlugin(const std::string& name);
    void trustList(bool details = false);
    void trustAdd(const std::string& path);
    void trustRemove(const std::string& path);
    void trustReset();
    void trustStatus();

    // New install/repo methods
    void installPlugin();
    void uninstallPlugin();
    void updatePlugins();
    void repoListPlugins();
    void repoShowInfo();
    void repoShowVersions();

    YamsCLI* cli_{nullptr};
    // Persistent option storage to avoid dangling references in callbacks
    std::string scanDir_;
    std::string scanTarget_;
    std::string infoName_;
    std::string healthName_;
    std::string loadArg_;
    std::string configFile_;
    bool dryRun_{false};
    std::string unloadName_;
    std::string trustPath_;
    std::string untrustPath_;
    bool trustDetails_{false};
    std::string installSrc_;
    std::string nameToggle_;
    bool verboseList_{false};

    // Install options
    std::string installDir_;
    bool installForce_{false};
    bool installNoTrust_{false};
    bool installNoLoad_{false};
    bool installDryRun_{false};

    // Repo options
    std::string repoFilter_;
    std::string repoPluginName_;
    std::string repoVersion_;

    // Uninstall options
    std::string uninstallName_;
    bool uninstallKeepTrust_{false};

    // Update options
    std::string updateName_;
    bool updateAll_{false};
};

// Factory
std::unique_ptr<ICommand> createPluginCommand() {
    return std::make_unique<PluginCommand>();
}

} // namespace yams::cli

namespace yams::cli {

namespace {

void printDaemonAcquireFailure(const char* actionLabel, const yams::Error& err) {
    if (yams::cli::is_transport_failure(err)) {
        std::cout << actionLabel << " failed: " << err.message << "\n";
        return;
    }
    std::cout << "Failed to acquire daemon client: " << err.message << "\n";
}

} // namespace

void PluginCommand::listPlugins() {
    using namespace yams::daemon;
    try {
        ClientConfig cfg;
        if (cli_->hasExplicitDataDir()) {
            cfg.dataDir = cli_->getDataPath();
        }
        cfg.enableChunkedResponses = false;
        cfg.requestTimeout = std::chrono::milliseconds(5000); // Fast timeout for simple list
        cfg.autoStart = true;
        auto leaseRes = yams::cli::acquire_cli_daemon_client_shared_with_fallback(
            cfg, yams::cli::CliDaemonAccessPolicy::AllowInProcessFallback);
        if (!leaseRes) {
            printDaemonAcquireFailure("Plugin list", leaseRes.error());
            return;
        }
        auto leaseHandle = std::move(leaseRes.value());
        auto& client = **leaseHandle;

        // Simple case: just get status with provider list (fast path)
        yams::daemon::StatusRequest sreq;
        sreq.detailed = true; // Need providers list
        auto sres = yams::cli::run_result<StatusResponse>(client.call(sreq),
                                                          std::chrono::milliseconds(5000));
        if (!sres) {
            std::cout << "Plugin list failed: " << sres.error().message << "\n";
            return;
        }

        const auto& st = sres.value();
        if (st.providers.empty()) {
            std::cout << "Loaded plugins (0):\n";
            return;
        }

        // Build interface map only when verbose (requires slower GetStats call)
        std::map<std::string, std::vector<std::string>> ifaceMap;
        if (verboseList_) {
            GetStatsRequest greq;
            greq.detailed = true;
            auto gres = yams::cli::run_result<GetStatsResponse>(client.call(greq),
                                                                std::chrono::milliseconds(10000));
            if (gres) {
                auto it = gres.value().additionalStats.find("plugins_json");
                if (it != gres.value().additionalStats.end()) {
                    try {
                        nlohmann::json pj = nlohmann::json::parse(it->second, nullptr, false);
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
                }
            }
        }

        std::cout << "Loaded plugins (" << st.providers.size() << "):\n";
        for (const auto& p : st.providers) {
            std::cout << "  - " << p.name;
            if (verboseList_) {
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
                    for (size_t i = 0; i < itf->second.size(); ++i) {
                        if (i)
                            std::cout << ",";
                        std::cout << itf->second[i];
                    }
                }
            }
            std::cout << "\n";
        }

        // When verbose, show skipped plugin diagnostics if present
        if (verboseList_ && !st.skippedPlugins.empty()) {
            std::cout << "\nSkipped plugins (" << st.skippedPlugins.size() << "):\n";
            for (const auto& sp : st.skippedPlugins) {
                std::cout << "  - " << sp.path << ": " << sp.reason << "\n";
            }
        }
    } catch (const std::exception& e) {
        std::cout << "Error: " << e.what() << "\n";
    }
}

void PluginCommand::showPluginInfo(const std::string& name) {
    using namespace yams::daemon;
    try {
        ClientConfig cfg;
        if (cli_->hasExplicitDataDir()) {
            cfg.dataDir = cli_->getDataPath();
        }
        cfg.enableChunkedResponses = false;
        cfg.requestTimeout = std::chrono::milliseconds(10000);
        auto leaseRes = yams::cli::acquire_cli_daemon_client_shared_with_fallback(
            cfg, yams::cli::CliDaemonAccessPolicy::AllowInProcessFallback);
        if (!leaseRes) {
            printDaemonAcquireFailure("Plugin info", leaseRes.error());
            return;
        }
        auto leaseHandle = std::move(leaseRes.value());
        auto& client = **leaseHandle;

        // First try StatusResponse which has typed provider info
        StatusRequest sreq;
        sreq.detailed = true;
        auto sres = yams::cli::run_result<StatusResponse>(client.call(sreq),
                                                          std::chrono::milliseconds(10000));

        // Also get stats for extended plugin info (interfaces, path, etc.)
        GetStatsRequest greq;
        greq.detailed = true;
        auto gres = yams::cli::run_result<GetStatsResponse>(client.call(greq),
                                                            std::chrono::milliseconds(10000));

        // Build combined info from both sources
        nlohmann::json pluginInfo;
        bool found = false;

        // Check StatusResponse.providers first (authoritative for loaded plugins)
        if (sres && !sres.value().providers.empty()) {
            for (const auto& p : sres.value().providers) {
                if (p.name == name) {
                    found = true;
                    pluginInfo["name"] = p.name;
                    pluginInfo["ready"] = p.ready;
                    pluginInfo["degraded"] = p.degraded;
                    if (!p.error.empty())
                        pluginInfo["error"] = p.error;
                    if (p.isProvider) {
                        pluginInfo["provider"] = true;
                        pluginInfo["models_loaded"] = p.modelsLoaded;
                    }
                    if (!p.interfaces.empty()) {
                        pluginInfo["interfaces"] = p.interfaces;
                    }
                    if (!p.capabilities.empty()) {
                        pluginInfo["capabilities"] = p.capabilities;
                    }
                    break;
                }
            }
        }

        // Enrich with plugins_json data (has path, interfaces, type)
        if (gres) {
            auto it = gres.value().additionalStats.find("plugins_json");
            if (it != gres.value().additionalStats.end()) {
                nlohmann::json pj = nlohmann::json::parse(it->second, nullptr, false);
                if (!pj.is_discarded()) {
                    for (const auto& rec : pj) {
                        if (rec.value("name", std::string()) == name) {
                            if (!found) {
                                // Use plugins_json as primary source if not in providers
                                pluginInfo = rec;
                                found = true;
                            } else {
                                // Merge additional fields from plugins_json
                                if (rec.contains("path"))
                                    pluginInfo["path"] = rec["path"];
                                if (rec.contains("type"))
                                    pluginInfo["type"] = rec["type"];
                                if (rec.contains("interfaces"))
                                    pluginInfo["interfaces"] = rec["interfaces"];
                            }
                            break;
                        }
                    }
                }
            }
        }

        if (found) {
            std::cout << pluginInfo.dump(2) << "\n";
        } else {
            std::cout << "Plugin not found: " << name << "\n";
            // Show available plugins as hint
            if (sres && !sres.value().providers.empty()) {
                std::cout << "Loaded plugins: ";
                bool first = true;
                for (const auto& p : sres.value().providers) {
                    if (!first)
                        std::cout << ", ";
                    std::cout << p.name;
                    first = false;
                }
                std::cout << "\n";
            }
        }
    } catch (const std::exception& e) {
        std::cout << "Error: " << e.what() << "\n";
    }
}

void PluginCommand::showPluginHealth(const std::string& name) {
    using namespace yams::daemon;
    try {
        ClientConfig cfg;
        if (cli_->hasExplicitDataDir()) {
            cfg.dataDir = cli_->getDataPath();
        }
        cfg.enableChunkedResponses = false;
        // Use shorter timeout for health check - it uses cached daemon metrics
        cfg.requestTimeout = std::chrono::milliseconds(5000);
        auto leaseRes = yams::cli::acquire_cli_daemon_client_shared_with_fallback(
            cfg, yams::cli::CliDaemonAccessPolicy::AllowInProcessFallback);
        if (!leaseRes) {
            printDaemonAcquireFailure("Plugin health", leaseRes.error());
            return;
        }
        auto leaseHandle = std::move(leaseRes.value());
        auto& client = **leaseHandle;

        // Get StatusResponse which has typed provider info (uses cached metrics - non-blocking)
        StatusRequest sreq;
        sreq.detailed = true;
        auto sres = yams::cli::run_result<StatusResponse>(client.call(sreq),
                                                          std::chrono::milliseconds(5000));
        if (!sres) {
            std::cout << "Plugin health failed: " << sres.error().message << "\n";
            return;
        }

        const auto& status = sres.value();
        if (status.providers.empty()) {
            std::cout << "No plugins loaded.\n";
            return;
        }

        // Also get stats for extended plugin info (interfaces, type) - uses cached metrics
        GetStatsRequest greq;
        greq.detailed = true;
        auto gres = yams::cli::run_result<GetStatsResponse>(client.call(greq),
                                                            std::chrono::milliseconds(5000));

        // Build interface map from plugins_json
        std::map<std::string, std::vector<std::string>> ifaceMap;
        std::map<std::string, std::string> typeMap;
        if (gres) {
            auto it = gres.value().additionalStats.find("plugins_json");
            if (it != gres.value().additionalStats.end()) {
                nlohmann::json pj = nlohmann::json::parse(it->second, nullptr, false);
                if (!pj.is_discarded()) {
                    for (const auto& rec : pj) {
                        auto pname = rec.value("name", std::string{});
                        if (!pname.empty()) {
                            if (rec.contains("interfaces")) {
                                std::vector<std::string> v;
                                for (const auto& s : rec["interfaces"]) {
                                    if (s.is_string())
                                        v.push_back(s.get<std::string>());
                                }
                                if (!v.empty())
                                    ifaceMap[pname] = std::move(v);
                            }
                            if (rec.contains("type"))
                                typeMap[pname] = rec["type"].get<std::string>();
                        }
                    }
                }
            }
        }

        bool found = false;
        for (const auto& p : status.providers) {
            // Filter by name if provided
            if (!name.empty() && p.name != name)
                continue;

            found = true;
            std::cout << "Plugin: " << p.name << "\n";

            // Status indicator
            if (p.ready && !p.degraded) {
                std::cout << "  Status: OK\n";
            } else if (p.degraded) {
                std::cout << "  Status: DEGRADED\n";
            } else {
                std::cout << "  Status: NOT READY\n";
            }

            // Plugin type
            auto tit = typeMap.find(p.name);
            if (tit != typeMap.end()) {
                std::cout << "  Type: " << tit->second << "\n";
            }

            // Role
            if (p.isProvider) {
                std::cout << "  Role: model_provider\n";
                std::cout << "  Models loaded: " << p.modelsLoaded << "\n";
            }

            // Interfaces
            auto iit = ifaceMap.find(p.name);
            if (iit != ifaceMap.end() && !iit->second.empty()) {
                std::cout << "  Interfaces: ";
                for (size_t i = 0; i < iit->second.size(); ++i) {
                    if (i)
                        std::cout << ", ";
                    std::cout << iit->second[i];
                }
                std::cout << "\n";
            }

            // Error
            if (!p.error.empty()) {
                std::cout << "  Error: " << p.error << "\n";
            }

            std::cout << "\n";
        }

        if (!found && !name.empty()) {
            std::cout << "Plugin not found: " << name << "\n";
            std::cout << "Loaded plugins: ";
            bool first = true;
            for (const auto& p : status.providers) {
                if (!first)
                    std::cout << ", ";
                std::cout << p.name;
                first = false;
            }
            std::cout << "\n";
        }
    } catch (const std::exception& e) {
        std::cout << "Error: " << e.what() << "\n";
    }
}

} // namespace yams::cli

namespace yams::cli {

void PluginCommand::scanPlugins(const std::string& dir, const std::string& target) {
    using namespace yams::daemon;
    try {
        ClientConfig cfg;
        if (cli_->hasExplicitDataDir()) {
            cfg.dataDir = cli_->getDataPath();
        }
        cfg.requestTimeout = std::chrono::milliseconds(15000);
        auto leaseRes = yams::cli::acquire_cli_daemon_client_shared_with_fallback(
            cfg, yams::cli::CliDaemonAccessPolicy::AllowInProcessFallback);
        if (!leaseRes) {
            printDaemonAcquireFailure("Plugin scan", leaseRes.error());
            return;
        }
        auto leaseHandle = std::move(leaseRes.value());
        auto& client = **leaseHandle;
        PluginScanRequest req;
        req.dir = dir;
        req.target = target;
        auto res = yams::cli::run_result<yams::daemon::PluginScanResponse>(
            client.call(req), std::chrono::milliseconds(15000));
        if (!res) {
            std::cout << "Scan failed: " << res.error().message << "\n";
            return;
        }
        const auto& r = res.value();
        std::cout << "Discovered plugins (" << r.plugins.size() << "):\n";
        for (const auto& pr : r.plugins) {
            std::cout << "  - " << pr.name << " v" << pr.version << " (ABI " << pr.abiVersion
                      << ")\n";
            std::cout << "    path: " << pr.path << "\n";
            if (!pr.interfaces.empty()) {
                std::cout << "    interfaces: ";
                for (size_t i = 0; i < pr.interfaces.size(); ++i) {
                    if (i)
                        std::cout << ", ";
                    std::cout << pr.interfaces[i];
                }
                std::cout << "\n";
            }
        }
    } catch (const std::exception& e) {
        std::cout << "Error: " << e.what() << "\n";
    }
}

void PluginCommand::loadPlugin(const std::string& arg, const std::string& cfgJson, bool dryRun) {
    using namespace yams::daemon;
    try {
        ClientConfig cfg;
        if (cli_->hasExplicitDataDir()) {
            cfg.dataDir = cli_->getDataPath();
        }
        cfg.requestTimeout = std::chrono::milliseconds(20000);
        auto leaseRes = yams::cli::acquire_cli_daemon_client_shared_with_fallback(
            cfg, yams::cli::CliDaemonAccessPolicy::AllowInProcessFallback);
        if (!leaseRes) {
            printDaemonAcquireFailure("Plugin load", leaseRes.error());
            return;
        }
        auto leaseHandle = std::move(leaseRes.value());
        auto& client = **leaseHandle;
        PluginLoadRequest req;
        req.pathOrName = arg;
        req.configJson = cfgJson;
        req.dryRun = dryRun;
        auto res = yams::cli::run_result<PluginLoadResponse>(client.call(req),
                                                             std::chrono::milliseconds(20000));
        if (!res) {
            auto statusFetcher = [&]() -> plugin::StatusResult {
                return yams::cli::run_result<yams::daemon::StatusResponse>(
                    client.status(), std::chrono::milliseconds(5000));
            };
            if (!plugin::handle_plugin_rpc_error(res.error(), statusFetcher, "Plugin load"))
                return;
            return;
        }
        const auto& r = res.value();
        std::cout << (r.loaded ? "Loaded" : "Scanned") << ": " << r.record.name << " v"
                  << r.record.version << " (ABI " << r.record.abiVersion << ")\n";
        if (!r.message.empty())
            std::cout << "  note: " << r.message << "\n";
    } catch (const std::exception& e) {
        std::cout << "Error: " << e.what() << "\n";
    }
}

void PluginCommand::unloadPlugin(const std::string& name) {
    using namespace yams::daemon;
    try {
        ClientConfig cfg;
        if (cli_->hasExplicitDataDir()) {
            cfg.dataDir = cli_->getDataPath();
        }
        cfg.requestTimeout = std::chrono::milliseconds(10000);
        auto leaseRes = yams::cli::acquire_cli_daemon_client_shared_with_fallback(
            cfg, yams::cli::CliDaemonAccessPolicy::AllowInProcessFallback);
        if (!leaseRes) {
            printDaemonAcquireFailure("Plugin unload", leaseRes.error());
            return;
        }
        auto leaseHandle = std::move(leaseRes.value());
        auto& client = **leaseHandle;
        PluginUnloadRequest req;
        req.name = name;
        auto res = yams::cli::run_result<SuccessResponse>(client.call(req),
                                                          std::chrono::milliseconds(10000));
        if (!res) {
            auto statusFetcher = [&]() -> plugin::StatusResult {
                return yams::cli::run_result<yams::daemon::StatusResponse>(
                    client.status(), std::chrono::milliseconds(5000));
            };
            if (!plugin::handle_plugin_rpc_error(res.error(), statusFetcher, "Plugin unload"))
                return;
            return;
        }
        std::cout << "Unloaded: " << name << "\n";
    } catch (const std::exception& e) {
        std::cout << "Error: " << e.what() << "\n";
    }
}

void PluginCommand::trustList(bool details) {
    using namespace yams::daemon;
    try {
        ClientConfig cfg;
        if (cli_->hasExplicitDataDir()) {
            cfg.dataDir = cli_->getDataPath();
        }
        cfg.requestTimeout = std::chrono::milliseconds(15000);
        auto leaseRes = yams::cli::acquire_cli_daemon_client_shared_with_fallback(
            cfg, yams::cli::CliDaemonAccessPolicy::AllowInProcessFallback);
        if (!leaseRes) {
            printDaemonAcquireFailure("Plugin trust list", leaseRes.error());
            return;
        }
        auto leaseHandle = std::move(leaseRes.value());
        auto& client = **leaseHandle;
        PluginTrustListRequest req;
        auto res = yams::cli::run_result<PluginTrustListResponse>(client.call(req),
                                                                  std::chrono::milliseconds(15000));
        if (!res) {
            auto statusFetcher = [&]() -> plugin::StatusResult {
                return yams::cli::run_result<yams::daemon::StatusResponse>(
                    client.status(), std::chrono::milliseconds(5000));
            };
            if (!plugin::handle_plugin_rpc_error(res.error(), statusFetcher, "Plugin trust list"))
                return;
            return;
        }
        const auto& r = res.value();
        std::cout << "Trusted plugin paths (" << r.paths.size() << "):\n";
        for (const auto& p : r.paths)
            std::cout << "  - " << p << "\n";

        if (details) {
            auto parseBool = [](std::string value, bool fallback) {
                if (value.empty()) {
                    return fallback;
                }
                std::transform(value.begin(), value.end(), value.begin(),
                               [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
                return value == "1" || value == "true" || value == "yes" || value == "on";
            };

            std::cout << "\nTrust file: " << yams::config::get_daemon_plugin_trust_file().string()
                      << "\n";
            std::cout << "Legacy trust file: "
                      << yams::config::get_legacy_plugin_trust_file().string() << "\n";

            bool strictMode = false;
            auto cfgMap = yams::config::parse_simple_toml(yams::config::get_config_path());
            if (auto it = cfgMap.find("daemon.plugin_dir_strict"); it != cfgMap.end()) {
                strictMode = parseBool(it->second, strictMode);
            }
            if (const char* envStrict = std::getenv("YAMS_PLUGIN_DIR_STRICT")) {
                strictMode = parseBool(envStrict, strictMode);
            }

            std::vector<std::filesystem::path> defaultRoots;
            if (!strictMode) {
#ifdef _WIN32
                defaultRoots.push_back(yams::config::get_data_dir() / "plugins");
#else
                if (const char* home = std::getenv("HOME")) {
                    defaultRoots.push_back(std::filesystem::path(home) / ".local" / "lib" / "yams" /
                                           "plugins");
                }
#ifdef __APPLE__
                defaultRoots.push_back(std::filesystem::path("/opt/homebrew/lib/yams/plugins"));
#endif
                defaultRoots.push_back(std::filesystem::path("/usr/local/lib/yams/plugins"));
                defaultRoots.push_back(std::filesystem::path("/usr/lib/yams/plugins"));
#endif
#ifdef YAMS_INSTALL_PREFIX
                defaultRoots.push_back(std::filesystem::path(YAMS_INSTALL_PREFIX) / "lib" / "yams" /
                                       "plugins");
#endif
            }
            defaultRoots = dedupePluginRoots(std::move(defaultRoots));

            std::cout << "Strict plugin-dir mode: " << (strictMode ? "on" : "off") << "\n";
            std::cout << "Default plugin roots (" << defaultRoots.size() << "):\n";
            for (const auto& p : defaultRoots) {
                std::cout << "  - " << p.string() << "\n";
            }
        }
    } catch (const std::exception& e) {
        std::cout << "Error: " << e.what() << "\n";
    }
}

void PluginCommand::trustAdd(const std::string& path) {
    using namespace yams::daemon;
    try {
        ClientConfig cfg;
        if (cli_->hasExplicitDataDir()) {
            cfg.dataDir = cli_->getDataPath();
        }
        cfg.requestTimeout = std::chrono::milliseconds(15000);
        auto leaseRes = yams::cli::acquire_cli_daemon_client_shared_with_fallback(
            cfg, yams::cli::CliDaemonAccessPolicy::AllowInProcessFallback);
        if (!leaseRes) {
            printDaemonAcquireFailure("Plugin trust add", leaseRes.error());
            return;
        }
        auto leaseHandle = std::move(leaseRes.value());
        auto& client = **leaseHandle;
        PluginTrustAddRequest req;
        req.path = path;
        auto res = yams::cli::run_result<SuccessResponse>(client.call(req),
                                                          std::chrono::milliseconds(15000));
        if (!res) {
            auto statusFetcher = [&]() -> plugin::StatusResult {
                return yams::cli::run_result<yams::daemon::StatusResponse>(
                    client.status(), std::chrono::milliseconds(5000));
            };
            if (!plugin::handle_plugin_rpc_error(res.error(), statusFetcher, "Plugin trust add"))
                return;
            return;
        }
        std::cout << "Trusted: " << path
                  << " (scan/load queued in background; run 'yams plugin list' shortly)\n";
    } catch (const std::exception& e) {
        std::cout << "Error: " << e.what() << "\n";
    }
}

void PluginCommand::trustRemove(const std::string& path) {
    using namespace yams::daemon;
    try {
        ClientConfig cfg;
        if (cli_->hasExplicitDataDir()) {
            cfg.dataDir = cli_->getDataPath();
        }
        cfg.requestTimeout = std::chrono::milliseconds(15000);
        auto leaseRes = yams::cli::acquire_cli_daemon_client_shared_with_fallback(
            cfg, yams::cli::CliDaemonAccessPolicy::AllowInProcessFallback);
        if (!leaseRes) {
            printDaemonAcquireFailure("Plugin trust remove", leaseRes.error());
            return;
        }
        auto leaseHandle = std::move(leaseRes.value());
        auto& client = **leaseHandle;
        PluginTrustRemoveRequest req;
        req.path = path;
        auto res = yams::cli::run_result<SuccessResponse>(client.call(req),
                                                          std::chrono::milliseconds(15000));
        if (!res) {
            auto statusFetcher = [&]() -> plugin::StatusResult {
                return yams::cli::run_result<yams::daemon::StatusResponse>(
                    client.status(), std::chrono::milliseconds(5000));
            };
            if (!plugin::handle_plugin_rpc_error(res.error(), statusFetcher, "Plugin trust remove"))
                return;
            return;
        }
        std::cout << "Untrusted: " << path << "\n";
    } catch (const std::exception& e) {
        std::cout << "Error: " << e.what() << "\n";
    }
}

void PluginCommand::trustReset() {
    using namespace yams::daemon;
    try {
        ClientConfig cfg;
        if (cli_->hasExplicitDataDir()) {
            cfg.dataDir = cli_->getDataPath();
        }
        cfg.requestTimeout = std::chrono::milliseconds(15000);
        auto leaseRes = yams::cli::acquire_cli_daemon_client_shared_with_fallback(
            cfg, yams::cli::CliDaemonAccessPolicy::AllowInProcessFallback);
        if (!leaseRes) {
            printDaemonAcquireFailure("Plugin trust reset", leaseRes.error());
            return;
        }

        auto leaseHandle = std::move(leaseRes.value());
        auto& client = **leaseHandle;

        PluginTrustListRequest listReq;
        auto listRes = yams::cli::run_result<PluginTrustListResponse>(
            client.call(listReq), std::chrono::milliseconds(15000));
        if (!listRes) {
            auto statusFetcher = [&]() -> plugin::StatusResult {
                return yams::cli::run_result<yams::daemon::StatusResponse>(
                    client.status(), std::chrono::milliseconds(5000));
            };
            (void)plugin::handle_plugin_rpc_error(listRes.error(), statusFetcher,
                                                  "Plugin trust reset");
            return;
        }

        const auto& paths = listRes.value().paths;
        if (paths.empty()) {
            std::cout << "Trust list is already empty.\n";
            return;
        }

        std::size_t removed = 0;
        std::vector<std::string> failed;
        for (const auto& path : paths) {
            PluginTrustRemoveRequest req;
            req.path = path;
            auto rmRes = yams::cli::run_result<SuccessResponse>(client.call(req),
                                                                std::chrono::milliseconds(15000));
            if (rmRes) {
                ++removed;
            } else {
                failed.push_back(path);
            }
        }

        std::cout << "Trust reset complete: removed " << removed << " path(s).\n";
        if (!failed.empty()) {
            std::cout << "Failed to remove " << failed.size() << " path(s):\n";
            for (const auto& p : failed) {
                std::cout << "  - " << p << "\n";
            }
        }
        std::cout << "Note: default plugin roots may still be scanned unless strict mode is enabled"
                  << " (YAMS_PLUGIN_DIR_STRICT=1 or daemon.plugin_dir_strict=true).\n";
    } catch (const std::exception& e) {
        std::cout << "Error: " << e.what() << "\n";
    }
}

void PluginCommand::trustStatus() {
    using namespace yams::daemon;

    auto parseBool = [](std::string value, bool fallback) {
        if (value.empty()) {
            return fallback;
        }
        std::transform(value.begin(), value.end(), value.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        return value == "1" || value == "true" || value == "yes" || value == "on";
    };

    auto splitEnvPaths = [](const char* raw) {
        std::vector<std::string> out;
        if (!raw || !*raw) {
            return out;
        }
        std::string cur;
        for (char ch : std::string(raw)) {
            if (ch == ':' || ch == ';') {
                if (!cur.empty()) {
                    out.push_back(cur);
                    cur.clear();
                }
                continue;
            }
            cur.push_back(ch);
        }
        if (!cur.empty()) {
            out.push_back(cur);
        }
        return out;
    };

    auto cfgMap = yams::config::parse_simple_toml(yams::config::get_config_path());
    bool strictMode = false;
    if (auto it = cfgMap.find("daemon.plugin_dir_strict"); it != cfgMap.end()) {
        strictMode = parseBool(it->second, strictMode);
    }
    if (const char* envStrict = std::getenv("YAMS_PLUGIN_DIR_STRICT")) {
        strictMode = parseBool(envStrict, strictMode);
    }

    std::vector<std::filesystem::path> defaultRoots;
    if (!strictMode) {
#ifdef _WIN32
        defaultRoots.push_back(yams::config::get_data_dir() / "plugins");
#else
        if (const char* home = std::getenv("HOME")) {
            defaultRoots.push_back(std::filesystem::path(home) / ".local" / "lib" / "yams" /
                                   "plugins");
        }
#ifdef __APPLE__
        defaultRoots.push_back(std::filesystem::path("/opt/homebrew/lib/yams/plugins"));
#endif
        defaultRoots.push_back(std::filesystem::path("/usr/local/lib/yams/plugins"));
        defaultRoots.push_back(std::filesystem::path("/usr/lib/yams/plugins"));
#endif
#ifdef YAMS_INSTALL_PREFIX
        defaultRoots.push_back(std::filesystem::path(YAMS_INSTALL_PREFIX) / "lib" / "yams" /
                               "plugins");
#endif
    }
    defaultRoots = dedupePluginRoots(std::move(defaultRoots));

    std::set<std::string> effectiveRoots;
    for (const auto& p : defaultRoots) {
        effectiveRoots.insert(p.string());
    }

    std::vector<std::string> trustedPaths;
    try {
        ClientConfig cfg;
        if (cli_->hasExplicitDataDir()) {
            cfg.dataDir = cli_->getDataPath();
        }
        cfg.requestTimeout = std::chrono::milliseconds(15000);
        auto leaseRes = yams::cli::acquire_cli_daemon_client_shared_with_fallback(
            cfg, yams::cli::CliDaemonAccessPolicy::AllowInProcessFallback);
        if (leaseRes) {
            auto leaseHandle = std::move(leaseRes.value());
            auto& client = **leaseHandle;
            PluginTrustListRequest req;
            auto res = yams::cli::run_result<PluginTrustListResponse>(
                client.call(req), std::chrono::milliseconds(15000));
            if (res) {
                trustedPaths = res.value().paths;
            }
        }
    } catch (...) {
    }

    for (const auto& p : trustedPaths) {
        effectiveRoots.insert(p);
    }

    auto envPluginDirs = splitEnvPaths(std::getenv("YAMS_PLUGIN_DIR"));
    for (const auto& p : envPluginDirs) {
        effectiveRoots.insert(p);
    }

    std::cout << "Plugin trust/discovery status\n";
    std::cout << "Trust file: " << yams::config::get_daemon_plugin_trust_file().string() << "\n";
    std::cout << "Legacy trust file: " << yams::config::get_legacy_plugin_trust_file().string()
              << "\n";
    std::cout << "Strict plugin-dir mode: " << (strictMode ? "on" : "off") << "\n";

    std::cout << "\nEnvironment overrides:\n";
    std::cout << "  YAMS_PLUGIN_DIR="
              << (std::getenv("YAMS_PLUGIN_DIR") ? std::getenv("YAMS_PLUGIN_DIR") : "(unset)")
              << "\n";
    std::cout << "  YAMS_PLUGIN_DIR_STRICT="
              << (std::getenv("YAMS_PLUGIN_DIR_STRICT") ? std::getenv("YAMS_PLUGIN_DIR_STRICT")
                                                        : "(unset)")
              << "\n";
    std::cout << "  YAMS_DISABLE_ABI_PLUGINS="
              << (std::getenv("YAMS_DISABLE_ABI_PLUGINS") ? std::getenv("YAMS_DISABLE_ABI_PLUGINS")
                                                          : "(unset)")
              << "\n";

    std::cout << "\nDefault plugin roots (" << defaultRoots.size() << "):\n";
    for (const auto& p : defaultRoots) {
        std::cout << "  - " << p.string() << "\n";
    }

    std::cout << "\nPersisted trusted roots (" << trustedPaths.size() << "):\n";
    for (const auto& p : trustedPaths) {
        std::cout << "  - " << p << "\n";
    }

    std::cout << "\nEffective scan roots (" << effectiveRoots.size() << "):\n";
    for (const auto& p : effectiveRoots) {
        std::cout << "  - " << p << "\n";
    }

    if (auto it = cfgMap.find("daemon.plugin_name_policy"); it != cfgMap.end()) {
        std::cout << "\nplugin_name_policy: " << it->second << "\n";
    }
}

void PluginCommand::installPlugin() {
    using namespace yams::plugins;

    try {
        // Create repository client and installer
        auto repoClient = makePluginRepoClient();
        auto installer = makePluginInstaller(
            std::shared_ptr<IPluginRepoClient>(repoClient.release()),
            installDir_.empty() ? std::filesystem::path{} : std::filesystem::path(installDir_));

        InstallOptions options;
        options.force = installForce_;
        options.autoTrust = !installNoTrust_;
        options.autoLoad = !installNoLoad_;
        options.dryRun = installDryRun_;

        // Progress callback
        options.onProgress = [](const InstallProgress& progress) {
            const char* stageNames[] = {"Querying",   "Downloading", "Verifying", "Extracting",
                                        "Installing", "Trusting",    "Loading",   "Complete"};
            int stageIdx = static_cast<int>(progress.stage);
            if (stageIdx >= 0 && stageIdx < 8) {
                std::cout << "\r[" << stageNames[stageIdx] << "] " << progress.message;
                if (progress.totalBytes > 0) {
                    std::cout << " (" << (progress.bytesDownloaded / 1024) << "/"
                              << (progress.totalBytes / 1024) << " KB)";
                }
                std::cout << std::flush;
                if (progress.stage == InstallProgress::Stage::Complete) {
                    std::cout << "\n";
                }
            }
        };

        auto result = installer->install(installSrc_, options);
        returnOnError(result, "Installation");

        const auto& r = result.value();
        std::cout << "\nInstalled: " << r.pluginName << " v" << r.version << "\n";
        std::cout << "  Path: " << r.installedPath.string() << "\n";
        std::cout << "  Size: " << ui::format_bytes(r.sizeBytes) << "\n";
        std::cout << "  Checksum: " << r.checksum << "\n";
        if (r.wasUpgrade) {
            std::cout << "  Upgraded from: v" << r.previousVersion << "\n";
        }
        std::cout << "  Elapsed: " << r.elapsed.count() << " ms\n";

        if (!installNoLoad_) {
            std::cout << "\nTo load the plugin, run: yams plugin load " << r.pluginName << "\n";
        }
    } catch (const std::exception& e) {
        std::cout << "Error: " << e.what() << "\n";
    }
}

void PluginCommand::uninstallPlugin() {
    using namespace yams::plugins;

    try {
        auto repoClient = makePluginRepoClient();
        auto installer =
            makePluginInstaller(std::shared_ptr<IPluginRepoClient>(repoClient.release()));

        auto result = installer->uninstall(uninstallName_, !uninstallKeepTrust_);
        returnOnError(result, "Uninstall");

        std::cout << "Uninstalled: " << uninstallName_ << "\n";
    } catch (const std::exception& e) {
        std::cout << "Error: " << e.what() << "\n";
    }
}

void PluginCommand::updatePlugins() {
    using namespace yams::plugins;

    try {
        auto repoClient = makePluginRepoClient();
        auto installer =
            makePluginInstaller(std::shared_ptr<IPluginRepoClient>(repoClient.release()));

        // Check for updates
        auto updates = installer->checkUpdates(updateAll_ ? "" : updateName_);
        if (!updates) {
            std::cout << "Failed to check for updates: " << updates.error().message << "\n";
            return;
        }

        if (updates.value().empty()) {
            std::cout << "All plugins are up to date.\n";
            return;
        }

        std::cout << "Available updates:\n";
        for (const auto& [name, version] : updates.value()) {
            auto currentVer = installer->installedVersion(name);
            std::string current =
                (currentVer && currentVer.value()) ? *currentVer.value() : "unknown";
            std::cout << "  " << name << ": " << current << " -> " << version << "\n";
        }

        if (!updateAll_ && updateName_.empty()) {
            std::cout
                << "\nRun 'yams plugin update <name>' or 'yams plugin update --all' to update.\n";
            return;
        }

        // Perform updates
        for (const auto& [name, version] : updates.value()) {
            std::cout << "\nUpdating " << name << " to v" << version << "...\n";

            InstallOptions options;
            options.force = true;
            options.version = version;

            auto result = installer->install(name, options);
            if (!result) {
                std::cout << "  Failed: " << result.error().message << "\n";
            } else {
                std::cout << "  Updated successfully.\n";
            }
        }
    } catch (const std::exception& e) {
        std::cout << "Error: " << e.what() << "\n";
    }
}

void PluginCommand::repoListPlugins() {
    using namespace yams::plugins;

    try {
        auto repoClient = makePluginRepoClient();
        auto result = repoClient->list(repoFilter_);
        returnOnError(result, "Failed to list plugins");

        const auto& plugins = result.value();
        if (plugins.empty()) {
            std::cout << "No plugins found.\n";
            return;
        }

        std::cout << "Available plugins (" << plugins.size() << "):\n";
        for (const auto& p : plugins) {
            std::cout << "  " << p.name;
            if (!p.latestVersion.empty()) {
                std::cout << " v" << p.latestVersion;
            }
            if (!p.description.empty()) {
                std::cout << " - " << p.description;
            }
            if (!p.interfaces.empty()) {
                std::cout << "\n    interfaces: ";
                for (size_t i = 0; i < p.interfaces.size(); ++i) {
                    if (i)
                        std::cout << ", ";
                    std::cout << p.interfaces[i];
                }
            }
            if (p.downloads > 0) {
                std::cout << " [" << p.downloads << " downloads]";
            }
            std::cout << "\n";
        }
    } catch (const std::exception& e) {
        std::cout << "Error: " << e.what() << "\n";
    }
}

void PluginCommand::repoShowInfo() {
    using namespace yams::plugins;

    try {
        auto repoClient = makePluginRepoClient();
        std::optional<std::string> version =
            repoVersion_.empty() ? std::nullopt : std::make_optional(repoVersion_);
        auto result = repoClient->get(repoPluginName_, version);
        returnOnError(result, "Failed to get plugin info");

        const auto& info = result.value();
        std::cout << "Plugin: " << info.name << "\n";
        std::cout << "  Version: " << info.version << "\n";
        if (!info.description.empty())
            std::cout << "  Description: " << info.description << "\n";
        if (!info.author.empty())
            std::cout << "  Author: " << info.author << "\n";
        if (!info.license.empty())
            std::cout << "  License: " << info.license << "\n";
        if (!info.interfaces.empty()) {
            std::cout << "  Interfaces: ";
            for (size_t i = 0; i < info.interfaces.size(); ++i) {
                if (i)
                    std::cout << ", ";
                std::cout << info.interfaces[i];
            }
            std::cout << "\n";
        }
        if (!info.platform.empty())
            std::cout << "  Platform: " << info.platform << "\n";
        if (!info.arch.empty())
            std::cout << "  Architecture: " << info.arch << "\n";
        std::cout << "  ABI Version: " << info.abiVersion << "\n";
        if (info.sizeBytes > 0)
            std::cout << "  Size: " << ui::format_bytes(info.sizeBytes) << "\n";
        if (!info.checksum.empty())
            std::cout << "  Checksum: " << info.checksum << "\n";
        if (!info.downloadUrl.empty())
            std::cout << "  Download URL: " << info.downloadUrl << "\n";
        if (info.downloads > 0)
            std::cout << "  Downloads: " << info.downloads << "\n";
        if (!info.publishedAt.empty())
            std::cout << "  Published: " << info.publishedAt << "\n";
    } catch (const std::exception& e) {
        std::cout << "Error: " << e.what() << "\n";
    }
}

void PluginCommand::repoShowVersions() {
    using namespace yams::plugins;

    try {
        auto repoClient = makePluginRepoClient();
        auto result = repoClient->versions(repoPluginName_);
        returnOnError(result, "Failed to get versions");

        const auto& versions = result.value();
        if (versions.empty()) {
            std::cout << "No versions found for " << repoPluginName_ << ".\n";
            return;
        }

        std::cout << "Available versions for " << repoPluginName_ << ":\n";
        for (const auto& v : versions) {
            std::cout << "  " << v << "\n";
        }
    } catch (const std::exception& e) {
        std::cout << "Error: " << e.what() << "\n";
    }
}

} // namespace yams::cli
