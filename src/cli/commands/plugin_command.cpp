#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <future>
#include <iostream>
#include <optional>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <yams/cli/command.h>
#include <yams/cli/daemon_helpers.h>
#include <yams/cli/yams_cli.h>
#include <yams/core/types.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>

namespace yams::cli {

namespace {
using StatusResult = yams::Result<yams::daemon::StatusResponse>;

void print_plugin_not_ready_hint(const std::optional<yams::daemon::StatusResponse>& statusOpt) {
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
    std::cout << "Inspect 'yams daemon status -d' or '~/.yams/data/status.json' for detailed "
                 "plugin state."
              << '\n';
}

template <typename FetchStatusFn>
bool handle_plugin_rpc_error(const Error& err, const FetchStatusFn& fetchStatus,
                             const std::string& actionLabel) {
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
} // namespace

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

            // plugin trust (add/list/remove)
            auto* trust = plugin->add_subcommand("trust", "Manage plugin trust policy");
            trust->require_subcommand(1);

            trust->add_subcommand("add", "Trust a plugin directory or file")
                ->add_option("path", trustPath_, "Path to trust")
                ->required();
            trust->get_subcommand("add")->callback([this]() { trustAdd(trustPath_); });

            trust->add_subcommand("list", "List trusted plugin paths")->callback([this]() {
                trustList();
            });

            trust->add_subcommand("remove", "Remove a trusted plugin path")
                ->add_option("path", untrustPath_, "Path to remove")
                ->required();
            trust->get_subcommand("remove")->callback([this]() { trustRemove(untrustPath_); });

            // plugin install (stub)
            plugin->add_subcommand("install", "Install plugin from URL or path (stub)")
                ->add_option("src", installSrc_, "URL or local path")
                ->required();
            plugin->get_subcommand("install")->callback(
                [this]() { std::cout << "yams plugin install: TODO src=" << installSrc_ << "\n"; });

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
    void scanPlugins(const std::string& dir, const std::string& target);
    void loadPlugin(const std::string& arg, const std::string& cfg, bool dryRun);
    void unloadPlugin(const std::string& name);
    void trustList();
    void trustAdd(const std::string& path);
    void trustRemove(const std::string& path);

    YamsCLI* cli_{nullptr};
    // Persistent option storage to avoid dangling references in callbacks
    std::string scanDir_;
    std::string scanTarget_;
    std::string infoName_;
    std::string loadArg_;
    std::string configFile_;
    bool dryRun_{false};
    std::string unloadName_;
    std::string trustPath_;
    std::string untrustPath_;
    std::string installSrc_;
    std::string nameToggle_;
    bool verboseList_{false};
};

// Factory
std::unique_ptr<ICommand> createPluginCommand() {
    return std::make_unique<PluginCommand>();
}

} // namespace yams::cli

namespace yams::cli {

void PluginCommand::listPlugins() {
    using namespace yams::daemon;
    try {
        ClientConfig cfg;
        if (cli_->hasExplicitDataDir()) {
            cfg.dataDir = cli_->getDataPath();
        }
        cfg.enableChunkedResponses = false;
        cfg.requestTimeout = std::chrono::milliseconds(10000);
        cfg.autoStart = true; // start a managed daemon if not running
        auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(cfg);
        if (!leaseRes) {
            std::cout << "Failed to acquire daemon client: " << leaseRes.error().message << "\n";
            return;
        }
        auto leaseHandle = std::move(leaseRes.value());

        auto fetch_status = [leaseHandle]() -> Result<StatusResponse> {
            auto& client = **leaseHandle;
            return yams::cli::run_result<StatusResponse>(client.status(),
                                                         std::chrono::milliseconds(10000));
        };

        auto fetch_stats = [leaseHandle]() -> Result<GetStatsResponse> {
            auto& client = **leaseHandle;
            GetStatsRequest req;
            req.detailed = true;
            return yams::cli::run_result<GetStatsResponse>(client.call(req),
                                                           std::chrono::milliseconds(10000));
        };

        // Try once
        auto sres = fetch_status();
        auto res = fetch_stats();
        bool have_typed = sres && !sres.value().providers.empty();
        bool have_json = res && res.value().additionalStats.contains("plugins_json");
        // If not available, request a scan and then wait briefly for readiness/providers
        bool need_scan = !(have_typed || have_json);
        if (need_scan) {
            PluginScanRequest scan; // empty -> server uses configured search paths
            auto& client = **leaseHandle;
            auto scanRes = yams::cli::run_result<PluginScanResponse>(
                client.call(scan), std::chrono::milliseconds(10000));
            if (!scanRes) {
                auto err = scanRes.error();
                auto statusFetcher = [&]() -> StatusResult { return fetch_status(); };
                if (!handle_plugin_rpc_error(err, statusFetcher, "Plugin scan"))
                    return;
            }
            // Wait up to ~3s for plugin readiness/providers
            for (int i = 0; i < 6; ++i) {
                sres = fetch_status();
                if (sres) {
                    const auto& st = sres.value();
                    if (!st.providers.empty())
                        break;
                    auto itp = st.readinessStates.find("plugins");
                    if (itp != st.readinessStates.end() && itp->second)
                        break;
                    auto itmp = st.readinessStates.find("model_provider");
                    if (itmp != st.readinessStates.end() && itmp->second)
                        break;
                }
                // Fallback to JSON snapshot
                res = fetch_stats();
                if (res && res.value().additionalStats.contains("plugins_json"))
                    break;
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
        }
        if (!sres && !res) {
            std::cout << "Failed to query daemon for plugins\n";
            return;
        }
        // Prefer typed providers if present, but merge in interface hints from plugins_json when
        // available
        if (sres && !sres.value().providers.empty()) {
            const auto& st = sres.value();
            // Optional: fetch plugins_json to enrich with interfaces
            std::map<std::string, std::vector<std::string>> ifaceMap;
            try {
                auto gres = fetch_stats();
                if (gres) {
                    auto it = gres.value().additionalStats.find("plugins_json");
                    if (it != gres.value().additionalStats.end()) {
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
                    }
                }
            } catch (...) {
            }

            std::cout << "Loaded plugins (" << st.providers.size() << "):\n";
            for (const auto& p : st.providers) {
                std::cout << "  - " << p.name;
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
                std::cout << "\n";
            }
            // When verbose, show skipped plugin diagnostics if present
            if (verboseList_ && !st.skippedPlugins.empty()) {
                std::cout << "\nSkipped plugins (" << st.skippedPlugins.size() << "):\n";
                for (const auto& sp : st.skippedPlugins) {
                    std::cout << "  - " << sp.path << ": " << sp.reason << "\n";
                }
            }
            return;
        }
        const auto& resp = res.value();
        auto it = resp.additionalStats.find("plugins_json");
        if (it == resp.additionalStats.end()) {
            std::cout << "No plugin information available.\n"
                         "- Ensure the daemon is running (yams daemon start)\n"
                         "- Configure [daemon].plugin_dir in ~/.config/yams/config.toml\n"
                         "- Place plugins there (e.g., libyams_onnx_plugin.so) and re-run 'yams "
                         "plugins list'\n";
            return;
        }
        nlohmann::json pj = nlohmann::json::parse(it->second, nullptr, false);
        if (pj.is_discarded()) {
            std::cout << "Invalid plugin JSON.\n";
            return;
        }
        std::cout << "Loaded plugins (" << pj.size() << "):\n";
        for (const auto& rec : pj) {
            std::string name = rec.value("name", "");
            std::string path = rec.value("path", "");
            bool provider = rec.value("provider", false);
            bool degraded = rec.value("degraded", false);
            std::string error = rec.value("error", std::string());
            int models = 0;
            if (rec.contains("models_loaded")) {
                try {
                    models = rec["models_loaded"].get<int>();
                } catch (...) {
                }
            }
            std::cout << "  - " << name;
            if (!path.empty())
                std::cout << " (" << path << ")";
            if (provider)
                std::cout << " [provider]";
            if (degraded)
                std::cout << " [degraded]";
            if (models > 0)
                std::cout << " models=" << models;
            if (!error.empty())
                std::cout << " error=\"" << error << "\"";
            std::cout << "\n";
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
        auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(cfg);
        if (!leaseRes) {
            std::cout << "Failed to acquire daemon client: " << leaseRes.error().message << "\n";
            return;
        }
        auto leaseHandle = std::move(leaseRes.value());
        auto& client = **leaseHandle;
        GetStatsRequest req;
        req.detailed = true; // ensure plugin details are included
        auto res = yams::cli::run_result<yams::daemon::GetStatsResponse>(
            client.call(req), std::chrono::milliseconds(10000));
        if (!res) {
            std::cout << "Failed to query daemon stats for plugins\n";
            return;
        }
        const auto& resp = res.value();
        auto it = resp.additionalStats.find("plugins_json");
        if (it == resp.additionalStats.end()) {
            std::cout << "No plugin information available.\n";
            return;
        }
        nlohmann::json pj = nlohmann::json::parse(it->second, nullptr, false);
        if (pj.is_discarded()) {
            std::cout << "Invalid plugin JSON.\n";
            return;
        }
        for (const auto& rec : pj) {
            if (rec.value("name", std::string()) == name) {
                std::cout << rec.dump(2) << "\n";
                return;
            }
        }
        std::cout << "Plugin not found: " << name << "\n";
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
        auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(cfg);
        if (!leaseRes) {
            std::cout << "Failed to acquire daemon client: " << leaseRes.error().message << "\n";
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
        auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(cfg);
        if (!leaseRes) {
            std::cout << "Failed to acquire daemon client: " << leaseRes.error().message << "\n";
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
            auto statusFetcher = [&]() -> StatusResult {
                return yams::cli::run_result<yams::daemon::StatusResponse>(
                    client.status(), std::chrono::milliseconds(5000));
            };
            if (!handle_plugin_rpc_error(res.error(), statusFetcher, "Plugin load"))
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
        auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(cfg);
        if (!leaseRes) {
            std::cout << "Failed to acquire daemon client: " << leaseRes.error().message << "\n";
            return;
        }
        auto leaseHandle = std::move(leaseRes.value());
        auto& client = **leaseHandle;
        PluginUnloadRequest req;
        req.name = name;
        auto res = yams::cli::run_result<SuccessResponse>(client.call(req),
                                                          std::chrono::milliseconds(10000));
        if (!res) {
            auto statusFetcher = [&]() -> StatusResult {
                return yams::cli::run_result<yams::daemon::StatusResponse>(
                    client.status(), std::chrono::milliseconds(5000));
            };
            if (!handle_plugin_rpc_error(res.error(), statusFetcher, "Plugin unload"))
                return;
            return;
        }
        std::cout << "Unloaded: " << name << "\n";
    } catch (const std::exception& e) {
        std::cout << "Error: " << e.what() << "\n";
    }
}

void PluginCommand::trustList() {
    using namespace yams::daemon;
    try {
        ClientConfig cfg;
        if (cli_->hasExplicitDataDir()) {
            cfg.dataDir = cli_->getDataPath();
        }
        cfg.requestTimeout = std::chrono::milliseconds(15000);
        auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(cfg);
        if (!leaseRes) {
            std::cout << "Failed to acquire daemon client: " << leaseRes.error().message << "\n";
            return;
        }
        auto leaseHandle = std::move(leaseRes.value());
        auto& client = **leaseHandle;
        PluginTrustListRequest req;
        auto res = yams::cli::run_result<PluginTrustListResponse>(client.call(req),
                                                                  std::chrono::milliseconds(15000));
        if (!res) {
            auto statusFetcher = [&]() -> StatusResult {
                return yams::cli::run_result<yams::daemon::StatusResponse>(
                    client.status(), std::chrono::milliseconds(5000));
            };
            if (!handle_plugin_rpc_error(res.error(), statusFetcher, "Plugin trust list"))
                return;
            return;
        }
        const auto& r = res.value();
        std::cout << "Trusted plugin paths (" << r.paths.size() << "):\n";
        for (const auto& p : r.paths)
            std::cout << "  - " << p << "\n";
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
        auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(cfg);
        if (!leaseRes) {
            std::cout << "Failed to acquire daemon client: " << leaseRes.error().message << "\n";
            return;
        }
        auto leaseHandle = std::move(leaseRes.value());
        auto& client = **leaseHandle;
        PluginTrustAddRequest req;
        req.path = path;
        auto res = yams::cli::run_result<SuccessResponse>(client.call(req),
                                                          std::chrono::milliseconds(15000));
        if (!res) {
            auto statusFetcher = [&]() -> StatusResult {
                return yams::cli::run_result<yams::daemon::StatusResponse>(
                    client.status(), std::chrono::milliseconds(5000));
            };
            if (!handle_plugin_rpc_error(res.error(), statusFetcher, "Plugin trust add"))
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
        auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(cfg);
        if (!leaseRes) {
            std::cout << "Failed to acquire daemon client: " << leaseRes.error().message << "\n";
            return;
        }
        auto leaseHandle = std::move(leaseRes.value());
        auto& client = **leaseHandle;
        PluginTrustRemoveRequest req;
        req.path = path;
        auto res = yams::cli::run_result<SuccessResponse>(client.call(req),
                                                          std::chrono::milliseconds(15000));
        if (!res) {
            auto statusFetcher = [&]() -> StatusResult {
                return yams::cli::run_result<yams::daemon::StatusResponse>(
                    client.status(), std::chrono::milliseconds(5000));
            };
            if (!handle_plugin_rpc_error(res.error(), statusFetcher, "Plugin trust remove"))
                return;
            return;
        }
        std::cout << "Untrusted: " << path << "\n";
    } catch (const std::exception& e) {
        std::cout << "Error: " << e.what() << "\n";
    }
}

} // namespace yams::cli
