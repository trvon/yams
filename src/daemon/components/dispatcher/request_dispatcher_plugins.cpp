// Split from RequestDispatcher.cpp: plugin-related handlers
#include <algorithm>
#include <filesystem>
#include <set>
#include <vector>
#include <yams/daemon/components/dispatch_utils.hpp>
#include <yams/daemon/components/PluginHostFsm.h>
#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/resource/abi_plugin_loader.h>
#include <yams/daemon/resource/external_plugin_host.h>
#include <yams/daemon/resource/plugin_host.h>

namespace yams::daemon {

namespace {
std::string to_string(PluginHostState state) {
    switch (state) {
        case PluginHostState::NotInitialized:
            return "not_initialized";
        case PluginHostState::ScanningDirectories:
            return "scanning";
        case PluginHostState::VerifyingTrust:
            return "verifying_trust";
        case PluginHostState::LoadingPlugins:
            return "loading";
        case PluginHostState::Ready:
            return "ready";
        case PluginHostState::Failed:
            return "failed";
        default:
            return "unknown";
    }
}

template <typename Awaitable>
boost::asio::awaitable<Response> guard_plugin_host_ready(ServiceManager* sm, Awaitable&& fn) {
    if (!sm) {
        co_return ErrorResponse{ErrorCode::InvalidState, "Plugin host unavailable"};
    }
    auto snapshot = sm->getPluginHostFsmSnapshot();
    if (snapshot.state != PluginHostState::Ready) {
        co_return ErrorResponse{ErrorCode::InvalidState, "Plugin subsystem not ready (state=" +
                                                             to_string(snapshot.state) + ")"};
    }
    co_return co_await fn();
}

} // namespace

static PluginRecord toRecord(const PluginDescriptor& sr) {
    PluginRecord pr;
    pr.name = sr.name;
    pr.version = sr.version;
    pr.abiVersion = sr.abiVersion;
    pr.path = sr.path.string();
    pr.manifestJson = sr.manifestJson;
    pr.interfaces = sr.interfaces;
    return pr;
}

boost::asio::awaitable<Response>
RequestDispatcher::handlePluginScanRequest(const PluginScanRequest& req) {
    co_return co_await yams::daemon::dispatch::guard_await(
        "plugin_scan", [this, req]() -> boost::asio::awaitable<Response> {
            co_return co_await guard_plugin_host_ready(
                serviceManager_, [this, req]() -> boost::asio::awaitable<Response> {
                    auto abi = serviceManager_ ? serviceManager_->getAbiPluginHost() : nullptr;
                    auto external =
                        serviceManager_ ? serviceManager_->getExternalPluginHost() : nullptr;

                    if (!abi && !external)
                        co_return ErrorResponse{ErrorCode::NotImplemented,
                                                "No plugin host available"};

                    PluginScanResponse resp;
                    auto scanTarget = [&](const auto& host, const auto& target) {
                        if (host) {
                            if (auto r = host->scanTarget(target)) {
                                resp.plugins.push_back(toRecord(r.value()));
                                return true;
                            }
                        }
                        return false;
                    };
                    auto scanDirectory = [&](const auto& host, const auto& dir) {
                        if (host) {
                            if (auto r = host->scanDirectory(dir))
                                for (auto& sr : r.value())
                                    resp.plugins.push_back(toRecord(sr));
                        }
                    };

                    if (!req.target.empty()) {
                        // Try ABI first, then external
                        bool any = scanTarget(abi, req.target);
                        if (!any && external) {
                            any = scanTarget(external, req.target);
                        }
                        if (!any)
                            co_return ErrorResponse{ErrorCode::NotFound,
                                                    "No plugin found at target"};
                    } else if (!req.dir.empty()) {
                        scanDirectory(abi, req.dir);
                        scanDirectory(external, req.dir);
                    } else {
                        // Scan default directories for ABI plugins
                        for (const auto& dir : yams::daemon::dispatch::defaultAbiPluginDirs()) {
                            scanDirectory(abi, dir);
                        }
                        // Scan default directories for external plugins
                        for (const auto& dir :
                             yams::daemon::dispatch::defaultExternalPluginDirs()) {
                            scanDirectory(external, dir);
                        }
                    }
                    co_return resp;
                });
        });
}

boost::asio::awaitable<Response>
RequestDispatcher::handlePluginLoadRequest(const PluginLoadRequest& req) {
    co_return co_await yams::daemon::dispatch::guard_await(
        "plugin_load", [this, req]() -> boost::asio::awaitable<Response> {
            co_return co_await guard_plugin_host_ready(
                serviceManager_, [this, req]() -> boost::asio::awaitable<Response> {
                    auto abi = serviceManager_ ? serviceManager_->getAbiPluginHost() : nullptr;
                    auto external =
                        serviceManager_ ? serviceManager_->getExternalPluginHost() : nullptr;

                    if (!abi && !external) {
                        co_return ErrorResponse{ErrorCode::NotImplemented,
                                                "No plugin host available"};
                    }

                    PluginLoadResponse lr;
                    std::filesystem::path target(req.pathOrName);

                    // Check if it's an external plugin file (.py, .js, or yams-plugin.json nearby)
                    auto isExternalPlugin = [](const std::filesystem::path& p) -> bool {
                        auto ext = p.extension().string();
                        if (ext == ".py" || ext == ".js")
                            return true;
                        // Check for yams-plugin.json in same directory
                        if (std::filesystem::exists(p.parent_path() / "yams-plugin.json"))
                            return true;
                        return false;
                    };

                    auto scanTarget = [&](auto host) -> bool {
                        if (host && host->scanTarget(target)) {
                            lr = {false, "dry-run", toRecord(host->scanTarget(target).value())};
                            return true;
                        }
                        return false;
                    };

                    if (req.dryRun) {
                        // Try ABI first, then external
                        if (scanTarget(abi)) {
                            co_return lr;
                        }
                        if (scanTarget(external)) {
                            co_return lr;
                        }
                        co_return ErrorResponse{ErrorCode::NotFound, "Plugin not found"};
                    }

                    if (!std::filesystem::exists(target)) {
                        // Search ABI plugin directories
                        for (const auto& dir : yams::daemon::dispatch::defaultAbiPluginDirs()) {
                            auto candidate = dir / req.pathOrName;
                            if (std::filesystem::exists(candidate)) {
                                target = candidate;
                                break;
                            }
                        }
                        // Search external plugin directories
                        if (!std::filesystem::exists(target)) {
                            for (const auto& dir :
                                 yams::daemon::dispatch::defaultExternalPluginDirs()) {
                                auto candidate = dir / req.pathOrName;
                                if (std::filesystem::exists(candidate)) {
                                    target = candidate;
                                    break;
                                }
                            }
                        }
                        if (!std::filesystem::exists(target)) {
                            co_return ErrorResponse{ErrorCode::NotFound, "Plugin not found"};
                        }
                    }

                    auto loadPlugin = [&](auto host, const std::string& ext = "") -> bool {
                        if (host && (ext.empty() || target.extension() == ext)) {
                            if (auto r = host->load(target, req.configJson)) {
                                lr = {true, "loaded", toRecord(r.value())};
                                if (serviceManager_) {
                                    serviceManager_->adoptModelProviderFromHosts(lr.record.name);
                                    // Refresh status snapshot so new plugin appears in list
                                    serviceManager_->refreshPluginStatusSnapshot();
                                }
                                return true;
                            }
                        }
                        return false;
                    };

                    // Route to appropriate host based on file type
                    if (isExternalPlugin(target)) {
                        // Try external host first for Python/JS plugins
                        if (loadPlugin(external)) {
                            co_return lr;
                        }
                    } else {
                        // Try ABI host for native plugins
                        if (loadPlugin(abi)) {
                            co_return lr;
                        }
                    }

                    // Fallback: try the other host
                    if (isExternalPlugin(target)) {
                        if (loadPlugin(abi)) {
                            co_return lr;
                        }
                    } else {
                        if (loadPlugin(external)) {
                            co_return lr;
                        }
                    }

                    co_return ErrorResponse{ErrorCode::InternalError,
                                            "Plugin load failed for: " + target.string()};
                });
        });
}

boost::asio::awaitable<Response>
RequestDispatcher::handlePluginUnloadRequest(const PluginUnloadRequest& req) const {
    co_return co_await yams::daemon::dispatch::guard_await(
        "plugin_unload", [this, req]() -> boost::asio::awaitable<Response> {
            co_return co_await guard_plugin_host_ready(
                serviceManager_, [this, req]() -> boost::asio::awaitable<Response> {
                    auto abi = serviceManager_ ? serviceManager_->getAbiPluginHost() : nullptr;
                    auto external =
                        serviceManager_ ? serviceManager_->getExternalPluginHost() : nullptr;

                    bool ok = false;
                    // Try ABI host first
                    if (abi) {
                        if (auto r = abi->unload(req.name))
                            ok = true;
                    }
                    // Try external host
                    if (!ok && external) {
                        if (auto r = external->unload(req.name))
                            ok = true;
                    }
                    if (!ok)
                        co_return ErrorResponse{ErrorCode::NotFound,
                                                "Plugin not found or unload failed"};
                    // Refresh status snapshot so unloaded plugin is removed from list
                    if (serviceManager_) {
                        serviceManager_->refreshPluginStatusSnapshot();
                    }
                    co_return SuccessResponse{"unloaded"};
                });
        });
}

boost::asio::awaitable<Response>
RequestDispatcher::handlePluginTrustListRequest(const PluginTrustListRequest& /*req*/) const {
    co_return co_await yams::daemon::dispatch::guard_await(
        "plugin_trust_list", [this]() -> boost::asio::awaitable<Response> {
            co_return co_await guard_plugin_host_ready(
                serviceManager_, [this]() -> boost::asio::awaitable<Response> {
                    auto abi = serviceManager_ ? serviceManager_->getAbiPluginHost() : nullptr;
                    auto external =
                        serviceManager_ ? serviceManager_->getExternalPluginHost() : nullptr;

                    PluginTrustListResponse resp;
                    if (abi)
                        for (auto& p : abi->trustList())
                            resp.paths.push_back(p.string());
                    if (external)
                        for (auto& p : external->trustList())
                            resp.paths.push_back(p.string());
                    std::sort(resp.paths.begin(), resp.paths.end());
                    resp.paths.erase(std::unique(resp.paths.begin(), resp.paths.end()),
                                     resp.paths.end());
                    co_return resp;
                });
        });
}

boost::asio::awaitable<Response>
RequestDispatcher::handlePluginTrustAddRequest(const PluginTrustAddRequest& req) const {
    co_return co_await yams::daemon::dispatch::guard_await(
        "plugin_trust_add", [this, req]() -> boost::asio::awaitable<Response> {
            co_return co_await guard_plugin_host_ready(
                serviceManager_, [this, req]() -> boost::asio::awaitable<Response> {
                    auto abi = serviceManager_ ? serviceManager_->getAbiPluginHost() : nullptr;
                    auto external =
                        serviceManager_ ? serviceManager_->getExternalPluginHost() : nullptr;

                    // Add to both ABI and external trust lists
                    bool abiOk = abi && abi->trustAdd(req.path);
                    bool externalOk = external && external->trustAdd(req.path);

                    if (!abiOk && !externalOk) {
                        co_return ErrorResponse{ErrorCode::Unknown, "Trust add failed"};
                    }

                    auto path = std::filesystem::path(req.path);
                    auto exec = serviceManager_->getWorkerExecutor();

                    boost::asio::co_spawn(
                        exec,
                        [abi, external, path,
                         sm = serviceManager_]() -> boost::asio::awaitable<void> {
                            try {
                                // Helper to get paths of already-loaded plugins
                                auto getLoadedPaths = [](auto host) -> std::set<std::filesystem::path> {
                                    std::set<std::filesystem::path> paths;
                                    if (host) {
                                        for (const auto& desc : host->listLoaded()) {
                                            // Store both the exact path and parent directory
                                            paths.insert(std::filesystem::weakly_canonical(desc.path));
                                            if (desc.path.has_parent_path()) {
                                                paths.insert(std::filesystem::weakly_canonical(
                                                    desc.path.parent_path()));
                                            }
                                        }
                                    }
                                    return paths;
                                };

                                // Get paths of currently loaded plugins to skip scanning them
                                auto abiLoadedPaths = getLoadedPaths(abi);
                                auto externalLoadedPaths = getLoadedPaths(external);

                                // Check if the path is already loaded (exact match or parent of loaded)
                                auto isAlreadyLoaded = [](const std::filesystem::path& p,
                                                          const std::set<std::filesystem::path>& loaded) {
                                    auto canonical = std::filesystem::weakly_canonical(p);
                                    return loaded.count(canonical) > 0;
                                };

                                // Skip entirely if path is already loaded
                                bool skipAbi = isAlreadyLoaded(path, abiLoadedPaths);
                                bool skipExternal = isAlreadyLoaded(path, externalLoadedPaths);

                                if (skipAbi && skipExternal) {
                                    spdlog::debug("trust add: path {} already loaded, skipping scan",
                                                  path.string());
                                    co_return;
                                }

                                auto loadPlugins = [](auto host, const auto& dir) {
                                    if (host) {
                                        if (auto r = host->scanDirectory(dir)) {
                                            for (const auto& d : r.value()) {
                                                (void)host->load(d.path, "");
                                            }
                                        }
                                    }
                                };

                                if (std::filesystem::is_directory(path)) {
                                    if (!skipAbi)
                                        loadPlugins(abi, path);
                                    if (!skipExternal)
                                        loadPlugins(external, path);
                                } else if (std::filesystem::is_regular_file(path)) {
                                    // Route based on file type
                                    auto ext = path.extension().string();
                                    if (ext == ".py" || ext == ".js") {
                                        if (external && !skipExternal)
                                            (void)external->load(path, "");
                                    } else {
                                        if (abi && !skipAbi)
                                            (void)abi->load(path, "");
                                    }
                                }

                                if (sm) {
                                    (void)sm->adoptModelProviderFromHosts();
                                    // Refresh status so newly loaded plugins appear in list
                                    sm->refreshPluginStatusSnapshot();
                                }
                            } catch (...) {
                                // Handle exceptions silently
                            }
                            co_return;
                        },
                        boost::asio::detached);

                    co_return SuccessResponse{"ok"};
                });
        });
}

boost::asio::awaitable<Response>
RequestDispatcher::handlePluginTrustRemoveRequest(const PluginTrustRemoveRequest& req) const {
    co_return co_await yams::daemon::dispatch::guard_await(
        "plugin_trust_remove", [this, req]() -> boost::asio::awaitable<Response> {
            co_return co_await guard_plugin_host_ready(
                serviceManager_, [this, req]() -> boost::asio::awaitable<Response> {
                    auto abi = serviceManager_ ? serviceManager_->getAbiPluginHost() : nullptr;
                    auto external =
                        serviceManager_ ? serviceManager_->getExternalPluginHost() : nullptr;

                    // Remove from both hosts
                    bool abiOk = abi && abi->trustRemove(req.path);
                    bool externalOk = external && external->trustRemove(req.path);

                    if (!abiOk && !externalOk) {
                        co_return ErrorResponse{ErrorCode::Unknown, "Trust remove failed"};
                    }

                    co_return SuccessResponse{"ok"};
                });
        });
}

} // namespace yams::daemon
