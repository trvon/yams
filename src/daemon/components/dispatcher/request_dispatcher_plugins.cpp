// Split from RequestDispatcher.cpp: plugin-related handlers
#include <algorithm>
#include <filesystem>

#include <vector>
#include <boost/asio/thread_pool.hpp>
#include <yams/daemon/components/dispatch_utils.hpp>
#include <yams/daemon/components/PluginHostFsm.h>
#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/resource/abi_plugin_loader.h>
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

boost::asio::any_io_executor plugin_fallback_executor() {
    static boost::asio::thread_pool pool(1);
    return pool.get_executor();
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
                    if (!abi)
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
                        bool any = scanTarget(abi, req.target);
                        if (!any)
                            co_return ErrorResponse{ErrorCode::NotFound,
                                                    "No plugin found at target"};
                    } else if (!req.dir.empty()) {
                        scanDirectory(abi, req.dir);
                    } else {
                        for (const auto& dir : yams::daemon::dispatch::defaultAbiPluginDirs()) {
                            scanDirectory(abi, dir);
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
                    auto getHost = [this](auto getter) {
                        return serviceManager_ ? (serviceManager_->*getter)() : nullptr;
                    };

                    auto abi = getHost(&ServiceManager::getAbiPluginHost);

                    if (!abi) {
                        co_return ErrorResponse{ErrorCode::NotImplemented,
                                                "No plugin host available"};
                    }

                    PluginLoadResponse lr;
                    std::filesystem::path target(req.pathOrName);

                    auto scanTarget = [&](auto host) -> bool {
                        if (host && host->scanTarget(target)) {
                            lr = {false, "dry-run", toRecord(host->scanTarget(target).value())};
                            return true;
                        }
                        return false;
                    };

                    if (req.dryRun) {
                        if (scanTarget(abi)) {
                            co_return lr;
                        }
                        co_return ErrorResponse{ErrorCode::NotFound, "Plugin not found"};
                    }

                    if (!std::filesystem::exists(target)) {
                        for (const auto& dir : yams::daemon::dispatch::defaultAbiPluginDirs()) {
                            auto candidate = dir / req.pathOrName;
                            if (std::filesystem::exists(candidate)) {
                                target = candidate;
                                break;
                            }
                        }
                        if (!std::filesystem::exists(target)) {
                            co_return ErrorResponse{ErrorCode::NotFound, "Plugin not found"};
                        }
                    }

                    auto loadPlugin = [&](auto host, const std::string& ext = "") -> bool {
                        if (host && (ext.empty() || target.extension() == ext)) {
                            if (auto r = host->load(target, "")) {
                                lr = {true, "loaded", toRecord(r.value())};
                                if (serviceManager_) {
                                    serviceManager_->adoptModelProviderFromHosts(lr.record.name);
                                }
                                return true;
                            }
                        }
                        return false;
                    };

                    if (loadPlugin(abi)) {
                        co_return lr;
                    }

                    co_return ErrorResponse{ErrorCode::InvalidState, "Load failed"};
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
                    bool ok = false;
                    if (abi) {
                        if (auto r = abi->unload(req.name))
                            ok = true;
                    }
                    if (!ok)
                        co_return ErrorResponse{ErrorCode::NotFound,
                                                "Plugin not found or unload failed"};
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
                    PluginTrustListResponse resp;
                    if (abi)
                        for (auto& p : abi->trustList())
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
                    auto addTrust = [&](auto host) -> bool {
                        return host && host->trustAdd(req.path);
                    };

                    auto abi = serviceManager_ ? serviceManager_->getAbiPluginHost() : nullptr;

                    if (!addTrust(abi)) {
                        co_return ErrorResponse{ErrorCode::Unknown, "Trust add failed"};
                    }

                    auto path = std::filesystem::path(req.path);
                    auto exec = serviceManager_ ? serviceManager_->getWorkerExecutor()
                                                : plugin_fallback_executor();

                    boost::asio::co_spawn(
                        exec,
                        [abi, path, sm = serviceManager_]() -> boost::asio::awaitable<void> {
                            try {
                                auto loadPlugins = [&](auto host, const auto& dir) {
                                    if (host) {
                                        if (auto r = host->scanDirectory(dir)) {
                                            for (const auto& d : r.value()) {
                                                (void)host->load(d.path, "");
                                            }
                                        }
                                    }
                                };

                                if (std::filesystem::is_directory(path)) {
                                    loadPlugins(abi, path);
                                } else if (std::filesystem::is_regular_file(path)) {
                                    if (abi)
                                        (void)abi->load(path, "");
                                }

                                if (sm)
                                    (void)sm->adoptModelProviderFromHosts();
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
                    auto removeTrust = [&](auto host) {
                        return host && host->trustRemove(req.path);
                    };

                    auto abi = serviceManager_ ? serviceManager_->getAbiPluginHost() : nullptr;

                    if (!removeTrust(abi)) {
                        co_return ErrorResponse{ErrorCode::Unknown, "Trust remove failed"};
                    }

                    co_return SuccessResponse{"ok"};
                });
        });
}

} // namespace yams::daemon
