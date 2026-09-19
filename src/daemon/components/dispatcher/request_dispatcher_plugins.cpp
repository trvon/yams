// Split from RequestDispatcher.cpp: plugin-related handlers
#include <filesystem>
#include <vector>
#include <yams/daemon/components/dispatch_response.hpp>
#include <yams/daemon/components/dispatch_utils.hpp>
#include <yams/daemon/components/PluginHostFsm.h>
#include <yams/daemon/components/RequestDispatcher.h>
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
        co_return yams::daemon::dispatch::makeErrorResponse(ErrorCode::InvalidState,
                                                            "Plugin host unavailable");
    }
    auto snapshot = sm->getPluginHostFsmSnapshot();
    if (snapshot.state != PluginHostState::Ready) {
        co_return yams::daemon::dispatch::makeErrorResponse(
            ErrorCode::InvalidState,
            "Plugin subsystem not ready (state=" + to_string(snapshot.state) + ")");
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
                    auto* pluginManager = serviceManager_->getPluginManager();
                    if (!pluginManager) {
                        co_return yams::daemon::dispatch::makeErrorResponse(
                            ErrorCode::InvalidState, "Plugin manager unavailable");
                    }
                    if (!pluginManager->getPluginHost() &&
                        !pluginManager->getExternalPluginHost()) {
                        co_return yams::daemon::dispatch::makeErrorResponse(
                            ErrorCode::NotImplemented, "No plugin host available");
                    }

                    std::vector<PluginManager::DiscoveredPlugin> discovered;
                    if (!req.target.empty()) {
                        auto result = pluginManager->scanPluginTarget(req.target);
                        if (!result) {
                            co_return yams::daemon::dispatch::makeErrorResponse(
                                ErrorCode::NotFound, "No plugin found at target");
                        }
                        discovered.push_back(result.value());
                    } else {
                        auto result = req.dir.empty() ? pluginManager->scanConfiguredPluginRoots()
                                                      : pluginManager->scanPluginDirectory(req.dir);
                        if (!result) {
                            co_return yams::daemon::dispatch::makeErrorResponse(
                                result.error().code, result.error().message);
                        }
                        discovered = result.value();
                    }

                    PluginScanResponse response;
                    response.plugins.reserve(discovered.size());
                    for (const auto& plugin : discovered) {
                        response.plugins.push_back(toRecord(plugin.descriptor));
                    }
                    co_return response;
                });
        });
}

boost::asio::awaitable<Response>
RequestDispatcher::handlePluginLoadRequest(const PluginLoadRequest& req) {
    co_return co_await yams::daemon::dispatch::guard_await(
        "plugin_load", [this, req]() -> boost::asio::awaitable<Response> {
            co_return co_await guard_plugin_host_ready(
                serviceManager_, [this, req]() -> boost::asio::awaitable<Response> {
                    auto* pluginManager = serviceManager_->getPluginManager();
                    if (!pluginManager) {
                        co_return yams::daemon::dispatch::makeErrorResponse(
                            ErrorCode::InvalidState, "Plugin manager unavailable");
                    }
                    if (!pluginManager->getPluginHost() &&
                        !pluginManager->getExternalPluginHost()) {
                        co_return yams::daemon::dispatch::makeErrorResponse(
                            ErrorCode::NotImplemented, "No plugin host available");
                    }

                    auto resolved = pluginManager->resolvePluginTarget(req.pathOrName);
                    if (!resolved) {
                        co_return yams::daemon::dispatch::makeErrorResponse(ErrorCode::NotFound,
                                                                            "Plugin not found");
                    }
                    const auto& target = resolved.value();

                    if (req.dryRun) {
                        auto discovered = pluginManager->scanPluginTarget(target);
                        if (!discovered) {
                            co_return yams::daemon::dispatch::makeErrorResponse(ErrorCode::NotFound,
                                                                                "Plugin not found");
                        }
                        co_return PluginLoadResponse{false, "dry-run",
                                                     toRecord(discovered.value().descriptor)};
                    }

                    auto loaded = pluginManager->loadPluginTarget(target, req.configJson);
                    if (!loaded) {
                        co_return yams::daemon::dispatch::makeErrorResponse(
                            ErrorCode::InternalError, "Plugin load failed for: " + target.string());
                    }

                    PluginLoadResponse response{true, "loaded", toRecord(loaded.value())};
                    serviceManager_->adoptModelProviderFromHosts(response.record.name);
                    serviceManager_->refreshPluginStatusSnapshot();
                    co_return response;
                });
        });
}

boost::asio::awaitable<Response>
RequestDispatcher::handlePluginUnloadRequest(const PluginUnloadRequest& req) const {
    co_return co_await yams::daemon::dispatch::guard_await(
        "plugin_unload", [this, req]() -> boost::asio::awaitable<Response> {
            co_return co_await guard_plugin_host_ready(
                serviceManager_, [this, req]() -> boost::asio::awaitable<Response> {
                    auto* pluginManager = serviceManager_->getPluginManager();
                    if (!pluginManager) {
                        co_return yams::daemon::dispatch::makeErrorResponse(
                            ErrorCode::InvalidState, "Plugin manager unavailable");
                    }
                    auto result = pluginManager->unloadPlugin(req.name);
                    if (!result) {
                        co_return yams::daemon::dispatch::makeErrorResponse(
                            result.error().code, "Plugin not found or unload failed");
                    }
                    serviceManager_->refreshPluginStatusSnapshot();
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
                    auto* pluginManager = serviceManager_->getPluginManager();
                    if (!pluginManager) {
                        co_return yams::daemon::dispatch::makeErrorResponse(
                            ErrorCode::InvalidState, "Plugin manager unavailable");
                    }

                    PluginTrustListResponse response;
                    for (const auto& path : pluginManager->trustList()) {
                        response.paths.push_back(path.string());
                    }
                    co_return response;
                });
        });
}

boost::asio::awaitable<Response>
RequestDispatcher::handlePluginTrustAddRequest(const PluginTrustAddRequest& req) const {
    co_return co_await yams::daemon::dispatch::guard_await(
        "plugin_trust_add", [this, req]() -> boost::asio::awaitable<Response> {
            co_return co_await guard_plugin_host_ready(
                serviceManager_, [this, req]() -> boost::asio::awaitable<Response> {
                    auto* pluginManager = serviceManager_->getPluginManager();
                    if (!pluginManager) {
                        co_return yams::daemon::dispatch::makeErrorResponse(
                            ErrorCode::InvalidState, "Plugin manager unavailable");
                    }
                    if (auto trustResult = pluginManager->trustAdd(req.path); !trustResult) {
                        co_return yams::daemon::dispatch::makeErrorResponse(ErrorCode::Unknown,
                                                                            "Trust add failed");
                    }

                    const auto path = std::filesystem::path(req.path);
                    if (pluginManager->isPluginLoadedFrom(path)) {
                        spdlog::debug("trust add: path {} already loaded, skipping scan",
                                      path.string());
                        co_return SuccessResponse{"ok"};
                    }

                    auto refreshLoadedPlugins = [sm = serviceManager_]() {
                        (void)sm->adoptModelProviderFromHosts();
                        sm->refreshPluginStatusSnapshot();
                    };

                    if (std::filesystem::is_regular_file(path)) {
                        auto loaded = pluginManager->loadPluginTarget(path);
                        if (!loaded) {
                            co_return yams::daemon::dispatch::makeErrorResponse(
                                ErrorCode::InternalError,
                                "Plugin load failed for: " + path.string());
                        }
                        refreshLoadedPlugins();
                        co_return SuccessResponse{"ok"};
                    }

                    if (std::filesystem::is_directory(path)) {
                        auto loaded = pluginManager->loadPluginDirectory(path);
                        if (!loaded) {
                            yams::daemon::dispatch::logDispatchFailureNoexcept(
                                "plugin_trust_add", loaded.error().message.c_str());
                        }
                    }
                    refreshLoadedPlugins();
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
                    auto* pluginManager = serviceManager_->getPluginManager();
                    if (!pluginManager || !pluginManager->trustRemove(req.path)) {
                        co_return yams::daemon::dispatch::makeErrorResponse(ErrorCode::Unknown,
                                                                            "Trust remove failed");
                    }
                    co_return SuccessResponse{"ok"};
                });
        });
}

} // namespace yams::daemon
