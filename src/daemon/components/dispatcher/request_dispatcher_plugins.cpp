// Split from RequestDispatcher.cpp: plugin-related handlers
#include <algorithm>
#include <filesystem>

#include <yams/daemon/components/dispatch_utils.hpp>
#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/resource/abi_plugin_loader.h>
#include <yams/daemon/resource/plugin_host.h>
#include <yams/daemon/resource/plugin_loader.h>

namespace yams::daemon {

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
            auto abi = serviceManager_ ? serviceManager_->getAbiPluginHost() : nullptr;
            auto wasm = serviceManager_ ? serviceManager_->getWasmPluginHost() : nullptr;
            auto ext = serviceManager_ ? serviceManager_->getExternalPluginHost() : nullptr;
            if (!abi && !wasm && !ext)
                co_return ErrorResponse{ErrorCode::NotImplemented, "No plugin host available"};
            PluginScanResponse resp;
            if (!req.target.empty()) {
                bool any = false;
                if (abi) {
                    if (auto r = abi->scanTarget(req.target)) {
                        resp.plugins.push_back(toRecord(r.value()));
                        any = true;
                    }
                }
                if (wasm) {
                    if (auto r = wasm->scanTarget(req.target)) {
                        resp.plugins.push_back(toRecord(r.value()));
                        any = true;
                    }
                }
                if (ext) {
                    if (auto r = ext->scanTarget(req.target)) {
                        resp.plugins.push_back(toRecord(r.value()));
                        any = true;
                    }
                }
                if (!any)
                    co_return ErrorResponse{ErrorCode::NotFound, "No plugin found at target"};
            } else if (!req.dir.empty()) {
                if (abi) {
                    if (auto r = abi->scanDirectory(req.dir))
                        for (auto& sr : r.value())
                            resp.plugins.push_back(toRecord(sr));
                }
                if (wasm) {
                    if (auto r = wasm->scanDirectory(req.dir))
                        for (auto& sr : r.value())
                            resp.plugins.push_back(toRecord(sr));
                }
                if (ext) {
                    if (auto r = ext->scanDirectory(req.dir))
                        for (auto& sr : r.value())
                            resp.plugins.push_back(toRecord(sr));
                }
            } else {
                // Default directories: reuse existing model PluginLoader directories
                for (const auto& dir : PluginLoader::getDefaultPluginDirectories()) {
                    if (abi)
                        if (auto r = abi->scanDirectory(dir))
                            for (auto& sr : r.value())
                                resp.plugins.push_back(toRecord(sr));
                    if (wasm)
                        if (auto r = wasm->scanDirectory(dir))
                            for (auto& sr : r.value())
                                resp.plugins.push_back(toRecord(sr));
                    if (ext)
                        if (auto r = ext->scanDirectory(dir))
                            for (auto& sr : r.value())
                                resp.plugins.push_back(toRecord(sr));
                }
            }
            co_return resp;
        });
}

boost::asio::awaitable<Response>
RequestDispatcher::handlePluginLoadRequest(const PluginLoadRequest& req) {
    co_return co_await yams::daemon::dispatch::guard_await(
        "plugin_load", [this, req]() -> boost::asio::awaitable<Response> {
            auto abi = serviceManager_ ? serviceManager_->getAbiPluginHost() : nullptr;
            auto wasm = serviceManager_ ? serviceManager_->getWasmPluginHost() : nullptr;
            auto ext = serviceManager_ ? serviceManager_->getExternalPluginHost() : nullptr;
            auto appContext = serviceManager_ ? serviceManager_->getAppContext()
                                              : yams::app::services::AppContext{};
            if (!abi && !wasm && !ext)
                co_return ErrorResponse{ErrorCode::NotImplemented, "No plugin host available"};
            PluginLoadResponse lr;
            std::filesystem::path target(req.pathOrName);
            if (req.dryRun) {
                if (abi) {
                    if (auto r = abi->scanTarget(target)) {
                        lr.loaded = false;
                        lr.message = "dry-run";
                        lr.record = toRecord(r.value());
                        co_return lr;
                    }
                }
                if (wasm) {
                    if (auto r = wasm->scanTarget(target)) {
                        lr.loaded = false;
                        lr.message = "dry-run";
                        lr.record = toRecord(r.value());
                        co_return lr;
                    }
                }
                co_return ErrorResponse{ErrorCode::NotFound, "Plugin not found"};
            }
            if (!std::filesystem::exists(target)) {
                // Try default directories by file name first
                bool found = false;
                for (const auto& dir : PluginLoader::getDefaultPluginDirectories()) {
                    auto candidate = std::filesystem::path(dir) / req.pathOrName;
                    if (std::filesystem::exists(candidate)) {
                        target = candidate;
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    co_return ErrorResponse{ErrorCode::NotFound, "Plugin not found"};
                }
            }
            // Load via appropriate host
            if (abi && target.extension() != ".wasm") {
                if (auto r = abi->load(target, "")) {
                    lr.loaded = true;
                    lr.message = "loaded";
                    lr.record = toRecord(r.value());
                    // Attempt to adopt model provider dynamically if this plugin provides it
                    if (serviceManager_) {
                        (void)serviceManager_->adoptModelProviderFromHosts(lr.record.name);
                    }
                    co_return lr;
                }
            }
            if (wasm && target.extension() == ".wasm") {
                if (auto r = wasm->load(target, "")) {
                    lr.loaded = true;
                    lr.message = "loaded";
                    lr.record = toRecord(r.value());
                    if (serviceManager_) {
                        (void)serviceManager_->adoptModelProviderFromHosts(lr.record.name);
                    }
                    co_return lr;
                }
            }
            if (ext) {
                if (auto r = ext->load(target, "")) {
                    lr.loaded = true;
                    lr.message = "loaded";
                    lr.record = toRecord(r.value());
                    if (serviceManager_) {
                        (void)serviceManager_->adoptModelProviderFromHosts(lr.record.name);
                    }
                    co_return lr;
                }
            }
            co_return ErrorResponse{ErrorCode::InvalidState, "Load failed"};
        });
}

boost::asio::awaitable<Response>
RequestDispatcher::handlePluginUnloadRequest(const PluginUnloadRequest& req) {
    co_return co_await yams::daemon::dispatch::guard_await(
        "plugin_unload", [this, req]() -> boost::asio::awaitable<Response> {
            auto abi = serviceManager_ ? serviceManager_->getAbiPluginHost() : nullptr;
            auto wasm = serviceManager_ ? serviceManager_->getWasmPluginHost() : nullptr;
            auto ext = serviceManager_ ? serviceManager_->getExternalPluginHost() : nullptr;
            bool ok = false;
            if (abi) {
                if (auto r = abi->unload(req.name))
                    ok = true;
            }
            if (wasm && !ok) {
                if (auto r = wasm->unload(req.name))
                    ok = true;
            }
            if (ext && !ok) {
                if (auto r = ext->unload(req.name))
                    ok = true;
            }
            if (!ok)
                co_return ErrorResponse{ErrorCode::NotFound, "Plugin not found or unload failed"};
            co_return SuccessResponse{"unloaded"};
        });
}

boost::asio::awaitable<Response>
RequestDispatcher::handlePluginTrustListRequest(const PluginTrustListRequest& /*req*/) {
    co_return co_await yams::daemon::dispatch::guard_await(
        "plugin_trust_list", [this]() -> boost::asio::awaitable<Response> {
            auto abi = serviceManager_ ? serviceManager_->getAbiPluginHost() : nullptr;
            auto wasm = serviceManager_ ? serviceManager_->getWasmPluginHost() : nullptr;
            auto ext = serviceManager_ ? serviceManager_->getExternalPluginHost() : nullptr;
            PluginTrustListResponse resp;
            if (abi)
                for (auto& p : abi->trustList())
                    resp.paths.push_back(p.string());
            if (wasm)
                for (auto& p : wasm->trustList())
                    resp.paths.push_back(p.string());
            if (ext)
                for (auto& p : ext->trustList())
                    resp.paths.push_back(p.string());
            std::sort(resp.paths.begin(), resp.paths.end());
            resp.paths.erase(std::unique(resp.paths.begin(), resp.paths.end()), resp.paths.end());
            co_return resp;
        });
}

boost::asio::awaitable<Response>
RequestDispatcher::handlePluginTrustAddRequest(const PluginTrustAddRequest& req) {
    co_return co_await yams::daemon::dispatch::guard_await(
        "plugin_trust_add", [this, req]() -> boost::asio::awaitable<Response> {
            auto abi = serviceManager_ ? serviceManager_->getAbiPluginHost() : nullptr;
            auto wasm = serviceManager_ ? serviceManager_->getWasmPluginHost() : nullptr;
            auto ext = serviceManager_ ? serviceManager_->getExternalPluginHost() : nullptr;
            bool ok = false;
            if (abi)
                if (auto r = abi->trustAdd(req.path))
                    ok = true;
            if (wasm)
                if (auto r = wasm->trustAdd(req.path))
                    ok = true;
            if (ext)
                if (auto r = ext->trustAdd(req.path))
                    ok = true;
            if (!ok)
                co_return ErrorResponse{ErrorCode::Unknown, "Trust add failed"};
            try {
                std::filesystem::path p = req.path;
                if (std::filesystem::is_directory(p)) {
                    if (abi)
                        if (auto r = abi->scanDirectory(p))
                            for (const auto& d : r.value())
                                (void)abi->load(d.path, "");
                    if (wasm)
                        if (auto r = wasm->scanDirectory(p))
                            for (const auto& d : r.value())
                                (void)wasm->load(d.path, "");
                } else if (std::filesystem::is_regular_file(p)) {
                    if (abi)
                        (void)abi->load(p, "");
                    if (wasm && p.extension() == ".wasm")
                        (void)wasm->load(p, "");
                }
                if (serviceManager_)
                    (void)serviceManager_->adoptModelProviderFromHosts();
            } catch (...) {
            }
            co_return SuccessResponse{"ok"};
        });
}

boost::asio::awaitable<Response>
RequestDispatcher::handlePluginTrustRemoveRequest(const PluginTrustRemoveRequest& req) {
    co_return co_await yams::daemon::dispatch::guard_await(
        "plugin_trust_remove", [this, req]() -> boost::asio::awaitable<Response> {
            auto abi = serviceManager_ ? serviceManager_->getAbiPluginHost() : nullptr;
            auto wasm = serviceManager_ ? serviceManager_->getWasmPluginHost() : nullptr;
            auto ext = serviceManager_ ? serviceManager_->getExternalPluginHost() : nullptr;
            bool ok = false;
            if (abi)
                if (auto r = abi->trustRemove(req.path))
                    ok = true;
            if (wasm)
                if (auto r = wasm->trustRemove(req.path))
                    ok = true;
            if (ext)
                if (auto r = ext->trustRemove(req.path))
                    ok = true;
            if (!ok)
                co_return ErrorResponse{ErrorCode::Unknown, "Trust remove failed"};
            co_return SuccessResponse{"ok"};
        });
}

} // namespace yams::daemon
