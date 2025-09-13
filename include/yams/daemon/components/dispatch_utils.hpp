#pragma once

#include <algorithm>
#include <future>
#include <memory>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <yams/core/types.h>
#include <yams/daemon/components/init_utils.hpp>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/resource/model_provider.h>
#include <yams/daemon/resource/plugin_host.h>

namespace yams::daemon::dispatch {

// Provider availability (adopt from hosts if absent). Synchronous helper used by
// request handlers to avoid duplicating adoption logic.
inline yams::Result<std::shared_ptr<IModelProvider>> ensure_provider_available(ServiceManager* sm) {
    try {
        if (!sm)
            return yams::Error{yams::ErrorCode::InvalidState, "ServiceManager unavailable"};
        auto provider = sm->getModelProvider();
        if (provider && provider->isAvailable())
            return provider;
        (void)sm->autoloadPluginsNow();
        auto adopted = sm->adoptModelProviderFromHosts();
        if (adopted && adopted.value()) {
            provider = sm->getModelProvider();
        }
        if (!provider || !provider->isAvailable()) {
            return yams::Error{yams::ErrorCode::InvalidState, "Model provider unavailable"};
        }
        return provider;
    } catch (const std::exception& e) {
        return yams::Error{yams::ErrorCode::InternalError, e.what()};
    }
}

// Load model with optional options JSON and timeout. Uses std::async to remain synchronous
// from handler perspective.
inline yams::Result<void> ensure_model_loaded(IModelProvider* provider, const std::string& model,
                                              int timeout_ms, const std::string& optionsJson = {}) {
    if (!provider)
        return yams::Error{yams::ErrorCode::InvalidState, "Provider null"};
    if (model.empty())
        return yams::Error{yams::ErrorCode::InvalidData, "Model name required"};
    if (provider->isModelLoaded(model))
        return yams::Result<void>();
    try {
        std::future<yams::Result<void>> fut;
        if (!optionsJson.empty()) {
            fut = std::async(std::launch::async,
                             [&]() { return provider->loadModelWithOptions(model, optionsJson); });
        } else {
            fut = std::async(std::launch::async, [&]() { return provider->loadModel(model); });
        }
        if (fut.wait_for(std::chrono::milliseconds(timeout_ms)) == std::future_status::timeout) {
            return yams::Error{yams::ErrorCode::Timeout, "Model load timed out"};
        }
        return fut.get();
    } catch (const std::exception& e) {
        return yams::Error{yams::ErrorCode::InternalError, e.what()};
    }
}

// Embedding generation (single)
inline yams::Result<std::vector<float>> generate_single(IModelProvider* provider,
                                                        const std::string& model,
                                                        const std::string& text,
                                                        bool /*normalize*/) {
    if (!provider)
        return yams::Error{yams::ErrorCode::InvalidState, "Provider null"};
    try {
        if (model.empty())
            return provider->generateEmbedding(text);
        return provider->generateEmbeddingFor(model, text);
    } catch (const std::exception& e) {
        return yams::Error{yams::ErrorCode::InternalError, e.what()};
    }
}

// Embedding generation (batch)
inline yams::Result<std::vector<std::vector<float>>>
generate_batch(IModelProvider* provider, const std::string& model,
               const std::vector<std::string>& texts, bool /*normalize*/) {
    if (!provider)
        return yams::Error{yams::ErrorCode::InvalidState, "Provider null"};
    try {
        if (model.empty())
            return provider->generateBatchEmbeddings(texts);
        return provider->generateBatchEmbeddingsFor(model, texts);
    } catch (const std::exception& e) {
        return yams::Error{yams::ErrorCode::InternalError, e.what()};
    }
}

struct VectorDiag {
    bool embeddingsAvailable{false};
    bool scoringEnabled{false};
    std::string buildReason{"unknown"};
};

inline VectorDiag collect_vector_diag(ServiceManager* sm) {
    VectorDiag d;
    if (!sm)
        return d;
    try {
        auto eng = sm->getSearchEngineSnapshot();
        auto gen = sm->getEmbeddingGenerator();
        if (gen) {
            try {
                d.embeddingsAvailable = gen->isInitialized();
            } catch (...) {
            }
        }
        if (eng) {
            try {
                const auto& cfg = eng->getConfig();
                d.scoringEnabled = (cfg.vector_weight > 0.0f) && d.embeddingsAvailable;
            } catch (...) {
            }
        }
        try {
            d.buildReason = sm->getLastSearchBuildReason();
        } catch (...) {
        }
    } catch (...) {
    }
    return d;
}

// Re-export coroutine timeout/retry wrappers for dispatcher locality
template <typename T, typename Fn>
inline boost::asio::awaitable<yams::Result<T>> await_with_timeout(Fn&& fn, int timeout_ms) {
    co_return co_await yams::daemon::init::await_with_timeout<T>(std::forward<Fn>(fn), timeout_ms);
}
template <typename T, typename Fn, typename Backoff>
inline boost::asio::awaitable<yams::Result<T>> await_with_retry(Fn&& fn, int attempts,
                                                                Backoff backoff_ms) {
    co_return co_await yams::daemon::init::await_with_retry<T>(std::forward<Fn>(fn), attempts,
                                                               backoff_ms);
}

// Build plugins JSON snapshot and count
inline std::pair<std::string, size_t> build_plugins_json(ServiceManager* sm) {
    nlohmann::json arr = nlohmann::json::array();
    size_t count = 0;
    try {
        if (!sm)
            return {arr.dump(), 0};
        auto* abi = sm->getAbiPluginHost();
        if (abi) {
            auto descs = abi->listLoaded();
            count += descs.size();
            for (const auto& d : descs) {
                nlohmann::json rec;
                rec["name"] = d.name;
                rec["path"] = d.path.string();
                if (!d.interfaces.empty())
                    rec["interfaces"] = d.interfaces;
                try {
                    if (!sm->adoptedProviderPluginName().empty() &&
                        sm->adoptedProviderPluginName() == d.name) {
                        rec["provider"] = true;
                        if (auto mp = sm->getModelProvider()) {
                            rec["models_loaded"] = mp->getLoadedModels().size();
                        }
                        if (sm->isModelProviderDegraded()) {
                            rec["degraded"] = true;
                            if (!sm->lastModelError().empty())
                                rec["error"] = sm->lastModelError();
                        }
                    }
                } catch (...) {
                }
                arr.push_back(std::move(rec));
            }
        } else {
            auto plugins = sm->getLoadedPlugins();
            count = plugins.size();
            for (const auto& pi : plugins) {
                nlohmann::json rec;
                rec["name"] = pi.name;
                rec["path"] = pi.path.string();
                arr.push_back(std::move(rec));
            }
        }
    } catch (...) {
    }
    return {arr.dump(), count};
}

// Build typed providers list for StatusResponse
inline std::vector<yams::daemon::StatusResponse::ProviderInfo>
build_typed_providers(ServiceManager* sm, const yams::daemon::StateComponent* state) {
    std::vector<yams::daemon::StatusResponse::ProviderInfo> providers;
    if (!sm)
        return providers;
    try {
        const bool providerReady = state ? state->readiness.modelProviderReady.load() : false;
        const bool providerDegraded = sm->isModelProviderDegraded();
        const std::string lastErr = sm->lastModelError();
        uint32_t modelsLoaded = 0;
        try {
            auto mp = sm->getModelProvider();
            if (mp)
                modelsLoaded = static_cast<uint32_t>(mp->getLoadedModels().size());
        } catch (...) {
        }
        std::string adopted = sm->adoptedProviderPluginName();
        if (auto* abi = sm->getAbiPluginHost()) {
            for (const auto& d : abi->listLoaded()) {
                yams::daemon::StatusResponse::ProviderInfo p{};
                p.name = d.name;
                p.isProvider = (!adopted.empty() && adopted == d.name);
                p.ready =
                    p.isProvider ? providerReady : true; // loaded plugins are considered ready
                p.degraded = p.isProvider ? providerDegraded : false;
                p.error = p.isProvider ? lastErr : std::string();
                p.modelsLoaded = p.isProvider ? modelsLoaded : 0u;
                providers.push_back(std::move(p));
            }
        } else {
            for (const auto& pi : sm->getLoadedPlugins()) {
                yams::daemon::StatusResponse::ProviderInfo p{};
                p.name = pi.name;
                p.isProvider = (!adopted.empty() && adopted == pi.name);
                p.ready = p.isProvider ? providerReady : true;
                p.degraded = p.isProvider ? providerDegraded : false;
                p.error = p.isProvider ? lastErr : std::string();
                p.modelsLoaded = p.isProvider ? modelsLoaded : 0u;
                providers.push_back(std::move(p));
            }
        }
    } catch (...) {
    }
    return providers;
}

// guard_await: standardize exception capture and error mapping for awaitable handlers.
template <typename Fn> boost::asio::awaitable<Response> guard_await(const char* tag, Fn&& fn) {
    try {
        co_return co_await fn();
    } catch (const std::exception& e) {
        try {
            spdlog::warn("[Dispatch] {}: exception: {}", tag, e.what());
        } catch (...) {
        }
        co_return ErrorResponse{ErrorCode::InternalError, e.what()};
    } catch (...) {
        try {
            spdlog::warn("[Dispatch] {}: unknown exception", tag);
        } catch (...) {
        }
        co_return ErrorResponse{ErrorCode::InternalError, "unknown exception"};
    }
}

} // namespace yams::daemon::dispatch
