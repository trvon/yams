#pragma once

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <type_traits>
#include <utility>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <yams/core/types.h>
#include <yams/daemon/components/init_utils.hpp>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/WorkerPool.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/resource/model_provider.h>
#include <yams/daemon/resource/plugin_host.h>

#include <filesystem>
namespace yams::daemon::dispatch {
const std::vector<std::filesystem::path>& defaultAbiPluginDirs() noexcept;
}

namespace yams::daemon::dispatch {

template <typename Fn>
inline boost::asio::awaitable<std::remove_cvref_t<std::invoke_result_t<Fn&>>>
offload_to_worker(ServiceManager* sm, Fn&& fn) {
    using RawResult = std::invoke_result_t<Fn&>;
    using ResultType = std::remove_cvref_t<RawResult>;
    static_assert(!std::is_void_v<ResultType>, "offload_to_worker requires non-void result type");
    auto pool = sm ? sm->getWorkerPool() : nullptr;
    if (pool) {
        auto exec = pool->executor();
        auto shared = co_await boost::asio::co_spawn(
            exec,
            [fn = std::forward<Fn>(
                 fn)]() mutable -> boost::asio::awaitable<std::shared_ptr<ResultType>> {
                try {
                    auto value = fn();
                    co_return std::make_shared<ResultType>(std::move(value));
                } catch (...) {
                    std::rethrow_exception(std::current_exception());
                }
            },
            boost::asio::use_awaitable);
        co_return std::move(*shared);
    }
    co_return fn();
}

// Provider availability (adopt from hosts if absent). Synchronous helper used by
// request handlers to avoid duplicating adoption logic.
inline yams::Result<std::shared_ptr<IModelProvider>> ensure_provider_available(ServiceManager* sm) {
    try {
        if (!sm)
            return yams::Error{yams::ErrorCode::InvalidState, "ServiceManager unavailable"};
        spdlog::info("ensure_provider_available: checking provider");
        auto provider = sm->getModelProvider();
        if (provider && provider->isAvailable())
            return provider;
        spdlog::info("ensure_provider_available: provider not ready, autoloading");
        (void)sm->autoloadPluginsNow();
        auto adopted = sm->adoptModelProviderFromHosts();
        if (adopted && adopted.value()) {
            provider = sm->getModelProvider();
        }
        if (!provider || !provider->isAvailable()) {
            spdlog::info("ensure_provider_available: provider unavailable after autoload");
            return yams::Error{yams::ErrorCode::InvalidState, "Model provider unavailable"};
        }
        spdlog::info("ensure_provider_available: provider ready now");
        return provider;
    } catch (const std::exception& e) {
        spdlog::info("ensure_provider_available: exception {}", e.what());
        return yams::Error{yams::ErrorCode::InternalError, e.what()};
    }
}

// Load model with optional options JSON and timeout by offloading to ServiceManager worker pool.
inline boost::asio::awaitable<yams::Result<void>>
ensure_model_loaded(ServiceManager* sm, std::shared_ptr<IModelProvider> provider,
                    const std::string& model, int timeout_ms, const std::string& optionsJson = {}) {
    if (!provider)
        co_return yams::Error{yams::ErrorCode::InvalidState, "Provider null"};
    if (model.empty())
        co_return yams::Error{yams::ErrorCode::InvalidData, "Model name required"};
    if (provider->isModelLoaded(model))
        co_return yams::Result<void>();
    auto run_load = [provider, model, optionsJson]() -> yams::Result<void> {
        try {
            if (!optionsJson.empty())
                return provider->loadModelWithOptions(model, optionsJson);
            return provider->loadModel(model);
        } catch (const std::exception& e) {
            return yams::Error{yams::ErrorCode::InternalError, e.what()};
        }
    };
    co_return co_await yams::daemon::init::await_with_timeout<void>(
        [sm, run_load]() mutable -> boost::asio::awaitable<yams::Result<void>> {
            co_return co_await offload_to_worker(sm, run_load);
        },
        timeout_ms);
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
        // Phase 2.2: Use non-blocking cached snapshot instead of getSearchEngineSnapshot()
        auto cachedEngine = sm->getCachedSearchEngine();
        auto provider = sm->getModelProvider();
        if (provider) {
            try {
                d.embeddingsAvailable = provider->isAvailable();
            } catch (...) {
            }
        }
        if (cachedEngine) {
            try {
                const auto& cfg = cachedEngine->getConfig();
                d.scoringEnabled = (cfg.vector_weight > 0.0f) && d.embeddingsAvailable;
            } catch (...) {
            }
        }
        try {
            // Get build reason from FSM snapshot instead of lastSearchBuildReason_
            auto fsmSnapshot = sm->getSearchEngineFsmSnapshot();
            d.buildReason = fsmSnapshot.buildReason;
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
                        try {
                            auto es = sm->getEmbeddingProviderFsmSnapshot();
                            const bool providerDegraded =
                                (es.state == EmbeddingProviderState::Degraded ||
                                 es.state == EmbeddingProviderState::Failed);
                            if (providerDegraded) {
                                rec["degraded"] = true;
                                if (!sm->lastModelError().empty())
                                    rec["error"] = sm->lastModelError();
                            }
                        } catch (...) {
                        }
                    }
                } catch (...) {
                }
                arr.push_back(std::move(rec));
            }
        } else {
            // Legacy loader removed; no fallback list available
        }
    } catch (...) {
    }
    return {arr.dump(), count};
}

// Build typed providers list for StatusResponse
inline std::vector<yams::daemon::StatusResponse::ProviderInfo>
build_typed_providers(ServiceManager* sm, const yams::daemon::StateComponent* state) {
    std::vector<yams::daemon::StatusResponse::ProviderInfo> providers;
    if (!sm) {
        spdlog::debug("[build_typed_providers] ServiceManager is null");
        return providers;
    }
    try {
        const bool providerReady = state ? state->readiness.modelProviderReady.load() : false;
        bool providerDegraded = false;
        try {
            auto es = sm->getEmbeddingProviderFsmSnapshot();
            providerDegraded = (es.state == EmbeddingProviderState::Degraded ||
                                es.state == EmbeddingProviderState::Failed);
        } catch (...) {
        }
        const std::string lastErr = sm->lastModelError();
        const auto statusSnapshot = sm->getPluginStatusSnapshot();
        if (!statusSnapshot.records.empty()) {
            for (const auto& rec : statusSnapshot.records) {
                yams::daemon::StatusResponse::ProviderInfo p{};
                p.name = rec.name;
                p.isProvider = rec.isProvider;
                p.ready = rec.isProvider ? (rec.ready || providerReady) : rec.ready;
                p.degraded = rec.isProvider ? (rec.degraded || providerDegraded) : rec.degraded;
                p.error = rec.isProvider && !lastErr.empty() ? lastErr : rec.error;
                if (p.isProvider && p.error.empty())
                    p.error = lastErr;
                p.modelsLoaded = rec.modelsLoaded;
                spdlog::debug("[build_typed_providers]   snapshot plugin: name='{}' provider={}",
                              rec.name, rec.isProvider);
                providers.push_back(std::move(p));
            }
        } else {
            std::string adopted = sm->adoptedProviderPluginName();
            spdlog::debug(
                "[build_typed_providers] snapshot empty, falling back to FSM. adopted='{}'",
                adopted);
            const auto pluginSnap = sm->getPluginHostFsmSnapshot();
            for (const auto& name : pluginSnap.loadedPlugins) {
                yams::daemon::StatusResponse::ProviderInfo p{};
                p.name = name;
                p.isProvider = (!adopted.empty() && adopted == name);
                p.ready =
                    p.isProvider ? providerReady : (pluginSnap.state == PluginHostState::Ready);
                p.degraded =
                    p.isProvider ? providerDegraded : (pluginSnap.state == PluginHostState::Failed);
                p.error = p.isProvider ? lastErr : pluginSnap.lastError;
                p.modelsLoaded = 0u;
                providers.push_back(std::move(p));
            }
        }
    } catch (const std::exception& e) {
        spdlog::error("[build_typed_providers] Exception: {}", e.what());
    } catch (...) {
        spdlog::error("[build_typed_providers] Unknown exception");
    }
    spdlog::info("[build_typed_providers] Returning {} providers", providers.size());
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
