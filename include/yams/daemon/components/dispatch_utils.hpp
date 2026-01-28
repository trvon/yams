#pragma once

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <type_traits>
#include <utility>
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <yams/core/types.h>
#include <yams/daemon/components/init_utils.hpp>
#include <yams/daemon/components/PluginHostFsm.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/WorkCoordinator.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/resource/external_plugin_host.h>
#include <yams/daemon/resource/model_provider.h>
#include <yams/daemon/resource/plugin_host.h>
#include <yams/search/search_engine.h>

#include <filesystem>

namespace yams::daemon::dispatch {

// Forward declarations
const std::vector<std::filesystem::path>& defaultAbiPluginDirs() noexcept;
const std::vector<std::filesystem::path>& defaultExternalPluginDirs() noexcept;

/**
 * @brief Offload a synchronous function to the WorkCoordinator's thread pool.
 *
 * This coroutine dispatches the provided callable to the WorkCoordinator's
 * executor, suspending the caller until the work completes on a worker thread.
 * This prevents blocking the IPC connection strand with heavy database
 * operations like list queries and snippet hydration.
 *
 * Uses boost::asio::async_initiate (non-experimental) to properly integrate
 * with the async operation model, posting completion back to the caller's executor.
 *
 * @tparam Fn Callable type that returns the result
 * @param sm ServiceManager providing access to WorkCoordinator
 * @param fn The synchronous function to execute on a worker thread
 * @return awaitable yielding the result of fn()
 */
template <typename Fn>
inline boost::asio::awaitable<std::remove_cvref_t<std::invoke_result_t<Fn&>>>
offload_to_worker(ServiceManager* sm, Fn&& fn) {
    using RawResult = std::invoke_result_t<Fn&>;
    using ResultType = std::remove_cvref_t<RawResult>;
    static_assert(!std::is_void_v<ResultType>, "offload_to_worker requires non-void result type");

    WorkCoordinator* coordinator = sm ? sm->getWorkCoordinator() : nullptr;
    if (!coordinator || !coordinator->isRunning()) {
        spdlog::debug("offload_to_worker: WorkCoordinator unavailable, sync exec");
        co_return fn();
    }

    auto work_executor = coordinator->getExecutor();
    auto ioCtx = coordinator->getIOContext();
    if (!ioCtx || ioCtx->stopped()) {
        spdlog::warn("offload_to_worker: io_context stopped, sync exec");
        co_return fn();
    }

    // Use a shared_ptr to track if work has started executing
    auto work_started = std::make_shared<std::atomic<bool>>(false);
    auto work_done = std::make_shared<std::atomic<bool>>(false);

    co_return co_await boost::asio::async_initiate<decltype(boost::asio::use_awaitable),
                                                   void(std::exception_ptr, ResultType)>(
        [work_executor, ioCtx, work_started, work_done](auto handler, auto f) mutable {
            if (ioCtx->stopped()) {
                auto completion_executor = boost::asio::get_associated_executor(handler);
                boost::asio::post(completion_executor,
                                  [handler = std::move(handler), f = std::move(f)]() mutable {
                                      std::exception_ptr ep;
                                      ResultType result{};
                                      try {
                                          result = f();
                                      } catch (...) {
                                          ep = std::current_exception();
                                      }
                                      std::move(handler)(ep, std::move(result));
                                  });
                return;
            }
            boost::asio::post(work_executor, [handler = std::move(handler), f = std::move(f),
                                              work_started, work_done]() mutable {
                work_started->store(true, std::memory_order_release);
                std::exception_ptr ep;
                ResultType result{};
                try {
                    result = f();
                } catch (const std::exception& e) {
                    spdlog::error("offload_to_worker: exception: {}", e.what());
                    ep = std::current_exception();
                } catch (...) {
                    spdlog::error("offload_to_worker: unknown exception");
                    ep = std::current_exception();
                }
                work_done->store(true, std::memory_order_release);
                auto completion_executor = boost::asio::get_associated_executor(handler);
                boost::asio::post(completion_executor, [handler = std::move(handler), ep,
                                                        result = std::move(result)]() mutable {
                    std::move(handler)(ep, std::move(result));
                });
            });
        },
        boost::asio::use_awaitable, std::forward<Fn>(fn));
}

inline yams::Result<std::shared_ptr<IModelProvider>>
check_provider_ready(const ServiceManager* sm) {
    try {
        if (!sm)
            return yams::Error{yams::ErrorCode::InvalidState, "ServiceManager unavailable"};

        auto provider = sm->getModelProvider();
        if (provider && provider->isAvailable()) {
            spdlog::debug("check_provider_ready: provider available");
            return provider;
        }

        auto pluginStatus = sm->getPluginStatusSnapshot();
        auto hostState = pluginStatus.host.state;

        std::string message;
        switch (hostState) {
            case PluginHostState::NotInitialized:
                message = "Plugin system not yet initialized - daemon is starting up";
                break;
            case PluginHostState::ScanningDirectories:
                message = "Plugin system scanning directories - initialization in progress";
                break;
            case PluginHostState::LoadingPlugins:
                message = "Plugin system loading plugins - initialization in progress";
                break;
            case PluginHostState::Failed:
                message = "Plugin system failed to initialize";
                if (!pluginStatus.host.lastError.empty()) {
                    message += ": " + pluginStatus.host.lastError;
                }
                break;
            case PluginHostState::Ready:
                message = "Plugin system ready but model provider not available";
                for (const auto& rec : pluginStatus.records) {
                    if (rec.isProvider) {
                        if (!rec.ready) {
                            message = "Model provider plugin loaded but not ready";
                            if (!rec.error.empty()) {
                                message += ": " + rec.error;
                            }
                        } else if (rec.degraded) {
                            message = "Model provider is degraded";
                            if (!rec.error.empty()) {
                                message += ": " + rec.error;
                            }
                        }
                        break;
                    }
                }
                break;
        }

        spdlog::info("check_provider_ready: provider not ready - {}", message);
        return yams::Error{yams::ErrorCode::InvalidState, message};
    } catch (const std::exception& e) {
        spdlog::warn("check_provider_ready: exception {}", e.what());
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
        if (texts.empty())
            return std::vector<std::vector<float>>{};

        std::size_t cap = TuneAdvisor::getEmbedDocCap();
        if (cap == 0 || cap > texts.size())
            cap = texts.size();
        if (cap < 1)
            cap = 1;

        if (texts.size() <= cap) {
            if (model.empty())
                return provider->generateBatchEmbeddings(texts);
            return provider->generateBatchEmbeddingsFor(model, texts);
        }

        std::vector<std::vector<float>> out;
        out.reserve(texts.size());
        for (std::size_t start = 0; start < texts.size(); start += cap) {
            std::size_t end = std::min(start + cap, texts.size());
            std::vector<std::string> sub(texts.begin() + start, texts.begin() + end);
            yams::Result<std::vector<std::vector<float>>> r;
            if (model.empty())
                r = provider->generateBatchEmbeddings(sub);
            else
                r = provider->generateBatchEmbeddingsFor(model, sub);
            if (!r)
                return r.error();
            auto& batch = r.value();
            if (batch.size() != sub.size()) {
                return yams::Error{yams::ErrorCode::InvalidData, "Batch embedding size mismatch"};
            }
            for (auto& v : batch)
                out.push_back(std::move(v));
        }
        return out;
    } catch (const std::exception& e) {
        return yams::Error{yams::ErrorCode::InternalError, e.what()};
    }
}

struct VectorDiag {
    bool embeddingsAvailable{false};
    bool scoringEnabled{false};
    std::string buildReason{"unknown"};
};

inline VectorDiag collect_vector_diag(const ServiceManager* sm) {
    VectorDiag d;
    if (!sm)
        return d;
    try {
        // Phase 2.2: Use non-blocking cached snapshot instead of getSearchEngineSnapshot()
        const auto* cachedEngine = sm->getCachedSearchEngine();
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
                d.scoringEnabled = (cfg.vectorWeight > 0.0f) && d.embeddingsAvailable;
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
inline std::pair<std::string, size_t> build_plugins_json(const ServiceManager* sm) {
    nlohmann::json arr = nlohmann::json::array();
    size_t count = 0;
    try {
        if (!sm)
            return {arr.dump(), 0};

        // Helper to add plugin descriptors to result
        auto addPlugins = [&](const std::vector<PluginDescriptor>& descs, const std::string& type) {
            count += descs.size();
            for (const auto& d : descs) {
                nlohmann::json rec;
                rec["name"] = d.name;
                rec["path"] = d.path.string();
                rec["type"] = type;
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
        };

        // ABI (native) plugins
        const auto* abi = sm->getAbiPluginHost();
        if (abi) {
            addPlugins(abi->listLoaded(), "native");
        }

        // External (Python/JS) plugins
        const auto* external = sm->getExternalPluginHost();
        if (external) {
            addPlugins(external->listLoaded(), "external");
        }
    } catch (...) {
    }
    return {arr.dump(), count};
}

// Build typed providers list for StatusResponse
inline std::vector<yams::daemon::StatusResponse::ProviderInfo>
build_typed_providers(const ServiceManager* sm, const yams::daemon::StateComponent* state) {
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
        // Use cached snapshot - refreshed on plugin load/unload events, not on every status
        // request. Calling refreshPluginStatusSnapshot() here caused 5+ second blocking when
        // external plugins (e.g., Ghidra) are slow, leading to CLI timeouts on `yams daemon status
        // -d`.
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
                p.interfaces = rec.interfaces;
                p.capabilities = rec.capabilities;
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
    spdlog::debug("[build_typed_providers] Returning {} providers", providers.size());
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
