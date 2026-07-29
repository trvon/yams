// Split from RequestDispatcher.cpp: model-related handlers
#include <spdlog/spdlog.h>
#include <cstdlib>
#include <yams/common/utf8_utils.h>
#include <yams/daemon/components/dispatch_response.hpp>
#include <yams/daemon/components/dispatch_utils.hpp>
#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/daemon_lifecycle.h>

namespace yams::daemon {

static inline ErrorResponse makeError(ErrorCode code, const std::string& msg) {
    return yams::daemon::dispatch::makeErrorResponse(code, common::sanitizeUtf8Strict(msg));
}

boost::asio::awaitable<Response>
RequestDispatcher::handleLoadModelRequest(const LoadModelRequest& req) {
    spdlog::info("[RequestDispatcher] handleLoadModelRequest: model={}", req.modelName);
    try {
        auto provRes = yams::daemon::dispatch::check_provider_ready(serviceManager_);
        if (!provRes)
            co_return makeError(provRes.error().code, provRes.error().message);
        const auto& provider = provRes.value();
        if (req.modelName.empty()) {
            co_return makeError(ErrorCode::InvalidData, "modelName is required");
        }

        // Dedupe work: ensure_model_loaded() is already idempotent, but rebuild scheduling is not.
        // Capture whether this request actually changes provider state.
        bool was_loaded = false;
        try {
            was_loaded = provider->isModelLoaded(req.modelName);
        } catch (...) {
            // Intentional best-effort path; keep the primary operation unaffected.
        }

        int timeout_ms = 30000;
        if (const char* env = std::getenv("YAMS_MODEL_LOAD_TIMEOUT_MS")) {
            try {
                timeout_ms = std::stoi(env);
                if (timeout_ms < 1000)
                    timeout_ms = 1000;
            } catch (...) {
                // Intentional best-effort path; keep the primary operation unaffected.
            }
        }
        Result<void> r = co_await yams::daemon::dispatch::ensure_model_loaded(
            serviceManager_, provider, req.modelName, timeout_ms, req.optionsJson);
        if (!r) {
            if (lifecycle_) {
                lifecycle_->setSubsystemDegraded("embedding", true, "provider_load_failed");
            }
            co_return makeError(r.error().code, r.error().message);
        }
        ModelLoadResponse resp;
        resp.success = true;
        resp.modelName = req.modelName;
        resp.memoryUsageMb = provider->getMemoryUsage() / (1024 * 1024);
        resp.loadTimeMs = 0;

        // Model is now loaded via provider and the success response is complete. Only schedule
        // follow-up rebuild work after all response fields have been collected, so late provider
        // failures do not leave detached background work running during error teardown.
        try {
            if (serviceManager_ && lifecycle_) {
                lifecycle_->setSubsystemDegraded("embedding", false, "");
                try {
                    // Only rebuild when it helps:
                    // - A model transitioned from not-loaded -> loaded (vector scoring can now be
                    // enabled)
                    // - Or the current engine is unhealthy due to missing embedding generator
                    bool should_rebuild = !was_loaded;
                    if (!should_rebuild) {
                        try {
                            if (auto eng = serviceManager_->getCachedSearchEngine(); eng) {
                                auto hc = eng->healthCheck();
                                if (!hc) {
                                    // Heuristic: if the engine complains about missing embedding
                                    // generator, a rebuild after embeddings become available is
                                    // useful.
                                    if (hc.error().message.find(
                                            "Embedding generator not initialized") !=
                                        std::string::npos) {
                                        should_rebuild = true;
                                    }
                                }
                            }
                        } catch (...) {
                            // Intentional best-effort path; keep the primary operation unaffected.
                        }
                    }

                    if (should_rebuild) {
                        spdlog::info("[RequestDispatcher] scheduling enableEmbeddingsAndRebuild "
                                     "(model_loaded={}, model={})",
                                     was_loaded ? "true" : "false", req.modelName);
                        auto exec = serviceManager_->getWorkerExecutor();
                        auto self = serviceManager_;
                        boost::asio::co_spawn(
                            exec,
                            [self]() -> boost::asio::awaitable<void> {
                                co_await self->co_enableEmbeddingsAndRebuild();
                            },
                            boost::asio::detached);
                    } else {
                        spdlog::debug("[RequestDispatcher] skipping rebuild; model already loaded "
                                      "and engine healthy (model={})",
                                      req.modelName);
                    }
                } catch (...) {
                    // Intentional best-effort path; keep the primary operation unaffected.
                }
            }
        } catch (...) {
            // Intentional best-effort path; keep the primary operation unaffected.
        }
        co_return resp;
    } catch (const std::exception& e) {
        co_return makeError(ErrorCode::InternalError,
                            std::string("Load model failed: ") + e.what());
    }
}

boost::asio::awaitable<Response>
RequestDispatcher::handleUnloadModelRequest(const UnloadModelRequest& req) {
    co_return co_await yams::daemon::dispatch::guard_await(
        "unload_model", [this, req]() -> boost::asio::awaitable<Response> {
            auto provRes = yams::daemon::dispatch::check_provider_ready(serviceManager_);
            if (!provRes)
                co_return yams::daemon::dispatch::makeErrorResponse(provRes.error().code,
                                                                    provRes.error().message);
            const auto& provider = provRes.value();
            if (req.modelName.empty()) {
                co_return yams::daemon::dispatch::makeErrorResponse(ErrorCode::InvalidData,
                                                                    "modelName is required");
            }
            auto r = provider->unloadModel(req.modelName);
            if (!r) {
                co_return yams::daemon::dispatch::makeErrorResponse(r.error().code,
                                                                    r.error().message);
            }
            SuccessResponse resp{"Model unloaded"};
            co_return resp;
        });
}

boost::asio::awaitable<Response>
RequestDispatcher::handleModelStatusRequest(const ModelStatusRequest& req) {
    co_return co_await yams::daemon::dispatch::guard_await(
        "model_status", [this, req]() -> boost::asio::awaitable<Response> {
            auto provider = serviceManager_ ? serviceManager_->getModelProvider() : nullptr;
            ModelStatusResponse resp{};
            if (!provider || !provider->isAvailable()) {
                co_return resp;
            }
            auto loaded = provider->getLoadedModels();
            for (const auto& name : loaded) {
                if (!req.modelName.empty() && req.modelName != name)
                    continue;
                ModelStatusResponse::ModelDetails d{};
                d.name = name;
                d.path = "";
                d.loaded = true;
                d.isHot = true;
                d.memoryMb = 0;
                if (auto mi = provider->getModelInfo(name); mi) {
                    d.memoryMb = mi.value().memoryUsageBytes / (1024 * 1024);
                    d.maxSequenceLength = mi.value().maxSequenceLength;
                }
                d.embeddingDim = provider->getEmbeddingDim(name);
                d.requestCount = 0;
                d.errorCount = 0;
                d.loadTime = {};
                d.lastAccess = {};
                resp.models.push_back(std::move(d));
            }
            resp.totalMemoryMb = provider->getMemoryUsage() / (1024 * 1024);
            resp.maxMemoryMb = 0;
            co_return resp;
        });
}

} // namespace yams::daemon
