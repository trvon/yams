// Split from RequestDispatcher.cpp: model-related handlers
#include <spdlog/spdlog.h>
#include <cstdlib>
#include <yams/daemon/components/dispatch_utils.hpp>
#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/daemon_lifecycle.h>

namespace yams::daemon {

// Ensure error messages are valid UTF-8 for protobuf transport
static inline std::string sanitizeUtf8(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    const unsigned char* p = reinterpret_cast<const unsigned char*>(s.data());
    size_t i = 0, n = s.size();
    auto append_replacement = [&]() { out += "\xEF\xBF\xBD"; };
    while (i < n) {
        unsigned char c = p[i];
        if (c < 0x80) {
            out.push_back(static_cast<char>(c));
            i++;
            continue;
        }
        if ((c & 0xE0) == 0xC0 && i + 1 < n) {
            unsigned char c1 = p[i + 1];
            if ((c1 & 0xC0) == 0x80 && (c >= 0xC2)) {
                out.push_back(static_cast<char>(c));
                out.push_back(static_cast<char>(c1));
                i += 2;
                continue;
            }
        }
        if ((c & 0xF0) == 0xE0 && i + 2 < n) {
            unsigned char c1 = p[i + 1], c2 = p[i + 2];
            if ((c1 & 0xC0) == 0x80 && (c2 & 0xC0) == 0x80) {
                if (!(c == 0xE0 && c1 < 0xA0) && !(c == 0xED && c1 >= 0xA0)) {
                    out.push_back(static_cast<char>(c));
                    out.push_back(static_cast<char>(c1));
                    out.push_back(static_cast<char>(c2));
                    i += 3;
                    continue;
                }
            }
        }
        if ((c & 0xF8) == 0xF0 && i + 3 < n) {
            unsigned char c1 = p[i + 1], c2 = p[i + 2], c3 = p[i + 3];
            if ((c1 & 0xC0) == 0x80 && (c2 & 0xC0) == 0x80 && (c3 & 0xC0) == 0x80) {
                if (!(c == 0xF0 && c1 < 0x90) && !(c == 0xF4 && c1 >= 0x90) && c <= 0xF4) {
                    out.push_back(static_cast<char>(c));
                    out.push_back(static_cast<char>(c1));
                    out.push_back(static_cast<char>(c2));
                    out.push_back(static_cast<char>(c3));
                    i += 4;
                    continue;
                }
            }
        }
        append_replacement();
        i++;
    }
    return out;
}
static inline ErrorResponse makeError(ErrorCode code, const std::string& msg) {
    return ErrorResponse{code, sanitizeUtf8(msg)};
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
        }

        int timeout_ms = 30000;
        if (const char* env = std::getenv("YAMS_MODEL_LOAD_TIMEOUT_MS")) {
            try {
                timeout_ms = std::stoi(env);
                if (timeout_ms < 1000)
                    timeout_ms = 1000;
            } catch (...) {
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
        // Model is now loaded via provider, embeddings are ready
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
                            if (auto* eng = serviceManager_->getCachedSearchEngine(); eng) {
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
                }
            }
        } catch (...) {
        }
        ModelLoadResponse resp;
        resp.success = true;
        resp.modelName = req.modelName;
        resp.memoryUsageMb = provider->getMemoryUsage() / (1024 * 1024);
        resp.loadTimeMs = 0;
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
                co_return ErrorResponse{provRes.error().code, provRes.error().message};
            const auto& provider = provRes.value();
            if (req.modelName.empty()) {
                co_return ErrorResponse{ErrorCode::InvalidData, "modelName is required"};
            }
            auto r = provider->unloadModel(req.modelName);
            if (!r) {
                co_return ErrorResponse{r.error().code, r.error().message};
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
            ModelStatusResponse resp;
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
