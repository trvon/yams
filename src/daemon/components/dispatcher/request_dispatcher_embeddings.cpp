// Split from RequestDispatcher.cpp: embedding-related handlers
#include <sstream>
#include <yams/daemon/components/dispatch_utils.hpp>
#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/repair/embedding_repair_util.h>
#include <yams/vector/batch_metrics.h>
#include <yams/vector/dynamic_batcher.h>

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
RequestDispatcher::handleGenerateEmbeddingRequest(const GenerateEmbeddingRequest& req) {
    try {
        auto provRes = yams::daemon::dispatch::check_provider_ready(serviceManager_);
        if (!provRes) {
            if (daemon_)
                daemon_->setSubsystemDegraded("embedding", true, "provider_unavailable");
            co_return makeError(provRes.error().code, provRes.error().message);
        }
        const auto& provider = provRes.value();
        spdlog::info("Embedding request: model='{}' normalize={} text_len={}", req.modelName,
                     req.normalize ? "true" : "false", req.text.size());
        if (!req.modelName.empty() && !provider->isModelLoaded(req.modelName)) {
            int timeout_ms = 30000;
            if (const char* env = std::getenv("YAMS_MODEL_LOAD_TIMEOUT_MS")) {
                try {
                    timeout_ms = std::stoi(env);
                    if (timeout_ms < 1000)
                        timeout_ms = 1000;
                } catch (...) {
                }
            }
            auto lr = co_await yams::daemon::dispatch::ensure_model_loaded(
                serviceManager_, provider, req.modelName, timeout_ms);
            if (!lr) {
                co_return makeError(lr.error().code, lr.error().message);
            }
        }
        auto r = yams::daemon::dispatch::generate_single(provider.get(), req.modelName, req.text,
                                                         req.normalize);
        if (!r) {
            co_return makeError(r.error().code, r.error().message);
        }
        EmbeddingResponse resp;
        resp.embedding = std::move(r.value());
        resp.dimensions = resp.embedding.size();
        resp.modelUsed = req.modelName;
        resp.processingTimeMs = 0;
        co_return resp;
    } catch (const std::exception& e) {
        co_return makeError(ErrorCode::InternalError,
                            std::string("Embedding generation failed: ") + e.what());
    }
}

boost::asio::awaitable<Response>
RequestDispatcher::handleBatchEmbeddingRequest(const BatchEmbeddingRequest& req) {
    try {
        auto t0 = std::chrono::steady_clock::now();
        auto hex_preview = [](const std::string& s, std::size_t n = 8) {
            std::ostringstream oss;
            oss << std::hex;
            std::size_t lim = std::min(n, s.size());
            for (std::size_t i = 0; i < lim; ++i) {
                oss << static_cast<unsigned int>(static_cast<unsigned char>(s[i]));
                if (i + 1 < lim)
                    oss << ' ';
            }
            return oss.str();
        };
        spdlog::info("BatchEmbedding dispatch: model='{}' (len={}, hex[:8]={}) count={} "
                     "normalize={} batchSize={}",
                     req.modelName, req.modelName.size(), hex_preview(req.modelName),
                     req.texts.size(), req.normalize ? "true" : "false", req.batchSize);
        auto provRes = yams::daemon::dispatch::check_provider_ready(serviceManager_);
        if (!provRes)
            co_return makeError(provRes.error().code, provRes.error().message);
        const auto& provider = provRes.value();
        spdlog::info("Batch embedding request: model='{}' count={} normalize={} batchSize={}",
                     req.modelName, req.texts.size(), req.normalize ? "true" : "false",
                     req.batchSize);
        if (!req.modelName.empty() && !provider->isModelLoaded(req.modelName)) {
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
                serviceManager_, provider, req.modelName, timeout_ms);
            if (!r) {
                co_return makeError(r.error().code, r.error().message);
            }
        }
        BatchEmbeddingResponse resp;
        auto rr = yams::daemon::dispatch::generate_batch(provider.get(), req.modelName, req.texts,
                                                         req.normalize);
        if (!rr) {
            // Fallback to per-item when NotImplemented
            if (rr.error().code == ErrorCode::NotImplemented) {
                resp.embeddings.reserve(req.texts.size());
                for (const auto& t : req.texts) {
                    auto r1 = yams::daemon::dispatch::generate_single(provider.get(), req.modelName,
                                                                      t, req.normalize);
                    if (!r1) {
                        resp.failureCount++;
                        continue;
                    }
                    resp.embeddings.push_back(std::move(r1.value()));
                    resp.successCount++;
                }
            } else {
                co_return makeError(rr.error().code, rr.error().message);
            }
        } else {
            resp.embeddings = std::move(rr.value());
            resp.successCount = resp.embeddings.size();
            resp.failureCount = req.texts.size() > resp.embeddings.size()
                                    ? (req.texts.size() - resp.embeddings.size())
                                    : 0;
        }
        (void)t0;
        co_return resp;
    } catch (const std::exception& e) {
        co_return makeError(ErrorCode::InternalError,
                            std::string("Batch embedding failed: ") + e.what());
    }
}

boost::asio::awaitable<Response>
RequestDispatcher::handleEmbedDocumentsRequest(const EmbedDocumentsRequest& req) {
    if (!serviceManager_) {
        co_return ErrorResponse{ErrorCode::NotInitialized, "ServiceManager not available"};
    }
    try {
        auto es = serviceManager_->getEmbeddingProviderFsmSnapshot();
        if (es.state == EmbeddingProviderState::Degraded ||
            es.state == EmbeddingProviderState::Failed) {
            co_return ErrorResponse{ErrorCode::InvalidState,
                                    "Embedding generation disabled: provider degraded"};
        }
    } catch (...) {
    }
    auto signal = getWorkerJobSignal();
    if (signal) {
        signal(true);
    }

    auto contentStore = serviceManager_->getContentStore();
    auto metadataRepo = serviceManager_->getMetadataRepo();
    auto modelProvider = serviceManager_->getModelProvider();
    auto contentExtractors = serviceManager_->getContentExtractors();

    std::optional<yams::Result<yams::repair::EmbeddingRepairStats>> result;

    if (!modelProvider || !modelProvider->isAvailable()) {
        result = Error{ErrorCode::NotInitialized, "Model provider not available"};
    } else {
        std::string modelName;
        try {
            modelName = serviceManager_->getEmbeddingModelName();
        } catch (...) {
        }
        if (modelName.empty()) {
            result = Error{ErrorCode::NotInitialized, "No embedding model configured"};
        } else if (!contentStore) {
            result = Error{ErrorCode::NotInitialized, "Content store not available"};
        } else if (!metadataRepo) {
            result = Error{ErrorCode::NotInitialized, "Metadata repository not available"};
        } else {
            yams::repair::EmbeddingRepairConfig repairConfig;
            repairConfig.batchSize = req.batchSize;
            repairConfig.skipExisting = req.skipExisting;
            try {
                repairConfig.dataPath = serviceManager_->getResolvedDataDir();
            } catch (...) {
            }
            auto stats = yams::repair::repairMissingEmbeddings(
                contentStore, metadataRepo, modelProvider, modelName, repairConfig,
                req.documentHashes, nullptr, contentExtractors);
            result = std::move(stats);
        }
    }

    if (signal) {
        signal(false);
    }
    if (!result) {
        co_return ErrorResponse{ErrorCode::InternalError, "Embedding repair failed"};
    }
    if (!result->has_value()) {
        co_return ErrorResponse{result->error().code, result->error().message};
    }
    const auto& stats = result->value();
    EmbedDocumentsResponse resp;
    resp.requested = stats.documentsProcessed;
    resp.embedded = stats.embeddingsGenerated;
    resp.skipped = stats.embeddingsSkipped;
    resp.failed = stats.failedOperations;
    co_return resp;
}

} // namespace yams::daemon
