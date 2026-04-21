#pragma once

#include <yams/search/search_engine.h>
#include <yams/daemon/resource/model_provider.h>

#include <memory>
#include <functional>

namespace yams::search {

/**
 * @brief Adapter to bridge IModelProvider to IReranker interface
 *
 * Wraps the model provider's scoreDocuments capability to implement the
 * search engine's IReranker interface for cross-encoder document reranking.
 *
 * This uses the existing plugin infrastructure - the ONNX plugin provides
 * the model provider which implements scoreDocuments via OnnxRerankerSession.
 */
class ModelProviderRerankerAdapter : public IReranker {
public:
    using ProviderGetter = std::function<std::shared_ptr<daemon::IModelProvider>()>;

    explicit ModelProviderRerankerAdapter(ProviderGetter providerGetter)
        : providerGetter_(std::move(providerGetter)) {}

    ~ModelProviderRerankerAdapter() override = default;

    Result<std::vector<float>> scoreDocuments(const std::string& query,
                                              const std::vector<std::string>& documents) override {
        auto provider = providerGetter_();
        if (!provider || !provider->isAvailable()) {
            return Error{ErrorCode::InvalidState, "Model provider not available for reranking"};
        }
        return provider->scoreDocuments(query, documents);
    }

    bool isReady() const override {
        auto provider = providerGetter_();
        return provider && provider->isAvailable();
    }

private:
    ProviderGetter providerGetter_;
};

/**
 * @brief Reranker adapter that falls back to a secondary implementation when
 * the primary backend is unavailable or lacks a downloaded model.
 */
class FallbackRerankerAdapter : public IReranker {
public:
    FallbackRerankerAdapter(std::shared_ptr<IReranker> primary, std::shared_ptr<IReranker> fallback)
        : primary_(std::move(primary)), fallback_(std::move(fallback)) {}

    Result<std::vector<float>> scoreDocuments(const std::string& query,
                                              const std::vector<std::string>& documents) override {
        Error primaryError{ErrorCode::InvalidState, "Primary reranker unavailable"};
        if (primary_ && primary_->isReady()) {
            auto primaryResult = primary_->scoreDocuments(query, documents);
            if (primaryResult) {
                return primaryResult;
            }
            primaryError = primaryResult.error();
            if (!shouldFallback(primaryError.code)) {
                return primaryError;
            }
        }

        if (fallback_ && fallback_->isReady()) {
            return fallback_->scoreDocuments(query, documents);
        }

        return primaryError;
    }

    bool isReady() const override {
        return (primary_ && primary_->isReady()) || (fallback_ && fallback_->isReady());
    }

private:
    static bool shouldFallback(ErrorCode code) {
        switch (code) {
            case ErrorCode::NotFound:
            case ErrorCode::NotImplemented:
            case ErrorCode::NotInitialized:
            case ErrorCode::InvalidState:
                return true;
            default:
                return false;
        }
    }

    std::shared_ptr<IReranker> primary_;
    std::shared_ptr<IReranker> fallback_;
};

} // namespace yams::search
