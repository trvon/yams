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

} // namespace yams::search
