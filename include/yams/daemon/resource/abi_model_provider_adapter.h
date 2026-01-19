#pragma once

#include <functional>
#include <string>
#include <vector>

#include <yams/daemon/resource/model_provider.h>
#include <yams/plugins/model_provider_v1.h>

namespace yams::daemon {

class AbiModelProviderAdapter : public IModelProvider {
public:
    explicit AbiModelProviderAdapter(yams_model_provider_v1* table);
    ~AbiModelProviderAdapter() override = default;

    void setProgressCallback(std::function<void(const ModelLoadEvent&)> cb) override;

    Result<std::vector<float>> generateEmbedding(const std::string& text) override;
    Result<std::vector<std::vector<float>>>
    generateBatchEmbeddings(const std::vector<std::string>& texts) override;
    Result<std::vector<float>> generateEmbeddingFor(const std::string& modelName,
                                                    const std::string& text) override;
    Result<std::vector<std::vector<float>>>
    generateBatchEmbeddingsFor(const std::string& modelName,
                               const std::vector<std::string>& texts) override;

    Result<void> loadModel(const std::string& modelName) override;
    Result<void> loadModelWithOptions(const std::string& modelName,
                                      const std::string& optionsJson) override;
    Result<void> unloadModel(const std::string& modelName) override;
    bool isModelLoaded(const std::string& modelName) const override;
    std::vector<std::string> getLoadedModels() const override;
    size_t getLoadedModelCount() const override;

    Result<ModelInfo> getModelInfo(const std::string& modelName) const override;
    size_t getEmbeddingDim(const std::string& modelName) const override;
    std::shared_ptr<vector::EmbeddingGenerator>
    getEmbeddingGenerator(const std::string& modelName) override;

    std::string getProviderName() const override;
    std::string getProviderVersion() const override;
    bool isAvailable() const override;

    size_t getMemoryUsage() const override;
    void releaseUnusedResources() override;
    void shutdown() override;

    // v1.3: Cross-encoder reranking
    Result<std::vector<float>> scoreDocuments(const std::string& query,
                                              const std::vector<std::string>& documents) override;

private:
    yams_model_provider_v1* table_{};
    std::function<void(const ModelLoadEvent&)> progress_{};

    static Error mapStatus(yams_status_t st, const std::string& context = {});
    static const char* mapPhaseName(int phase);
    std::string pickDefaultModel() const;
};

} // namespace yams::daemon
