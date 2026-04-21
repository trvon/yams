#include <yams/daemon/resource/simeon_model_provider.h>

#include <yams/vector/embedding_generator.h>
#include <yams/vector/simeon_embedding_backend.h>

#include <spdlog/spdlog.h>

#include <atomic>
#include <chrono>
#include <cmath>
#include <mutex>
#include <span>
#include <string>
#include <unordered_set>
#include <vector>

namespace yams::daemon {

namespace {

class SimeonModelProvider final : public IModelProvider {
public:
    explicit SimeonModelProvider(std::size_t embeddingDim) : embeddingDim_(embeddingDim) {
        vector::EmbeddingConfig cfg;
        cfg.backend = vector::EmbeddingConfig::Backend::Simeon;
        cfg.embedding_dim = embeddingDim_;
        backend_ = vector::makeSimeonBackend(cfg);
        if (!backend_ || !backend_->initialize()) {
            spdlog::error("[SimeonModelProvider] failed to initialize simeon backend");
            backend_.reset();
            return;
        }
        embeddingDim_ = backend_->getEmbeddingDimension();
        loadTime_ = std::chrono::system_clock::now();
        spdlog::info("[SimeonModelProvider] ready (dim={})", embeddingDim_);
    }

    ~SimeonModelProvider() override { shutdown(); }

    Result<std::vector<float>> generateEmbedding(const std::string& text) override {
        if (!backend_)
            return Error{ErrorCode::NotInitialized, "SimeonModelProvider not initialized"};
        auto r = backend_->generateEmbedding(text);
        if (r)
            ++requestCount_;
        return r;
    }

    Result<std::vector<float>> generateEmbeddingFor(const std::string& /*modelName*/,
                                                    const std::string& text) override {
        return generateEmbedding(text);
    }

    Result<std::vector<std::vector<float>>>
    generateBatchEmbeddings(const std::vector<std::string>& texts) override {
        if (!backend_)
            return Error{ErrorCode::NotInitialized, "SimeonModelProvider not initialized"};
        auto r = backend_->generateEmbeddings(std::span<const std::string>(texts));
        if (r)
            requestCount_ += texts.size();
        return r;
    }

    Result<std::vector<std::vector<float>>>
    generateBatchEmbeddingsFor(const std::string& /*modelName*/,
                               const std::vector<std::string>& texts) override {
        return generateBatchEmbeddings(texts);
    }

    Result<std::vector<float>> scoreDocuments(const std::string& query,
                                              const std::vector<std::string>& documents) override {
        if (!backend_)
            return Error{ErrorCode::NotInitialized, "SimeonModelProvider not initialized"};
        if (documents.empty())
            return std::vector<float>{};

        auto qEmbed = backend_->generateEmbedding(query);
        if (!qEmbed)
            return qEmbed.error();
        const auto& q = qEmbed.value();

        auto dEmbeds = backend_->generateEmbeddings(std::span<const std::string>(documents));
        if (!dEmbeds)
            return dEmbeds.error();
        const auto& ds = dEmbeds.value();
        if (ds.size() != documents.size())
            return Error{ErrorCode::InternalError, "SimeonModelProvider: embedding count mismatch"};

        std::vector<float> scores;
        scores.reserve(ds.size());
        double qMag = 0.0;
        for (float f : q)
            qMag += static_cast<double>(f) * static_cast<double>(f);
        qMag = std::sqrt(qMag);
        if (qMag == 0.0)
            qMag = 1.0;

        for (const auto& d : ds) {
            if (d.size() != q.size()) {
                scores.push_back(0.0f);
                continue;
            }
            double dot = 0.0;
            double dMag = 0.0;
            for (std::size_t i = 0; i < q.size(); ++i) {
                const double a = static_cast<double>(q[i]);
                const double b = static_cast<double>(d[i]);
                dot += a * b;
                dMag += b * b;
            }
            dMag = std::sqrt(dMag);
            const double denom = qMag * (dMag == 0.0 ? 1.0 : dMag);
            scores.push_back(static_cast<float>(dot / denom));
        }

        requestCount_ += documents.size();
        return scores;
    }

    Result<void> loadModel(const std::string& modelName) override {
        if (!backend_)
            return Error{ErrorCode::NotInitialized, "SimeonModelProvider not initialized"};
        std::lock_guard<std::mutex> lock(mu_);
        loaded_.insert(modelName.empty() ? defaultModel() : modelName);
        return Result<void>();
    }

    Result<void> unloadModel(const std::string& modelName) override {
        std::lock_guard<std::mutex> lock(mu_);
        loaded_.erase(modelName);
        return Result<void>();
    }

    bool isModelLoaded(const std::string& modelName) const override {
        std::lock_guard<std::mutex> lock(mu_);
        if (loaded_.count(modelName))
            return true;
        return modelName.empty() && !loaded_.empty();
    }

    std::vector<std::string> getLoadedModels() const override {
        std::lock_guard<std::mutex> lock(mu_);
        return {loaded_.begin(), loaded_.end()};
    }

    size_t getLoadedModelCount() const override {
        std::lock_guard<std::mutex> lock(mu_);
        return loaded_.size();
    }

    Result<ModelInfo> getModelInfo(const std::string& modelName) const override {
        std::lock_guard<std::mutex> lock(mu_);
        if (loaded_.find(modelName) == loaded_.end())
            return Error{ErrorCode::NotFound, "Model not loaded: " + modelName};
        ModelInfo info;
        info.name = modelName;
        info.path = "simeon://" + modelName;
        info.embeddingDim = embeddingDim_;
        info.maxSequenceLength = 0;
        info.memoryUsageBytes = 0;
        info.loadTime = loadTime_;
        info.requestCount = requestCount_.load();
        info.errorCount = 0;
        return info;
    }

    size_t getEmbeddingDim(const std::string& /*modelName*/) const override {
        return embeddingDim_;
    }

    std::shared_ptr<vector::EmbeddingGenerator>
    getEmbeddingGenerator(const std::string& /*modelName*/) override {
        if (!backend_)
            return nullptr;
        vector::EmbeddingConfig cfg;
        cfg.backend = vector::EmbeddingConfig::Backend::Simeon;
        cfg.embedding_dim = embeddingDim_;
        auto freshBackend = vector::makeSimeonBackend(cfg);
        if (!freshBackend || !freshBackend->initialize())
            return nullptr;
        auto gen = std::make_shared<vector::EmbeddingGenerator>(std::move(freshBackend), cfg);
        if (!gen->initialize())
            return nullptr;
        return gen;
    }

    std::string getProviderName() const override { return "Simeon"; }
    std::string getProviderVersion() const override { return "1.0.0"; }
    bool isAvailable() const override { return backend_ != nullptr; }
    bool isTrainingFree() const override { return true; }
    size_t getMemoryUsage() const override { return 0; }
    void releaseUnusedResources() override {}

    void shutdown() override {
        std::lock_guard<std::mutex> lock(mu_);
        if (backend_) {
            backend_->shutdown();
            backend_.reset();
        }
        loaded_.clear();
    }

private:
    static std::string defaultModel() { return std::string("simeon-default"); }

    mutable std::mutex mu_;
    std::unique_ptr<vector::IEmbeddingBackend> backend_;
    std::size_t embeddingDim_{384};
    std::unordered_set<std::string> loaded_;
    std::chrono::system_clock::time_point loadTime_{};
    std::atomic<std::size_t> requestCount_{0};
};

} // namespace

std::unique_ptr<IModelProvider> makeSimeonModelProvider(std::size_t embeddingDim) {
    return std::make_unique<SimeonModelProvider>(embeddingDim ? embeddingDim : 1024);
}

void forceLinkSimeonProvider() noexcept {}

namespace {
const ModelProviderFactoryRegistration s_simeon_registration{
    "simeon", [] { return makeSimeonModelProvider(); }};
} // namespace

} // namespace yams::daemon
