#include <yams/daemon/resource/simeon_model_provider.h>

#include <yams/vector/embedding_generator.h>
#include <yams/vector/simeon_embedding_backend.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cmath>
#include <limits>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

namespace yams::daemon {

namespace {

std::vector<std::string> extractDocumentFragments(std::string_view text,
                                                  std::size_t maxFragments = 8) {
    std::vector<std::string> fragments;
    if (text.empty()) {
        return fragments;
    }

    std::size_t start = 0;
    while (start < text.size() && fragments.size() < maxFragments) {
        while (start < text.size() && (std::isspace(static_cast<unsigned char>(text[start])) ||
                                       text[start] == '\r' || text[start] == '\n')) {
            ++start;
        }
        if (start >= text.size()) {
            break;
        }

        std::size_t end = start;
        while (end < text.size()) {
            char c = text[end];
            if (c == '\n' || c == '\r') {
                break;
            }
            if ((c == '.' || c == '?' || c == '!' || c == ';') &&
                (end + 1 == text.size() ||
                 std::isspace(static_cast<unsigned char>(text[end + 1])))) {
                ++end;
                break;
            }
            ++end;
        }

        std::string_view seg = text.substr(start, end - start);
        while (!seg.empty() && std::isspace(static_cast<unsigned char>(seg.back()))) {
            seg.remove_suffix(1);
        }

        if (seg.size() >= 5) {
            fragments.emplace_back(seg);
        }
        start = end + 1;
    }

    if (fragments.empty()) {
        fragments.emplace_back(text);
    } else if (fragments.size() > 1 && fragments.size() < maxFragments) {
        std::string fullDoc(text.substr(0, std::min<std::size_t>(text.size(), 512)));
        bool found = false;
        for (const auto& f : fragments) {
            if (f == fullDoc) {
                found = true;
                break;
            }
        }
        if (!found) {
            fragments.push_back(std::move(fullDoc));
        }
    }

    return fragments;
}

inline float computeCosineSimilarity(std::span<const float> a, std::span<const float> b) {
    if (a.size() != b.size() || a.empty()) {
        return 0.0f;
    }
    double dot = 0.0;
    double aMag = 0.0;
    double bMag = 0.0;
    for (std::size_t i = 0; i < a.size(); ++i) {
        const double av = static_cast<double>(a[i]);
        const double bv = static_cast<double>(b[i]);
        dot += av * bv;
        aMag += av * av;
        bMag += bv * bv;
    }
    const double denom = std::sqrt(aMag) * std::sqrt(bMag);
    if (denom <= 0.0) {
        return 0.0f;
    }
    return static_cast<float>(dot / denom);
}

class SimeonModelProvider final : public IModelProvider {
public:
    explicit SimeonModelProvider(
        std::size_t embeddingDim,
        SimeonScoringMode scoringMode = SimeonScoringMode::SingleVectorCosine)
        : embeddingDim_(embeddingDim), scoringMode_(scoringMode) {
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

    void setScoringMode(SimeonScoringMode mode) noexcept {
        std::lock_guard<std::mutex> lock(mu_);
        scoringMode_ = mode;
    }

    [[nodiscard]] SimeonScoringMode scoringMode() const noexcept {
        std::lock_guard<std::mutex> lock(mu_);
        return scoringMode_;
    }

    Result<std::vector<float>>
    scoreDocumentsSingleVectorCosine(const std::string& query,
                                     const std::vector<std::string>& documents) {
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
        for (const auto& d : ds) {
            scores.push_back(computeCosineSimilarity(q, d));
        }

        requestCount_ += documents.size();
        return scores;
    }

    Result<std::vector<float>>
    scoreDocumentsOuterMaxSim(const std::string& query, const std::vector<std::string>& documents) {
        auto queryFragments = extractDocumentFragments(query, 4);
        if (queryFragments.empty()) {
            queryFragments.push_back(query);
        }

        auto qEmbeds = backend_->generateEmbeddings(std::span<const std::string>(queryFragments));
        if (!qEmbeds) {
            return qEmbeds.error();
        }
        const auto& qs = qEmbeds.value();
        if (qs.size() != queryFragments.size()) {
            return Error{ErrorCode::InternalError,
                         "SimeonModelProvider: query embedding count mismatch"};
        }

        std::vector<std::string> flatFragments;
        std::vector<std::pair<std::size_t, std::size_t>> docSpans;
        docSpans.reserve(documents.size());

        for (const auto& doc : documents) {
            auto frags = extractDocumentFragments(doc, 8);
            if (frags.empty()) {
                frags.push_back(doc);
            }
            std::size_t startIdx = flatFragments.size();
            for (auto& f : frags) {
                flatFragments.push_back(std::move(f));
            }
            std::size_t endIdx = flatFragments.size();
            docSpans.emplace_back(startIdx, endIdx);
        }

        auto dEmbeds = backend_->generateEmbeddings(std::span<const std::string>(flatFragments));
        if (!dEmbeds) {
            return dEmbeds.error();
        }
        const auto& ds = dEmbeds.value();
        if (ds.size() != flatFragments.size()) {
            return Error{ErrorCode::InternalError,
                         "SimeonModelProvider: fragment embedding count mismatch"};
        }

        std::vector<float> scores;
        scores.reserve(documents.size());

        for (std::size_t docIdx = 0; docIdx < documents.size(); ++docIdx) {
            const auto [startIdx, endIdx] = docSpans[docIdx];
            if (startIdx >= endIdx) {
                scores.push_back(0.0f);
                continue;
            }

            double qSimSum = 0.0;
            for (const auto& qVec : qs) {
                float maxSim = -std::numeric_limits<float>::infinity();
                for (std::size_t fi = startIdx; fi < endIdx; ++fi) {
                    float sim = computeCosineSimilarity(qVec, ds[fi]);
                    if (sim > maxSim) {
                        maxSim = sim;
                    }
                }
                if (std::isfinite(maxSim)) {
                    qSimSum += static_cast<double>(maxSim);
                }
            }
            scores.push_back(static_cast<float>(qSimSum / static_cast<double>(qs.size())));
        }

        requestCount_ += documents.size();
        return scores;
    }

    Result<std::vector<float>> scoreDocuments(const std::string& query,
                                              const std::vector<std::string>& documents) override {
        if (!backend_)
            return Error{ErrorCode::NotInitialized, "SimeonModelProvider not initialized"};
        if (documents.empty())
            return std::vector<float>{};

        SimeonScoringMode mode;
        {
            std::lock_guard<std::mutex> lock(mu_);
            mode = scoringMode_;
        }

        if (mode == SimeonScoringMode::FragmentOuterMaxSim) {
            return scoreDocumentsOuterMaxSim(query, documents);
        }
        return scoreDocumentsSingleVectorCosine(query, documents);
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
        std::lock_guard<std::mutex> lock(mu_);
        if (!backend_)
            return nullptr;
        if (embeddingGenerator_) {
            return embeddingGenerator_;
        }
        vector::EmbeddingConfig cfg;
        cfg.backend = vector::EmbeddingConfig::Backend::Simeon;
        cfg.embedding_dim = embeddingDim_;
        auto freshBackend = vector::makeSimeonBackend(cfg);
        if (!freshBackend || !freshBackend->initialize())
            return nullptr;
        auto gen = std::make_shared<vector::EmbeddingGenerator>(std::move(freshBackend), cfg);
        if (!gen->initialize())
            return nullptr;
        embeddingGenerator_ = gen;
        return embeddingGenerator_;
    }

    std::string getProviderName() const override { return "Simeon"; }
    std::string getProviderVersion() const override { return "1.0.0"; }
    std::string getEmbeddingSpaceIdentity(const std::string& /*modelName*/) const override {
        return backend_ ? backend_->getEmbeddingSpaceIdentity() : std::string{};
    }
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
        embeddingGenerator_.reset();
        loaded_.clear();
    }

private:
    static std::string defaultModel() { return std::string("simeon-default"); }

    mutable std::mutex mu_;
    std::unique_ptr<vector::IEmbeddingBackend> backend_;
    std::shared_ptr<vector::EmbeddingGenerator> embeddingGenerator_;
    std::size_t embeddingDim_{384};
    SimeonScoringMode scoringMode_{SimeonScoringMode::SingleVectorCosine};
    std::unordered_set<std::string> loaded_;
    std::chrono::system_clock::time_point loadTime_{};
    std::atomic<std::size_t> requestCount_{0};
};

} // namespace

std::unique_ptr<IModelProvider> makeSimeonModelProvider(std::size_t embeddingDim,
                                                        SimeonScoringMode scoringMode) {
    return std::make_unique<SimeonModelProvider>(embeddingDim ? embeddingDim : 1024, scoringMode);
}

void setSimeonScoringMode(IModelProvider& provider, SimeonScoringMode mode) noexcept {
    if (auto* simeon = dynamic_cast<SimeonModelProvider*>(&provider)) {
        simeon->setScoringMode(mode);
    }
}

void forceLinkSimeonProvider() noexcept {}

namespace {
const ModelProviderFactoryRegistration s_simeon_registration{
    "simeon", [] { return makeSimeonModelProvider(); }};
} // namespace

} // namespace yams::daemon
