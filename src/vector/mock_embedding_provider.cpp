#include <spdlog/spdlog.h>
#include <cmath>
#include <random>
#include <yams/ml/provider.h>

namespace yams::ml {

class MockEmbeddingProvider : public IEmbeddingProvider {
public:
    explicit MockEmbeddingProvider(size_t dimension = 384)
        : dimension_(dimension), initialized_(false) {
        spdlog::debug("MockEmbeddingProvider created with dimension {}", dimension);
    }

    ~MockEmbeddingProvider() override {
        if (initialized_) {
            shutdown();
        }
    }

    Result<void> initialize() override {
        if (initialized_) {
            return Result<void>();
        }

        spdlog::info("Initializing MockEmbeddingProvider");
        initialized_ = true;
        return Result<void>();
    }

    void shutdown() override {
        if (!initialized_) {
            return;
        }

        spdlog::info("Shutting down MockEmbeddingProvider");
        initialized_ = false;
    }

    Result<std::vector<float>>
    generateEmbedding([[maybe_unused]] const std::string& text) override {
        if (!initialized_) {
            return Error{ErrorCode::NotInitialized, "Mock provider not initialized"};
        }

        // Generate deterministic embedding based on text hash
        std::hash<std::string> hasher;
        const std::size_t seed = hasher(text);
        std::mt19937 gen(static_cast<std::mt19937::result_type>(seed));
        std::normal_distribution<float> dist(0.0f, 1.0f);

        std::vector<float> embedding(dimension_);
        for (size_t i = 0; i < dimension_; ++i) {
            embedding[i] = dist(gen);
        }

        // Normalize to unit length
        float norm = 0.0f;
        for (float val : embedding) {
            norm += val * val;
        }
        norm = std::sqrt(norm);

        if (norm > 0) {
            for (float& val : embedding) {
                val /= norm;
            }
        }

        return embedding;
    }

    Result<std::vector<std::vector<float>>>
    generateBatchEmbeddings([[maybe_unused]] const std::vector<std::string>& texts) override {
        if (!initialized_) {
            return Error{ErrorCode::NotInitialized, "Mock provider not initialized"};
        }

        std::vector<std::vector<float>> embeddings;
        embeddings.reserve(texts.size());

        for (const auto& text : texts) {
            auto result = generateEmbedding(text);
            if (!result) {
                return result.error();
            }
            embeddings.push_back(std::move(result.value()));
        }

        return embeddings;
    }

    bool isAvailable() const override {
        return true; // Mock is always available
    }

    std::string getProviderName() const override { return "Mock"; }

    size_t getEmbeddingDimension() const override { return dimension_; }

    size_t getMaxSequenceLength() const override {
        return 512; // Mock limit
    }

private:
    size_t dimension_;
    bool initialized_;
};

std::unique_ptr<IEmbeddingProvider> createMockEmbeddingProvider() {
    return std::make_unique<MockEmbeddingProvider>();
}

} // namespace yams::ml
