#include <spdlog/spdlog.h>
#include <map>
#include <numeric>
#include <random>
#include <yams/ml/provider.h>

namespace yams::ml {

// ============================================================================
// Mock Embedding Provider Implementation
// ============================================================================

/**
 * Mock embedding provider for testing and development
 * Generates deterministic embeddings based on text hash
 */
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
        size_t seed = hasher(text);
        std::mt19937 gen(seed);
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

// ============================================================================
// Daemon Client Embedding Provider
// ============================================================================

/**
 * Embedding provider that delegates to the YAMS daemon
 * This avoids circular dependency by using the daemon client
 */
class DaemonClientEmbeddingProvider : public IEmbeddingProvider {
public:
    DaemonClientEmbeddingProvider() : initialized_(false) {
        spdlog::debug("DaemonClientEmbeddingProvider created");
    }

    ~DaemonClientEmbeddingProvider() override {
        if (initialized_) {
            shutdown();
        }
    }

    Result<void> initialize() override {
        if (initialized_) {
            return Result<void>();
        }

        // TODO: Initialize daemon client connection
        // For now, we'll just mark as unavailable
        spdlog::warn("DaemonClientEmbeddingProvider not yet implemented");
        return Error{ErrorCode::NotImplemented, "Daemon client provider not yet implemented"};
    }

    void shutdown() override {
        if (!initialized_) {
            return;
        }

        // TODO: Close daemon client connection
        initialized_ = false;
    }

    Result<std::vector<float>>
    generateEmbedding([[maybe_unused]] const std::string& text) override {
        if (!initialized_) {
            return Error{ErrorCode::NotInitialized, "Daemon client not initialized"};
        }

        // TODO: Send request to daemon
        return Error{ErrorCode::NotImplemented, "Daemon client provider not yet implemented"};
    }

    Result<std::vector<std::vector<float>>>
    generateBatchEmbeddings([[maybe_unused]] const std::vector<std::string>& texts) override {
        if (!initialized_) {
            return Error{ErrorCode::NotInitialized, "Daemon client not initialized"};
        }

        // TODO: Send batch request to daemon
        return Error{ErrorCode::NotImplemented, "Daemon client provider not yet implemented"};
    }

    bool isAvailable() const override {
        // TODO: Check if daemon is running
        return false;
    }

    std::string getProviderName() const override { return "DaemonClient"; }

    size_t getEmbeddingDimension() const override {
        return 384; // Default, should query from daemon
    }

    size_t getMaxSequenceLength() const override {
        return 512; // Default, should query from daemon
    }

private:
    bool initialized_;
    // TODO: Add daemon client member
};

// ============================================================================
// Provider Factory Implementation
// ============================================================================

static std::map<std::string, EmbeddingProviderFactory> g_embeddingProviders;

void registerEmbeddingProvider(const std::string& name, EmbeddingProviderFactory factory) {
    g_embeddingProviders[name] = factory;
}

std::vector<std::string> getRegisteredEmbeddingProviders() {
    std::vector<std::string> names;
    for (const auto& [name, _] : g_embeddingProviders) {
        names.push_back(name);
    }
    return names;
}

std::unique_ptr<IEmbeddingProvider> createEmbeddingProvider(const std::string& preferredProvider) {
    // Initialize default providers if not already registered
    static bool initialized = false;
    if (!initialized) {
        registerEmbeddingProvider("Mock", []() -> std::unique_ptr<IEmbeddingProvider> {
            return std::make_unique<MockEmbeddingProvider>();
        });

        registerEmbeddingProvider("DaemonClient", []() -> std::unique_ptr<IEmbeddingProvider> {
            return std::make_unique<DaemonClientEmbeddingProvider>();
        });

        initialized = true;
    }

    // If a specific provider is requested, try to create it
    if (!preferredProvider.empty()) {
        auto it = g_embeddingProviders.find(preferredProvider);
        if (it != g_embeddingProviders.end()) {
            return it->second();
        }
        spdlog::warn("Preferred embedding provider '{}' not found", preferredProvider);
    }

    // Try providers in order of preference
    // 1. Try daemon client first (if daemon is running)
    if (auto it = g_embeddingProviders.find("DaemonClient"); it != g_embeddingProviders.end()) {
        auto provider = it->second();
        if (provider && provider->isAvailable()) {
            spdlog::info("Using DaemonClient embedding provider");
            return provider;
        }
    }

    // 2. Try ONNX if available (will be registered when ONNX is enabled)
    if (auto it = g_embeddingProviders.find("ONNX"); it != g_embeddingProviders.end()) {
        auto provider = it->second();
        if (provider && provider->isAvailable()) {
            spdlog::info("Using ONNX embedding provider");
            return provider;
        }
    }

    // 3. Fall back to mock provider
    if (auto it = g_embeddingProviders.find("Mock"); it != g_embeddingProviders.end()) {
        spdlog::info("Using Mock embedding provider");
        return it->second();
    }

    spdlog::error("No embedding providers available");
    return nullptr;
}

} // namespace yams::ml
