#include <spdlog/spdlog.h>
#include <mutex>
#include <unordered_map>
#include <yams/daemon/resource/model_provider.h>

// Forward declaration to avoid include issues
namespace yams::daemon {
struct ModelPoolConfig {
    size_t maxLoadedModels = 3;
    size_t hotPoolSize = 1;
    size_t maxMemoryGB = 4;
    std::string evictionPolicy = "lru";
    std::chrono::seconds modelIdleTimeout{300};
    std::chrono::seconds modelLoadTimeout{30};
    std::vector<std::string> preloadModels;
    bool lazyLoading = false;
    bool enableGPU = false;
    int numThreads = 4;
};
} // namespace yams::daemon

namespace yams::daemon {

// ============================================================================
// Mock Model Provider for Testing
// ============================================================================

class MockModelProvider : public IModelProvider {
public:
    MockModelProvider() {
        spdlog::debug("[MockModelProvider] Created mock model provider for testing");
    }

    ~MockModelProvider() override {
        spdlog::debug("[MockModelProvider] Destroying mock model provider");
    }

    // ========================================================================
    // Model Operations
    // ========================================================================

    Result<std::vector<float>> generateEmbedding(const std::string& text) override {
        if (!isInitialized_) {
            return Error{ErrorCode::NotInitialized, "Mock provider not initialized"};
        }

        // Generate deterministic mock embedding based on text hash
        std::hash<std::string> hasher;
        size_t hash = hasher(text);

        std::vector<float> embedding(embeddingDim_);
        for (size_t i = 0; i < embeddingDim_; ++i) {
            // Generate pseudo-random but deterministic values
            embedding[i] = static_cast<float>((hash ^ (i * 31)) % 1000) / 1000.0f;
        }

        requestCount_++;
        return embedding;
    }

    Result<std::vector<std::vector<float>>>
    generateBatchEmbeddings(const std::vector<std::string>& texts) override {
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

    // ========================================================================
    // Model Management
    // ========================================================================

    Result<void> loadModel(const std::string& modelName) override {
        std::lock_guard<std::mutex> lock(mutex_);

        spdlog::debug("[MockModelProvider] Loading mock model: {}", modelName);

        // Simulate model loading
        loadedModels_[modelName] =
            ModelInfo{.name = modelName,
                      .path = "/mock/path/" + modelName,
                      .embeddingDim = embeddingDim_,
                      .maxSequenceLength = 512,
                      .memoryUsageBytes = 100 * 1024 * 1024, // 100MB mock size
                      .loadTime = std::chrono::system_clock::now(),
                      .requestCount = 0,
                      .errorCount = 0};

        isInitialized_ = true;
        return Result<void>();
    }

    Result<void> unloadModel(const std::string& modelName) override {
        std::lock_guard<std::mutex> lock(mutex_);

        auto it = loadedModels_.find(modelName);
        if (it == loadedModels_.end()) {
            return Error{ErrorCode::NotFound, "Model not loaded: " + modelName};
        }

        spdlog::debug("[MockModelProvider] Unloading mock model: {}", modelName);
        loadedModels_.erase(it);

        if (loadedModels_.empty()) {
            isInitialized_ = false;
        }

        return Result<void>();
    }

    bool isModelLoaded(const std::string& modelName) const override {
        std::lock_guard<std::mutex> lock(mutex_);
        return loadedModels_.find(modelName) != loadedModels_.end();
    }

    std::vector<std::string> getLoadedModels() const override {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<std::string> models;
        models.reserve(loadedModels_.size());

        for (const auto& [name, info] : loadedModels_) {
            models.push_back(name);
        }

        return models;
    }

    // ========================================================================
    // Model Information
    // ========================================================================

    Result<ModelInfo> getModelInfo(const std::string& modelName) const override {
        std::lock_guard<std::mutex> lock(mutex_);

        auto it = loadedModels_.find(modelName);
        if (it == loadedModels_.end()) {
            return Error{ErrorCode::NotFound, "Model not loaded: " + modelName};
        }

        return it->second;
    }

    size_t getEmbeddingDim(const std::string& modelName) const override {
        std::lock_guard<std::mutex> lock(mutex_);

        auto it = loadedModels_.find(modelName);
        if (it == loadedModels_.end()) {
            return 0;
        }

        return it->second.embeddingDim;
    }

    // ========================================================================
    // Provider Information
    // ========================================================================

    std::string getProviderName() const override { return "MockProvider"; }

    std::string getProviderVersion() const override { return "1.0.0-test"; }

    bool isAvailable() const override {
        return true; // Mock provider is always available
    }

    // ========================================================================
    // Resource Management
    // ========================================================================

    size_t getMemoryUsage() const override {
        std::lock_guard<std::mutex> lock(mutex_);

        size_t totalMemory = 0;
        for (const auto& [name, info] : loadedModels_) {
            totalMemory += info.memoryUsageBytes;
        }

        return totalMemory;
    }

    void releaseUnusedResources() override {
        // No-op for mock provider
        spdlog::debug("[MockModelProvider] Releasing unused resources (no-op)");
    }

    void shutdown() override {
        std::lock_guard<std::mutex> lock(mutex_);

        spdlog::debug("[MockModelProvider] Shutting down mock provider");
        loadedModels_.clear();
        isInitialized_ = false;
    }

private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, ModelInfo> loadedModels_;
    bool isInitialized_ = false;
    size_t embeddingDim_ = 384; // Default embedding dimension
    mutable size_t requestCount_ = 0;
};

// ============================================================================
// Null Model Provider (No-op)
// ============================================================================

class NullModelProvider : public IModelProvider {
public:
    Result<std::vector<float>> generateEmbedding(const std::string&) override {
        return Error{ErrorCode::NotImplemented, "No model provider available"};
    }

    Result<std::vector<std::vector<float>>>
    generateBatchEmbeddings(const std::vector<std::string>&) override {
        return Error{ErrorCode::NotImplemented, "No model provider available"};
    }

    Result<void> loadModel(const std::string&) override {
        return Error{ErrorCode::NotImplemented, "No model provider available"};
    }

    Result<void> unloadModel(const std::string&) override {
        return Error{ErrorCode::NotImplemented, "No model provider available"};
    }

    bool isModelLoaded(const std::string&) const override { return false; }
    std::vector<std::string> getLoadedModels() const override { return {}; }

    Result<ModelInfo> getModelInfo(const std::string&) const override {
        return Error{ErrorCode::NotImplemented, "No model provider available"};
    }

    size_t getEmbeddingDim(const std::string&) const override { return 0; }
    std::string getProviderName() const override { return "NullProvider"; }
    std::string getProviderVersion() const override { return "1.0.0"; }
    bool isAvailable() const override { return false; }
    size_t getMemoryUsage() const override { return 0; }
    void releaseUnusedResources() override {}
    void shutdown() override {}
};

// ============================================================================
// Factory Implementation
// ============================================================================

static std::unordered_map<std::string, ModelProviderFactory>& getProviderRegistry() {
    static std::unordered_map<std::string, ModelProviderFactory> registry;
    return registry;
}

void registerModelProvider(const std::string& name, ModelProviderFactory factory) {
    getProviderRegistry()[name] = factory;
}

std::vector<std::string> getRegisteredProviders() {
    std::vector<std::string> providers;
    for (const auto& [name, factory] : getProviderRegistry()) {
        providers.push_back(name);
    }
    return providers;
}

std::unique_ptr<IModelProvider> createModelProvider(const std::string& preferredProvider) {
    // Default configuration
    ModelPoolConfig defaultConfig;
    return createModelProvider(defaultConfig, preferredProvider);
}

std::unique_ptr<IModelProvider> createModelProvider(const ModelPoolConfig& config,
                                                    const std::string& preferredProvider) {
    auto& registry = getProviderRegistry();

    // Check for test/mock mode
    if (std::getenv("YAMS_USE_MOCK_PROVIDER") != nullptr) {
        spdlog::info("Using mock model provider (YAMS_USE_MOCK_PROVIDER set)");
        return std::make_unique<MockModelProvider>();
    }

    // Check if ONNX is disabled
    if (std::getenv("YAMS_DISABLE_ONNX") != nullptr) {
        spdlog::info("ONNX disabled (YAMS_DISABLE_ONNX set), using null provider");
        return std::make_unique<NullModelProvider>();
    }

    // Try preferred provider if specified
    if (!preferredProvider.empty()) {
        auto it = registry.find(preferredProvider);
        if (it != registry.end() && it->second) {
            if (auto provider = it->second()) {
                spdlog::info("Created {} model provider", preferredProvider);
                return provider;
            }
        }
    }

    // Try ONNX provider (will be registered by plugin if available)
    auto it = registry.find("ONNX");
    if (it != registry.end() && it->second) {
        if (auto provider = it->second()) {
            spdlog::info("Created ONNX model provider");
            return provider;
        }
    }

    // Fall back to mock provider in debug/test builds
#ifdef DEBUG
    spdlog::info("No model provider available, using mock provider (DEBUG build)");
    return std::make_unique<MockModelProvider>();
#else
    // Production: return null provider
    spdlog::warn("No model provider available, using null provider");
    return std::make_unique<NullModelProvider>();
#endif
}

} // namespace yams::daemon