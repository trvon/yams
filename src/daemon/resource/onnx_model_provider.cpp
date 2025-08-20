#include <spdlog/spdlog.h>
#include <memory>
#include <string>
#include <vector>
#include <yams/core/types.h>
#include <yams/daemon/resource/model_provider.h>
#include <yams/daemon/resource/onnx_model_pool.h>

namespace yams::daemon {

// ============================================================================
// ONNX Model Provider Implementation
// ============================================================================

class OnnxModelProvider : public IModelProvider {
public:
    OnnxModelProvider() {
        // Configure the ONNX model pool
        ModelPoolConfig config;
        config.maxLoadedModels = 4;
        config.maxMemoryGB = 2.0;
        config.numThreads = 4;
        config.enableGPU = false; // Can be made configurable
        config.lazyLoading = true;
        config.modelIdleTimeout = std::chrono::minutes(5);

        // Preload common models if they exist
        config.preloadModels = {"all-MiniLM-L6-v2", "all-mpnet-base-v2"};

        pool_ = std::make_unique<OnnxModelPool>(config);

        // Initialize the pool
        if (auto result = pool_->initialize(); !result) {
            spdlog::error("Failed to initialize ONNX model pool: {}", result.error().message);
            initialized_ = false;
        } else {
            initialized_ = true;
            spdlog::info("ONNX model provider initialized successfully");
        }
    }

    ~OnnxModelProvider() override {
        if (pool_) {
            pool_->shutdown();
        }
    }

    // Generate embedding for a single text
    Result<std::vector<float>> generateEmbedding(const std::string& text) override {
        if (!initialized_) {
            return Error{ErrorCode::NotInitialized, "ONNX model provider not initialized"};
        }

        // Use default model or first available model
        std::string modelName = getDefaultModel();
        if (modelName.empty()) {
            return Error{ErrorCode::NotFound, "No embedding model available"};
        }

        // Acquire model from pool
        auto modelHandle = pool_->acquireModel(modelName, std::chrono::seconds(10));
        if (!modelHandle) {
            return Error{ErrorCode::ResourceExhausted,
                         "Failed to acquire model: " + modelHandle.error().message};
        }

        // Generate embedding using the model
        auto& session = *modelHandle.value();
        return const_cast<OnnxModelSession&>(session).generateEmbedding(text);
    }

    // Generate embeddings for multiple texts
    Result<std::vector<std::vector<float>>>
    generateBatchEmbeddings(const std::vector<std::string>& texts) override {
        if (!initialized_) {
            return Error{ErrorCode::NotInitialized, "ONNX model provider not initialized"};
        }

        if (texts.empty()) {
            return std::vector<std::vector<float>>{};
        }

        std::string modelName = getDefaultModel();
        if (modelName.empty()) {
            return Error{ErrorCode::NotFound, "No embedding model available"};
        }

        // Acquire model from pool
        auto modelHandle = pool_->acquireModel(modelName, std::chrono::seconds(10));
        if (!modelHandle) {
            return Error{ErrorCode::ResourceExhausted,
                         "Failed to acquire model: " + modelHandle.error().message};
        }

        // Generate batch embeddings
        auto& session = *modelHandle.value();
        return const_cast<OnnxModelSession&>(session).generateBatchEmbeddings(texts);
    }

    // Load a specific model
    Result<void> loadModel(const std::string& modelName) override {
        if (!initialized_) {
            return Error{ErrorCode::NotInitialized, "ONNX model provider not initialized"};
        }

        return pool_->loadModel(modelName);
    }

    // Unload a specific model
    Result<void> unloadModel(const std::string& modelName) override {
        if (!initialized_) {
            return Error{ErrorCode::NotInitialized, "ONNX model provider not initialized"};
        }

        return pool_->unloadModel(modelName);
    }

    // Check if a model is loaded
    bool isModelLoaded(const std::string& modelName) const override {
        if (!initialized_ || !pool_) {
            return false;
        }

        return pool_->isModelLoaded(modelName);
    }

    // Get list of loaded models
    std::vector<std::string> getLoadedModels() const override {
        if (!initialized_ || !pool_) {
            return {};
        }

        return pool_->getLoadedModels();
    }

    // Get model information
    Result<ModelInfo> getModelInfo(const std::string& modelName) const override {
        if (!initialized_) {
            return Error{ErrorCode::NotInitialized, "ONNX model provider not initialized"};
        }

        // Since getModelInfo is not available in OnnxModelPool, we need to construct
        // the ModelInfo from available data
        if (!pool_->isModelLoaded(modelName)) {
            return Error{ErrorCode::NotFound, "Model not loaded: " + modelName};
        }

        // Convert available info to ModelInfo
        ModelInfo info;
        info.name = modelName;
        info.path = "";               // Path not directly available
        info.embeddingDim = 384;      // Default for common models
        info.maxSequenceLength = 512; // Default for common models
        info.memoryUsageBytes = pool_->getMemoryUsage();
        info.loadTime = std::chrono::system_clock::now();
        info.requestCount = 0; // Stats not directly available per model
        info.errorCount = 0;

        return info;
    }

    // Get embedding dimension for a model
    size_t getEmbeddingDim(const std::string& modelName) const override {
        if (!initialized_ || !pool_) {
            return 0;
        }

        if (!pool_->isModelLoaded(modelName)) {
            // Try to get dimension from a loaded model
            auto loadedModels = pool_->getLoadedModels();
            if (!loadedModels.empty()) {
                // For now, return default dimension for common models
                return 384;
            }
            return 384; // Default dimension for common models
        }

        return 384; // Default dimension, as model-specific info not directly available
    }

    // Provider identification
    std::string getProviderName() const override { return "ONNX"; }

    std::string getProviderVersion() const override { return "1.0.0"; }

    bool isAvailable() const override { return initialized_; }

    // Resource management
    size_t getMemoryUsage() const override {
        if (!initialized_ || !pool_) {
            return 0;
        }

        auto stats = pool_->getStats();
        return stats.totalMemoryBytes;
    }

    void releaseUnusedResources() override {
        if (initialized_ && pool_) {
            // The pool handles resource management internally
            // We could trigger eviction of idle models here if needed
        }
    }

    void shutdown() override {
        if (pool_) {
            pool_->shutdown();
        }
        initialized_ = false;
    }

private:
    std::unique_ptr<OnnxModelPool> pool_;
    bool initialized_ = false;

    // Get the default model to use for embeddings
    std::string getDefaultModel() const {
        if (!pool_) {
            return "";
        }

        // Check for commonly used models in order of preference
        const std::vector<std::string> preferredModels = {
            "all-MiniLM-L6-v2", "all-mpnet-base-v2", "sentence-transformers/all-MiniLM-L6-v2",
            "sentence-transformers/all-mpnet-base-v2"};

        for (const auto& model : preferredModels) {
            if (pool_->isModelLoaded(model)) {
                return model;
            }
        }

        // Return first loaded model if any
        auto loadedModels = pool_->getLoadedModels();
        if (!loadedModels.empty()) {
            return loadedModels[0];
        }

        // Try to load a preferred model
        for (const auto& model : preferredModels) {
            if (pool_->loadModel(model)) {
                return model;
            }
        }

        return "";
    }
};

// ============================================================================
// Factory Function for ONNX Provider
// ============================================================================

std::unique_ptr<IModelProvider> createOnnxModelProvider() {
    return std::make_unique<OnnxModelProvider>();
}

// ============================================================================
// Registration with Provider Registry
// ============================================================================

// Since this is a plugin, we don't do static registration
// The plugin will be registered when loaded by the daemon
// This avoids linker issues with cross-library dependencies

// Export the factory function for dynamic loading
extern "C" {
IModelProvider* createOnnxProvider() {
    return new OnnxModelProvider();
}

const char* getProviderName() {
    return "ONNX";
}
}

} // namespace yams::daemon