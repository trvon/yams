#include <spdlog/spdlog.h>
#include <filesystem>
#include <fstream>
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
        // Prefer lazy loading to avoid blocking startup on ONNX session creation
        config.lazyLoading = true;
        config.modelIdleTimeout = std::chrono::minutes(5);

        // Read embeddings settings from config.toml (preferred_model, model_path, keep_model_hot)
        std::string preferredModel;
        std::string modelsRoot;
        bool keepModelHot = true; // default: keep model in memory
        try {
            namespace fs = std::filesystem;
            fs::path cfgPath;
            if (const char* xdg = std::getenv("XDG_CONFIG_HOME")) {
                cfgPath = fs::path(xdg) / "yams" / "config.toml";
            } else if (const char* home = std::getenv("HOME")) {
                cfgPath = fs::path(home) / ".config" / "yams" / "config.toml";
            }
            if (!cfgPath.empty() && fs::exists(cfgPath)) {
                std::ifstream file(cfgPath);
                std::string line;
                std::string section;
                auto trim = [](std::string& s) {
                    if (s.empty())
                        return;
                    s.erase(0, s.find_first_not_of(" \t"));
                    auto pos = s.find_last_not_of(" \t");
                    if (pos != std::string::npos)
                        s.erase(pos + 1);
                };
                while (std::getline(file, line)) {
                    if (line.empty() || line[0] == '#')
                        continue;
                    if (line[0] == '[') {
                        auto end = line.find(']');
                        section = (end != std::string::npos) ? line.substr(1, end - 1) : "";
                        continue;
                    }
                    auto eq = line.find('=');
                    if (eq == std::string::npos)
                        continue;
                    std::string key = line.substr(0, eq);
                    std::string value = line.substr(eq + 1);
                    trim(key);
                    trim(value);
                    auto hash = value.find('#');
                    if (hash != std::string::npos) {
                        value = value.substr(0, hash);
                        trim(value);
                    }
                    if (value.size() >= 2 && value.front() == '"' && value.back() == '"') {
                        value = value.substr(1, value.size() - 2);
                    }
                    if (section == "embeddings") {
                        if (key == "preferred_model" && preferredModel.empty())
                            preferredModel = value;
                        else if (key == "model_path" && modelsRoot.empty())
                            modelsRoot = value;
                        else if (key == "keep_model_hot") {
                            std::string v = value;
                            for (auto& c : v)
                                c = static_cast<char>(std::tolower(c));
                            keepModelHot = !(v == "false" || v == "0" || v == "no" || v == "off");
                        }
                    }
                }
            }
        } catch (const std::exception& e) {
            spdlog::debug("Config read error (embeddings): {}", e.what());
        }
        preferredModel_ = preferredModel;
        modelsRoot_ = modelsRoot;
        // Expand ~ in modelsRoot if present
        if (!modelsRoot_.empty() && modelsRoot_[0] == '~') {
            if (const char* home = std::getenv("HOME")) {
                modelsRoot_ = std::string(home) + modelsRoot_.substr(1);
            }
        }
        // Map keep_model_hot to lazy loading (inverse)
        config.lazyLoading = !keepModelHot;
        // If keeping hot, allow preloading preferred model; otherwise skip preloads
        if (keepModelHot && !preferredModel_.empty()) {
            if (!modelsRoot_.empty()) {
                config.preloadModels = {modelsRoot_ + "/" + preferredModel_ + "/model.onnx"};
            } else {
                config.preloadModels = {preferredModel_};
            }
        }
        configuredPreloads_ = config.preloadModels;

        // Honor configured models root if provided
        if (!modelsRoot_.empty()) {
            config.modelsRoot = modelsRoot_;
        }
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
    std::string preferredModel_;
    std::string modelsRoot_;
    std::vector<std::string> configuredPreloads_;

    // Get the default model to use for embeddings
    std::string getDefaultModel() const {
        if (!pool_) {
            return "";
        }

        // Try configured preferred model first (full path under modelsRoot, then by name)
        if (!preferredModel_.empty()) {
            if (!modelsRoot_.empty()) {
                std::string full = modelsRoot_ + "/" + preferredModel_ + "/model.onnx";
                if (pool_->isModelLoaded(full)) {
                    return full;
                }
                if (pool_->loadModel(full)) {
                    return full;
                }
            }
            if (pool_->isModelLoaded(preferredModel_)) {
                return preferredModel_;
            }
            if (pool_->loadModel(preferredModel_)) {
                return preferredModel_;
            }
        }

        // Prefer configured preloads first (from DaemonConfig -> ModelPoolConfig)
        if (!configuredPreloads_.empty()) {
            for (const auto& model : configuredPreloads_) {
                if (pool_->isModelLoaded(model)) {
                    return model;
                }
            }
        }

        // Check for commonly used models in order of preference
        const std::vector<std::string> defaultPreferred = {
            "all-MiniLM-L6-v2", "all-mpnet-base-v2", "sentence-transformers/all-MiniLM-L6-v2",
            "sentence-transformers/all-mpnet-base-v2"};

        // Any default already loaded?
        for (const auto& model : defaultPreferred) {
            if (pool_->isModelLoaded(model)) {
                return model;
            }
        }

        // Return first loaded model if any
        auto loadedModels = pool_->getLoadedModels();
        if (!loadedModels.empty()) {
            return loadedModels[0];
        }

        // Try to load configured preferred models first
        if (!configuredPreloads_.empty()) {
            for (const auto& model : configuredPreloads_) {
                if (pool_->loadModel(model)) {
                    return model;
                }
            }
        }

        // Then try to load a default preferred model
        for (const auto& model : defaultPreferred) {
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
