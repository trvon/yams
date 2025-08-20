#include <yams/daemon/resource/onnx_model_pool.h>

#include <onnxruntime_cxx_api.h>
#include <spdlog/spdlog.h>

// Check if we should skip model loading for tests
#ifdef GTEST_API_
#include <yams/common/test_utils.h>
#endif

#include <algorithm>
#include <cstdlib>
#include <filesystem>

namespace yams::daemon {

namespace fs = std::filesystem;

// ============================================================================
// OnnxModelSession Implementation
// ============================================================================

class OnnxModelSession::Impl {
public:
    Impl(const std::string& modelPath, const std::string& modelName,
         const vector::EmbeddingConfig& config)
        : modelPath_(modelPath), modelName_(modelName), config_(config) {
#ifdef GTEST_API_
        // Check if we should skip model loading in test mode
        if (yams::test::shouldSkipModelLoading()) {
            test_mode_ = true;
            spdlog::debug("[ONNX] Test mode enabled - skipping actual model loading");
            return;
        }
#endif

        // Initialize ONNX Runtime environment
        env_ = std::make_unique<Ort::Env>(ORT_LOGGING_LEVEL_WARNING, modelName.c_str());

        // Configure session options
        sessionOptions_ = std::make_unique<Ort::SessionOptions>();
        sessionOptions_->SetIntraOpNumThreads(config.num_threads > 0 ? config.num_threads : 4);
        sessionOptions_->SetGraphOptimizationLevel(GraphOptimizationLevel::ORT_ENABLE_ALL);

        if (config.enable_gpu) {
            // TODO: Add GPU provider when available
            spdlog::warn("GPU support requested but not yet implemented");
        }
    }

    Result<void> loadModel() {
        if (test_mode_) {
            // In test mode, pretend loading succeeded
            isLoaded_ = true;
            return Result<void>();
        }

        try {
            spdlog::info("[ONNX] Creating Ort::Session for model '{}' at {}", modelName_.c_str(),
                         modelPath_.c_str());
            session_ = std::make_unique<Ort::Session>(*env_, modelPath_.c_str(), *sessionOptions_);

            // Get input/output information
            size_t numInputs = session_->GetInputCount();
            size_t numOutputs = session_->GetOutputCount();

            if (numInputs < 2) {
                return Error{ErrorCode::InvalidData,
                             "Model must have at least 2 inputs (input_ids, attention_mask)"};
            }

            // Get input/output names
            Ort::AllocatorWithDefaultOptions allocator;
            for (size_t i = 0; i < numInputs; ++i) {
                auto inputName = session_->GetInputNameAllocated(i, allocator);
                inputNames_.push_back(inputName.get());
            }

            for (size_t i = 0; i < numOutputs; ++i) {
                auto outputName = session_->GetOutputNameAllocated(i, allocator);
                outputNames_.push_back(outputName.get());
            }

            // Get output shape to determine embedding dimension
            if (numOutputs > 0) {
                auto outputInfo = session_->GetOutputTypeInfo(0);
                auto tensorInfo = outputInfo.GetTensorTypeAndShapeInfo();
                auto shape = tensorInfo.GetShape();
                if (shape.size() >= 2 && shape.back() > 0) {
                    embeddingDim_ = static_cast<size_t>(shape.back());
                }
            }

            isLoaded_ = true;
            spdlog::info("[ONNX] Session ready for '{}' (inputs={}, outputs={}, dim={})",
                         modelName_.c_str(), numInputs, numOutputs, embeddingDim_);
            return Result<void>();

        } catch (const Ort::Exception& e) {
            spdlog::warn("[ONNX] Failed to load '{}': {}", modelName_.c_str(), e.what());
            return Error{ErrorCode::InternalError,
                         std::string("Failed to load ONNX model: ") + e.what()};
        }
    }

    Result<std::vector<float>> generateEmbedding(const std::string& text) {
        if (test_mode_) {
            // Return mock embedding in test mode
            std::vector<float> embedding(embeddingDim_, 0.1f);
            return embedding;
        }

        if (!isLoaded_) {
            if (auto result = loadModel(); !result) {
                return result.error();
            }
        }

        try {
            // TODO: Implement tokenization and actual embedding generation
            // For now, return a dummy embedding
            std::vector<float> embedding(embeddingDim_, 0.0f);

            // Simple hash-based dummy embedding for testing
            std::hash<std::string> hasher;
            size_t hash = hasher(text);
            for (size_t i = 0; i < embeddingDim_; ++i) {
                embedding[i] = static_cast<float>((hash >> i) & 1) * 0.1f;
            }

            return embedding;

        } catch (const std::exception& e) {
            return Error{ErrorCode::InternalError,
                         std::string("Failed to generate embedding: ") + e.what()};
        }
    }

    bool isValid() const { return isLoaded_ && session_ != nullptr; }

    size_t getEmbeddingDim() const { return embeddingDim_; }
    size_t getMaxSequenceLength() const { return maxSequenceLength_; }

private:
    std::string modelPath_;
    std::string modelName_;
    vector::EmbeddingConfig config_;

    std::unique_ptr<Ort::Env> env_;
    std::unique_ptr<Ort::SessionOptions> sessionOptions_;
    std::unique_ptr<Ort::Session> session_;

    std::vector<std::string> inputNames_;
    std::vector<std::string> outputNames_;

    size_t embeddingDim_ = 384;
    size_t maxSequenceLength_ = 512;
    bool isLoaded_ = false;
    bool test_mode_ = false;
};

// ============================================================================
// OnnxModelSession Public Interface
// ============================================================================

OnnxModelSession::OnnxModelSession(const std::string& modelPath, const std::string& modelName,
                                   const vector::EmbeddingConfig& config)
    : pImpl(std::make_unique<Impl>(modelPath, modelName, config)) {
    info_.name = modelName;
    info_.path = modelPath;
    info_.loadTime = std::chrono::system_clock::now();

    // Get file size for memory estimation
    if (fs::exists(modelPath)) {
        info_.memoryUsageBytes = fs::file_size(modelPath);
    }

    // Load the model
    if (auto result = pImpl->loadModel(); result) {
        info_.embeddingDim = pImpl->getEmbeddingDim();
        info_.maxSequenceLength = pImpl->getMaxSequenceLength();
    }
}

OnnxModelSession::~OnnxModelSession() = default;

Result<std::vector<float>> OnnxModelSession::generateEmbedding(const std::string& text) {
    info_.requestCount++;

    auto result = pImpl->generateEmbedding(text);
    if (!result) {
        info_.errorCount++;
    }

    return result;
}

Result<std::vector<std::vector<float>>>
OnnxModelSession::generateBatchEmbeddings(const std::vector<std::string>& texts) {
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

bool OnnxModelSession::isValid() const {
    return pImpl->isValid();
}

// ============================================================================
// OnnxModelPool Implementation
// ============================================================================

OnnxModelPool::OnnxModelPool(const ModelPoolConfig& config) : config_(config) {
    // Initialize ONNX Runtime environment (shared across all models)
    ortEnv_ = std::make_shared<Ort::Env>(ORT_LOGGING_LEVEL_WARNING, "YamsDaemon");

    sessionOptions_ = std::make_shared<Ort::SessionOptions>();
    sessionOptions_->SetIntraOpNumThreads(config.numThreads);
    sessionOptions_->SetGraphOptimizationLevel(GraphOptimizationLevel::ORT_ENABLE_ALL);

    if (config.enableGPU) {
        // TODO: Add GPU provider configuration
        spdlog::info("GPU support requested but not yet implemented");
    }
}

OnnxModelPool::~OnnxModelPool() {
    shutdown();
}

Result<void> OnnxModelPool::initialize() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (initialized_) {
        return Result<void>();
    }

    spdlog::info("Initializing ONNX model pool with max {} models", config_.maxLoadedModels);

    // Preload configured models
    if (!config_.lazyLoading && !config_.preloadModels.empty()) {
        if (auto result = preloadModels(); !result) {
            spdlog::warn("Failed to preload some models: {}", result.error().message);
        }
    }

    initialized_ = true;
    return Result<void>();
}

void OnnxModelPool::shutdown() {
    std::lock_guard<std::mutex> lock(mutex_);

    spdlog::info("Shutting down ONNX model pool");

    // Shutdown all model pools
    for (auto& [name, entry] : models_) {
        if (entry.pool) {
            entry.pool->shutdown();
        }
    }

    models_.clear();
    initialized_ = false;
}

Result<OnnxModelPool::ModelHandle> OnnxModelPool::acquireModel(const std::string& modelName,
                                                               std::chrono::milliseconds timeout) {
    if (!initialized_) {
        if (auto result = initialize(); !result) {
            return result.error();
        }
    }

    totalRequests_++;

    // Check if model is already loaded
    {
        std::lock_guard<std::mutex> lock(mutex_);

        auto it = models_.find(modelName);
        if (it != models_.end() && it->second.pool) {
            // Model is loaded, acquire from pool
            updateAccessStats(modelName);
            cacheHits_++;

            return it->second.pool->acquire(timeout);
        }
    }

    cacheMisses_++;

    // Model not loaded, need to load it
    if (auto result = loadModel(modelName); !result) {
        return result.error();
    }

    // Try again after loading
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = models_.find(modelName);
    if (it != models_.end() && it->second.pool) {
        updateAccessStats(modelName);
        return it->second.pool->acquire(timeout);
    }

    return Error{ErrorCode::NotFound, "Failed to load model: " + modelName};
}

bool OnnxModelPool::isModelLoaded(const std::string& modelName) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = models_.find(modelName);
    return it != models_.end() && it->second.pool != nullptr;
}

std::vector<std::string> OnnxModelPool::getLoadedModels() const {
    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<std::string> loaded;
    for (const auto& [name, entry] : models_) {
        if (entry.pool) {
            loaded.push_back(name);
        }
    }

    return loaded;
}

Result<void> OnnxModelPool::loadModel(const std::string& modelName) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Check if already loaded
    auto it = models_.find(modelName);
    if (it != models_.end() && it->second.pool) {
        return Result<void>(); // Already loaded
    }

    // Check if we need to evict models
    size_t loadedCount = 0;
    for (const auto& [name, entry] : models_) {
        if (entry.pool)
            loadedCount++;
    }

    if (loadedCount >= config_.maxLoadedModels) {
        // Need to evict
        evictLRU(1);
    }

    // Resolve model path
    std::string modelPath = resolveModelPath(modelName);
    if (!fs::exists(modelPath) || !fs::is_regular_file(modelPath)) {
        // Use debug level for expected failures (e.g., in tests)
        // to avoid log spam when models are intentionally missing
        spdlog::debug("Model file not found: {} (searched for: {})", modelName, modelPath);
        return Error{
            ErrorCode::NotFound,
            "Model file not found: " + modelPath +
                ". Please download the model to ~/.yams/models/ or /usr/local/share/yams/models/"};
    }

    // Check memory constraints
    size_t modelSize = fs::file_size(modelPath);
    if (!canLoadModel(modelSize)) {
        return Error{ErrorCode::ResourceExhausted, "Insufficient memory to load model"};
    }

    // Create model entry
    ModelEntry& entry = models_[modelName];
    entry.name = modelName;
    entry.path = modelPath;
    entry.lastAccess = std::chrono::steady_clock::now();

    // Configure pool for this model
    PoolConfig<OnnxModelSession> poolConfig;
    poolConfig.minSize = 1;
    poolConfig.maxSize = 3; // Allow up to 3 concurrent users per model
    poolConfig.maxIdle = 2;
    poolConfig.idleTimeout = config_.modelIdleTimeout;
    // Respect lazy-loading: if enabled, don't pre-create sessions here to avoid blocking.
    poolConfig.preCreateResources = !config_.lazyLoading;

    // Create embedding config
    vector::EmbeddingConfig embConfig;
    embConfig.model_path = modelPath;
    embConfig.model_name = modelName;
    embConfig.enable_gpu = config_.enableGPU;
    embConfig.num_threads = config_.numThreads;

    // Create resource pool for this model
    entry.pool = std::make_shared<ResourcePool<OnnxModelSession>>(
        poolConfig,
        [modelPath, modelName, embConfig](const std::string& id) -> Result<ModelSessionPtr> {
            (void)id;
            try {
                auto session = std::make_shared<OnnxModelSession>(modelPath, modelName, embConfig);
                return session;
            } catch (const std::exception& e) {
                return Error{ErrorCode::InternalError,
                             std::string("Failed to create model session: ") + e.what()};
            }
        },
        [](const OnnxModelSession& session) { return session.isValid(); });

    spdlog::info("Loaded ONNX model entry: {} ({}MB) preCreate={}", modelName,
                 modelSize / (1024 * 1024), poolConfig.preCreateResources ? "true" : "false");

    return Result<void>();
}

Result<void> OnnxModelPool::unloadModel(const std::string& modelName) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = models_.find(modelName);
    if (it == models_.end()) {
        return Error{ErrorCode::NotFound, "Model not found: " + modelName};
    }

    if (it->second.isHot) {
        return Error{ErrorCode::InvalidOperation, "Cannot unload hot model: " + modelName};
    }

    if (it->second.pool) {
        it->second.pool->shutdown();
        it->second.pool.reset();
    }

    spdlog::info("Unloaded ONNX model: {}", modelName);

    return Result<void>();
}

Result<void> OnnxModelPool::preloadModels() {
    for (const auto& modelName : config_.preloadModels) {
        spdlog::info("Preloading model: {}", modelName);

        if (auto result = loadModel(modelName); !result) {
            spdlog::warn("Failed to preload model {}: {}", modelName, result.error().message);
        } else {
            // Mark as hot model
            auto it = models_.find(modelName);
            if (it != models_.end()) {
                it->second.isHot = true;
            }
        }
    }

    return Result<void>();
}

void OnnxModelPool::evictLRU(size_t numToEvict) {
    // Find least recently used models that are not hot
    std::vector<std::pair<std::string, std::chrono::steady_clock::time_point>> candidates;

    for (const auto& [name, entry] : models_) {
        if (entry.pool && !entry.isHot) {
            candidates.emplace_back(name, entry.lastAccess);
        }
    }

    // Sort by last access time
    std::sort(candidates.begin(), candidates.end(),
              [](const auto& a, const auto& b) { return a.second < b.second; });

    // Evict the oldest models
    for (size_t i = 0; i < std::min(numToEvict, candidates.size()); ++i) {
        const auto& modelName = candidates[i].first;
        auto it = models_.find(modelName);
        if (it != models_.end() && it->second.pool) {
            spdlog::info("Evicting model due to LRU: {}", modelName);
            it->second.pool->shutdown();
            it->second.pool.reset();
        }
    }
}

OnnxModelPool::PoolStats OnnxModelPool::getStats() const {
    std::lock_guard<std::mutex> lock(mutex_);

    PoolStats stats;
    stats.totalRequests = totalRequests_.load();
    stats.cacheHits = cacheHits_.load();
    stats.cacheMisses = cacheMisses_.load();
    stats.hitRate =
        stats.totalRequests > 0 ? static_cast<double>(stats.cacheHits) / stats.totalRequests : 0.0;

    stats.loadedModels = 0;
    stats.totalMemoryBytes = 0;

    for (const auto& [name, entry] : models_) {
        if (entry.pool) {
            stats.loadedModels++;

            // Get model info (approximate from first session in pool)
            // Note: This is simplified - in production, track actual memory usage
            if (fs::exists(entry.path)) {
                size_t modelSize = fs::file_size(entry.path);
                stats.totalMemoryBytes += modelSize;
            }
        }
    }

    return stats;
}

size_t OnnxModelPool::getMemoryUsage() const {
    return getStats().totalMemoryBytes;
}

void OnnxModelPool::performMaintenance() {
    std::lock_guard<std::mutex> lock(mutex_);

    auto now = std::chrono::steady_clock::now();

    for (auto& [name, entry] : models_) {
        if (entry.pool && !entry.isHot) {
            // Check if model has been idle too long
            auto idleTime = now - entry.lastAccess;
            if (idleTime > config_.modelIdleTimeout) {
                spdlog::info("Unloading idle model: {}", name);
                entry.pool->shutdown();
                entry.pool.reset();
            } else if (entry.pool) {
                // Clean up expired resources in the pool
                entry.pool->evictExpired();
            }
        }
    }
}

std::string OnnxModelPool::resolveModelPath(const std::string& modelName) const {
    // Get home directory
    const char* home = std::getenv("HOME");
    std::string homeDir = home ? home : "";

    // Try different paths - check standard "model.onnx" name first (most common)
    std::vector<std::string> searchPaths = {
        modelName, // If it's already a full path
        homeDir + "/.yams/models/" + modelName +
            "/model.onnx", // Standard Hugging Face naming (check first)
        homeDir + "/.yams/models/" + modelName + "/" + modelName + ".onnx",
        homeDir + "/.yams/models/" + modelName + ".onnx",
        "models/" + modelName + "/model.onnx", // Standard naming in local dir
        "models/" + modelName + "/" + modelName + ".onnx",
        "models/" + modelName + ".onnx",
        "/usr/local/share/yams/models/" + modelName +
            "/model.onnx", // Standard naming in system dir
        "/usr/local/share/yams/models/" + modelName + "/" + modelName + ".onnx",
        "/usr/local/share/yams/models/" + modelName + ".onnx"};

    for (const auto& path : searchPaths) {
        if (!path.empty() && fs::exists(path)) {
            spdlog::debug("Found model at: {}", path);
            return path;
        }
    }

    spdlog::debug("Model {} not found in any search path", modelName);
    // Default to models directory
    return "models/" + modelName + ".onnx";
}

Result<OnnxModelPool::ModelSessionPtr>
OnnxModelPool::createModelSession(const std::string& modelName) {
    std::string modelPath = resolveModelPath(modelName);

    vector::EmbeddingConfig config;
    config.model_path = modelPath;
    config.model_name = modelName;
    config.enable_gpu = config_.enableGPU;
    config.num_threads = config_.numThreads;

    try {
        auto session = std::make_shared<OnnxModelSession>(modelPath, modelName, config);
        return session;
    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError,
                     std::string("Failed to create model session: ") + e.what()};
    }
}

void OnnxModelPool::updateAccessStats(const std::string& modelName) {
    auto it = models_.find(modelName);
    if (it != models_.end()) {
        it->second.lastAccess = std::chrono::steady_clock::now();
        it->second.accessCount++;
    }
}

bool OnnxModelPool::canLoadModel(size_t estimatedSize) const {
    size_t currentUsage = getMemoryUsage();
    size_t maxBytes = config_.maxMemoryGB * 1024 * 1024 * 1024;

    return (currentUsage + estimatedSize) <= maxBytes;
}

} // namespace yams::daemon
