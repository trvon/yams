#pragma once

#include <yams/daemon/resource/resource_pool.h>
#include <yams/vector/embedding_generator.h>

#include <atomic>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

// Forward declaration for ONNX Runtime
namespace Ort {
class Env;
class Session;
class SessionOptions;
} // namespace Ort

namespace yams::daemon {

// ============================================================================
// ONNX Model Information
// ============================================================================

struct OnnxModelInfo {
    std::string name;
    std::string path;
    size_t embeddingDim;
    size_t maxSequenceLength;
    size_t memoryUsageBytes;
    std::chrono::system_clock::time_point loadTime;
    std::atomic<size_t> requestCount{0};
    std::atomic<size_t> errorCount{0};
};

// ============================================================================
// ONNX Model Session Wrapper
// ============================================================================

class OnnxModelSession {
public:
    OnnxModelSession(const std::string& modelPath, const std::string& modelName,
                     const vector::EmbeddingConfig& config);
    ~OnnxModelSession();

    // Generate embedding for single text
    Result<std::vector<float>> generateEmbedding(const std::string& text);

    // Generate embeddings for batch of texts
    Result<std::vector<std::vector<float>>>
    generateBatchEmbeddings(const std::vector<std::string>& texts);

    // Model information
    const OnnxModelInfo& getInfo() const { return info_; }
    std::string getName() const { return info_.name; }
    size_t getEmbeddingDim() const { return info_.embeddingDim; }
    size_t getMaxSequenceLength() const { return info_.maxSequenceLength; }

    // Statistics
    size_t getRequestCount() const { return info_.requestCount.load(); }
    size_t getErrorCount() const { return info_.errorCount.load(); }

    // Validation
    bool isValid() const;

private:
    OnnxModelInfo info_;
    vector::EmbeddingConfig config_;

    // ONNX Runtime components (using unique_ptr for PIMPL)
    class Impl;
    std::unique_ptr<Impl> pImpl;
};

// ============================================================================
// Model Pool Configuration
// ============================================================================

struct ModelPoolConfig {
    // Pool sizing
    size_t maxLoadedModels = 3; // Maximum models in memory
    size_t hotPoolSize = 1;     // Models to keep always loaded

    // Memory management
    size_t maxMemoryGB = 4;             // Maximum memory for all models
    std::string evictionPolicy = "lru"; // lru, lfu, fifo

    // Timeouts
    std::chrono::seconds modelIdleTimeout{300}; // Unload after idle
    std::chrono::seconds modelLoadTimeout{30};  // Max time to load model

    // Preloading
    std::vector<std::string> preloadModels; // Models to load on startup
    bool lazyLoading = false;               // Load models only when needed

    // Model discovery
    std::string modelsRoot; // Optional root directory to search first (e.g., ~/.yams/models)

    // Performance
    bool enableGPU = false;
    int numThreads = 4;
};

// ============================================================================
// ONNX Model Pool Manager
// ============================================================================

class OnnxModelPool {
public:
    using ModelSessionPtr = std::shared_ptr<OnnxModelSession>;
    using ModelHandle = ResourcePool<OnnxModelSession>::Handle;

    explicit OnnxModelPool(const ModelPoolConfig& config = {});
    ~OnnxModelPool();

    // Initialize pool and preload models
    Result<void> initialize();

    // Shutdown pool and release all models
    void shutdown();

    // ========================================================================
    // Model Access
    // ========================================================================

    // Acquire a model session (loads if not in memory)
    Result<ModelHandle>
    acquireModel(const std::string& modelName,
                 std::chrono::milliseconds timeout = std::chrono::milliseconds(0));

    // Check if a model is loaded
    bool isModelLoaded(const std::string& modelName) const;

    // Get list of loaded models
    std::vector<std::string> getLoadedModels() const;

    // ========================================================================
    // Model Management
    // ========================================================================

    // Load a model into the pool
    Result<void> loadModel(const std::string& modelName);

    // Unload a model from the pool
    Result<void> unloadModel(const std::string& modelName);

    // Preload models specified in config
    Result<void> preloadModels();

    // Evict least recently used models
    void evictLRU(size_t numToEvict = 1);

    // ========================================================================
    // Statistics and Monitoring
    // ========================================================================

    struct PoolStats {
        size_t loadedModels;
        size_t totalMemoryBytes;
        size_t totalRequests;
        size_t cacheHits;
        size_t cacheMisses;
        double hitRate;
        std::unordered_map<std::string, OnnxModelInfo> modelStats;
    };

    PoolStats getStats() const;

    // Get memory usage in bytes
    size_t getMemoryUsage() const;

    // Clean up expired/idle models
    void performMaintenance();

private:
    // Model registry entry
    struct ModelEntry {
        std::string name;
        std::string path;
        std::shared_ptr<ResourcePool<OnnxModelSession>> pool;
        std::chrono::steady_clock::time_point lastAccess;
        size_t accessCount = 0;
        bool isHot = false; // Part of hot pool (always loaded)
    };

    // Find model path from name
    std::string resolveModelPath(const std::string& modelName) const;

    // Create a new model session
    Result<ModelSessionPtr> createModelSession(const std::string& modelName);

    // Update access statistics
    void updateAccessStats(const std::string& modelName);

    // Check memory constraints
    bool canLoadModel(size_t estimatedSize) const;

    ModelPoolConfig config_;

    // Model registry and pools
    mutable std::mutex mutex_;
    std::unordered_map<std::string, ModelEntry> models_;

    // Statistics
    std::atomic<size_t> totalRequests_{0};
    std::atomic<size_t> cacheHits_{0};
    std::atomic<size_t> cacheMisses_{0};

    // ONNX Runtime environment (shared across all models)
    std::shared_ptr<Ort::Env> ortEnv_;
    std::shared_ptr<Ort::SessionOptions> sessionOptions_;

    bool initialized_ = false;
};

} // namespace yams::daemon