#pragma once

#include <yams/core/types.h>
#include <yams/vector/model_registry.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <list>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>

namespace yams::vector {

// Forward declarations
class ModelLoader;
class EmbeddingGenerator;

/**
 * Cached model entry
 */
struct CachedModel {
    std::string model_id;
    std::shared_ptr<void> model_handle; // Opaque handle to loaded model
    ModelInfo info;
    size_t memory_usage_bytes = 0;
    std::chrono::system_clock::time_point loaded_at;
    std::chrono::system_clock::time_point last_accessed;
    std::atomic<size_t> access_count{0};
    std::atomic<bool> is_warming_up{false};

    // Performance stats
    std::atomic<double> avg_inference_time_ms{0.0};
    std::atomic<size_t> total_inferences{0};

    CachedModel() = default;
    CachedModel(const CachedModel&) = delete;
    CachedModel& operator=(const CachedModel&) = delete;
    CachedModel(CachedModel&&) = delete;
    CachedModel& operator=(CachedModel&&) = delete;
};

/**
 * Cache eviction policies
 */
enum class EvictionPolicy {
    LRU,  // Least Recently Used
    LFU,  // Least Frequently Used
    FIFO, // First In First Out
    TTL,  // Time To Live based
    SIZE  // Size-based (evict largest)
};

/**
 * Model cache configuration
 */
struct ModelCacheConfig {
    size_t max_memory_bytes = 2ULL * 1024 * 1024 * 1024; // 2GB default
    size_t max_models = 10;                              // Max models in cache
    EvictionPolicy eviction_policy = EvictionPolicy::LRU;
    std::chrono::seconds ttl{3600}; // 1 hour TTL
    bool enable_preloading = true;
    bool enable_warmup = true;
    size_t warmup_iterations = 10;
    std::vector<std::string> preload_models; // Models to preload
};

/**
 * Model cache for managing loaded embedding models
 */
class ModelCache {
public:
    explicit ModelCache(const ModelCacheConfig& config = {});
    ~ModelCache();

    // Non-copyable but movable
    ModelCache(const ModelCache&) = delete;
    ModelCache& operator=(const ModelCache&) = delete;
    ModelCache(ModelCache&&) noexcept;
    ModelCache& operator=(ModelCache&&) noexcept;

    // Initialize cache with model loader
    Result<void> initialize(std::shared_ptr<ModelLoader> loader);
    void shutdown();

    // Model loading and retrieval
    Result<std::shared_ptr<void>> getModel(const std::string& model_id);
    Result<void> preloadModel(const std::string& model_id);
    Result<void> unloadModel(const std::string& model_id);

    // Batch operations
    Result<void> preloadModels(const std::vector<std::string>& model_ids);
    void unloadAllModels();

    // Cache management
    Result<void> evictLRU();
    Result<void> evictExpired();
    Result<void> resize(size_t new_max_memory);
    void clear();

    // Model warmup
    Result<void> warmupModel(const std::string& model_id);
    bool isWarmedUp(const std::string& model_id) const;

    // Hot-swapping support
    Result<void> swapModel(const std::string& old_model_id, const std::string& new_model_id);

    // Monitoring
    bool hasModel(const std::string& model_id) const;
    size_t getModelCount() const;
    size_t getMemoryUsage() const;
    size_t getAvailableMemory() const;
    std::vector<std::string> getCachedModels() const;

    // Statistics
    struct CacheStats {
        size_t total_models_loaded = 0;
        size_t cache_hits = 0;
        size_t cache_misses = 0;
        size_t evictions = 0;
        size_t memory_usage_bytes = 0;
        size_t max_memory_bytes = 0;
        double hit_rate = 0.0;
        std::chrono::milliseconds avg_load_time{0};
    };

    CacheStats getStats() const;
    void resetStats();

    // Performance metrics
    Result<void> updateModelMetrics(const std::string& model_id, double inference_time_ms);

    // Configuration
    void updateConfig(const ModelCacheConfig& config);
    const ModelCacheConfig& getConfig() const;

private:
    class Impl;
    std::unique_ptr<Impl> pImpl;
};

/**
 * Model preloader for background loading
 */
class ModelPreloader {
public:
    explicit ModelPreloader(std::shared_ptr<ModelCache> cache);
    ~ModelPreloader();

    // Start/stop background preloading
    void start();
    void stop();
    bool isRunning() const;

    // Queue models for preloading
    void queueModel(const std::string& model_id, int priority = 0);
    void queueModels(const std::vector<std::string>& model_ids);

    // Clear preload queue
    void clearQueue();
    size_t getQueueSize() const;

    // Wait for specific model to be loaded
    Result<void> waitForModel(const std::string& model_id,
                              std::chrono::milliseconds timeout = std::chrono::milliseconds(5000));

private:
    class Impl;
    std::unique_ptr<Impl> pImpl;
};

/**
 * Cache warming utilities
 */
namespace warming {
// Generate sample inputs for warmup
std::vector<std::string> generateWarmupSamples(size_t count = 10, size_t avg_length = 100);

// Warm up model with sample inputs
Result<void> warmupWithSamples(std::shared_ptr<void> model,
                               const std::vector<std::string>& samples);

// Benchmark model performance during warmup
struct WarmupMetrics {
    double min_time_ms = std::numeric_limits<double>::max();
    double max_time_ms = 0.0;
    double avg_time_ms = 0.0;
    double stddev_time_ms = 0.0;
    size_t iterations = 0;
};

Result<WarmupMetrics> benchmarkDuringWarmup(std::shared_ptr<void> model, size_t iterations = 100);
} // namespace warming

/**
 * Memory manager for model cache
 */
class CacheMemoryManager {
public:
    explicit CacheMemoryManager(size_t max_memory_bytes);
    ~CacheMemoryManager();

    // Memory allocation tracking
    bool canAllocate(size_t bytes) const;
    Result<void> allocate(const std::string& model_id, size_t bytes);
    void deallocate(const std::string& model_id);

    // Memory queries
    size_t getTotalMemory() const;
    size_t getUsedMemory() const;
    size_t getAvailableMemory() const;
    size_t getModelMemory(const std::string& model_id) const;

    // Memory pressure handling
    std::vector<std::string> getSuggestedEvictions(size_t bytes_needed) const;
    bool isUnderPressure() const;
    double getMemoryPressure() const; // 0.0 to 1.0

    // Configuration
    void setMaxMemory(size_t bytes);
    void setHighWatermark(double ratio); // e.g., 0.9 for 90%

private:
    class Impl;
    std::unique_ptr<Impl> pImpl;
};

} // namespace yams::vector