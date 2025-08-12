#include <yams/vector/model_cache.h>
#include <yams/vector/model_loader.h>
#include <spdlog/spdlog.h>
#include <unordered_map>
#include <list>
#include <queue>
#include <thread>
#include <condition_variable>

namespace yams::vector {

class ModelCache::Impl {
public:
    explicit Impl(const ModelCacheConfig& config) 
        : config_(config)
        , memory_manager_(config.max_memory_bytes) {
    }
    
    ~Impl() {
        shutdown();
    }
    
    Result<void> initialize(std::shared_ptr<ModelLoader> loader) {
        if (!loader) {
            return Error{ErrorCode::InvalidArgument, "Model loader is null"};
        }
        
        loader_ = loader;
        initialized_ = true;
        
        // Preload models if configured
        if (config_.enable_preloading && !config_.preload_models.empty()) {
            for (const auto& model_id : config_.preload_models) {
                auto result = preloadModel(model_id);
                if (!result.has_value()) {
                    spdlog::warn("Failed to preload model {}: {}", 
                        model_id, result.error().message);
                }
            }
        }
        
        return Result<void>();
    }
    
    void shutdown() {
        std::unique_lock lock(mutex_);
        cache_.clear();
        lru_list_.clear();
        initialized_ = false;
    }
    
    Result<std::shared_ptr<void>> getModel(const std::string& model_id) {
        std::unique_lock lock(mutex_);
        
        auto it = cache_.find(model_id);
        if (it != cache_.end()) {
            // Cache hit
            stats_.cache_hits++;
            updateLRU(model_id);
            it->second->access_count++;
            it->second->last_accessed = std::chrono::system_clock::now();
            return it->second->model_handle;
        }
        
        // Cache miss
        stats_.cache_misses++;
        
        // Load model
        lock.unlock();
        auto load_result = loadModelInternal(model_id);
        lock.lock();
        
        if (!load_result.has_value()) {
            return load_result.error();
        }
        
        return load_result.value()->model_handle;
    }
    
    Result<void> preloadModel(const std::string& model_id) {
        auto result = loadModelInternal(model_id);
        if (!result.has_value()) {
            return Error{result.error().code, result.error().message};
        }
        
        if (config_.enable_warmup) {
            return warmupModel(model_id);
        }
        
        return Result<void>();
    }
    
    Result<void> unloadModel(const std::string& model_id) {
        std::unique_lock lock(mutex_);
        
        auto it = cache_.find(model_id);
        if (it == cache_.end()) {
            return Error{ErrorCode::NotFound, "Model not in cache"};
        }
        
        memory_manager_.deallocate(model_id);
        cache_.erase(it);
        
        // Remove from LRU list
        lru_list_.erase(
            std::remove(lru_list_.begin(), lru_list_.end(), model_id),
            lru_list_.end()
        );
        
        spdlog::info("Unloaded model: {}", model_id);
        return Result<void>();
    }
    
    Result<void> warmupModel(const std::string& model_id) {
        std::unique_lock lock(mutex_);
        
        auto it = cache_.find(model_id);
        if (it == cache_.end()) {
            return Error{ErrorCode::NotFound, "Model not in cache"};
        }
        
        if (it->second->is_warming_up) {
            return Error{ErrorCode::InvalidState, "Model is already warming up"};
        }
        
        it->second->is_warming_up = true;
        auto model = it->second->model_handle;
        lock.unlock();
        
        // Generate warmup samples
        auto samples = warming::generateWarmupSamples(
            config_.warmup_iterations, 100);
        
        // Run warmup
        auto warmup_result = warming::warmupWithSamples(model, samples);
        
        lock.lock();
        it->second->is_warming_up = false;
        
        if (!warmup_result.has_value()) {
            return warmup_result;
        }
        
        spdlog::info("Warmed up model: {}", model_id);
        return Result<void>();
    }
    
    bool hasModel(const std::string& model_id) const {
        std::shared_lock lock(mutex_);
        return cache_.find(model_id) != cache_.end();
    }
    
    size_t getModelCount() const {
        std::shared_lock lock(mutex_);
        return cache_.size();
    }
    
    size_t getMemoryUsage() const {
        return memory_manager_.getUsedMemory();
    }
    
    size_t getAvailableMemory() const {
        return memory_manager_.getAvailableMemory();
    }
    
    ModelCache::CacheStats getStats() const {
        std::shared_lock lock(mutex_);
        
        CacheStats stats = stats_;
        stats.memory_usage_bytes = memory_manager_.getUsedMemory();
        stats.max_memory_bytes = memory_manager_.getTotalMemory();
        
        if (stats.cache_hits + stats.cache_misses > 0) {
            stats.hit_rate = static_cast<double>(stats.cache_hits) / 
                (stats.cache_hits + stats.cache_misses);
        }
        
        return stats;
    }
    
private:
    Result<std::shared_ptr<CachedModel>> loadModelInternal(const std::string& model_id) {
        if (!loader_) {
            return Error{ErrorCode::InvalidState, "Model loader not initialized"};
        }
        
        auto start = std::chrono::high_resolution_clock::now();
        
        // Load model (simplified - would get path from registry)
#if defined(YAMS_HAS_STD_FORMAT) && YAMS_HAS_STD_FORMAT
        std::string model_path = std::format("models/{}.onnx", model_id);
#else
        std::string model_path = std::string("models/") + model_id + ".onnx";
#endif
        auto load_result = loader_->loadModel(model_path);
        
        if (!load_result.has_value()) {
            return Error{load_result.error().code, load_result.error().message};
        }
        
        auto end = std::chrono::high_resolution_clock::now();
        auto load_time = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        
        // Create cached entry
        auto cached = std::make_shared<CachedModel>();
        cached->model_id = model_id;
        cached->model_handle = load_result.value();
        cached->loaded_at = std::chrono::system_clock::now();
        cached->last_accessed = cached->loaded_at;
        
        // Estimate memory usage (simplified)
        cached->memory_usage_bytes = estimateModelMemory(model_id);
        
        // Check memory and evict if needed
        while (!memory_manager_.canAllocate(cached->memory_usage_bytes)) {
            auto evict_result = evictLRU();
            if (!evict_result.has_value()) {
                return Error{ErrorCode::ResourceExhausted, "Cannot allocate memory for model"};
            }
        }
        
        // Allocate memory
        auto alloc_result = memory_manager_.allocate(model_id, cached->memory_usage_bytes);
        if (!alloc_result.has_value()) {
            return Error{alloc_result.error().code, alloc_result.error().message};
        }
        
        // Add to cache
        std::unique_lock lock(mutex_);
        cache_[model_id] = cached;
        lru_list_.push_front(model_id);
        
        // Update stats
        stats_.total_models_loaded++;
        stats_.avg_load_time = std::chrono::milliseconds(
            (stats_.avg_load_time.count() * (stats_.total_models_loaded - 1) + 
             load_time.count()) / stats_.total_models_loaded
        );
        
        spdlog::info("Loaded model {} in {}ms", model_id, load_time.count());
        
        return cached;
    }
    
    Result<void> evictLRU() {
        std::unique_lock lock(mutex_);
        
        if (lru_list_.empty()) {
            return Error{ErrorCode::InvalidState, "No models to evict"};
        }
        
        // Get LRU model
        std::string model_id = lru_list_.back();
        lru_list_.pop_back();
        
        // Remove from cache
        auto it = cache_.find(model_id);
        if (it != cache_.end()) {
            memory_manager_.deallocate(model_id);
            cache_.erase(it);
            stats_.evictions++;
            
            spdlog::info("Evicted model: {}", model_id);
        }
        
        return Result<void>();
    }
    
    void updateLRU(const std::string& model_id) {
        // Move to front of LRU list
        lru_list_.erase(
            std::remove(lru_list_.begin(), lru_list_.end(), model_id),
            lru_list_.end()
        );
        lru_list_.push_front(model_id);
    }
    
    size_t estimateModelMemory(const std::string& model_id) {
        // Simplified estimation based on model ID
        if (model_id.find("large") != std::string::npos) {
            return 500 * 1024 * 1024;  // 500MB
        } else if (model_id.find("base") != std::string::npos) {
            return 200 * 1024 * 1024;  // 200MB
        } else {
            return 100 * 1024 * 1024;  // 100MB default
        }
    }
    
private:
    ModelCacheConfig config_;
    std::shared_ptr<ModelLoader> loader_;
    CacheMemoryManager memory_manager_;
    
    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, std::shared_ptr<CachedModel>> cache_;
    std::list<std::string> lru_list_;
    
    mutable CacheStats stats_;
    std::atomic<bool> initialized_{false};
};

// ModelCache implementation
ModelCache::ModelCache(const ModelCacheConfig& config) 
    : pImpl(std::make_unique<Impl>(config)) {}
ModelCache::~ModelCache() = default;
ModelCache::ModelCache(ModelCache&&) noexcept = default;
ModelCache& ModelCache::operator=(ModelCache&&) noexcept = default;

Result<void> ModelCache::initialize(std::shared_ptr<ModelLoader> loader) {
    return pImpl->initialize(loader);
}

void ModelCache::shutdown() {
    pImpl->shutdown();
}

Result<std::shared_ptr<void>> ModelCache::getModel(const std::string& model_id) {
    return pImpl->getModel(model_id);
}

Result<void> ModelCache::preloadModel(const std::string& model_id) {
    return pImpl->preloadModel(model_id);
}

Result<void> ModelCache::unloadModel(const std::string& model_id) {
    return pImpl->unloadModel(model_id);
}

Result<void> ModelCache::warmupModel(const std::string& model_id) {
    return pImpl->warmupModel(model_id);
}

bool ModelCache::hasModel(const std::string& model_id) const {
    return pImpl->hasModel(model_id);
}

size_t ModelCache::getModelCount() const {
    return pImpl->getModelCount();
}

size_t ModelCache::getMemoryUsage() const {
    return pImpl->getMemoryUsage();
}

size_t ModelCache::getAvailableMemory() const {
    return pImpl->getAvailableMemory();
}

ModelCache::CacheStats ModelCache::getStats() const {
    return pImpl->getStats();
}

// CacheMemoryManager implementation
class CacheMemoryManager::Impl {
public:
    explicit Impl(size_t max_memory_bytes) 
        : max_memory_(max_memory_bytes)
        , used_memory_(0)
        , high_watermark_(0.9) {
    }
    
    bool canAllocate(size_t bytes) const {
        std::shared_lock lock(mutex_);
        return used_memory_ + bytes <= max_memory_;
    }
    
    Result<void> allocate(const std::string& model_id, size_t bytes) {
        std::unique_lock lock(mutex_);
        
        if (used_memory_ + bytes > max_memory_) {
            return Error{ErrorCode::ResourceExhausted, 
                "Not enough memory to allocate model"};
        }
        
        allocations_[model_id] = bytes;
        used_memory_ += bytes;
        
        return Result<void>();
    }
    
    void deallocate(const std::string& model_id) {
        std::unique_lock lock(mutex_);
        
        auto it = allocations_.find(model_id);
        if (it != allocations_.end()) {
            used_memory_ -= it->second;
            allocations_.erase(it);
        }
    }
    
    size_t getTotalMemory() const {
        return max_memory_;
    }
    
    size_t getUsedMemory() const {
        std::shared_lock lock(mutex_);
        return used_memory_;
    }
    
    size_t getAvailableMemory() const {
        std::shared_lock lock(mutex_);
        return max_memory_ - used_memory_;
    }
    
    bool isUnderPressure() const {
        std::shared_lock lock(mutex_);
        return static_cast<double>(used_memory_) / max_memory_ > high_watermark_;
    }
    
    double getMemoryPressure() const {
        std::shared_lock lock(mutex_);
        return static_cast<double>(used_memory_) / max_memory_;
    }
    
private:
    mutable std::shared_mutex mutex_;
    size_t max_memory_;
    size_t used_memory_;
    double high_watermark_;
    std::unordered_map<std::string, size_t> allocations_;
};

CacheMemoryManager::CacheMemoryManager(size_t max_memory_bytes)
    : pImpl(std::make_unique<Impl>(max_memory_bytes)) {}

CacheMemoryManager::~CacheMemoryManager() = default;

bool CacheMemoryManager::canAllocate(size_t bytes) const {
    return pImpl->canAllocate(bytes);
}

Result<void> CacheMemoryManager::allocate(const std::string& model_id, size_t bytes) {
    return pImpl->allocate(model_id, bytes);
}

void CacheMemoryManager::deallocate(const std::string& model_id) {
    pImpl->deallocate(model_id);
}

size_t CacheMemoryManager::getTotalMemory() const {
    return pImpl->getTotalMemory();
}

size_t CacheMemoryManager::getUsedMemory() const {
    return pImpl->getUsedMemory();
}

size_t CacheMemoryManager::getAvailableMemory() const {
    return pImpl->getAvailableMemory();
}

bool CacheMemoryManager::isUnderPressure() const {
    return pImpl->isUnderPressure();
}

double CacheMemoryManager::getMemoryPressure() const {
    return pImpl->getMemoryPressure();
}

// Warming utilities
namespace warming {

std::vector<std::string> generateWarmupSamples(size_t count, size_t avg_length) {
    std::vector<std::string> samples;
    samples.reserve(count);
    
    const std::vector<std::string> sentences = {
        "The quick brown fox jumps over the lazy dog.",
        "Machine learning models require warmup for optimal performance.",
        "Vector embeddings capture semantic meaning of text.",
        "Natural language processing enables computers to understand text.",
        "Deep learning has revolutionized artificial intelligence."
    };
    
    for (size_t i = 0; i < count; ++i) {
        std::string sample;
        while (sample.length() < avg_length) {
            sample += sentences[i % sentences.size()] + " ";
        }
        samples.push_back(sample.substr(0, avg_length));
    }
    
    return samples;
}

Result<void> warmupWithSamples(
    std::shared_ptr<void> model,
    const std::vector<std::string>& samples
) {
    if (!model) {
        return Error{ErrorCode::InvalidArgument, "Model is null"};
    }
    
    // Simplified warmup - would actually run inference
    for (const auto& sample : samples) {
        // Simulate inference
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    
    return Result<void>();
}

} // namespace warming

} // namespace yams::vector