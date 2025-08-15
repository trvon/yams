#pragma once

#include <chrono>
#include <functional>
#include <list>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <yams/core/types.h>
#include <yams/search/cache_key.h>
#include <yams/search/cache_stats.h>
#include <yams/search/search_results.h>

namespace yams::search {

/**
 * @brief Configuration for search result cache
 */
struct SearchCacheConfig {
    size_t maxEntries = 1000;             ///< Maximum number of cache entries
    size_t maxMemoryMB = 100;             ///< Maximum memory usage in MB
    std::chrono::seconds defaultTTL{300}; ///< Default TTL (5 minutes)
    bool enableStatistics = true;         ///< Enable statistics tracking
    bool enablePatternTracking = false;   ///< Track query patterns
    bool enableCompression = false;       ///< Compress cached results

    // Eviction policy settings
    enum class EvictionPolicy {
        LRU,  ///< Least Recently Used
        LFU,  ///< Least Frequently Used
        FIFO, ///< First In First Out
        TTL   ///< Time To Live based
    };

    EvictionPolicy evictionPolicy = EvictionPolicy::LRU;

    // Cache warming settings
    bool enableWarmup = false;              ///< Enable cache warming
    std::vector<std::string> warmupQueries; ///< Queries to pre-cache
};

/**
 * @brief Thread-safe LRU cache for search results
 */
class SearchCache {
public:
    /**
     * @brief Cache entry with metadata
     */
    struct CacheEntry {
        SearchResults data;
        std::chrono::system_clock::time_point insertTime;
        std::chrono::system_clock::time_point lastAccessTime;
        std::chrono::seconds ttl;
        size_t accessCount = 0;
        size_t memorySize = 0;

        /**
         * @brief Check if entry has expired
         */
        bool isExpired() const {
            auto now = std::chrono::system_clock::now();
            return (now - insertTime) > ttl;
        }
    };

    /**
     * @brief Constructor
     */
    explicit SearchCache(const SearchCacheConfig& config = {});

    /**
     * @brief Destructor
     */
    ~SearchCache() = default;

    // ===== Core Operations =====

    /**
     * @brief Get cached search results
     * @return nullopt if not found or expired
     */
    std::optional<SearchResults> get(const CacheKey& key);

    /**
     * @brief Store search results in cache
     */
    void put(const CacheKey& key, const SearchResults& results,
             std::chrono::seconds ttl = std::chrono::seconds{0});

    /**
     * @brief Check if key exists and is not expired
     */
    bool contains(const CacheKey& key) const;

    /**
     * @brief Remove entry from cache
     */
    void invalidate(const CacheKey& key);

    /**
     * @brief Invalidate entries matching a pattern
     */
    size_t invalidatePattern(const std::string& pattern);

    /**
     * @brief Clear entire cache
     */
    void clear();

    // ===== Cache Management =====

    /**
     * @brief Get current cache size
     */
    size_t size() const;

    /**
     * @brief Get current memory usage
     */
    size_t memoryUsage() const;

    /**
     * @brief Manually trigger eviction
     */
    void evict(size_t count = 1);

    /**
     * @brief Remove expired entries
     */
    size_t removeExpired();

    // ===== Statistics =====

    /**
     * @brief Get cache statistics
     */
    CacheStats getStats() const;

    /**
     * @brief Reset statistics
     */
    void resetStats();

    // ===== Cache Warming =====

    /**
     * @brief Warm cache with predefined queries
     */
    using QueryExecutor = std::function<SearchResults(const std::string&)>;
    void warmUp(const QueryExecutor& executor);

    /**
     * @brief Add query to warmup list
     */
    void addWarmupQuery(const std::string& query);

    // ===== Configuration =====

    /**
     * @brief Update cache configuration
     */
    void setConfig(const SearchCacheConfig& config);

    /**
     * @brief Get current configuration
     */
    const SearchCacheConfig& getConfig() const { return config_; }

    // ===== Persistence =====

    /**
     * @brief Save cache to disk
     */
    Result<void> saveToDisk(const std::string& path) const;

    /**
     * @brief Load cache from disk
     */
    Result<void> loadFromDisk(const std::string& path);

private:
    // LRU cache implementation
    using KeyType = CacheKey;
    using ValueType = std::shared_ptr<CacheEntry>;
    using ListIterator = std::list<KeyType>::iterator;

    mutable std::shared_mutex mutex_;
    SearchCacheConfig config_;

    // LRU data structures
    std::list<KeyType> lruList_; ///< LRU ordering
    std::unordered_map<KeyType, ListIterator, CacheKeyHash> keyToListIter_;
    std::unordered_map<KeyType, ValueType, CacheKeyHash> cache_;

    // Statistics
    mutable ExtendedCacheStats stats_;

    // Memory tracking
    std::atomic<size_t> currentMemoryUsage_{0};

    /**
     * @brief Move key to front (most recently used)
     */
    void moveToFront(const KeyType& key);

    /**
     * @brief Evict least recently used entry
     */
    void evictLRU();

    /**
     * @brief Evict based on configured policy
     */
    void evictByPolicy();

    /**
     * @brief Calculate memory size of a search response
     */
    static size_t calculateMemorySize(const SearchResults& response);

    /**
     * @brief Update access statistics
     */
    void updateAccessStats(ValueType& entry, bool isHit, std::chrono::microseconds accessTime);

    /**
     * @brief Check memory limits and evict if necessary
     */
    void enforceMemoryLimit();

    /**
     * @brief Serialize cache entry
     */
    std::string serializeEntry(const CacheEntry& entry) const;

    /**
     * @brief Deserialize cache entry
     */
    std::unique_ptr<CacheEntry> deserializeEntry(const std::string& data) const;
};

/**
 * @brief Factory for creating search cache instances
 */
class SearchCacheFactory {
public:
    /**
     * @brief Create default cache
     */
    static std::unique_ptr<SearchCache> createDefault();

    /**
     * @brief Create cache with custom config
     */
    static std::unique_ptr<SearchCache> create(const SearchCacheConfig& config);

    /**
     * @brief Create distributed cache (future enhancement)
     */
    static std::unique_ptr<SearchCache> createDistributed(const SearchCacheConfig& config,
                                                          const std::string& redisUrl);
};

} // namespace yams::search