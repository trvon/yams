#pragma once

#include <atomic>
#include <chrono>
#include <mutex>
#include <unordered_map>

namespace yams::search {

/**
 * @brief Statistics for cache performance monitoring
 */
struct CacheStats {
    // Basic counters
    std::atomic<uint64_t> hits{0};          ///< Number of cache hits
    std::atomic<uint64_t> misses{0};        ///< Number of cache misses
    std::atomic<uint64_t> evictions{0};     ///< Number of evictions
    std::atomic<uint64_t> invalidations{0}; ///< Number of invalidations
    std::atomic<uint64_t> insertions{0};    ///< Number of insertions

    // Size metrics
    std::atomic<size_t> currentSize{0}; ///< Current number of entries
    std::atomic<size_t> memoryUsage{0}; ///< Current memory usage in bytes
    size_t maxSize = 0;                 ///< Maximum cache size
    size_t maxMemory = 0;               ///< Maximum memory limit

    // Performance metrics
    std::atomic<uint64_t> totalAccessTime{0}; ///< Total access time in microseconds
    std::atomic<uint64_t> totalHitTime{0};    ///< Total hit access time
    std::atomic<uint64_t> totalMissTime{0};   ///< Total miss access time

    // TTL metrics
    std::atomic<uint64_t> ttlExpirations{0}; ///< Number of TTL expirations

    // Default constructor
    CacheStats() = default;

    // Copy constructor (needed because of atomic members)
    CacheStats(const CacheStats& other) {
        hits.store(other.hits.load());
        misses.store(other.misses.load());
        evictions.store(other.evictions.load());
        invalidations.store(other.invalidations.load());
        insertions.store(other.insertions.load());
        currentSize.store(other.currentSize.load());
        memoryUsage.store(other.memoryUsage.load());
        maxSize = other.maxSize;
        maxMemory = other.maxMemory;
        totalAccessTime.store(other.totalAccessTime.load());
        totalHitTime.store(other.totalHitTime.load());
        totalMissTime.store(other.totalMissTime.load());
        ttlExpirations.store(other.ttlExpirations.load());
    }

    // Assignment operator (needed because of atomic members)
    CacheStats& operator=(const CacheStats& other) {
        if (this != &other) {
            hits.store(other.hits.load());
            misses.store(other.misses.load());
            evictions.store(other.evictions.load());
            invalidations.store(other.invalidations.load());
            insertions.store(other.insertions.load());
            currentSize.store(other.currentSize.load());
            memoryUsage.store(other.memoryUsage.load());
            maxSize = other.maxSize;
            maxMemory = other.maxMemory;
            totalAccessTime.store(other.totalAccessTime.load());
            totalHitTime.store(other.totalHitTime.load());
            totalMissTime.store(other.totalMissTime.load());
            ttlExpirations.store(other.ttlExpirations.load());
        }
        return *this;
    }

    /**
     * @brief Calculate hit rate
     */
    double hitRate() const {
        uint64_t total = hits.load() + misses.load();
        return total > 0 ? static_cast<double>(hits.load()) / total : 0.0;
    }

    /**
     * @brief Calculate average access time
     */
    std::chrono::microseconds avgAccessTime() const {
        uint64_t total = hits.load() + misses.load();
        return total > 0 ? std::chrono::microseconds(totalAccessTime.load() / total)
                         : std::chrono::microseconds(0);
    }

    /**
     * @brief Calculate average hit time
     */
    std::chrono::microseconds avgHitTime() const {
        uint64_t h = hits.load();
        return h > 0 ? std::chrono::microseconds(totalHitTime.load() / h)
                     : std::chrono::microseconds(0);
    }

    /**
     * @brief Calculate average miss time
     */
    std::chrono::microseconds avgMissTime() const {
        uint64_t m = misses.load();
        return m > 0 ? std::chrono::microseconds(totalMissTime.load() / m)
                     : std::chrono::microseconds(0);
    }

    /**
     * @brief Reset all statistics
     */
    void reset() {
        hits.store(0);
        misses.store(0);
        evictions.store(0);
        invalidations.store(0);
        insertions.store(0);
        currentSize.store(0);
        memoryUsage.store(0);
        totalAccessTime.store(0);
        totalHitTime.store(0);
        totalMissTime.store(0);
        ttlExpirations.store(0);
    }
};

/**
 * @brief Extended statistics with query pattern tracking
 */
class ExtendedCacheStats : public CacheStats {
public:
    /**
     * @brief Record a query pattern
     */
    void recordQueryPattern(const std::string& pattern) {
        std::lock_guard<std::mutex> lock(mutex_);
        queryPatterns_[pattern]++;
    }

    /**
     * @brief Get top N query patterns
     */
    std::vector<std::pair<std::string, uint64_t>> getTopPatterns(size_t n = 10) const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<std::pair<std::string, uint64_t>> patterns(queryPatterns_.begin(),
                                                               queryPatterns_.end());

        std::partial_sort(patterns.begin(), patterns.begin() + std::min(n, patterns.size()),
                          patterns.end(),
                          [](const auto& a, const auto& b) { return a.second > b.second; });

        if (patterns.size() > n) {
            patterns.resize(n);
        }

        return patterns;
    }

    /**
     * @brief Clear query patterns
     */
    void clearPatterns() {
        std::lock_guard<std::mutex> lock(mutex_);
        queryPatterns_.clear();
    }

private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, uint64_t> queryPatterns_;
};

} // namespace yams::search