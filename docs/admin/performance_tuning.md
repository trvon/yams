# YAMS Search Performance Tuning Guide

This guide provides comprehensive performance tuning strategies for the YAMS search system.

## Table of Contents

1. [Performance Overview](#performance-overview)
2. [System Requirements](#system-requirements)
3. [Database Optimization](#database-optimization)
4. [Query Optimization](#query-optimization)
5. [Cache Optimization](#cache-optimization)
6. [Indexing Optimization](#indexing-optimization)
7. [Memory Management](#memory-management)
8. [I/O Optimization](#i-o-optimization)
9. [Concurrency Tuning](#concurrency-tuning)
10. [Monitoring and Profiling](#monitoring-and-profiling)
11. [Performance Benchmarks](#performance-benchmarks)
12. [Troubleshooting Performance Issues](#troubleshooting-performance-issues)

## Performance Overview

The YAMS search system is designed for high performance with typical query response times under 50ms. Performance depends on several factors:

- Document corpus size and complexity
- Query patterns and complexity
- System resources (CPU, memory, storage)
- Configuration settings
- Hardware characteristics

### Performance Targets

| Metric | Target | Acceptable | Poor |
|--------|---------|------------|------|
| Simple Query Latency | <15ms | <50ms | >100ms |
| Complex Query Latency | <50ms | <150ms | >300ms |
| Cache Hit Latency | <1ms | <5ms | >10ms |
| Index Update Latency | <10ms | <50ms | >100ms |
| Memory Usage | <1GB | <2GB | >4GB |
| CPU Usage | <30% | <60% | >80% |

## System Requirements

### Minimum Requirements

- **CPU**: 2 cores, 2.0 GHz
- **Memory**: 2GB RAM
- **Storage**: 10GB available space
- **Documents**: Up to 100,000 documents

### Recommended Requirements

- **CPU**: 4+ cores, 3.0+ GHz
- **Memory**: 8GB+ RAM
- **Storage**: SSD with 50GB+ space
- **Documents**: Up to 1,000,000 documents

### High-Performance Requirements

- **CPU**: 8+ cores, 3.5+ GHz
- **Memory**: 16GB+ RAM
- **Storage**: NVMe SSD with 100GB+ space
- **Documents**: 1,000,000+ documents

## Database Optimization

### SQLite Configuration

The foundation of search performance is proper database configuration:

```cpp
DatabaseConfig config;
config.pragmas = {
    // Enable WAL mode for better concurrency
    {"journal_mode", "WAL"},
    
    // Balance durability and performance
    {"synchronous", "NORMAL"},
    
    // Large cache for better performance (pages in memory)
    {"cache_size", "50000"},  // ~200MB cache
    
    // Use memory for temporary operations
    {"temp_store", "MEMORY"},
    
    // Memory-mapped I/O for large databases
    {"mmap_size", "1073741824"},  // 1GB
    
    // Optimize page size for your workload
    {"page_size", "4096"},
    
    // Auto-vacuum for space efficiency
    {"auto_vacuum", "INCREMENTAL"},
    
    // Optimize FTS5
    {"optimize", ""}
};
```

### FTS5 Index Optimization

Regular FTS5 optimization is crucial for performance:

```cpp
// Run optimization periodically
void optimizeFTSIndex(std::shared_ptr<Database> db) {
    auto statement = db->prepare("INSERT INTO search_fts(search_fts) VALUES('optimize')");
    statement->execute();
}

// Schedule optimization
std::thread optimizationThread([db]() {
    while (running) {
        std::this_thread::sleep_for(std::chrono::hours(1));
        optimizeFTSIndex(db);
    }
});
```

### Connection Pool Sizing

Optimize connection pool for your workload:

```cpp
DatabaseConfig config;
// For read-heavy workloads
config.connectionPoolSize = std::thread::hardware_concurrency() * 2;

// For write-heavy workloads
config.connectionPoolSize = std::min(10u, std::thread::hardware_concurrency());
```

## Query Optimization

### Query Pattern Analysis

Understanding your query patterns helps optimize performance:

```cpp
struct QueryMetrics {
    std::string query;
    std::chrono::milliseconds avgLatency;
    size_t hitCount;
    double cacheHitRate;
};

class QueryAnalyzer {
public:
    void recordQuery(const std::string& query, std::chrono::milliseconds latency, bool cacheHit) {
        auto& metrics = queryMetrics[query];
        metrics.query = query;
        metrics.hitCount++;
        
        // Update average latency
        auto total = metrics.avgLatency.count() * (metrics.hitCount - 1) + latency.count();
        metrics.avgLatency = std::chrono::milliseconds(total / metrics.hitCount);
        
        // Update cache hit rate
        metrics.cacheHitRate = (metrics.cacheHitRate * (metrics.hitCount - 1) + (cacheHit ? 1.0 : 0.0)) / metrics.hitCount;
    }
    
    std::vector<QueryMetrics> getSlowQueries(std::chrono::milliseconds threshold) {
        std::vector<QueryMetrics> slow;
        for (const auto& [query, metrics] : queryMetrics) {
            if (metrics.avgLatency > threshold) {
                slow.push_back(metrics);
            }
        }
        return slow;
    }

private:
    std::unordered_map<std::string, QueryMetrics> queryMetrics;
};
```

### Query Rewriting

Optimize complex queries automatically:

```cpp
class QueryOptimizer {
public:
    std::string optimizeQuery(const std::string& originalQuery) {
        std::string optimized = originalQuery;
        
        // Convert broad wildcards to more specific terms
        optimized = replaceLeadingWildcards(optimized);
        
        // Reorder terms by selectivity
        optimized = reorderBySelectivity(optimized);
        
        // Add field restrictions where possible
        optimized = addFieldRestrictions(optimized);
        
        return optimized;
    }

private:
    std::string replaceLeadingWildcards(const std::string& query) {
        // Replace *term with term* if possible
        std::regex leadingWildcard(R"(\*([a-zA-Z0-9_]{3,}))");
        return std::regex_replace(query, leadingWildcard, "$1*");
    }
    
    std::string reorderBySelectivity(const std::string& query) {
        // Parse query and reorder terms by estimated selectivity
        // More selective (rare) terms first
        return query;  // Simplified
    }
};
```

### Result Limit Optimization

Optimize result limits based on actual usage:

```cpp
struct SearchRequest {
    std::string query;
    size_t maxResults = 20;  // Start conservative
    size_t offset = 0;
};

// Adaptive result limiting
class AdaptiveResultLimiter {
public:
    size_t getOptimalLimit(const std::string& query) {
        auto it = queryLimits.find(query);
        if (it != queryLimits.end()) {
            return it->second;
        }
        return defaultLimit;
    }
    
    void recordUsage(const std::string& query, size_t requestedLimit, size_t actualUsed) {
        // Adjust future limits based on usage patterns
        if (actualUsed < requestedLimit * 0.5) {
            queryLimits[query] = std::max(10ul, actualUsed * 2);
        }
    }

private:
    std::unordered_map<std::string, size_t> queryLimits;
    size_t defaultLimit = 20;
};
```

## Cache Optimization

### Cache Size Tuning

Optimal cache size depends on available memory and query patterns:

```cpp
// Calculate optimal cache size based on available memory
SearchCacheConfig calculateOptimalCacheConfig() {
    SearchCacheConfig config;
    
    // Get available memory
    auto availableMemory = getAvailableMemory();
    
    // Allocate up to 25% of available memory to search cache
    config.maxMemoryMB = std::min(2048ul, availableMemory / 4);
    
    // Estimate entries based on average result size
    size_t avgResultSize = 2048;  // bytes
    config.maxEntries = (config.maxMemoryMB * 1024 * 1024) / avgResultSize;
    
    return config;
}
```

### Cache Warming

Pre-populate cache with common queries:

```cpp
class CacheWarmer {
public:
    CacheWarmer(std::shared_ptr<SearchExecutor> executor, 
                std::shared_ptr<SearchCache> cache)
        : executor_(executor), cache_(cache) {}
    
    void warmCache(const std::vector<std::string>& commonQueries) {
        for (const auto& query : commonQueries) {
            SearchRequest request;
            request.query = query;
            
            auto result = executor_->search(request);
            if (result.isSuccess()) {
                auto key = CacheKey::fromQuery(query, nullptr, 0, 20);
                cache_->put(key, result.value(), std::chrono::minutes(30));
            }
        }
    }
    
    // Load common queries from analytics
    std::vector<std::string> getCommonQueries(size_t count = 100) {
        // Return top queries from usage statistics
        return queryAnalytics_->getTopQueries(count);
    }

private:
    std::shared_ptr<SearchExecutor> executor_;
    std::shared_ptr<SearchCache> cache_;
    std::unique_ptr<QueryAnalytics> queryAnalytics_;
};
```

### Cache Partitioning

Partition cache by query type for better hit rates:

```cpp
class PartitionedCache {
public:
    std::optional<SearchResponse> get(const CacheKey& key) {
        auto partition = selectPartition(key);
        return caches_[partition]->get(key);
    }
    
    void put(const CacheKey& key, const SearchResponse& response, std::chrono::seconds ttl) {
        auto partition = selectPartition(key);
        caches_[partition]->put(key, response, ttl);
    }

private:
    enum Partition {
        SIMPLE_QUERIES = 0,
        COMPLEX_QUERIES = 1,
        FACETED_QUERIES = 2,
        FILTERED_QUERIES = 3
    };
    
    Partition selectPartition(const CacheKey& key) {
        // Partition based on query characteristics
        if (key.queryString.find(" AND ") != std::string::npos ||
            key.queryString.find(" OR ") != std::string::npos) {
            return COMPLEX_QUERIES;
        }
        if (key.hasFacets) {
            return FACETED_QUERIES;
        }
        if (key.hasFilters) {
            return FILTERED_QUERIES;
        }
        return SIMPLE_QUERIES;
    }
    
    std::array<std::unique_ptr<SearchCache>, 4> caches_;
};
```

## Indexing Optimization

### Batch Size Optimization

Find optimal batch size through testing:

```cpp
class BatchSizeOptimizer {
public:
    size_t findOptimalBatchSize(const std::vector<std::string>& documents) {
        std::vector<size_t> batchSizes = {100, 500, 1000, 2000, 5000};
        size_t bestBatchSize = 1000;
        std::chrono::milliseconds bestTime{std::numeric_limits<long>::max()};
        
        for (size_t batchSize : batchSizes) {
            auto start = std::chrono::high_resolution_clock::now();
            
            // Index documents with this batch size
            indexDocuments(documents, batchSize);
            
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            
            if (duration < bestTime) {
                bestTime = duration;
                bestBatchSize = batchSize;
            }
        }
        
        return bestBatchSize;
    }

private:
    void indexDocuments(const std::vector<std::string>& documents, size_t batchSize) {
        // Implementation of batch indexing
    }
};
```

### Incremental Indexing

Optimize incremental updates:

```cpp
class IncrementalIndexer {
public:
    void indexChanges(const std::vector<DocumentChange>& changes) {
        // Group changes by type for optimal processing
        std::vector<DocumentChange> updates, deletes, inserts;
        
        for (const auto& change : changes) {
            switch (change.type) {
                case ChangeType::INSERT:
                    inserts.push_back(change);
                    break;
                case ChangeType::UPDATE:
                    updates.push_back(change);
                    break;
                case ChangeType::DELETE:
                    deletes.push_back(change);
                    break;
            }
        }
        
        // Process in optimal order: deletes first, then inserts, then updates
        processDeletes(deletes);
        processInserts(inserts);
        processUpdates(updates);
    }
    
private:
    void processDeletes(const std::vector<DocumentChange>& deletes) {
        // Batch delete operations
        if (deletes.empty()) return;
        
        auto stmt = db_->prepare("DELETE FROM search_fts WHERE docid = ?");
        for (const auto& change : deletes) {
            stmt->bind(1, change.documentId);
            stmt->execute();
            stmt->reset();
        }
    }
    
    void processInserts(const std::vector<DocumentChange>& inserts) {
        // Batch insert operations
        if (inserts.empty()) return;
        
        auto stmt = db_->prepare("INSERT INTO search_fts(docid, content, title) VALUES (?, ?, ?)");
        for (const auto& change : inserts) {
            stmt->bind(1, change.documentId);
            stmt->bind(2, change.content);
            stmt->bind(3, change.title);
            stmt->execute();
            stmt->reset();
        }
    }
};
```

## Memory Management

### Memory Pool Allocation

Use memory pools for frequently allocated objects:

```cpp
template<typename T>
class MemoryPool {
public:
    explicit MemoryPool(size_t poolSize) : poolSize_(poolSize) {
        pool_.reserve(poolSize);
        for (size_t i = 0; i < poolSize; ++i) {
            pool_.emplace_back(std::make_unique<T>());
        }
    }
    
    std::unique_ptr<T> acquire() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!pool_.empty()) {
            auto obj = std::move(pool_.back());
            pool_.pop_back();
            return obj;
        }
        return std::make_unique<T>();
    }
    
    void release(std::unique_ptr<T> obj) {
        if (obj) {
            // Reset object state
            obj->reset();
            
            std::lock_guard<std::mutex> lock(mutex_);
            if (pool_.size() < poolSize_) {
                pool_.push_back(std::move(obj));
            }
        }
    }

private:
    std::vector<std::unique_ptr<T>> pool_;
    size_t poolSize_;
    std::mutex mutex_;
};

// Usage
MemoryPool<SearchResponse> responsePool(1000);
```

### Memory Monitoring

Monitor memory usage for optimization:

```cpp
class MemoryMonitor {
public:
    struct MemoryStats {
        size_t totalMemory;
        size_t usedMemory;
        size_t freeMemory;
        size_t cacheMemory;
        double memoryPressure;
    };
    
    MemoryStats getMemoryStats() {
        MemoryStats stats;
        
        // Get system memory info
        stats.totalMemory = getTotalSystemMemory();
        stats.usedMemory = getUsedMemory();
        stats.freeMemory = stats.totalMemory - stats.usedMemory;
        
        // Get cache memory usage
        stats.cacheMemory = cache_->getMemoryUsage();
        
        // Calculate memory pressure (0.0 to 1.0)
        stats.memoryPressure = static_cast<double>(stats.usedMemory) / stats.totalMemory;
        
        return stats;
    }
    
    bool isMemoryPressureHigh() {
        return getMemoryStats().memoryPressure > 0.85;
    }
    
    void handleMemoryPressure() {
        if (isMemoryPressureHigh()) {
            // Trigger cache eviction
            cache_->evictLRU(0.2);  // Evict 20% of cache
            
            // Force garbage collection if available
            triggerGC();
        }
    }

private:
    std::shared_ptr<SearchCache> cache_;
};
```

## I/O Optimization

### Asynchronous I/O

Use async I/O for better performance:

```cpp
class AsyncSearchExecutor {
public:
    std::future<SearchResponse> searchAsync(const SearchRequest& request) {
        return std::async(std::launch::async, [this, request]() {
            return search(request);
        });
    }
    
    std::vector<std::future<SearchResponse>> searchBatch(
        const std::vector<SearchRequest>& requests) {
        
        std::vector<std::future<SearchResponse>> futures;
        futures.reserve(requests.size());
        
        for (const auto& request : requests) {
            futures.push_back(searchAsync(request));
        }
        
        return futures;
    }
    
private:
    SearchResponse search(const SearchRequest& request) {
        // Actual search implementation
        return {};
    }
};
```

### I/O Batching

Batch I/O operations for efficiency:

```cpp
class BatchedIndexer {
public:
    void indexDocument(const Document& doc) {
        buffer_.push_back(doc);
        
        if (buffer_.size() >= batchSize_) {
            flushBuffer();
        }
    }
    
    void flush() {
        if (!buffer_.empty()) {
            flushBuffer();
        }
    }
    
private:
    void flushBuffer() {
        // Begin transaction
        auto transaction = db_->beginTransaction();
        
        try {
            auto stmt = db_->prepare("INSERT INTO search_fts(docid, content, title) VALUES (?, ?, ?)");
            
            for (const auto& doc : buffer_) {
                stmt->bind(1, doc.id);
                stmt->bind(2, doc.content);
                stmt->bind(3, doc.title);
                stmt->execute();
                stmt->reset();
            }
            
            transaction->commit();
            buffer_.clear();
            
        } catch (const std::exception& e) {
            transaction->rollback();
            throw;
        }
    }
    
    std::vector<Document> buffer_;
    size_t batchSize_ = 1000;
    std::shared_ptr<Database> db_;
};
```

## Concurrency Tuning

### Thread Pool Optimization

Optimize thread pools for your workload:

```cpp
class OptimizedThreadPool {
public:
    explicit OptimizedThreadPool(size_t numThreads = 0) {
        if (numThreads == 0) {
            // Auto-detect optimal thread count
            numThreads = calculateOptimalThreadCount();
        }
        
        workers_.reserve(numThreads);
        for (size_t i = 0; i < numThreads; ++i) {
            workers_.emplace_back([this] { workerLoop(); });
        }
    }
    
    template<typename F>
    auto submit(F&& f) -> std::future<decltype(f())> {
        auto task = std::make_shared<std::packaged_task<decltype(f())()>>(
            std::forward<F>(f)
        );
        
        auto future = task->get_future();
        
        {
            std::unique_lock<std::mutex> lock(queueMutex_);
            queue_.emplace([task] { (*task)(); });
        }
        
        condition_.notify_one();
        return future;
    }
    
private:
    size_t calculateOptimalThreadCount() {
        auto cores = std::thread::hardware_concurrency();
        
        // For I/O bound tasks (search), use more threads
        // For CPU bound tasks (indexing), use fewer threads
        return std::max(2u, cores * 2);
    }
    
    void workerLoop() {
        while (running_) {
            std::function<void()> task;
            
            {
                std::unique_lock<std::mutex> lock(queueMutex_);
                condition_.wait(lock, [this] { return !queue_.empty() || !running_; });
                
                if (!running_) break;
                
                task = std::move(queue_.front());
                queue_.pop();
            }
            
            task();
        }
    }
    
    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> queue_;
    std::mutex queueMutex_;
    std::condition_variable condition_;
    std::atomic<bool> running_{true};
};
```

### Lock-Free Data Structures

Use lock-free structures where possible:

```cpp
// Lock-free statistics counter
class LockFreeCounter {
public:
    void increment() {
        count_.fetch_add(1, std::memory_order_relaxed);
    }
    
    void add(size_t value) {
        count_.fetch_add(value, std::memory_order_relaxed);
    }
    
    size_t get() const {
        return count_.load(std::memory_order_relaxed);
    }
    
    void reset() {
        count_.store(0, std::memory_order_relaxed);
    }

private:
    std::atomic<size_t> count_{0};
};

// Lock-free cache statistics
struct LockFreeCacheStats {
    LockFreeCounter hits;
    LockFreeCounter misses;
    LockFreeCounter evictions;
    
    double getHitRate() const {
        auto totalHits = hits.get();
        auto totalMisses = misses.get();
        auto total = totalHits + totalMisses;
        
        return total > 0 ? static_cast<double>(totalHits) / total : 0.0;
    }
};
```

## Monitoring and Profiling

### Performance Metrics Collection

Comprehensive performance monitoring:

```cpp
class PerformanceMonitor {
public:
    struct Metrics {
        // Query metrics
        std::chrono::milliseconds avgQueryTime{0};
        std::chrono::milliseconds maxQueryTime{0};
        size_t queryCount = 0;
        
        // Cache metrics
        double cacheHitRate = 0.0;
        size_t cacheSize = 0;
        
        // System metrics
        double cpuUsage = 0.0;
        size_t memoryUsage = 0;
        
        // Database metrics
        size_t dbConnections = 0;
        std::chrono::milliseconds avgDbTime{0};
    };
    
    void recordQuery(std::chrono::milliseconds duration) {
        std::lock_guard<std::mutex> lock(mutex_);
        
        queryCount_++;
        totalQueryTime_ += duration;
        maxQueryTime_ = std::max(maxQueryTime_, duration);
        
        // Calculate rolling average
        avgQueryTime_ = totalQueryTime_ / queryCount_;
    }
    
    Metrics getMetrics() {
        std::lock_guard<std::mutex> lock(mutex_);
        
        Metrics metrics;
        metrics.avgQueryTime = avgQueryTime_;
        metrics.maxQueryTime = maxQueryTime_;
        metrics.queryCount = queryCount_;
        metrics.cacheHitRate = cache_->getStats().hitRate;
        metrics.cacheSize = cache_->getStats().size;
        metrics.cpuUsage = getCPUUsage();
        metrics.memoryUsage = getMemoryUsage();
        
        return metrics;
    }
    
    void reset() {
        std::lock_guard<std::mutex> lock(mutex_);
        queryCount_ = 0;
        totalQueryTime_ = std::chrono::milliseconds{0};
        avgQueryTime_ = std::chrono::milliseconds{0};
        maxQueryTime_ = std::chrono::milliseconds{0};
    }

private:
    mutable std::mutex mutex_;
    size_t queryCount_ = 0;
    std::chrono::milliseconds totalQueryTime_{0};
    std::chrono::milliseconds avgQueryTime_{0};
    std::chrono::milliseconds maxQueryTime_{0};
    std::shared_ptr<SearchCache> cache_;
};
```

### Real-time Performance Dashboard

Create a performance monitoring dashboard:

```cpp
class PerformanceDashboard {
public:
    void startMonitoring() {
        monitoringThread_ = std::thread([this]() {
            while (running_) {
                auto metrics = monitor_->getMetrics();
                
                // Log metrics
                logMetrics(metrics);
                
                // Check for performance issues
                checkPerformanceThresholds(metrics);
                
                std::this_thread::sleep_for(std::chrono::seconds(30));
            }
        });
    }
    
    void stopMonitoring() {
        running_ = false;
        if (monitoringThread_.joinable()) {
            monitoringThread_.join();
        }
    }

private:
    void logMetrics(const PerformanceMonitor::Metrics& metrics) {
        std::cout << "Performance Metrics:" << std::endl
                  << "  Avg Query Time: " << metrics.avgQueryTime.count() << "ms" << std::endl
                  << "  Cache Hit Rate: " << (metrics.cacheHitRate * 100) << "%" << std::endl
                  << "  Memory Usage: " << (metrics.memoryUsage / 1024 / 1024) << "MB" << std::endl
                  << "  CPU Usage: " << (metrics.cpuUsage * 100) << "%" << std::endl;
    }
    
    void checkPerformanceThresholds(const PerformanceMonitor::Metrics& metrics) {
        // Alert if query time is too high
        if (metrics.avgQueryTime > std::chrono::milliseconds(100)) {
            alertHighLatency(metrics.avgQueryTime);
        }
        
        // Alert if cache hit rate is too low
        if (metrics.cacheHitRate < 0.5) {
            alertLowCacheHitRate(metrics.cacheHitRate);
        }
        
        // Alert if memory usage is too high
        if (metrics.memoryUsage > maxMemoryLimit_) {
            alertHighMemoryUsage(metrics.memoryUsage);
        }
    }
    
    std::unique_ptr<PerformanceMonitor> monitor_;
    std::thread monitoringThread_;
    std::atomic<bool> running_{false};
    size_t maxMemoryLimit_ = 4 * 1024 * 1024 * 1024;  // 4GB
};
```

## Performance Benchmarks

### Automated Benchmarking

Regular performance benchmarks (Google Benchmark based; module-local under src/<module>/benchmarks):

Build:
```bash
cmake -S . -B build -DYAMS_BUILD_PROFILE=dev -DYAMS_BUILD_BENCHMARKS=ON
cmake --build build -j
```

Run a single benchmark:
```bash
./build/src/search/benchmarks/query_parser_bench --benchmark_min_time=0.1
```

Discover and run all benchmarks (Linux/macOS):
```bash
find build -type f \( -name "*_bench" -o -name "*_bench.exe" \) -print0 \
| xargs -0 -I{} sh -c '{} --benchmark_min_time=0.1 --benchmark_format=json --benchmark_out="{}.json" || true'
```

Notes:
- Reusable framework lives at src/benchmarks (target alias yams::benchmarks).
- Benchmark binaries are excluded from coverage (src/**/benchmarks/*).
- CI runs the discovery script above and uploads JSON outputs from build/bench_results/.

```cpp
class PerformanceBenchmark {
public:
    struct BenchmarkResult {
        std::string testName;
        std::chrono::milliseconds avgTime;
        size_t throughput;  // operations per second
        bool passed;
        std::string details;
    };
    
    std::vector<BenchmarkResult> runBenchmarks() {
        std::vector<BenchmarkResult> results;
        
        results.push_back(benchmarkSimpleQueries());
        results.push_back(benchmarkComplexQueries());
        results.push_back(benchmarkConcurrentQueries());
        results.push_back(benchmarkIndexingPerformance());
        results.push_back(benchmarkCachePerformance());
        
        return results;
    }

private:
    BenchmarkResult benchmarkSimpleQueries() {
        const std::vector<std::string> queries = {
            "database", "performance", "optimization", "search", "index"
        };
        
        auto start = std::chrono::high_resolution_clock::now();
        
        for (int i = 0; i < 1000; ++i) {
            for (const auto& query : queries) {
                SearchRequest request;
                request.query = query;
                auto result = executor_->search(request);
            }
        }
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        
        BenchmarkResult result;
        result.testName = "Simple Queries";
        result.avgTime = duration / (1000 * queries.size());
        result.throughput = (1000 * queries.size() * 1000) / duration.count();
        result.passed = result.avgTime < std::chrono::milliseconds(20);
        
        return result;
    }
    
    BenchmarkResult benchmarkConcurrentQueries() {
        const size_t numThreads = 10;
        const size_t queriesPerThread = 100;
        
        auto start = std::chrono::high_resolution_clock::now();
        
        std::vector<std::thread> threads;
        for (size_t i = 0; i < numThreads; ++i) {
            threads.emplace_back([this, queriesPerThread]() {
                for (size_t j = 0; j < queriesPerThread; ++j) {
                    SearchRequest request;
                    request.query = "test query " + std::to_string(j);
                    auto result = executor_->search(request);
                }
            });
        }
        
        for (auto& thread : threads) {
            thread.join();
        }
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        
        BenchmarkResult result;
        result.testName = "Concurrent Queries";
        result.avgTime = duration / (numThreads * queriesPerThread);
        result.throughput = (numThreads * queriesPerThread * 1000) / duration.count();
        result.passed = result.throughput > 500;  // 500 QPS minimum
        
        return result;
    }
    
    std::shared_ptr<SearchExecutor> executor_;
};
```

## Troubleshooting Performance Issues

### Common Performance Problems

1. **Slow Query Performance**
   ```cpp
   // Diagnosis
   void diagnoseSlowQueries() {
       auto slowQueries = queryAnalyzer_->getSlowQueries(std::chrono::milliseconds(100));
       
       for (const auto& query : slowQueries) {
           std::cout << "Slow query: " << query.query 
                     << " avg time: " << query.avgLatency.count() << "ms" << std::endl;
           
           // Analyze query patterns
           if (query.query.find("*") == 0) {
               std::cout << "  Issue: Leading wildcard" << std::endl;
           }
           
           if (query.cacheHitRate < 0.1) {
               std::cout << "  Issue: Low cache hit rate" << std::endl;
           }
       }
   }
   ```

2. **High Memory Usage**
   ```cpp
   void diagnoseMemoryUsage() {
       auto memStats = memoryMonitor_->getMemoryStats();
       
       std::cout << "Memory usage: " << memStats.usedMemory / 1024 / 1024 << "MB" << std::endl;
       std::cout << "Cache memory: " << memStats.cacheMemory / 1024 / 1024 << "MB" << std::endl;
       
       if (memStats.cacheMemory > memStats.usedMemory * 0.5) {
           std::cout << "Recommendation: Reduce cache size" << std::endl;
       }
   }
   ```

3. **Low Cache Hit Rates**
   ```cpp
   void optimizeCacheHitRate() {
       auto cacheStats = cache_->getStats();
       
       if (cacheStats.hitRate < 0.5) {
           // Increase cache size
           auto newConfig = cacheConfig_;
           newConfig.maxEntries *= 2;
           newConfig.maxMemoryMB *= 2;
           
           cache_ = std::make_shared<SearchCache>(newConfig);
           
           // Warm cache with common queries
           cacheWarmer_->warmCache(queryAnalyzer_->getTopQueries(100));
       }
   }
   ```

This performance tuning guide provides comprehensive strategies for optimizing the YAMS search system. Regular monitoring, benchmarking, and tuning based on actual usage patterns will ensure optimal performance for your specific workload.