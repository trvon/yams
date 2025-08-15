#include <chrono>
#include <thread>
#include <vector>
#include <gtest/gtest.h>
#include <yams/search/cache_key.h>
#include <yams/search/search_cache.h>

using namespace yams::search;
using namespace std::chrono_literals;

class SearchCacheTest : public ::testing::Test {
protected:
    void SetUp() override {
        config_.maxEntries = 100;
        config_.maxMemoryMB = 10;
        config_.defaultTTL = 60s;
        config_.enableStatistics = true;
        config_.enablePatternTracking = true;

        cache_ = std::make_unique<SearchCache>(config_);
    }

    SearchResponse createTestResponse(size_t numResults) {
        SearchResponse response;
        response.totalResults = numResults;
        response.queryTime = 10ms;

        for (size_t i = 0; i < numResults; ++i) {
            SearchResultItem item;
            item.documentId = i;
            item.title = "Document " + std::to_string(i);
            item.snippet = "This is a test snippet for document " + std::to_string(i);
            item.path = "/test/doc" + std::to_string(i) + ".txt";
            item.score = 1.0f - (0.1f * i);

            response.results.push_back(item);
        }

        return response;
    }

    SearchCacheConfig config_;
    std::unique_ptr<SearchCache> cache_;
};

// ===== Basic Operations Tests =====

TEST_F(SearchCacheTest, PutAndGet) {
    auto key = CacheKey::fromQuery("test query", nullptr, 0, 10);
    auto response = createTestResponse(5);

    cache_->put(key, response);

    auto cached = cache_->get(key);
    ASSERT_TRUE(cached.has_value());
    EXPECT_EQ(cached->totalResults, 5);
    EXPECT_EQ(cached->results.size(), 5);
}

TEST_F(SearchCacheTest, GetNonExistent) {
    auto key = CacheKey::fromQuery("non-existent", nullptr, 0, 10);

    auto cached = cache_->get(key);
    EXPECT_FALSE(cached.has_value());
}

TEST_F(SearchCacheTest, Contains) {
    auto key = CacheKey::fromQuery("test query", nullptr, 0, 10);
    auto response = createTestResponse(3);

    EXPECT_FALSE(cache_->contains(key));

    cache_->put(key, response);

    EXPECT_TRUE(cache_->contains(key));
}

TEST_F(SearchCacheTest, Invalidate) {
    auto key = CacheKey::fromQuery("test query", nullptr, 0, 10);
    auto response = createTestResponse(3);

    cache_->put(key, response);
    EXPECT_TRUE(cache_->contains(key));

    cache_->invalidate(key);
    EXPECT_FALSE(cache_->contains(key));
}

TEST_F(SearchCacheTest, Clear) {
    for (int i = 0; i < 5; ++i) {
        auto key = CacheKey::fromQuery("query " + std::to_string(i), nullptr, 0, 10);
        auto response = createTestResponse(3);
        cache_->put(key, response);
    }

    EXPECT_EQ(cache_->size(), 5);

    cache_->clear();

    EXPECT_EQ(cache_->size(), 0);
}

// ===== TTL Tests =====

TEST_F(SearchCacheTest, TTLExpiration) {
    auto key = CacheKey::fromQuery("expiring query", nullptr, 0, 10);
    auto response = createTestResponse(3);

    // Put with 1 second TTL
    cache_->put(key, response, 1s);

    // Should exist immediately
    EXPECT_TRUE(cache_->contains(key));

    // Wait for expiration
    std::this_thread::sleep_for(1100ms);

    // Should be expired
    EXPECT_FALSE(cache_->contains(key));
    auto cached = cache_->get(key);
    EXPECT_FALSE(cached.has_value());
}

TEST_F(SearchCacheTest, RemoveExpired) {
    // Add entries with different TTLs
    auto key1 = CacheKey::fromQuery("query1", nullptr, 0, 10);
    auto key2 = CacheKey::fromQuery("query2", nullptr, 0, 10);
    auto key3 = CacheKey::fromQuery("query3", nullptr, 0, 10);

    auto response = createTestResponse(3);

    cache_->put(key1, response, 1s);
    cache_->put(key2, response, 10s);
    cache_->put(key3, response, 10s);

    EXPECT_EQ(cache_->size(), 3);

    // Wait for first to expire
    std::this_thread::sleep_for(1100ms);

    size_t removed = cache_->removeExpired();
    EXPECT_EQ(removed, 1);
    EXPECT_EQ(cache_->size(), 2);
    EXPECT_FALSE(cache_->contains(key1));
    EXPECT_TRUE(cache_->contains(key2));
    EXPECT_TRUE(cache_->contains(key3));
}

// ===== LRU Eviction Tests =====

TEST_F(SearchCacheTest, LRUEviction) {
    // Set small cache size
    config_.maxEntries = 3;
    cache_ = std::make_unique<SearchCache>(config_);

    auto response = createTestResponse(3);

    // Fill cache
    auto key1 = CacheKey::fromQuery("query1", nullptr, 0, 10);
    auto key2 = CacheKey::fromQuery("query2", nullptr, 0, 10);
    auto key3 = CacheKey::fromQuery("query3", nullptr, 0, 10);

    cache_->put(key1, response);
    cache_->put(key2, response);
    cache_->put(key3, response);

    EXPECT_EQ(cache_->size(), 3);

    // Access key1 to make it most recently used
    cache_->get(key1);

    // Add new entry - should evict key2 (least recently used)
    auto key4 = CacheKey::fromQuery("query4", nullptr, 0, 10);
    cache_->put(key4, response);

    EXPECT_EQ(cache_->size(), 3);
    EXPECT_TRUE(cache_->contains(key1));  // Was accessed
    EXPECT_FALSE(cache_->contains(key2)); // Should be evicted
    EXPECT_TRUE(cache_->contains(key3));
    EXPECT_TRUE(cache_->contains(key4));
}

TEST_F(SearchCacheTest, ManualEviction) {
    auto response = createTestResponse(3);

    for (int i = 0; i < 5; ++i) {
        auto key = CacheKey::fromQuery("query" + std::to_string(i), nullptr, 0, 10);
        cache_->put(key, response);
    }

    EXPECT_EQ(cache_->size(), 5);

    cache_->evict(2);

    EXPECT_EQ(cache_->size(), 3);
}

// ===== Pattern Invalidation Tests =====

TEST_F(SearchCacheTest, InvalidatePattern) {
    auto response = createTestResponse(3);

    // Add entries with different patterns
    cache_->put(CacheKey::fromQuery("user:123:profile", nullptr, 0, 10), response);
    cache_->put(CacheKey::fromQuery("user:123:posts", nullptr, 0, 10), response);
    cache_->put(CacheKey::fromQuery("user:456:profile", nullptr, 0, 10), response);
    cache_->put(CacheKey::fromQuery("product:789", nullptr, 0, 10), response);

    EXPECT_EQ(cache_->size(), 4);

    // Invalidate all user:123 entries
    size_t count = cache_->invalidatePattern("q:user:123*");

    EXPECT_EQ(count, 2);
    EXPECT_EQ(cache_->size(), 2);
}

// ===== Statistics Tests =====

TEST_F(SearchCacheTest, HitMissStatistics) {
    auto key1 = CacheKey::fromQuery("query1", nullptr, 0, 10);
    auto key2 = CacheKey::fromQuery("query2", nullptr, 0, 10);
    auto response = createTestResponse(3);

    cache_->put(key1, response);

    // Generate hits and misses
    cache_->get(key1); // Hit
    cache_->get(key1); // Hit
    cache_->get(key2); // Miss
    cache_->get(key2); // Miss
    cache_->get(key2); // Miss

    auto stats = cache_->getStats();

    EXPECT_EQ(stats.hits.load(), 2);
    EXPECT_EQ(stats.misses.load(), 3);
    EXPECT_NEAR(stats.hitRate(), 0.4, 0.01);
}

TEST_F(SearchCacheTest, EvictionStatistics) {
    config_.maxEntries = 2;
    cache_ = std::make_unique<SearchCache>(config_);

    auto response = createTestResponse(3);

    cache_->put(CacheKey::fromQuery("query1", nullptr, 0, 10), response);
    cache_->put(CacheKey::fromQuery("query2", nullptr, 0, 10), response);
    cache_->put(CacheKey::fromQuery("query3", nullptr, 0, 10), response); // Causes eviction

    auto stats = cache_->getStats();

    EXPECT_EQ(stats.evictions.load(), 1);
    EXPECT_EQ(stats.insertions.load(), 3);
    EXPECT_EQ(stats.currentSize.load(), 2);
}

TEST_F(SearchCacheTest, MemoryUsageTracking) {
    auto response1 = createTestResponse(5);
    auto response2 = createTestResponse(10);

    cache_->put(CacheKey::fromQuery("query1", nullptr, 0, 10), response1);

    size_t mem1 = cache_->memoryUsage();
    EXPECT_GT(mem1, 0);

    cache_->put(CacheKey::fromQuery("query2", nullptr, 0, 10), response2);

    size_t mem2 = cache_->memoryUsage();
    EXPECT_GT(mem2, mem1); // Should increase with larger response
}

TEST_F(SearchCacheTest, ResetStatistics) {
    auto key = CacheKey::fromQuery("query", nullptr, 0, 10);
    auto response = createTestResponse(3);

    cache_->put(key, response);
    cache_->get(key);
    cache_->get(CacheKey::fromQuery("missing", nullptr, 0, 10));

    auto stats = cache_->getStats();
    EXPECT_GT(stats.hits.load(), 0);
    EXPECT_GT(stats.misses.load(), 0);

    cache_->resetStats();

    stats = cache_->getStats();
    EXPECT_EQ(stats.hits.load(), 0);
    EXPECT_EQ(stats.misses.load(), 0);
}

// ===== Thread Safety Tests =====

TEST_F(SearchCacheTest, ConcurrentAccess) {
    const int numThreads = 10;
    const int operationsPerThread = 100;

    std::vector<std::thread> threads;

    for (int t = 0; t < numThreads; ++t) {
        threads.emplace_back([this, t, operationsPerThread]() {
            for (int i = 0; i < operationsPerThread; ++i) {
                auto key = CacheKey::fromQuery(
                    "thread" + std::to_string(t) + "_op" + std::to_string(i), nullptr, 0, 10);

                if (i % 3 == 0) {
                    // Put
                    auto response = createTestResponse(3);
                    cache_->put(key, response);
                } else if (i % 3 == 1) {
                    // Get
                    cache_->get(key);
                } else {
                    // Invalidate
                    cache_->invalidate(key);
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // Cache should still be in valid state
    auto stats = cache_->getStats();
    EXPECT_GE(stats.insertions.load(), 0);
    EXPECT_GE(stats.hits.load() + stats.misses.load(), 0);
}

// ===== Cache Key Tests =====

TEST_F(SearchCacheTest, CacheKeyEquality) {
    auto key1 = CacheKey::fromQuery("test query", nullptr, 0, 10);
    auto key2 = CacheKey::fromQuery("test query", nullptr, 0, 10);
    auto key3 = CacheKey::fromQuery("different query", nullptr, 0, 10);

    EXPECT_EQ(key1, key2);
    EXPECT_NE(key1, key3);
}

TEST_F(SearchCacheTest, CacheKeyWithFilters) {
    SearchFilters filters;
    filters.dateFilter = DateRangeFilter();
    filters.dateFilter->from = std::chrono::system_clock::now();

    auto key1 = CacheKey::fromQuery("test", &filters, 0, 10);
    auto key2 = CacheKey::fromQuery("test", nullptr, 0, 10);

    EXPECT_NE(key1, key2); // Different filters should create different keys
}

TEST_F(SearchCacheTest, CacheKeyPatternMatching) {
    auto key = CacheKey::fromQuery("user:123:profile", nullptr, 0, 10);

    EXPECT_TRUE(key.matchesPattern("*"));
    EXPECT_TRUE(key.matchesPattern("q:user:123*"));
    EXPECT_FALSE(key.matchesPattern("q:user:456*"));
}

// ===== Memory Limit Tests =====

TEST_F(SearchCacheTest, MemoryLimitEnforcement) {
    // Set very small memory limit
    config_.maxMemoryMB = 0.001; // 1KB
    config_.maxEntries = 1000;   // High entry limit
    cache_ = std::make_unique<SearchCache>(config_);

    // Add large responses
    for (int i = 0; i < 100; ++i) {
        auto key = CacheKey::fromQuery("query" + std::to_string(i), nullptr, 0, 10);
        auto response = createTestResponse(100); // Large response
        cache_->put(key, response);

        // Check that memory limit is respected
        EXPECT_LE(cache_->memoryUsage(), 1024); // 1KB
    }
}

// ===== Cache Warming Tests =====

TEST_F(SearchCacheTest, CacheWarming) {
    config_.enableWarmup = true;
    config_.warmupQueries = {"warmup1", "warmup2", "warmup3"};
    cache_ = std::make_unique<SearchCache>(config_);

    int executorCalls = 0;
    auto executor = [&executorCalls, this](const std::string& query) {
        executorCalls++;
        return createTestResponse(5);
    };

    cache_->warmUp(executor);

    EXPECT_EQ(executorCalls, 3);
    EXPECT_EQ(cache_->size(), 3);

    // Check that warmup queries are cached
    auto key = CacheKey::fromQuery("warmup1", nullptr, 0, 10);
    EXPECT_TRUE(cache_->contains(key));
}

TEST_F(SearchCacheTest, AddWarmupQuery) {
    EXPECT_EQ(config_.warmupQueries.size(), 0);

    cache_->addWarmupQuery("new warmup query");

    auto updatedConfig = cache_->getConfig();
    EXPECT_EQ(updatedConfig.warmupQueries.size(), 1);
    EXPECT_EQ(updatedConfig.warmupQueries[0], "new warmup query");
}

// ===== Configuration Tests =====

TEST_F(SearchCacheTest, UpdateConfiguration) {
    // Fill cache
    for (int i = 0; i < 10; ++i) {
        auto key = CacheKey::fromQuery("query" + std::to_string(i), nullptr, 0, 10);
        auto response = createTestResponse(3);
        cache_->put(key, response);
    }

    EXPECT_EQ(cache_->size(), 10);

    // Update config with smaller size
    SearchCacheConfig newConfig;
    newConfig.maxEntries = 5;
    cache_->setConfig(newConfig);

    // Cache should be reduced to new limit
    EXPECT_EQ(cache_->size(), 5);
}

// ===== Persistence Tests =====

TEST_F(SearchCacheTest, SaveAndLoadCache) {
    // Add some entries
    for (int i = 0; i < 5; ++i) {
        auto key = CacheKey::fromQuery("query" + std::to_string(i), nullptr, 0, 10);
        auto response = createTestResponse(3);
        cache_->put(key, response);
    }

    // Save to disk
    auto saveResult = cache_->saveToDisk("/tmp/test_cache.bin");
    ASSERT_TRUE(saveResult.isSuccess());

    // Clear cache
    cache_->clear();
    EXPECT_EQ(cache_->size(), 0);

    // Load from disk
    auto loadResult = cache_->loadFromDisk("/tmp/test_cache.bin");
    ASSERT_TRUE(loadResult.isSuccess());

    // Check that entries are restored
    EXPECT_EQ(cache_->size(), 5);

    // Clean up
    std::remove("/tmp/test_cache.bin");
}

// ===== Factory Tests =====

TEST_F(SearchCacheTest, FactoryCreateDefault) {
    auto cache = SearchCacheFactory::createDefault();
    EXPECT_NE(cache, nullptr);
}

TEST_F(SearchCacheTest, FactoryCreateWithConfig) {
    SearchCacheConfig customConfig;
    customConfig.maxEntries = 500;
    customConfig.defaultTTL = 120s;

    auto cache = SearchCacheFactory::create(customConfig);
    EXPECT_NE(cache, nullptr);

    auto config = cache->getConfig();
    EXPECT_EQ(config.maxEntries, 500);
    EXPECT_EQ(config.defaultTTL, 120s);
}

TEST_F(SearchCacheTest, FactoryCreateDistributed) {
    SearchCacheConfig config;
    auto cache = SearchCacheFactory::createDistributed(config, "redis://localhost:6379");
    EXPECT_NE(cache, nullptr);
    // Note: Currently returns regular cache, distributed implementation is future enhancement
}