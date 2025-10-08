/**
 * fts5_performance_stress_test.cpp
 *
 * Large-scale FTS5 performance and stress tests to validate search behavior
 * at production scale (100K+ documents, multi-GB corpus).
 *
 * These tests are OPTIONAL and controlled by environment variables to avoid
 * overwhelming CI systems. Run locally for performance validation.
 *
 * Environment variables:
 *   YAMS_RUN_STRESS_TESTS=1    - Enable stress tests
 *   YAMS_STRESS_DOC_COUNT=N    - Number of documents (default: 50000)
 *   YAMS_STRESS_TIMEOUT_MS=N   - Max time per query (default: 5000ms)
 */

#include <spdlog/spdlog.h>
#include <chrono>
#include <filesystem>
#include <random>
#include <thread>
#include <gtest/gtest.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/metadata_repository.h>

using namespace yams;
using namespace yams::metadata;
using namespace std::chrono;

class FTS5StressTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Skip unless explicitly enabled
        if (!isStressTestEnabled()) {
            GTEST_SKIP() << "Stress tests disabled. Set YAMS_RUN_STRESS_TESTS=1 to enable.";
        }

        auto tempDir = std::filesystem::temp_directory_path() / "yams_fts5_stress";
        std::filesystem::create_directories(tempDir);
        dbPath_ = tempDir / "stress_test.db";
        std::filesystem::remove(dbPath_);

        ConnectionPoolConfig config;
        config.minConnections = 4;
        config.maxConnections = 8;

        pool_ = std::make_unique<ConnectionPool>(dbPath_.string(), config);
        auto initResult = pool_->initialize();
        ASSERT_TRUE(initResult.has_value()) << "Failed to initialize pool";

        repo_ = std::make_unique<MetadataRepository>(*pool_);

        docCount_ = getEnvInt("YAMS_STRESS_DOC_COUNT", 50000);
        timeoutMs_ = getEnvInt("YAMS_STRESS_TIMEOUT_MS", 5000);

        spdlog::info("=== FTS5 Stress Test Configuration ===");
        spdlog::info("Documents: {}", docCount_);
        spdlog::info("Timeout: {}ms", timeoutMs_);
        spdlog::info("Database: {}", dbPath_.string());
    }

    void TearDown() override {
        repo_.reset();
        if (pool_) {
            pool_->shutdown();
        }
        pool_.reset();
        std::filesystem::remove(dbPath_);
    }

    bool isStressTestEnabled() {
        const char* env = std::getenv("YAMS_RUN_STRESS_TESTS");
        return env && (std::string(env) == "1" || std::string(env) == "true");
    }

    int getEnvInt(const char* name, int defaultValue) {
        const char* env = std::getenv(name);
        if (!env)
            return defaultValue;
        try {
            return std::stoi(env);
        } catch (...) {
            return defaultValue;
        }
    }

    void populateLargeCorpus(size_t count) {
        spdlog::info("Populating {} documents...", count);
        auto start = high_resolution_clock::now();

        std::vector<std::string> words = {
            "ServiceManager", "IndexingPipeline", "ContentStore", "MetadataRepository",
            "SearchEngine",   "VectorIndex",      "HybridSearch", "FTS5",
            "SQLite",         "database",         "performance",  "optimization",
            "benchmark",      "stress",           "test",         "document",
            "content",        "search",           "query",        "result",
            "score",          "ranking",          "relevance"};

        std::mt19937 rng(42); // Deterministic seed
        std::uniform_int_distribution<> wordDist(0, words.size() - 1);
        std::uniform_int_distribution<> wordCountDist(10, 100);

        for (size_t i = 0; i < count; ++i) {
            DocumentInfo doc;
            doc.sha256Hash =
                "hash_" + std::to_string(i) + "_" + std::to_string(std::hash<size_t>{}(i));
            doc.fileName = "doc_" + std::to_string(i) + ".txt";
            doc.filePath = "path/to/" + doc.fileName;
            doc.fileSize = 1024 + (i % 10000);
            doc.mimeType = "text/plain";
            doc.createdTime = system_clock::now();
            doc.modifiedTime = doc.createdTime;
            doc.indexedTime = doc.createdTime;
            doc.contentExtracted = true;
            doc.extractionStatus = ExtractionStatus::Success;

            // Generate realistic content
            std::string content;
            int numWords = wordCountDist(rng);
            for (int w = 0; w < numWords; ++w) {
                if (w > 0)
                    content += " ";
                content += words[wordDist(rng)];
            }

            auto insertResult = repo_->insertDocument(doc);
            ASSERT_TRUE(insertResult.has_value())
                << "Failed to insert document " << i << ": " << insertResult.error().message;

            if (insertResult.has_value() && !content.empty()) {
                auto indexResult =
                    repo_->indexDocumentContent(insertResult.value(), doc.fileName, content);
                if (!indexResult.has_value()) {
                    spdlog::warn("Failed to index content for doc {}: {}", i,
                                 indexResult.error().message);
                }
            }

            // Progress indicator
            if ((i + 1) % 10000 == 0) {
                auto elapsed = duration_cast<seconds>(high_resolution_clock::now() - start);
                spdlog::info("  Progress: {}/{} ({:.1f}%) - {}s elapsed", i + 1, count,
                             (i + 1) * 100.0 / count, elapsed.count());
            }
        }

        auto elapsed = duration_cast<milliseconds>(high_resolution_clock::now() - start);
        spdlog::info("Populated {} documents in {}ms ({:.2f} docs/sec)", count, elapsed.count(),
                     count * 1000.0 / elapsed.count());
    }

    std::filesystem::path dbPath_;
    std::unique_ptr<ConnectionPool> pool_;
    std::unique_ptr<MetadataRepository> repo_;
    size_t docCount_;
    int timeoutMs_;
};

// Test: Large corpus FTS5 search with COUNT performance
TEST_F(FTS5StressTest, LargeCorpusSearchPerformance) {
    populateLargeCorpus(docCount_);

    // Test queries with different selectivity
    std::vector<std::pair<std::string, std::string>> queries = {
        {"ServiceManager", "common term"},
        {"IndexingPipeline", "medium term"},
        {"optimization benchmark", "multi-word"},
        {"xyz_nonexistent_term", "no results"}};

    for (const auto& [query, desc] : queries) {
        spdlog::info("Testing query: '{}' ({})", query, desc);

        auto start = high_resolution_clock::now();
        auto result = repo_->search(query, 10, 0, std::nullopt);
        auto elapsed = duration_cast<milliseconds>(high_resolution_clock::now() - start);

        ASSERT_TRUE(result.has_value()) << "Query failed: " << result.error().message;

        const auto& searchResults = result.value();

        spdlog::info("  Results: {} total, {} returned in {}ms", searchResults.totalCount,
                     searchResults.results.size(), elapsed.count());

        // Performance assertion: should complete within timeout
        EXPECT_LT(elapsed.count(), timeoutMs_) << "Query '" << query << "' took " << elapsed.count()
                                               << "ms, exceeds timeout of " << timeoutMs_ << "ms";

        // Validate results
        EXPECT_LE(searchResults.results.size(), 10);
        if (query == "xyz_nonexistent_term") {
            EXPECT_EQ(searchResults.totalCount, 0);
        }
    }
}

// Test: COUNT query performance (the bottleneck we identified)
TEST_F(FTS5StressTest, CountQueryPerformance) {
    populateLargeCorpus(docCount_);

    // Query that matches many documents
    std::string broadQuery = "search";

    spdlog::info("Testing COUNT performance for broad query: '{}'", broadQuery);

    auto start = high_resolution_clock::now();
    auto result = repo_->search(broadQuery, 10, 0, std::nullopt);
    auto elapsed = duration_cast<milliseconds>(high_resolution_clock::now() - start);

    ASSERT_TRUE(result.has_value()) << "Query failed";

    spdlog::info("  Count query completed in {}ms, found {} total matches", elapsed.count(),
                 result.value().totalCount);

    // With our optimization, COUNT should be fast even with many matches
    EXPECT_LT(elapsed.count(), 1000) << "COUNT query took " << elapsed.count()
                                     << "ms - should be < 1s with bounded count optimization";
}

// Test: Concurrent search stress
TEST_F(FTS5StressTest, ConcurrentSearchStress) {
    populateLargeCorpus(std::min(docCount_, size_t(10000))); // Smaller for concurrent test

    const int numThreads = 8;
    const int queriesPerThread = 20;

    std::vector<std::thread> threads;
    std::atomic<int> successCount{0};
    std::atomic<int> failureCount{0};
    std::atomic<int64_t> totalTimeMs{0};

    spdlog::info("Running {} concurrent threads, {} queries each", numThreads, queriesPerThread);

    auto start = high_resolution_clock::now();

    for (int t = 0; t < numThreads; ++t) {
        threads.emplace_back([&, t]() {
            std::mt19937 rng(t);
            std::uniform_int_distribution<> dist(0, 2);
            std::vector<std::string> queries = {"ServiceManager", "search", "optimization"};

            for (int q = 0; q < queriesPerThread; ++q) {
                auto queryStart = high_resolution_clock::now();
                auto result = repo_->search(queries[dist(rng)], 10, 0, std::nullopt);
                auto queryElapsed =
                    duration_cast<milliseconds>(high_resolution_clock::now() - queryStart);

                if (result.has_value()) {
                    successCount++;
                    totalTimeMs += queryElapsed.count();
                } else {
                    failureCount++;
                    spdlog::warn("Query failed in thread {}: {}", t, result.error().message);
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    auto elapsed = duration_cast<milliseconds>(high_resolution_clock::now() - start);
    int totalQueries = numThreads * queriesPerThread;
    double avgTimeMs = totalTimeMs.load() / static_cast<double>(successCount.load());

    spdlog::info("Concurrent test results:");
    spdlog::info("  Total time: {}ms", elapsed.count());
    spdlog::info("  Success: {}/{}", successCount.load(), totalQueries);
    spdlog::info("  Failures: {}", failureCount.load());
    spdlog::info("  Avg query time: {:.2f}ms", avgTimeMs);
    spdlog::info("  Throughput: {:.2f} queries/sec", totalQueries * 1000.0 / elapsed.count());

    EXPECT_EQ(failureCount.load(), 0) << "Some queries failed";
    EXPECT_GT(successCount.load(), totalQueries * 0.95) << "Less than 95% success rate";
}

// Test: Path resolution at scale (for cat/get commands)
TEST_F(FTS5StressTest, PathResolutionAtScale) {
    populateLargeCorpus(std::min(docCount_, size_t(10000)));

    // Test various path patterns
    std::vector<std::string> pathPatterns = {
        "doc_1234.txt",         // Exact filename
        "path/to/doc_1234.txt", // Relative path
        "%/doc_1234.txt",       // Wildcard prefix
        "doc_%",                // Wildcard suffix
    };

    for (const auto& pattern : pathPatterns) {
        spdlog::info("Testing path pattern: '{}'", pattern);

        auto start = high_resolution_clock::now();
        auto result = repo_->queryDocumentsByPattern(pattern, 100);
        auto elapsed = duration_cast<milliseconds>(high_resolution_clock::now() - start);

        ASSERT_TRUE(result.has_value()) << "Pattern query failed";

        spdlog::info("  Found {} matches in {}ms", result.value().size(), elapsed.count());

        EXPECT_LT(elapsed.count(), 1000)
            << "Path resolution took too long: " << elapsed.count() << "ms";
    }
}

// Test: Memory usage with large result sets
TEST_F(FTS5StressTest, LargeResultSetMemory) {
    populateLargeCorpus(std::min(docCount_, size_t(20000)));

    // Query for many results
    auto start = high_resolution_clock::now();
    auto result = repo_->search("search", 10000, 0, std::nullopt);
    auto elapsed = duration_cast<milliseconds>(high_resolution_clock::now() - start);

    ASSERT_TRUE(result.has_value()) << "Query failed";

    spdlog::info("Large result set: {} results in {}ms", result.value().results.size(),
                 elapsed.count());

    // With our limit of 10K, should not crash or take too long
    EXPECT_LE(result.value().results.size(), 10000);
    EXPECT_LT(elapsed.count(), 5000) << "Large result query took " << elapsed.count() << "ms";
}
