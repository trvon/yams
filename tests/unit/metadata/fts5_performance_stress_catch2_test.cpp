// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Migrated from GTest: fts5_performance_stress_test.cpp
// Large-scale FTS5 performance and stress tests.
// Opt-in via YAMS_RUN_STRESS_TESTS=1.

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <spdlog/spdlog.h>

#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/metadata_repository.h>

using namespace yams;
using namespace yams::metadata;
using namespace std::chrono;

namespace {

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

struct FTS5StressFixture {
    FTS5StressFixture() {
        auto tempDir = std::filesystem::temp_directory_path() / "yams_fts5_stress_catch2";
        std::filesystem::create_directories(tempDir);
        dbPath = tempDir / "stress_test.db";
        std::filesystem::remove(dbPath);

        ConnectionPoolConfig config;
        config.minConnections = 4;
        config.maxConnections = 8;
        pool = std::make_unique<ConnectionPool>(dbPath.string(), config);
        auto initResult = pool->initialize();
        REQUIRE(initResult.has_value());

        repo = std::make_unique<MetadataRepository>(*pool);

        docCount = static_cast<size_t>(getEnvInt("YAMS_STRESS_DOC_COUNT", 50000));
        timeoutMs = getEnvInt("YAMS_STRESS_TIMEOUT_MS", 5000);

        spdlog::info("=== FTS5 Stress Test Configuration ===");
        spdlog::info("Documents: {}", docCount);
        spdlog::info("Timeout: {}ms", timeoutMs);
        spdlog::info("Database: {}", dbPath.string());
    }

    ~FTS5StressFixture() {
        repo.reset();
        if (pool)
            pool->shutdown();
        pool.reset();
        std::filesystem::remove(dbPath);
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

        std::mt19937 rng(42);
        std::uniform_int_distribution<> wordDist(0, static_cast<int>(words.size()) - 1);
        std::uniform_int_distribution<> wordCountDist(10, 100);

        for (size_t i = 0; i < count; ++i) {
            DocumentInfo doc;
            doc.sha256Hash =
                "hash_" + std::to_string(i) + "_" + std::to_string(std::hash<size_t>{}(i));
            doc.fileName = "doc_" + std::to_string(i) + ".txt";
            doc.filePath = "path/to/" + doc.fileName;
            doc.fileSize = 1024 + (i % 10000);
            doc.mimeType = "text/plain";
            auto now = std::chrono::time_point_cast<std::chrono::seconds>(system_clock::now());
            doc.createdTime = now;
            doc.modifiedTime = now;
            doc.indexedTime = now;
            doc.contentExtracted = true;
            doc.extractionStatus = ExtractionStatus::Success;

            std::string content;
            int numWords = wordCountDist(rng);
            for (int w = 0; w < numWords; ++w) {
                if (w > 0)
                    content += " ";
                content += words[wordDist(rng)];
            }

            auto insertResult = repo->insertDocument(doc);
            REQUIRE(insertResult.has_value());

            if (insertResult.has_value() && !content.empty()) {
                auto indexResult = repo->indexDocumentContent(insertResult.value(), doc.fileName,
                                                              content, "text/plain");
                if (!indexResult.has_value()) {
                    spdlog::warn("Failed to index content for doc {}: {}", i,
                                 indexResult.error().message);
                }
            }

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

    std::filesystem::path dbPath;
    std::unique_ptr<ConnectionPool> pool;
    std::unique_ptr<MetadataRepository> repo;
    size_t docCount;
    int timeoutMs;
};

} // namespace

TEST_CASE("FTS5 Stress: large corpus search performance", "[stress][metadata][fts5]") {
    if (!isStressTestEnabled())
        SKIP("Stress tests disabled. Set YAMS_RUN_STRESS_TESTS=1 to enable.");

    FTS5StressFixture f;
    f.populateLargeCorpus(f.docCount);

    std::vector<std::pair<std::string, std::string>> queries = {
        {"ServiceManager", "common term"},
        {"IndexingPipeline", "medium term"},
        {"optimization benchmark", "multi-word"},
        {"xyz_nonexistent_term", "no results"}};

    for (const auto& [query, desc] : queries) {
        spdlog::info("Testing query: '{}' ({})", query, desc);

        auto start = high_resolution_clock::now();
        auto result = f.repo->search(query, 10, 0, std::nullopt);
        auto elapsed = duration_cast<milliseconds>(high_resolution_clock::now() - start);

        REQUIRE(result.has_value());
        const auto& searchResults = result.value();

        spdlog::info("  Results: {} total, {} returned in {}ms", searchResults.totalCount,
                     searchResults.results.size(), elapsed.count());

        CHECK(elapsed.count() < f.timeoutMs);
        CHECK(searchResults.results.size() <= 10);
        if (query == "xyz_nonexistent_term") {
            CHECK(searchResults.totalCount == 0);
        }
    }
}

TEST_CASE("FTS5 Stress: COUNT query performance", "[stress][metadata][fts5]") {
    if (!isStressTestEnabled())
        SKIP("Stress tests disabled. Set YAMS_RUN_STRESS_TESTS=1 to enable.");

    FTS5StressFixture f;
    f.populateLargeCorpus(f.docCount);

    std::string broadQuery = "search";
    spdlog::info("Testing COUNT performance for broad query: '{}'", broadQuery);

    auto start = high_resolution_clock::now();
    auto result = f.repo->search(broadQuery, 10, 0, std::nullopt);
    auto elapsed = duration_cast<milliseconds>(high_resolution_clock::now() - start);

    REQUIRE(result.has_value());
    spdlog::info("  Count query completed in {}ms, found {} total matches", elapsed.count(),
                 result.value().totalCount);
    CHECK(elapsed.count() < 1000);
}

TEST_CASE("FTS5 Stress: concurrent search", "[stress][metadata][fts5]") {
    if (!isStressTestEnabled())
        SKIP("Stress tests disabled. Set YAMS_RUN_STRESS_TESTS=1 to enable.");

    FTS5StressFixture f;
    f.populateLargeCorpus(std::min(f.docCount, size_t(10000)));

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
                auto result = f.repo->search(queries[dist(rng)], 10, 0, std::nullopt);
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

    for (auto& thread : threads)
        thread.join();

    auto elapsed = duration_cast<milliseconds>(high_resolution_clock::now() - start);
    int totalQueries = numThreads * queriesPerThread;
    double avgTimeMs = totalTimeMs.load() / static_cast<double>(successCount.load());

    spdlog::info("Concurrent test results:");
    spdlog::info("  Total time: {}ms", elapsed.count());
    spdlog::info("  Success: {}/{}", successCount.load(), totalQueries);
    spdlog::info("  Failures: {}", failureCount.load());
    spdlog::info("  Avg query time: {:.2f}ms", avgTimeMs);
    spdlog::info("  Throughput: {:.2f} queries/sec", totalQueries * 1000.0 / elapsed.count());

    CHECK(failureCount.load() == 0);
    CHECK(successCount.load() > totalQueries * 0.95);
}

TEST_CASE("FTS5 Stress: path resolution at scale", "[stress][metadata][fts5]") {
    if (!isStressTestEnabled())
        SKIP("Stress tests disabled. Set YAMS_RUN_STRESS_TESTS=1 to enable.");

    FTS5StressFixture f;
    f.populateLargeCorpus(std::min(f.docCount, size_t(10000)));

    std::vector<std::string> pathPatterns = {
        "doc_1234.txt",
        "path/to/doc_1234.txt",
        "%/doc_1234.txt",
        "doc_%",
    };

    for (const auto& pattern : pathPatterns) {
        spdlog::info("Testing path pattern: '{}'", pattern);

        auto start = high_resolution_clock::now();
        metadata::DocumentQueryOptions opts{};
        opts.likePattern = pattern;
        auto result = f.repo->queryDocuments(opts);
        auto elapsed = duration_cast<milliseconds>(high_resolution_clock::now() - start);

        REQUIRE(result.has_value());
        spdlog::info("  Found {} matches in {}ms", result.value().size(), elapsed.count());
        CHECK(elapsed.count() < 1000);
    }
}

TEST_CASE("FTS5 Stress: large result set memory", "[stress][metadata][fts5]") {
    if (!isStressTestEnabled())
        SKIP("Stress tests disabled. Set YAMS_RUN_STRESS_TESTS=1 to enable.");

    FTS5StressFixture f;
    f.populateLargeCorpus(std::min(f.docCount, size_t(20000)));

    auto start = high_resolution_clock::now();
    auto result = f.repo->search("search", 10000, 0, std::nullopt);
    auto elapsed = duration_cast<milliseconds>(high_resolution_clock::now() - start);

    REQUIRE(result.has_value());
    spdlog::info("Large result set: {} results in {}ms", result.value().results.size(),
                 elapsed.count());
    CHECK(result.value().results.size() <= 10000);
    CHECK(elapsed.count() < 5000);
}
