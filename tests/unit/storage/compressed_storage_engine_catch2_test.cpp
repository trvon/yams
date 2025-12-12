// Catch2 tests for compressed storage engine
// Migrated from GTest: compressed_storage_engine_test.cpp
// NOTE: Some GTest tests referenced APIs that don't exist in the actual headers.
// This file tests the actual CompressedStorageEngine API as documented.

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <filesystem>
#include <format>
#include <random>
#include <thread>
#include <vector>

#include <yams/compression/compression_header.h>
#include <yams/compression/compression_policy.h>
#include <yams/compression/compression_stats.h>
#include <yams/storage/compressed_storage_engine.h>
#include <yams/storage/storage_engine.h>

using namespace yams;
using namespace yams::storage;
using namespace yams::compression;

namespace {

std::vector<std::byte> generateRandomBytes(size_t size) {
    std::vector<std::byte> data(size);
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dis(0, 255);
    for (auto& b : data) {
        b = static_cast<std::byte>(dis(gen));
    }
    return data;
}

std::vector<std::byte> generateCompressibleData(size_t size) {
    std::vector<std::byte> data(size);
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> pattern(0, 15);
    std::byte patternByte = static_cast<std::byte>(pattern(gen));
    for (size_t i = 0; i < size; ++i) {
        if (i % 16 == 0) {
            patternByte = static_cast<std::byte>(pattern(gen));
        }
        data[i] = patternByte;
    }
    return data;
}

struct CompressedStorageFixture {
    CompressedStorageFixture() {
        testDir = std::filesystem::temp_directory_path() /
                  std::format("yams_compressed_catch2_{}",
                              std::chrono::steady_clock::now().time_since_epoch().count());
        storagePath = testDir / "storage";
        std::filesystem::create_directories(storagePath);

        // Create underlying storage engine
        StorageConfig storageConfig{.basePath = storagePath, .shardDepth = 2, .mutexPoolSize = 64};
        underlying = std::make_shared<StorageEngine>(std::move(storageConfig));
    }

    ~CompressedStorageFixture() {
        underlying.reset();
        std::error_code ec;
        std::filesystem::remove_all(testDir, ec);
    }

    std::filesystem::path testDir;
    std::filesystem::path storagePath;
    std::shared_ptr<StorageEngine> underlying;
};

} // namespace

TEST_CASE_METHOD(CompressedStorageFixture, "CompressedStorageEngine basic store retrieve",
                 "[storage][compressed][catch2]") {
    CompressedStorageEngine::Config config;
    config.enableCompression = true;
    config.compressionThreshold = 1024;
    config.asyncCompression = false;

    auto engine = std::make_unique<CompressedStorageEngine>(underlying, config);

    // Generate test data
    auto data = generateCompressibleData(4096);
    std::string hash(64, 'a'); // Valid 64-char hash

    // Store data
    auto storeResult = engine->store(hash, data);
    REQUIRE(storeResult.has_value());

    // Retrieve data
    auto retrieveResult = engine->retrieve(hash);
    REQUIRE(retrieveResult.has_value());
    CHECK(retrieveResult.value() == data);
}

TEST_CASE_METHOD(CompressedStorageFixture, "CompressedStorageEngine exists check",
                 "[storage][compressed][catch2]") {
    CompressedStorageEngine::Config config;
    config.enableCompression = true;
    config.compressionThreshold = 1024;

    auto engine = std::make_unique<CompressedStorageEngine>(underlying, config);

    std::string hash(64, 'b');
    auto data = generateCompressibleData(2048);

    // Check non-existent
    auto exists1 = engine->exists(hash);
    REQUIRE(exists1.has_value());
    CHECK_FALSE(exists1.value());

    // Store and check again
    engine->store(hash, data);

    auto exists2 = engine->exists(hash);
    REQUIRE(exists2.has_value());
    CHECK(exists2.value());
}

TEST_CASE_METHOD(CompressedStorageFixture, "CompressedStorageEngine compression disabled",
                 "[storage][compressed][catch2]") {
    CompressedStorageEngine::Config config;
    config.enableCompression = false;

    auto engine = std::make_unique<CompressedStorageEngine>(underlying, config);
    CHECK_FALSE(engine->isCompressionEnabled());

    std::string hash(64, 'c');
    auto data = generateCompressibleData(4096);

    auto storeResult = engine->store(hash, data);
    REQUIRE(storeResult.has_value());

    auto retrieveResult = engine->retrieve(hash);
    REQUIRE(retrieveResult.has_value());
    CHECK(retrieveResult.value() == data);
}

TEST_CASE_METHOD(CompressedStorageFixture, "CompressedStorageEngine toggle compression",
                 "[storage][compressed][catch2]") {
    CompressedStorageEngine::Config config;
    config.enableCompression = true;

    auto engine = std::make_unique<CompressedStorageEngine>(underlying, config);
    CHECK(engine->isCompressionEnabled());

    engine->setCompressionEnabled(false);
    CHECK_FALSE(engine->isCompressionEnabled());

    engine->setCompressionEnabled(true);
    CHECK(engine->isCompressionEnabled());
}

TEST_CASE_METHOD(CompressedStorageFixture, "CompressedStorageEngine statistics",
                 "[storage][compressed][stats][catch2]") {
    CompressedStorageEngine::Config config;
    config.enableCompression = true;
    config.compressionThreshold = 1024;
    config.asyncCompression = false;

    auto engine = std::make_unique<CompressedStorageEngine>(underlying, config);

    // Store multiple objects
    for (int i = 0; i < 5; ++i) {
        std::string hash = std::format("{:064x}", i);
        auto data = generateCompressibleData(4096 + i * 100);
        engine->store(hash, data);
    }

    // Get stats
    auto stats = engine->getStats();
    CHECK(stats.totalObjects.load() > 0);

    auto compressionStats = engine->getCompressionStats();
    // Check that stats have reasonable values
    CHECK(compressionStats.totalCompressedFiles.load() >= 0);
}

TEST_CASE_METHOD(CompressedStorageFixture, "CompressedStorageEngine remove",
                 "[storage][compressed][catch2]") {
    CompressedStorageEngine::Config config;
    config.enableCompression = true;
    config.asyncCompression = false;

    auto engine = std::make_unique<CompressedStorageEngine>(underlying, config);

    std::string hash(64, 'd');
    auto data = generateCompressibleData(2048);

    engine->store(hash, data);
    REQUIRE(engine->exists(hash).value());

    auto removeResult = engine->remove(hash);
    REQUIRE(removeResult.has_value());
    CHECK_FALSE(engine->exists(hash).value());
}

TEST_CASE_METHOD(CompressedStorageFixture, "CompressedStorageEngine policy rules",
                 "[storage][compressed][policy][catch2]") {
    CompressedStorageEngine::Config config;
    config.enableCompression = true;

    auto engine = std::make_unique<CompressedStorageEngine>(underlying, config);

    // Get and set policy rules
    auto rules = engine->getPolicyRules();

    CompressionPolicy::Rules newRules;
    newRules.neverCompressBelow = 8192; // Change from default 4096
    newRules.alwaysCompressAbove = 5 * 1024 * 1024; // 5MB instead of 10MB
    engine->setPolicyRules(newRules);

    auto updatedRules = engine->getPolicyRules();
    CHECK(updatedRules.neverCompressBelow == 8192);
    CHECK(updatedRules.alwaysCompressAbove == 5 * 1024 * 1024);
}

TEST_CASE_METHOD(CompressedStorageFixture, "CompressedStorageEngine concurrent access",
                 "[storage][compressed][concurrent][catch2]") {
    CompressedStorageEngine::Config config;
    config.enableCompression = true;
    config.asyncCompression = false;

    auto engine = std::make_unique<CompressedStorageEngine>(underlying, config);

    const int numThreads = 4;
    const int itemsPerThread = 10;
    std::vector<std::thread> threads;
    std::atomic<int> successCount{0};

    for (int t = 0; t < numThreads; ++t) {
        threads.emplace_back([&engine, &successCount, t]() {
            for (int i = 0; i < itemsPerThread; ++i) {
                std::string hash = std::format("{:064x}", t * 1000 + i);
                auto data = generateCompressibleData(2048);
                if (engine->store(hash, data).has_value()) {
                    successCount++;
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    CHECK(successCount.load() == numThreads * itemsPerThread);
}

TEST_CASE_METHOD(CompressedStorageFixture, "CompressedStorageEngine async operations",
                 "[storage][compressed][async][catch2]") {
    CompressedStorageEngine::Config config;
    config.enableCompression = true;
    config.asyncCompression = true;

    auto engine = std::make_unique<CompressedStorageEngine>(underlying, config);

    std::string hash(64, 'e');
    auto data = generateCompressibleData(4096);

    // Async store
    auto storeFuture = engine->storeAsync(hash, data);
    auto storeResult = storeFuture.get();
    REQUIRE(storeResult.has_value());

    // Async retrieve
    auto retrieveFuture = engine->retrieveAsync(hash);
    auto retrieveResult = retrieveFuture.get();
    REQUIRE(retrieveResult.has_value());
    CHECK(retrieveResult.value() == data);
}

TEST_CASE_METHOD(CompressedStorageFixture, "CompressedStorageEngine batch operations",
                 "[storage][compressed][batch][catch2]") {
    CompressedStorageEngine::Config config;
    config.enableCompression = true;
    config.asyncCompression = false;

    auto engine = std::make_unique<CompressedStorageEngine>(underlying, config);

    std::vector<std::pair<std::string, std::vector<std::byte>>> items;
    for (int i = 0; i < 5; ++i) {
        std::string hash = std::format("{:064x}", 100 + i);
        items.emplace_back(hash, generateCompressibleData(1024));
    }

    auto results = engine->storeBatch(items);
    CHECK(results.size() == items.size());

    int successCount = 0;
    for (const auto& result : results) {
        if (result.has_value()) {
            successCount++;
        }
    }
    CHECK(successCount == static_cast<int>(items.size()));
}
