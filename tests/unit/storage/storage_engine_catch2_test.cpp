// Catch2 tests for storage engine
// Migrated from GTest: storage_engine_test.cpp

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <filesystem>
#include <format>
#include <fstream>
#include <random>
#include <thread>
#include <vector>

#include <yams/crypto/hasher.h>
#include <yams/storage/storage_engine.h>

using namespace yams;
using namespace yams::storage;

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

struct StorageEngineFixture {
    StorageEngineFixture() {
        testDir = std::filesystem::temp_directory_path() /
                  std::format("yams_storage_catch2_{}", std::chrono::steady_clock::now().time_since_epoch().count());
        storagePath = testDir / "storage_test";
        std::filesystem::create_directories(storagePath);

        StorageConfig config{.basePath = storagePath, .shardDepth = 2, .mutexPoolSize = 128};
        storage = std::make_unique<StorageEngine>(std::move(config));
    }

    ~StorageEngineFixture() {
        storage.reset();
        std::error_code ec;
        std::filesystem::remove_all(testDir, ec);
    }

    std::pair<std::string, std::vector<std::byte>> generateTestData(size_t size) {
        auto data = generateRandomBytes(size);
        auto hasher = crypto::createSHA256Hasher();
        auto hash = hasher->hash(data);
        return {hash, data};
    }

    std::filesystem::path testDir;
    std::filesystem::path storagePath;
    std::unique_ptr<StorageEngine> storage;
};

} // namespace

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine store and retrieve",
                 "[storage][engine][catch2]") {
    auto [hash, data] = generateTestData(1024);

    auto storeResult = storage->store(hash, data);
    REQUIRE(storeResult.has_value());

    auto retrieveResult = storage->retrieve(hash);
    REQUIRE(retrieveResult.has_value());
    CHECK(retrieveResult.value() == data);
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine store existing object",
                 "[storage][engine][catch2]") {
    auto [hash, data] = generateTestData(1024);

    auto result1 = storage->store(hash, data);
    REQUIRE(result1.has_value());

    auto result2 = storage->store(hash, data);
    REQUIRE(result2.has_value());

    auto stats = storage->getStats();
    CHECK(stats.totalObjects.load() == 1u);
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine retrieve non-existent",
                 "[storage][engine][catch2]") {
    std::string fakeHash(64, '0');

    auto result = storage->retrieve(fakeHash);
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error() == ErrorCode::ChunkNotFound);
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine exists check",
                 "[storage][engine][catch2]") {
    auto [hash, data] = generateTestData(512);

    auto exists1 = storage->exists(hash);
    REQUIRE(exists1.has_value());
    CHECK_FALSE(exists1.value());

    storage->store(hash, data);

    auto exists2 = storage->exists(hash);
    REQUIRE(exists2.has_value());
    CHECK(exists2.value());
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine remove object",
                 "[storage][engine][catch2]") {
    auto [hash, data] = generateTestData(2048);

    storage->store(hash, data);
    REQUIRE(storage->exists(hash).value());

    auto removeResult = storage->remove(hash);
    REQUIRE(removeResult.has_value());
    CHECK_FALSE(storage->exists(hash).value());

    auto stats = storage->getStats();
    CHECK(stats.totalObjects.load() == 0u);
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine invalid hash length",
                 "[storage][engine][catch2]") {
    std::string shortHash = "abc123";
    std::vector<std::byte> data{std::byte{1}, std::byte{2}, std::byte{3}};

    auto result = storage->store(shortHash, data);
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error() == ErrorCode::InvalidArgument);
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine large file",
                 "[storage][engine][large][catch2]") {
    auto [hash, data] = generateTestData(10 * 1024 * 1024);

    auto storeResult = storage->store(hash, data);
    REQUIRE(storeResult.has_value());

    auto retrieveResult = storage->retrieve(hash);
    REQUIRE(retrieveResult.has_value());
    CHECK(retrieveResult.value().size() == data.size());
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine directory sharding",
                 "[storage][engine][catch2]") {
    auto [hash, data] = generateTestData(100);

    storage->store(hash, data);

    auto expectedPath = storagePath / "objects" / hash.substr(0, 2) / hash.substr(2);
    CHECK(std::filesystem::exists(expectedPath));
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine statistics",
                 "[storage][engine][stats][catch2]") {
    std::vector<std::pair<std::string, std::vector<std::byte>>> testData;
    for (int i = 0; i < 5; ++i) {
        testData.push_back(generateTestData(1024 * (i + 1)));
    }

    for (const auto& [hash, data] : testData) {
        storage->store(hash, data);
    }

    auto stats = storage->getStats();
    CHECK(stats.totalObjects.load() == 5u);
    CHECK(stats.writeOperations.load() == 5u);
    CHECK(stats.totalBytes.load() > 0u);

    for (const auto& [hash, data] : testData) {
        storage->retrieve(hash);
    }

    auto readStats = storage->getStats();
    CHECK(readStats.readOperations.load() == 5u);
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine async operations",
                 "[storage][engine][async][catch2]") {
    auto [hash, data] = generateTestData(4096);

    auto storeFuture = storage->storeAsync(hash, data);
    auto storeResult = storeFuture.get();
    REQUIRE(storeResult.has_value());

    auto retrieveFuture = storage->retrieveAsync(hash);
    auto retrieveResult = retrieveFuture.get();
    REQUIRE(retrieveResult.has_value());
    CHECK(retrieveResult.value() == data);
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine batch operations",
                 "[storage][engine][batch][catch2]") {
    std::vector<std::pair<std::string, std::vector<std::byte>>> items;
    for (int i = 0; i < 10; ++i) {
        items.push_back(generateTestData(512));
    }

    auto results = storage->storeBatch(items);
    REQUIRE(results.size() == items.size());

    for (const auto& result : results) {
        CHECK(result.has_value());
    }

    for (const auto& [hash, data] : items) {
        CHECK(storage->exists(hash).value());
    }
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine concurrent reads",
                 "[storage][engine][concurrent][catch2]") {
    auto [hash, data] = generateTestData(1024);
    storage->store(hash, data);

    const int numThreads = 10;
    std::vector<std::thread> threads;
    std::atomic<int> successCount{0};

    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back([this, &hash, &data, &successCount]() {
            for (int j = 0; j < 100; ++j) {
                auto result = storage->retrieve(hash);
                if (result.has_value() && result.value() == data) {
                    successCount++;
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    CHECK(successCount.load() == numThreads * 100);
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine concurrent writes",
                 "[storage][engine][concurrent][catch2]") {
    const int numThreads = 10;
    const int objectsPerThread = 10;
    std::vector<std::thread> threads;
    std::atomic<int> successCount{0};

    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back([this, &successCount]() {
            for (int j = 0; j < objectsPerThread; ++j) {
                auto [hash, data] = generateTestData(256);
                auto result = storage->store(hash, data);
                if (result.has_value()) {
                    successCount++;
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    CHECK(successCount.load() == numThreads * objectsPerThread);

    auto stats = storage->getStats();
    CHECK(stats.totalObjects.load() == static_cast<uint64_t>(numThreads * objectsPerThread));
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine concurrent same object writes",
                 "[storage][engine][concurrent][catch2]") {
    auto [hash, data] = generateTestData(512);

    const int numThreads = 20;
    std::vector<std::thread> threads;
    std::atomic<int> successCount{0};

    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back([this, &hash, &data, &successCount]() {
            auto result = storage->store(hash, data);
            if (result.has_value()) {
                successCount++;
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    CHECK(successCount.load() == numThreads);

    auto stats = storage->getStats();
    CHECK(stats.totalObjects.load() == 1u);
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine mixed concurrent operations",
                 "[storage][engine][concurrent][catch2]") {
    std::vector<std::pair<std::string, std::vector<std::byte>>> objects;
    for (int i = 0; i < 10; ++i) {
        auto obj = generateTestData(512);
        objects.push_back(obj);
        storage->store(obj.first, obj.second);
    }

    const int numThreads = 8;
    std::vector<std::thread> threads;
    std::atomic<int> readSuccess{0};
    std::atomic<int> writeSuccess{0};

    for (int i = 0; i < numThreads; ++i) {
        if (i % 2 == 0) {
            threads.emplace_back([this, &objects, &readSuccess]() {
                std::random_device rd;
                std::mt19937 gen(rd());
                std::uniform_int_distribution<size_t> dis(0, objects.size() - 1);

                for (int j = 0; j < 50; ++j) {
                    auto& [hash, data] = objects[dis(gen)];
                    auto result = storage->retrieve(hash);
                    if (result.has_value() && result.value() == data) {
                        readSuccess++;
                    }
                }
            });
        } else {
            threads.emplace_back([this, &writeSuccess]() {
                for (int j = 0; j < 10; ++j) {
                    auto [hash, data] = generateTestData(256);
                    auto result = storage->store(hash, data);
                    if (result.has_value()) {
                        writeSuccess++;
                    }
                }
            });
        }
    }

    for (auto& t : threads) {
        t.join();
    }

    CHECK(readSuccess.load() == (numThreads / 2) * 50);
    CHECK(writeSuccess.load() == (numThreads / 2) * 10);
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine cleanup temp files",
                 "[storage][engine][catch2]") {
    auto tempDir = storagePath / "temp";
    std::filesystem::create_directories(tempDir);

    for (int i = 0; i < 5; ++i) {
        auto tempFile = tempDir / std::format("test{}.tmp", i);
        std::ofstream(tempFile) << "test data";
    }

    auto result = storage->cleanupTempFiles();
    REQUIRE(result.has_value());

    CHECK(std::distance(std::filesystem::directory_iterator(tempDir),
                        std::filesystem::directory_iterator{}) == 5);
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine storage size",
                 "[storage][engine][catch2]") {
    size_t totalSize = 0;
    for (int i = 0; i < 5; ++i) {
        auto [hash, data] = generateTestData(1024);
        storage->store(hash, data);
        totalSize += data.size();
    }

    auto sizeResult = storage->getStorageSize();
    REQUIRE(sizeResult.has_value());
    CHECK(sizeResult.value() == totalSize);
}
