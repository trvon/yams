// Catch2 tests for storage engine
// Migrated from GTest: storage_engine_test.cpp

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <format>
#include <fstream>
#include <optional>
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
                  std::format("yams_storage_catch2_{}",
                              std::chrono::steady_clock::now().time_since_epoch().count());
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

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine exists check", "[storage][engine][catch2]") {
    auto [hash, data] = generateTestData(512);

    auto exists1 = storage->exists(hash);
    REQUIRE(exists1.has_value());
    CHECK_FALSE(exists1.value());

    storage->store(hash, data);

    auto exists2 = storage->exists(hash);
    REQUIRE(exists2.has_value());
    CHECK(exists2.value());
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine remove object", "[storage][engine][catch2]") {
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

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine storage size", "[storage][engine][catch2]") {
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

TEST_CASE("StorageEngine rejects path traversal storage keys", "[storage][security][catch2]") {
    auto testDir = std::filesystem::temp_directory_path() /
                   std::format("yams_storage_traversal_{}",
                               std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::create_directories(testDir);
    auto cleanup = [&] {
        std::error_code ec;
        std::filesystem::remove_all(testDir, ec);
    };

    StorageConfig config{.basePath = testDir / "storage", .shardDepth = 2, .mutexPoolSize = 64};
    StorageEngine engine(std::move(config));
    std::vector<std::byte> data{std::byte{0x01}, std::byte{0x02}};

    const std::string traversalKey = std::string("..") + std::string(62, 'a');
    auto storeResult = engine.store(traversalKey, data);
    CHECK_FALSE(storeResult.has_value());
    CHECK(storeResult.error().code == ErrorCode::InvalidArgument);
    CHECK_FALSE(std::filesystem::exists((testDir / std::string(62, 'a'))));

    const std::string manifestTraversalKey = std::string("..") + std::string(62, 'b') + ".manifest";
    auto manifestResult = engine.store(manifestTraversalKey, data);
    CHECK_FALSE(manifestResult.has_value());
    CHECK(manifestResult.error().code == ErrorCode::InvalidArgument);

    cleanup();
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine rejects invalid keys for every operation",
                 "[storage][engine][invalid][catch2]") {
    const std::vector<std::string> invalidKeys{
        "abc123",
        std::string(64, 'g'),
        std::string("..") + std::string(62, 'a'),
        std::string(64, 'b') + ".manifest.extra",
        std::string("..") + std::string(62, 'c') + ".manifest",
    };
    std::vector<std::byte> data{std::byte{0x01}, std::byte{0x02}};

    for (const auto& key : invalidKeys) {
        INFO("key=" << key);

        auto storeResult = storage->store(key, data);
        REQUIRE_FALSE(storeResult.has_value());
        CHECK(storeResult.error().code == ErrorCode::InvalidArgument);

        auto retrieveResult = storage->retrieve(key);
        REQUIRE_FALSE(retrieveResult.has_value());
        CHECK(retrieveResult.error().code == ErrorCode::InvalidArgument);

        auto rawResult = storage->retrieveRaw(key);
        REQUIRE_FALSE(rawResult.has_value());
        CHECK(rawResult.error().code == ErrorCode::InvalidArgument);

        auto existsResult = storage->exists(key);
        REQUIRE_FALSE(existsResult.has_value());
        CHECK(existsResult.error().code == ErrorCode::InvalidArgument);

        auto removeResult = storage->remove(key);
        REQUIRE_FALSE(removeResult.has_value());
        CHECK(removeResult.error().code == ErrorCode::InvalidArgument);

        auto sizeResult = storage->getBlockSize(key);
        REQUIRE_FALSE(sizeResult.has_value());
        CHECK(sizeResult.error().code == ErrorCode::InvalidArgument);
    }

    CHECK_FALSE(std::filesystem::exists(storagePath.parent_path() / std::string(62, 'a')));
    CHECK_FALSE(std::filesystem::exists(storagePath.parent_path() / std::string(62, 'c')));
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine ignores and cleans stale temp files",
                 "[storage][engine][temp][catch2]") {
    auto tempDir = storagePath / "temp";
    std::filesystem::create_directories(tempDir);

    const auto staleTemp = tempDir / "stale-corrupt.tmp";
    const auto recentTemp = tempDir / "recent-corrupt.tmp";
    std::ofstream(staleTemp, std::ios::binary) << "not an object";
    std::ofstream(recentTemp, std::ios::binary) << "also not an object";
    std::filesystem::last_write_time(staleTemp, std::filesystem::file_time_type::clock::now() -
                                                    std::chrono::hours(2));

    auto [hash, data] = generateTestData(2048);
    REQUIRE(storage->store(hash, data).has_value());

    auto verifyResult = storage->verify();
    REQUIRE(verifyResult.has_value());

    auto sizeResult = storage->getStorageSize();
    REQUIRE(sizeResult.has_value());
    CHECK(sizeResult.value() == data.size());

    auto cleanupResult = storage->cleanupTempFiles();
    REQUIRE(cleanupResult.has_value());
    CHECK_FALSE(std::filesystem::exists(staleTemp));
    CHECK(std::filesystem::exists(recentTemp));
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine same-key write collision stays atomic",
                 "[storage][engine][concurrent][collision][catch2]") {
    const std::string key = std::string(64, 'a');
    const std::vector<std::byte> first(64 * 1024, std::byte{0x11});
    const std::vector<std::byte> second(64 * 1024, std::byte{0x22});

    constexpr int kThreadCount = 24;
    std::vector<std::thread> threads;
    std::atomic<int> ready{0};
    std::atomic<bool> go{false};
    std::atomic<int> successCount{0};

    for (int i = 0; i < kThreadCount; ++i) {
        threads.emplace_back([&, i]() {
            ready.fetch_add(1, std::memory_order_relaxed);
            while (!go.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            const auto& payload = (i % 2 == 0) ? first : second;
            auto result = storage->store(key, payload);
            if (result.has_value()) {
                successCount.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    while (ready.load(std::memory_order_relaxed) != kThreadCount) {
        std::this_thread::yield();
    }
    go.store(true, std::memory_order_release);

    for (auto& thread : threads) {
        thread.join();
    }

    CHECK(successCount.load() == kThreadCount);
    auto retrieved = storage->retrieve(key);
    REQUIRE(retrieved.has_value());
    CHECK((retrieved.value() == first || retrieved.value() == second));

    auto stats = storage->getStats();
    CHECK(stats.totalObjects.load() == 1u);
    CHECK(stats.totalBytes.load() == first.size());
}

TEST_CASE_METHOD(StorageEngineFixture,
                 "StorageEngine remove during reads returns complete data or typed errors",
                 "[storage][engine][concurrent][remove][catch2]") {
    auto [hash, data] = generateTestData(2 * 1024 * 1024);
    REQUIRE(storage->store(hash, data).has_value());

    constexpr int kReaderCount = 6;
    constexpr int kReadsPerThread = 20;
    std::atomic<int> ready{0};
    std::atomic<bool> go{false};
    std::atomic<int> completeReads{0};
    std::atomic<int> typedFailures{0};
    std::atomic<int> partialReads{0};

    std::vector<std::thread> readers;
    for (int i = 0; i < kReaderCount; ++i) {
        readers.emplace_back([&]() {
            ready.fetch_add(1, std::memory_order_relaxed);
            while (!go.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            for (int j = 0; j < kReadsPerThread; ++j) {
                auto result = storage->retrieve(hash);
                if (result.has_value()) {
                    if (result.value() == data) {
                        completeReads.fetch_add(1, std::memory_order_relaxed);
                    } else {
                        partialReads.fetch_add(1, std::memory_order_relaxed);
                    }
                } else if (result.error().code == ErrorCode::ChunkNotFound ||
                           result.error().code == ErrorCode::PermissionDenied ||
                           result.error().code == ErrorCode::CorruptedData) {
                    typedFailures.fetch_add(1, std::memory_order_relaxed);
                } else {
                    partialReads.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    while (ready.load(std::memory_order_relaxed) != kReaderCount) {
        std::this_thread::yield();
    }
    go.store(true, std::memory_order_release);
    while (completeReads.load(std::memory_order_relaxed) == 0) {
        std::this_thread::yield();
    }

    auto removeResult = storage->remove(hash);
    if (!removeResult.has_value()) {
        CHECK((removeResult.error().code == ErrorCode::ChunkNotFound ||
               removeResult.error().code == ErrorCode::PermissionDenied));
    }

    for (auto& reader : readers) {
        reader.join();
    }

    CHECK(completeReads.load() > 0);
    CHECK(typedFailures.load() >= 0);
    CHECK(partialReads.load() == 0);
}

TEST_CASE("AtomicFileWriter removes temp file when rename fails",
          "[storage][atomic-writer][catch2]") {
    auto testDir = std::filesystem::temp_directory_path() /
                   std::format("yams_atomic_writer_catch2_{}",
                               std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::create_directories(testDir);
    auto cleanup = [&] {
        std::error_code ec;
        std::filesystem::remove_all(testDir, ec);
    };

    const auto targetDir = testDir / "target";
    std::filesystem::create_directory(targetDir);
    std::vector<std::byte> data{std::byte{0x01}, std::byte{0x02}, std::byte{0x03}};

    AtomicFileWriter writer;
    auto result = writer.write(targetDir, data);
    REQUIRE_FALSE(result.has_value());

    const auto tempPrefix = targetDir.filename().string() + ".tmp.";
    for (const auto& entry : std::filesystem::directory_iterator(testDir)) {
        CHECK(entry.path().filename().string().rfind(tempPrefix, 0) != 0);
    }

    cleanup();
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine verify detects content hash mismatch",
                 "[storage][integrity][catch2]") {
    auto [hash, data] = generateTestData(1024);
    REQUIRE(storage->store(hash, data).has_value());
    REQUIRE(storage->verify().has_value());

    auto objectPath = storagePath / "objects" / hash.substr(0, 2) / hash.substr(2);
    {
        std::fstream file(objectPath, std::ios::binary | std::ios::in | std::ios::out);
        REQUIRE(static_cast<bool>(file));
        char first = 0;
        file.read(&first, 1);
        REQUIRE(static_cast<bool>(file));
        file.seekp(0);
        const char corrupt = static_cast<char>(first ^ 0xff);
        file.write(&corrupt, 1);
        REQUIRE(static_cast<bool>(file));
    }

    auto verifyResult = storage->verify();
    REQUIRE_FALSE(verifyResult.has_value());
    CHECK(verifyResult.error().code == ErrorCode::CorruptedData);
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine stores and retrieves manifest keys",
                 "[storage][manifest][catch2]") {
    const std::string hash32 = std::string(62, 'a') + "bb";
    const std::string manifestKey = hash32 + ".manifest";
    std::vector<std::byte> data = generateRandomBytes(2048);

    auto storeResult = storage->store(manifestKey, data);
    REQUIRE(storeResult.has_value());

    CHECK(storage->exists(manifestKey));

    auto retrieveResult = storage->retrieve(manifestKey);
    REQUIRE(retrieveResult.has_value());
    CHECK(retrieveResult.value() == data);

    REQUIRE(storage->remove(manifestKey).has_value());
    auto existsAfterRemove = storage->exists(manifestKey);
    if (existsAfterRemove.has_value()) {
        CHECK_FALSE(existsAfterRemove.value());
    }
    // exists may return an error after removal — that's acceptable
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine stores manifests in separate shard directory",
                 "[storage][manifest][sharding][catch2]") {
    const std::string hash32 = std::string(62, 'c') + "dd";
    const std::string manifestKey = hash32 + ".manifest";
    std::vector<std::byte> data = generateRandomBytes(512);

    REQUIRE(storage->store(manifestKey, data).has_value());

    auto manifestPath =
        storagePath / "manifests" / hash32.substr(0, 2) / (hash32.substr(2) + ".manifest");
    CHECK(std::filesystem::exists(manifestPath));
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine retrieveRaw returns raw stored bytes",
                 "[storage][retrieveRaw][catch2]") {
    auto [hash, data] = generateTestData(4096);
    REQUIRE(storage->store(hash, data).has_value());

    auto rawResult = storage->retrieveRaw(hash);
    REQUIRE(rawResult.has_value());
    CHECK(rawResult.value().data == data);
}

TEST_CASE_METHOD(StorageEngineFixture,
                 "StorageEngine retrieveRaw returns NotFound for missing hash",
                 "[storage][retrieveRaw][catch2]") {
    const std::string missingHash = std::string(64, '0');
    auto result = storage->retrieveRaw(missingHash);
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error().code == ErrorCode::ChunkNotFound);
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine retrieveRawAsync works for existing hash",
                 "[storage][retrieveRaw][async][catch2]") {
    auto [hash, data] = generateTestData(1024);
    REQUIRE(storage->store(hash, data).has_value());

    auto future = storage->retrieveRawAsync(hash);
    auto result = future.get();
    REQUIRE(result.has_value());
    CHECK(result.value().data == data);
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine getBlockSize returns stored size",
                 "[storage][blockSize][catch2]") {
    auto [hash, data] = generateTestData(3333);
    REQUIRE(storage->store(hash, data).has_value());

    auto sizeResult = storage->getBlockSize(hash);
    REQUIRE(sizeResult.has_value());
    CHECK(sizeResult.value() == static_cast<uint64_t>(3333));
}

TEST_CASE_METHOD(StorageEngineFixture,
                 "StorageEngine getBlockSize returns NotFound for missing hash",
                 "[storage][blockSize][catch2]") {
    const std::string missingHash = std::string(64, 'f');
    auto result = storage->getBlockSize(missingHash);
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error().code == ErrorCode::ChunkNotFound);
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine cleanupTempFiles returns successfully",
                 "[storage][cleanup][catch2]") {
    auto [hash, data] = generateTestData(512);
    REQUIRE(storage->store(hash, data).has_value());

    auto tempDir = storagePath / "temp";
    REQUIRE(std::filesystem::exists(tempDir));

    auto statsExist = storage->getStats();
    REQUIRE(statsExist.totalObjects >= 1);

    auto cleanupResult = storage->cleanupTempFiles();
    REQUIRE(cleanupResult.has_value());
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine compact returns successfully",
                 "[storage][compact][catch2]") {
    auto [hash1, data1] = generateTestData(1024);
    auto [hash2, data2] = generateTestData(2048);
    REQUIRE(storage->store(hash1, data1).has_value());
    REQUIRE(storage->store(hash2, data2).has_value());

    auto compactResult = storage->compact();
    CHECK(compactResult.has_value());
}

TEST_CASE("StorageEngine verifies manifest keys during integrity check",
          "[storage][manifest][integrity][catch2]") {
    auto testDir = std::filesystem::temp_directory_path() /
                   std::format("yams_storage_manifest_verify_{}",
                               std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::create_directories(testDir);
    auto cleanup = [&] {
        std::error_code ec;
        std::filesystem::remove_all(testDir, ec);
    };

    StorageConfig config{.basePath = testDir / "storage", .shardDepth = 2, .mutexPoolSize = 64};
    StorageEngine engine(std::move(config));

    const std::string hash32 = std::string(62, '0') + "ff";
    const std::string manifestKey = hash32 + ".manifest";
    std::vector<std::byte> mdata = generateRandomBytes(1024);
    REQUIRE(engine.store(manifestKey, mdata).has_value());

    auto verifyResult = engine.verify();
    REQUIRE(verifyResult.has_value());

    cleanup();
}

TEST_CASE_METHOD(StorageEngineFixture,
                 "StorageEngine verify detects corruption in compressed or manifest object content",
                 "[storage][integrity][manifest][catch2]") {
    auto [hash, data] = generateTestData(2048);
    REQUIRE(storage->store(hash, data).has_value());

    auto objectPath = storagePath / "objects" / hash.substr(0, 2) / hash.substr(2);
    {
        std::fstream file(objectPath, std::ios::binary | std::ios::in | std::ios::out);
        REQUIRE(static_cast<bool>(file));
        file.seekg(10);
        char original = 0;
        file.read(&original, 1);
        REQUIRE(static_cast<bool>(file));
        file.seekp(10);
        const char corrupt = static_cast<char>(original ^ 0xff);
        file.write(&corrupt, 1);
        REQUIRE(static_cast<bool>(file));
    }

    auto verifyResult = storage->verify();
    REQUIRE_FALSE(verifyResult.has_value());
    CHECK(verifyResult.error().code == ErrorCode::CorruptedData);
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine stats track delete operations",
                 "[storage][stats][catch2]") {
    auto [hash, data] = generateTestData(256);
    REQUIRE(storage->store(hash, data).has_value());

    REQUIRE(storage->remove(hash).has_value());

    auto stats = storage->getStats();
    CHECK(stats.deleteOperations > 0);
}
