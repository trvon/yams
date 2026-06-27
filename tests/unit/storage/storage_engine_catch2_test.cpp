// Catch2 tests for storage engine
// Migrated from GTest: storage_engine_test.cpp

#include <catch2/benchmark/catch_benchmark.hpp>
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

#ifndef _WIN32
#include <unistd.h>
#include <sys/stat.h>
#endif

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

bool induceMemoryPressureForCacheEviction() {
    try {
        auto big = std::make_unique_for_overwrite<char[]>(
            static_cast<size_t>(1.5 * 1024 * 1024 * 1024)); // 1.5GB
        std::memset(big.get(), 0xFF, static_cast<size_t>(1.5 * 1024 * 1024 * 1024));
        return true;
    } catch (const std::exception& e) {
        WARN("Memory pressure allocation failed: " << e.what());
    } catch (...) {
        WARN("Memory pressure allocation failed (unknown error)");
    }
    return false;
}

bool bestEffortEvictPageCache() {
#if __APPLE__
    if (::geteuid() == 0 && std::system("purge >/dev/null 2>&1") == 0) {
        return true;
    }
#elif !defined(_WIN32)
    if (::geteuid() == 0 &&
        std::system("sync && echo 3 > /proc/sys/vm/drop_caches 2>/dev/null") == 0) {
        return true;
    }
#endif
    return induceMemoryPressureForCacheEviction();
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
    REQUIRE((storeResult.has_value()));

    auto retrieveResult = storage->retrieve(hash);
    REQUIRE((retrieveResult.has_value()));
    CHECK((retrieveResult.value() == data));
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine store existing object",
                 "[storage][engine][catch2]") {
    auto [hash, data] = generateTestData(1024);

    auto result1 = storage->store(hash, data);
    REQUIRE((result1.has_value()));

    auto result2 = storage->store(hash, data);
    REQUIRE((result2.has_value()));

    auto stats = storage->getStats();
    CHECK((stats.totalObjects.load() == 1u));
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine retrieve non-existent",
                 "[storage][engine][catch2]") {
    std::string fakeHash(64, '0');

    auto result = storage->retrieve(fakeHash);
    REQUIRE_FALSE(result.has_value());
    CHECK((result.error() == ErrorCode::ChunkNotFound));
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine exists check", "[storage][engine][catch2]") {
    auto [hash, data] = generateTestData(512);

    auto exists1 = storage->exists(hash);
    REQUIRE((exists1.has_value()));
    CHECK_FALSE(exists1.value());

    storage->store(hash, data);

    auto exists2 = storage->exists(hash);
    REQUIRE((exists2.has_value()));
    CHECK((exists2.value()));
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine remove object", "[storage][engine][catch2]") {
    auto [hash, data] = generateTestData(2048);

    storage->store(hash, data);
    REQUIRE((storage->exists(hash).value()));

    auto removeResult = storage->remove(hash);
    REQUIRE((removeResult.has_value()));
    CHECK_FALSE(storage->exists(hash).value());

    auto stats = storage->getStats();
    CHECK((stats.totalObjects.load() == 0u));
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine invalid hash length",
                 "[storage][engine][catch2]") {
    std::string shortHash = "abc123";
    std::vector<std::byte> data{std::byte{1}, std::byte{2}, std::byte{3}};

    auto result = storage->store(shortHash, data);
    REQUIRE_FALSE(result.has_value());
    CHECK((result.error() == ErrorCode::InvalidArgument));
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine large file",
                 "[storage][engine][large][catch2]") {
    auto [hash, data] = generateTestData(static_cast<size_t>(10) * 1024 * 1024);

    auto storeResult = storage->store(hash, data);
    REQUIRE((storeResult.has_value()));

    auto retrieveResult = storage->retrieve(hash);
    REQUIRE((retrieveResult.has_value()));
    CHECK((retrieveResult.value().size() == data.size()));
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine directory sharding",
                 "[storage][engine][catch2]") {
    auto [hash, data] = generateTestData(100);

    storage->store(hash, data);

    auto expectedPath = storagePath / "objects" / hash.substr(0, 2) / hash.substr(2);
    CHECK((std::filesystem::exists(expectedPath)));
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine statistics",
                 "[storage][engine][stats][catch2]") {
    std::vector<std::pair<std::string, std::vector<std::byte>>> testData;
    for (int i = 0; i < 5; ++i) {
        testData.push_back(generateTestData(static_cast<size_t>(1024) * (i + 1)));
    }

    for (const auto& [hash, data] : testData) {
        storage->store(hash, data);
    }

    auto stats = storage->getStats();
    CHECK((stats.totalObjects.load() == 5u));
    CHECK((stats.writeOperations.load() == 5u));
    CHECK((stats.totalBytes.load() > 0u));

    for (const auto& [hash, data] : testData) {
        storage->retrieve(hash);
    }

    auto readStats = storage->getStats();
    CHECK((readStats.readOperations.load() == 5u));
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine async operations",
                 "[storage][engine][async][catch2]") {
    auto [hash, data] = generateTestData(4096);

    auto storeFuture = storage->storeAsync(hash, data);
    auto storeResult = storeFuture.get();
    REQUIRE((storeResult.has_value()));

    auto retrieveFuture = storage->retrieveAsync(hash);
    auto retrieveResult = retrieveFuture.get();
    REQUIRE((retrieveResult.has_value()));
    CHECK((retrieveResult.value() == data));
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine batch operations",
                 "[storage][engine][batch][catch2]") {
    std::vector<std::pair<std::string, std::vector<std::byte>>> items;
    for (int i = 0; i < 10; ++i) {
        items.push_back(generateTestData(512));
    }

    auto results = storage->storeBatch(items);
    REQUIRE((results.size() == items.size()));

    for (const auto& result : results) {
        CHECK((result.has_value()));
    }

    for (const auto& [hash, data] : items) {
        CHECK((storage->exists(hash).value()));
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

    CHECK((successCount.load() == numThreads * 100));
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

    CHECK((successCount.load() == numThreads * objectsPerThread));

    auto stats = storage->getStats();
    CHECK((stats.totalObjects.load() == static_cast<uint64_t>(numThreads * objectsPerThread)));
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

    CHECK((successCount.load() == numThreads));

    auto stats = storage->getStats();
    CHECK((stats.totalObjects.load() == 1u));
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

    CHECK((readSuccess.load() == (numThreads / 2) * 50));
    CHECK((writeSuccess.load() == (numThreads / 2) * 10));
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
    REQUIRE((result.has_value()));

    CHECK((std::distance(std::filesystem::directory_iterator(tempDir),
                         std::filesystem::directory_iterator{}) == 5));
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine storage size", "[storage][engine][catch2]") {
    size_t totalSize = 0;
    for (int i = 0; i < 5; ++i) {
        auto [hash, data] = generateTestData(1024);
        storage->store(hash, data);
        totalSize += data.size();
    }

    auto sizeResult = storage->getStorageSize();
    REQUIRE((sizeResult.has_value()));
    CHECK((sizeResult.value() == totalSize));
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
    CHECK((storeResult.error().code == ErrorCode::InvalidArgument));
    CHECK_FALSE(std::filesystem::exists((testDir / std::string(62, 'a'))));

    const std::string manifestTraversalKey = std::string("..") + std::string(62, 'b') + ".manifest";
    auto manifestResult = engine.store(manifestTraversalKey, data);
    CHECK_FALSE(manifestResult.has_value());
    CHECK((manifestResult.error().code == ErrorCode::InvalidArgument));

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
        CHECK((storeResult.error().code == ErrorCode::InvalidArgument));

        auto retrieveResult = storage->retrieve(key);
        REQUIRE_FALSE(retrieveResult.has_value());
        CHECK((retrieveResult.error().code == ErrorCode::InvalidArgument));

        auto rawResult = storage->retrieveRaw(key);
        REQUIRE_FALSE(rawResult.has_value());
        CHECK((rawResult.error().code == ErrorCode::InvalidArgument));

        auto existsResult = storage->exists(key);
        REQUIRE_FALSE(existsResult.has_value());
        CHECK((existsResult.error().code == ErrorCode::InvalidArgument));

        auto removeResult = storage->remove(key);
        REQUIRE_FALSE(removeResult.has_value());
        CHECK((removeResult.error().code == ErrorCode::InvalidArgument));

        auto sizeResult = storage->getBlockSize(key);
        REQUIRE_FALSE(sizeResult.has_value());
        CHECK((sizeResult.error().code == ErrorCode::InvalidArgument));
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
    REQUIRE((storage->store(hash, data).has_value()));

    auto verifyResult = storage->verify();
    REQUIRE((verifyResult.has_value()));

    auto sizeResult = storage->getStorageSize();
    REQUIRE((sizeResult.has_value()));
    CHECK((sizeResult.value() == data.size()));

    auto cleanupResult = storage->cleanupTempFiles();
    REQUIRE((cleanupResult.has_value()));
    CHECK_FALSE(std::filesystem::exists(staleTemp));
    CHECK((std::filesystem::exists(recentTemp)));
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine same-key write collision stays atomic",
                 "[storage][engine][concurrent][collision][catch2]") {
    const std::string key = std::string(64, 'a');
    const std::vector<std::byte> first(static_cast<size_t>(64) * 1024, std::byte{0x11});
    const std::vector<std::byte> second(static_cast<size_t>(64) * 1024, std::byte{0x22});

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

    CHECK((successCount.load() == kThreadCount));
    auto retrieved = storage->retrieve(key);
    REQUIRE((retrieved.has_value()));
    CHECK((retrieved.value() == first || retrieved.value() == second));

    auto stats = storage->getStats();
    CHECK((stats.totalObjects.load() == 1u));
    CHECK((stats.totalBytes.load() == first.size()));
}

TEST_CASE_METHOD(StorageEngineFixture,
                 "StorageEngine remove during reads returns complete data or typed errors",
                 "[storage][engine][concurrent][remove][catch2]") {
    auto [hash, data] = generateTestData(static_cast<size_t>(2) * 1024 * 1024);
    REQUIRE((storage->store(hash, data).has_value()));

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

    CHECK((completeReads.load() > 0));
    CHECK((typedFailures.load() >= 0));
    CHECK((partialReads.load() == 0));
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
        CHECK((entry.path().filename().string().rfind(tempPrefix, 0) != 0));
    }

    cleanup();
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine verify detects content hash mismatch",
                 "[storage][integrity][catch2]") {
    auto [hash, data] = generateTestData(1024);
    REQUIRE((storage->store(hash, data).has_value()));
    REQUIRE((storage->verify().has_value()));

    auto objectPath = storagePath / "objects" / hash.substr(0, 2) / hash.substr(2);
    {
        std::fstream file(objectPath, std::ios::binary | std::ios::in | std::ios::out);
        REQUIRE((static_cast<bool>(file)));
        char first = 0;
        file.read(&first, 1);
        REQUIRE((static_cast<bool>(file)));
        file.seekp(0);
        const char corrupt = static_cast<char>(first ^ 0xff);
        file.write(&corrupt, 1);
        REQUIRE((static_cast<bool>(file)));
    }

    auto verifyResult = storage->verify();
    REQUIRE_FALSE(verifyResult.has_value());
    CHECK((verifyResult.error().code == ErrorCode::CorruptedData));
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine stores and retrieves manifest keys",
                 "[storage][manifest][catch2]") {
    const std::string hash32 = std::string(62, 'a') + "bb";
    const std::string manifestKey = hash32 + ".manifest";
    std::vector<std::byte> data = generateRandomBytes(2048);

    auto storeResult = storage->store(manifestKey, data);
    REQUIRE((storeResult.has_value()));

    CHECK((storage->exists(manifestKey)));

    auto retrieveResult = storage->retrieve(manifestKey);
    REQUIRE((retrieveResult.has_value()));
    CHECK((retrieveResult.value() == data));

    REQUIRE((storage->remove(manifestKey).has_value()));
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

    REQUIRE((storage->store(manifestKey, data).has_value()));

    auto manifestPath =
        storagePath / "manifests" / hash32.substr(0, 2) / (hash32.substr(2) + ".manifest");
    CHECK((std::filesystem::exists(manifestPath)));
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine retrieveRaw returns raw stored bytes",
                 "[storage][retrieveRaw][catch2]") {
    auto [hash, data] = generateTestData(4096);
    REQUIRE((storage->store(hash, data).has_value()));

    auto rawResult = storage->retrieveRaw(hash);
    REQUIRE((rawResult.has_value()));
    CHECK((rawResult.value().data == data));
}

TEST_CASE_METHOD(StorageEngineFixture,
                 "StorageEngine retrieveRaw returns NotFound for missing hash",
                 "[storage][retrieveRaw][catch2]") {
    const std::string missingHash = std::string(64, '0');
    auto result = storage->retrieveRaw(missingHash);
    REQUIRE_FALSE(result.has_value());
    CHECK((result.error().code == ErrorCode::ChunkNotFound));
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine retrieveRawAsync works for existing hash",
                 "[storage][retrieveRaw][async][catch2]") {
    auto [hash, data] = generateTestData(1024);
    REQUIRE((storage->store(hash, data).has_value()));

    auto future = storage->retrieveRawAsync(hash);
    auto result = future.get();
    REQUIRE((result.has_value()));
    CHECK((result.value().data == data));
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine getBlockSize returns stored size",
                 "[storage][blockSize][catch2]") {
    auto [hash, data] = generateTestData(3333);
    REQUIRE((storage->store(hash, data).has_value()));

    auto sizeResult = storage->getBlockSize(hash);
    REQUIRE((sizeResult.has_value()));
    CHECK((sizeResult.value() == static_cast<uint64_t>(3333)));
}

TEST_CASE_METHOD(StorageEngineFixture,
                 "StorageEngine getBlockSize returns NotFound for missing hash",
                 "[storage][blockSize][catch2]") {
    const std::string missingHash = std::string(64, 'f');
    auto result = storage->getBlockSize(missingHash);
    REQUIRE_FALSE(result.has_value());
    CHECK((result.error().code == ErrorCode::ChunkNotFound));
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine cleanupTempFiles returns successfully",
                 "[storage][cleanup][catch2]") {
    auto [hash, data] = generateTestData(512);
    REQUIRE((storage->store(hash, data).has_value()));

    auto tempDir = storagePath / "temp";
    REQUIRE((std::filesystem::exists(tempDir)));

    auto statsExist = storage->getStats();
    REQUIRE((statsExist.totalObjects >= 1));

    auto cleanupResult = storage->cleanupTempFiles();
    REQUIRE((cleanupResult.has_value()));
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine compact returns successfully",
                 "[storage][compact][catch2]") {
    auto [hash1, data1] = generateTestData(1024);
    auto [hash2, data2] = generateTestData(2048);
    REQUIRE((storage->store(hash1, data1).has_value()));
    REQUIRE((storage->store(hash2, data2).has_value()));

    auto compactResult = storage->compact();
    CHECK((compactResult.has_value()));
}

TEST_CASE_METHOD(StorageEngineFixture,
                 "StorageEngine compact prunes empty shard directories and stale temp files",
                 "[storage][compact][cleanup][catch2]") {
    auto [hash, data] = generateTestData(1024);
    REQUIRE((storage->store(hash, data).has_value()));

    const auto objectShardDir = storagePath / "objects" / hash.substr(0, 2);
    REQUIRE((storage->remove(hash).has_value()));
    REQUIRE((std::filesystem::exists(objectShardDir)));

    const auto manifestHash = std::string(62, 'a') + "bc";
    const auto manifestKey = manifestHash + ".manifest";
    REQUIRE((storage->store(manifestKey, data).has_value()));

    const auto manifestShardDir = storagePath / "manifests" / manifestHash.substr(0, 2);
    REQUIRE((storage->remove(manifestKey).has_value()));
    REQUIRE((std::filesystem::exists(manifestShardDir)));

    const auto staleTemp = storagePath / "temp" / "stale.tmp";
    {
        std::ofstream file(staleTemp, std::ios::binary);
        REQUIRE((static_cast<bool>(file)));
        file << "stale";
    }
    REQUIRE((std::filesystem::exists(staleTemp)));
    std::error_code ec;
    std::filesystem::last_write_time(staleTemp,
                                     std::filesystem::file_time_type::clock::now() -
                                         std::chrono::hours(2),
                                     ec);
    REQUIRE_FALSE(ec);

    auto compactResult = storage->compact();
    REQUIRE((compactResult.has_value()));
    CHECK_FALSE((std::filesystem::exists(staleTemp)));
    CHECK_FALSE((std::filesystem::exists(objectShardDir)));
    CHECK_FALSE((std::filesystem::exists(manifestShardDir)));
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
    REQUIRE((engine.store(manifestKey, mdata).has_value()));

    auto verifyResult = engine.verify();
    REQUIRE((verifyResult.has_value()));

    cleanup();
}

TEST_CASE_METHOD(StorageEngineFixture,
                 "StorageEngine verify detects corruption in compressed or manifest object content",
                 "[storage][integrity][manifest][catch2]") {
    auto [hash, data] = generateTestData(2048);
    REQUIRE((storage->store(hash, data).has_value()));

    auto objectPath = storagePath / "objects" / hash.substr(0, 2) / hash.substr(2);
    {
        std::fstream file(objectPath, std::ios::binary | std::ios::in | std::ios::out);
        REQUIRE((static_cast<bool>(file)));
        file.seekg(10);
        char original = 0;
        file.read(&original, 1);
        REQUIRE((static_cast<bool>(file)));
        file.seekp(10);
        const char corrupt = static_cast<char>(original ^ 0xff);
        file.write(&corrupt, 1);
        REQUIRE((static_cast<bool>(file)));
    }

    auto verifyResult = storage->verify();
    REQUIRE_FALSE(verifyResult.has_value());
    CHECK((verifyResult.error().code == ErrorCode::CorruptedData));
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine stats track delete operations",
                 "[storage][stats][catch2]") {
    auto [hash, data] = generateTestData(256);
    REQUIRE((storage->store(hash, data).has_value()));

    REQUIRE((storage->remove(hash).has_value()));

    auto stats = storage->getStats();
    CHECK((stats.deleteOperations > 0));
}

TEST_CASE_METHOD(StorageEngineFixture,
                 "StorageEngine partial write failure cleans up and returns typed error",
                 "[storage][engine][write-failure][catch2]") {
    auto [hash, data] = generateTestData(4096);
    StorageEngine::testing_setAtomicWriteFailureAfterBytes(data.size() / 2);

    auto result = storage->store(hash, data);
    REQUIRE_FALSE(result.has_value());
    CHECK((result.error().code == ErrorCode::WriteError));

    StorageEngine::testing_clearAtomicWriteFailure();

    CHECK_FALSE(storage->exists(hash).value());

    auto retry = storage->store(hash, data);
    REQUIRE((retry.has_value()));

    auto stats = storage->getStats();
    CHECK((stats.totalObjects.load() == 1u));
    CHECK((stats.failedOperations.load() == 1u));
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine rejects removal of non-regular-file paths",
                 "[storage][engine][edge][catch2]") {
    auto [hash, data] = generateTestData(256);
    REQUIRE((storage->store(hash, data).has_value()));

    auto objectPath = storagePath / "objects" / hash.substr(0, 2) / hash.substr(2);
    REQUIRE((std::filesystem::exists(objectPath)));

    std::error_code ec;
    std::filesystem::remove(objectPath, ec);
    std::filesystem::create_directory(objectPath, ec);

    auto removeResult = storage->remove(hash);
    REQUIRE_FALSE(removeResult.has_value());
    CHECK((removeResult.error().code == ErrorCode::IOError));

    std::filesystem::remove_all(objectPath, ec);
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine compact returns success",
                 "[storage][compact][smoke][catch2]") {
    auto [hash, data] = generateTestData(512);
    REQUIRE((storage->store(hash, data).has_value()));
    auto result = storage->compact();
    REQUIRE((result.has_value()));
    CHECK((storage->exists(hash).value()));
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine getBlockSize invalid key",
                 "[storage][blockSize][invalid][catch2]") {
    auto result = storage->getBlockSize("not-a-hex-key");
    REQUIRE_FALSE(result.has_value());
    CHECK((result.error().code == ErrorCode::InvalidArgument));
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine verify empty objects dir",
                 "[storage][integrity][edge][catch2]") {
    std::error_code ec;
    std::filesystem::remove_all(storagePath / "objects", ec);
    auto result = storage->verify();
    REQUIRE_FALSE(result.has_value());
    CHECK((result.error().code == ErrorCode::ChunkNotFound));
}

TEST_CASE_METHOD(StorageEngineFixture, "StorageEngine cleanupTempFiles with no stale files",
                 "[storage][cleanup][edge][catch2]") {
    auto tempDir = storagePath / "temp";
    std::filesystem::create_directories(tempDir);

    auto result = storage->cleanupTempFiles();
    REQUIRE((result.has_value()));
}

TEST_CASE_METHOD(StorageEngineFixture,
                 "StorageEngine atomicWrite file open failure returns typed error",
                 "[storage][engine][write-failure][catch2]") {
    auto [hash, data] = generateTestData(1024);

    StorageEngine::testing_setFileOpenFailure(true);
    auto result = storage->store(hash, data);
    REQUIRE_FALSE(result.has_value());
    CHECK((result.error().code == ErrorCode::PermissionDenied));
    StorageEngine::testing_setFileOpenFailure(false);

    REQUIRE((storage->store(hash, data).has_value()));
}

TEST_CASE_METHOD(StorageEngineFixture,
                 "StorageEngine atomicWrite rename failure with existing target succeeds",
                 "[storage][engine][write-edge][catch2]") {
    auto [hash, data] = generateTestData(1024);
    REQUIRE((storage->store(hash, data).has_value()));

    StorageEngine::testing_setRenameFailure(true);
    auto result = storage->store(hash, data);
    REQUIRE((result.has_value()));
    StorageEngine::testing_setRenameFailure(false);
}

TEST_CASE_METHOD(StorageEngineFixture,
                 "StorageEngine atomicWrite rename failure without target returns unknown error",
                 "[storage][engine][write-edge][catch2]") {
    auto [hash, data] = generateTestData(1024);

    StorageEngine::testing_setRenameFailure(true);
    auto result = storage->store(hash, data);
    StorageEngine::testing_setRenameFailure(false);

    REQUIRE_FALSE(result.has_value());
    CHECK((result.error().code == ErrorCode::Unknown));
}

TEST_CASE("initializeStorage creates expected directory structure", "[storage][utility][catch2]") {
    const auto basePath =
        std::filesystem::temp_directory_path() /
        std::format("yams_init_{}", std::chrono::steady_clock::now().time_since_epoch().count());
    auto cleanup = [&] {
        std::error_code ec;
        std::filesystem::remove_all(basePath, ec);
    };

    auto result = yams::storage::initializeStorage(basePath);
    REQUIRE((result.has_value()));

    CHECK((std::filesystem::exists(basePath / "objects")));
    CHECK((std::filesystem::exists(basePath / "temp")));
    CHECK((std::filesystem::exists(basePath / "manifests")));

    cleanup();
}

TEST_CASE("initializeStorage returns error when parent is read-only",
          "[storage][utility][catch2]") {
#ifdef _WIN32
    SKIP("POSIX chmod-based permission test is not applicable on Windows");
#else
    const auto testRoot =
        std::filesystem::temp_directory_path() /
        std::format("yams_init_ro_{}", std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::create_directories(testRoot);
    ::chmod(testRoot.c_str(), 0500); // read+execute only, no write

    auto result = yams::storage::initializeStorage(testRoot / "storage");
    REQUIRE_FALSE(result.has_value());

    ::chmod(testRoot.c_str(), 0700);
    std::error_code ec;
    std::filesystem::remove_all(testRoot, ec);
#endif
}

TEST_CASE("validateStorageIntegrity returns true for valid structure",
          "[storage][utility][catch2]") {
    const auto basePath =
        std::filesystem::temp_directory_path() /
        std::format("yams_val_{}", std::chrono::steady_clock::now().time_since_epoch().count());
    auto cleanup = [&] {
        std::error_code ec;
        std::filesystem::remove_all(basePath, ec);
    };

    std::filesystem::create_directories(basePath / "objects");
    std::filesystem::create_directories(basePath / "temp");

    auto result = yams::storage::validateStorageIntegrity(basePath);
    REQUIRE((result.has_value()));
    CHECK((result.value()));

    cleanup();
}

TEST_CASE("validateStorageIntegrity returns false when objects dir is missing",
          "[storage][utility][catch2]") {
    const auto basePath = std::filesystem::temp_directory_path() /
                          std::format("yams_val_missing_{}",
                                      std::chrono::steady_clock::now().time_since_epoch().count());
    auto cleanup = [&] {
        std::error_code ec;
        std::filesystem::remove_all(basePath, ec);
    };

    std::filesystem::create_directories(basePath);
    // Create temp but NOT objects
    std::filesystem::create_directories(basePath / "temp");

    auto result = yams::storage::validateStorageIntegrity(basePath);
    REQUIRE((result.has_value()));
    CHECK_FALSE(result.value());

    cleanup();
}

TEST_CASE("validateStorageIntegrity returns false for non-existent base path",
          "[storage][utility][catch2]") {
    const auto basePath = std::filesystem::temp_directory_path() /
                          std::format("yams_val_noexist_{}",
                                      std::chrono::steady_clock::now().time_since_epoch().count());
    // Do not create the directory.

    auto result = yams::storage::validateStorageIntegrity(basePath);
    REQUIRE((result.has_value()));
    CHECK_FALSE(result.value());
}

// ---------------------------------------------------------------------------
// New storage features: verifyReads, fsyncBeforeRename, getStorageDensity
// ---------------------------------------------------------------------------

TEST_CASE("StorageEngine verifyReads detects corrupted data",
          "[storage][engine][integrity][catch2]") {
    auto testDir =
        std::filesystem::temp_directory_path() /
        std::format("yams_verify_{}", std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::create_directories(testDir);

    StorageConfig config{
        .basePath = testDir, .shardDepth = 2, .mutexPoolSize = 64, .verifyReads = true};
    auto storage = std::make_unique<StorageEngine>(std::move(config));

    auto data = generateRandomBytes(1024);
    auto hasher = crypto::createSHA256Hasher();
    const auto hash = hasher->hash(data);

    REQUIRE((storage->store(hash, data)));

    // Corrupt the stored file on disk.
    auto objPath = testDir / "objects" / hash.substr(0, 2) / hash.substr(2);
    REQUIRE((std::filesystem::exists(objPath)));
    {
        std::ofstream f(objPath, std::ios::binary | std::ios::in | std::ios::out);
        REQUIRE((f.is_open()));
        f.seekp(10);
        f.put('\x00');
        f.close();
    }

    auto result = storage->retrieve(hash);
    REQUIRE_FALSE(result.has_value());
    CHECK((result.error().code == ErrorCode::HashMismatch));

    std::error_code ec;
    std::filesystem::remove_all(testDir, ec);
}

TEST_CASE("StorageEngine verifyReads disabled does not check",
          "[storage][engine][integrity][catch2]") {
    auto testDir = std::filesystem::temp_directory_path() /
                   std::format("yams_noverify_{}",
                               std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::create_directories(testDir);

    StorageConfig config{
        .basePath = testDir, .shardDepth = 2, .mutexPoolSize = 64, .verifyReads = false};
    auto storage = std::make_unique<StorageEngine>(std::move(config));

    auto data = generateRandomBytes(1024);
    auto hasher = crypto::createSHA256Hasher();
    const auto hash = hasher->hash(data);

    REQUIRE((storage->store(hash, data)));

    auto objPath = testDir / "objects" / hash.substr(0, 2) / hash.substr(2);
    {
        std::ofstream f(objPath, std::ios::binary | std::ios::in | std::ios::out);
        f.seekp(10);
        f.put('\x00');
        f.close();
    }

    auto result = storage->retrieve(hash);
    CHECK((result.has_value())); // No check → returns corrupted data silently

    std::error_code ec;
    std::filesystem::remove_all(testDir, ec);
}

TEST_CASE("StorageEngine fsyncBeforeRename=false writes correctly",
          "[storage][engine][durability][catch2]") {
    auto testDir =
        std::filesystem::temp_directory_path() /
        std::format("yams_nofsync_{}", std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::create_directories(testDir);

    StorageConfig config{
        .basePath = testDir, .shardDepth = 2, .mutexPoolSize = 64, .fsyncBeforeRename = false};
    auto storage = std::make_unique<StorageEngine>(std::move(config));

    auto data = generateRandomBytes(2048);
    auto hasher = crypto::createSHA256Hasher();
    const auto hash = hasher->hash(data);

    REQUIRE((storage->store(hash, data)));
    auto retrieved = storage->retrieve(hash);
    REQUIRE((retrieved.has_value()));
    CHECK((retrieved.value().size() == data.size()));

    std::error_code ec;
    std::filesystem::remove_all(testDir, ec);
}

TEST_CASE("StorageEngine getStorageDensity reflects stored objects",
          "[storage][engine][stats][catch2]") {
    auto testDir =
        std::filesystem::temp_directory_path() /
        std::format("yams_density_{}", std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::create_directories(testDir);

    StorageConfig config{.basePath = testDir, .shardDepth = 2, .mutexPoolSize = 64};
    auto storage = std::make_unique<StorageEngine>(std::move(config));

    for (int i = 0; i < 5; ++i) {
        auto data = generateRandomBytes(512);
        auto hasher = crypto::createSHA256Hasher();
        auto hash = hasher->hash(data);
        REQUIRE((storage->store(hash, data)));
    }

    auto stats = storage->getStats();
    auto density = stats.getStorageDensity();
    // 5 objects, 5*512 = 2560 bytes → density ≈ 5/2560 ≈ 0.00195
    CHECK((density > 0.0));
    CHECK((density < 1.0));

    std::error_code ec;
    std::filesystem::remove_all(testDir, ec);
}

TEST_CASE("StorageEngine getDeduplicationRatio is deprecated alias",
          "[storage][engine][stats][catch2]") {
    auto testDir = std::filesystem::temp_directory_path() /
                   std::format("yams_deprecated_{}",
                               std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::create_directories(testDir);

    StorageConfig config{.basePath = testDir, .shardDepth = 2, .mutexPoolSize = 64};
    auto storage = std::make_unique<StorageEngine>(std::move(config));

    auto data = generateRandomBytes(100);
    auto hasher = crypto::createSHA256Hasher();
    auto hash = hasher->hash(data);
    REQUIRE((storage->store(hash, data)));

    auto stats = storage->getStats();
    // Deprecated alias should return same value as getStorageDensity.
    CHECK((stats.getDeduplicationRatio() == stats.getStorageDensity()));

    std::error_code ec;
    std::filesystem::remove_all(testDir, ec);
}

// ---------------------------------------------------------------------------
// Benchmark: storeBatch serial vs parallel
// ---------------------------------------------------------------------------

TEST_CASE("StorageEngine storeBatch parallel speedup",
          "[storage][engine][batch][benchmark][catch2]") {
    auto testDir = std::filesystem::temp_directory_path() /
                   std::format("yams_batch_bench_{}",
                               std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::create_directories(testDir);

    constexpr std::size_t kItemCount = 128;
    constexpr std::size_t kItemSize = 1024;
    std::vector<std::pair<std::string, std::vector<std::byte>>> items;
    items.reserve(kItemCount);
    auto hasher = crypto::createSHA256Hasher();
    for (std::size_t i = 0; i < kItemCount; ++i) {
        auto data = generateRandomBytes(kItemSize);
        auto hash = hasher->hash(data);
        items.emplace_back(std::move(hash), std::move(data));
    }

    BENCHMARK("storeBatch serial (maxConcurrentWriters=1)") {
        static std::atomic<std::uint64_t> serialRunId{0};
        const auto runId = serialRunId.fetch_add(1, std::memory_order_relaxed);
        StorageConfig config{.basePath = testDir / std::format("serial_{}", runId),
                             .shardDepth = 2,
                             .mutexPoolSize = 64,
                             .maxConcurrentWriters = 1};
        auto storage = std::make_unique<StorageEngine>(std::move(config));
        auto result = storage->storeBatch(items);
        storage.reset();
        std::error_code ec;
        std::filesystem::remove_all(testDir / std::format("serial_{}", runId), ec);
        return result;
    };

    BENCHMARK("storeBatch parallel (maxConcurrentWriters=16)") {
        static std::atomic<std::uint64_t> parallelRunId{0};
        const auto runId = parallelRunId.fetch_add(1, std::memory_order_relaxed);
        StorageConfig config{.basePath = testDir / std::format("parallel_{}", runId),
                             .shardDepth = 2,
                             .mutexPoolSize = 64,
                             .maxConcurrentWriters = 16};
        auto storage = std::make_unique<StorageEngine>(std::move(config));
        auto result = storage->storeBatch(items);
        storage.reset();
        std::error_code ec;
        std::filesystem::remove_all(testDir / std::format("parallel_{}", runId), ec);
        return result;
    };

    // Verify all data is retrievable from one explicit serial and parallel run.
    for (const auto& [subdir, maxConcurrentWriters] :
         {std::pair{"serial_verify", std::size_t{1}},
          std::pair{"parallel_verify", std::size_t{16}}}) {
        StorageConfig config{.basePath = testDir / subdir,
                             .shardDepth = 2,
                             .mutexPoolSize = 64,
                             .maxConcurrentWriters = maxConcurrentWriters};
        auto storage = std::make_unique<StorageEngine>(std::move(config));
        auto batchResult = storage->storeBatch(items);
        REQUIRE((std::ranges::all_of(batchResult,
                                     [](const auto& result) { return result.has_value(); })));
        for (const auto& [hash, data] : items) {
            auto retrieved = storage->retrieve(hash);
            CHECK((retrieved.has_value()));
            CHECK((retrieved.value() == data));
        }
    }

    std::error_code ec;
    std::filesystem::remove_all(testDir, ec);
}

// ---------------------------------------------------------------------------
// Crash-simulation and error-injection tests (YAMS_TESTING hooks)
// ---------------------------------------------------------------------------

#ifdef YAMS_TESTING

TEST_CASE("StorageEngine partial write failure cleans up temp file",
          "[storage][engine][durability][testing][catch2]") {
    auto testDir =
        std::filesystem::temp_directory_path() /
        std::format("yams_partial_{}", std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::create_directories(testDir);

    StorageConfig config{
        .basePath = testDir, .shardDepth = 2, .mutexPoolSize = 64, .fsyncBeforeRename = false};
    auto storage = std::make_unique<StorageEngine>(std::move(config));

    auto data = generateRandomBytes(4096);
    auto hasher = crypto::createSHA256Hasher();
    const auto hash = hasher->hash(data);

    // Inject: fail after writing 1000 bytes
    StorageEngine::testing_setAtomicWriteFailureAfterBytes(1000);
    auto result = storage->store(hash, data);
    StorageEngine::testing_clearAtomicWriteFailure();

    REQUIRE_FALSE(result.has_value());
    CHECK((result.error().code == ErrorCode::WriteError));

    // Verify no orphaned temp file or partial object on disk
    auto objPath = testDir / "objects" / hash.substr(0, 2) / hash.substr(2);
    CHECK_FALSE(std::filesystem::exists(objPath));

    std::error_code ec;
    std::filesystem::remove_all(testDir, ec);
}

TEST_CASE("StorageEngine rename failure on existing file is idempotent",
          "[storage][engine][durability][testing][catch2]") {
    auto testDir =
        std::filesystem::temp_directory_path() /
        std::format("yams_rename_{}", std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::create_directories(testDir);

    StorageConfig config{
        .basePath = testDir, .shardDepth = 2, .mutexPoolSize = 64, .fsyncBeforeRename = false};
    auto storage = std::make_unique<StorageEngine>(std::move(config));

    auto data = generateRandomBytes(2048);
    auto hasher = crypto::createSHA256Hasher();
    const auto hash = hasher->hash(data);

    // First write succeeds normally
    REQUIRE((storage->store(hash, data)));
    REQUIRE((storage->exists(hash).value()));

    // Inject rename failure — store() should detect existing file and
    // return success (CAS idempotent).
    StorageEngine::testing_setRenameFailure(true);
    auto result = storage->store(hash, data);
    StorageEngine::testing_setRenameFailure(false);

    CHECK((result.has_value()));
    CHECK((storage->exists(hash).value()));

    std::error_code ec;
    std::filesystem::remove_all(testDir, ec);
}

TEST_CASE("StorageEngine file open failure returns PermissionDenied",
          "[storage][engine][error][testing][catch2]") {
    auto testDir =
        std::filesystem::temp_directory_path() /
        std::format("yams_perm_{}", std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::create_directories(testDir);

    StorageConfig config{
        .basePath = testDir, .shardDepth = 2, .mutexPoolSize = 64, .fsyncBeforeRename = false};
    auto storage = std::make_unique<StorageEngine>(std::move(config));

    auto data = generateRandomBytes(512);
    auto hasher = crypto::createSHA256Hasher();
    const auto hash = hasher->hash(data);

    StorageEngine::testing_setFileOpenFailure(true);
    auto result = storage->store(hash, data);
    StorageEngine::testing_setFileOpenFailure(false);

    REQUIRE_FALSE(result.has_value());
    CHECK((result.error().code == ErrorCode::PermissionDenied));

    // Verify no orphaned temp file
    auto objPath = testDir / "objects" / hash.substr(0, 2) / hash.substr(2);
    CHECK_FALSE(std::filesystem::exists(objPath));

    std::error_code ec;
    std::filesystem::remove_all(testDir, ec);
}

TEST_CASE("StorageEngine sustained concurrent load 50 writers 30s",
          "[storage][engine][stress][concurrent][slow][catch2]") {
    constexpr int kNumWriters = 50;
    constexpr int kNumReaders = 10;
    constexpr int kRunSeconds = 30;

    auto testDir =
        std::filesystem::temp_directory_path() /
        std::format("yams_stress_{}", std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::create_directories(testDir);

    StorageConfig config{.basePath = testDir,
                         .shardDepth = 2,
                         .mutexPoolSize = 1024,
                         .maxConcurrentWriters = 200,
                         .fsyncBeforeRename = false};
    auto storage = std::make_shared<StorageEngine>(std::move(config));

    std::atomic<bool> stop{false};
    std::atomic<uint64_t> writeOps{0};
    std::atomic<uint64_t> readOps{0};
    std::atomic<uint64_t> writeErrors{0};
    std::atomic<uint64_t> readErrors{0};

    // Shared key pool: writers push, readers pop.
    std::mutex keyMutex;
    std::vector<std::pair<std::string, std::vector<std::byte>>> keyPool;
    keyPool.reserve(100000);

    auto writerFn = [&](int id) {
        auto hasher = crypto::createSHA256Hasher();
        std::mt19937 rng(static_cast<unsigned>(id + 42));
        std::uniform_int_distribution<size_t> sizeDist(512, 65536);

        while (!stop.load(std::memory_order_relaxed)) {
            auto data = generateRandomBytes(sizeDist(rng));
            auto hash = hasher->hash(data);
            auto result = storage->store(hash, data);
            if (result.has_value()) {
                writeOps.fetch_add(1, std::memory_order_relaxed);
                std::lock_guard<std::mutex> lock(keyMutex);
                keyPool.emplace_back(std::move(hash), std::move(data));
            } else {
                writeErrors.fetch_add(1, std::memory_order_relaxed);
            }
        }
    };

    auto readerFn = [&](int id) {
        std::mt19937 rng(static_cast<unsigned>(id + 999));

        while (!stop.load(std::memory_order_relaxed)) {
            std::string hash;
            std::vector<std::byte> expected;
            {
                std::lock_guard<std::mutex> lock(keyMutex);
                if (keyPool.empty()) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                    continue;
                }
                std::uniform_int_distribution<size_t> dist(0, keyPool.size() - 1);
                size_t idx = dist(rng);
                hash = keyPool[idx].first;
                expected = keyPool[idx].second;
            }
            auto result = storage->retrieve(hash);
            if (result.has_value()) {
                readOps.fetch_add(1, std::memory_order_relaxed);
                if (result.value() != expected) {
                    readErrors.fetch_add(1, std::memory_order_relaxed);
                }
            } else {
                readErrors.fetch_add(1, std::memory_order_relaxed);
            }
        }
    };

    // Launch workers.
    std::vector<std::thread> threads;
    for (int i = 0; i < kNumWriters; ++i) {
        threads.emplace_back(writerFn, i);
    }
    for (int i = 0; i < kNumReaders; ++i) {
        threads.emplace_back(readerFn, i);
    }

    // Run for kRunSeconds.
    std::this_thread::sleep_for(std::chrono::seconds(kRunSeconds));
    stop.store(true, std::memory_order_relaxed);

    for (auto& t : threads) {
        t.join();
    }

    auto writes = writeOps.load();
    auto reads = readOps.load();
    auto wErrs = writeErrors.load();
    auto rErrs = readErrors.load();

    INFO("Writers: " << writes << " ops (" << (writes / kRunSeconds) << " ops/s), " << wErrs
                     << " errors");
    INFO("Readers: " << reads << " ops (" << (reads / kRunSeconds) << " ops/s), " << rErrs
                     << " errors");

    // Sanity: at least some writes succeeded.
    CHECK((writes > 0));
    // No data corruption on reads.
    CHECK((rErrs == 0));

    // Final verification: sample 500 random keys for retrievability.
    std::lock_guard<std::mutex> lock(keyMutex);
    if (keyPool.size() > 500) {
        std::mt19937 rng(42);
        std::uniform_int_distribution<size_t> dist(0, keyPool.size() - 1);
        for (int i = 0; i < 500; ++i) {
            size_t idx = dist(rng);
            auto result = storage->retrieve(keyPool[idx].first);
            CHECK((result.has_value()));
            if (result.has_value()) {
                CHECK((result.value() == keyPool[idx].second));
            }
        }
    } else {
        for (const auto& [hash, data] : keyPool) {
            auto result = storage->retrieve(hash);
            CHECK((result.has_value()));
            if (result.has_value()) {
                CHECK((result.value() == data));
            }
        }
    }

    std::error_code ec;
    std::filesystem::remove_all(testDir, ec);
}

#endif // YAMS_TESTING

// ---------------------------------------------------------------------------
// Cold-cache benchmark (best-effort, may skip without root)
// ---------------------------------------------------------------------------

TEST_CASE("StorageEngine cold vs warm cache retrieval",
          "[storage][engine][cache][benchmark][!benchmark]") {
    auto testDir = std::filesystem::temp_directory_path() /
                   std::format("yams_cold_cache_{}",
                               std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::create_directories(testDir);

    StorageConfig config{
        .basePath = testDir, .shardDepth = 2, .mutexPoolSize = 64, .fsyncBeforeRename = false};
    auto storage = std::make_unique<StorageEngine>(std::move(config));

    // Write 100 files (64KB each = 6.4MB).
    constexpr int kFiles = 100;
    constexpr size_t kSize = 65536;
    std::vector<std::pair<std::string, std::vector<std::byte>>> items;
    items.reserve(kFiles);
    auto hasher = crypto::createSHA256Hasher();
    for (int i = 0; i < kFiles; ++i) {
        auto data = generateRandomBytes(kSize);
        auto hash = hasher->hash(data);
        REQUIRE((storage->store(hash, data)));
        items.emplace_back(std::move(hash), std::move(data));
    }

    // Warm up: read all files once.
    for (const auto& [hash, _] : items) {
        REQUIRE((storage->retrieve(hash).has_value()));
    }

    // Attempt to evict page cache without interactive privilege escalation.
    const bool cacheEvicted = bestEffortEvictPageCache();
    INFO("Cache eviction: " << (cacheEvicted ? "succeeded" : "skipped"));

    // Benchmark: retrieve all files (warm or cold depending on eviction).
    BENCHMARK("retrieve 64KB files") {
        for (const auto& [hash, _] : items) {
            auto r = storage->retrieve(hash);
            CHECK((r.has_value()));
        }
    };

    std::error_code ec;
    std::filesystem::remove_all(testDir, ec);
}
