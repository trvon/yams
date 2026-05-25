// Catch2 tests for compressed storage engine
// Migrated from GTest: compressed_storage_engine_test.cpp
// NOTE: Some GTest tests referenced APIs that don't exist in the actual headers.
// This file tests the actual CompressedStorageEngine API as documented.

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <format>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <thread>
#include <vector>

#include <yams/compression/compression_header.h>
#include <yams/compression/compression_policy.h>
#include <yams/compression/compression_stats.h>
#include <yams/crypto/hasher.h>
#include <yams/storage/compressed_storage_engine.h>
#include <yams/storage/storage_engine.h>

#include "src/compression/zstandard_compressor.h"

using namespace yams;
using namespace yams::storage;
using namespace yams::compression;

namespace {

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

std::vector<std::byte> generateHighEntropyData(size_t size) {
    std::vector<std::byte> data(size);
    uint64_t state = 0x9e3779b97f4a7c15ULL;
    for (size_t i = 0; i < size; ++i) {
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        data[i] = static_cast<std::byte>((state * 0x2545F4914F6CDD1DULL) >> 56);
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

class CapturingStorageEngine final : public IStorageEngine {
public:
    Result<void> store(std::string_view hash, std::span<const std::byte> data) override {
        std::lock_guard lock(mutex_);
        objects_[std::string(hash)] = std::vector<std::byte>(data.begin(), data.end());
        return {};
    }

    Result<std::vector<std::byte>> retrieve(std::string_view hash) const override {
        auto raw = retrieveRaw(hash);
        if (!raw) {
            return raw.error();
        }
        return raw.value().data;
    }

    Result<RawObject> retrieveRaw(std::string_view hash) const override {
        std::lock_guard lock(mutex_);
        auto it = objects_.find(std::string(hash));
        if (it == objects_.end()) {
            return Error{ErrorCode::ChunkNotFound, "captured key not found"};
        }
        RawObject out;
        out.data = it->second;
        auto parsed = CompressionHeader::parse(out.data);
        if (parsed) {
            const auto& header = parsed.value();
            if (header.compressedSize <= out.data.size() &&
                out.data.size() ==
                    CompressionHeader::SIZE + static_cast<size_t>(header.compressedSize)) {
                out.header = header;
            }
        }
        return out;
    }

    Result<bool> exists(std::string_view hash) const noexcept override {
        try {
            std::lock_guard lock(mutex_);
            return objects_.contains(std::string(hash));
        } catch (...) {
            return Error{ErrorCode::InternalError, "exists failed"};
        }
    }

    Result<void> remove(std::string_view hash) override {
        std::lock_guard lock(mutex_);
        objects_.erase(std::string(hash));
        return {};
    }

    Result<uint64_t> getBlockSize(std::string_view hash) const override {
        auto raw = retrieveRaw(hash);
        if (!raw) {
            return raw.error();
        }
        return static_cast<uint64_t>(raw.value().data.size());
    }

    std::future<Result<void>> storeAsync(std::string_view hash,
                                         std::span<const std::byte> data) override {
        return std::async(std::launch::async,
                          [this, hash = std::string(hash),
                           copy = std::vector<std::byte>(data.begin(), data.end())]() {
                              return store(hash, copy);
                          });
    }

    std::future<Result<std::vector<std::byte>>>
    retrieveAsync(std::string_view hash) const override {
        return std::async(std::launch::async,
                          [this, hash = std::string(hash)]() { return retrieve(hash); });
    }

    std::future<Result<RawObject>> retrieveRawAsync(std::string_view hash) const override {
        return std::async(std::launch::async,
                          [this, hash = std::string(hash)]() { return retrieveRaw(hash); });
    }

    std::vector<Result<void>>
    storeBatch(const std::vector<std::pair<std::string, std::vector<std::byte>>>& items) override {
        std::vector<Result<void>> out;
        out.reserve(items.size());
        for (const auto& [hash, data] : items) {
            out.push_back(store(hash, data));
        }
        return out;
    }

    yams::storage::StorageStats getStats() const noexcept override { return {}; }

    Result<std::vector<std::string>> list(std::string_view = "") const override {
        std::lock_guard lock(mutex_);
        std::vector<std::string> keys;
        for (const auto& [k, _] : objects_) {
            keys.push_back(k);
        }
        return keys;
    }

    Result<uint64_t> getStorageSize() const override {
        std::lock_guard lock(mutex_);
        uint64_t total = 0;
        for (const auto& [_, data] : objects_) {
            total += data.size();
        }
        return total;
    }

    std::vector<std::byte> captured(std::string_view hash) const {
        std::lock_guard lock(mutex_);
        return objects_.at(std::string(hash));
    }

private:
    mutable std::mutex mutex_;
    std::map<std::string, std::vector<std::byte>> objects_;
};

struct ScopedZstdUnavailable {
    ScopedZstdUnavailable() {
        CompressionRegistry::instance().registerCompressor(CompressionAlgorithm::Zstandard,
                                                           [] { return nullptr; });
    }

    ~ScopedZstdUnavailable() {
        CompressionRegistry::instance().registerCompressor(CompressionAlgorithm::Zstandard, [] {
            return std::make_unique<ZstandardCompressor>();
        });
    }
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
    auto storeResult = engine->store(hash, data);
    REQUIRE(storeResult.has_value());

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
        auto storeResult = engine->store(hash, data);
        REQUIRE(storeResult.has_value());
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

    auto storeResult = engine->store(hash, data);
    REQUIRE(storeResult.has_value());
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
    newRules.neverCompressBelow = 8192;             // Change from default 4096
    newRules.alwaysCompressAbove = 5 * 1024 * 1024; // 5MB instead of 10MB
    engine->setPolicyRules(newRules);

    auto updatedRules = engine->getPolicyRules();
    CHECK(updatedRules.neverCompressBelow == 8192);
    CHECK(updatedRules.alwaysCompressAbove == 5 * 1024 * 1024);

    auto data = generateCompressibleData(4096);
    auto hash = yams::crypto::SHA256Hasher::hash(std::span<const std::byte>(data));
    REQUIRE(engine->store(hash, data).has_value());

    auto raw = underlying->retrieveRaw(hash);
    REQUIRE(raw.has_value());
    CHECK_FALSE(raw.value().header.has_value());
    CHECK(raw.value().data == data);
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

TEST_CASE_METHOD(CompressedStorageFixture,
                 "CompressedStorageEngine does not treat KRNC-prefixed raw data as compressed",
                 "[storage][compressed][header][catch2]") {
    CompressedStorageEngine::Config config;
    config.enableCompression = true;
    config.asyncCompression = false;

    auto engine = std::make_unique<CompressedStorageEngine>(underlying, config);

    std::string hash = std::format("{:064x}", 0xabc123);
    std::vector<std::byte> data(CompressionHeader::SIZE, std::byte{0x42});
    const uint32_t magic = CompressionHeader::MAGIC;
    std::memcpy(data.data(), &magic, sizeof(magic));

    auto storeResult = engine->store(hash, data);
    REQUIRE(storeResult.has_value());

    auto retrieveResult = engine->retrieve(hash);
    REQUIRE(retrieveResult.has_value());
    CHECK(retrieveResult.value() == data);
}

TEST_CASE_METHOD(CompressedStorageFixture,
                 "CompressedStorageEngine stores blocks that base StorageEngine can verify",
                 "[storage][compressed][integrity][catch2]") {
    CompressedStorageEngine::Config config;
    config.enableCompression = true;
    config.asyncCompression = false;
    config.compressionThreshold = 64;
    config.policyRules.neverCompressBelow = 64;

    auto engine = std::make_unique<CompressedStorageEngine>(underlying, config);

    auto data = generateCompressibleData(8192);
    auto hasher = yams::crypto::createSHA256Hasher();
    auto hash = hasher->hash(data);

    auto storeResult = engine->store(hash, data);
    REQUIRE(storeResult.has_value());

    auto verifyResult = underlying->verify();
    REQUIRE(verifyResult.has_value());
}

TEST_CASE_METHOD(CompressedStorageFixture,
                 "CompressedStorageEngine stores original bytes when compression is not smaller",
                 "[storage][compressed][optimality][catch2]") {
    CompressedStorageEngine::Config config;
    config.enableCompression = true;
    config.asyncCompression = false;
    config.compressionThreshold = 64;
    config.policyRules.neverCompressBelow = 64;

    auto engine = std::make_unique<CompressedStorageEngine>(underlying, config);

    auto data = generateHighEntropyData(8192);
    auto hash = yams::crypto::SHA256Hasher::hash(std::span<const std::byte>(data));

    auto storeResult = engine->store(hash, data);
    REQUIRE(storeResult.has_value());

    auto raw = underlying->retrieveRaw(hash);
    REQUIRE(raw.has_value());
    CHECK_FALSE(raw.value().header.has_value());
    CHECK(raw.value().data.size() == data.size());
    CHECK(raw.value().data == data);
}

TEST_CASE("CompressedStorageEngine compresses before generic backend store",
          "[storage][compressed][backend][remote][catch2]") {
    auto capturing = std::make_shared<CapturingStorageEngine>();

    CompressedStorageEngine::Config config;
    config.enableCompression = true;
    config.asyncCompression = false;
    config.compressionThreshold = 64;
    config.policyRules.neverCompressBelow = 64;

    CompressedStorageEngine engine(std::static_pointer_cast<IStorageEngine>(capturing), config);

    auto data = generateCompressibleData(8192);
    auto hash = yams::crypto::SHA256Hasher::hash(std::span<const std::byte>(data));

    REQUIRE(engine.store(hash, data).has_value());

    auto uploaded = capturing->captured(hash);
    CHECK(uploaded.size() < data.size());
    CHECK(CompressionHeader::parse(uploaded).has_value());

    auto readBack = engine.retrieve(hash);
    REQUIRE(readBack.has_value());
    CHECK(readBack.value() == data);
}

TEST_CASE("CompressedStorageEngine stores incompressible bytes unchanged for generic backend",
          "[storage][compressed][backend][remote][optimality][catch2]") {
    auto capturing = std::make_shared<CapturingStorageEngine>();

    CompressedStorageEngine::Config config;
    config.enableCompression = true;
    config.asyncCompression = false;
    config.compressionThreshold = 64;
    config.policyRules.neverCompressBelow = 64;

    CompressedStorageEngine engine(std::static_pointer_cast<IStorageEngine>(capturing), config);

    auto data = generateHighEntropyData(8192);
    auto hash = yams::crypto::SHA256Hasher::hash(std::span<const std::byte>(data));

    REQUIRE(engine.store(hash, data).has_value());

    auto uploaded = capturing->captured(hash);
    CHECK(uploaded == data);
    CHECK_FALSE(CompressionHeader::parse(uploaded).has_value());
}

TEST_CASE("CompressedStorageEngine rejects corrupted compressed payload from generic backend",
          "[storage][compressed][backend][corrupt][catch2]") {
    auto capturing = std::make_shared<CapturingStorageEngine>();

    CompressedStorageEngine::Config config;
    config.enableCompression = true;
    config.asyncCompression = false;
    config.compressionThreshold = 64;
    config.policyRules.neverCompressBelow = 64;

    CompressedStorageEngine engine(std::static_pointer_cast<IStorageEngine>(capturing), config);

    auto data = generateCompressibleData(8192);
    auto hash = yams::crypto::SHA256Hasher::hash(std::span<const std::byte>(data));

    REQUIRE(engine.store(hash, data).has_value());
    auto corrupted = capturing->captured(hash);
    REQUIRE(corrupted.size() > CompressionHeader::SIZE);
    corrupted[CompressionHeader::SIZE] ^= std::byte{0x01};
    REQUIRE(capturing->store(hash, corrupted).has_value());

    auto readBack = engine.retrieve(hash);
    REQUIRE_FALSE(readBack.has_value());
    CHECK(readBack.error().code == ErrorCode::HashMismatch);
}

TEST_CASE("CompressedStorageEngine keeps incomplete header-shaped raw bytes unchanged",
          "[storage][compressed][backend][corrupt][catch2]") {
    auto capturing = std::make_shared<CapturingStorageEngine>();

    CompressedStorageEngine::Config config;
    config.enableCompression = true;
    config.asyncCompression = false;

    CompressedStorageEngine engine(std::static_pointer_cast<IStorageEngine>(capturing), config);

    CompressionHeader header;
    header.algorithm = static_cast<uint8_t>(CompressionAlgorithm::Zstandard);
    header.level = 3;
    header.uncompressedSize = 100;
    header.compressedSize = 10;

    auto raw = header.serialize();
    raw.resize(CompressionHeader::SIZE + 5, std::byte{0x5a});
    std::string hash(64, 'f');
    REQUIRE(capturing->store(hash, raw).has_value());

    auto rawObject = engine.retrieveRaw(hash);
    REQUIRE(rawObject.has_value());
    CHECK_FALSE(rawObject.value().header.has_value());

    auto readBack = engine.retrieve(hash);
    REQUIRE(readBack.has_value());
    CHECK(readBack.value() == raw);
}

TEST_CASE("CompressedStorageEngine stores raw bytes when compressor is unavailable",
          "[storage][compressed][backend][registry][catch2]") {
    auto capturing = std::make_shared<CapturingStorageEngine>();

    CompressedStorageEngine::Config config;
    config.enableCompression = true;
    config.asyncCompression = false;
    config.compressionThreshold = 64;
    config.policyRules.neverCompressBelow = 64;

    CompressedStorageEngine engine(std::static_pointer_cast<IStorageEngine>(capturing), config);

    auto data = generateCompressibleData(8192);
    auto hash = yams::crypto::SHA256Hasher::hash(std::span<const std::byte>(data));

    {
        ScopedZstdUnavailable unavailable;
        REQUIRE(engine.store(hash, data).has_value());
    }

    auto uploaded = capturing->captured(hash);
    CHECK(uploaded == data);
    CHECK_FALSE(CompressionHeader::parse(uploaded).has_value());
}

TEST_CASE("CompressedStorageEngine rejects compressed bytes when decompressor is unavailable",
          "[storage][compressed][backend][registry][catch2]") {
    auto capturing = std::make_shared<CapturingStorageEngine>();

    CompressedStorageEngine::Config config;
    config.enableCompression = true;
    config.asyncCompression = false;
    config.compressionThreshold = 64;
    config.policyRules.neverCompressBelow = 64;

    CompressedStorageEngine engine(std::static_pointer_cast<IStorageEngine>(capturing), config);

    auto data = generateCompressibleData(8192);
    auto hash = yams::crypto::SHA256Hasher::hash(std::span<const std::byte>(data));

    REQUIRE(engine.store(hash, data).has_value());
    auto uploaded = capturing->captured(hash);
    REQUIRE(CompressionHeader::parse(uploaded).has_value());

    {
        ScopedZstdUnavailable unavailable;
        auto readBack = engine.retrieve(hash);
        REQUIRE_FALSE(readBack.has_value());
        CHECK(readBack.error().code == ErrorCode::InvalidArgument);
    }
}

TEST_CASE("CompressedStorageEngine shutdown clears queued async compression jobs",
          "[storage][compressed][async][shutdown][catch2]") {
    auto capturing = std::make_shared<CapturingStorageEngine>();

    CompressedStorageEngine::Config config;
    config.enableCompression = true;
    config.asyncCompression = false;
    config.maxAsyncQueue = 4;

    CompressedStorageEngine engine(std::static_pointer_cast<IStorageEngine>(capturing), config);

    REQUIRE(engine.testing_enqueueCompressionJob(std::string(64, '1')).has_value());
    REQUIRE(engine.testing_enqueueCompressionJob(std::string(64, '2')).has_value());
    CHECK(engine.testing_asyncQueueDepth() == 2);

    engine.testing_shutdownAsyncWorkers();

    CHECK(engine.testing_asyncQueueDepth() == 0);
    CHECK(engine.waitForAsyncOperations(std::chrono::milliseconds{0}));
    auto rejected = engine.testing_enqueueCompressionJob(std::string(64, '3'));
    REQUIRE_FALSE(rejected.has_value());
    CHECK(rejected.error().code == ErrorCode::SystemShutdown);
}

TEST_CASE("CompressedStorageEngine triggerCompressionScan queues uncompressed objects",
          "[storage][compressed][scan][catch2]") {
    auto capturing = std::make_shared<CapturingStorageEngine>();

    CompressedStorageEngine::Config config;
    config.enableCompression = true;
    config.asyncCompression = true;
    config.maxAsyncQueue = 100;
    config.compressionThreshold = 0;
    config.policyRules.neverCompressBelow = 0;
    config.policyRules.neverCompressBefore = std::chrono::hours{0};
    config.policyRules.defaultZstdLevel = 3;

    CompressedStorageEngine engine(std::static_pointer_cast<IStorageEngine>(capturing), config);

    auto data1 = generateCompressibleData(4096);
    auto hash1 = crypto::createSHA256Hasher()->hash(data1);
    auto data2 = generateCompressibleData(8192);
    auto hash2 = crypto::createSHA256Hasher()->hash(data2);

    REQUIRE(capturing->store(hash1, data1).has_value());
    REQUIRE(capturing->store(hash2, data2).has_value());

    auto queued = engine.triggerCompressionScan();
    REQUIRE(queued.has_value());
    CHECK(queued.value() == 2);

    bool finished = engine.waitForAsyncOperations(std::chrono::seconds{5});
    CHECK(finished);

    auto uploaded1 = capturing->captured(hash1);
    CHECK(CompressionHeader::parse(uploaded1).has_value());

    auto uploaded2 = capturing->captured(hash2);
    CHECK(CompressionHeader::parse(uploaded2).has_value());
}
