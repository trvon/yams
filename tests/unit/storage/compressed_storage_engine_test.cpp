#include <random>
#include <thread>
#include <gtest/gtest.h>
#include <yams/compression/compressor_interface.h>
#include <yams/compression/zstandard_compressor.h>
#include <yams/storage/compressed_storage_engine.h>
#include <yams/storage/storage_engine.h>

using namespace yams;
using namespace yams::storage;
using namespace yams::compression;

// Mock storage engine for testing
class MockStorageEngine : public StorageEngine {
public:
    Result<void> put(const Hash& key, ByteSpan data) override {
        std::lock_guard lock(mutex_);
        data_[key] = ByteVector(data.begin(), data.end());
        return {};
    }

    Result<ByteVector> get(const Hash& key) override {
        std::lock_guard lock(mutex_);
        auto it = data_.find(key);
        if (it == data_.end()) {
            return Error(ErrorCode::NotFound);
        }
        return it->second;
    }

    Result<bool> exists(const Hash& key) override {
        std::lock_guard lock(mutex_);
        return data_.contains(key);
    }

    Result<void> remove(const Hash& key) override {
        std::lock_guard lock(mutex_);
        data_.erase(key);
        return {};
    }

    Result<uint64_t> size(const Hash& key) override {
        std::lock_guard lock(mutex_);
        auto it = data_.find(key);
        if (it == data_.end()) {
            return Error(ErrorCode::NotFound);
        }
        return it->second.size();
    }

    Result<std::vector<Hash>> list() override {
        std::lock_guard lock(mutex_);
        std::vector<Hash> keys;
        for (const auto& [key, _] : data_) {
            keys.push_back(key);
        }
        return keys;
    }

    Result<StorageStats> stats() override {
        std::lock_guard lock(mutex_);
        StorageStats stats;
        stats.totalObjects = data_.size();
        stats.totalBytes = 0;
        for (const auto& [_, data] : data_) {
            stats.totalBytes += data.size();
        }
        return stats;
    }

    Result<void> compact() override { return {}; }

    // Test helper
    size_t getRawSize(const Hash& key) {
        std::lock_guard lock(mutex_);
        auto it = data_.find(key);
        return it != data_.end() ? it->second.size() : 0;
    }

private:
    std::unordered_map<Hash, ByteVector> data_;
    mutable std::mutex mutex_;
};

class CompressedStorageEngineTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Register compressors
        CompressionRegistry::instance().registerCompressor(CompressionAlgorithm::Zstandard, []() {
            return std::make_unique<ZstandardCompressor>();
        });

        mockStorage_ = std::make_shared<MockStorageEngine>();
    }

    ByteVector generateData(size_t size, bool compressible = true) {
        ByteVector data(size);
        std::random_device rd;
        std::mt19937 gen(rd());

        if (compressible) {
            // Generate compressible data (repeating patterns)
            std::uniform_int_distribution<> pattern(0, 15);
            std::byte patternByte = static_cast<std::byte>(pattern(gen));
            for (size_t i = 0; i < size; ++i) {
                if (i % 16 == 0) {
                    patternByte = static_cast<std::byte>(pattern(gen));
                }
                data[i] = patternByte;
            }
        } else {
            // Generate random incompressible data
            std::uniform_int_distribution<> dist(0, 255);
            for (auto& byte : data) {
                byte = static_cast<std::byte>(dist(gen));
            }
        }

        return data;
    }

    Hash hashData(ByteSpan data) { return Hash::compute(data); }

    std::shared_ptr<MockStorageEngine> mockStorage_;
};

TEST_F(CompressedStorageEngineTest, BasicCompression) {
    CompressedStorageEngine::Config config;
    config.enableCompression = true;
    config.compressionThreshold = 1024;

    auto engine = std::make_unique<CompressedStorageEngine>(mockStorage_, config);

    // Generate compressible data
    auto data = generateData(4096, true);
    auto key = hashData(data);

    // Store data
    ASSERT_TRUE(engine->put(key, data));

    // Verify data is compressed in storage
    size_t storedSize = mockStorage_->getRawSize(key);
    EXPECT_LT(storedSize, data.size());

    // Retrieve data
    auto retrieved = engine->get(key);
    ASSERT_TRUE(retrieved);
    EXPECT_EQ(retrieved.value(), data);
}

TEST_F(CompressedStorageEngineTest, BelowThreshold) {
    CompressedStorageEngine::Config config;
    config.enableCompression = true;
    config.compressionThreshold = 1024;

    auto engine = std::make_unique<CompressedStorageEngine>(mockStorage_, config);

    // Generate small data below threshold
    auto data = generateData(512, true);
    auto key = hashData(data);

    // Store data
    ASSERT_TRUE(engine->put(key, data));

    // Verify data is NOT compressed in storage
    size_t storedSize = mockStorage_->getRawSize(key);
    EXPECT_EQ(storedSize, data.size());
}

TEST_F(CompressedStorageEngineTest, CompressionDisabled) {
    CompressedStorageEngine::Config config;
    config.enableCompression = false;

    auto engine = std::make_unique<CompressedStorageEngine>(mockStorage_, config);

    // Generate compressible data
    auto data = generateData(4096, true);
    auto key = hashData(data);

    // Store data
    ASSERT_TRUE(engine->put(key, data));

    // Verify data is NOT compressed
    size_t storedSize = mockStorage_->getRawSize(key);
    EXPECT_EQ(storedSize, data.size());
}

TEST_F(CompressedStorageEngineTest, MixedData) {
    CompressedStorageEngine::Config config;
    config.enableCompression = true;
    config.compressionThreshold = 1024;

    auto engine = std::make_unique<CompressedStorageEngine>(mockStorage_, config);

    // Store some compressed data
    auto compressibleData = generateData(4096, true);
    auto key1 = hashData(compressibleData);
    ASSERT_TRUE(engine->put(key1, compressibleData));

    // Disable compression and store more data
    engine->setCompressionEnabled(false);
    auto uncompressedData = generateData(4096, false);
    auto key2 = hashData(uncompressedData);
    ASSERT_TRUE(engine->put(key2, uncompressedData));

    // Retrieve both
    auto retrieved1 = engine->get(key1);
    ASSERT_TRUE(retrieved1);
    EXPECT_EQ(retrieved1.value(), compressibleData);

    auto retrieved2 = engine->get(key2);
    ASSERT_TRUE(retrieved2);
    EXPECT_EQ(retrieved2.value(), uncompressedData);
}

TEST_F(CompressedStorageEngineTest, CompressExisting) {
    CompressedStorageEngine::Config config;
    config.enableCompression = false; // Start with compression disabled

    auto engine = std::make_unique<CompressedStorageEngine>(mockStorage_, config);

    // Store uncompressed data
    auto data = generateData(4096, true);
    auto key = hashData(data);
    ASSERT_TRUE(engine->put(key, data));

    size_t originalSize = mockStorage_->getRawSize(key);
    EXPECT_EQ(originalSize, data.size());

    // Compress existing data
    ASSERT_TRUE(engine->compressExisting(key, true));

    // Verify compression
    size_t compressedSize = mockStorage_->getRawSize(key);
    EXPECT_LT(compressedSize, originalSize);

    // Verify data integrity
    auto retrieved = engine->get(key);
    ASSERT_TRUE(retrieved);
    EXPECT_EQ(retrieved.value(), data);
}

TEST_F(CompressedStorageEngineTest, DecompressExisting) {
    CompressedStorageEngine::Config config;
    config.enableCompression = true;

    auto engine = std::make_unique<CompressedStorageEngine>(mockStorage_, config);

    // Store compressed data
    auto data = generateData(4096, true);
    auto key = hashData(data);
    ASSERT_TRUE(engine->put(key, data));

    size_t compressedSize = mockStorage_->getRawSize(key);

    // Decompress existing data
    ASSERT_TRUE(engine->decompressExisting(key));

    // Verify decompression
    size_t decompressedSize = mockStorage_->getRawSize(key);
    EXPECT_GT(decompressedSize, compressedSize);
    EXPECT_EQ(decompressedSize, data.size());

    // Verify data integrity
    auto retrieved = engine->get(key);
    ASSERT_TRUE(retrieved);
    EXPECT_EQ(retrieved.value(), data);
}

TEST_F(CompressedStorageEngineTest, Size) {
    CompressedStorageEngine::Config config;
    config.enableCompression = true;

    auto engine = std::make_unique<CompressedStorageEngine>(mockStorage_, config);

    // Store compressed data
    auto data = generateData(4096, true);
    auto key = hashData(data);
    ASSERT_TRUE(engine->put(key, data));

    // Size should return original size, not compressed size
    auto size = engine->size(key);
    ASSERT_TRUE(size);
    EXPECT_EQ(size.value(), data.size());
}

TEST_F(CompressedStorageEngineTest, Statistics) {
    CompressedStorageEngine::Config config;
    config.enableCompression = true;

    auto engine = std::make_unique<CompressedStorageEngine>(mockStorage_, config);

    // Store multiple files
    for (int i = 0; i < 10; ++i) {
        auto data = generateData(4096 + i * 100, true);
        auto key = hashData(data);
        ASSERT_TRUE(engine->put(key, data));
    }

    // Check compression statistics
    auto stats = engine->getCompressionStats();
    EXPECT_EQ(stats.totalCompressedFiles.load(), 10);
    EXPECT_GT(stats.totalSpaceSaved.load(), 0);
    EXPECT_GT(stats.overallCompressionRatio(), 1.0);
}

TEST_F(CompressedStorageEngineTest, ConcurrentAccess) {
    CompressedStorageEngine::Config config;
    config.enableCompression = true;
    config.asyncCompression = false; // Disable async for predictable testing

    auto engine = std::make_unique<CompressedStorageEngine>(mockStorage_, config);

    const int numThreads = 4;
    const int itemsPerThread = 25;
    std::vector<std::thread> threads;

    // Concurrent writes
    for (int t = 0; t < numThreads; ++t) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < itemsPerThread; ++i) {
                auto data = generateData(2048 + (t * 100) + i, true);
                auto key = hashData(data);
                EXPECT_TRUE(engine->put(key, data));
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // Verify all data
    auto stats = engine->getCompressionStats();
    EXPECT_EQ(stats.totalCompressedFiles.load(), numThreads * itemsPerThread);
}

TEST_F(CompressedStorageEngineTest, PolicyIntegration) {
    CompressedStorageEngine::Config config;
    config.enableCompression = true;
    config.compressionThreshold = 1024;

    // Set up policy rules
    config.policyRules.sizeRules.push_back(
        {.minSize = 0, .maxSize = 2048, .algorithm = CompressionAlgorithm::None, .level = 0});
    config.policyRules.sizeRules.push_back({.minSize = 2048,
                                            .maxSize = std::numeric_limits<size_t>::max(),
                                            .algorithm = CompressionAlgorithm::Zstandard,
                                            .level = 3});

    auto engine = std::make_unique<CompressedStorageEngine>(mockStorage_, config);

    // Small file - should not be compressed by policy
    auto smallData = generateData(1500, true);
    auto smallKey = hashData(smallData);
    ASSERT_TRUE(engine->put(smallKey, smallData));
    EXPECT_EQ(mockStorage_->getRawSize(smallKey), smallData.size());

    // Large file - should be compressed
    auto largeData = generateData(4096, true);
    auto largeKey = hashData(largeData);
    ASSERT_TRUE(engine->put(largeKey, largeData));
    EXPECT_LT(mockStorage_->getRawSize(largeKey), largeData.size());
}

TEST_F(CompressedStorageEngineTest, ErrorHandling) {
    CompressedStorageEngine::Config config;
    config.enableCompression = true;

    auto engine = std::make_unique<CompressedStorageEngine>(mockStorage_, config);

    // Try to compress non-existent key
    Hash nonExistentKey;
    auto result = engine->compressExisting(nonExistentKey);
    EXPECT_FALSE(result);
    EXPECT_EQ(result.error().code, ErrorCode::NotFound);

    // Try to decompress non-compressed data
    auto data = generateData(1024, true);
    auto key = hashData(data);

    // Store uncompressed
    engine->setCompressionEnabled(false);
    ASSERT_TRUE(engine->put(key, data));

    // Try to decompress
    result = engine->decompressExisting(key);
    EXPECT_FALSE(result);
    EXPECT_EQ(result.error().code, ErrorCode::InvalidState);
}