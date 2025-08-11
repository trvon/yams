#include <gtest/gtest.h>
#include <yams/storage/storage_engine.h>
#include <yams/crypto/hasher.h>
#include "test_helpers.h"

#include <algorithm>
#include <random>
#include <thread>

using namespace yams;
using namespace yams::storage;
using namespace yams::test;

class StorageEngineTest : public YamsTest {
protected:
    std::unique_ptr<StorageEngine> storage;
    std::filesystem::path storagePath;
    
    void SetUp() override {
        YamsTest::SetUp();
        
        // Create temporary storage directory
        storagePath = testDir / "storage_test";
        std::filesystem::create_directories(storagePath);
        
        StorageConfig config{
            .basePath = storagePath,
            .shardDepth = 2,
            .mutexPoolSize = 128
        };
        
        storage = std::make_unique<StorageEngine>(std::move(config));
    }
    
    void TearDown() override {
        storage.reset();
        std::filesystem::remove_all(storagePath);
        YamsTest::TearDown();
    }
    
    // Helper to generate test data with hash
    std::pair<std::string, std::vector<std::byte>> generateTestData(size_t size) {
        auto data = generateRandomBytes(size);
        auto hasher = crypto::createSHA256Hasher();
        auto hash = hasher->hash(data);
        return {hash, data};
    }
};

TEST_F(StorageEngineTest, StoreAndRetrieve) {
    auto [hash, data] = generateTestData(1024);
    
    // Store data
    auto storeResult = storage->store(hash, data);
    ASSERT_TRUE(storeResult.has_value());
    
    // Retrieve data
    auto retrieveResult = storage->retrieve(hash);
    ASSERT_TRUE(retrieveResult.has_value());
    EXPECT_EQ(retrieveResult.value(), data);
}

TEST_F(StorageEngineTest, StoreExistingObject) {
    auto [hash, data] = generateTestData(1024);
    
    // Store data twice
    auto result1 = storage->store(hash, data);
    ASSERT_TRUE(result1.has_value());
    
    auto result2 = storage->store(hash, data);
    ASSERT_TRUE(result2.has_value());
    
    // Stats should show only one object
    auto stats = storage->getStats();
    EXPECT_EQ(stats.totalObjects.load(), 1u);
}

TEST_F(StorageEngineTest, RetrieveNonExistent) {
    std::string fakeHash(64, '0');
    
    auto result = storage->retrieve(fakeHash);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::ChunkNotFound);
}

TEST_F(StorageEngineTest, ExistsCheck) {
    auto [hash, data] = generateTestData(512);
    
    // Check non-existent
    auto exists1 = storage->exists(hash);
    ASSERT_TRUE(exists1.has_value());
    EXPECT_FALSE(exists1.value());
    
    // Store and check again
    storage->store(hash, data);
    
    auto exists2 = storage->exists(hash);
    ASSERT_TRUE(exists2.has_value());
    EXPECT_TRUE(exists2.value());
}

TEST_F(StorageEngineTest, RemoveObject) {
    auto [hash, data] = generateTestData(2048);
    
    // Store data
    storage->store(hash, data);
    ASSERT_TRUE(storage->exists(hash).value());
    
    // Remove data
    auto removeResult = storage->remove(hash);
    ASSERT_TRUE(removeResult.has_value());
    
    // Verify removed
    ASSERT_FALSE(storage->exists(hash).value());
    
    // Stats should show zero objects
    auto stats = storage->getStats();
    EXPECT_EQ(stats.totalObjects.load(), 0u);
}

TEST_F(StorageEngineTest, InvalidHashLength) {
    std::string shortHash = "abc123";
    std::vector<std::byte> data{std::byte{1}, std::byte{2}, std::byte{3}};
    
    auto result = storage->store(shortHash, data);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::InvalidArgument);
}

TEST_F(StorageEngineTest, LargeFile) {
    // Test with 10MB file
    auto [hash, data] = generateTestData(10 * 1024 * 1024);
    
    auto storeResult = storage->store(hash, data);
    ASSERT_TRUE(storeResult.has_value());
    
    auto retrieveResult = storage->retrieve(hash);
    ASSERT_TRUE(retrieveResult.has_value());
    EXPECT_EQ(retrieveResult.value().size(), data.size());
}

TEST_F(StorageEngineTest, DirectorySharding) {
    auto [hash, data] = generateTestData(100);
    
    storage->store(hash, data);
    
    // Verify file is stored in sharded directory
    auto expectedPath = storagePath / "objects" / hash.substr(0, 2) / hash.substr(2);
    EXPECT_TRUE(std::filesystem::exists(expectedPath));
}

TEST_F(StorageEngineTest, Statistics) {
    // Store multiple objects
    std::vector<std::pair<std::string, std::vector<std::byte>>> testData;
    for (int i = 0; i < 5; ++i) {
        testData.push_back(generateTestData(1024 * (i + 1)));
    }
    
    for (const auto& [hash, data] : testData) {
        storage->store(hash, data);
    }
    
    auto stats = storage->getStats();
    EXPECT_EQ(stats.totalObjects.load(), 5u);
    EXPECT_EQ(stats.writeOperations.load(), 5u);
    EXPECT_GT(stats.totalBytes.load(), 0u);
    
    // Read operations
    for (const auto& [hash, data] : testData) {
        storage->retrieve(hash);
    }
    
    auto readStats = storage->getStats();
    EXPECT_EQ(readStats.readOperations.load(), 5u);
}

TEST_F(StorageEngineTest, AsyncOperations) {
    auto [hash, data] = generateTestData(4096);
    
    // Async store
    auto storeFuture = storage->storeAsync(hash, data);
    auto storeResult = storeFuture.get();
    ASSERT_TRUE(storeResult.has_value());
    
    // Async retrieve
    auto retrieveFuture = storage->retrieveAsync(hash);
    auto retrieveResult = retrieveFuture.get();
    ASSERT_TRUE(retrieveResult.has_value());
    EXPECT_EQ(retrieveResult.value(), data);
}

TEST_F(StorageEngineTest, BatchOperations) {
    // Prepare batch data
    std::vector<std::pair<std::string, std::vector<std::byte>>> items;
    for (int i = 0; i < 10; ++i) {
        items.push_back(generateTestData(512));
    }
    
    // Batch store
    auto results = storage->storeBatch(items);
    
    ASSERT_EQ(results.size(), items.size());
    for (const auto& result : results) {
        EXPECT_TRUE(result.has_value());
    }
    
    // Verify all stored
    for (const auto& [hash, data] : items) {
        EXPECT_TRUE(storage->exists(hash).value());
    }
}

// Concurrent tests
TEST_F(StorageEngineTest, ConcurrentReads) {
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
    
    EXPECT_EQ(successCount.load(), numThreads * 100);
}

TEST_F(StorageEngineTest, ConcurrentWrites) {
    const int numThreads = 10;
    const int objectsPerThread = 10;
    std::vector<std::thread> threads;
    std::atomic<int> successCount{0};
    
    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back([this, i, &successCount]() {
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
    
    EXPECT_EQ(successCount.load(), numThreads * objectsPerThread);
    
    auto stats = storage->getStats();
    EXPECT_EQ(stats.totalObjects.load(), static_cast<uint64_t>(numThreads * objectsPerThread));
}

TEST_F(StorageEngineTest, ConcurrentSameObjectWrites) {
    auto [hash, data] = generateTestData(512);
    
    const int numThreads = 20;
    std::vector<std::thread> threads;
    std::atomic<int> successCount{0};
    
    // Multiple threads trying to write the same object
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
    
    // All should succeed (idempotent operation)
    EXPECT_EQ(successCount.load(), numThreads);
    
    // But only one object should exist
    auto stats = storage->getStats();
    EXPECT_EQ(stats.totalObjects.load(), 1u);
}

TEST_F(StorageEngineTest, MixedConcurrentOperations) {
    // Pre-store some objects
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
    
    // Mix of readers and writers
    for (int i = 0; i < numThreads; ++i) {
        if (i % 2 == 0) {
            // Reader thread
            threads.emplace_back([this, &objects, &readSuccess]() {
                std::random_device rd;
                std::mt19937 gen(rd());
                std::uniform_int_distribution<> dis(0, objects.size() - 1);
                
                for (int j = 0; j < 50; ++j) {
                    auto& [hash, data] = objects[dis(gen)];
                    auto result = storage->retrieve(hash);
                    if (result.has_value() && result.value() == data) {
                        readSuccess++;
                    }
                }
            });
        } else {
            // Writer thread
            threads.emplace_back([this, i, &writeSuccess]() {
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
    
    EXPECT_EQ(readSuccess.load(), (numThreads / 2) * 50);
    EXPECT_EQ(writeSuccess.load(), (numThreads / 2) * 10);
}

TEST_F(StorageEngineTest, CleanupTempFiles) {
    // Create some temp files
    auto tempDir = storagePath / "temp";
    std::filesystem::create_directories(tempDir);
    
    for (int i = 0; i < 5; ++i) {
        auto tempFile = tempDir / std::format("test{}.tmp", i);
        std::ofstream(tempFile) << "test data";
    }
    
    // Run cleanup
    auto result = storage->cleanupTempFiles();
    ASSERT_TRUE(result.has_value());
    
    // Old temp files should still exist (less than 1 hour old)
    EXPECT_EQ(std::distance(std::filesystem::directory_iterator(tempDir),
                           std::filesystem::directory_iterator{}), 5);
}

TEST_F(StorageEngineTest, StorageSize) {
    // Store known amount of data
    size_t totalSize = 0;
    for (int i = 0; i < 5; ++i) {
        auto [hash, data] = generateTestData(1024);
        storage->store(hash, data);
        totalSize += data.size();
    }
    
    auto sizeResult = storage->getStorageSize();
    ASSERT_TRUE(sizeResult.has_value());
    EXPECT_EQ(sizeResult.value(), totalSize);
}