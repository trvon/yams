#include <gtest/gtest.h>
#include <yams/storage/reference_counter.h>
#include <yams/storage/storage_engine.h>
#include <filesystem>
#include <thread>
#include <random>
#include <future>

using namespace yams::storage;

class ReferenceCounterTest : public ::testing::Test {
protected:
    std::filesystem::path testDbPath;
    std::unique_ptr<ReferenceCounter> refCounter;
    
    void SetUp() override {
        // Create temporary test database
        testDbPath = std::filesystem::temp_directory_path() / 
                     std::format("kronos_refcount_test_{}.db", 
                                std::chrono::system_clock::now().time_since_epoch().count());
        
        ReferenceCounter::Config config{
            .databasePath = testDbPath,
            .enableWAL = true,
            .enableStatistics = true,
            .cacheSize = 1000,
            .busyTimeout = 1000,
            .enableAuditLog = false
        };
        
        refCounter = std::make_unique<ReferenceCounter>(std::move(config));
    }
    
    void TearDown() override {
        refCounter.reset();
        std::filesystem::remove(testDbPath);
        std::filesystem::remove(testDbPath.string() + "-wal");
        std::filesystem::remove(testDbPath.string() + "-shm");
    }
    
    std::string generateHash(int i) {
        return std::format("hash_{:064}", i);
    }
};

TEST_F(ReferenceCounterTest, BasicIncrementDecrement) {
    const std::string hash = generateHash(1);
    const size_t blockSize = 4096;
    
    // Initially, reference count should be 0
    auto count = refCounter->getRefCount(hash);
    ASSERT_TRUE(count.has_value());
    EXPECT_EQ(count.value(), 0);
    
    // Increment
    auto result = refCounter->increment(hash, blockSize);
    ASSERT_TRUE(result.has_value());
    
    count = refCounter->getRefCount(hash);
    ASSERT_TRUE(count.has_value());
    EXPECT_EQ(count.value(), 1);
    
    // Increment again
    result = refCounter->increment(hash, blockSize);
    ASSERT_TRUE(result.has_value());
    
    count = refCounter->getRefCount(hash);
    ASSERT_TRUE(count.has_value());
    EXPECT_EQ(count.value(), 2);
    
    // Decrement
    result = refCounter->decrement(hash);
    ASSERT_TRUE(result.has_value());
    
    count = refCounter->getRefCount(hash);
    ASSERT_TRUE(count.has_value());
    EXPECT_EQ(count.value(), 1);
    
    // Decrement to zero
    result = refCounter->decrement(hash);
    ASSERT_TRUE(result.has_value());
    
    count = refCounter->getRefCount(hash);
    ASSERT_TRUE(count.has_value());
    EXPECT_EQ(count.value(), 0);
}

TEST_F(ReferenceCounterTest, HasReferences) {
    const std::string hash = generateHash(2);
    
    // Initially no references
    auto hasRefs = refCounter->hasReferences(hash);
    ASSERT_TRUE(hasRefs.has_value());
    EXPECT_FALSE(hasRefs.value());
    
    // After increment, has references
    refCounter->increment(hash, 1024);
    hasRefs = refCounter->hasReferences(hash);
    ASSERT_TRUE(hasRefs.has_value());
    EXPECT_TRUE(hasRefs.value());
    
    // After decrement to zero, no references
    refCounter->decrement(hash);
    hasRefs = refCounter->hasReferences(hash);
    ASSERT_TRUE(hasRefs.has_value());
    EXPECT_FALSE(hasRefs.value());
}

TEST_F(ReferenceCounterTest, BatchOperations) {
    const size_t batchSize = 100;
    std::vector<std::string> hashes;
    
    for (size_t i = 0; i < batchSize; ++i) {
        hashes.push_back(generateHash(i));
    }
    
    // Batch increment
    auto result = refCounter->incrementBatch(hashes, 2048);
    ASSERT_TRUE(result.has_value());
    
    // Verify all incremented
    for (const auto& hash : hashes) {
        auto count = refCounter->getRefCount(hash);
        ASSERT_TRUE(count.has_value());
        EXPECT_EQ(count.value(), 1);
    }
    
    // Batch decrement
    result = refCounter->decrementBatch(hashes);
    ASSERT_TRUE(result.has_value());
    
    // Verify all decremented
    for (const auto& hash : hashes) {
        auto count = refCounter->getRefCount(hash);
        ASSERT_TRUE(count.has_value());
        EXPECT_EQ(count.value(), 0);
    }
}

TEST_F(ReferenceCounterTest, BatchWithSizes) {
    struct BlockInfo {
        std::string hash;
        size_t size;
    };
    
    std::vector<BlockInfo> blocks = {
        {generateHash(100), 1024},
        {generateHash(101), 2048},
        {generateHash(102), 4096},
        {generateHash(103), 8192}
    };
    
    auto result = refCounter->incrementBatchWithSizes(blocks);
    ASSERT_TRUE(result.has_value());
    
    // Verify all incremented
    for (const auto& block : blocks) {
        auto count = refCounter->getRefCount(block.hash);
        ASSERT_TRUE(count.has_value());
        EXPECT_EQ(count.value(), 1);
    }
}

TEST_F(ReferenceCounterTest, TransactionCommit) {
    const std::string hash1 = generateHash(200);
    const std::string hash2 = generateHash(201);
    
    auto txn = refCounter->beginTransaction();
    ASSERT_NE(txn, nullptr);
    ASSERT_TRUE(txn->isActive());
    
    // Add operations to transaction
    txn->increment(hash1, 1024);
    txn->increment(hash2, 2048);
    
    // Counts should still be zero (not committed)
    auto count1 = refCounter->getRefCount(hash1);
    ASSERT_TRUE(count1.has_value());
    EXPECT_EQ(count1.value(), 0);
    
    // Commit transaction
    auto result = txn->commit();
    ASSERT_TRUE(result.has_value());
    ASSERT_FALSE(txn->isActive());
    
    // Now counts should be updated
    count1 = refCounter->getRefCount(hash1);
    ASSERT_TRUE(count1.has_value());
    EXPECT_EQ(count1.value(), 1);
    
    auto count2 = refCounter->getRefCount(hash2);
    ASSERT_TRUE(count2.has_value());
    EXPECT_EQ(count2.value(), 1);
}

TEST_F(ReferenceCounterTest, TransactionRollback) {
    const std::string hash = generateHash(300);
    
    auto txn = refCounter->beginTransaction();
    ASSERT_NE(txn, nullptr);
    
    // Add operation to transaction
    txn->increment(hash, 1024);
    
    // Rollback transaction
    txn->rollback();
    ASSERT_FALSE(txn->isActive());
    
    // Count should still be zero
    auto count = refCounter->getRefCount(hash);
    ASSERT_TRUE(count.has_value());
    EXPECT_EQ(count.value(), 0);
}

TEST_F(ReferenceCounterTest, TransactionAutoRollback) {
    const std::string hash = generateHash(400);
    
    {
        auto txn = refCounter->beginTransaction();
        txn->increment(hash, 1024);
        // Transaction destroyed without commit - should rollback
    }
    
    // Count should still be zero
    auto count = refCounter->getRefCount(hash);
    ASSERT_TRUE(count.has_value());
    EXPECT_EQ(count.value(), 0);
}

TEST_F(ReferenceCounterTest, GetUnreferencedBlocks) {
    std::vector<std::string> allHashes;
    std::vector<std::string> unreferencedHashes;
    
    // Create some blocks with references
    for (int i = 0; i < 5; ++i) {
        auto hash = generateHash(500 + i);
        allHashes.push_back(hash);
        refCounter->increment(hash, 1024);
    }
    
    // Create some unreferenced blocks
    for (int i = 5; i < 10; ++i) {
        auto hash = generateHash(500 + i);
        allHashes.push_back(hash);
        unreferencedHashes.push_back(hash);
        
        // Increment then decrement to create unreferenced entry
        refCounter->increment(hash, 1024);
        refCounter->decrement(hash);
    }
    
    // Get unreferenced blocks (with 0 seconds min age)
    auto result = refCounter->getUnreferencedBlocks(100, std::chrono::seconds(0));
    ASSERT_TRUE(result.has_value());
    
    const auto& blocks = result.value();
    EXPECT_EQ(blocks.size(), unreferencedHashes.size());
    
    // Verify we got the right blocks
    std::set<std::string> foundBlocks(blocks.begin(), blocks.end());
    std::set<std::string> expectedBlocks(unreferencedHashes.begin(), unreferencedHashes.end());
    EXPECT_EQ(foundBlocks, expectedBlocks);
}

TEST_F(ReferenceCounterTest, Statistics) {
    // Add some blocks
    const size_t numBlocks = 10;
    const size_t blockSize = 1024;
    
    for (size_t i = 0; i < numBlocks; ++i) {
        refCounter->increment(generateHash(600 + i), blockSize);
    }
    
    // Decrement some to make them unreferenced
    for (size_t i = 0; i < 3; ++i) {
        refCounter->decrement(generateHash(600 + i));
    }
    
    // Get statistics
    auto statsResult = refCounter->getStats();
    ASSERT_TRUE(statsResult.has_value());
    
    const auto& stats = statsResult.value();
    EXPECT_EQ(stats.totalBlocks, numBlocks);
    EXPECT_EQ(stats.totalReferences, numBlocks - 3);  // 3 were decremented
    EXPECT_EQ(stats.totalBytes, numBlocks * blockSize);
    EXPECT_EQ(stats.unreferencedBlocks, 3);
    EXPECT_EQ(stats.unreferencedBytes, 3 * blockSize);
}

TEST_F(ReferenceCounterTest, ConcurrentOperations) {
    const size_t numThreads = 10;
    const size_t opsPerThread = 100;
    const std::string sharedHash = generateHash(700);
    
    std::vector<std::thread> threads;
    std::atomic<size_t> successCount{0};
    
    // Each thread increments the same hash
    for (size_t t = 0; t < numThreads; ++t) {
        threads.emplace_back([this, &successCount, &sharedHash, opsPerThread]() {
            for (size_t i = 0; i < opsPerThread; ++i) {
                auto result = refCounter->increment(sharedHash, 1024);
                if (result.has_value()) {
                    successCount++;
                }
            }
        });
    }
    
    // Wait for all threads
    for (auto& t : threads) {
        t.join();
    }
    
    // Verify final count
    auto count = refCounter->getRefCount(sharedHash);
    ASSERT_TRUE(count.has_value());
    EXPECT_EQ(count.value(), successCount.load());
    EXPECT_EQ(count.value(), numThreads * opsPerThread);
}

TEST_F(ReferenceCounterTest, ConcurrentTransactions) {
    // Original test complexity: 5 threads × 20 operations each
    const size_t numThreads = 5;
    const size_t hashesPerThread = 20;
    
    std::vector<std::thread> threads;
    std::atomic<size_t> commitCount{0};
    std::atomic<size_t> threadStarted{0};
    std::atomic<size_t> threadCompleted{0};
    std::atomic<size_t> errorCount{0};
    
    for (size_t t = 0; t < numThreads; ++t) {
        threads.emplace_back([this, &commitCount, &threadStarted, &threadCompleted, &errorCount, t, hashesPerThread]() {
            threadStarted++;
            
            try {
                auto txn = refCounter->beginTransaction();
                if (!txn) {
                    errorCount++;
                    threadCompleted++;
                    return;
                }
                
                // Each thread works on its own set of hashes
                for (size_t i = 0; i < hashesPerThread; ++i) {
                    auto hash = generateHash(800 + t * 100 + i);
                    txn->increment(hash, 2048);
                }
                
                auto result = txn->commit();
                if (result.has_value()) {
                    commitCount++;
                } else {
                    errorCount++;
                }
                
            } catch (const std::exception& e) {
                errorCount++;
                // Don't use ASSERT in thread - it would abort the thread
            }
            
            threadCompleted++;
        });
    }
    
    // Wait for all threads to complete
    for (auto& t : threads) {
        t.join();
    }
    
    // Verify all threads completed without errors
    EXPECT_EQ(threadStarted.load(), numThreads);
    EXPECT_EQ(threadCompleted.load(), numThreads);
    EXPECT_EQ(errorCount.load(), 0);
    EXPECT_EQ(commitCount.load(), numThreads);
    
    // Verify all hashes were incremented correctly
    for (size_t t = 0; t < numThreads; ++t) {
        for (size_t i = 0; i < hashesPerThread; ++i) {
            auto hash = generateHash(800 + t * 100 + i);
            auto count = refCounter->getRefCount(hash);
            ASSERT_TRUE(count.has_value());
            EXPECT_EQ(count.value(), 1);
        }
    }
}

TEST_F(ReferenceCounterTest, MaintenanceOperations) {
    // Add some data
    for (int i = 0; i < 100; ++i) {
        refCounter->increment(generateHash(900 + i), 1024);
    }
    
    // Test vacuum
    auto result = refCounter->vacuum();
    EXPECT_TRUE(result.has_value());
    
    // Test checkpoint
    result = refCounter->checkpoint();
    EXPECT_TRUE(result.has_value());
    
    // Test analyze
    result = refCounter->analyze();
    EXPECT_TRUE(result.has_value());
    
    // Test integrity check
    auto integrityResult = refCounter->verifyIntegrity();
    ASSERT_TRUE(integrityResult.has_value());
    EXPECT_TRUE(integrityResult.value());
}

TEST_F(ReferenceCounterTest, BackupRestore) {
    // Add some data
    const std::string hash1 = generateHash(1000);
    const std::string hash2 = generateHash(1001);
    
    refCounter->increment(hash1, 1024);
    refCounter->increment(hash2, 2048);
    refCounter->increment(hash2, 2048);  // Count = 2
    
    // Create backup
    auto backupPath = std::filesystem::temp_directory_path() / "kronos_backup.db";
    auto result = refCounter->backup(backupPath);
    ASSERT_TRUE(result.has_value());
    
    // Modify data
    refCounter->decrement(hash1);
    refCounter->increment(generateHash(1002), 4096);
    
    // Restore from backup
    result = refCounter->restore(backupPath);
    ASSERT_TRUE(result.has_value());
    
    // Verify restored state
    auto count1 = refCounter->getRefCount(hash1);
    ASSERT_TRUE(count1.has_value());
    EXPECT_EQ(count1.value(), 1);  // Original value
    
    auto count2 = refCounter->getRefCount(hash2);
    ASSERT_TRUE(count2.has_value());
    EXPECT_EQ(count2.value(), 2);  // Original value
    
    auto count3 = refCounter->getRefCount(generateHash(1002));
    ASSERT_TRUE(count3.has_value());
    EXPECT_EQ(count3.value(), 0);  // Not in backup
    
    // Cleanup
    std::filesystem::remove(backupPath);
}

TEST_F(ReferenceCounterTest, AsyncBatchOperations) {
    const size_t batchSize = 50;
    std::vector<std::string> hashes;
    
    for (size_t i = 0; i < batchSize; ++i) {
        hashes.push_back(generateHash(1100 + i));
    }
    
    // Async batch increment
    auto future = refCounter->incrementBatchAsync(hashes, 1024);
    auto result = future.get();
    ASSERT_TRUE(result.has_value());
    
    // Verify all incremented
    for (const auto& hash : hashes) {
        auto count = refCounter->getRefCount(hash);
        ASSERT_TRUE(count.has_value());
        EXPECT_EQ(count.value(), 1);
    }
}

// Test fixture for garbage collector
class GarbageCollectorTest : public ::testing::Test {
protected:
    std::filesystem::path testDbPath;
    std::filesystem::path testStoragePath;
    std::unique_ptr<ReferenceCounter> refCounter;
    std::unique_ptr<StorageEngine> storageEngine;
    std::unique_ptr<GarbageCollector> gc;
    
    void SetUp() override {
        // Create temporary paths
        auto tempDir = std::filesystem::temp_directory_path();
        testDbPath = tempDir / std::format("kronos_gc_test_{}.db", 
                                         std::chrono::system_clock::now().time_since_epoch().count());
        testStoragePath = tempDir / std::format("kronos_gc_storage_{}", 
                                              std::chrono::system_clock::now().time_since_epoch().count());
        
        // Create reference counter
        ReferenceCounter::Config config{
            .databasePath = testDbPath,
            .enableWAL = true,
            .enableStatistics = true
        };
        refCounter = std::make_unique<ReferenceCounter>(std::move(config));
        
        // Create storage engine
        StorageConfig storageConfig{
            .basePath = testStoragePath,
            .shardDepth = 2,
            .mutexPoolSize = 100
        };
        storageEngine = std::make_unique<StorageEngine>(std::move(storageConfig));
        
        // Create garbage collector
        gc = std::make_unique<GarbageCollector>(*refCounter, *storageEngine);
    }
    
    void TearDown() override {
        gc.reset();
        storageEngine.reset();
        refCounter.reset();
        
        std::filesystem::remove_all(testStoragePath);
        std::filesystem::remove(testDbPath);
        std::filesystem::remove(testDbPath.string() + "-wal");
        std::filesystem::remove(testDbPath.string() + "-shm");
    }
    
    std::string generateHash(int i) {
        return std::format("{:064x}", i);  // Generate exactly 64 hex characters
    }
};

TEST_F(GarbageCollectorTest, BasicCollection) {
    // Store some blocks
    std::vector<std::byte> data(1024, std::byte{42});
    std::vector<std::string> hashes;
    
    for (int i = 0; i < 5; ++i) {
        auto hash = generateHash(i);
        hashes.push_back(hash);
        
        // Store in storage engine
        auto storeResult = storageEngine->store(hash, data);
        ASSERT_TRUE(storeResult.has_value());
        
        // Add reference
        auto refResult = refCounter->increment(hash, data.size());
        ASSERT_TRUE(refResult.has_value());
    }
    
    // Remove references from some blocks
    for (int i = 0; i < 3; ++i) {
        auto refResult = refCounter->decrement(hashes[i]);
        ASSERT_TRUE(refResult.has_value());
    }
    
    // Run garbage collection (with 0 second min age for testing)
    GCOptions options{
        .maxBlocksPerRun = 10,
        .minAgeSeconds = 0,
        .dryRun = false
    };
    
    auto result = gc->collect(options);
    ASSERT_TRUE(result.has_value());
    
    const auto& stats = result.value();
    EXPECT_EQ(stats.blocksScanned, 3);  // 3 unreferenced blocks
    EXPECT_EQ(stats.blocksDeleted, 3);
    EXPECT_GT(stats.bytesReclaimed, 0);
    EXPECT_TRUE(stats.errors.empty());
    
    // Verify unreferenced blocks were deleted
    for (int i = 0; i < 3; ++i) {
        auto exists = storageEngine->exists(hashes[i]);
        ASSERT_TRUE(exists.has_value());
        EXPECT_FALSE(exists.value());
    }
    
    // Verify referenced blocks still exist
    for (int i = 3; i < 5; ++i) {
        auto exists = storageEngine->exists(hashes[i]);
        ASSERT_TRUE(exists.has_value());
        EXPECT_TRUE(exists.value());
    }
}

TEST_F(GarbageCollectorTest, DryRunCollection) {
    // Store some unreferenced blocks
    std::vector<std::byte> data(1024, std::byte{42});
    
    for (int i = 0; i < 3; ++i) {
        auto hash = generateHash(100 + i);
        
        // Store in storage engine
        storageEngine->store(hash, data);
        
        // Add and remove reference
        refCounter->increment(hash, data.size());
        refCounter->decrement(hash);
    }
    
    // Run dry-run collection
    GCOptions options{
        .maxBlocksPerRun = 10,
        .minAgeSeconds = 0,
        .dryRun = true
    };
    
    auto result = gc->collect(options);
    ASSERT_TRUE(result.has_value());
    
    const auto& stats = result.value();
    EXPECT_EQ(stats.blocksScanned, 3);
    EXPECT_EQ(stats.blocksDeleted, 3);  // Counted but not actually deleted
    
    // Verify blocks still exist (dry run)
    for (int i = 0; i < 3; ++i) {
        auto exists = storageEngine->exists(generateHash(100 + i));
        ASSERT_TRUE(exists.has_value());
        EXPECT_TRUE(exists.value());
    }
}

TEST_F(GarbageCollectorTest, AsyncCollection) {
    // Create unreferenced block
    std::vector<std::byte> data(1024, std::byte{42});
    auto hash = generateHash(200);
    
    storageEngine->store(hash, data);
    refCounter->increment(hash, data.size());
    refCounter->decrement(hash);
    
    // Run async collection
    GCOptions options{
        .maxBlocksPerRun = 10,
        .minAgeSeconds = 0,
        .dryRun = false
    };
    
    auto future = gc->collectAsync(options);
    auto result = future.get();
    
    ASSERT_TRUE(result.has_value());
    const auto& stats = result.value();
    EXPECT_EQ(stats.blocksDeleted, 1);
}

TEST_F(GarbageCollectorTest, ConcurrentCollectionPrevented) {
    // Create an unreferenced block so the garbage collector has work to do
    std::vector<std::byte> data(1024, std::byte{42});
    auto hash = generateHash(300);
    storageEngine->store(hash, data);
    refCounter->increment(hash, data.size());
    refCounter->decrement(hash);
    
    // Start a long-running collection
    std::promise<void> collectionStartedPromise;
    auto collectionStartedFuture = collectionStartedPromise.get_future();
    std::atomic<bool> allowCompletion{false};
    std::atomic<int> callbackCount{0};
    
    GCOptions options{
        .maxBlocksPerRun = 10,
        .minAgeSeconds = 0,
        .dryRun = false,
        .progressCallback = [&collectionStartedPromise, &allowCompletion, &callbackCount]
                           (const std::string&, size_t) {
            int count = callbackCount.fetch_add(1);
            if (count == 0) {
                // First call - signal that collection has started
                collectionStartedPromise.set_value();
            }
            // Don't block indefinitely - just delay briefly if not allowed to complete
            if (!allowCompletion) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }
    };
    
    // Start first collection
    auto future1 = gc->collectAsync(options);
    
    // Wait for it to start with timeout
    if (collectionStartedFuture.wait_for(std::chrono::seconds(2)) == 
        std::future_status::timeout) {
        FAIL() << "Collection did not start within timeout";
    }
    
    // Small delay to ensure the async collection is actually running
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    
    // Try to start second collection - should fail immediately
    auto result2 = gc->collect(options);
    EXPECT_FALSE(result2.has_value());
    EXPECT_EQ(result2.error().code, yams::ErrorCode::OperationInProgress);
    
    // Allow first collection to complete
    allowCompletion = true;
    
    // Wait for completion with timeout
    if (future1.wait_for(std::chrono::seconds(5)) == 
        std::future_status::timeout) {
        FAIL() << "Collection did not complete within timeout";
    }
    
    auto result1 = future1.get();
    EXPECT_TRUE(result1.has_value());
}

TEST_F(GarbageCollectorTest, GetLastStats) {
    // Initially empty stats
    auto stats = gc->getLastStats();
    EXPECT_EQ(stats.blocksScanned, 0);
    EXPECT_EQ(stats.blocksDeleted, 0);
    
    // Run collection
    GCOptions options{
        .maxBlocksPerRun = 10,
        .minAgeSeconds = 0,
        .dryRun = true
    };
    
    gc->collect(options);
    
    // Get updated stats
    stats = gc->getLastStats();
    EXPECT_GE(stats.blocksScanned, 0);
    EXPECT_GT(stats.duration.count(), 0);
}