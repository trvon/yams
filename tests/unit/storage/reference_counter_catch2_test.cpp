// Catch2 tests for reference counter
// Migrated from GTest: reference_counter_test.cpp

#include <catch2/catch_test_macros.hpp>

#include <filesystem>
#include <format>
#include <future>
#include <random>
#include <set>
#include <thread>

#include <yams/storage/reference_counter.h>
#include <yams/storage/storage_engine.h>

using namespace yams::storage;

namespace {

struct ReferenceCounterFixture {
    ReferenceCounterFixture() {
        testDbPath = std::filesystem::temp_directory_path() /
                     std::format("yams_refcount_catch2_{}.db",
                                 std::chrono::system_clock::now().time_since_epoch().count());

        ReferenceCounter::Config config{.databasePath = testDbPath,
                                        .enableWAL = true,
                                        .enableStatistics = true,
                                        .cacheSize = 1000,
                                        .busyTimeout = 1000,
                                        .enableAuditLog = false};

        refCounter = std::make_unique<ReferenceCounter>(std::move(config));
    }

    ~ReferenceCounterFixture() {
        refCounter.reset();
        std::filesystem::remove(testDbPath);
        std::filesystem::remove(testDbPath.string() + "-wal");
        std::filesystem::remove(testDbPath.string() + "-shm");
    }

    std::string generateHash(int i) { return std::format("hash_{:064}", i); }

    std::filesystem::path testDbPath;
    std::unique_ptr<ReferenceCounter> refCounter;
};

struct GarbageCollectorFixture {
    GarbageCollectorFixture() {
        auto tempDir = std::filesystem::temp_directory_path();
        testDbPath =
            tempDir / std::format("yams_gc_catch2_{}.db",
                                  std::chrono::system_clock::now().time_since_epoch().count());
        testStoragePath =
            tempDir / std::format("yams_gc_storage_catch2_{}",
                                  std::chrono::system_clock::now().time_since_epoch().count());

        ReferenceCounter::Config config{
            .databasePath = testDbPath, .enableWAL = true, .enableStatistics = true};
        refCounter = std::make_unique<ReferenceCounter>(std::move(config));

        StorageConfig storageConfig{
            .basePath = testStoragePath, .shardDepth = 2, .mutexPoolSize = 100};
        storageEngine = std::make_unique<StorageEngine>(std::move(storageConfig));

        gc = std::make_unique<GarbageCollector>(*refCounter, *storageEngine);
    }

    ~GarbageCollectorFixture() {
        gc.reset();
        storageEngine.reset();
        refCounter.reset();

        std::filesystem::remove_all(testStoragePath);
        std::filesystem::remove(testDbPath);
        std::filesystem::remove(testDbPath.string() + "-wal");
        std::filesystem::remove(testDbPath.string() + "-shm");
    }

    std::string generateHash(int i) { return std::format("{:064x}", i); }

    std::filesystem::path testDbPath;
    std::filesystem::path testStoragePath;
    std::unique_ptr<ReferenceCounter> refCounter;
    std::unique_ptr<StorageEngine> storageEngine;
    std::unique_ptr<GarbageCollector> gc;
};

} // namespace

TEST_CASE_METHOD(ReferenceCounterFixture, "ReferenceCounter basic increment decrement",
                 "[storage][refcount][catch2]") {
    const std::string hash = generateHash(1);
    const size_t blockSize = 4096;

    // Initially, reference count should be 0
    auto count = refCounter->getRefCount(hash);
    REQUIRE(count.has_value());
    CHECK(count.value() == 0u);

    // Increment
    auto result = refCounter->increment(hash, blockSize);
    REQUIRE(result.has_value());

    count = refCounter->getRefCount(hash);
    REQUIRE(count.has_value());
    CHECK(count.value() == 1u);

    // Increment again
    result = refCounter->increment(hash, blockSize);
    REQUIRE(result.has_value());

    count = refCounter->getRefCount(hash);
    REQUIRE(count.has_value());
    CHECK(count.value() == 2u);

    // Decrement
    result = refCounter->decrement(hash);
    REQUIRE(result.has_value());

    count = refCounter->getRefCount(hash);
    REQUIRE(count.has_value());
    CHECK(count.value() == 1u);

    // Decrement to zero
    result = refCounter->decrement(hash);
    REQUIRE(result.has_value());

    count = refCounter->getRefCount(hash);
    REQUIRE(count.has_value());
    CHECK(count.value() == 0u);
}

TEST_CASE_METHOD(ReferenceCounterFixture, "ReferenceCounter hasReferences",
                 "[storage][refcount][catch2]") {
    const std::string hash = generateHash(2);

    // Initially no references
    auto hasRefs = refCounter->hasReferences(hash);
    REQUIRE(hasRefs.has_value());
    CHECK_FALSE(hasRefs.value());

    // After increment, has references
    refCounter->increment(hash, 1024);
    hasRefs = refCounter->hasReferences(hash);
    REQUIRE(hasRefs.has_value());
    CHECK(hasRefs.value());

    // After decrement to zero, no references
    refCounter->decrement(hash);
    hasRefs = refCounter->hasReferences(hash);
    REQUIRE(hasRefs.has_value());
    CHECK_FALSE(hasRefs.value());
}

TEST_CASE_METHOD(ReferenceCounterFixture, "ReferenceCounter batch operations",
                 "[storage][refcount][batch][catch2]") {
    const size_t batchSize = 100;
    std::vector<std::string> hashes;

    for (size_t i = 0; i < batchSize; ++i) {
        hashes.push_back(generateHash(i));
    }

    // Batch increment
    auto result = refCounter->incrementBatch(hashes, 2048);
    REQUIRE(result.has_value());

    // Verify all incremented
    for (const auto& hash : hashes) {
        auto count = refCounter->getRefCount(hash);
        REQUIRE(count.has_value());
        CHECK(count.value() == 1u);
    }

    // Batch decrement
    result = refCounter->decrementBatch(hashes);
    REQUIRE(result.has_value());

    // Verify all decremented
    for (const auto& hash : hashes) {
        auto count = refCounter->getRefCount(hash);
        REQUIRE(count.has_value());
        CHECK(count.value() == 0u);
    }
}

TEST_CASE_METHOD(ReferenceCounterFixture, "ReferenceCounter batch with sizes",
                 "[storage][refcount][batch][catch2]") {
    struct BlockInfo {
        std::string hash;
        size_t size;
    };

    std::vector<BlockInfo> blocks = {{generateHash(100), 1024},
                                     {generateHash(101), 2048},
                                     {generateHash(102), 4096},
                                     {generateHash(103), 8192}};

    auto result = refCounter->incrementBatchWithSizes(blocks);
    REQUIRE(result.has_value());

    // Verify all incremented
    for (const auto& block : blocks) {
        auto count = refCounter->getRefCount(block.hash);
        REQUIRE(count.has_value());
        CHECK(count.value() == 1u);
    }
}

TEST_CASE_METHOD(ReferenceCounterFixture, "ReferenceCounter transaction commit",
                 "[storage][refcount][transaction][catch2]") {
    const std::string hash1 = generateHash(200);
    const std::string hash2 = generateHash(201);

    auto txn = refCounter->beginTransaction();
    REQUIRE(txn != nullptr);
    REQUIRE(txn->isActive());

    // Add operations to transaction
    txn->increment(hash1, 1024);
    txn->increment(hash2, 2048);

    // Counts should still be zero (not committed)
    auto count1 = refCounter->getRefCount(hash1);
    REQUIRE(count1.has_value());
    CHECK(count1.value() == 0u);

    // Commit transaction
    auto result = txn->commit();
    REQUIRE(result.has_value());
    CHECK_FALSE(txn->isActive());

    // Now counts should be updated
    count1 = refCounter->getRefCount(hash1);
    REQUIRE(count1.has_value());
    CHECK(count1.value() == 1u);

    auto count2 = refCounter->getRefCount(hash2);
    REQUIRE(count2.has_value());
    CHECK(count2.value() == 1u);
}

TEST_CASE_METHOD(ReferenceCounterFixture, "ReferenceCounter transaction rollback",
                 "[storage][refcount][transaction][catch2]") {
    const std::string hash = generateHash(300);

    auto txn = refCounter->beginTransaction();
    REQUIRE(txn != nullptr);

    // Add operation to transaction
    txn->increment(hash, 1024);

    // Rollback transaction
    txn->rollback();
    CHECK_FALSE(txn->isActive());

    // Count should still be zero
    auto count = refCounter->getRefCount(hash);
    REQUIRE(count.has_value());
    CHECK(count.value() == 0u);
}

TEST_CASE_METHOD(ReferenceCounterFixture, "ReferenceCounter transaction auto rollback",
                 "[storage][refcount][transaction][catch2]") {
    const std::string hash = generateHash(400);

    {
        auto txn = refCounter->beginTransaction();
        txn->increment(hash, 1024);
        // Transaction destroyed without commit - should rollback
    }

    // Count should still be zero
    auto count = refCounter->getRefCount(hash);
    REQUIRE(count.has_value());
    CHECK(count.value() == 0u);
}

TEST_CASE_METHOD(ReferenceCounterFixture, "ReferenceCounter get unreferenced blocks",
                 "[storage][refcount][catch2]") {
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
    REQUIRE(result.has_value());

    const auto& blocks = result.value();
    CHECK(blocks.size() == unreferencedHashes.size());

    // Verify we got the right blocks
    std::set<std::string> foundBlocks(blocks.begin(), blocks.end());
    std::set<std::string> expectedBlocks(unreferencedHashes.begin(), unreferencedHashes.end());
    CHECK(foundBlocks == expectedBlocks);
}

TEST_CASE_METHOD(ReferenceCounterFixture, "ReferenceCounter statistics",
                 "[storage][refcount][stats][catch2]") {
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
    REQUIRE(statsResult.has_value());

    const auto& stats = statsResult.value();
    CHECK(stats.totalBlocks == numBlocks);
    CHECK(stats.totalReferences == numBlocks - 3); // 3 were decremented
    CHECK(stats.totalBytes == numBlocks * blockSize);
    CHECK(stats.unreferencedBlocks == 3u);
    CHECK(stats.unreferencedBytes == 3u * blockSize);
}

TEST_CASE_METHOD(ReferenceCounterFixture, "ReferenceCounter concurrent operations",
                 "[storage][refcount][concurrent][catch2]") {
    const size_t numThreads = 10;
    const size_t opsPerThread = 100;
    const std::string sharedHash = generateHash(700);

    std::vector<std::thread> threads;
    std::atomic<size_t> successCount{0};

    // Each thread increments the same hash
    for (size_t t = 0; t < numThreads; ++t) {
        threads.emplace_back([this, &successCount, &sharedHash]() {
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
    REQUIRE(count.has_value());
    CHECK(count.value() == successCount.load());
    CHECK(count.value() == numThreads * opsPerThread);
}

TEST_CASE_METHOD(ReferenceCounterFixture, "ReferenceCounter concurrent transactions",
                 "[storage][refcount][concurrent][transaction][catch2]") {
    const size_t numThreads = 5;
    const size_t hashesPerThread = 20;

    std::vector<std::thread> threads;
    std::atomic<size_t> commitCount{0};
    std::atomic<size_t> errorCount{0};

    for (size_t t = 0; t < numThreads; ++t) {
        threads.emplace_back([this, &commitCount, &errorCount, t]() {
            try {
                auto txn = refCounter->beginTransaction();
                if (!txn) {
                    errorCount++;
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
            } catch (const std::exception&) {
                errorCount++;
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    CHECK(errorCount.load() == 0u);
    CHECK(commitCount.load() == numThreads);

    // Verify all hashes were incremented correctly
    for (size_t t = 0; t < numThreads; ++t) {
        for (size_t i = 0; i < hashesPerThread; ++i) {
            auto hash = generateHash(800 + t * 100 + i);
            auto count = refCounter->getRefCount(hash);
            REQUIRE(count.has_value());
            CHECK(count.value() == 1u);
        }
    }
}

TEST_CASE_METHOD(ReferenceCounterFixture, "ReferenceCounter maintenance operations",
                 "[storage][refcount][maintenance][catch2]") {
    // Add some data
    for (int i = 0; i < 100; ++i) {
        refCounter->increment(generateHash(900 + i), 1024);
    }

    // Test vacuum
    auto result = refCounter->vacuum();
    CHECK(result.has_value());

    // Test checkpoint
    result = refCounter->checkpoint();
    CHECK(result.has_value());

    // Test analyze
    result = refCounter->analyze();
    CHECK(result.has_value());

    // Test integrity check
    auto integrityResult = refCounter->verifyIntegrity();
    REQUIRE(integrityResult.has_value());
    CHECK(integrityResult.value());
}

TEST_CASE_METHOD(ReferenceCounterFixture, "ReferenceCounter backup restore",
                 "[storage][refcount][backup][catch2]") {
    // Add some data
    const std::string hash1 = generateHash(1000);
    const std::string hash2 = generateHash(1001);

    refCounter->increment(hash1, 1024);
    refCounter->increment(hash2, 2048);
    refCounter->increment(hash2, 2048); // Count = 2

    // Create backup
    auto backupPath = std::filesystem::temp_directory_path() / "yams_backup_catch2.db";
    auto result = refCounter->backup(backupPath);
    REQUIRE(result.has_value());

    // Modify data
    refCounter->decrement(hash1);
    refCounter->increment(generateHash(1002), 4096);

    // Restore from backup
    result = refCounter->restore(backupPath);
    REQUIRE(result.has_value());

    // Verify restored state
    auto count1 = refCounter->getRefCount(hash1);
    REQUIRE(count1.has_value());
    CHECK(count1.value() == 1u); // Original value

    auto count2 = refCounter->getRefCount(hash2);
    REQUIRE(count2.has_value());
    CHECK(count2.value() == 2u); // Original value

    auto count3 = refCounter->getRefCount(generateHash(1002));
    REQUIRE(count3.has_value());
    CHECK(count3.value() == 0u); // Not in backup

    // Cleanup
    std::filesystem::remove(backupPath);
}

TEST_CASE_METHOD(ReferenceCounterFixture, "ReferenceCounter async batch operations",
                 "[storage][refcount][async][catch2]") {
    const size_t batchSize = 50;
    std::vector<std::string> hashes;

    for (size_t i = 0; i < batchSize; ++i) {
        hashes.push_back(generateHash(1100 + i));
    }

    // Async batch increment
    auto future = refCounter->incrementBatchAsync(hashes, 1024);
    auto result = future.get();
    REQUIRE(result.has_value());

    // Verify all incremented
    for (const auto& hash : hashes) {
        auto count = refCounter->getRefCount(hash);
        REQUIRE(count.has_value());
        CHECK(count.value() == 1u);
    }
}

// Garbage Collector tests

TEST_CASE_METHOD(GarbageCollectorFixture, "GarbageCollector basic collection",
                 "[storage][gc][catch2]") {
    // Store some blocks
    std::vector<std::byte> data(1024, std::byte{42});
    std::vector<std::string> hashes;

    for (int i = 0; i < 5; ++i) {
        auto hash = generateHash(i);
        hashes.push_back(hash);

        // Store in storage engine
        auto storeResult = storageEngine->store(hash, data);
        REQUIRE(storeResult.has_value());

        // Add reference
        auto refResult = refCounter->increment(hash, data.size());
        REQUIRE(refResult.has_value());
    }

    // Remove references from some blocks
    for (int i = 0; i < 3; ++i) {
        auto refResult = refCounter->decrement(hashes[i]);
        REQUIRE(refResult.has_value());
    }

    // Run garbage collection (with 0 second min age for testing)
    GCOptions options{
        .maxBlocksPerRun = 10, .minAgeSeconds = 0, .dryRun = false, .progressCallback = nullptr};

    auto result = gc->collect(options);
    REQUIRE(result.has_value());

    const auto& stats = result.value();
    CHECK(stats.blocksScanned == 3u); // 3 unreferenced blocks
    CHECK(stats.blocksDeleted == 3u);
    CHECK(stats.bytesReclaimed > 0u);
    CHECK(stats.errors.empty());

    // Verify unreferenced blocks were deleted
    for (int i = 0; i < 3; ++i) {
        auto exists = storageEngine->exists(hashes[i]);
        REQUIRE(exists.has_value());
        CHECK_FALSE(exists.value());
    }

    // Verify referenced blocks still exist
    for (int i = 3; i < 5; ++i) {
        auto exists = storageEngine->exists(hashes[i]);
        REQUIRE(exists.has_value());
        CHECK(exists.value());
    }
}

TEST_CASE_METHOD(GarbageCollectorFixture, "GarbageCollector dry run collection",
                 "[storage][gc][catch2]") {
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
        .maxBlocksPerRun = 10, .minAgeSeconds = 0, .dryRun = true, .progressCallback = nullptr};

    auto result = gc->collect(options);
    REQUIRE(result.has_value());

    const auto& stats = result.value();
    CHECK(stats.blocksScanned == 3u);
    CHECK(stats.blocksDeleted == 3u); // Counted but not actually deleted

    // Verify blocks still exist (dry run)
    for (int i = 0; i < 3; ++i) {
        auto exists = storageEngine->exists(generateHash(100 + i));
        REQUIRE(exists.has_value());
        CHECK(exists.value());
    }
}

TEST_CASE_METHOD(GarbageCollectorFixture, "GarbageCollector async collection",
                 "[storage][gc][async][catch2]") {
    // Create unreferenced block
    std::vector<std::byte> data(1024, std::byte{42});
    auto hash = generateHash(200);

    storageEngine->store(hash, data);
    refCounter->increment(hash, data.size());
    refCounter->decrement(hash);

    // Run async collection
    GCOptions options{
        .maxBlocksPerRun = 10, .minAgeSeconds = 0, .dryRun = false, .progressCallback = nullptr};

    auto future = gc->collectAsync(options);
    auto result = future.get();

    REQUIRE(result.has_value());
    const auto& stats = result.value();
    CHECK(stats.blocksDeleted == 1u);
}

TEST_CASE_METHOD(GarbageCollectorFixture, "GarbageCollector concurrent collection prevented",
                 "[storage][gc][concurrent][catch2]") {
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

    GCOptions options{.maxBlocksPerRun = 10,
                      .minAgeSeconds = 0,
                      .dryRun = false,
                      .progressCallback = [&collectionStartedPromise, &allowCompletion,
                                           &callbackCount](const std::string&, size_t) {
                          int count = callbackCount.fetch_add(1);
                          if (count == 0) {
                              // First call - signal that collection has started
                              collectionStartedPromise.set_value();
                          }
                          // Block until allowed to complete to ensure concurrent attempt fails
                          while (!allowCompletion) {
                              std::this_thread::sleep_for(std::chrono::milliseconds(10));
                          }
                      }};

    // Start first collection
    auto future1 = gc->collectAsync(options);

    // Wait for it to start with timeout
    auto status = collectionStartedFuture.wait_for(std::chrono::seconds(2));
    REQUIRE(status != std::future_status::timeout);

    // Small delay to ensure the async collection is actually running
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // Try to start second collection - should fail immediately
    auto result2 = gc->collect(options);
    CHECK_FALSE(result2.has_value());
    CHECK(result2.error().code == yams::ErrorCode::OperationInProgress);

    // Allow first collection to complete
    allowCompletion = true;

    // Wait for completion with timeout
    auto completeStatus = future1.wait_for(std::chrono::seconds(5));
    REQUIRE(completeStatus != std::future_status::timeout);

    auto result1 = future1.get();
    CHECK(result1.has_value());
}

TEST_CASE_METHOD(GarbageCollectorFixture, "GarbageCollector get last stats",
                 "[storage][gc][stats][catch2]") {
    // Initially empty stats
    auto stats = gc->getLastStats();
    CHECK(stats.blocksScanned == 0u);
    CHECK(stats.blocksDeleted == 0u);

    // Run collection
    GCOptions options{
        .maxBlocksPerRun = 10, .minAgeSeconds = 0, .dryRun = true, .progressCallback = nullptr};

    gc->collect(options);

    // Get updated stats
    stats = gc->getLastStats();
    CHECK(stats.blocksScanned >= 0u);
    CHECK(stats.duration.count() >= 0); // Duration can be 0 for very fast operations
}
