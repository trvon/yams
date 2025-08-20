#include <gtest/gtest.h>
#include <yams/crypto/hasher.h>
#include <yams/storage/reference_counter.h>
#include <yams/storage/storage_engine.h>

#include <filesystem>
#include <fstream>
#include <random>

using namespace yams;

class MaintenanceToolsTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create temporary test directory
        testDir = std::filesystem::temp_directory_path() /
                  ("yams_tools_test_" + std::to_string(std::random_device{}()));
        std::filesystem::create_directories(testDir);

        // Create storage configuration
        storage::StorageConfig storageConfig{
            .basePath = testDir / "blocks", .shardDepth = 2, .mutexPoolSize = 64};

        storage = std::make_unique<storage::StorageEngine>(std::move(storageConfig));

        // Create reference counter configuration
        storage::ReferenceCounter::Config refConfig{
            .databasePath = testDir / "refs.db", .enableWAL = true, .enableStatistics = true};

        refCounter = std::make_unique<storage::ReferenceCounter>(std::move(refConfig));

        // Store some test data
        storeTestData();
    }

    void TearDown() override {
        refCounter.reset();
        storage.reset();

        // Clean up test directory
        std::error_code ec;
        std::filesystem::remove_all(testDir, ec);
    }

    void storeTestData() {
        hasher = crypto::createSHA256Hasher();

        // Store some blocks
        std::vector<std::string> testData = {"Hello, world!", "This is test data 1",
                                             "This is test data 2", "Another block of test data"};

        for (const auto& data : testData) {
            // Convert to bytes
            std::vector<std::byte> bytes;
            bytes.reserve(data.size());
            for (char c : data) {
                bytes.push_back(static_cast<std::byte>(c));
            }

            // Calculate hash
            std::string hash = hasher->hash(bytes);

            // Store data
            auto storeResult = storage->store(hash, bytes);
            ASSERT_TRUE(storeResult.has_value()) << "Failed to store test data";

            // Add reference
            auto txn = refCounter->beginTransaction();
            txn->increment(hash, bytes.size());
            auto commitResult = txn->commit();
            ASSERT_TRUE(commitResult.has_value()) << "Failed to commit reference";

            storedHashes.push_back(hash);
        }

        // Create one unreferenced block (no reference added)
        std::string unreferencedData = "Unreferenced block";
        std::vector<std::byte> unreferencedBytes;
        unreferencedBytes.reserve(unreferencedData.size());
        for (char c : unreferencedData) {
            unreferencedBytes.push_back(static_cast<std::byte>(c));
        }

        std::string unreferencedHash = hasher->hash(unreferencedBytes);
        auto storeResult = storage->store(unreferencedHash, unreferencedBytes);
        ASSERT_TRUE(storeResult.has_value()) << "Failed to store unreferenced data";

        unreferencedHashes.push_back(unreferencedHash);
    }

    std::filesystem::path testDir;
    std::unique_ptr<storage::StorageEngine> storage;
    std::unique_ptr<storage::ReferenceCounter> refCounter;
    std::unique_ptr<crypto::IContentHasher> hasher;
    std::vector<std::string> storedHashes;
    std::vector<std::string> unreferencedHashes;
};

TEST_F(MaintenanceToolsTest, StatsToolExecution) {
    // Test storage statistics functionality directly instead of calling external tool
    auto storageStats = storage->getStats();

    // Verify we have the expected test data
    EXPECT_EQ(storageStats.totalObjects.load(), storedHashes.size() + unreferencedHashes.size())
        << "Storage should contain all test objects";
    EXPECT_GT(storageStats.totalBytes.load(), 0) << "Storage should have non-zero size";

    // Verify reference counter statistics
    auto refStatsResult = refCounter->getStats();
    ASSERT_TRUE(refStatsResult.has_value()) << "Should be able to get reference statistics";

    const auto& refStats = refStatsResult.value();
    EXPECT_EQ(refStats.totalBlocks, storedHashes.size())
        << "Should have correct number of referenced blocks";
    EXPECT_GT(refStats.totalReferences, 0) << "Should have references";
    EXPECT_GT(refStats.totalBytes, 0) << "Referenced blocks should have non-zero size";
}

TEST_F(MaintenanceToolsTest, GCToolDryRun) {
    // Test garbage collection functionality directly (simulating dry-run mode)

    // First verify we have unreferenced blocks
    EXPECT_EQ(unreferencedHashes.size(), 1) << "Should have one unreferenced block";

    // Check that unreferenced blocks have zero reference count
    for (const auto& hash : unreferencedHashes) {
        auto refCount = refCounter->getRefCount(hash);
        ASSERT_TRUE(refCount.has_value()) << "Should be able to get reference count";
        EXPECT_EQ(refCount.value(), 0) << "Unreferenced block should have zero references";

        // Verify the block exists in storage
        auto existsResult = storage->exists(hash);
        ASSERT_TRUE(existsResult.has_value());
        EXPECT_TRUE(existsResult.value()) << "Unreferenced block should still exist in storage";
    }

    // Check that referenced blocks have non-zero reference count
    for (const auto& hash : storedHashes) {
        auto refCount = refCounter->getRefCount(hash);
        ASSERT_TRUE(refCount.has_value()) << "Should be able to get reference count";
        EXPECT_GT(refCount.value(), 0) << "Referenced block should have positive reference count";
    }

    // In a real GC dry-run, we would identify unreferenced blocks but not delete them
    // This test verifies we can identify them correctly
}

TEST_F(MaintenanceToolsTest, StorageHasTestData) {
    // Verify our test setup created the expected data
    auto storageStats = storage->getStats();
    EXPECT_GT(storageStats.totalObjects.load(), 0) << "Storage should contain test objects";
    EXPECT_GT(storageStats.totalBytes.load(), 0) << "Storage should have non-zero size";

    auto refStatsResult = refCounter->getStats();
    ASSERT_TRUE(refStatsResult.has_value()) << "Should be able to get reference statistics";

    const auto& refStats = refStatsResult.value();
    EXPECT_GT(refStats.totalBlocks, 0) << "Should have referenced blocks";
    EXPECT_GT(refStats.totalReferences, 0) << "Should have references";

    // Should have unreferenced blocks from our test setup
    // Note: This depends on the specific implementation of how unreferenced blocks are tracked
}

TEST_F(MaintenanceToolsTest, VerifyStoredData) {
    // Verify we can retrieve all stored data
    for (const auto& hash : storedHashes) {
        auto retrieveResult = storage->retrieve(hash);
        EXPECT_TRUE(retrieveResult.has_value())
            << "Should be able to retrieve stored block: " << hash;

        auto existsResult = storage->exists(hash);
        EXPECT_TRUE(existsResult.has_value() && existsResult.value())
            << "Stored block should exist: " << hash;
    }
}

// Test that storage and reference counter handle nonexistent paths gracefully
TEST_F(MaintenanceToolsTest, ToolsHandleNonexistentPath) {
    std::string nonexistentPath = "/tmp/nonexistent_yams_storage";

    // Test that StorageEngine handles nonexistent path appropriately
    {
        storage::StorageConfig badConfig{.basePath =
                                             std::filesystem::path(nonexistentPath) / "blocks",
                                         .shardDepth = 2,
                                         .mutexPoolSize = 64};

        // Creating storage with nonexistent path should work (it creates the directory)
        EXPECT_NO_THROW({ storage::StorageEngine testStorage(std::move(badConfig)); })
            << "StorageEngine should handle creating new directories";
    }

    // Test that ReferenceCounter handles nonexistent database path
    {
        storage::ReferenceCounter::Config badConfig{
            .databasePath = std::filesystem::path(nonexistentPath) / "refs.db",
            .enableWAL = true,
            .enableStatistics = true};

        // Creating reference counter with nonexistent path should work (it creates the database)
        EXPECT_NO_THROW({ storage::ReferenceCounter testRefCounter(std::move(badConfig)); })
            << "ReferenceCounter should handle creating new database";
    }

    // Clean up any created directories
    std::error_code ec;
    std::filesystem::remove_all(nonexistentPath, ec);
}