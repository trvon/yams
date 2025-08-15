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
                  ("kronos_tools_test_" + std::to_string(std::random_device{}()));
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
    // Test that we can run the stats tool on our test storage
    std::string command =
        "/Volumes/picaso/work/tools/kronos/tools/kronos-stats " + testDir.string();

    int result = std::system(command.c_str());
    EXPECT_EQ(result, 0) << "Stats tool should execute successfully";
}

TEST_F(MaintenanceToolsTest, GCToolDryRun) {
    // Test that we can run the GC tool in dry-run mode
    std::string command = "/Volumes/picaso/work/tools/kronos/tools/kronos-gc " + testDir.string() +
                          " --dry-run --force";

    int result = std::system(command.c_str());
    EXPECT_EQ(result, 0) << "GC tool should execute successfully in dry-run mode";
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

// Test that requires filesystem access to verify tool behavior
TEST_F(MaintenanceToolsTest, ToolsHandleNonexistentPath) {
    std::string nonexistentPath = "/tmp/nonexistent_kronos_storage";

    // Stats tool should handle nonexistent path gracefully
    std::string statsCommand =
        "/Volumes/picaso/work/tools/kronos/tools/kronos-stats " + nonexistentPath + " 2>/dev/null";
    int statsResult = std::system(statsCommand.c_str());
    EXPECT_NE(statsResult, 0) << "Stats tool should return error for nonexistent path";

    // GC tool should handle nonexistent path gracefully
    std::string gcCommand = "/Volumes/picaso/work/tools/kronos/tools/kronos-gc " + nonexistentPath +
                            " --dry-run 2>/dev/null";
    int gcResult = std::system(gcCommand.c_str());
    EXPECT_NE(gcResult, 0) << "GC tool should return error for nonexistent path";
}