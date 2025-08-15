#include "test_helpers.h"
#include <gtest/gtest.h>
#include <yams/wal/wal_manager.h>

#include <fstream>
#include <thread>

using namespace yams;
using namespace yams::wal;
using namespace yams::test;

class WALManagerTest : public YamsTest {
protected:
    std::unique_ptr<WALManager> wal;
    std::filesystem::path walDir;

    void SetUp() override {
        YamsTest::SetUp();

        walDir = testDir / "wal_test";
        std::filesystem::create_directories(walDir);

        WALManager::Config config{.walDirectory = walDir,
                                  .maxLogSize = 1024 * 1024, // 1MB
                                  .syncInterval = 1000,      // Sync every 1000 entries
                                  .syncTimeout = std::chrono::milliseconds(100)};

        wal = std::make_unique<WALManager>(std::move(config));
    }

    void TearDown() override {
        wal.reset();
        std::filesystem::remove_all(walDir);
        YamsTest::TearDown();
    }
};

TEST_F(WALManagerTest, InitializationAndShutdown) {
    auto result = wal->initialize();
    if (!result.has_value()) {
        std::cout << "Initialize failed with error: " << result.error().message << std::endl;
    }
    ASSERT_TRUE(result.has_value());

    // Should create log directory
    EXPECT_TRUE(std::filesystem::exists(walDir));

    auto shutdownResult = wal->shutdown();
    EXPECT_TRUE(shutdownResult.has_value());
}

TEST_F(WALManagerTest, BasicTransactionWorkflow) {
    wal->initialize();

    // Begin transaction using WAL's API
    auto transaction = wal->beginTransaction();
    ASSERT_TRUE(transaction != nullptr);

    // Add operation to transaction
    auto storeResult = transaction->storeBlock("test_hash", 1024, 1);
    ASSERT_TRUE(storeResult.has_value());

    // Commit transaction
    auto commitResult = transaction->commit();
    ASSERT_TRUE(commitResult.has_value());

    wal->shutdown();
}

TEST_F(WALManagerTest, TransactionRollback) {
    wal->initialize();

    // Begin transaction
    auto transaction = wal->beginTransaction();
    ASSERT_TRUE(transaction != nullptr);

    // Add operation
    auto storeResult = transaction->storeBlock("test_hash", 1024, 1);
    ASSERT_TRUE(storeResult.has_value());

    // Rollback transaction
    auto rollbackResult = transaction->rollback();
    ASSERT_TRUE(rollbackResult.has_value());

    wal->shutdown();
}

TEST_F(WALManagerTest, Recovery) {
    wal->initialize();

    // Create and commit a transaction
    auto transaction = wal->beginTransaction();
    transaction->storeBlock("test_hash", 1024, 1);
    transaction->commit();

    wal->shutdown();

    // Recreate WAL and test recovery
    wal = std::make_unique<WALManager>(
        WALManager::Config{.walDirectory = walDir,
                           .maxLogSize = 1024 * 1024,
                           .syncInterval = 1000,
                           .syncTimeout = std::chrono::milliseconds(100)});
    wal->initialize();

    // Test recovery
    std::vector<WALEntry> recoveredEntries;
    auto recoverResult = wal->recover([&](const WALEntry& entry) -> Result<void> {
        recoveredEntries.push_back(entry);
        return {};
    });

    ASSERT_TRUE(recoverResult.has_value());
    auto stats = recoverResult.value();

    EXPECT_GT(stats.entriesProcessed, 0u);
    EXPECT_GT(recoveredEntries.size(), 0u);

    wal->shutdown();
}

TEST_F(WALManagerTest, Checkpoint) {
    wal->initialize();

    // Create some transactions
    for (int i = 0; i < 3; ++i) {
        auto transaction = wal->beginTransaction();
        transaction->storeBlock(std::format("hash_{}", i), 1024, 1);
        transaction->commit();
    }

    // Create checkpoint
    auto checkpointResult = wal->checkpoint();
    ASSERT_TRUE(checkpointResult.has_value());

    wal->shutdown();
}

TEST_F(WALManagerTest, Sync) {
    wal->initialize();

    // Create a transaction
    auto transaction = wal->beginTransaction();
    transaction->storeBlock("test_hash", 1024, 1);
    transaction->commit();

    // Force sync
    auto syncResult = wal->sync();
    ASSERT_TRUE(syncResult.has_value());

    wal->shutdown();
}

TEST_F(WALManagerTest, CurrentState) {
    wal->initialize();

    // Test state methods
    auto sequence = wal->getCurrentSequence();
    EXPECT_GE(sequence, 0u);

    auto pending = wal->getPendingEntries();
    EXPECT_GE(pending, 0u);

    auto logPath = wal->getCurrentLogPath();
    EXPECT_FALSE(logPath.empty());

    wal->shutdown();
}