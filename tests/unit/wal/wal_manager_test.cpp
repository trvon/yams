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
    {
        auto initResult = wal->initialize();
        ASSERT_TRUE(initResult.has_value());
    }

    // Begin transaction using WAL's API
    auto transaction = wal->beginTransaction();
    ASSERT_TRUE(transaction != nullptr);

    // Add operation to transaction
    auto storeResult = transaction->storeBlock("test_hash", 1024, 1);
    ASSERT_TRUE(storeResult.has_value());

    // Commit transaction
    auto commitResult = transaction->commit();
    ASSERT_TRUE(commitResult.has_value());

    {
        auto shutdownResult = wal->shutdown();
        ASSERT_TRUE(shutdownResult.has_value());
    }
}

TEST_F(WALManagerTest, TransactionRollback) {
    auto initResult_rollback = wal->initialize();
    ASSERT_TRUE(initResult_rollback.has_value());

    // Begin transaction
    auto transaction = wal->beginTransaction();
    ASSERT_TRUE(transaction != nullptr);

    // Add operation
    auto storeResult = transaction->storeBlock("test_hash", 1024, 1);
    ASSERT_TRUE(storeResult.has_value());

    // Rollback transaction
    auto rollbackResult = transaction->rollback();
    ASSERT_TRUE(rollbackResult.has_value());

    auto shutdownResult_rollback = wal->shutdown();
    ASSERT_TRUE(shutdownResult_rollback.has_value());
}

TEST_F(WALManagerTest, Recovery) {
    auto initResult_recovery = wal->initialize();
    ASSERT_TRUE(initResult_recovery.has_value());

    // Create and commit a transaction
    auto transaction = wal->beginTransaction();
    auto storeResult_recovery = transaction->storeBlock("test_hash", 1024, 1);
    ASSERT_TRUE(storeResult_recovery.has_value());
    auto commitResult_recovery = transaction->commit();
    ASSERT_TRUE(commitResult_recovery.has_value());

    auto shutdownResult_recovery1 = wal->shutdown();
    ASSERT_TRUE(shutdownResult_recovery1.has_value());

    // Recreate WAL and test recovery
    wal = std::make_unique<WALManager>(
        WALManager::Config{.walDirectory = walDir,
                           .maxLogSize = 1024 * 1024,
                           .syncInterval = 1000,
                           .syncTimeout = std::chrono::milliseconds(100)});
    auto initResult_recovery2 = wal->initialize();
    ASSERT_TRUE(initResult_recovery2.has_value());

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

    auto shutdownResult_recovery2 = wal->shutdown();
    ASSERT_TRUE(shutdownResult_recovery2.has_value());
}

TEST_F(WALManagerTest, Checkpoint) {
    auto initResult_checkpoint = wal->initialize();
    ASSERT_TRUE(initResult_checkpoint.has_value());

    // Create some transactions
    for (int i = 0; i < 3; ++i) {
        auto transaction = wal->beginTransaction();
        auto storeRes_loop = transaction->storeBlock(std::format("hash_{}", i), 1024, 1);
        ASSERT_TRUE(storeRes_loop.has_value());
        auto commitRes_loop = transaction->commit();
        ASSERT_TRUE(commitRes_loop.has_value());
    }

    // Create checkpoint
    auto checkpointResult = wal->checkpoint();
    ASSERT_TRUE(checkpointResult.has_value());

    auto shutdownResult_checkpoint = wal->shutdown();
    ASSERT_TRUE(shutdownResult_checkpoint.has_value());
}

TEST_F(WALManagerTest, Sync) {
    auto initResult_sync = wal->initialize();
    ASSERT_TRUE(initResult_sync.has_value());

    // Create a transaction
    auto transaction = wal->beginTransaction();
    auto storeResult_sync = transaction->storeBlock("test_hash", 1024, 1);
    ASSERT_TRUE(storeResult_sync.has_value());
    auto commitResult_sync = transaction->commit();
    ASSERT_TRUE(commitResult_sync.has_value());

    // Force sync
    auto syncResult = wal->sync();
    ASSERT_TRUE(syncResult.has_value());

    auto shutdownResult_sync = wal->shutdown();
    ASSERT_TRUE(shutdownResult_sync.has_value());
}

TEST_F(WALManagerTest, CurrentState) {
    auto initResult_currentState = wal->initialize();
    ASSERT_TRUE(initResult_currentState.has_value());

    // Test state methods
    auto sequence = wal->getCurrentSequence();
    EXPECT_GE(sequence, 0u);

    auto pending = wal->getPendingEntries();
    EXPECT_GE(pending, 0u);

    auto logPath = wal->getCurrentLogPath();
    EXPECT_FALSE(logPath.empty());

    auto shutdownResult_currentState = wal->shutdown();
    ASSERT_TRUE(shutdownResult_currentState.has_value());
}

TEST_F(WALManagerTest, CompressedRotationRecovery) {
    WALManager::Config config{.walDirectory = walDir,
                              .maxLogSize = 256,
                              .syncInterval = 1,
                              .syncTimeout = std::chrono::milliseconds(10),
                              .compressOldLogs = true};

    wal = std::make_unique<WALManager>(config);
    ASSERT_TRUE(wal->initialize().has_value());

    // Write a transaction so the current log has content.
    {
        auto txn = wal->beginTransaction();
        ASSERT_NE(txn, nullptr);

        ASSERT_TRUE(txn->storeBlock("compressed_hash", 4096, 1).has_value());
        ASSERT_TRUE(txn->commit().has_value());
    }

    const auto originalLogPath = wal->getCurrentLogPath();
    ASSERT_FALSE(originalLogPath.empty());
    ASSERT_TRUE(std::filesystem::exists(originalLogPath));

    // Force a rotation to trigger compression of the previous log file.
    ASSERT_TRUE(wal->rotateLogs().has_value());

    const auto compressedLogPath = originalLogPath.string() + ".zst";
    EXPECT_TRUE(std::filesystem::exists(compressedLogPath));
    EXPECT_FALSE(std::filesystem::exists(originalLogPath));

    // Shutdown the writer before running recovery.
    ASSERT_TRUE(wal->shutdown().has_value());

    // Recover with a fresh WAL manager instance.
    WALManager recoveryWal(config);
    ASSERT_TRUE(recoveryWal.initialize().has_value());

    size_t storeOps = 0;
    auto recoverResult = recoveryWal.recover([&](const WALEntry& entry) -> Result<void> {
        if (entry.header.operation == WALEntry::OpType::StoreBlock) {
            ++storeOps;
        }
        return {};
    });

    ASSERT_TRUE(recoverResult.has_value());
    const auto stats = recoverResult.value();
    EXPECT_GT(stats.entriesProcessed, 0u);
    EXPECT_EQ(storeOps, 1u);

    ASSERT_TRUE(recoveryWal.shutdown().has_value());
    EXPECT_TRUE(std::filesystem::exists(compressedLogPath));
}
