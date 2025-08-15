#include <chrono>
#include <filesystem>
#include <fstream>
#include <random>
#include <thread>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <yams/core/types.h>
#include <yams/wal/wal_entry.h>
#include <yams/wal/wal_manager.h>

using namespace yams;
using namespace yams::wal;
using namespace std::chrono_literals;

namespace fs = std::filesystem;

class WALManagerComprehensiveTest : public ::testing::Test {
protected:
    std::unique_ptr<WALManager> wal;
    fs::path walDir;

    void SetUp() override {
        // Create unique test directory
        walDir = fs::temp_directory_path() /
                 ("kronos_wal_test_" + std::to_string(std::random_device{}()));
        fs::create_directories(walDir);

        WALManager::Config config{.walDirectory = walDir,
                                  .maxLogSize = 1024 * 1024, // 1MB
                                  .syncInterval = 100,       // Sync every 100 entries
                                  .syncTimeout = 100ms};

        wal = std::make_unique<WALManager>(std::move(config));
    }

    void TearDown() override {
        if (wal) {
            wal->shutdown();
        }
        fs::remove_all(walDir);
    }

    // Helper to create test data
    std::vector<std::byte> createTestData(size_t size) {
        std::vector<std::byte> data(size);
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, 255);

        for (auto& byte : data) {
            byte = static_cast<std::byte>(dis(gen));
        }
        return data;
    }
};

// Test multiple transactions
TEST_F(WALManagerComprehensiveTest, MultipleTransactions) {
    ASSERT_TRUE(wal->initialize().has_value());

    const int numTransactions = 10;
    std::vector<std::string> hashes;

    // Create multiple transactions
    for (int i = 0; i < numTransactions; ++i) {
        auto tx = wal->beginTransaction();
        ASSERT_NE(tx, nullptr);

        std::string hash = "hash_" + std::to_string(i);
        hashes.push_back(hash);

        ASSERT_TRUE(tx->storeBlock(hash, 1024 * (i + 1), i + 1).has_value());
        ASSERT_TRUE(tx->commit().has_value());
    }

    // Verify log contains all entries
    EXPECT_GE(wal->getCurrentSequence(), static_cast<uint64_t>(numTransactions));
}

// Test transaction interleaving
TEST_F(WALManagerComprehensiveTest, InterleavedTransactions) {
    ASSERT_TRUE(wal->initialize().has_value());

    // Start multiple transactions
    auto tx1 = wal->beginTransaction();
    auto tx2 = wal->beginTransaction();
    auto tx3 = wal->beginTransaction();

    ASSERT_NE(tx1, nullptr);
    ASSERT_NE(tx2, nullptr);
    ASSERT_NE(tx3, nullptr);

    // Add operations in interleaved order
    ASSERT_TRUE(tx2->storeBlock("hash2", 2048, 2).has_value());
    ASSERT_TRUE(tx1->storeBlock("hash1", 1024, 1).has_value());
    ASSERT_TRUE(tx3->storeBlock("hash3", 3072, 3).has_value());

    // Commit in different order
    ASSERT_TRUE(tx3->commit().has_value());
    ASSERT_TRUE(tx1->commit().has_value());
    ASSERT_TRUE(tx2->commit().has_value());
}

// Test transaction with multiple operations
TEST_F(WALManagerComprehensiveTest, TransactionMultipleOperations) {
    ASSERT_TRUE(wal->initialize().has_value());

    auto tx = wal->beginTransaction();
    ASSERT_NE(tx, nullptr);

    // Store multiple blocks
    ASSERT_TRUE(tx->storeBlock("hash1", 1024, 1).has_value());
    ASSERT_TRUE(tx->storeBlock("hash2", 2048, 1).has_value());
    ASSERT_TRUE(tx->storeBlock("hash3", 3072, 1).has_value());

    // Update references
    ASSERT_TRUE(tx->updateReference("hash1", 1).has_value());
    ASSERT_TRUE(tx->updateReference("hash2", 2).has_value());

    // Delete block
    ASSERT_TRUE(tx->deleteBlock("hash3").has_value());

    ASSERT_TRUE(tx->commit().has_value());

    // Verify all operations logged
    EXPECT_GE(wal->getCurrentSequence(), 6u);
}

// Test large transactions
TEST_F(WALManagerComprehensiveTest, LargeTransaction) {
    ASSERT_TRUE(wal->initialize().has_value());

    auto tx = wal->beginTransaction();
    ASSERT_NE(tx, nullptr);

    const int numOps = 1000;
    for (int i = 0; i < numOps; ++i) {
        std::string hash = "large_hash_" + std::to_string(i);
        ASSERT_TRUE(tx->storeBlock(hash, 4096, 1).has_value());
    }

    auto start = std::chrono::steady_clock::now();
    ASSERT_TRUE(tx->commit().has_value());
    auto end = std::chrono::steady_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Should complete reasonably fast even with many operations
    EXPECT_LT(duration.count(), 5000); // < 5 seconds
}

// Test recovery with multiple log files
TEST_F(WALManagerComprehensiveTest, RecoveryMultipleLogFiles) {
    ASSERT_TRUE(wal->initialize().has_value());

    // Create enough transactions to trigger log rotation
    const size_t opsPerTx = 100;
    const size_t numTx = 20;

    for (size_t i = 0; i < numTx; ++i) {
        auto tx = wal->beginTransaction();
        for (size_t j = 0; j < opsPerTx; ++j) {
            std::string hash = "multi_" + std::to_string(i) + "_" + std::to_string(j);
            ASSERT_TRUE(tx->storeBlock(hash, 8192, 1).has_value());
        }
        ASSERT_TRUE(tx->commit().has_value());
    }

    ASSERT_TRUE(wal->shutdown().has_value());

    // Create new WAL and recover
    wal = std::make_unique<WALManager>(WALManager::Config{.walDirectory = walDir,
                                                          .maxLogSize = 1024 * 1024,
                                                          .syncInterval = 100,
                                                          .syncTimeout = 100ms});

    ASSERT_TRUE(wal->initialize().has_value());

    std::atomic<size_t> recoveredCount{0};
    auto result = wal->recover([&](const WALEntry& entry) -> Result<void> {
        recoveredCount++;
        return {};
    });

    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(recoveredCount.load(), numTx * opsPerTx);
}

// Test partial write recovery
TEST_F(WALManagerComprehensiveTest, PartialWriteRecovery) {
    ASSERT_TRUE(wal->initialize().has_value());

    // Write some complete entries
    for (int i = 0; i < 5; ++i) {
        auto tx = wal->beginTransaction();
        ASSERT_TRUE(tx->storeBlock("complete_" + std::to_string(i), 1024, 1).has_value());
        ASSERT_TRUE(tx->commit().has_value());
    }

    // Force sync
    ASSERT_TRUE(wal->sync().has_value());

    // Get current log file and corrupt it by truncating
    auto logPath = wal->getCurrentLogPath();
    ASSERT_TRUE(wal->shutdown().has_value());

    // Truncate file to simulate partial write
    {
        std::ofstream file(logPath, std::ios::binary | std::ios::app);
        file << "PARTIAL_DATA_THAT_IS_INCOMPLETE";
    }

    // Create new WAL and recover
    wal = std::make_unique<WALManager>(WALManager::Config{.walDirectory = walDir,
                                                          .maxLogSize = 1024 * 1024,
                                                          .syncInterval = 100,
                                                          .syncTimeout = 100ms});

    ASSERT_TRUE(wal->initialize().has_value());

    std::vector<WALEntry> recovered;
    auto result = wal->recover([&](const WALEntry& entry) -> Result<void> {
        recovered.push_back(entry);
        return {};
    });

    ASSERT_TRUE(result.has_value());
    // Should recover only complete entries
    EXPECT_EQ(recovered.size(), 5u);
}

// Test concurrent transactions
TEST_F(WALManagerComprehensiveTest, ConcurrentTransactions) {
    ASSERT_TRUE(wal->initialize().has_value());

    const int numThreads = 10;
    const int opsPerThread = 50;
    std::vector<std::thread> threads;
    std::atomic<int> successCount{0};

    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back([this, i, &successCount]() {
            for (int j = 0; j < opsPerThread; ++j) {
                auto tx = wal->beginTransaction();
                if (!tx)
                    continue;

                std::string hash = "concurrent_" + std::to_string(i) + "_" + std::to_string(j);
                if (!tx->storeBlock(hash, 1024, 1).has_value())
                    continue;

                if (tx->commit().has_value()) {
                    successCount++;
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(successCount.load(), numThreads * opsPerThread);
}

// Test sync behavior
TEST_F(WALManagerComprehensiveTest, SyncBehavior) {
    WALManager::Config config{.walDirectory = walDir,
                              .maxLogSize = 10 * 1024 * 1024, // 10MB
                              .syncInterval = 5,              // Sync every 5 entries
                              .syncTimeout = 50ms};

    wal = std::make_unique<WALManager>(std::move(config));
    ASSERT_TRUE(wal->initialize().has_value());

    // Write entries and check sync behavior
    for (int i = 0; i < 12; ++i) {
        auto tx = wal->beginTransaction();
        ASSERT_TRUE(tx->storeBlock("sync_test_" + std::to_string(i), 1024, 1).has_value());
        ASSERT_TRUE(tx->commit().has_value());

        // After 5 and 10 entries, should have synced
        if (i == 4 || i == 9) {
            // Give time for auto-sync
            std::this_thread::sleep_for(100ms);
        }
    }

    // Manual sync
    ASSERT_TRUE(wal->sync().has_value());
}

// Test checkpoint functionality
TEST_F(WALManagerComprehensiveTest, CheckpointOperations) {
    ASSERT_TRUE(wal->initialize().has_value());

    // Create transactions
    for (int i = 0; i < 10; ++i) {
        auto tx = wal->beginTransaction();
        ASSERT_TRUE(tx->storeBlock("checkpoint_" + std::to_string(i), 2048, 1).has_value());
        ASSERT_TRUE(tx->commit().has_value());
    }

    // Create checkpoint
    auto checkpointResult = wal->checkpoint();
    ASSERT_TRUE(checkpointResult.has_value());

    // Add more transactions after checkpoint
    for (int i = 10; i < 15; ++i) {
        auto tx = wal->beginTransaction();
        ASSERT_TRUE(tx->storeBlock("after_checkpoint_" + std::to_string(i), 2048, 1).has_value());
        ASSERT_TRUE(tx->commit().has_value());
    }

    // Shutdown and recover
    ASSERT_TRUE(wal->shutdown().has_value());

    wal = std::make_unique<WALManager>(WALManager::Config{.walDirectory = walDir,
                                                          .maxLogSize = 1024 * 1024,
                                                          .syncInterval = 100,
                                                          .syncTimeout = 100ms});

    ASSERT_TRUE(wal->initialize().has_value());

    std::vector<std::string> recoveredHashes;
    auto result = wal->recover([&](const WALEntry& entry) -> Result<void> {
        if (entry.header.operation == WALEntry::OpType::StoreBlock && !entry.data.empty()) {
            // Extract block hash from entry data
            // For StoreBlock, data format is typically: hash + size + refcount
            std::string hash(reinterpret_cast<const char*>(entry.data.data()),
                             std::min<size_t>(64, entry.data.size())); // SHA-256 is 64 chars
            recoveredHashes.push_back(hash);
        }
        return {};
    });

    ASSERT_TRUE(result.has_value());
    // Should recover entries after checkpoint
    EXPECT_GE(recoveredHashes.size(), 5u);
}

// Test error handling in recovery
TEST_F(WALManagerComprehensiveTest, RecoveryErrorHandling) {
    ASSERT_TRUE(wal->initialize().has_value());

    // Write some entries
    for (int i = 0; i < 5; ++i) {
        auto tx = wal->beginTransaction();
        ASSERT_TRUE(tx->storeBlock("error_test_" + std::to_string(i), 1024, 1).has_value());
        ASSERT_TRUE(tx->commit().has_value());
    }

    ASSERT_TRUE(wal->shutdown().has_value());

    // Create new WAL with recovery that fails on 3rd entry
    wal = std::make_unique<WALManager>(WALManager::Config{.walDirectory = walDir,
                                                          .maxLogSize = 1024 * 1024,
                                                          .syncInterval = 100,
                                                          .syncTimeout = 100ms});

    ASSERT_TRUE(wal->initialize().has_value());

    int processedCount = 0;
    int errorCount = 0;
    auto result = wal->recover([&](const WALEntry& entry) -> Result<void> {
        processedCount++;
        if (processedCount == 3) {
            errorCount++;
            return Result<void>(ErrorCode::InternalError);
        }
        return {};
    });

    // Recovery should succeed but report errors in stats
    ASSERT_TRUE(result.has_value());
    auto stats = result.value();
    EXPECT_GT(stats.errorsEncountered, 0u);
    EXPECT_GT(stats.entriesProcessed, 0u);
}

// Test transaction abort on destruction
TEST_F(WALManagerComprehensiveTest, TransactionAutoAbort) {
    ASSERT_TRUE(wal->initialize().has_value());

    {
        auto tx = wal->beginTransaction();
        ASSERT_TRUE(tx->storeBlock("auto_abort_test", 1024, 1).has_value());
        // Transaction destroyed without commit
    }

    // Create another transaction to ensure previous was cleaned up
    auto tx2 = wal->beginTransaction();
    ASSERT_NE(tx2, nullptr);
    ASSERT_TRUE(tx2->storeBlock("after_abort", 1024, 1).has_value());
    ASSERT_TRUE(tx2->commit().has_value());
}

// Test max log size rotation
TEST_F(WALManagerComprehensiveTest, LogRotation) {
    WALManager::Config config{.walDirectory = walDir,
                              .maxLogSize = 10 * 1024, // 10KB - small for testing
                              .syncInterval = 1000,
                              .syncTimeout = 100ms};

    wal = std::make_unique<WALManager>(std::move(config));
    ASSERT_TRUE(wal->initialize().has_value());

    auto initialLogPath = wal->getCurrentLogPath();

    // Write enough data to trigger rotation
    for (int i = 0; i < 100; ++i) {
        auto tx = wal->beginTransaction();
        std::string hash = "rotation_test_" + std::to_string(i);
        ASSERT_TRUE(tx->storeBlock(hash, 1024, 1).has_value());
        ASSERT_TRUE(tx->commit().has_value());
    }

    auto currentLogPath = wal->getCurrentLogPath();
    EXPECT_NE(initialLogPath, currentLogPath);

    // Verify multiple log files exist
    int logFileCount = 0;
    for (const auto& entry : fs::directory_iterator(walDir)) {
        if (entry.path().extension() == ".log") {
            logFileCount++;
        }
    }
    EXPECT_GT(logFileCount, 1);
}

// Performance test
TEST_F(WALManagerComprehensiveTest, PerformanceBenchmark) {
    ASSERT_TRUE(wal->initialize().has_value());

    const int numOperations = 10000;

    auto start = std::chrono::steady_clock::now();

    for (int i = 0; i < numOperations; ++i) {
        auto tx = wal->beginTransaction();
        ASSERT_TRUE(tx->storeBlock("perf_" + std::to_string(i), 4096, 1).has_value());
        ASSERT_TRUE(tx->commit().has_value());
    }

    auto end = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    double opsPerSecond = (numOperations * 1000.0) / duration.count();

    // Should achieve reasonable throughput
    EXPECT_GT(opsPerSecond, 1000); // > 1000 ops/sec

    std::cout << "WAL Performance: " << opsPerSecond << " ops/sec" << std::endl;
}