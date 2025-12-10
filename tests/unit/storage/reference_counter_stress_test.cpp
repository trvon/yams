#include <atomic>
#include <barrier>
#include <chrono>
#include <filesystem>
#include <format>
#include <future>
#include <latch>
#include <random>
#include <thread>
#include <vector>
#include <gtest/gtest.h>
#include <yams/storage/reference_counter.h>

using namespace yams::storage;
using namespace std::chrono_literals;

// Re-enabled on Windows after stabilizing SQLite access
#define SKIP_REFCOUNT_STRESS_ON_WINDOWS() ((void)0)

class ReferenceCounterStressTest : public ::testing::Test {
protected:
    std::filesystem::path testDbPath;
    std::unique_ptr<ReferenceCounter> refCounter;

    void SetUp() override {
        SKIP_REFCOUNT_STRESS_ON_WINDOWS();

        testDbPath = std::filesystem::temp_directory_path() /
                     std::format("yams_refcount_stress_{}.db",
                                 std::chrono::system_clock::now().time_since_epoch().count());

        ReferenceCounter::Config config{.databasePath = testDbPath,
                                        .enableWAL = true,
                                        .enableStatistics = true,
                                        .cacheSize = 10000,
                                        .busyTimeout = 5000,
                                        .enableAuditLog = false};

        refCounter = std::make_unique<ReferenceCounter>(std::move(config));
    }

    void TearDown() override {
        if (!refCounter) {
            return;
        }
        refCounter.reset();
        std::filesystem::remove(testDbPath);
        std::filesystem::remove(testDbPath.string() + "-wal");
        std::filesystem::remove(testDbPath.string() + "-shm");
    }

    std::string generateHash(int i) { return std::format("stress_hash_{:064}", i); }
};

// Test concurrent getStats() calls - reproduces the original crash scenario
TEST_F(ReferenceCounterStressTest, ConcurrentGetStats) {
    constexpr int numThreads = 20;
    constexpr int numIterations = 100;
    constexpr int numBlocks = 50;

    // Populate some data first
    for (int i = 0; i < numBlocks; ++i) {
        refCounter->increment(generateHash(i), 4096);
    }

    std::atomic<int> successCount{0};
    std::atomic<int> errorCount{0};
    std::vector<std::thread> threads;
    std::latch startLatch(numThreads);

    for (int t = 0; t < numThreads; ++t) {
        threads.emplace_back([&, threadId = t]() {
            // Wait for all threads to be ready
            startLatch.arrive_and_wait();

            for (int i = 0; i < numIterations; ++i) {
                auto stats = refCounter->getStats();
                if (stats.has_value()) {
                    successCount++;
                    // Verify stats make sense
                    EXPECT_GE(stats.value().totalBlocks, 0u);
                    EXPECT_GE(stats.value().totalReferences, 0u);
                } else {
                    errorCount++;
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_GT(successCount.load(), 0);
    EXPECT_EQ(errorCount.load(), 0) << "All getStats() calls should succeed";

    // Final stats check
    auto finalStats = refCounter->getStats();
    ASSERT_TRUE(finalStats.has_value());
    EXPECT_EQ(finalStats.value().totalBlocks, numBlocks);
    EXPECT_EQ(finalStats.value().totalReferences, numBlocks);
}

// Test mixed concurrent operations (increment, decrement, getStats, getRefCount)
TEST_F(ReferenceCounterStressTest, ConcurrentMixedOperations) {
    constexpr int numThreads = 16;
    constexpr int opsPerThread = 500;
    constexpr int numHashes = 100;

    std::atomic<int> successCount{0};
    std::atomic<int> errorCount{0};
    std::vector<std::thread> threads;
    std::latch startLatch(numThreads);

    for (int t = 0; t < numThreads; ++t) {
        threads.emplace_back([&, threadId = t]() {
            std::mt19937 rng(threadId);
            std::uniform_int_distribution<int> hashDist(0, numHashes - 1);
            std::uniform_int_distribution<int> opDist(0, 3);

            startLatch.arrive_and_wait();

            for (int i = 0; i < opsPerThread; ++i) {
                auto hash = generateHash(hashDist(rng));
                int op = opDist(rng);

                bool success = false;
                switch (op) {
                    case 0: // increment
                        success = refCounter->increment(hash, 4096).has_value();
                        break;
                    case 1: // decrement
                        success = refCounter->decrement(hash).has_value();
                        break;
                    case 2: // getRefCount
                        success = refCounter->getRefCount(hash).has_value();
                        break;
                    case 3: // getStats
                        success = refCounter->getStats().has_value();
                        break;
                }

                if (success) {
                    successCount++;
                } else {
                    errorCount++;
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_GT(successCount.load(), numThreads * opsPerThread * 0.99)
        << "At least 99% of operations should succeed";
}

// Test concurrent transactions with commit/rollback
TEST_F(ReferenceCounterStressTest, ConcurrentTransactions) {
    constexpr int numThreads = 12;
    constexpr int txnsPerThread = 50;
    constexpr int opsPerTxn = 10;

    std::atomic<int> commitCount{0};
    std::atomic<int> rollbackCount{0};
    std::atomic<int> errorCount{0};
    std::vector<std::thread> threads;
    std::latch startLatch(numThreads);

    for (int t = 0; t < numThreads; ++t) {
        threads.emplace_back([&, threadId = t]() {
            std::mt19937 rng(threadId);
            std::uniform_int_distribution<int> hashDist(0, 99);
            std::bernoulli_distribution shouldCommit(0.8); // 80% commit, 20% rollback

            startLatch.arrive_and_wait();

            for (int i = 0; i < txnsPerThread; ++i) {
                auto txn = refCounter->beginTransaction();
                if (!txn) {
                    errorCount++;
                    continue;
                }

                // Add operations
                for (int op = 0; op < opsPerTxn; ++op) {
                    auto hash = generateHash(threadId * 1000 + i * 100 + op);
                    if (op % 2 == 0) {
                        txn->increment(hash, 4096);
                    } else {
                        txn->decrement(hash);
                    }
                }

                // Randomly commit or rollback
                if (shouldCommit(rng)) {
                    auto result = txn->commit();
                    if (result.has_value()) {
                        commitCount++;
                    } else {
                        errorCount++;
                    }
                } else {
                    txn->rollback();
                    rollbackCount++;
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_GT(commitCount.load(), 0);
    EXPECT_GT(rollbackCount.load(), 0);
    EXPECT_EQ(errorCount.load(), 0) << "No transactions should fail";
    EXPECT_EQ(commitCount + rollbackCount, numThreads * txnsPerThread);
}

// Test getUnreferencedBlocks under concurrent modification
TEST_F(ReferenceCounterStressTest, ConcurrentGetUnreferencedBlocks) {
    constexpr int numReaderThreads = 4;
    constexpr int numWriterThreads = 8;
    constexpr int iterations = 100;
    constexpr int numBlocks = 200;

    std::atomic<bool> stopFlag{false};
    std::atomic<int> readerSuccessCount{0};
    std::atomic<int> writerSuccessCount{0};
    std::vector<std::thread> threads;

    // Writer threads: constantly increment/decrement
    for (int t = 0; t < numWriterThreads; ++t) {
        threads.emplace_back([&, threadId = t]() {
            std::mt19937 rng(threadId);
            std::uniform_int_distribution<int> blockDist(0, numBlocks - 1);

            while (!stopFlag.load(std::memory_order_relaxed)) {
                auto hash = generateHash(blockDist(rng));
                if (threadId % 2 == 0) {
                    if (refCounter->increment(hash, 4096).has_value()) {
                        writerSuccessCount++;
                    }
                } else {
                    if (refCounter->decrement(hash).has_value()) {
                        writerSuccessCount++;
                    }
                }
                std::this_thread::sleep_for(1ms);
            }
        });
    }

    // Reader threads: constantly call getUnreferencedBlocks
    for (int t = 0; t < numReaderThreads; ++t) {
        threads.emplace_back([&]() {
            for (int i = 0; i < iterations; ++i) {
                auto blocks = refCounter->getUnreferencedBlocks(50, std::chrono::seconds(0));
                if (blocks.has_value()) {
                    readerSuccessCount++;
                }
                std::this_thread::sleep_for(5ms);
            }
        });
    }

    // Let readers finish
    std::this_thread::sleep_for(1s);
    stopFlag.store(true, std::memory_order_relaxed);

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_GT(readerSuccessCount.load(), 0);
    EXPECT_GT(writerSuccessCount.load(), 0);
}

// Test heavy load with batch operations
TEST_F(ReferenceCounterStressTest, HighLoadBatchOperations) {
    constexpr int numThreads = 8;
    constexpr int batchesPerThread = 100;
    constexpr int batchSize = 100;

    std::atomic<int> successCount{0};
    std::vector<std::thread> threads;
    std::latch startLatch(numThreads);

    for (int t = 0; t < numThreads; ++t) {
        threads.emplace_back([&, threadId = t]() {
            startLatch.arrive_and_wait();

            for (int b = 0; b < batchesPerThread; ++b) {
                std::vector<std::string> hashes;
                for (int i = 0; i < batchSize; ++i) {
                    hashes.push_back(generateHash(threadId * 100000 + b * 1000 + i));
                }

                if (refCounter->incrementBatch(hashes, 4096).has_value()) {
                    successCount++;
                }

                if (refCounter->decrementBatch(hashes).has_value()) {
                    successCount++;
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(successCount.load(), numThreads * batchesPerThread * 2);
}

// Torture test: all operations at once under sustained load
TEST_F(ReferenceCounterStressTest, TortureTest) {
    constexpr int numThreads = 24;
    constexpr auto testDuration = 5s;
    constexpr int numHashes = 1000;

    std::atomic<bool> stopFlag{false};
    std::atomic<int> totalOps{0};
    std::atomic<int> errors{0};
    std::vector<std::thread> threads;

    auto startTime = std::chrono::steady_clock::now();

    for (int t = 0; t < numThreads; ++t) {
        threads.emplace_back([&, threadId = t]() {
            std::mt19937 rng(threadId + 12345);
            std::uniform_int_distribution<int> hashDist(0, numHashes - 1);
            std::uniform_int_distribution<int> opDist(0, 10);

            while (!stopFlag.load(std::memory_order_relaxed)) {
                auto hash = generateHash(hashDist(rng));
                bool success = false;

                switch (opDist(rng)) {
                    case 0:
                    case 1:
                    case 2:
                        success = refCounter->increment(hash, 4096).has_value();
                        break;
                    case 3:
                    case 4:
                        success = refCounter->decrement(hash).has_value();
                        break;
                    case 5:
                        success = refCounter->getRefCount(hash).has_value();
                        break;
                    case 6:
                        success = refCounter->hasReferences(hash).has_value();
                        break;
                    case 7:
                        success = refCounter->getStats().has_value();
                        break;
                    case 8:
                        success = refCounter->getUnreferencedBlocks(10, 0s).has_value();
                        break;
                    case 9:
                    case 10: {
                        auto txn = refCounter->beginTransaction();
                        if (txn) {
                            txn->increment(hash, 4096);
                            success = txn->commit().has_value();
                        }
                        break;
                    }
                }

                if (success) {
                    totalOps++;
                } else {
                    errors++;
                }
            }
        });
    }

    // Let it run for the test duration
    std::this_thread::sleep_for(testDuration);
    stopFlag.store(true, std::memory_order_relaxed);

    for (auto& t : threads) {
        t.join();
    }

    auto endTime = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

    int opsCompleted = totalOps.load();
    double opsPerSecond = (opsCompleted * 1000.0) / elapsed.count();

    std::cout << "Torture Test Results:\n"
              << "  Duration: " << elapsed.count() << "ms\n"
              << "  Operations: " << opsCompleted << "\n"
              << "  Errors: " << errors.load() << "\n"
              << "  Throughput: " << std::format("{:.2f}", opsPerSecond) << " ops/sec\n";

    EXPECT_GT(opsCompleted, 1000) << "Should complete at least 1000 operations";
    EXPECT_LT(errors.load(), opsCompleted * 0.01) << "Error rate should be < 1%";

    // Final consistency check
    auto finalStats = refCounter->getStats();
    ASSERT_TRUE(finalStats.has_value());
    std::cout << "  Final Stats: " << finalStats.value().totalBlocks << " blocks, "
              << finalStats.value().totalReferences << " refs\n";
}

// Test that reproduces the exact crash scenario from the bug report
TEST_F(ReferenceCounterStressTest, ReproduceGetStatsCrash) {
    constexpr int numThreads = 10;
    constexpr int iterations = 200;

    // This simulates the daemon metrics refresh thread calling getStats()
    // while other threads are doing work

    std::atomic<bool> stopFlag{false};
    std::vector<std::thread> threads;

    // Background worker threads
    for (int t = 0; t < numThreads - 1; ++t) {
        threads.emplace_back([&, threadId = t]() {
            std::mt19937 rng(threadId);
            std::uniform_int_distribution<int> hashDist(0, 49);

            while (!stopFlag.load(std::memory_order_relaxed)) {
                auto hash = generateHash(hashDist(rng));
                refCounter->increment(hash, 4096);
                std::this_thread::sleep_for(10ms);
                refCounter->decrement(hash);
            }
        });
    }

    // Metrics thread (reproduces DaemonMetrics::refresh calling getStats)
    threads.emplace_back([&]() {
        for (int i = 0; i < iterations; ++i) {
            auto stats = refCounter->getStats();
            ASSERT_TRUE(stats.has_value())
                << "getStats() should never fail (iteration " << i << ")";

            // Verify stats are valid (not garbage from use-after-free)
            EXPECT_LT(stats.value().totalBlocks, 1000000u);
            EXPECT_LT(stats.value().totalReferences, 1000000u);

            std::this_thread::sleep_for(20ms); // Similar to daemon refresh interval
        }
    });

    // Let it run
    std::this_thread::sleep_for(5s);
    stopFlag.store(true, std::memory_order_relaxed);

    for (auto& t : threads) {
        t.join();
    }
}
