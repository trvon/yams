// Catch2 stress tests for reference counter
// Migrated from GTest: reference_counter_stress_test.cpp

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <format>
#include <future>
#include <latch>
#include <random>
#include <thread>
#include <vector>

#include <yams/storage/reference_counter.h>

using namespace yams::storage;
using namespace std::chrono_literals;

namespace {

struct ReferenceCounterStressFixture {
    ReferenceCounterStressFixture() {
        testDbPath = std::filesystem::temp_directory_path() /
                     std::format("yams_refcount_stress_catch2_{}.db",
                                 std::chrono::system_clock::now().time_since_epoch().count());

        ReferenceCounter::Config config{.databasePath = testDbPath,
                                        .enableWAL = true,
                                        .enableStatistics = true,
                                        .cacheSize = 10000,
                                        .busyTimeout = 5000,
                                        .enableAuditLog = false};

        refCounter = std::make_unique<ReferenceCounter>(std::move(config));
    }

    ~ReferenceCounterStressFixture() {
        refCounter.reset();
        std::filesystem::remove(testDbPath);
        std::filesystem::remove(testDbPath.string() + "-wal");
        std::filesystem::remove(testDbPath.string() + "-shm");
    }

    std::string generateHash(int i) { return std::format("stress_hash_{:064}", i); }

    std::filesystem::path testDbPath;
    std::unique_ptr<ReferenceCounter> refCounter;
};

} // namespace

TEST_CASE_METHOD(ReferenceCounterStressFixture, "ReferenceCounter concurrent getStats",
                 "[storage][refcount][stress][catch2]") {
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
        threads.emplace_back([&]() {
            // Wait for all threads to be ready
            startLatch.arrive_and_wait();

            for (int i = 0; i < numIterations; ++i) {
                auto stats = refCounter->getStats();
                if (stats.has_value()) {
                    successCount++;
                    // Verify stats make sense
                    CHECK(stats.value().totalBlocks >= 0u);
                    CHECK(stats.value().totalReferences >= 0u);
                } else {
                    errorCount++;
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    CHECK(successCount.load() > 0);
    CHECK(errorCount.load() == 0);

    // Final stats check
    auto finalStats = refCounter->getStats();
    REQUIRE(finalStats.has_value());
    CHECK(finalStats.value().totalBlocks == static_cast<uint64_t>(numBlocks));
    CHECK(finalStats.value().totalReferences == static_cast<uint64_t>(numBlocks));
}

TEST_CASE_METHOD(ReferenceCounterStressFixture, "ReferenceCounter concurrent mixed operations",
                 "[storage][refcount][stress][catch2]") {
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

    CHECK(successCount.load() > numThreads * opsPerThread * 0.99);
}

TEST_CASE_METHOD(ReferenceCounterStressFixture, "ReferenceCounter concurrent transactions",
                 "[storage][refcount][stress][transaction][catch2]") {
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

    CHECK(commitCount.load() > 0);
    CHECK(rollbackCount.load() > 0);
    CHECK(errorCount.load() == 0);
    CHECK(commitCount + rollbackCount == numThreads * txnsPerThread);
}

TEST_CASE_METHOD(ReferenceCounterStressFixture, "ReferenceCounter concurrent getUnreferencedBlocks",
                 "[storage][refcount][stress][catch2]") {
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

    CHECK(readerSuccessCount.load() > 0);
    CHECK(writerSuccessCount.load() > 0);
}

TEST_CASE_METHOD(ReferenceCounterStressFixture, "ReferenceCounter high load batch operations",
                 "[storage][refcount][stress][batch][catch2]") {
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

    CHECK(successCount.load() == numThreads * batchesPerThread * 2);
}

TEST_CASE_METHOD(ReferenceCounterStressFixture, "ReferenceCounter reproduce getStats crash",
                 "[storage][refcount][stress][regression][catch2]") {
    // This test reproduces the exact crash scenario from the bug report
    // Simulates the daemon metrics refresh thread calling getStats()
    // while other threads are doing work

    constexpr int numThreads = 10;
    constexpr int iterations = 200;

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
            REQUIRE(stats.has_value());

            // Verify stats are valid (not garbage from use-after-free)
            CHECK(stats.value().totalBlocks < 1000000u);
            CHECK(stats.value().totalReferences < 1000000u);

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
