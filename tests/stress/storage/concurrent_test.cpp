#include <spdlog/spdlog.h>
#include <gtest/gtest.h>
#include <yams/crypto/hasher.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/storage/storage_engine.h>

#include <array>
#include <atomic>
#include <barrier>
#include <chrono>
#include <latch>
#include <random>
#include <thread>
#include <vector>

using namespace yams;
using namespace yams::storage;
using namespace std::chrono_literals;

// Helper function to generate random bytes
static std::vector<std::byte> generateRandomBytes(size_t size) {
    std::vector<std::byte> data(size);
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 255);
    for (auto& b : data) {
        b = std::byte{static_cast<uint8_t>(dis(gen))};
    }
    return data;
}

class ConcurrentStorageTest : public ::testing::Test {
protected:
    std::unique_ptr<StorageEngine> storage;
    std::filesystem::path storagePath;

    void SetUp() override {
        storagePath = std::filesystem::temp_directory_path() / "yams_stress_concurrent_test";
        std::filesystem::create_directories(storagePath);

        StorageConfig config{.basePath = storagePath,
                             .shardDepth = 2,
                             .mutexPoolSize = 1024,
                             .maxConcurrentReaders = 1000,
                             .maxConcurrentWriters = 100};

        storage = std::make_unique<StorageEngine>(std::move(config));
    }

    void TearDown() override {
        storage.reset();
        std::filesystem::remove_all(storagePath);
    }
};

double runPoolWorkload(yams::metadata::ConnectionPool& pool, std::size_t workers,
                       std::size_t opsPerThread) {
    std::atomic<std::size_t> errors{0};
    auto start = std::chrono::steady_clock::now();
    std::vector<std::thread> threads;
    threads.reserve(workers);
    for (std::size_t t = 0; t < workers; ++t) {
        threads.emplace_back([&, threadIndex = t]() {
            for (std::size_t op = 0; op < opsPerThread; ++op) {
                auto res =
                    pool.withConnection([&](yams::metadata::Database& db) -> yams::Result<void> {
                        auto stmtRes = db.prepare("INSERT INTO kv(test_key, value) VALUES (?, ?);");
                        if (!stmtRes)
                            return stmtRes.error();
                        auto stmt = std::move(stmtRes.value());
                        auto bindRes = stmt.bindAll(static_cast<int>(threadIndex),
                                                    std::string("payload-") + std::to_string(op));
                        if (!bindRes)
                            return bindRes.error();
                        return stmt.execute();
                    });
                if (!res)
                    errors.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    for (auto& th : threads)
        th.join();
    auto end = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration<double>(end - start).count();
    EXPECT_EQ(errors.load(), 0U);
    double ops = static_cast<double>(workers * opsPerThread);
    return (elapsed > 0.0) ? ops / elapsed : ops;
}

TEST_F(ConcurrentStorageTest, ThousandConcurrentReaders) {
    // Pre-store test objects with varying sizes
    constexpr size_t numObjects = 100;
    std::vector<std::pair<std::string, std::vector<std::byte>>> testObjects;

    // Test multiple object size categories
    std::vector<size_t> sizeBuckets = {512, 1024, 4096, 8192, 16384};

    for (size_t i = 0; i < numObjects; ++i) {
        size_t sizeCategory = sizeBuckets[i % sizeBuckets.size()];
        auto data = generateRandomBytes(sizeCategory + (i % 100));
        auto hasher = crypto::createSHA256Hasher();
        auto hash = hasher->hash(data);

        testObjects.emplace_back(hash, data);
        ASSERT_TRUE(storage->store(hash, data).has_value());
    }

    // Test with different reader counts to validate scalability
    std::vector<size_t> readerCounts = {100, 500, 1000};

    for (size_t numReaders : readerCounts) {
        std::cout << "\n=== Testing with " << numReaders << " concurrent readers ===" << std::endl;

        constexpr size_t readsPerThread = 100;

        std::atomic<size_t> totalReads{0};
        std::atomic<size_t> successfulReads{0};
        std::atomic<bool> hasErrors{false};

        // Use C++20 barrier for synchronization
        std::barrier startBarrier(numReaders + 1);

        auto startTime = std::chrono::steady_clock::now();

        std::vector<std::thread> readers;
        readers.reserve(numReaders);

        for (size_t i = 0; i < numReaders; ++i) {
            readers.emplace_back([&, threadId = i]() {
                std::random_device rd;
                std::mt19937 gen(rd() + threadId);
                std::uniform_int_distribution<size_t> dis(0, numObjects - 1);

                // Wait for all threads to be ready
                startBarrier.arrive_and_wait();

                for (size_t j = 0; j < readsPerThread; ++j) {
                    auto& [hash, expectedData] = testObjects[dis(gen)];

                    totalReads.fetch_add(1);
                    auto result = storage->retrieve(hash);

                    if (!result.has_value()) {
                        hasErrors.store(true);
                        spdlog::error("Read failed for hash: {}", hash);
                    } else if (result.value() != expectedData) {
                        hasErrors.store(true);
                        spdlog::error("Data mismatch for hash: {}", hash);
                    } else {
                        successfulReads.fetch_add(1);
                    }
                }
            });
        }

        // Start all threads simultaneously
        startBarrier.arrive_and_wait();

        // Wait for all readers to complete
        for (auto& reader : readers) {
            reader.join();
        }

        auto endTime = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

        // Verify results
        EXPECT_FALSE(hasErrors.load()) << "Errors with " << numReaders << " readers";
        EXPECT_EQ(successfulReads.load(), numReaders * readsPerThread);

        // Performance check
        auto readsPerSecond = (totalReads.load() * 1000) / duration.count();
        spdlog::info("Completed {} reads in {} ms ({} reads/sec)", totalReads.load(),
                     duration.count(), readsPerSecond);

        // Verify < 10ms latency requirement
        auto avgLatency = duration.count() / static_cast<double>(readsPerThread);
        EXPECT_LT(avgLatency, 15.0) << "Average latency too high with " << numReaders << " readers";
    }
}

TEST_F(ConcurrentStorageTest, HundredConcurrentWriters) {
    constexpr size_t numWriters = 100;
    constexpr size_t writesPerThread = 50;

    std::atomic<size_t> totalWrites{0};
    std::atomic<size_t> successfulWrites{0};
    std::atomic<bool> hasErrors{false};

    // C++20 latch for completion tracking
    std::latch completionLatch(numWriters);

    auto startTime = std::chrono::steady_clock::now();

    std::vector<std::thread> writers;
    writers.reserve(numWriters);

    for (size_t i = 0; i < numWriters; ++i) {
        writers.emplace_back([&, threadId = i]() {
            for (size_t j = 0; j < writesPerThread; ++j) {
                // Generate unique data for each write
                auto data = generateRandomBytes(512 + (threadId * 100 + j) % 1024);
                auto hasher = crypto::createSHA256Hasher();
                auto hash = hasher->hash(data);

                totalWrites.fetch_add(1);
                auto result = storage->store(hash, data);

                if (!result.has_value()) {
                    hasErrors.store(true);
                    spdlog::error("Write failed for thread {} iteration {}", threadId, j);
                } else {
                    successfulWrites.fetch_add(1);
                }
            }

            completionLatch.count_down();
        });
    }

    // Wait for all writers
    for (auto& writer : writers) {
        writer.join();
    }

    auto endTime = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

    // Verify results
    EXPECT_FALSE(hasErrors.load());
    EXPECT_EQ(successfulWrites.load(), numWriters * writesPerThread);

    // Check storage statistics
    auto stats = storage->getStats();
    EXPECT_EQ(stats.writeOperations.load(), numWriters * writesPerThread);

    spdlog::info("Completed {} writes in {} ms ({} writes/sec)", totalWrites.load(),
                 duration.count(), (totalWrites.load() * 1000) / duration.count());
}

TEST_F(ConcurrentStorageTest, MixedReadWriteWorkload) {
    // Test multiple read/write ratios
    struct WorkloadConfig {
        size_t numReaders;
        size_t numWriters;
        const char* description;
    };

    std::vector<WorkloadConfig> workloads = {{160, 40, "80/20 read/write (typical)"},
                                             {180, 20, "90/10 read/write (read-heavy)"},
                                             {100, 100, "50/50 read/write (balanced)"},
                                             {50, 150, "25/75 read/write (write-heavy)"}};

    for (const auto& workload : workloads) {
        spdlog::info("Testing workload: {}", workload.description);

        const size_t numThreads = workload.numReaders + workload.numWriters;
        constexpr size_t operationsPerThread = 100;

        // Pre-populate with some data
        std::vector<std::string> existingHashes;
        for (size_t i = 0; i < 500; ++i) {
            auto data = generateRandomBytes(1024 + (i % 1024));
            auto hasher = crypto::createSHA256Hasher();
            auto hash = hasher->hash(data);
            existingHashes.push_back(hash);
            storage->store(hash, data);
        }

        std::atomic<size_t> readOps{0};
        std::atomic<size_t> writeOps{0};
        std::atomic<size_t> conflicts{0};

        std::barrier startBarrier(numThreads + 1);
        std::vector<std::thread> threads;

        // Reader threads
        for (size_t i = 0; i < workload.numReaders; ++i) {
            threads.emplace_back([&]() {
                std::random_device rd;
                std::mt19937 gen(rd());
                std::uniform_int_distribution<size_t> dis(0, existingHashes.size() - 1);

                startBarrier.arrive_and_wait();

                for (size_t j = 0; j < operationsPerThread; ++j) {
                    auto& hash = existingHashes[dis(gen)];
                    storage->retrieve(hash);
                    readOps.fetch_add(1);
                }
            });
        }

        // Writer threads
        for (size_t i = 0; i < workload.numWriters; ++i) {
            threads.emplace_back([&, threadId = i]() {
                startBarrier.arrive_and_wait();

                for (size_t j = 0; j < operationsPerThread; ++j) {
                    auto data = generateRandomBytes(2048);
                    auto hasher = crypto::createSHA256Hasher();
                    auto hash = hasher->hash(data);

                    auto result = storage->store(hash, data);
                    if (result.has_value()) {
                        writeOps.fetch_add(1);
                    } else {
                        conflicts.fetch_add(1);
                    }
                }
            });
        }

        auto startTime = std::chrono::steady_clock::now();
        startBarrier.arrive_and_wait();

        for (auto& thread : threads) {
            thread.join();
        }

        auto endTime = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

        spdlog::info("  Completed: {} reads, {} writes in {} ms", readOps.load(), writeOps.load(),
                     duration.count());

        EXPECT_EQ(readOps.load(), workload.numReaders * operationsPerThread)
            << "Failed for workload: " << workload.description;
        EXPECT_EQ(writeOps.load(), workload.numWriters * operationsPerThread)
            << "Failed for workload: " << workload.description;
        EXPECT_EQ(conflicts.load(), 0u) << "Conflicts in workload: " << workload.description;

        // Cleanup for next workload
        existingHashes.clear();
    }
}

TEST_F(ConcurrentStorageTest, HighContentionSameObjects) {
    // Test with different contention levels
    struct ContentionConfig {
        size_t numThreads;
        size_t numObjects;
        const char* description;
    };

    std::vector<ContentionConfig> configs = {
        {50, 10, "High contention (50 threads, 10 objects)"},
        {100, 20, "Very high contention (100 threads, 20 objects)"},
        {25, 50, "Moderate contention (25 threads, 50 objects)"},
        {50, 100, "Low contention (50 threads, 100 objects)"}};

    for (const auto& config : configs) {
        spdlog::info("Testing: {}", config.description);

        constexpr size_t opsPerThread = 200;

        // Create a set of objects
        std::vector<std::pair<std::string, std::vector<std::byte>>> objects;
        for (size_t i = 0; i < config.numObjects; ++i) {
            auto data = generateRandomBytes(4096 + (i % 1024));
            auto hasher = crypto::createSHA256Hasher();
            auto hash = hasher->hash(data);
            objects.emplace_back(hash, data);
            storage->store(hash, data);
        }

        std::atomic<size_t> operations{0};
        std::atomic<bool> dataCorruption{false};
        std::atomic<size_t> readOps{0};
        std::atomic<size_t> writeOps{0};

        std::vector<std::thread> threads;
        for (size_t i = 0; i < config.numThreads; ++i) {
            threads.emplace_back([&]() {
                std::random_device rd;
                std::mt19937 gen(rd());
                std::uniform_int_distribution<size_t> objDis(0, config.numObjects - 1);
                std::uniform_int_distribution<int> opDis(0, 9);

                for (size_t j = 0; j < opsPerThread; ++j) {
                    auto& [hash, expectedData] = objects[objDis(gen)];

                    if (opDis(gen) < 8) { // 80% reads
                        auto result = storage->retrieve(hash);
                        if (!result.has_value() || result.value() != expectedData) {
                            dataCorruption.store(true);
                        }
                        readOps.fetch_add(1);
                    } else { // 20% re-writes (same data)
                        storage->store(hash, expectedData);
                        writeOps.fetch_add(1);
                    }

                    operations.fetch_add(1);
                }
            });
        }

        for (auto& thread : threads) {
            thread.join();
        }

        spdlog::info("  Ops: {} ({} reads, {} writes)", operations.load(), readOps.load(),
                     writeOps.load());

        EXPECT_FALSE(dataCorruption.load()) << "Data corruption in: " << config.description;
        EXPECT_EQ(operations.load(), config.numThreads * opsPerThread);
    }
}

TEST_F(ConcurrentStorageTest, RapidCreateDeleteCycles) {
    constexpr size_t numThreads = 20;
    constexpr size_t cyclesPerThread = 50;

    std::atomic<size_t> creates{0};
    std::atomic<size_t> deletes{0};
    std::atomic<bool> errors{false};

    std::vector<std::thread> threads;
    for (size_t i = 0; i < numThreads; ++i) {
        threads.emplace_back([&, threadId = i]() {
            for (size_t j = 0; j < cyclesPerThread; ++j) {
                // Generate unique data
                auto data = generateRandomBytes(1024 + threadId * 100 + j);
                auto hasher = crypto::createSHA256Hasher();
                auto hash = hasher->hash(data);

                // Create
                if (!storage->store(hash, data).has_value()) {
                    errors.store(true);
                    continue;
                }
                creates.fetch_add(1);

                // Verify
                auto result = storage->retrieve(hash);
                if (!result.has_value() || result.value() != data) {
                    errors.store(true);
                    continue;
                }

                // Delete
                if (!storage->remove(hash).has_value()) {
                    errors.store(true);
                    continue;
                }
                deletes.fetch_add(1);

                // Verify deletion
                auto existsRes = storage->exists(hash);
                if (!existsRes || existsRes.value()) {
                    errors.store(true);
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_FALSE(errors.load());
    EXPECT_EQ(creates.load(), numThreads * cyclesPerThread);
    EXPECT_EQ(deletes.load(), numThreads * cyclesPerThread);

    // Final storage should be empty
    auto stats = storage->getStats();
    EXPECT_EQ(stats.totalObjects.load(), 0u);
}

TEST_F(ConcurrentStorageTest, LatencyUnderLoad) {
    // Pre-populate storage
    for (size_t i = 0; i < 10000; ++i) {
        auto data = generateRandomBytes(512);
        auto hasher = crypto::createSHA256Hasher();
        auto hash = hasher->hash(data);
        storage->store(hash, data);
    }

    // Measure individual operation latencies under concurrent load
    constexpr size_t numBackgroundThreads = 50;
    constexpr size_t numMeasurements = 1000;

    std::atomic<bool> stopFlag{false};
    std::vector<std::thread> backgroundThreads;

    // Start background load
    for (size_t i = 0; i < numBackgroundThreads; ++i) {
        backgroundThreads.emplace_back([&]() {
            while (!stopFlag.load()) {
                auto data = generateRandomBytes(1024);
                auto hasher = crypto::createSHA256Hasher();
                auto hash = hasher->hash(data);
                storage->store(hash, data);
                storage->retrieve(hash);
            }
        });
    }

    // Measure latencies
    std::vector<double> latencies;
    latencies.reserve(numMeasurements);

    auto data = generateRandomBytes(1024);
    auto hasher = crypto::createSHA256Hasher();
    auto hash = hasher->hash(data);
    storage->store(hash, data);

    for (size_t i = 0; i < numMeasurements; ++i) {
        auto start = std::chrono::high_resolution_clock::now();
        auto exists = storage->exists(hash);
        auto end = std::chrono::high_resolution_clock::now();

        ASSERT_TRUE(exists.has_value());

        auto latency = std::chrono::duration<double, std::milli>(end - start).count();
        latencies.push_back(latency);
    }

    // Stop background threads
    stopFlag.store(true);
    for (auto& thread : backgroundThreads) {
        thread.join();
    }

    // Calculate statistics
    std::sort(latencies.begin(), latencies.end());
    double p50 = latencies[latencies.size() / 2];
    double p95 = latencies[latencies.size() * 95 / 100];
    double p99 = latencies[latencies.size() * 99 / 100];

    spdlog::info(
        "Existence check latencies under load - P50: {:.2f}ms, P95: {:.2f}ms, P99: {:.2f}ms", p50,
        p95, p99);

    // Verify < 10ms requirement
    EXPECT_LT(p99, 10.0);
}

TEST_F(ConcurrentStorageTest, ConnectionPoolWorkerSweep) {
    const auto dbPath = storagePath / "pool_bench.db";
    yams::metadata::Database bootstrap;
    ASSERT_TRUE(bootstrap.open(dbPath.string(), yams::metadata::ConnectionMode::Create));
    ASSERT_TRUE(bootstrap.execute("PRAGMA journal_mode=WAL;").has_value());
    ASSERT_TRUE(bootstrap.execute("CREATE TABLE IF NOT EXISTS kv (test_key INTEGER, value TEXT);")
                    .has_value());
    bootstrap.close();

    yams::metadata::ConnectionPoolConfig cfg;
    cfg.minConnections = 1;
    cfg.maxConnections = 8;
    cfg.busyTimeout = std::chrono::milliseconds(1000);
    yams::metadata::ConnectionPool pool(dbPath.string(), cfg);
    ASSERT_TRUE(pool.initialize());

    const std::array<std::size_t, 4> workerSweep{1, 2, 4, 8};
    const std::size_t opsPerThread = 150;
    double baseline = 0.0;

    for (std::size_t workers : workerSweep) {
        auto resetRes = pool.withConnection([](yams::metadata::Database& db) -> yams::Result<void> {
            return db.execute("DELETE FROM kv;");
        });
        ASSERT_TRUE(resetRes);

        double throughput = runPoolWorkload(pool, workers, opsPerThread);
        spdlog::info("pool sweep workers={} throughput={}", workers, throughput);
        if (baseline == 0.0) {
            baseline = throughput;
        } else if (baseline > 0.0) {
            double warnThreshold = baseline * 0.9;
            double cautionThreshold = baseline * 0.5;
            double failThreshold = baseline * 0.25;
            if (throughput < failThreshold) {
                ADD_FAILURE() << "connection pool regression for " << workers << " workers ("
                              << throughput << " < " << failThreshold << ")";
            } else if (throughput < cautionThreshold) {
                spdlog::warn(
                    "connection pool throughput dipped below 50% for workers={} ({} vs {} base)",
                    workers, throughput, baseline);
            } else if (throughput < warnThreshold) {
                spdlog::info(
                    "connection pool throughput dipped below 90% for workers={} ({} vs {} base)",
                    workers, throughput, baseline);
            }
        }
    }

    pool.shutdown();
    std::filesystem::remove(dbPath);
}

// New comprehensive stress test: Variable object sizes under concurrent access
TEST_F(ConcurrentStorageTest, VariableObjectSizesConcurrent) {
    spdlog::info("Testing variable object sizes under concurrent access");

    struct SizeClass {
        size_t minSize;
        size_t maxSize;
        size_t count;
        const char* name;
    };

    std::vector<SizeClass> sizeClasses = {{100, 1024, 100, "Tiny (100B-1KB)"},
                                          {1024, 10 * 1024, 100, "Small (1KB-10KB)"},
                                          {10 * 1024, 100 * 1024, 50, "Medium (10KB-100KB)"},
                                          {100 * 1024, 1024 * 1024, 20, "Large (100KB-1MB)"}};

    // Pre-populate with objects of varying sizes
    std::vector<std::pair<std::string, std::vector<std::byte>>> allObjects;

    for (const auto& sizeClass : sizeClasses) {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<size_t> sizeDist(sizeClass.minSize, sizeClass.maxSize);

        for (size_t i = 0; i < sizeClass.count; ++i) {
            auto data = generateRandomBytes(sizeDist(gen));
            auto hasher = crypto::createSHA256Hasher();
            auto hash = hasher->hash(data);
            allObjects.emplace_back(hash, data);
            ASSERT_TRUE(storage->store(hash, data).has_value());
        }
    }

    spdlog::info("Pre-populated {} objects across {} size classes", allObjects.size(),
                 sizeClasses.size());

    // Concurrent access test
    constexpr size_t numThreads = 50;
    constexpr size_t opsPerThread = 100;

    std::atomic<size_t> successfulOps{0};
    std::atomic<size_t> failedOps{0};
    std::atomic<size_t> corruptedReads{0};

    std::barrier startBarrier(numThreads + 1);
    std::vector<std::thread> threads;

    for (size_t i = 0; i < numThreads; ++i) {
        threads.emplace_back([&]() {
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<size_t> objDist(0, allObjects.size() - 1);

            startBarrier.arrive_and_wait();

            for (size_t j = 0; j < opsPerThread; ++j) {
                auto& [hash, expectedData] = allObjects[objDist(gen)];

                auto result = storage->retrieve(hash);
                if (!result.has_value()) {
                    failedOps.fetch_add(1);
                } else if (result.value() != expectedData) {
                    corruptedReads.fetch_add(1);
                } else {
                    successfulOps.fetch_add(1);
                }
            }
        });
    }

    startBarrier.arrive_and_wait();

    for (auto& thread : threads) {
        thread.join();
    }

    spdlog::info("Variable size test: {} successful, {} failed, {} corrupted", successfulOps.load(),
                 failedOps.load(), corruptedReads.load());

    EXPECT_EQ(successfulOps.load(), numThreads * opsPerThread);
    EXPECT_EQ(failedOps.load(), 0u);
    EXPECT_EQ(corruptedReads.load(), 0u);
}

// New stress test: Burst traffic patterns
TEST_F(ConcurrentStorageTest, BurstTrafficPattern) {
    spdlog::info("Testing burst traffic patterns");

    // Pre-populate
    std::vector<std::pair<std::string, std::vector<std::byte>>> objects;
    for (size_t i = 0; i < 100; ++i) {
        auto data = generateRandomBytes(2048);
        auto hasher = crypto::createSHA256Hasher();
        auto hash = hasher->hash(data);
        objects.emplace_back(hash, data);
        storage->store(hash, data);
    }

    struct BurstConfig {
        size_t numThreads;
        size_t opsPerThread;
        std::chrono::milliseconds burstDuration;
        std::chrono::milliseconds idleDuration;
        const char* name;
    };

    std::vector<BurstConfig> bursts = {{100, 50, 100ms, 200ms, "Quick burst"},
                                       {200, 25, 50ms, 150ms, "Intense burst"},
                                       {50, 100, 200ms, 100ms, "Sustained burst"}};

    for (const auto& burst : bursts) {
        spdlog::info("  Testing: {}", burst.name);

        std::atomic<size_t> totalOps{0};
        std::atomic<bool> hasError{false};

        // Burst phase
        std::vector<std::thread> threads;
        for (size_t i = 0; i < burst.numThreads; ++i) {
            threads.emplace_back([&]() {
                std::random_device rd;
                std::mt19937 gen(rd());
                std::uniform_int_distribution<size_t> objDist(0, objects.size() - 1);

                for (size_t j = 0; j < burst.opsPerThread; ++j) {
                    auto& [hash, expectedData] = objects[objDist(gen)];
                    auto result = storage->retrieve(hash);

                    if (!result.has_value() || result.value() != expectedData) {
                        hasError.store(true);
                    }
                    totalOps.fetch_add(1);
                }
            });
        }

        auto start = std::chrono::steady_clock::now();
        for (auto& thread : threads) {
            thread.join();
        }
        auto elapsed = std::chrono::steady_clock::now() - start;

        spdlog::info("    Completed {} ops in {}ms", totalOps.load(),
                     std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count());

        EXPECT_FALSE(hasError.load()) << "Errors in burst: " << burst.name;
        EXPECT_EQ(totalOps.load(), burst.numThreads * burst.opsPerThread);

        // Idle phase
        std::this_thread::sleep_for(burst.idleDuration);
    }
}

// New stress test: Long-running sustained load
TEST_F(ConcurrentStorageTest, LongRunningSustainedLoad) {
    spdlog::info("Testing long-running sustained load");

    constexpr size_t numThreads = 20;
    constexpr auto testDuration = 30s; // 30 seconds of sustained load

    // Pre-populate
    std::vector<std::pair<std::string, std::vector<std::byte>>> objects;
    for (size_t i = 0; i < 200; ++i) {
        auto data = generateRandomBytes(1024 + (i % 2048));
        auto hasher = crypto::createSHA256Hasher();
        auto hash = hasher->hash(data);
        objects.emplace_back(hash, data);
        storage->store(hash, data);
    }

    std::atomic<bool> stopFlag{false};
    std::atomic<size_t> totalOperations{0};
    std::atomic<size_t> readOps{0};
    std::atomic<size_t> writeOps{0};
    std::atomic<size_t> errors{0};

    std::vector<std::thread> threads;

    auto startTime = std::chrono::steady_clock::now();

    for (size_t i = 0; i < numThreads; ++i) {
        threads.emplace_back([&, threadId = i]() {
            std::random_device rd;
            std::mt19937 gen(rd() + threadId);
            std::uniform_int_distribution<size_t> objDist(0, objects.size() - 1);
            std::uniform_int_distribution<int> opTypeDist(0, 99);

            while (!stopFlag.load()) {
                int opType = opTypeDist(gen);

                if (opType < 70) { // 70% reads
                    auto& [hash, expectedData] = objects[objDist(gen)];
                    auto result = storage->retrieve(hash);

                    if (!result.has_value() || result.value() != expectedData) {
                        errors.fetch_add(1);
                    }
                    readOps.fetch_add(1);
                } else { // 30% writes (new objects)
                    auto data = generateRandomBytes(1024);
                    auto hasher = crypto::createSHA256Hasher();
                    auto hash = hasher->hash(data);

                    if (storage->store(hash, data).has_value()) {
                        writeOps.fetch_add(1);
                    } else {
                        errors.fetch_add(1);
                    }
                }

                totalOperations.fetch_add(1);
            }
        });
    }

    // Let it run for the test duration
    std::this_thread::sleep_for(testDuration);
    stopFlag.store(true);

    for (auto& thread : threads) {
        thread.join();
    }

    auto endTime = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(endTime - startTime).count();

    spdlog::info("Long-running test completed:");
    spdlog::info("  Duration: {}s", elapsed);
    spdlog::info("  Total ops: {}", totalOperations.load());
    spdlog::info("  Reads: {}, Writes: {}", readOps.load(), writeOps.load());
    spdlog::info("  Ops/sec: {}", totalOperations.load() / elapsed);
    spdlog::info("  Errors: {}", errors.load());

    EXPECT_EQ(errors.load(), 0u);
    EXPECT_GT(totalOperations.load(), numThreads * 100); // Should do many operations
}
