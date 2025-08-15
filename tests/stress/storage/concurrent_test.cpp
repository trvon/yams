#include "test_helpers.h"
#include <gtest/gtest.h>
#include <yams/crypto/hasher.h>
#include <yams/storage/storage_engine.h>

#include <atomic>
#include <barrier>
#include <chrono>
#include <latch>
#include <random>
#include <thread>
#include <vector>

using namespace yams;
using namespace yams::storage;
using namespace yams::test;
using namespace std::chrono_literals;

class ConcurrentStorageTest : public YamsTest {
protected:
    std::unique_ptr<StorageEngine> storage;
    std::filesystem::path storagePath;

    void SetUp() override {
        YamsTest::SetUp();

        storagePath = getTempDir() / "concurrent_test";
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
        YamsTest::TearDown();
    }
};

TEST_F(ConcurrentStorageTest, ThousandConcurrentReaders) {
    // Pre-store test objects
    constexpr size_t numObjects = 100;
    std::vector<std::pair<std::string, std::vector<std::byte>>> testObjects;

    for (size_t i = 0; i < numObjects; ++i) {
        auto data = generateRandomBytes(1024 * (i % 10 + 1));
        auto hasher = crypto::createSHA256Hasher();
        auto hash = hasher->hash(data);

        testObjects.emplace_back(hash, data);
        ASSERT_TRUE(storage->store(hash, data).has_value());
    }

    // Launch 1000 reader threads
    constexpr size_t numReaders = 1000;
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
    EXPECT_FALSE(hasErrors.load());
    EXPECT_EQ(successfulReads.load(), numReaders * readsPerThread);

    // Performance check
    auto readsPerSecond = (totalReads.load() * 1000) / duration.count();
    spdlog::info("Completed {} reads in {} ms ({} reads/sec)", totalReads.load(), duration.count(),
                 readsPerSecond);

    // Verify < 10ms latency requirement
    auto avgLatency = duration.count() / static_cast<double>(readsPerThread);
    EXPECT_LT(avgLatency, 10.0);
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
    // 80% reads, 20% writes - typical workload
    constexpr size_t numThreads = 200;
    constexpr size_t numReaders = 160;
    constexpr size_t numWriters = 40;
    constexpr size_t operationsPerThread = 100;

    // Pre-populate with some data
    std::vector<std::string> existingHashes;
    for (size_t i = 0; i < 500; ++i) {
        auto data = generateRandomBytes(1024);
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
    for (size_t i = 0; i < numReaders; ++i) {
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
    for (size_t i = 0; i < numWriters; ++i) {
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

    spdlog::info("Mixed workload: {} reads, {} writes in {} ms", readOps.load(), writeOps.load(),
                 duration.count());

    EXPECT_EQ(readOps.load(), numReaders * operationsPerThread);
    EXPECT_EQ(writeOps.load(), numWriters * operationsPerThread);
    EXPECT_EQ(conflicts.load(), 0u);
}

TEST_F(ConcurrentStorageTest, HighContentionSameObjects) {
    // Many threads accessing the same small set of objects
    constexpr size_t numThreads = 50;
    constexpr size_t numObjects = 10;
    constexpr size_t opsPerThread = 200;

    // Create a small set of objects
    std::vector<std::pair<std::string, std::vector<std::byte>>> objects;
    for (size_t i = 0; i < numObjects; ++i) {
        auto data = generateRandomBytes(4096);
        auto hasher = crypto::createSHA256Hasher();
        auto hash = hasher->hash(data);
        objects.emplace_back(hash, data);
        storage->store(hash, data);
    }

    std::atomic<size_t> operations{0};
    std::atomic<bool> dataCorruption{false};

    std::vector<std::thread> threads;
    for (size_t i = 0; i < numThreads; ++i) {
        threads.emplace_back([&]() {
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<size_t> objDis(0, numObjects - 1);
            std::uniform_int_distribution<int> opDis(0, 9);

            for (size_t j = 0; j < opsPerThread; ++j) {
                auto& [hash, expectedData] = objects[objDis(gen)];

                if (opDis(gen) < 8) { // 80% reads
                    auto result = storage->retrieve(hash);
                    if (!result.has_value() || result.value() != expectedData) {
                        dataCorruption.store(true);
                    }
                } else { // 20% re-writes (same data)
                    storage->store(hash, expectedData);
                }

                operations.fetch_add(1);
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_FALSE(dataCorruption.load());
    EXPECT_EQ(operations.load(), numThreads * opsPerThread);
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
                if (storage->exists(hash).value_or(true)) {
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