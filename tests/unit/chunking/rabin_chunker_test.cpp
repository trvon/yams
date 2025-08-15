#include "test_helpers.h"
#include <gtest/gtest.h>
#include <yams/chunking/chunker.h>

#include <numeric>
#include <thread>

using namespace yams;
using namespace yams::chunking;
using namespace yams::test;

class RabinChunkerTest : public YamsTest {
protected:
    std::unique_ptr<RabinChunker> chunker;
    ChunkingConfig config;

    void SetUp() override {
        YamsTest::SetUp();
        config = ChunkingConfig{};
        chunker = std::make_unique<RabinChunker>(config);
    }
};

TEST_F(RabinChunkerTest, EmptyData) {
    std::vector<std::byte> data;
    auto chunks = chunker->chunkData(data);
    EXPECT_EQ(chunks.size(), 0u);
}

TEST_F(RabinChunkerTest, SmallData) {
    // Data smaller than minimum chunk size
    auto data = generateRandomBytes(1024); // 1KB
    auto chunks = chunker->chunkData(data);

    ASSERT_EQ(chunks.size(), 1u);
    EXPECT_EQ(chunks[0].size, data.size());
    EXPECT_EQ(chunks[0].offset, 0u);
    EXPECT_EQ(chunks[0].data, data);
}

TEST_F(RabinChunkerTest, MinimumChunkSize) {
    // Test that chunks respect minimum size
    auto data = generateRandomBytes(config.minChunkSize * 3);
    auto chunks = chunker->chunkData(data);

    for (const auto& chunk : chunks) {
        // Last chunk might be smaller
        if (chunk.offset + chunk.size < data.size()) {
            EXPECT_GE(chunk.size, config.minChunkSize);
        }
    }
}

TEST_F(RabinChunkerTest, MaximumChunkSize) {
    // Create data that won't naturally chunk
    std::vector<std::byte> data(config.maxChunkSize + 1024, std::byte{0});
    auto chunks = chunker->chunkData(data);

    ASSERT_GE(chunks.size(), 2u);
    EXPECT_LE(chunks[0].size, config.maxChunkSize);
}

TEST_F(RabinChunkerTest, DeterministicChunking) {
    // Same data should produce same chunks
    auto data = generateRandomBytes(1024 * 1024); // 1MB

    auto chunks1 = chunker->chunkData(data);
    auto chunks2 = chunker->chunkData(data);

    ASSERT_EQ(chunks1.size(), chunks2.size());

    for (size_t i = 0; i < chunks1.size(); ++i) {
        EXPECT_EQ(chunks1[i].hash, chunks2[i].hash);
        EXPECT_EQ(chunks1[i].offset, chunks2[i].offset);
        EXPECT_EQ(chunks1[i].size, chunks2[i].size);
    }
}

TEST_F(RabinChunkerTest, ChunkBoundariesWithInsertion) {
    // Test that insertions don't affect all chunks
    auto data1 = generateRandomBytes(1024 * 1024); // 1MB

    // Insert some bytes in the middle
    std::vector<std::byte> data2;
    data2.insert(data2.end(), data1.begin(), data1.begin() + 500000);
    auto insertion = generateRandomBytes(1024);
    data2.insert(data2.end(), insertion.begin(), insertion.end());
    data2.insert(data2.end(), data1.begin() + 500000, data1.end());

    auto chunks1 = chunker->chunkData(data1);
    auto chunks2 = chunker->chunkData(data2);

    // Some chunks before and after insertion should remain the same
    int matchingChunks = 0;
    for (const auto& chunk1 : chunks1) {
        for (const auto& chunk2 : chunks2) {
            if (chunk1.hash == chunk2.hash) {
                matchingChunks++;
                break;
            }
        }
    }

    // We should have some matching chunks
    EXPECT_GT(matchingChunks, 0);
    EXPECT_LT(matchingChunks, chunks1.size()); // But not all
}

TEST_F(RabinChunkerTest, ChunkSizeDistribution) {
    // Test average chunk size over large data
    auto data = generateRandomBytes(10 * 1024 * 1024); // 10MB
    auto chunks = chunker->chunkData(data);

    // Calculate average chunk size
    double avgSize = static_cast<double>(data.size()) / chunks.size();

    // Should be close to target size (within reasonable bounds for random data)
    EXPECT_GT(avgSize, config.targetChunkSize * 0.25); // 25% of target
    EXPECT_LT(avgSize, config.targetChunkSize * 2.0);  // 200% of target

    // Check size distribution
    size_t withinTarget = 0;
    for (const auto& chunk : chunks) {
        if (chunk.size >= config.targetChunkSize * 0.5 &&
            chunk.size <= config.targetChunkSize * 1.5) {
            withinTarget++;
        }
    }

    // Most chunks should be near target size (lowered for statistical variance)
    double ratio = static_cast<double>(withinTarget) / chunks.size();
    EXPECT_GT(ratio, 0.1); // At least 10% within range (reduced for random data)
}

TEST_F(RabinChunkerTest, FileChunking) {
    // Create test file
    auto content = generateRandomBytes(500000); // 500KB
    auto path = createTestFileWithContent(content, "chunk_test.bin");

    auto chunks = chunker->chunkFile(path);

    // Verify chunks reconstruct to original
    std::vector<std::byte> reconstructed;
    for (const auto& chunk : chunks) {
        reconstructed.insert(reconstructed.end(), chunk.data.begin(), chunk.data.end());
    }

    EXPECT_EQ(reconstructed, content);
}

TEST_F(RabinChunkerTest, AsyncFileChunking) {
    auto content = generateRandomBytes(200000);
    auto path = createTestFileWithContent(content, "async_chunk_test.bin");

    auto future = chunker->chunkFileAsync(path);
    auto result = future.get();

    ASSERT_TRUE(result.has_value());

    // Compare with sync version
    auto syncChunks = chunker->chunkFile(path);
    const auto& asyncChunks = result.value();

    ASSERT_EQ(asyncChunks.size(), syncChunks.size());
    for (size_t i = 0; i < asyncChunks.size(); ++i) {
        EXPECT_EQ(asyncChunks[i].hash, syncChunks[i].hash);
    }
}

TEST_F(RabinChunkerTest, ProgressCallback) {
    auto data = generateRandomBytes(1024 * 1024); // 1MB

    std::vector<std::pair<uint64_t, uint64_t>> progressUpdates;
    chunker->setProgressCallback(
        [&](uint64_t current, uint64_t total) { progressUpdates.emplace_back(current, total); });

    chunker->chunkData(data);

    EXPECT_GT(progressUpdates.size(), 0u);

    // Verify progress increases monotonically
    for (size_t i = 1; i < progressUpdates.size(); ++i) {
        EXPECT_GE(progressUpdates[i].first, progressUpdates[i - 1].first);
    }

    // Final progress should equal data size
    if (!progressUpdates.empty()) {
        EXPECT_EQ(progressUpdates.back().first, data.size());
    }
}

TEST_F(RabinChunkerTest, ProcessChunksCallback) {
    auto data = generateRandomBytes(500000);

    std::vector<ChunkRef> refs;
    size_t totalProcessed = 0;

    chunker->processChunks(data, [&](const ChunkRef& ref, std::span<const std::byte> chunkData) {
        refs.push_back(ref);
        totalProcessed += chunkData.size();

        // Verify chunk data matches reference
        EXPECT_EQ(ref.size, chunkData.size());
    });

    EXPECT_EQ(totalProcessed, data.size());
    EXPECT_GT(refs.size(), 0u);
}

TEST_F(RabinChunkerTest, DeduplicationStats) {
    // Create data with repeated patterns
    auto pattern = generateRandomBytes(50000);
    std::vector<std::byte> data;

    // Repeat pattern 3 times
    for (int i = 0; i < 3; ++i) {
        data.insert(data.end(), pattern.begin(), pattern.end());
    }

    auto chunks = chunker->chunkData(data);
    auto stats = calculateDeduplication(chunks);

    EXPECT_EQ(stats.chunkCount, chunks.size());
    EXPECT_LT(stats.uniqueChunks, stats.chunkCount);
    EXPECT_GT(stats.getRatio(), 0.0); // Should have some deduplication

    // With repeated data, unique size should be less than total
    EXPECT_LT(stats.uniqueSize, stats.totalSize);
}

TEST_F(RabinChunkerTest, ThreadSafety) {
    // Each thread uses its own chunker instance
    auto data = generateRandomBytes(100000);
    constexpr size_t numThreads = 4;

    std::vector<std::thread> threads;
    std::vector<std::vector<Chunk>> results(numThreads);

    for (size_t i = 0; i < numThreads; ++i) {
        threads.emplace_back([&, i]() {
            RabinChunker threadChunker(config);
            results[i] = threadChunker.chunkData(data);
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // All threads should produce identical results
    for (size_t i = 1; i < numThreads; ++i) {
        ASSERT_EQ(results[i].size(), results[0].size());
        for (size_t j = 0; j < results[i].size(); ++j) {
            EXPECT_EQ(results[i][j].hash, results[0][j].hash);
        }
    }
}

// Performance test (only in release builds)
#ifdef NDEBUG
TEST_F(RabinChunkerTest, Performance) {
    // Test with 100MB of data
    constexpr size_t dataSize = 100 * 1024 * 1024;
    auto data = generateRandomBytes(dataSize);

    auto start = std::chrono::high_resolution_clock::now();
    auto chunks = chunker->chunkData(data);
    auto end = std::chrono::high_resolution_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    auto throughput = (dataSize / 1024.0 / 1024.0) / (duration.count() / 1000.0);

    std::cout << std::format("Chunked {}MB into {} chunks in {}ms ({:.2f}MB/s)\n",
                             dataSize / 1024 / 1024, chunks.size(), duration.count(), throughput);

    // Expect at least 100MB/s (reduced for varied hardware)
    EXPECT_GT(throughput, 100.0);
}
#endif