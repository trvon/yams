#include <cstring>
#include <random>
#include <vector>
#include <gtest/gtest.h>
#include <yams/compression/compression_utils.h>
#include <yams/compression/compressor_interface.h>

using namespace yams;
using namespace yams::compression;

class ZstandardCompressorTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Get compressor from registry
        auto& registry = CompressionRegistry::instance();
        compressor_ = registry.createCompressor(CompressionAlgorithm::Zstandard);
        ASSERT_NE(compressor_, nullptr);
    }

    // Generate test data patterns
    std::vector<std::byte> generateTestData(size_t size, int pattern) {
        std::vector<std::byte> data(size);

        switch (pattern) {
            case 0: // All zeros (highly compressible)
                std::fill(data.begin(), data.end(), std::byte{0});
                break;

            case 1: // Repeating pattern
                for (size_t i = 0; i < size; ++i) {
                    data[i] = std::byte{static_cast<uint8_t>(i % 256)};
                }
                break;

            case 2: // Random data (incompressible)
            {
                std::random_device rd;
                std::mt19937 gen(rd());
                std::uniform_int_distribution<> dis(0, 255);
                for (auto& b : data) {
                    b = std::byte{static_cast<uint8_t>(dis(gen))};
                }
            } break;

            case 3: // Text-like data
            {
                const char* text = "The quick brown fox jumps over the lazy dog. ";
                size_t textLen = std::strlen(text);
                for (size_t i = 0; i < size; ++i) {
                    data[i] = std::byte{static_cast<uint8_t>(text[i % textLen])};
                }
            } break;
        }

        return data;
    }

    std::unique_ptr<ICompressor> compressor_;
};

TEST_F(ZstandardCompressorTest, BasicProperties) {
    EXPECT_EQ(compressor_->algorithm(), CompressionAlgorithm::Zstandard);

    auto levels = compressor_->supportedLevels();
    EXPECT_EQ(levels.first, 1);
    EXPECT_EQ(levels.second, 22);

    EXPECT_TRUE(compressor_->supportsStreaming());
    EXPECT_TRUE(compressor_->supportsDictionary());
}

TEST_F(ZstandardCompressorTest, CompressEmptyData) {
    std::vector<std::byte> empty;

    auto result = compressor_->compress(empty);
    ASSERT_TRUE(result.has_value());

    const auto& compressed = result.value();
    EXPECT_EQ(compressed.algorithm, CompressionAlgorithm::Zstandard);
    EXPECT_EQ(compressed.originalSize, 0);
    EXPECT_GT(compressed.compressedSize, 0); // Even empty data has header
    EXPECT_FALSE(compressed.data.empty());
}

TEST_F(ZstandardCompressorTest, CompressSmallData) {
    std::vector<std::byte> data = {
        std::byte{0x48}, std::byte{0x65}, std::byte{0x6C}, std::byte{0x6C}, std::byte{0x6F}
        // "Hello"
    };

    auto result = compressor_->compress(data);
    ASSERT_TRUE(result.has_value());

    const auto& compressed = result.value();
    EXPECT_EQ(compressed.originalSize, 5);
    EXPECT_GT(compressed.compressedSize, 0);

    // Decompress and verify
    auto decompressed = compressor_->decompress(compressed.data);
    ASSERT_TRUE(decompressed.has_value());
    EXPECT_EQ(decompressed.value(), data);
}

TEST_F(ZstandardCompressorTest, CompressLargeData) {
    const size_t size = 1024 * 1024;       // 1MB
    auto data = generateTestData(size, 3); // Text-like pattern

    auto result = compressor_->compress(data);
    ASSERT_TRUE(result.has_value());

    const auto& compressed = result.value();
    EXPECT_EQ(compressed.originalSize, size);
    EXPECT_LT(compressed.compressedSize, size); // Should compress well

    // Calculate compression ratio
    double ratio = static_cast<double>(compressed.originalSize) / compressed.compressedSize;
    EXPECT_GT(ratio, 2.0); // Text should compress at least 2:1

    // Decompress and verify
    auto decompressed = compressor_->decompress(compressed.data);
    ASSERT_TRUE(decompressed.has_value());
    EXPECT_EQ(decompressed.value(), data);
}

TEST_F(ZstandardCompressorTest, CompressionLevels) {
    auto data = generateTestData(10000, 3); // Text-like data

    size_t prevSize = SIZE_MAX;

    // Test levels 1, 3, 9, 22
    for (int level : {1, 3, 9, 22}) {
        auto result = compressor_->compress(data, static_cast<uint8_t>(level));
        ASSERT_TRUE(result.has_value());

        const auto& compressed = result.value();
        EXPECT_EQ(compressed.level, level);

        // Higher levels should generally produce smaller output
        if (level > 1) {
            EXPECT_LE(compressed.compressedSize, prevSize);
        }
        prevSize = compressed.compressedSize;

        // Verify decompression works
        auto decompressed = compressor_->decompress(compressed.data);
        ASSERT_TRUE(decompressed.has_value());
        EXPECT_EQ(decompressed.value(), data);
    }
}

TEST_F(ZstandardCompressorTest, IncompressibleData) {
    auto data = generateTestData(1000, 2); // Random data

    auto result = compressor_->compress(data);
    ASSERT_TRUE(result.has_value());

    const auto& compressed = result.value();
    // Random data shouldn't compress much (or might even expand)
    double ratio = static_cast<double>(compressed.originalSize) / compressed.compressedSize;
    EXPECT_LT(ratio, 1.5); // Shouldn't compress more than 1.5:1
}

TEST_F(ZstandardCompressorTest, HighlyCompressibleData) {
    auto data = generateTestData(10000, 0); // All zeros

    auto result = compressor_->compress(data);
    ASSERT_TRUE(result.has_value());

    const auto& compressed = result.value();
    // All zeros should compress extremely well
    double ratio = static_cast<double>(compressed.originalSize) / compressed.compressedSize;
    EXPECT_GT(ratio, 100.0); // Should compress more than 100:1
}

TEST_F(ZstandardCompressorTest, DecompressWithExpectedSize) {
    auto data = generateTestData(1000, 3);

    auto compressed = compressor_->compress(data);
    ASSERT_TRUE(compressed.has_value());

    // Decompress with correct expected size
    auto decompressed = compressor_->decompress(compressed.value().data, 1000);
    ASSERT_TRUE(decompressed.has_value());
    EXPECT_EQ(decompressed.value(), data);
}

TEST_F(ZstandardCompressorTest, DecompressCorruptedData) {
    auto data = generateTestData(100, 3);

    auto compressed = compressor_->compress(data);
    ASSERT_TRUE(compressed.has_value());

    // Corrupt the compressed data
    auto corruptedData = compressed.value().data;
    if (corruptedData.size() > 10) {
        corruptedData[10] = std::byte{0xFF};
        corruptedData[11] = std::byte{0xFF};
    }

    // Decompression should fail
    auto decompressed = compressor_->decompress(corruptedData);
    EXPECT_FALSE(decompressed.has_value());
    if (!decompressed.has_value()) {
        EXPECT_EQ(decompressed.error().code, ErrorCode::CompressionError);
    }
}

TEST_F(ZstandardCompressorTest, MaxCompressedSize) {
    const size_t inputSize = 1000;
    size_t maxSize = compressor_->maxCompressedSize(inputSize);

    // Zstandard bound should be reasonable
    EXPECT_GT(maxSize, inputSize);     // Must be larger than input
    EXPECT_LT(maxSize, inputSize * 2); // Shouldn't be more than 2x

    // Verify it's sufficient for actual compression
    auto data = generateTestData(inputSize, 2); // Random data (worst case)
    auto compressed = compressor_->compress(data);
    ASSERT_TRUE(compressed.has_value());
    EXPECT_LE(compressed.value().compressedSize, maxSize);
}

// TODO: Streaming compression not yet implemented
/*TEST_F(ZstandardCompressorTest, StreamingCompression) {
    auto streamCompressor = compressor_->createStreamCompressor(3);
    ASSERT_NE(streamCompressor, nullptr);

    // Prepare test data in chunks
    const size_t chunkSize = 1024;
    const size_t numChunks = 10;
    std::vector<std::vector<std::byte>> chunks;

    for (size_t i = 0; i < numChunks; ++i) {
        chunks.push_back(generateTestData(chunkSize, 3));
    }

    // Compress in streaming fashion
    std::vector<std::byte> compressed;
    std::vector<std::byte> outputBuffer(streamCompressor->recommendedOutputSize(chunkSize));

    for (size_t i = 0; i < numChunks; ++i) {
        bool isLast = (i == numChunks - 1);
        auto result = streamCompressor->compress(chunks[i], outputBuffer, isLast);
        ASSERT_TRUE(result.has_value());

        compressed.insert(compressed.end(),
                         outputBuffer.begin(),
                         outputBuffer.begin() + result.value().bytesProduced);
    }

    // Decompress the entire stream
    auto decompressed = compressor_->decompress(compressed);
    ASSERT_TRUE(decompressed.has_value());

    // Verify data matches
    std::vector<std::byte> originalData;
    for (const auto& chunk : chunks) {
        originalData.insert(originalData.end(), chunk.begin(), chunk.end());
    }
    EXPECT_EQ(decompressed.value(), originalData);
}

TEST_F(ZstandardCompressorTest, StreamingDecompression) {
    // First compress some data
    auto data = generateTestData(10000, 3);
    auto compressed = compressor_->compress(data);
    ASSERT_TRUE(compressed.has_value());

    // Now decompress it in streaming fashion
    auto streamDecompressor = compressor_->createStreamDecompressor();
    ASSERT_NE(streamDecompressor, nullptr);

    std::vector<std::byte> decompressed;
    std::vector<std::byte> outputBuffer(streamDecompressor->recommendedOutputSize(1024));

    // Feed compressed data in small chunks
    const size_t chunkSize = 256;
    size_t offset = 0;

    while (offset < compressed.value().data.size()) {
        size_t remaining = compressed.value().data.size() - offset;
        size_t currentChunk = std::min(chunkSize, remaining);

        std::span<const std::byte> input(
            compressed.value().data.data() + offset, currentChunk);

        auto result = streamDecompressor->decompress(input, outputBuffer);
        ASSERT_TRUE(result.has_value());

        decompressed.insert(decompressed.end(),
                           outputBuffer.begin(),
                           outputBuffer.begin() + result.value().bytesProduced);

        offset += result.value().bytesConsumed;
    }

    EXPECT_TRUE(streamDecompressor->isComplete());
    EXPECT_EQ(decompressed, data);
}*/

TEST_F(ZstandardCompressorTest, CompressionPerformance) {
    // Test compression throughput
    const size_t dataSize = 10 * 1024 * 1024;  // 10MB
    auto data = generateTestData(dataSize, 3); // Text-like data

    auto start = std::chrono::steady_clock::now();
    auto compressed = compressor_->compress(data, 3); // Default level
    auto end = std::chrono::steady_clock::now();

    ASSERT_TRUE(compressed.has_value());

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    double throughputMBps = (dataSize / (1024.0 * 1024.0)) / (duration.count() / 1000.0);

    // Zstandard should achieve > 100MB/s at level 3
    EXPECT_GT(throughputMBps, 100.0);
}

TEST_F(ZstandardCompressorTest, DecompressionPerformance) {
    // First compress some data
    const size_t dataSize = 10 * 1024 * 1024; // 10MB
    auto data = generateTestData(dataSize, 3);
    auto compressed = compressor_->compress(data, 3);
    ASSERT_TRUE(compressed.has_value());

    // Test decompression throughput
    auto start = std::chrono::steady_clock::now();
    auto decompressed = compressor_->decompress(compressed.value().data);
    auto end = std::chrono::steady_clock::now();

    ASSERT_TRUE(decompressed.has_value());

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    double throughputMBps = (dataSize / (1024.0 * 1024.0)) / (duration.count() / 1000.0);

    // Zstandard should achieve > 500MB/s decompression
    EXPECT_GT(throughputMBps, 500.0);
}