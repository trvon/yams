#include <gtest/gtest.h>
#include <yams/compression/compressor_interface.h>
#include <yams/compression/compression_utils.h>
#include <random>
#include <vector>
#include <cstring>

using namespace yams;
using namespace yams::compression;

class LZMACompressorTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Get compressor from registry
        auto& registry = CompressionRegistry::instance();
        compressor_ = registry.createCompressor(CompressionAlgorithm::LZMA);
        ASSERT_NE(compressor_, nullptr);
    }
    
    // Generate test data patterns
    std::vector<std::byte> generateTestData(size_t size, int pattern) {
        std::vector<std::byte> data(size);
        
        switch (pattern) {
            case 0:  // All zeros (highly compressible)
                std::fill(data.begin(), data.end(), std::byte{0});
                break;
                
            case 1:  // Repeating pattern
                for (size_t i = 0; i < size; ++i) {
                    data[i] = std::byte{static_cast<uint8_t>(i % 256)};
                }
                break;
                
            case 2:  // Random data (incompressible)
                {
                    std::random_device rd;
                    std::mt19937 gen(rd());
                    std::uniform_int_distribution<> dis(0, 255);
                    for (auto& b : data) {
                        b = std::byte{static_cast<uint8_t>(dis(gen))};
                    }
                }
                break;
                
            case 3:  // Text-like data
                {
                    const char* text = "The quick brown fox jumps over the lazy dog. ";
                    size_t textLen = std::strlen(text);
                    for (size_t i = 0; i < size; ++i) {
                        data[i] = std::byte{static_cast<uint8_t>(text[i % textLen])};
                    }
                }
                break;
                
            case 4:  // XML-like structured data
                {
                    const char* xml = "<record><id>12345</id><name>Test User</name><value>42.0</value></record>\n";
                    size_t xmlLen = std::strlen(xml);
                    for (size_t i = 0; i < size; ++i) {
                        data[i] = std::byte{static_cast<uint8_t>(xml[i % xmlLen])};
                    }
                }
                break;
        }
        
        return data;
    }
    
    std::unique_ptr<ICompressor> compressor_;
};

TEST_F(LZMACompressorTest, BasicProperties) {
    EXPECT_EQ(compressor_->algorithm(), CompressionAlgorithm::LZMA);
    
    auto levels = compressor_->supportedLevels();
    EXPECT_EQ(levels.first, 0);
    EXPECT_EQ(levels.second, 9);
    
    EXPECT_TRUE(compressor_->supportsStreaming());
    EXPECT_TRUE(compressor_->supportsDictionary());
}

TEST_F(LZMACompressorTest, CompressEmptyData) {
    std::vector<std::byte> empty;
    
    auto result = compressor_->compress(empty);
    ASSERT_TRUE(result.has_value());
    
    const auto& compressed = result.value();
    EXPECT_EQ(compressed.algorithm, CompressionAlgorithm::LZMA);
    EXPECT_EQ(compressed.originalSize, 0);
    EXPECT_GT(compressed.compressedSize, 0);  // Even empty data has header
    EXPECT_FALSE(compressed.data.empty());
}

TEST_F(LZMACompressorTest, CompressSmallData) {
    std::vector<std::byte> data = {
        std::byte{0x48}, std::byte{0x65}, std::byte{0x6C}, std::byte{0x6C}, 
        std::byte{0x6F}  // "Hello"
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

TEST_F(LZMACompressorTest, CompressLargeData) {
    const size_t size = 1024 * 1024;  // 1MB
    auto data = generateTestData(size, 3);  // Text-like pattern
    
    auto result = compressor_->compress(data);
    ASSERT_TRUE(result.has_value());
    
    const auto& compressed = result.value();
    EXPECT_EQ(compressed.originalSize, size);
    EXPECT_LT(compressed.compressedSize, size);  // Should compress well
    
    // Calculate compression ratio
    double ratio = static_cast<double>(compressed.originalSize) / compressed.compressedSize;
    EXPECT_GT(ratio, 3.0);  // LZMA should compress text better than Zstandard
    
    // Decompress and verify
    auto decompressed = compressor_->decompress(compressed.data);
    ASSERT_TRUE(decompressed.has_value());
    EXPECT_EQ(decompressed.value(), data);
}

TEST_F(LZMACompressorTest, CompressionLevels) {
    auto data = generateTestData(10000, 3);  // Text-like data
    
    size_t prevSize = SIZE_MAX;
    
    // Test levels 0, 3, 6, 9
    for (int level : {0, 3, 6, 9}) {
        auto result = compressor_->compress(data, static_cast<uint8_t>(level));
        ASSERT_TRUE(result.has_value());
        
        const auto& compressed = result.value();
        EXPECT_EQ(compressed.level, level == 0 ? 5 : level);  // 0 means default (5)
        
        // Higher levels should generally produce smaller output
        if (level > 0) {
            EXPECT_LE(compressed.compressedSize, prevSize);
        }
        prevSize = compressed.compressedSize;
        
        // Verify decompression works
        auto decompressed = compressor_->decompress(compressed.data);
        ASSERT_TRUE(decompressed.has_value());
        EXPECT_EQ(decompressed.value(), data);
    }
}

TEST_F(LZMACompressorTest, HighlyCompressibleData) {
    // Test with all zeros - should compress extremely well
    auto data = generateTestData(10000, 0);
    
    auto result = compressor_->compress(data);
    ASSERT_TRUE(result.has_value());
    
    const auto& compressed = result.value();
    double ratio = static_cast<double>(compressed.originalSize) / compressed.compressedSize;
    EXPECT_GT(ratio, 200.0);  // LZMA should achieve >200:1 on zeros
}

TEST_F(LZMACompressorTest, StructuredDataCompression) {
    // Test with XML-like data - LZMA excels at structured data
    auto data = generateTestData(50000, 4);
    
    auto result = compressor_->compress(data);
    ASSERT_TRUE(result.has_value());
    
    const auto& compressed = result.value();
    double ratio = static_cast<double>(compressed.originalSize) / compressed.compressedSize;
    EXPECT_GT(ratio, 5.0);  // Should achieve >5:1 on structured data
    
    // Decompress and verify
    auto decompressed = compressor_->decompress(compressed.data);
    ASSERT_TRUE(decompressed.has_value());
    EXPECT_EQ(decompressed.value(), data);
}

TEST_F(LZMACompressorTest, IncompressibleData) {
    auto data = generateTestData(1000, 2);  // Random data
    
    auto result = compressor_->compress(data);
    ASSERT_TRUE(result.has_value());
    
    const auto& compressed = result.value();
    // Random data might even expand slightly
    double ratio = static_cast<double>(compressed.originalSize) / compressed.compressedSize;
    EXPECT_LT(ratio, 1.2);  // Should not compress much
}

TEST_F(LZMACompressorTest, DecompressWithExpectedSize) {
    auto data = generateTestData(1000, 3);
    
    auto compressed = compressor_->compress(data);
    ASSERT_TRUE(compressed.has_value());
    
    // Decompress with correct expected size
    auto decompressed = compressor_->decompress(
        compressed.value().data, 1000);
    ASSERT_TRUE(decompressed.has_value());
    EXPECT_EQ(decompressed.value(), data);
}

TEST_F(LZMACompressorTest, DecompressCorruptedData) {
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

TEST_F(LZMACompressorTest, MaxCompressedSize) {
    const size_t inputSize = 1000;
    size_t maxSize = compressor_->maxCompressedSize(inputSize);
    
    // LZMA bound should be reasonable
    EXPECT_GT(maxSize, inputSize);  // Must be larger than input
    EXPECT_LT(maxSize, inputSize * 2);  // Shouldn't be more than 2x
    
    // Verify it's sufficient for actual compression
    auto data = generateTestData(inputSize, 2);  // Random data (worst case)
    auto compressed = compressor_->compress(data);
    ASSERT_TRUE(compressed.has_value());
    EXPECT_LE(compressed.value().compressedSize, maxSize);
}

TEST_F(LZMACompressorTest, CompareWithZstandard) {
    // Compare compression ratios between LZMA and Zstandard
    auto& registry = CompressionRegistry::instance();
    auto zstd = registry.createCompressor(CompressionAlgorithm::Zstandard);
    ASSERT_NE(zstd, nullptr);
    
    // Test with structured data where LZMA should excel
    auto data = generateTestData(50000, 4);  // XML-like data
    
    // LZMA compression (level 6)
    auto lzmaResult = compressor_->compress(data, 6);
    ASSERT_TRUE(lzmaResult.has_value());
    
    // Zstandard compression (level 9 for best ratio)
    auto zstdCompressed = zstd->compress(data, 9);
    ASSERT_TRUE(zstdCompressed.has_value());
    
    // LZMA should achieve better compression ratio
    double lzmaRatio = static_cast<double>(data.size()) / lzmaResult.value().compressedSize;
    double zstdRatio = static_cast<double>(data.size()) / zstdCompressed.value().compressedSize;
    
    EXPECT_GT(lzmaRatio, zstdRatio * 1.2);  // LZMA should be >20% better
    
    // But Zstandard should be faster
    EXPECT_LT(zstdCompressed.value().duration.count(), 
              lzmaResult.value().duration.count());
}

TEST_F(LZMACompressorTest, LargeFileCompression) {
    // Test with larger data to verify memory efficiency
    const size_t size = 10 * 1024 * 1024;  // 10MB
    auto data = generateTestData(size, 3);  // Text-like data
    
    // Use moderate compression level for reasonable speed
    auto result = compressor_->compress(data, 3);
    ASSERT_TRUE(result.has_value());
    
    const auto& compressed = result.value();
    EXPECT_EQ(compressed.originalSize, size);
    EXPECT_LT(compressed.compressedSize, size / 3);  // Should achieve >3:1
    
    // Decompress and verify
    auto decompressed = compressor_->decompress(compressed.data);
    ASSERT_TRUE(decompressed.has_value());
    EXPECT_EQ(decompressed.value().size(), size);
    
    // Spot check some data
    EXPECT_EQ(decompressed.value()[0], data[0]);
    EXPECT_EQ(decompressed.value()[size/2], data[size/2]);
    EXPECT_EQ(decompressed.value()[size-1], data[size-1]);
}