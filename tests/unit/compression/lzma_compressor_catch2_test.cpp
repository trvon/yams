// Catch2 tests for LZMA Compressor
// Migrated from GTest: lzma_compressor_test.cpp

#include <catch2/catch_test_macros.hpp>

#include <cstring>
#include <random>
#include <vector>
#include <yams/compression/compression_utils.h>
#include <yams/compression/compressor_interface.h>

using namespace yams;
using namespace yams::compression;

namespace {
struct LZMACompressorFixture {
    LZMACompressorFixture() {
        // Get compressor from registry; tests will be skipped if LZMA is not available
        auto& registry = CompressionRegistry::instance();
        compressor_ = registry.createCompressor(CompressionAlgorithm::LZMA);
    }

    void skipIfUnavailable() const {
        if (!compressor_) {
            SKIP("LZMA compressor is not available (dependency disabled).");
        }
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

            case 4: // XML-like structured data
            {
                const char* xml =
                    "<record><id>12345</id><name>Test User</name><value>42.0</value></record>\n";
                size_t xmlLen = std::strlen(xml);
                for (size_t i = 0; i < size; ++i) {
                    data[i] = std::byte{static_cast<uint8_t>(xml[i % xmlLen])};
                }
            } break;
        }

        return data;
    }

    std::unique_ptr<ICompressor> compressor_;
};
} // namespace

TEST_CASE_METHOD(LZMACompressorFixture, "LZMACompressor - BasicProperties",
                 "[compression][lzma][catch2]") {
    skipIfUnavailable();

    CHECK(compressor_->algorithm() == CompressionAlgorithm::LZMA);

    auto levels = compressor_->supportedLevels();
    CHECK(levels.first == 0);
    CHECK(levels.second == 9);

    CHECK(compressor_->supportsStreaming());
    CHECK(compressor_->supportsDictionary());
}

TEST_CASE_METHOD(LZMACompressorFixture, "LZMACompressor - CompressEmptyData",
                 "[compression][lzma][catch2]") {
    skipIfUnavailable();

    std::vector<std::byte> empty;

    auto result = compressor_->compress(empty);
    REQUIRE(result.has_value());

    const auto& compressed = result.value();
    CHECK(compressed.algorithm == CompressionAlgorithm::LZMA);
    CHECK(compressed.originalSize == 0);
    CHECK(compressed.compressedSize > 0); // Even empty data has header
    CHECK_FALSE(compressed.data.empty());
}

TEST_CASE_METHOD(LZMACompressorFixture, "LZMACompressor - CompressSmallData",
                 "[compression][lzma][catch2]") {
    skipIfUnavailable();

    std::vector<std::byte> data = {
        std::byte{0x48}, std::byte{0x65}, std::byte{0x6C}, std::byte{0x6C}, std::byte{0x6F}
        // "Hello"
    };

    auto result = compressor_->compress(data);
    REQUIRE(result.has_value());

    const auto& compressed = result.value();
    CHECK(compressed.originalSize == 5);
    CHECK(compressed.compressedSize > 0);

    // Decompress and verify
    auto decompressed = compressor_->decompress(compressed.data);
    REQUIRE(decompressed.has_value());
    CHECK(decompressed.value() == data);
}

TEST_CASE_METHOD(LZMACompressorFixture, "LZMACompressor - CompressLargeData",
                 "[compression][lzma][catch2]") {
    skipIfUnavailable();

    const size_t size = 1024 * 1024;       // 1MB
    auto data = generateTestData(size, 3); // Text-like pattern

    auto result = compressor_->compress(data);
    REQUIRE(result.has_value());

    const auto& compressed = result.value();
    CHECK(compressed.originalSize == size);
    CHECK(compressed.compressedSize < size); // Should compress well

    // Calculate compression ratio
    double ratio = static_cast<double>(compressed.originalSize) / compressed.compressedSize;
    CHECK(ratio > 3.0); // LZMA should compress text better than Zstandard

    // Decompress and verify
    auto decompressed = compressor_->decompress(compressed.data);
    REQUIRE(decompressed.has_value());
    CHECK(decompressed.value() == data);
}

TEST_CASE_METHOD(LZMACompressorFixture, "LZMACompressor - CompressionLevels",
                 "[compression][lzma][catch2]") {
    skipIfUnavailable();

    auto data = generateTestData(10000, 3); // Text-like data

    size_t prevSize = SIZE_MAX;

    // Test levels 0, 3, 6, 9
    for (int level : {0, 3, 6, 9}) {
        auto result = compressor_->compress(data, static_cast<uint8_t>(level));
        REQUIRE(result.has_value());

        const auto& compressed = result.value();
        INFO("Testing level " << level);
        CHECK(compressed.level == (level == 0 ? 5 : level)); // 0 means default (5)

        // Higher levels should generally produce smaller output
        if (level > 0) {
            CHECK(compressed.compressedSize <= prevSize);
        }
        prevSize = compressed.compressedSize;

        // Verify decompression works
        auto decompressed = compressor_->decompress(compressed.data);
        REQUIRE(decompressed.has_value());
        CHECK(decompressed.value() == data);
    }
}

TEST_CASE_METHOD(LZMACompressorFixture, "LZMACompressor - HighlyCompressibleData",
                 "[compression][lzma][catch2]") {
    skipIfUnavailable();

    // Test with all zeros - should compress extremely well
    auto data = generateTestData(10000, 0);

    auto result = compressor_->compress(data);
    REQUIRE(result.has_value());

    const auto& compressed = result.value();
    double ratio = static_cast<double>(compressed.originalSize) / compressed.compressedSize;
    CHECK(ratio > 200.0); // LZMA should achieve >200:1 on zeros
}

TEST_CASE_METHOD(LZMACompressorFixture, "LZMACompressor - StructuredDataCompression",
                 "[compression][lzma][catch2]") {
    skipIfUnavailable();

    // Test with XML-like data - LZMA excels at structured data
    auto data = generateTestData(50000, 4);

    auto result = compressor_->compress(data);
    REQUIRE(result.has_value());

    const auto& compressed = result.value();
    double ratio = static_cast<double>(compressed.originalSize) / compressed.compressedSize;
    CHECK(ratio > 5.0); // Should achieve >5:1 on structured data

    // Decompress and verify
    auto decompressed = compressor_->decompress(compressed.data);
    REQUIRE(decompressed.has_value());
    CHECK(decompressed.value() == data);
}

TEST_CASE_METHOD(LZMACompressorFixture, "LZMACompressor - IncompressibleData",
                 "[compression][lzma][catch2]") {
    skipIfUnavailable();

    auto data = generateTestData(1000, 2); // Random data

    auto result = compressor_->compress(data);
    REQUIRE(result.has_value());

    const auto& compressed = result.value();
    // Random data might even expand slightly
    double ratio = static_cast<double>(compressed.originalSize) / compressed.compressedSize;
    CHECK(ratio < 1.2); // Should not compress much
}

TEST_CASE_METHOD(LZMACompressorFixture, "LZMACompressor - DecompressWithExpectedSize",
                 "[compression][lzma][catch2]") {
    skipIfUnavailable();

    auto data = generateTestData(1000, 3);

    auto compressed = compressor_->compress(data);
    REQUIRE(compressed.has_value());

    // Decompress with correct expected size
    auto decompressed = compressor_->decompress(compressed.value().data, 1000);
    REQUIRE(decompressed.has_value());
    CHECK(decompressed.value() == data);
}

TEST_CASE_METHOD(LZMACompressorFixture, "LZMACompressor - DecompressCorruptedData",
                 "[compression][lzma][catch2]") {
    skipIfUnavailable();

    auto data = generateTestData(100, 3);

    auto compressed = compressor_->compress(data);
    REQUIRE(compressed.has_value());

    // Corrupt the compressed data
    auto corruptedData = compressed.value().data;
    if (corruptedData.size() > 10) {
        corruptedData[10] = std::byte{0xFF};
        corruptedData[11] = std::byte{0xFF};
    }

    // Decompression should fail
    auto decompressed = compressor_->decompress(corruptedData);
    REQUIRE_FALSE(decompressed.has_value());
    CHECK(decompressed.error().code == ErrorCode::CompressionError);
}

TEST_CASE_METHOD(LZMACompressorFixture, "LZMACompressor - MaxCompressedSize",
                 "[compression][lzma][catch2]") {
    skipIfUnavailable();

    const size_t inputSize = 1000;
    size_t maxSize = compressor_->maxCompressedSize(inputSize);

    // LZMA bound should be reasonable
    CHECK(maxSize > inputSize);     // Must be larger than input
    CHECK(maxSize < inputSize * 2); // Shouldn't be more than 2x

    // Verify it's sufficient for actual compression
    auto data = generateTestData(inputSize, 2); // Random data (worst case)
    auto compressed = compressor_->compress(data);
    REQUIRE(compressed.has_value());
    CHECK(compressed.value().compressedSize <= maxSize);
}

TEST_CASE_METHOD(LZMACompressorFixture, "LZMACompressor - CompareWithZstandard",
                 "[compression][lzma][catch2]") {
    skipIfUnavailable();

    // Compare compression ratios between LZMA and Zstandard
    auto& registry = CompressionRegistry::instance();
    auto zstd = registry.createCompressor(CompressionAlgorithm::Zstandard);
    REQUIRE(zstd != nullptr);

    // Test with structured data where LZMA should excel
    auto data = generateTestData(50000, 4); // XML-like data

    // LZMA compression (level 6)
    auto lzmaResult = compressor_->compress(data, 6);
    REQUIRE(lzmaResult.has_value());

    // Zstandard compression (level 9 for best ratio)
    auto zstdCompressed = zstd->compress(data, 9);
    REQUIRE(zstdCompressed.has_value());

    // LZMA should achieve better compression ratio
    double lzmaRatio = static_cast<double>(data.size()) / lzmaResult.value().compressedSize;
    double zstdRatio = static_cast<double>(data.size()) / zstdCompressed.value().compressedSize;

    CHECK(lzmaRatio > zstdRatio * 1.2); // LZMA should be >20% better

    // But Zstandard should be faster
    CHECK(zstdCompressed.value().duration.count() < lzmaResult.value().duration.count());
}

TEST_CASE_METHOD(LZMACompressorFixture, "LZMACompressor - LargeFileCompression",
                 "[compression][lzma][catch2][.]") {
    skipIfUnavailable();

    // Test with larger data to verify memory efficiency
    const size_t size = 10 * 1024 * 1024;  // 10MB
    auto data = generateTestData(size, 3); // Text-like data

    // Use moderate compression level for reasonable speed
    auto result = compressor_->compress(data, 3);
    REQUIRE(result.has_value());

    const auto& compressed = result.value();
    CHECK(compressed.originalSize == size);
    CHECK(compressed.compressedSize < size / 3); // Should achieve >3:1

    // Decompress and verify
    auto decompressed = compressor_->decompress(compressed.data);
    REQUIRE(decompressed.has_value());
    CHECK(decompressed.value().size() == size);

    // Spot check some data
    CHECK(decompressed.value()[0] == data[0]);
    CHECK(decompressed.value()[size / 2] == data[size / 2]);
    CHECK(decompressed.value()[size - 1] == data[size - 1]);
}
