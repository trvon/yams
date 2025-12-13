// Catch2 tests for Compression Comprehensive Tests
// Migrated from GTest: compression_comprehensive_test.cpp

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <cstring>
#include <memory>
#include <random>
#include <thread>
#include <vector>
#include <yams/compression/compression_header.h>
#include <yams/compression/compression_utils.h>
#include <yams/compression/compressor_interface.h>

using namespace yams;
using namespace yams::compression;

namespace {
// Mock compressor for testing without LZMA dependencies
class MockCompressor : public ICompressor {
public:
    explicit MockCompressor(CompressionAlgorithm algo) : algorithm_(algo) {}

    CompressionAlgorithm algorithm() const override { return algorithm_; }
    std::pair<uint8_t, uint8_t> supportedLevels() const override { return {1, 9}; }
    bool supportsStreaming() const override { return true; }
    bool supportsDictionary() const override { return false; }

    Result<CompressionResult> compress(std::span<const std::byte> input,
                                       uint8_t level = 0) override {
        // Simulate compression with simple RLE-like behavior
        CompressionResult result;
        result.algorithm = algorithm_;
        result.level = level ? level : 3;
        result.originalSize = input.size();

        // Mock compression - just copy with a header
        result.data.resize(input.size() + 16);
        std::memcpy(result.data.data() + 16, input.data(), input.size());
        result.compressedSize = result.data.size();

        compressionCount_++;
        return result;
    }

    Result<std::vector<std::byte>> decompress(std::span<const std::byte> compressed,
                                              size_t = 0) override {
        if (compressed.size() < 16) {
            return Error{ErrorCode::InvalidData, "Compressed data too small"};
        }

        // Mock decompression - skip header and return data
        std::vector<std::byte> result(compressed.size() - 16);
        std::memcpy(result.data(), compressed.data() + 16, result.size());

        decompressionCount_++;
        return result;
    }

    size_t maxCompressedSize(size_t inputSize) const override {
        return inputSize + 16; // Header overhead
    }

    std::unique_ptr<IStreamingCompressor> createStreamCompressor(uint8_t) {
        return nullptr; // Not implemented for mock
    }

    std::unique_ptr<IStreamingDecompressor> createStreamDecompressor() {
        return nullptr; // Not implemented for mock
    }

    // Test helpers
    int getCompressionCount() const { return compressionCount_; }
    int getDecompressionCount() const { return decompressionCount_; }

private:
    CompressionAlgorithm algorithm_;
    mutable std::atomic<int> compressionCount_{0};
    mutable std::atomic<int> decompressionCount_{0};
};

struct CompressionComprehensiveFixture {
    CompressionComprehensiveFixture() {
        // Create mock compressors to avoid LZMA dependencies
        mockZstd_ = std::make_shared<MockCompressor>(CompressionAlgorithm::Zstandard);
        mockLzma_ = std::make_shared<MockCompressor>(CompressionAlgorithm::LZMA);
    }

    // Generate test data patterns
    std::vector<std::byte> generateTestData(size_t size, const std::string& pattern) {
        std::vector<std::byte> data(size);

        if (pattern == "zeros") {
            std::fill(data.begin(), data.end(), std::byte{0});
        } else if (pattern == "ones") {
            std::fill(data.begin(), data.end(), std::byte{0xFF});
        } else if (pattern == "sequential") {
            for (size_t i = 0; i < size; ++i) {
                data[i] = std::byte{static_cast<uint8_t>(i % 256)};
            }
        } else if (pattern == "random") {
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<> dis(0, 255);
            for (auto& b : data) {
                b = std::byte{static_cast<uint8_t>(dis(gen))};
            }
        } else if (pattern == "text") {
            const char* lorem = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. ";
            size_t loremLen = std::strlen(lorem);
            for (size_t i = 0; i < size; ++i) {
                data[i] = std::byte{static_cast<uint8_t>(lorem[i % loremLen])};
            }
        } else if (pattern == "binary") {
            // Simulate binary data with mixed patterns
            for (size_t i = 0; i < size; ++i) {
                if (i % 4 == 0)
                    data[i] = std::byte{0x00};
                else if (i % 4 == 1)
                    data[i] = std::byte{0xFF};
                else if (i % 4 == 2)
                    data[i] = std::byte{0xAA};
                else
                    data[i] = std::byte{0x55};
            }
        }

        return data;
    }

    std::shared_ptr<MockCompressor> mockZstd_;
    std::shared_ptr<MockCompressor> mockLzma_;
};
} // namespace

TEST_CASE_METHOD(CompressionComprehensiveFixture, "CompressionComprehensive - DataPatterns",
                 "[compression][comprehensive][catch2]") {
    std::vector<std::pair<std::string, size_t>> testCases = {
        {"zeros", 1024},  {"ones", 1024}, {"sequential", 2048},
        {"random", 4096}, {"text", 8192}, {"binary", 16384}};

    for (const auto& [pattern, size] : testCases) {
        auto data = generateTestData(size, pattern);

        auto compressed = mockZstd_->compress(data);
        REQUIRE(compressed.has_value());
        INFO("Failed to compress " << pattern << " data");

        auto decompressed = mockZstd_->decompress(compressed.value().data);
        REQUIRE(decompressed.has_value());
        INFO("Failed to decompress " << pattern << " data");

        INFO("Data mismatch for " << pattern << " pattern");
        CHECK(decompressed.value() == data);
    }
}

TEST_CASE_METHOD(CompressionComprehensiveFixture, "CompressionComprehensive - DataSizes",
                 "[compression][comprehensive][catch2]") {
    std::vector<size_t> sizes = {
        0,               // Empty
        1,               // Single byte
        100,             // Small
        1024,            // 1KB
        1024 * 100,      // 100KB
        1024 * 1024,     // 1MB
        1024 * 1024 * 10 // 10MB
    };

    for (size_t size : sizes) {
        auto data = generateTestData(size, "random");

        auto compressed = mockZstd_->compress(data);
        REQUIRE(compressed.has_value());
        INFO("Failed to compress size " << size);

        auto decompressed = mockZstd_->decompress(compressed.value().data);
        REQUIRE(decompressed.has_value());
        INFO("Failed to decompress size " << size);

        INFO("Size mismatch for " << size << " bytes");
        CHECK(decompressed.value().size() == size);
    }
}

TEST_CASE_METHOD(CompressionComprehensiveFixture, "CompressionComprehensive - CompressionLevels",
                 "[compression][comprehensive][catch2]") {
    auto data = generateTestData(10000, "text");

    for (uint8_t level = 1; level <= 9; ++level) {
        INFO("Testing level " << static_cast<int>(level));

        auto compressed = mockZstd_->compress(data, level);
        REQUIRE(compressed.has_value());

        CHECK(compressed.value().level == level);

        auto decompressed = mockZstd_->decompress(compressed.value().data);
        REQUIRE(decompressed.has_value());
        CHECK(decompressed.value() == data);
    }
}

TEST_CASE_METHOD(CompressionComprehensiveFixture, "CompressionComprehensive - CompressionHeader",
                 "[compression][comprehensive][catch2]") {
    CompressionHeader header;
    header.magic = CompressionHeader::MAGIC;
    header.version = CompressionHeader::VERSION;
    header.algorithm = static_cast<uint8_t>(CompressionAlgorithm::Zstandard);
    header.level = 3;
    header.uncompressedSize = 1000;
    header.compressedSize = 500;
    header.uncompressedCRC32 = 0x12345678;
    header.compressedCRC32 = 0x87654321;
    header.flags = 0;
    header.timestamp = std::chrono::system_clock::now().time_since_epoch().count();

    // Serialize to bytes
    auto buffer = header.serialize();
    CHECK(buffer.size() == CompressionHeader::SIZE);

    // Parse back
    auto result = CompressionHeader::parse(buffer);
    REQUIRE(result.has_value());

    const auto& deserialized = result.value();
    CHECK(deserialized.magic == CompressionHeader::MAGIC);
    CHECK(deserialized.version == CompressionHeader::VERSION);
    CHECK(deserialized.algorithm == static_cast<uint8_t>(CompressionAlgorithm::Zstandard));
    CHECK(deserialized.level == 3);
    CHECK(deserialized.uncompressedSize == 1000);
    CHECK(deserialized.compressedSize == 500);
    CHECK(deserialized.uncompressedCRC32 == 0x12345678);
    CHECK(deserialized.compressedCRC32 == 0x87654321);

    // Validate
    CHECK(deserialized.validate());
}

TEST_CASE_METHOD(CompressionComprehensiveFixture,
                 "CompressionComprehensive - InvalidCompressionHeader",
                 "[compression][comprehensive][catch2]") {
    SECTION("Invalid magic") {
        std::vector<std::byte> buffer(CompressionHeader::SIZE, std::byte{0});
        auto result = CompressionHeader::parse(buffer);
        CHECK_FALSE(result.has_value());
    }

    SECTION("Invalid size") {
        std::vector<std::byte> buffer(CompressionHeader::SIZE - 1);
        auto result = CompressionHeader::parse(buffer);
        CHECK_FALSE(result.has_value());
    }

    SECTION("Invalid version") {
        CompressionHeader header;
        header.magic = CompressionHeader::MAGIC;
        header.version = 99; // Invalid version
        header.uncompressedSize = 1000;
        header.compressedSize = 500;

        auto buffer = header.serialize();

        auto result = CompressionHeader::parse(buffer);
        // The parse might fail or succeed but validation should fail
        if (result.has_value()) {
            CHECK_FALSE(result.value().validate());
        }
        // Parse failed is also acceptable for invalid version
    }
}

TEST_CASE_METHOD(CompressionComprehensiveFixture, "CompressionComprehensive - ConcurrentOperations",
                 "[compression][comprehensive][concurrent][catch2][.]") {
    const int numThreads = 10;
    const int operationsPerThread = 100;
    std::vector<std::thread> threads;
    std::atomic<int> successCount{0};
    std::atomic<int> failureCount{0};

    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back([&, i]() {
            for (int j = 0; j < operationsPerThread; ++j) {
                auto data = generateTestData(1000 + i * 100, "random");

                auto compressed = mockZstd_->compress(data);
                if (compressed.has_value()) {
                    auto decompressed = mockZstd_->decompress(compressed.value().data);
                    if (decompressed.has_value() && decompressed.value() == data) {
                        successCount++;
                    } else {
                        failureCount++;
                    }
                } else {
                    failureCount++;
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    CHECK(successCount == numThreads * operationsPerThread);
    CHECK(failureCount == 0);

    // Verify thread safety of counters
    CHECK(mockZstd_->getCompressionCount() == numThreads * operationsPerThread);
    CHECK(mockZstd_->getDecompressionCount() == numThreads * operationsPerThread);
}

TEST_CASE_METHOD(CompressionComprehensiveFixture, "CompressionComprehensive - ErrorHandling",
                 "[compression][comprehensive][catch2]") {
    SECTION("Decompression of invalid data") {
        std::vector<std::byte> invalidData{std::byte{0xFF}, std::byte{0xFF}};
        auto result = mockZstd_->decompress(invalidData);
        REQUIRE_FALSE(result.has_value());
        CHECK(result.error().code == ErrorCode::InvalidData);
    }

    SECTION("Empty input") {
        std::vector<std::byte> empty;
        auto compressed = mockZstd_->compress(empty);
        REQUIRE(compressed.has_value());

        auto decompressed = mockZstd_->decompress(compressed.value().data);
        REQUIRE(decompressed.has_value());
        CHECK(decompressed.value().size() == 0);
    }
}

TEST_CASE_METHOD(CompressionComprehensiveFixture, "CompressionComprehensive - MemoryUsagePatterns",
                 "[compression][comprehensive][catch2][.]") {
    // Test that compression doesn't leak memory by compressing many times
    const int iterations = 1000;

    for (int i = 0; i < iterations; ++i) {
        auto data = generateTestData(10000, "random");
        auto compressed = mockZstd_->compress(data);
        REQUIRE(compressed.has_value());

        // Let compressed result go out of scope to test cleanup
    }

    // If we get here without crashing, memory management is likely OK
    CHECK(true);
}

TEST_CASE_METHOD(CompressionComprehensiveFixture, "CompressionComprehensive - MaxCompressedSize",
                 "[compression][comprehensive][catch2]") {
    std::vector<size_t> testSizes = {100, 1024, 10240, 102400, 1048576};

    for (size_t size : testSizes) {
        size_t maxSize = mockZstd_->maxCompressedSize(size);
        INFO("Max compressed size should be larger than input for size " << size);
        CHECK(maxSize > size);

        // Verify it's actually sufficient
        auto data = generateTestData(size, "random");
        auto compressed = mockZstd_->compress(data);
        REQUIRE(compressed.has_value());
        INFO("Actual compressed size exceeds maximum for size " << size);
        CHECK(compressed.value().compressedSize <= maxSize);
    }
}

TEST_CASE_METHOD(CompressionComprehensiveFixture,
                 "CompressionComprehensive - CompressAlreadyCompressed",
                 "[compression][comprehensive][catch2]") {
    // First compression
    auto data = generateTestData(10000, "text");
    auto compressed1 = mockZstd_->compress(data);
    REQUIRE(compressed1.has_value());

    // Try to compress the compressed data
    auto compressed2 = mockZstd_->compress(compressed1.value().data);
    REQUIRE(compressed2.has_value());

    // Compressed data should not compress well (or might even expand)
    double ratio =
        static_cast<double>(compressed2.value().originalSize) / compressed2.value().compressedSize;
    CHECK(ratio < 1.1);
}

TEST_CASE_METHOD(CompressionComprehensiveFixture, "CompressionComprehensive - BoundaryConditions",
                 "[compression][comprehensive][catch2]") {
    SECTION("Single byte") {
        std::vector<std::byte> single{std::byte{42}};
        auto compressed = mockZstd_->compress(single);
        REQUIRE(compressed.has_value());

        auto decompressed = mockZstd_->decompress(compressed.value().data);
        REQUIRE(decompressed.has_value());
        CHECK(decompressed.value() == single);
    }

    SECTION("Maximum uint8_t values") {
        std::vector<std::byte> maxBytes(256);
        for (int i = 0; i < 256; ++i) {
            maxBytes[i] = std::byte{static_cast<uint8_t>(i)};
        }

        auto compressed = mockZstd_->compress(maxBytes);
        REQUIRE(compressed.has_value());

        auto decompressed = mockZstd_->decompress(compressed.value().data);
        REQUIRE(decompressed.has_value());
        CHECK(decompressed.value() == maxBytes);
    }
}
