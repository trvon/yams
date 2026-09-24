// Catch2 tests for Zstandard Compressor
// Migrated from GTest: zstandard_compressor_test.cpp

#include <zstd.h>
#include <catch2/catch_test_macros.hpp>

#include <cstring>
#include <random>
#include <vector>
#include <yams/compression/compression_utils.h>
#include <yams/compression/compressor_interface.h>

using namespace yams;
using namespace yams::compression;

namespace {
struct ZstandardCompressorFixture {
    ZstandardCompressorFixture() {
        auto& registry = CompressionRegistry::instance();
        compressor_ = registry.createCompressor(CompressionAlgorithm::Zstandard);
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
} // namespace

TEST_CASE_METHOD(ZstandardCompressorFixture, "ZstandardCompressor - BasicProperties",
                 "[compression][zstd][catch2]") {
    REQUIRE(compressor_ != nullptr);
    CHECK(compressor_->algorithm() == CompressionAlgorithm::Zstandard);

    auto levels = compressor_->supportedLevels();
    CHECK(levels.first == 1);
    CHECK(levels.second == 22);

    CHECK(compressor_->supportsStreaming());
    CHECK(compressor_->supportsDictionary());
}

TEST_CASE_METHOD(ZstandardCompressorFixture, "ZstandardCompressor - CompressEmptyData",
                 "[compression][zstd][catch2]") {
    REQUIRE(compressor_ != nullptr);
    std::vector<std::byte> empty;

    auto result = compressor_->compress(empty);
    REQUIRE(result.has_value());

    const auto& compressed = result.value();
    // Empty data may return None (uncompressed) or Zstandard depending on implementation
    CHECK((compressed.algorithm == CompressionAlgorithm::Zstandard ||
           compressed.algorithm == CompressionAlgorithm::None));
    CHECK(compressed.originalSize == 0);
}

TEST_CASE_METHOD(ZstandardCompressorFixture, "ZstandardCompressor - CompressDecompressRoundTrip",
                 "[compression][zstd][catch2]") {
    REQUIRE(compressor_ != nullptr);

    SECTION("Highly compressible data") {
        auto testData = generateTestData(10000, 0);
        auto compressResult = compressor_->compress(testData);
        REQUIRE(compressResult.has_value());

        const auto& compressed = compressResult.value();
        CHECK(compressed.compressedSize < compressed.originalSize);

        auto decompressResult = compressor_->decompress(compressed.data, compressed.originalSize);
        REQUIRE(decompressResult.has_value());

        const auto& decompressed = decompressResult.value();
        REQUIRE(decompressed.size() == testData.size());
        CHECK(std::memcmp(decompressed.data(), testData.data(), testData.size()) == 0);
    }

    SECTION("Repeating pattern data") {
        auto testData = generateTestData(10000, 1);
        auto compressResult = compressor_->compress(testData);
        REQUIRE(compressResult.has_value());

        auto decompressResult = compressor_->decompress(compressResult.value().data,
                                                        compressResult.value().originalSize);
        REQUIRE(decompressResult.has_value());

        const auto& decompressed = decompressResult.value();
        CHECK(decompressed.size() == testData.size());
        CHECK(std::memcmp(decompressed.data(), testData.data(), testData.size()) == 0);
    }

    SECTION("Text-like data") {
        auto testData = generateTestData(10000, 3);
        auto compressResult = compressor_->compress(testData);
        REQUIRE(compressResult.has_value());

        auto decompressResult = compressor_->decompress(compressResult.value().data,
                                                        compressResult.value().originalSize);
        REQUIRE(decompressResult.has_value());

        const auto& decompressed = decompressResult.value();
        CHECK(decompressed.size() == testData.size());
        CHECK(std::memcmp(decompressed.data(), testData.data(), testData.size()) == 0);
    }
}

TEST_CASE_METHOD(ZstandardCompressorFixture, "ZstandardCompressor - CompressionLevels",
                 "[compression][zstd][catch2]") {
    REQUIRE(compressor_ != nullptr);
    auto testData = generateTestData(10000, 3);

    SECTION("Low compression level (fast)") {
        auto result = compressor_->compress(testData, 1);
        REQUIRE(result.has_value());
        CHECK(result.value().level == 1);
    }

    SECTION("Default compression level") {
        auto result = compressor_->compress(testData, 3);
        REQUIRE(result.has_value());
        CHECK(result.value().level == 3);
    }

    SECTION("High compression level (slow)") {
        auto result = compressor_->compress(testData, 19);
        REQUIRE(result.has_value());
        CHECK(result.value().level == 19);
    }
}

TEST_CASE_METHOD(ZstandardCompressorFixture, "ZstandardCompressor - MaxCompressedSize",
                 "[compression][zstd][catch2]") {
    REQUIRE(compressor_ != nullptr);

    size_t inputSize = 10000;
    size_t maxSize = compressor_->maxCompressedSize(inputSize);

    // Max compressed size should be larger than input to handle worst case
    CHECK(maxSize >= inputSize);
}

TEST_CASE_METHOD(ZstandardCompressorFixture, "ZstandardCompressor - VariousSizes",
                 "[compression][zstd][catch2]") {
    REQUIRE(compressor_ != nullptr);

    std::vector<size_t> sizes = {1, 10, 100, 1000, 10000, 100000};

    for (size_t size : sizes) {
        auto testData = generateTestData(size, 3);
        auto compressResult = compressor_->compress(testData);
        REQUIRE(compressResult.has_value());

        const auto& compressed = compressResult.value();
        INFO("Testing size: " << size);

        // If compression was ineffective, data is stored uncompressed
        if (compressed.algorithm == CompressionAlgorithm::None) {
            CHECK(compressed.data.size() == testData.size());
            CHECK(std::memcmp(compressed.data.data(), testData.data(), testData.size()) == 0);
        } else {
            auto decompressResult =
                compressor_->decompress(compressed.data, compressed.originalSize);
            REQUIRE(decompressResult.has_value());
            CHECK(decompressResult.value().size() == testData.size());
        }
    }
}

// The expected size comes from an unchecksummed storage header; it must agree with the
// content size the zstd frame records rather than drive a buffer allocation on its own.
TEST_CASE_METHOD(ZstandardCompressorFixture,
                 "ZstandardCompressor - Expected size must match the frame content size",
                 "[compression][zstd][catch2]") {
    std::vector<std::byte> data(64 * 1024, std::byte{'a'});
    auto compressed = compressor_->compress(data);
    REQUIRE(compressed.has_value());

    auto inflated = compressor_->decompress(compressed.value().data, data.size() + 1);
    CHECK_FALSE(inflated.has_value());

    auto exact = compressor_->decompress(compressed.value().data, data.size());
    REQUIRE(exact.has_value());
    CHECK(exact.value() == data);
}

// Frames from the streaming compressor do not record a content size; decoding them with a
// caller-provided size must grow with the real output and stop at that size.
TEST_CASE_METHOD(ZstandardCompressorFixture,
                 "ZstandardCompressor - Unknown-size frames are bounded by the expected size",
                 "[compression][zstd][catch2]") {
    std::vector<std::byte> data(256 * 1024, std::byte{'z'});
    std::vector<std::byte> frame(ZSTD_compressBound(data.size()));
    ZSTD_CCtx* cctx = ZSTD_createCCtx();
    REQUIRE(cctx != nullptr);
    ZSTD_inBuffer in{data.data(), data.size(), 0};
    ZSTD_outBuffer out{frame.data(), frame.size(), 0};
    // Feeding input before ending the frame leaves the content size unknown, as in the
    // chunked streaming compressor.
    REQUIRE_FALSE(ZSTD_isError(ZSTD_compressStream2(cctx, &out, &in, ZSTD_e_continue)));
    ZSTD_inBuffer none{nullptr, 0, 0};
    const size_t left = ZSTD_compressStream2(cctx, &out, &none, ZSTD_e_end);
    ZSTD_freeCCtx(cctx);
    REQUIRE(left == 0);
    frame.resize(out.pos);
    REQUIRE(ZSTD_getFrameContentSize(frame.data(), frame.size()) == ZSTD_CONTENTSIZE_UNKNOWN);

    auto exact = compressor_->decompress(frame, data.size());
    REQUIRE(exact.has_value());
    CHECK(exact.value() == data);

    auto tooSmall = compressor_->decompress(frame, data.size() - 1);
    CHECK_FALSE(tooSmall.has_value());
}
