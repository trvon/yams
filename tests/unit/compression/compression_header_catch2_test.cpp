// Catch2 tests for CompressionHeader serialization and validation.

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <vector>
#include <yams/compression/compression_header.h>
#include <yams/compression/compressor_interface.h>

using namespace yams;
using namespace yams::compression;

TEST_CASE("CompressionHeader - serialize/parse round trip", "[compression][header][catch2]") {
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

TEST_CASE("CompressionHeader - invalid headers are rejected", "[compression][header][catch2]") {
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
