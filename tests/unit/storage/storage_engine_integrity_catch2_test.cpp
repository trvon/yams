// SPDX-License-Identifier: GPL-3.0-or-later
// StorageEngine::verify() on compressed objects: each integrity check on the stored header and
// payload must flag a damaged object instead of accepting it.

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <format>
#include <fstream>
#include <functional>
#include <span>
#include <string>
#include <vector>

#include <yams/compression/compression_header.h>
#include <yams/compression/compression_utils.h>
#include <yams/compression/compressor_interface.h>
#include <yams/crypto/hasher.h>
#include <yams/storage/storage_engine.h>

using namespace yams;
using namespace yams::storage;
using yams::compression::CompressionAlgorithm;
using yams::compression::CompressionHeader;

namespace {

struct CompressedObjectFixture {
    std::filesystem::path root;
    std::unique_ptr<StorageEngine> engine;

    CompressedObjectFixture() {
        root = std::filesystem::temp_directory_path() /
               std::format("yams_storage_integrity_{}",
                           std::chrono::steady_clock::now().time_since_epoch().count());
        StorageConfig config{.basePath = root, .shardDepth = 2, .mutexPoolSize = 16};
        engine = std::make_unique<StorageEngine>(std::move(config));
    }
    ~CompressedObjectFixture() {
        engine.reset();
        std::error_code ec;
        std::filesystem::remove_all(root, ec);
    }
    CompressedObjectFixture(const CompressedObjectFixture&) = delete;
    CompressedObjectFixture& operator=(const CompressedObjectFixture&) = delete;

    // Writes `content` as a zstd-compressed object under its content hash. `damage` may edit the
    // header and payload before they are written.
    std::string writeCompressed(
        const std::vector<std::byte>& content,
        const std::function<void(CompressionHeader&, std::vector<std::byte>&)>& damage = {}) {
        auto compressor = compression::CompressionRegistry::instance().createCompressor(
            CompressionAlgorithm::Zstandard);
        REQUIRE(compressor);
        auto compressed = compressor->compress(content, 3);
        REQUIRE(compressed.has_value());
        auto payload = compressed.value().data;

        CompressionHeader header{};
        header.magic = CompressionHeader::MAGIC;
        header.version = CompressionHeader::VERSION;
        header.algorithm = static_cast<uint8_t>(CompressionAlgorithm::Zstandard);
        header.level = 3;
        header.uncompressedSize = content.size();
        header.compressedSize = payload.size();
        header.uncompressedCRC32 = compression::calculateCRC32(content);
        header.compressedCRC32 = compression::calculateCRC32(payload);
        if (damage) {
            damage(header, payload);
        }

        const auto hash = crypto::SHA256Hasher::hash(std::span<const std::byte>(content));
        const auto path = root / "objects" / hash.substr(0, 2) / hash.substr(2);
        std::filesystem::create_directories(path.parent_path());
        std::ofstream out(path, std::ios::binary);
        auto headerBytes = header.serialize();
        out.write(reinterpret_cast<const char*>(headerBytes.data()),
                  static_cast<std::streamsize>(headerBytes.size()));
        out.write(reinterpret_cast<const char*>(payload.data()),
                  static_cast<std::streamsize>(payload.size()));
        REQUIRE(out.good());
        return hash;
    }
};

std::vector<std::byte> sampleContent() {
    std::vector<std::byte> content(4096);
    for (size_t i = 0; i < content.size(); ++i) {
        content[i] = static_cast<std::byte>((i * 31) % 251);
    }
    return content;
}

void requireCorrupted(const StorageEngine& engine) {
    auto result = engine.verify();
    REQUIRE_FALSE(result.has_value());
    CHECK((result.error().code == ErrorCode::CorruptedData));
}

} // namespace

TEST_CASE("StorageEngine verify accepts an intact compressed object",
          "[storage][integrity][compression][catch2]") {
    CompressedObjectFixture f;
    f.writeCompressed(sampleContent());
    CHECK(f.engine->verify().has_value());
}

TEST_CASE("StorageEngine verify rejects a compressed payload that fails its CRC",
          "[storage][integrity][compression][catch2]") {
    CompressedObjectFixture f;
    f.writeCompressed(sampleContent(), [](CompressionHeader&, std::vector<std::byte>& payload) {
        payload[payload.size() / 2] ^= std::byte{0xff};
    });
    requireCorrupted(*f.engine);
}

TEST_CASE("StorageEngine verify rejects a header whose uncompressed size is wrong",
          "[storage][integrity][compression][catch2]") {
    // zstd checks the size against its frame header first; either check must reject it.
    CompressedObjectFixture f;
    f.writeCompressed(sampleContent(), [](CompressionHeader& header, std::vector<std::byte>&) {
        header.uncompressedSize -= 1;
    });
    requireCorrupted(*f.engine);
}

TEST_CASE("StorageEngine verify rejects decompressed bytes that fail their CRC",
          "[storage][integrity][compression][catch2]") {
    CompressedObjectFixture f;
    f.writeCompressed(sampleContent(), [](CompressionHeader& header, std::vector<std::byte>&) {
        header.uncompressedCRC32 ^= 0x1u;
    });
    requireCorrupted(*f.engine);
}

TEST_CASE("StorageEngine verify rejects a compressed object it cannot decompress",
          "[storage][integrity][compression][catch2]") {
    CompressedObjectFixture f;
    // LZMA has no registered compressor in this build.
    REQUIRE_FALSE(
        compression::CompressionRegistry::instance().createCompressor(CompressionAlgorithm::LZMA));
    f.writeCompressed(sampleContent(), [](CompressionHeader& header, std::vector<std::byte>&) {
        header.algorithm = static_cast<uint8_t>(CompressionAlgorithm::LZMA);
    });
    requireCorrupted(*f.engine);
}

TEST_CASE("StorageEngine verify rejects a compressed object stored under another hash",
          "[storage][integrity][compression][catch2]") {
    CompressedObjectFixture f;
    const auto hash = f.writeCompressed(sampleContent());
    // Same bytes under a different key: decodes cleanly but hashes to the original key.
    auto other = sampleContent();
    other[0] ^= std::byte{0x01};
    const auto otherHash = crypto::SHA256Hasher::hash(std::span<const std::byte>(other));
    const auto from = f.root / "objects" / hash.substr(0, 2) / hash.substr(2);
    const auto to = f.root / "objects" / otherHash.substr(0, 2) / otherHash.substr(2);
    std::filesystem::create_directories(to.parent_path());
    std::filesystem::rename(from, to);
    requireCorrupted(*f.engine);
}
