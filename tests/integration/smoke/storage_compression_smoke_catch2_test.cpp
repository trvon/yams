#include <cstddef>
#include <filesystem>
#include <random>
#include <vector>
#include <catch2/catch_test_macros.hpp>

#include <yams/compression/compression_header.h>
#include <yams/crypto/hasher.h>
#include <yams/storage/compressed_storage_engine.h>
#include <yams/storage/storage_engine.h>

using namespace yams;
using namespace yams::storage;
using namespace yams::compression;

namespace {
std::vector<std::byte> make_compressible(size_t size) {
    std::vector<std::byte> data(size);
    // Fill with repeating pattern to ensure good compression
    for (size_t i = 0; i < size; ++i) {
        data[i] = static_cast<std::byte>((i / 32) & 0xFF);
    }
    return data;
}

std::string hash_of(std::span<const std::byte> data) {
    return crypto::SHA256Hasher::hash(data);
}
} // namespace

// Verifies that when compression is enabled in the storage wrapper, data is
// stored in compressed form (with YAMS compression header) and transparently
// decompressed on retrieval.
TEST_CASE("StorageCompressionSmoke.CompressionEnabledStoresCompressed", "[smoke][storagecompressionsmoke]") {
    // Temp directory for on-disk object storage
    auto temp_root = std::filesystem::temp_directory_path() / "yams_compression_smoke";
    std::filesystem::create_directories(temp_root);

    StorageConfig base_cfg{.basePath = temp_root, .enableCompression = false};
    auto base = std::make_shared<StorageEngine>(base_cfg);

    CompressedStorageEngine::Config ccfg;
    ccfg.enableCompression = true;
    ccfg.asyncCompression = false;  // deterministic for test
    ccfg.compressionThreshold = 64; // compress small payloads
    auto comp = std::make_unique<CompressedStorageEngine>(base, ccfg);

    auto payload = make_compressible(4096);
    auto key = hash_of(payload);

    // Store via compressed engine
    REQUIRE(comp->store(key, std::span<const std::byte>(payload)).has_value());

    // Raw bytes in underlying storage should begin with compression header magic
    auto raw = base->retrieve(key);
    REQUIRE(raw.has_value());
    REQUIRE(raw.value().size() >= CompressionHeader::SIZE);
    uint32_t magic = 0;
    uint64_t uncompressedSize = 0;
    std::memcpy(&magic, raw.value().data() + offsetof(CompressionHeader, magic), sizeof(magic));
    std::memcpy(&uncompressedSize,
                raw.value().data() + offsetof(CompressionHeader, uncompressedSize),
                sizeof(uncompressedSize));
    CHECK(magic == CompressionHeader::MAGIC);
    CHECK(uncompressedSize == payload.size());

    // Retrieval via wrapper must transparently decompress
    auto roundtrip = comp->retrieve(key);
    REQUIRE(roundtrip.has_value());
    CHECK(roundtrip.value().size() == payload.size());
    CHECK(std::equal(roundtrip.value().begin(), roundtrip.value().end(), payload.begin(),
                           payload.end()));
}

// Verifies that when compression is disabled, data is stored verbatim.
TEST_CASE("StorageCompressionSmoke.CompressionDisabledStoresPlain", "[smoke][storagecompressionsmoke]") {
    auto temp_root = std::filesystem::temp_directory_path() / "yams_compression_smoke_off";
    std::filesystem::create_directories(temp_root);

    StorageConfig base_cfg{.basePath = temp_root, .enableCompression = false};
    auto base = std::make_shared<StorageEngine>(base_cfg);

    CompressedStorageEngine::Config ccfg;
    ccfg.enableCompression = false; // disabled
    ccfg.asyncCompression = false;
    ccfg.compressionThreshold = 64;
    auto comp = std::make_unique<CompressedStorageEngine>(base, ccfg);

    auto payload = make_compressible(4096);
    auto key = hash_of(payload);

    REQUIRE(comp->store(key, std::span<const std::byte>(payload)).has_value());

    // Raw bytes should NOT have compression header
    auto raw = base->retrieve(key);
    REQUIRE(raw.has_value());
    REQUIRE(raw.value().size() >= 1u);
    if (raw.value().size() >= CompressionHeader::SIZE) {
        CompressionHeader hdr{};
        std::memcpy(&hdr, raw.value().data(), sizeof(hdr));
        CHECK(hdr.magic != CompressionHeader::MAGIC);
    }
    // And retrieving via wrapper should match original
    auto roundtrip = comp->retrieve(key);
    REQUIRE(roundtrip.has_value());
    CHECK(roundtrip.value().size() == payload.size());
    CHECK(std::equal(roundtrip.value().begin(), roundtrip.value().end(), payload.begin(),
                           payload.end()));
}
