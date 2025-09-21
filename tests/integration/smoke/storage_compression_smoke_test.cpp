#include <filesystem>
#include <random>
#include <vector>
#include <gtest/gtest.h>

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
TEST(StorageCompressionSmoke, CompressionEnabledStoresCompressed) {
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
    ASSERT_TRUE(comp->store(key, std::span<const std::byte>(payload)).has_value());

    // Raw bytes in underlying storage should begin with compression header magic
    auto raw = base->retrieve(key);
    ASSERT_TRUE(raw.has_value());
    ASSERT_GE(raw.value().size(), CompressionHeader::SIZE);
    CompressionHeader hdr{};
    std::memcpy(&hdr, raw.value().data(), sizeof(hdr));
    EXPECT_EQ(hdr.magic, CompressionHeader::MAGIC);
    EXPECT_EQ(hdr.uncompressedSize, payload.size());

    // Retrieval via wrapper must transparently decompress
    auto roundtrip = comp->retrieve(key);
    ASSERT_TRUE(roundtrip.has_value());
    EXPECT_EQ(roundtrip.value().size(), payload.size());
    EXPECT_TRUE(std::equal(roundtrip.value().begin(), roundtrip.value().end(), payload.begin(),
                           payload.end()));
}

// Verifies that when compression is disabled, data is stored verbatim.
TEST(StorageCompressionSmoke, CompressionDisabledStoresPlain) {
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

    ASSERT_TRUE(comp->store(key, std::span<const std::byte>(payload)).has_value());

    // Raw bytes should NOT have compression header
    auto raw = base->retrieve(key);
    ASSERT_TRUE(raw.has_value());
    ASSERT_GE(raw.value().size(), 1u);
    if (raw.value().size() >= CompressionHeader::SIZE) {
        CompressionHeader hdr{};
        std::memcpy(&hdr, raw.value().data(), sizeof(hdr));
        EXPECT_NE(hdr.magic, CompressionHeader::MAGIC);
    }
    // And retrieving via wrapper should match original
    auto roundtrip = comp->retrieve(key);
    ASSERT_TRUE(roundtrip.has_value());
    EXPECT_EQ(roundtrip.value().size(), payload.size());
    EXPECT_TRUE(std::equal(roundtrip.value().begin(), roundtrip.value().end(), payload.begin(),
                           payload.end()));
}
