#include <algorithm>
#include <filesystem>
#include <vector>
#include <gtest/gtest.h>

#include <yams/compression/compressor_interface.h>
#include <yams/crypto/hasher.h>
#include <yams/storage/compressed_storage_engine.h>
#include <yams/storage/storage_engine.h>

using namespace yams;
using namespace yams::storage;
using namespace yams::compression;

namespace {
std::vector<std::byte> make_compressible(size_t n) {
    std::vector<std::byte> v(n);
    for (size_t i = 0; i < n; ++i) {
        v[i] = static_cast<std::byte>((i / 32) & 0xFF);
    }
    return v;
}
} // namespace

class CompressedStorageStatsTest : public ::testing::Test {
protected:
    void SetUp() override {
        temp_root = std::filesystem::temp_directory_path() / "yams_stats_test";
        std::filesystem::create_directories(temp_root);
    }

    std::filesystem::path temp_root;
};

TEST_F(CompressedStorageStatsTest, CompressionAndDecompressionUpdateStats) {
    StorageConfig base_cfg{.basePath = temp_root, .enableCompression = false};
    auto base = std::make_shared<StorageEngine>(base_cfg);

    CompressedStorageEngine::Config cfg;
    cfg.enableCompression = true;
    cfg.asyncCompression = false;  // deterministic
    cfg.compressionThreshold = 64; // small

    CompressedStorageEngine engine(base, cfg);

    auto data = make_compressible(16 * 1024);
    auto hash = crypto::SHA256Hasher::hash(std::span<const std::byte>(data.data(), data.size()));

    ASSERT_TRUE(engine.store(hash, std::span<const std::byte>(data)).has_value());

    // Stats after compression
    auto stats = engine.getCompressionStats();
    EXPECT_EQ(stats.totalCompressedFiles.load(), 1u);
    EXPECT_GT(stats.totalSpaceSaved.load(), 0u);
    ASSERT_TRUE(stats.algorithmStats.contains(CompressionAlgorithm::Zstandard));
    const auto algo = stats.algorithmStats.at(CompressionAlgorithm::Zstandard);
    EXPECT_EQ(algo.filesCompressed.load(), 1u);
    EXPECT_GT(algo.averageRatio(), 1.0);

    // Trigger decompression path
    auto rt = engine.retrieve(hash);
    ASSERT_TRUE(rt.has_value());
    const auto& out = rt.value();
    EXPECT_EQ(out.size(), data.size());
    EXPECT_TRUE(std::equal(out.begin(), out.end(), data.begin(), data.end()));

    // Stats after decompression
    stats = engine.getCompressionStats();
    const auto algo2 = stats.algorithmStats.at(CompressionAlgorithm::Zstandard);
    EXPECT_GE(algo2.filesDecompressed.load(), 1u);
}

TEST_F(CompressedStorageStatsTest, CompressionRatioThresholdsTracked) {
    StorageConfig base_cfg{.basePath = temp_root, .enableCompression = false};
    auto base = std::make_shared<StorageEngine>(base_cfg);

    CompressedStorageEngine::Config cfg;
    cfg.enableCompression = true;
    cfg.asyncCompression = false;
    cfg.compressionThreshold = 64;

    CompressedStorageEngine engine(base, cfg);

    // Highly compressible payload should yield ratio > 1
    auto data = make_compressible(64 * 1024);
    auto hash = crypto::SHA256Hasher::hash(std::span<const std::byte>(data.data(), data.size()));
    ASSERT_TRUE(engine.store(hash, std::span<const std::byte>(data)).has_value());

    auto stats = engine.getCompressionStats();
    ASSERT_TRUE(stats.algorithmStats.contains(CompressionAlgorithm::Zstandard));
    const auto algo = stats.algorithmStats.at(CompressionAlgorithm::Zstandard);
    EXPECT_GT(algo.averageRatio(), 1.0);
}
