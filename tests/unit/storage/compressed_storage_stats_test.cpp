#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <vector>
#include <gtest/gtest.h>

#include <yams/compression/compression_header.h>
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

TEST_F(CompressedStorageStatsTest, ThroughputAndOverallRatioAreNonNegative) {
    StorageConfig base_cfg{.basePath = temp_root, .enableCompression = false};
    auto base = std::make_shared<StorageEngine>(base_cfg);

    CompressedStorageEngine::Config cfg;
    cfg.enableCompression = true;
    cfg.asyncCompression = false;
    cfg.compressionThreshold = 32;

    CompressedStorageEngine engine(base, cfg);

    // Mixed workloads
    auto data1 = make_compressible(32 * 1024);
    auto hash1 = crypto::SHA256Hasher::hash(std::span<const std::byte>(data1.data(), data1.size()));
    ASSERT_TRUE(engine.store(hash1, std::span<const std::byte>(data1)).has_value());

    auto data2 = make_compressible(8 * 1024);
    auto hash2 = crypto::SHA256Hasher::hash(std::span<const std::byte>(data2.data(), data2.size()));
    ASSERT_TRUE(engine.store(hash2, std::span<const std::byte>(data2)).has_value());

    // Touch decompression path to record timings
    ASSERT_TRUE(engine.retrieve(hash1).has_value());
    ASSERT_TRUE(engine.retrieve(hash2).has_value());

    auto stats = engine.getCompressionStats();
    ASSERT_TRUE(stats.algorithmStats.contains(CompressionAlgorithm::Zstandard));
    const auto algo = stats.algorithmStats.at(CompressionAlgorithm::Zstandard);

    // Throughput metrics should be well-defined (may be zero on very fast runs)
    EXPECT_GE(algo.compressionThroughputMBps(), 0.0);
    EXPECT_GE(algo.decompressionThroughputMBps(), 0.0);

    // Overall ratio should be non-negative and typically > 1 for compressible data
    EXPECT_GE(stats.overallCompressionRatio(), 0.0);
}

TEST_F(CompressedStorageStatsTest, CRCMismatchOnRetrieveIncrementsErrorCounters) {
    // Ensure CRC checks are enabled
#if defined(_WIN32)
    _putenv_s("YAMS_SKIP_DECOMPRESS_CRC", "");
#else
    unsetenv("YAMS_SKIP_DECOMPRESS_CRC");
#endif

    StorageConfig base_cfg{.basePath = temp_root, .enableCompression = false};
    auto base = std::make_shared<StorageEngine>(base_cfg);

    CompressedStorageEngine::Config cfg;
    cfg.enableCompression = true;
    cfg.asyncCompression = false;
    cfg.compressionThreshold = 64;

    CompressedStorageEngine engine(base, cfg);

    auto data = make_compressible(16 * 1024);
    auto hash = crypto::SHA256Hasher::hash(std::span<const std::byte>(data.data(), data.size()));
    ASSERT_TRUE(engine.store(hash, std::span<const std::byte>(data)).has_value());

    // Read the stored object and corrupt a byte in the compressed payload
    const std::string h = std::string(hash);
    const std::string prefix = h.substr(0, base_cfg.shardDepth);
    const std::string suffix = h.substr(base_cfg.shardDepth);
    auto object_path = base->getBasePath() / "objects" / prefix / suffix;

    std::ifstream in(object_path, std::ios::binary);
    ASSERT_TRUE(in.good());
    // Read bytes as chars and convert to std::byte to satisfy libc++'s iterator constraints
    std::vector<char> charBuf((std::istreambuf_iterator<char>(in)),
                              std::istreambuf_iterator<char>());
    in.close();
    std::vector<std::byte> fileBytes(charBuf.size());
    std::transform(charBuf.begin(), charBuf.end(), fileBytes.begin(),
                   [](unsigned char c) { return static_cast<std::byte>(c); });
    ASSERT_GE(fileBytes.size(), compression::CompressionHeader::SIZE + 16);

    // Flip a byte within the compressed payload so compressed CRC will mismatch
    const size_t offset = compression::CompressionHeader::SIZE + 10;
    fileBytes[offset] = static_cast<std::byte>(~static_cast<unsigned char>(fileBytes[offset]));

    std::ofstream out(object_path, std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(out.good());
    out.write(reinterpret_cast<const char*>(fileBytes.data()),
              static_cast<std::streamsize>(fileBytes.size()));
    out.close();

    // Snapshot error-related counters
    auto before = engine.getCompressionStats();
    const uint64_t evictBefore = before.cacheEvictions.load();

    auto rt = engine.retrieve(hash);
    ASSERT_FALSE(rt.has_value());
    EXPECT_EQ(rt.error().code, ErrorCode::HashMismatch);

    auto after = engine.getCompressionStats();
    EXPECT_GT(after.cacheEvictions.load(), evictBefore);
}
