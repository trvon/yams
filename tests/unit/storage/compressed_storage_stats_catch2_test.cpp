// Catch2 tests for compressed storage stats
// Migrated from GTest: compressed_storage_stats_test.cpp

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <vector>

#include <yams/compression/compression_header.h>
#include <yams/compression/compressor_interface.h>
#include <yams/crypto/hasher.h>
#include <yams/storage/compressed_storage_engine.h>
#include <yams/storage/storage_engine.h>

using namespace yams;
using namespace yams::storage;
using namespace yams::compression;

namespace {

std::vector<std::byte> makeCompressible(size_t n) {
    std::vector<std::byte> v(n);
    for (size_t i = 0; i < n; ++i) {
        v[i] = static_cast<std::byte>((i / 32) & 0xFF);
    }
    return v;
}

struct CompressedStorageStatsFixture {
    CompressedStorageStatsFixture() {
        tempRoot = std::filesystem::temp_directory_path() / "yams_stats_catch2_test";
        std::filesystem::create_directories(tempRoot);
    }

    ~CompressedStorageStatsFixture() {
        std::error_code ec;
        std::filesystem::remove_all(tempRoot, ec);
    }

    std::filesystem::path tempRoot;
};

} // namespace

TEST_CASE_METHOD(CompressedStorageStatsFixture,
                 "CompressedStorageStats compression and decompression update stats",
                 "[storage][compression][stats][catch2]") {
    StorageConfig baseCfg{.basePath = tempRoot, .enableCompression = false};
    auto base = std::make_shared<StorageEngine>(baseCfg);

    CompressedStorageEngine::Config cfg;
    cfg.enableCompression = true;
    cfg.asyncCompression = false;
    cfg.compressionThreshold = 64;

    CompressedStorageEngine engine(base, cfg);

    auto data = makeCompressible(16 * 1024);
    auto hash = crypto::SHA256Hasher::hash(std::span<const std::byte>(data.data(), data.size()));

    REQUIRE(engine.store(hash, std::span<const std::byte>(data)).has_value());

    // Stats after compression
    auto stats = engine.getCompressionStats();
    CHECK(stats.totalCompressedFiles.load() == 1u);
    CHECK(stats.totalSpaceSaved.load() > 0u);
    REQUIRE(stats.algorithmStats.contains(CompressionAlgorithm::Zstandard));

    const auto algo = stats.algorithmStats.at(CompressionAlgorithm::Zstandard);
    CHECK(algo.filesCompressed.load() == 1u);
    CHECK(algo.averageRatio() > 1.0);

    // Trigger decompression path
    auto rt = engine.retrieve(hash);
    REQUIRE(rt.has_value());
    const auto& out = rt.value();
    CHECK(out.size() == data.size());
    CHECK(std::equal(out.begin(), out.end(), data.begin(), data.end()));

    // Stats after decompression
    stats = engine.getCompressionStats();
    const auto algo2 = stats.algorithmStats.at(CompressionAlgorithm::Zstandard);
    CHECK(algo2.filesDecompressed.load() >= 1u);
}

TEST_CASE_METHOD(CompressedStorageStatsFixture,
                 "CompressedStorageStats compression ratio thresholds tracked",
                 "[storage][compression][stats][catch2]") {
    StorageConfig baseCfg{.basePath = tempRoot, .enableCompression = false};
    auto base = std::make_shared<StorageEngine>(baseCfg);

    CompressedStorageEngine::Config cfg;
    cfg.enableCompression = true;
    cfg.asyncCompression = false;
    cfg.compressionThreshold = 64;

    CompressedStorageEngine engine(base, cfg);

    // Highly compressible payload should yield ratio > 1
    auto data = makeCompressible(64 * 1024);
    auto hash = crypto::SHA256Hasher::hash(std::span<const std::byte>(data.data(), data.size()));
    REQUIRE(engine.store(hash, std::span<const std::byte>(data)).has_value());

    auto stats = engine.getCompressionStats();
    REQUIRE(stats.algorithmStats.contains(CompressionAlgorithm::Zstandard));
    const auto algo = stats.algorithmStats.at(CompressionAlgorithm::Zstandard);
    CHECK(algo.averageRatio() > 1.0);
}

TEST_CASE_METHOD(CompressedStorageStatsFixture,
                 "CompressedStorageStats throughput and overall ratio are non-negative",
                 "[storage][compression][stats][catch2]") {
    StorageConfig baseCfg{.basePath = tempRoot, .enableCompression = false};
    auto base = std::make_shared<StorageEngine>(baseCfg);

    CompressedStorageEngine::Config cfg;
    cfg.enableCompression = true;
    cfg.asyncCompression = false;
    cfg.compressionThreshold = 32;

    CompressedStorageEngine engine(base, cfg);

    // Mixed workloads
    auto data1 = makeCompressible(32 * 1024);
    auto hash1 = crypto::SHA256Hasher::hash(std::span<const std::byte>(data1.data(), data1.size()));
    REQUIRE(engine.store(hash1, std::span<const std::byte>(data1)).has_value());

    auto data2 = makeCompressible(8 * 1024);
    auto hash2 = crypto::SHA256Hasher::hash(std::span<const std::byte>(data2.data(), data2.size()));
    REQUIRE(engine.store(hash2, std::span<const std::byte>(data2)).has_value());

    // Touch decompression path to record timings
    REQUIRE(engine.retrieve(hash1).has_value());
    REQUIRE(engine.retrieve(hash2).has_value());

    auto stats = engine.getCompressionStats();
    REQUIRE(stats.algorithmStats.contains(CompressionAlgorithm::Zstandard));
    const auto algo = stats.algorithmStats.at(CompressionAlgorithm::Zstandard);

    CHECK(algo.compressionThroughputMBps() >= 0.0);
    CHECK(algo.decompressionThroughputMBps() >= 0.0);
    CHECK(stats.overallCompressionRatio() >= 0.0);
}

TEST_CASE_METHOD(CompressedStorageStatsFixture,
                 "CompressedStorageStats CRC mismatch on retrieve increments error counters",
                 "[storage][compression][stats][crc][catch2]") {
    // Ensure CRC checks are enabled
#if defined(_WIN32)
    _putenv_s("YAMS_SKIP_DECOMPRESS_CRC", "");
#else
    unsetenv("YAMS_SKIP_DECOMPRESS_CRC");
#endif

    StorageConfig baseCfg{.basePath = tempRoot, .enableCompression = false};
    auto base = std::make_shared<StorageEngine>(baseCfg);

    CompressedStorageEngine::Config cfg;
    cfg.enableCompression = true;
    cfg.asyncCompression = false;
    cfg.compressionThreshold = 64;

    CompressedStorageEngine engine(base, cfg);

    auto data = makeCompressible(16 * 1024);
    auto hash = crypto::SHA256Hasher::hash(std::span<const std::byte>(data.data(), data.size()));
    REQUIRE(engine.store(hash, std::span<const std::byte>(data)).has_value());

    // Read the stored object and corrupt a byte in the compressed payload
    const std::string h = std::string(hash);
    const std::string prefix = h.substr(0, baseCfg.shardDepth);
    const std::string suffix = h.substr(baseCfg.shardDepth);
    auto objectPath = base->getBasePath() / "objects" / prefix / suffix;

    std::ifstream in(objectPath, std::ios::binary);
    REQUIRE(in.good());
    std::vector<char> charBuf((std::istreambuf_iterator<char>(in)),
                              std::istreambuf_iterator<char>());
    in.close();
    std::vector<std::byte> fileBytes(charBuf.size());
    std::transform(charBuf.begin(), charBuf.end(), fileBytes.begin(),
                   [](unsigned char c) { return static_cast<std::byte>(c); });
    REQUIRE(fileBytes.size() >= compression::CompressionHeader::SIZE + 16);

    // Flip a byte within the compressed payload so compressed CRC will mismatch
    const size_t offset = compression::CompressionHeader::SIZE + 10;
    fileBytes[offset] = static_cast<std::byte>(~static_cast<unsigned char>(fileBytes[offset]));

    std::ofstream out(objectPath, std::ios::binary | std::ios::trunc);
    REQUIRE(out.good());
    out.write(reinterpret_cast<const char*>(fileBytes.data()),
              static_cast<std::streamsize>(fileBytes.size()));
    out.close();

    // Snapshot error-related counters
    auto before = engine.getCompressionStats();
    const uint64_t evictBefore = before.cacheEvictions.load();

    auto rt = engine.retrieve(hash);
    REQUIRE_FALSE(rt.has_value());
    CHECK(rt.error().code == ErrorCode::HashMismatch);

    auto after = engine.getCompressionStats();
    CHECK(after.cacheEvictions.load() > evictBefore);
}
