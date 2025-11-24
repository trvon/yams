#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <vector>
#include <gtest/gtest.h>
#include <yams/api/content_store_builder.h>
#include <yams/compat/unistd.h>
#include <yams/compression/compression_header.h>
#include <yams/crypto/hasher.h>
#include <yams/storage/storage_engine.h>


using namespace yams;
using namespace yams::api;
using namespace yams::storage;
using namespace yams::compression;

namespace {
std::vector<std::byte> make_compressible(size_t size) {
    std::vector<std::byte> data(size);
    for (size_t i = 0; i < size; ++i) {
        data[i] = static_cast<std::byte>((i / 16) & 0xFF);
    }
    return data;
}

std::string write_config(const std::filesystem::path& root, const std::string& body) {
    auto dir = root / "yams";
    std::filesystem::create_directories(dir);
    auto path = dir / "config.toml";
    std::ofstream f(path);
    f << body;
    f.close();
    return path.string();
}
} // namespace

// When never_compress_below is large, small payloads should be stored uncompressed.
TEST(StorageCompressionPolicyBuilderSmoke, ThresholdSkipsCompression) {
    auto temp_root = std::filesystem::temp_directory_path() / "yams_builder_policy_skip";
    std::filesystem::create_directories(temp_root);

    auto xdg = temp_root / "cfg";
    std::filesystem::create_directories(xdg);
    std::string cfg = R"TOML(
[compression]
never_compress_below = 1048576  # 1 MiB
async_compression = false
)TOML";
    write_config(xdg, cfg);
    setenv("XDG_CONFIG_HOME", xdg.string().c_str(), 1);

    ContentStoreBuilder b;
    b.withStoragePath(temp_root);
    auto storeRes = b.build();
    ASSERT_TRUE(storeRes.has_value());
    std::unique_ptr<IContentStore> store = std::move(storeRes.value());

    auto payload = make_compressible(4096);
    auto hash =
        crypto::SHA256Hasher::hash(std::span<const std::byte>(payload.data(), payload.size()));

    auto put = store->storeBytes(std::span<const std::byte>(payload.data(), payload.size()));
    ASSERT_TRUE(put.has_value());
    EXPECT_EQ(put.value().contentHash, hash);

    // Inspect raw object via plain storage engine
    StorageConfig base_cfg{.basePath = temp_root, .enableCompression = false};
    StorageEngine base(base_cfg);
    auto raw = base.retrieve(hash);
    ASSERT_TRUE(raw.has_value());
    if (raw.value().size() >= CompressionHeader::SIZE) {
        CompressionHeader hdr{};
        std::memcpy(&hdr, raw.value().data(), sizeof(hdr));
        EXPECT_NE(hdr.magic, CompressionHeader::MAGIC);
    }
}

// When never_compress_below is tiny, payloads should be stored compressed.
TEST(StorageCompressionPolicyBuilderSmoke, LowThresholdEnablesCompression) {
    auto temp_root = std::filesystem::temp_directory_path() / "yams_builder_policy_on";
    std::filesystem::create_directories(temp_root);

    auto xdg = temp_root / "cfg";
    std::filesystem::create_directories(xdg);
    std::string cfg = R"TOML(
[compression]
never_compress_below = 1
async_compression = false
)TOML";
    write_config(xdg, cfg);
    setenv("XDG_CONFIG_HOME", xdg.string().c_str(), 1);

    ContentStoreBuilder b;
    b.withStoragePath(temp_root);
    auto storeRes = b.build();
    ASSERT_TRUE(storeRes.has_value());
    std::unique_ptr<IContentStore> store = std::move(storeRes.value());

    auto payload = make_compressible(4096);
    auto hash =
        crypto::SHA256Hasher::hash(std::span<const std::byte>(payload.data(), payload.size()));

    auto put = store->storeBytes(std::span<const std::byte>(payload.data(), payload.size()));
    ASSERT_TRUE(put.has_value());
    EXPECT_EQ(put.value().contentHash, hash);

    StorageConfig base_cfg{.basePath = temp_root, .enableCompression = false};
    StorageEngine base(base_cfg);
    auto raw = base.retrieve(hash);
    ASSERT_TRUE(raw.has_value());
    ASSERT_GE(raw.value().size(), CompressionHeader::SIZE);
    CompressionHeader hdr{};
    std::memcpy(&hdr, raw.value().data(), sizeof(hdr));
    EXPECT_EQ(hdr.magic, CompressionHeader::MAGIC);
}
