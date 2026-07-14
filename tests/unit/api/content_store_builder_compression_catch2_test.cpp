#include <catch2/catch_test_macros.hpp>

#include <yams/api/content_store.h>
#include <yams/api/content_store_builder.h>

#include <cstddef>
#include <filesystem>
#include <random>
#include <system_error>
#include <vector>

namespace {

std::filesystem::path makeTempDir(const char* prefix) {
    auto dir = std::filesystem::temp_directory_path() /
               (std::string(prefix) + std::to_string(std::random_device{}()));
    std::filesystem::create_directories(dir);
    return dir;
}

} // namespace

TEST_CASE("ContentStoreBuilder default compression skips tiny objects",
          "[api][content-store][compression][hot-path]") {
    const auto tempDir = makeTempDir("yams_content_store_builder_compression_");
    auto cleanup = [&] {
        std::error_code ec;
        std::filesystem::remove_all(tempDir, ec);
    };

    auto storeResult = yams::api::ContentStoreBuilder::createDefault(tempDir);
    REQUIRE(storeResult.has_value());
    auto store = std::move(storeResult).value();

    std::vector<std::byte> tiny(800, std::byte{0x61});
    auto tinyStored = store->storeBytes(tiny);
    REQUIRE(tinyStored.has_value());
    auto tinyRaw = store->retrieveRaw(tinyStored.value().contentHash);
    REQUIRE(tinyRaw.has_value());
    CHECK((tinyRaw.value().data == tiny));
    CHECK_FALSE(tinyRaw.value().header.has_value());

    std::vector<std::byte> larger(4096, std::byte{0x62});
    auto largerStored = store->storeBytes(larger);
    REQUIRE(largerStored.has_value());
    auto largerRaw = store->retrieveRaw(largerStored.value().contentHash);
    REQUIRE(largerRaw.has_value());
    CHECK(largerRaw.value().header.has_value());

    store.reset();
    cleanup();
}
