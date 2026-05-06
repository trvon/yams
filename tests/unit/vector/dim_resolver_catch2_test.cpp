#include <catch2/catch_test_macros.hpp>

#include <yams/vector/dim_resolver.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>

namespace {

std::filesystem::path makeTempDir() {
    const auto base = std::filesystem::temp_directory_path();
    const auto stamp = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    auto dir = base / ("yams_dim_resolver_" + stamp);
    std::filesystem::create_directories(dir);
    return dir;
}

} // namespace

TEST_CASE("dimres::resolve_dim preserves existing sentinel dimensions",
          "[vector][dim-resolver][catch2]") {
    auto dir = makeTempDir();
    auto sentinel = dir / "vectors_sentinel.json";
    {
        std::ofstream out(sentinel);
        REQUIRE(out.good());
        out << R"({"embedding_dim":384})";
    }

    CHECK(yams::vector::dimres::resolve_dim(dir, 1024) == 384u);

    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

TEST_CASE("dimres::resolve_dim falls back to promoted 1024 default for new installs",
          "[vector][dim-resolver][catch2]") {
    auto dir = makeTempDir();
    CHECK(yams::vector::dimres::resolve_dim(dir, 0) == 1024u);

    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}
