#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/storage_preflight.h>

#include <chrono>
#include <filesystem>
#include <string>

#ifndef _WIN32
#include <sys/stat.h>
#endif

namespace fs = std::filesystem;

namespace {

fs::path makeScratchDir(const std::string& prefix) {
    auto base = fs::temp_directory_path() /
                (prefix + "_" +
                 std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    fs::create_directories(base);
    return base;
}

} // namespace

TEST_CASE("probeStorage: healthy local dir returns reachable=true",
          "[unit][daemon][storage_preflight][catch2]") {
    const auto dir = makeScratchDir("yams_preflight_ok");
    auto res = yams::daemon::probeStorage(dir);
    REQUIRE(res.has_value());
    REQUIRE(res.value().reachable);
    REQUIRE_FALSE(res.value().slow);
    REQUIRE(res.value().probeDuration.count() < 500);
    fs::remove_all(dir);
}

TEST_CASE("probeStorage: missing parent path is created (idempotent)",
          "[unit][daemon][storage_preflight][catch2]") {
    auto base = makeScratchDir("yams_preflight_missing");
    fs::path nested = base / "nested" / "deeper";
    REQUIRE_FALSE(fs::exists(nested));
    auto res = yams::daemon::probeStorage(nested);
    REQUIRE(res.has_value());
    REQUIRE(fs::exists(nested));
    fs::remove_all(base);
}

#ifndef _WIN32
TEST_CASE("probeStorage: read-only dir returns Error",
          "[unit][daemon][storage_preflight][catch2]") {
    const auto dir = makeScratchDir("yams_preflight_ro");
    REQUIRE(::chmod(dir.c_str(), 0500) == 0);
    auto res = yams::daemon::probeStorage(dir);
    // Restore so test cleanup can remove it.
    ::chmod(dir.c_str(), 0700);
    fs::remove_all(dir);
    REQUIRE_FALSE(res.has_value());
}
#endif

TEST_CASE("probeStorage: slow threshold triggers slow=true without erroring",
          "[unit][daemon][storage_preflight][catch2]") {
    const auto dir = makeScratchDir("yams_preflight_slow");
    auto res =
        yams::daemon::probeStorage(dir, std::chrono::seconds(2), std::chrono::milliseconds(0));
    REQUIRE(res.has_value());
    REQUIRE(res.value().slow);
    REQUIRE_FALSE(res.value().detail.empty());
    fs::remove_all(dir);
}

TEST_CASE("probeStorage: empty path is rejected", "[unit][daemon][storage_preflight][catch2]") {
    auto res = yams::daemon::probeStorage(fs::path{});
    REQUIRE_FALSE(res.has_value());
}
