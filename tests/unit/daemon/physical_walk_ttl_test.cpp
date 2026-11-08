#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/DaemonMetrics.h>

#include <filesystem>
#include <fstream>
#include <string>

using yams::daemon::computePhysicalWalkTtl;
using yams::daemon::PssSampleState;
using yams::daemon::pssSampleTtl;
using yams::daemon::sampleProcessPssKb;
using yams::daemon::storageTreeFingerprint;
using namespace std::chrono_literals;

TEST_CASE("computePhysicalWalkTtl: zero last-duration returns 60s floor",
          "[unit][daemon][tuning][catch2]") {
    REQUIRE(computePhysicalWalkTtl(0) == 60'000u);
}

TEST_CASE("computePhysicalWalkTtl: short walk pinned at 60s floor",
          "[unit][daemon][tuning][catch2]") {
    REQUIRE(computePhysicalWalkTtl(200) == 60'000u);
    REQUIRE(computePhysicalWalkTtl(5'000) == 60'000u);
}

TEST_CASE("computePhysicalWalkTtl: 30s walk produces 5min TTL", "[unit][daemon][tuning][catch2]") {
    REQUIRE(computePhysicalWalkTtl(30'000) == 300'000u);
}

TEST_CASE("computePhysicalWalkTtl: 60s walk produces 10min TTL", "[unit][daemon][tuning][catch2]") {
    REQUIRE(computePhysicalWalkTtl(60'000) == 600'000u);
}

TEST_CASE("computePhysicalWalkTtl: 5min walk clamps at 30min ceiling",
          "[unit][daemon][tuning][catch2]") {
    REQUIRE(computePhysicalWalkTtl(300'000) == 1'800'000u);
}

TEST_CASE("computePhysicalWalkTtl: extremely slow walk still clamps at ceiling",
          "[unit][daemon][tuning][catch2]") {
    REQUIRE(computePhysicalWalkTtl(60'000'000) == 1'800'000u);
}

TEST_CASE("sampleProcessPssKb reads smaps_rollup at most once per TTL",
          "[unit][daemon][metrics][catch2]") {
    PssSampleState state;
    int reads = 0;
    auto reader = [&reads]() -> std::uint64_t {
        ++reads;
        return 1000ull * static_cast<std::uint64_t>(reads);
    };
    const auto t0 = std::chrono::steady_clock::time_point{} + 1h;

    CHECK(sampleProcessPssKb(state, t0, 5s, reader) == 1000u);
    CHECK(reads == 1);
    CHECK(sampleProcessPssKb(state, t0 + 100ms, 5s, reader) == 1000u); // cached
    CHECK(sampleProcessPssKb(state, t0 + 4999ms, 5s, reader) == 1000u);
    CHECK(reads == 1);
    CHECK(sampleProcessPssKb(state, t0 + 5s, 5s, reader) == 2000u); // TTL elapsed
    CHECK(reads == 2);
    CHECK(sampleProcessPssKb(state, t0 + 2s, 5s, reader) == 3000u); // clock went backwards
    CHECK(reads == 3);
}

TEST_CASE("pssSampleTtl is 5 s busy and 10 s idle", "[unit][daemon][metrics][catch2]") {
    CHECK(pssSampleTtl(false) == 5000ms);
    CHECK(pssSampleTtl(true) == 10000ms);
}

namespace {
namespace fs = std::filesystem;

// A small CAS-shaped tree: objects/<shard>/<object>. Directory mtimes are pinned to a fixed
// past time so that a later change always moves them, whatever the kernel's timestamp tick.
struct StorageTree {
    fs::path root =
        fs::temp_directory_path() /
        ("yams_fp_" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    fs::path shard = root / "objects" / "ab";
    fs::file_time_type pinned = fs::file_time_type::clock::now() - std::chrono::hours(24);

    StorageTree() {
        fs::remove_all(root);
        fs::create_directories(shard);
        std::ofstream(shard / "abcdef") << "object";
        pin();
    }
    ~StorageTree() {
        std::error_code ec;
        fs::remove_all(root, ec);
    }
    void pin() const {
        for (const auto& dir : {root, root / "objects", shard}) {
            fs::last_write_time(dir, pinned);
        }
    }
};
} // namespace

TEST_CASE("storageTreeFingerprint is stable while nothing changes",
          "[unit][daemon][metrics][catch2]") {
    StorageTree tree;
    const auto first = storageTreeFingerprint(tree.root);
    CHECK(first != 0);
    CHECK(storageTreeFingerprint(tree.root) == first);
}

TEST_CASE("storageTreeFingerprint changes when an object is added or removed",
          "[unit][daemon][metrics][catch2]") {
    StorageTree tree;
    const auto before = storageTreeFingerprint(tree.root);

    SECTION("added to an existing shard") {
        std::ofstream(tree.shard / "123456") << "new object";
        CHECK(storageTreeFingerprint(tree.root) != before);
    }
    SECTION("removed from a shard") {
        fs::remove(tree.shard / "abcdef");
        CHECK(storageTreeFingerprint(tree.root) != before);
    }
    SECTION("a new shard") {
        fs::create_directories(tree.root / "objects" / "cd");
        CHECK(storageTreeFingerprint(tree.root) != before);
    }
}

TEST_CASE("storageTreeFingerprint is 0 for a missing root", "[unit][daemon][metrics][catch2]") {
    CHECK(storageTreeFingerprint(fs::temp_directory_path() / "yams_fp_does_not_exist") == 0);
}

TEST_CASE("storageTreeFingerprint is 0 when part of the tree cannot be read",
          "[unit][daemon][metrics][catch2]") {
#ifdef _WIN32
    SKIP("POSIX permissions");
#else
    StorageTree tree;
    const auto objects = tree.root / "objects";
    fs::permissions(objects, fs::perms::none);
    std::error_code probe;
    const bool readable = fs::directory_iterator(objects, probe) != fs::directory_iterator{};
    if (!probe && readable) {
        fs::permissions(objects, fs::perms::owner_all);
        SKIP("running with permission to read any directory");
    }
    // A partial fingerprint could repeat and keep stale totals; 0 forces the full walk.
    CHECK(storageTreeFingerprint(tree.root) == 0);
    fs::permissions(objects, fs::perms::owner_all);
#endif
}
