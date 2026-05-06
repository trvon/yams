#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/db_recovery.h>
#include <yams/metadata/database.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>

namespace fs = std::filesystem;

namespace {

fs::path makeScratchDir(const std::string& prefix) {
    auto base = fs::temp_directory_path() /
                (prefix + "_" +
                 std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    fs::create_directories(base);
    return base;
}

void writeFile(const fs::path& p, const std::string& content) {
    std::ofstream out(p, std::ios::binary);
    out.write(content.data(), static_cast<std::streamsize>(content.size()));
}

void corruptHeader(const fs::path& p) {
    std::fstream f(p, std::ios::in | std::ios::out | std::ios::binary);
    REQUIRE(f.is_open());
    f.seekp(20, std::ios::beg);
    const char garbage[] = "\xff\xff\xff\xff\xff\xff\xff\xff";
    f.write(garbage, 8);
}

} // namespace

TEST_CASE("Database::checkIntegrity passes on a fresh DB", "[unit][daemon][db_recovery]") {
    auto dir = makeScratchDir("yams_db_recovery_ok");
    auto dbPath = dir / "yams.db";

    yams::metadata::Database db;
    auto open = db.open(dbPath.string(), yams::metadata::ConnectionMode::Create);
    REQUIRE(open);

    auto check = db.checkIntegrity();
    REQUIRE(check);

    db.close();
    std::error_code ec;
    fs::remove_all(dir, ec);
}

TEST_CASE("Database::checkIntegrity detects header corruption", "[unit][daemon][db_recovery]") {
    auto dir = makeScratchDir("yams_db_recovery_corrupt");
    auto dbPath = dir / "yams.db";

    {
        yams::metadata::Database db;
        REQUIRE(db.open(dbPath.string(), yams::metadata::ConnectionMode::Create));
        REQUIRE(db.execute("CREATE TABLE t(x INTEGER)"));
        REQUIRE(db.execute("INSERT INTO t VALUES(1)"));
        db.close();
    }
    corruptHeader(dbPath);

    yams::metadata::Database db;
    auto open = db.open(dbPath.string(), yams::metadata::ConnectionMode::ReadWrite);
    if (!open) {
        SUCCEED("open failed; corruption detected at open time");
    } else {
        auto check = db.checkIntegrity();
        REQUIRE_FALSE(check);
    }
    db.close();
    std::error_code ec;
    fs::remove_all(dir, ec);
}

TEST_CASE("quarantineAndRecreate renames db + sentinel + sibling files",
          "[unit][daemon][db_recovery]") {
    auto dir = makeScratchDir("yams_db_recovery_quarantine");
    auto dbPath = dir / "yams.db";
    writeFile(dbPath, std::string("SQLite format 3\0fakecontent", 27));
    writeFile(fs::path(dbPath.string() + "-wal"), "wal");
    writeFile(fs::path(dbPath.string() + "-shm"), "shm");

    auto res = yams::daemon::quarantineAndRecreate(dbPath);
    REQUIRE(res);

    REQUIRE_FALSE(fs::exists(dbPath));
    REQUIRE(fs::exists(res.value().quarantinedPath));
    REQUIRE(fs::exists(res.value().sentinelPath));
    REQUIRE(fs::exists(fs::path(res.value().quarantinedPath.string() + "-wal")));
    REQUIRE(fs::exists(fs::path(res.value().quarantinedPath.string() + "-shm")));

    auto sentinel = yams::daemon::readLatestRecoverySentinel(dbPath);
    REQUIRE(sentinel.has_value());
    REQUIRE(sentinel->quarantinedPath == res.value().quarantinedPath);
    REQUIRE(sentinel->timestamp == res.value().timestamp);

    std::error_code ec;
    fs::remove_all(dir, ec);
}

TEST_CASE("quarantineAndRecreate fails cleanly when source is missing",
          "[unit][daemon][db_recovery]") {
    auto dir = makeScratchDir("yams_db_recovery_missing");
    auto dbPath = dir / "yams.db";

    auto res = yams::daemon::quarantineAndRecreate(dbPath);
    REQUIRE_FALSE(res);

    auto sentinel = yams::daemon::readLatestRecoverySentinel(dbPath);
    REQUIRE_FALSE(sentinel.has_value());

    std::error_code ec;
    fs::remove_all(dir, ec);
}

TEST_CASE("Recovery sentinel survives across calls and is idempotent to read",
          "[unit][daemon][db_recovery]") {
    auto dir = makeScratchDir("yams_db_recovery_sentinel");
    auto dbPath = dir / "yams.db";
    writeFile(dbPath, "stub");

    auto first = yams::daemon::quarantineAndRecreate(dbPath);
    REQUIRE(first);

    auto sentinel1 = yams::daemon::readLatestRecoverySentinel(dbPath);
    auto sentinel2 = yams::daemon::readLatestRecoverySentinel(dbPath);
    REQUIRE(sentinel1.has_value());
    REQUIRE(sentinel2.has_value());
    REQUIRE(sentinel1->quarantinedPath == sentinel2->quarantinedPath);

    std::error_code ec;
    fs::remove_all(dir, ec);
}
