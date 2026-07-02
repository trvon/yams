#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/db_recovery.h>
#include <yams/metadata/database.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>

#include "../../common/sqlite_corruption.h"

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

using yams::test::corruptHeader;

void seedTable(const fs::path& dbPath, int rows) {
    yams::metadata::Database db;
    REQUIRE(db.open(dbPath.string(), yams::metadata::ConnectionMode::Create));
    REQUIRE(db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT)"));
    REQUIRE(db.execute("BEGIN IMMEDIATE"));
    for (int i = 0; i < rows; ++i) {
        auto stmt = db.prepare("INSERT INTO t(payload) VALUES(?)");
        REQUIRE(stmt);
        REQUIRE(stmt.value().bind(1, std::string(200, 'x')));
        REQUIRE(stmt.value().execute());
    }
    REQUIRE(db.execute("COMMIT"));
    REQUIRE(db.execute("PRAGMA wal_checkpoint(TRUNCATE)"));
    db.close();
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

TEST_CASE("Database::checkIntegrity classifies FTS5 corruption distinctly",
          "[unit][daemon][db_recovery]") {
    using yams::metadata::testing_isTransientIntegrityCheckMessage;

    // FTS5 inverted-index + "locked" is FTS5 corruption, not transient lock.
    CHECK_FALSE(testing_isTransientIntegrityCheckMessage(
        "unable to validate the inverted index for FTS5 table main.documents_fts: database is "
        "locked"));
    // Genuine transient lock messages should still be classified as transient.
    CHECK(testing_isTransientIntegrityCheckMessage("database table is locked"));
    CHECK(testing_isTransientIntegrityCheckMessage("database is busy"));
    // Hard corruption should still be non-transient.
    CHECK_FALSE(testing_isTransientIntegrityCheckMessage("database disk image is malformed"));
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

TEST_CASE("Database::checkIntegrity detection matrix", "[unit][daemon][db_recovery][corruption]") {
    auto dir = makeScratchDir("yams_db_recovery_matrix");
    auto dbPath = dir / "yams.db";
    seedTable(dbPath, 500);

    SECTION("truncated file") { yams::test::truncateFile(dbPath, 0.5); }
    SECTION("corrupt table root page") {
        yams::test::corruptPage(
            dbPath, static_cast<std::uint64_t>(yams::test::tableRootPage(dbPath, "t")));
    }
    SECTION("corrupt mid-file page") {
        const auto pageCount = yams::test::sqlitePageCount(dbPath);
        REQUIRE(pageCount > 4);
        yams::test::corruptPage(dbPath, pageCount / 2);
    }

    yams::metadata::Database db;
    auto open = db.open(dbPath.string(), yams::metadata::ConnectionMode::ReadWrite);
    if (!open) {
        SUCCEED("open failed; corruption detected at open time");
    } else {
        auto check = db.checkIntegrity();
        REQUIRE_FALSE(check);
        CHECK(check.error().code != yams::ErrorCode::ResourceBusy);
        db.close();
    }

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
