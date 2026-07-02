// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <filesystem>
#include <string>

#include <catch2/catch_test_macros.hpp>

#include <yams/metadata/database.h>

#include "../../common/sqlite_corruption.h"

using namespace yams;
using namespace yams::metadata;
using namespace yams::test;

// NOLINTBEGIN(bugprone-chained-comparison)
namespace {

constexpr int kSeedRows = 500;

std::filesystem::path makeSeededDb(std::string_view prefix, bool checkpoint = true) {
    auto dbPath = make_temp_sqlite_path(prefix);
    Database db;
    REQUIRE(db.open(dbPath.string(), ConnectionMode::Create));
    REQUIRE(db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT NOT NULL)"));
    REQUIRE(db.execute("BEGIN IMMEDIATE"));
    for (int i = 0; i < kSeedRows; ++i) {
        auto stmt = db.prepare("INSERT INTO t(payload) VALUES(?)");
        REQUIRE(stmt);
        REQUIRE(stmt.value().bind(1, std::string(200, 'a' + (i % 26))));
        REQUIRE(stmt.value().execute());
    }
    REQUIRE(db.execute("COMMIT"));
    if (checkpoint) {
        REQUIRE(db.execute("PRAGMA wal_checkpoint(TRUNCATE)"));
    }
    db.close();
    return dbPath;
}

int64_t countRows(Database& db) {
    auto stmt = db.prepare("SELECT COUNT(*) FROM t");
    REQUIRE(stmt);
    auto row = stmt.value().step();
    REQUIRE(row);
    REQUIRE(row.value());
    return stmt.value().getInt64(0);
}

struct ArtifactGuard {
    explicit ArtifactGuard(std::filesystem::path p) : path(std::move(p)) {}
    ~ArtifactGuard() { remove_sqlite_artifacts(path); }
    std::filesystem::path path;
};

} // namespace

TEST_CASE("checkIntegrity detects mid-file page corruption as non-transient",
          "[unit][metadata][corruption]") {
    auto dbPath = makeSeededDb("yams_corrupt_page_");
    ArtifactGuard guard{dbPath};

    const auto rootPage = tableRootPage(dbPath, "t");
    REQUIRE(rootPage > 1);
    corruptPage(dbPath, static_cast<std::uint64_t>(rootPage));

    Database db;
    auto open = db.open(dbPath.string(), ConnectionMode::ReadWrite);
    if (open) {
        auto check = db.checkIntegrity();
        REQUIRE_FALSE(check);
        CHECK(check.error().code != ErrorCode::ResourceBusy);
        db.close();
    }
}

TEST_CASE("checkIntegrity detects file truncation", "[unit][metadata][corruption]") {
    auto dbPath = makeSeededDb("yams_corrupt_trunc_");
    ArtifactGuard guard{dbPath};

    truncateFile(dbPath, 0.5);

    Database db;
    auto open = db.open(dbPath.string(), ConnectionMode::ReadWrite);
    if (open) {
        auto check = db.checkIntegrity();
        REQUIRE_FALSE(check);
        CHECK(check.error().code != ErrorCode::ResourceBusy);
        db.close();
    }
}

TEST_CASE("open succeeds on page-corrupt DB and error surfaces on first read",
          "[unit][metadata][corruption]") {
    auto dbPath = makeSeededDb("yams_corrupt_deferred_");
    ArtifactGuard guard{dbPath};

    corruptPage(dbPath, static_cast<std::uint64_t>(tableRootPage(dbPath, "t")));

    Database db;
    REQUIRE(db.open(dbPath.string(), ConnectionMode::ReadWrite));

    auto stmt = db.prepare("SELECT COUNT(*) FROM t");
    if (stmt) {
        auto row = stmt.value().step();
        REQUIRE_FALSE(row);
        CHECK(row.error().code == ErrorCode::CorruptedData);
    } else {
        CHECK(stmt.error().code == ErrorCode::CorruptedData);
    }
    db.close();
}

TEST_CASE("writes against corrupt DB fail cleanly", "[unit][metadata][corruption]") {
    auto dbPath = makeSeededDb("yams_corrupt_write_");
    ArtifactGuard guard{dbPath};

    corruptPage(dbPath, static_cast<std::uint64_t>(tableRootPage(dbPath, "t")));

    Database db;
    REQUIRE(db.open(dbPath.string(), ConnectionMode::ReadWrite));
    auto exec = db.execute("INSERT INTO t(payload) VALUES('x')");
    REQUIRE_FALSE(exec);
    db.close();
}

TEST_CASE("corrupt WAL frame drops uncheckpointed commit but keeps prior state",
          "[unit][metadata][corruption]") {
    auto dbPath = make_temp_sqlite_path("yams_corrupt_wal_");
    ArtifactGuard guard{dbPath};

    Database db;
    REQUIRE(db.open(dbPath.string(), ConnectionMode::Create));
    REQUIRE(db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT NOT NULL)"));
    REQUIRE(db.execute("INSERT INTO t(payload) VALUES('committed')"));
    REQUIRE(db.execute("PRAGMA wal_checkpoint(TRUNCATE)"));
    REQUIRE(db.execute("INSERT INTO t(payload) VALUES('in-wal-only')"));

    auto snapshot = copySqliteFiles(dbPath, "yams_corrupt_wal_snap_");
    ArtifactGuard snapGuard{snapshot};
    db.close();

    REQUIRE(std::filesystem::exists(walPath(snapshot)));
    corruptWalFrame(snapshot);

    Database reopened;
    REQUIRE(reopened.open(snapshot.string(), ConnectionMode::ReadWrite));
    REQUIRE(reopened.checkIntegrity());
    auto stmt = reopened.prepare("SELECT COUNT(*) FROM t WHERE payload = 'committed'");
    REQUIRE(stmt);
    auto row = stmt.value().step();
    REQUIRE(row);
    REQUIRE(row.value());
    CHECK(stmt.value().getInt64(0) == 1);

    auto walOnly = reopened.prepare("SELECT COUNT(*) FROM t WHERE payload = 'in-wal-only'");
    REQUIRE(walOnly);
    auto walRow = walOnly.value().step();
    REQUIRE(walRow);
    REQUIRE(walRow.value());
    CHECK(walOnly.value().getInt64(0) == 0);
    reopened.close();
}

TEST_CASE("corrupt -shm does not poison a fresh open", "[unit][metadata][corruption]") {
    auto dbPath = make_temp_sqlite_path("yams_corrupt_shm_");
    ArtifactGuard guard{dbPath};

    Database db;
    REQUIRE(db.open(dbPath.string(), ConnectionMode::Create));
    REQUIRE(db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT NOT NULL)"));
    REQUIRE(db.execute("INSERT INTO t(payload) VALUES('row')"));

    auto snapshot = copySqliteFiles(dbPath, "yams_corrupt_shm_snap_");
    ArtifactGuard snapGuard{snapshot};
    db.close();

    if (std::filesystem::exists(shmPath(snapshot))) {
        corruptShm(snapshot);
    }

    Database reopened;
    REQUIRE(reopened.open(snapshot.string(), ConnectionMode::ReadWrite));
    REQUIRE(reopened.checkIntegrity());
    CHECK(countRows(reopened) == 1);
    reopened.close();
}

TEST_CASE("mid-transaction crash snapshot replays to pre-transaction state",
          "[unit][metadata][corruption]") {
    auto dbPath = makeSeededDb("yams_corrupt_crash_", false);
    ArtifactGuard guard{dbPath};

    auto snapshot = snapshotMidTransaction(
        dbPath,
        [](Database& db) {
            REQUIRE(db.execute("INSERT INTO t(payload) VALUES('uncommitted')"));
            REQUIRE(db.execute("DELETE FROM t WHERE id <= 10"));
        },
        "yams_corrupt_crash_snap_");
    ArtifactGuard snapGuard{snapshot};

    Database reopened;
    REQUIRE(reopened.open(snapshot.string(), ConnectionMode::ReadWrite));
    REQUIRE(reopened.checkIntegrity());
    CHECK(countRows(reopened) == kSeedRows);

    auto stmt = reopened.prepare("SELECT COUNT(*) FROM t WHERE payload = 'uncommitted'");
    REQUIRE(stmt);
    auto row = stmt.value().step();
    REQUIRE(row);
    REQUIRE(row.value());
    CHECK(stmt.value().getInt64(0) == 0);
    reopened.close();
}

TEST_CASE("transient integrity message classification", "[unit][metadata][corruption]") {
    using yams::metadata::testing_isTransientIntegrityCheckMessage;

    CHECK(testing_isTransientIntegrityCheckMessage("database is locked"));
    CHECK(testing_isTransientIntegrityCheckMessage("database table is locked"));
    CHECK(testing_isTransientIntegrityCheckMessage("database is busy"));

    CHECK_FALSE(testing_isTransientIntegrityCheckMessage("database disk image is malformed"));
    CHECK_FALSE(testing_isTransientIntegrityCheckMessage(
        "row 3 missing from index idx_documents_path"));
    CHECK_FALSE(testing_isTransientIntegrityCheckMessage("btreeInitPage() returns error code 11"));
    CHECK_FALSE(testing_isTransientIntegrityCheckMessage(
        "unable to validate the inverted index for FTS5 table main.documents_fts: database is "
        "locked"));
    CHECK_FALSE(testing_isTransientIntegrityCheckMessage(""));
}
// NOLINTEND(bugprone-chained-comparison)
