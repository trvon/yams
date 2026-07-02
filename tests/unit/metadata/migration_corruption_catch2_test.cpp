// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <filesystem>
#include <string>

#include <catch2/catch_test_macros.hpp>

#include <yams/metadata/database.h>
#include <yams/metadata/migration.h>

#include "../../common/sqlite_corruption.h"

using namespace yams;
using namespace yams::metadata;
using namespace yams::test;

// NOLINTBEGIN(bugprone-chained-comparison)
namespace {

struct ArtifactGuard {
    explicit ArtifactGuard(std::filesystem::path p) : path(std::move(p)) {}
    ~ArtifactGuard() { remove_sqlite_artifacts(path); }
    std::filesystem::path path;
};

Migration makeTableMigration(int version, const std::string& table) {
    return MigrationBuilder(version, "create_" + table)
        .up("CREATE TABLE " + table + "(id INTEGER PRIMARY KEY, payload TEXT)")
        .down("DROP TABLE " + table)
        .build();
}

} // namespace

TEST_CASE("getCurrentVersion fails on corrupt migration_history",
          "[unit][metadata][corruption][migration]") {
    auto dbPath = make_temp_sqlite_path("yams_mig_corrupt_hist_");
    ArtifactGuard guard{dbPath};

    {
        Database db;
        REQUIRE(db.open(dbPath.string(), ConnectionMode::Create));
        MigrationManager mm(db);
        REQUIRE(mm.initialize());
        mm.registerMigration(makeTableMigration(1, "t1"));
        REQUIRE(mm.migrate());
        REQUIRE(db.execute("PRAGMA wal_checkpoint(TRUNCATE)"));
        db.close();
    }

    corruptPage(dbPath, static_cast<std::uint64_t>(tableRootPage(dbPath, "migration_history")));

    Database db;
    REQUIRE(db.open(dbPath.string(), ConnectionMode::ReadWrite));
    MigrationManager mm(db);
    auto version = mm.getCurrentVersion();
    REQUIRE_FALSE(version);
    CHECK(version.error().code == ErrorCode::CorruptedData);
    db.close();
}

TEST_CASE("migrate on truncated DB fails cleanly", "[unit][metadata][corruption][migration]") {
    auto dbPath = migrated_metadata_db_template().clone("yams_mig_corrupt_trunc_");
    ArtifactGuard guard{dbPath};

    truncateFile(dbPath, 0.5);

    Database db;
    auto open = db.open(dbPath.string(), ConnectionMode::ReadWrite);
    if (!open) {
        SUCCEED("open failed; truncation detected at open time");
        return;
    }

    MigrationManager mm(db);
    bool anyFailed = false;
    auto init = mm.initialize();
    if (!init) {
        anyFailed = true;
    } else {
        mm.registerMigrations(YamsMetadataMigrations::getAllMigrations());
        auto migrated = mm.migrate();
        auto verified = mm.verifyIntegrity();
        anyFailed = !migrated || !verified;
    }
    CHECK(anyFailed);
    db.close();
}

TEST_CASE("failed migration is recorded and retry resumes",
          "[unit][metadata][corruption][migration]") {
    auto dbPath = make_temp_sqlite_path("yams_mig_crash_resume_");
    ArtifactGuard guard{dbPath};

    Database db;
    REQUIRE(db.open(dbPath.string(), ConnectionMode::Create));
    MigrationManager mm(db);
    REQUIRE(mm.initialize());
    mm.registerMigration(makeTableMigration(1, "t1"));

    bool failOnce = true;
    mm.registerMigration(
        MigrationBuilder(2, "flaky_v2")
            .upFunction([&failOnce](Database& mdb) -> Result<void> {
                if (failOnce) {
                    failOnce = false;
                    return Error{ErrorCode::DatabaseError, "simulated crash mid-migration"};
                }
                return mdb.execute("CREATE TABLE t2(id INTEGER PRIMARY KEY)");
            })
            .down("DROP TABLE t2")
            .build());

    auto firstAttempt = mm.migrate();
    REQUIRE_FALSE(firstAttempt);

    auto versionAfterFailure = mm.getCurrentVersion();
    REQUIRE(versionAfterFailure);
    CHECK(versionAfterFailure.value() == 1);

    auto history = mm.getHistory();
    REQUIRE(history);
    bool failureRecorded = false;
    for (const auto& entry : history.value()) {
        if (!entry.success && entry.version == 2) {
            failureRecorded = true;
        }
    }
    CHECK(failureRecorded);

    auto secondAttempt = mm.migrate();
    REQUIRE(secondAttempt);
    auto versionAfterRetry = mm.getCurrentVersion();
    REQUIRE(versionAfterRetry);
    CHECK(versionAfterRetry.value() == 2);

    auto t2Exists = db.tableExists("t2");
    REQUIRE(t2Exists);
    CHECK(t2Exists.value());
    db.close();
}

TEST_CASE("newer-than-binary schema is not silently rolled back",
          "[unit][metadata][corruption][migration]") {
    auto dbPath = make_temp_sqlite_path("yams_mig_newer_schema_");
    ArtifactGuard guard{dbPath};

    Database db;
    REQUIRE(db.open(dbPath.string(), ConnectionMode::Create));
    {
        MigrationManager newerBinary(db);
        REQUIRE(newerBinary.initialize());
        newerBinary.registerMigration(makeTableMigration(1, "t1"));
        newerBinary.registerMigration(makeTableMigration(2, "t2"));
        REQUIRE(newerBinary.migrate());
    }

    MigrationManager olderBinary(db);
    REQUIRE(olderBinary.initialize());
    olderBinary.registerMigration(makeTableMigration(1, "t1"));

    auto result = olderBinary.migrate();
    REQUIRE_FALSE(result);
    CHECK(result.error().code == ErrorCode::InvalidState);

    auto version = olderBinary.getCurrentVersion();
    REQUIRE(version);
    CHECK(version.value() == 2);
    auto t2Exists = db.tableExists("t2");
    REQUIRE(t2Exists);
    CHECK(t2Exists.value());

    MigrationManager fullBinary(db);
    REQUIRE(fullBinary.initialize());
    fullBinary.registerMigration(makeTableMigration(1, "t1"));
    fullBinary.registerMigration(makeTableMigration(2, "t2"));
    auto explicitRollback = fullBinary.migrateTo(1);
    REQUIRE(explicitRollback);
    auto rolledBack = fullBinary.getCurrentVersion();
    REQUIRE(rolledBack);
    CHECK(rolledBack.value() == 1);
    db.close();
}

TEST_CASE("verifyIntegrity reports page corruption", "[unit][metadata][corruption][migration]") {
    auto dbPath = make_temp_sqlite_path("yams_mig_verify_corrupt_");
    ArtifactGuard guard{dbPath};

    {
        Database db;
        REQUIRE(db.open(dbPath.string(), ConnectionMode::Create));
        MigrationManager mm(db);
        REQUIRE(mm.initialize());
        mm.registerMigration(makeTableMigration(1, "t1"));
        REQUIRE(mm.migrate());
        REQUIRE(db.execute("BEGIN IMMEDIATE"));
        for (int i = 0; i < 500; ++i) {
            auto stmt = db.prepare("INSERT INTO t1(payload) VALUES(?)");
            REQUIRE(stmt);
            REQUIRE(stmt.value().bind(1, std::string(200, 'x')));
            REQUIRE(stmt.value().execute());
        }
        REQUIRE(db.execute("COMMIT"));
        REQUIRE(db.execute("PRAGMA wal_checkpoint(TRUNCATE)"));
        REQUIRE(mm.verifyIntegrity());
        db.close();
    }

    corruptPage(dbPath, static_cast<std::uint64_t>(tableRootPage(dbPath, "t1")));

    Database db;
    REQUIRE(db.open(dbPath.string(), ConnectionMode::ReadWrite));
    MigrationManager mm(db);
    auto verified = mm.verifyIntegrity();
    REQUIRE_FALSE(verified);
    CHECK(verified.error().code != ErrorCode::ResourceBusy);
    db.close();
}
// NOLINTEND(bugprone-chained-comparison)
