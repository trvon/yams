// Copyright (c) 2026 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Tests for auto-VACUUM during database startup to prevent file bloat
// from repeated crash-recovery cycles.
//
// PBI: auto-vacuum-bloat

#include <catch2/catch_test_macros.hpp>

#include <yams/metadata/database.h>

#include <chrono>
#include <cmath>
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

// Create a DB with insert/delete cycles to bloat the file with free pages.
std::uintmax_t bloatDatabase(yams::metadata::Database& db, const fs::path& dbPath,
                             int numCycles = 5, int rowsPerCycle = 5000) {
    REQUIRE(db.open(dbPath.string(), yams::metadata::ConnectionMode::Create));
    REQUIRE(db.execute("CREATE TABLE IF NOT EXISTS bloated(x INTEGER PRIMARY KEY, y TEXT)"));

    for (int cycle = 0; cycle < numCycles; ++cycle) {
        REQUIRE(db.execute("BEGIN"));
        for (int i = 0; i < rowsPerCycle; ++i) {
            auto sql =
                "INSERT INTO bloated VALUES(" + std::to_string(cycle * rowsPerCycle + i) +
                ", 'padding_data_to_make_pages_larger_abcdefghijklmnopqrstuvwxyz0123456789')";
            REQUIRE(db.execute(sql));
        }
        REQUIRE(db.execute("COMMIT"));

        // Delete most rows leaving free pages
        REQUIRE(db.execute("BEGIN"));
        auto delRange = "DELETE FROM bloated WHERE x < " +
                        std::to_string(cycle * rowsPerCycle + rowsPerCycle - 1);
        REQUIRE(db.execute(delRange));
        REQUIRE(db.execute("COMMIT"));
    }

    // Keep a few rows
    auto lastCycle = numCycles;
    REQUIRE(db.execute("INSERT INTO bloated VALUES(" +
                       std::to_string(lastCycle * rowsPerCycle + 1) + ", 'survivor_row')"));

    db.close();

    std::error_code ec;
    auto size = fs::file_size(dbPath, ec);
    return ec ? 0 : size;
}

} // namespace

TEST_CASE("VACUUM shrinks a bloated database file", "[unit][daemon][db_recovery][vacuum]") {
    auto dir = makeScratchDir("yams_vacuum_shrink");
    auto dbPath = dir / "yams.db";

    std::uintmax_t sizeBefore = 0;
    {
        yams::metadata::Database db;
        sizeBefore = bloatDatabase(db, dbPath, 5, 5000);
    }

    if (sizeBefore == 0) {
        SUCCEED("Skipping — file_size unavailable on this filesystem");
        std::error_code ec;
        fs::remove_all(dir, ec);
        return;
    }

    INFO("DB size before VACUUM: " << sizeBefore << " bytes");
    REQUIRE(sizeBefore > 50'000);

    // Run VACUUM
    {
        yams::metadata::Database db;
        REQUIRE(db.open(dbPath.string(), yams::metadata::ConnectionMode::ReadWrite));
        auto vacuumR = db.execute("VACUUM");
        REQUIRE(vacuumR);
        db.close();
    }

    std::error_code ec;
    auto sizeAfter = fs::file_size(dbPath, ec);
    REQUIRE_FALSE(ec);
    INFO("DB size after VACUUM: " << sizeAfter << " bytes");
    INFO("Size reduction: " << ((1.0 - static_cast<double>(sizeAfter) / sizeBefore) * 100.0)
                            << "%");

    // VACUUM must significantly shrink the file
    REQUIRE(sizeAfter < sizeBefore / 2);

    // Data must survive
    {
        yams::metadata::Database db;
        REQUIRE(db.open(dbPath.string(), yams::metadata::ConnectionMode::ReadWrite));
        auto stmtR = db.prepare("SELECT COUNT(*) FROM bloated WHERE y = 'survivor_row'");
        REQUIRE(stmtR);
        auto& stmt = stmtR.value();
        REQUIRE(stmt.step());
        REQUIRE(stmt.getInt(0) == 1);
        db.close();
    }

    fs::remove_all(dir, ec);
}

TEST_CASE("VACUUM preserves all table data", "[unit][daemon][db_recovery][vacuum]") {
    auto dir = makeScratchDir("yams_vacuum_preserve");
    auto dbPath = dir / "yams.db";

    {
        yams::metadata::Database db;
        REQUIRE(db.open(dbPath.string(), yams::metadata::ConnectionMode::Create));
        REQUIRE(db.execute("CREATE TABLE test_data(id INTEGER PRIMARY KEY, value TEXT)"));
        REQUIRE(db.execute("INSERT INTO test_data VALUES(1, 'alpha')"));
        REQUIRE(db.execute("INSERT INTO test_data VALUES(2, 'beta')"));
        REQUIRE(db.execute("INSERT INTO test_data VALUES(3, 'gamma')"));

        // Delete and re-insert to create free pages
        REQUIRE(db.execute("DELETE FROM test_data WHERE id = 2"));
        REQUIRE(db.execute("INSERT INTO test_data VALUES(2, 'beta_restored')"));

        REQUIRE(db.execute("CREATE TABLE test_data2(a INTEGER, b REAL)"));
        REQUIRE(db.execute("INSERT INTO test_data2 VALUES(100, 3.14)"));
        REQUIRE(db.execute("INSERT INTO test_data2 VALUES(200, 2.718)"));

        auto vacuumR = db.execute("VACUUM");
        REQUIRE(vacuumR);

        // Verify test_data rows
        {
            auto stmtR = db.prepare("SELECT id, value FROM test_data ORDER BY id");
            REQUIRE(stmtR);
            auto& stmt = stmtR.value();
            REQUIRE(stmt.step());
            REQUIRE(stmt.getInt(0) == 1);
            REQUIRE(stmt.getString(1) == "alpha");
            REQUIRE(stmt.step());
            REQUIRE(stmt.getInt(0) == 2);
            REQUIRE(stmt.getString(1) == "beta_restored");
            REQUIRE(stmt.step());
            REQUIRE(stmt.getInt(0) == 3);
            REQUIRE(stmt.getString(1) == "gamma");
            REQUIRE(!stmt.step().value());
        }
        // Verify test_data2 rows
        {
            auto stmtR = db.prepare("SELECT a, b FROM test_data2 ORDER BY a");
            REQUIRE(stmtR);
            auto& stmt = stmtR.value();
            REQUIRE(stmt.step());
            REQUIRE(stmt.getInt(0) == 100);
            REQUIRE(std::abs(stmt.getDouble(1) - 3.14) < 0.001);
            REQUIRE(stmt.step());
            REQUIRE(stmt.getInt(0) == 200);
            REQUIRE(std::abs(stmt.getDouble(1) - 2.718) < 0.001);
            REQUIRE(!stmt.step().value());
        }

        db.close();
    }

    // Reopen and verify persistence
    {
        yams::metadata::Database db;
        REQUIRE(db.open(dbPath.string(), yams::metadata::ConnectionMode::ReadWrite));
        auto stmtR = db.prepare("SELECT COUNT(*) FROM test_data");
        REQUIRE(stmtR);
        auto& stmt = stmtR.value();
        REQUIRE(stmt.step());
        REQUIRE(stmt.getInt(0) == 3);
        db.close();
    }

    std::error_code ec;
    fs::remove_all(dir, ec);
}

TEST_CASE("VACUUM is safe on a tiny database", "[unit][daemon][db_recovery][vacuum]") {
    auto dir = makeScratchDir("yams_vacuum_tiny");
    auto dbPath = dir / "yams.db";

    {
        yams::metadata::Database db;
        REQUIRE(db.open(dbPath.string(), yams::metadata::ConnectionMode::Create));
        REQUIRE(db.execute("CREATE TABLE tiny(x INTEGER)"));
        REQUIRE(db.execute("INSERT INTO tiny VALUES(1)"));
        REQUIRE(db.execute("INSERT INTO tiny VALUES(2)"));

        auto vacuumR = db.execute("VACUUM");
        REQUIRE(vacuumR);

        auto stmtR = db.prepare("SELECT COUNT(*) FROM tiny");
        REQUIRE(stmtR);
        auto& stmt = stmtR.value();
        REQUIRE(stmt.step());
        REQUIRE(stmt.getInt(0) == 2);

        db.close();
    }

    std::error_code ec;
    fs::remove_all(dir, ec);
}

TEST_CASE("VACUUM on empty database is safe", "[unit][daemon][db_recovery][vacuum]") {
    auto dir = makeScratchDir("yams_vacuum_empty");
    auto dbPath = dir / "yams.db";

    {
        yams::metadata::Database db;
        REQUIRE(db.open(dbPath.string(), yams::metadata::ConnectionMode::Create));
        // Opened but no tables/data
        auto vacuumR = db.execute("VACUUM");
        REQUIRE(vacuumR);
        db.close();
    }

    std::error_code ec;
    auto sizeAfter = fs::file_size(dbPath, ec);
    REQUIRE_FALSE(ec);
    INFO("Empty DB after VACUUM: " << sizeAfter << " bytes");
    REQUIRE(sizeAfter <= 4096);

    fs::remove_all(dir, ec);
}
