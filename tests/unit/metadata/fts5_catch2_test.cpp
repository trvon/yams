// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 YAMS Contributors

#include <chrono>
#include <filesystem>

#include <spdlog/spdlog.h>
#include <catch2/catch_test_macros.hpp>

#include <yams/metadata/database.h>

using namespace yams::metadata;

namespace {
struct FTS5Fixture {
    FTS5Fixture() {
        auto tempDir = std::filesystem::temp_directory_path() / "yams_test_fts5_catch2";
        std::filesystem::create_directories(tempDir);
        auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
        dbPath = tempDir / (std::string("yams_fts5_test_") + std::to_string(ts) + ".db");
        std::filesystem::remove(dbPath); // Clean if exists
    }

    ~FTS5Fixture() {
        std::error_code ec;
        std::filesystem::remove(dbPath, ec);
        std::filesystem::remove_all(dbPath.parent_path(), ec);
    }

    std::filesystem::path dbPath;
};
} // namespace

TEST_CASE("FTS5: check FTS5 support", "[unit][metadata][fts5]") {
    FTS5Fixture fix;

    Database db;
    auto openResult = db.open(fix.dbPath.string(), ConnectionMode::Create);
    REQUIRE(openResult.has_value());

    auto result = db.hasFTS5();
    REQUIRE(result.has_value());

    bool hasFTS5 = result.value();

    if (hasFTS5) {
        spdlog::info("✓ SQLite FTS5 support is ENABLED");

        SECTION("FTS5 table creation and search") {
            auto createResult =
                db.execute("CREATE VIRTUAL TABLE test_fts USING fts5(title, content)");
            REQUIRE(createResult.has_value());

            // Insert test data
            auto stmtResult = db.prepare("INSERT INTO test_fts(title, content) VALUES (?, ?)");
            REQUIRE(stmtResult.has_value());
            Statement stmt = std::move(stmtResult).value();
            stmt.bind(1, "Test Document");
            stmt.bind(2, "This is a test document for FTS5 search functionality");
            auto insertResult = stmt.execute();
            REQUIRE(insertResult.has_value());

            // Test FTS5 search
            auto searchStmt = db.prepare("SELECT * FROM test_fts WHERE test_fts MATCH 'test'");
            REQUIRE(searchStmt.has_value());
            Statement searchStmtRef = std::move(searchStmt).value();
            auto stepResult = searchStmtRef.step();
            REQUIRE(stepResult.has_value());
            bool hasResults = stepResult.value();
            CHECK(hasResults); // "FTS5 search returned no results"

            spdlog::info("✓ FTS5 table creation and search work correctly");
        }
    } else {
        spdlog::warn("⚠ SQLite FTS5 support is NOT available");
        SKIP("FTS5 not available in this SQLite build");
    }
}

TEST_CASE("FTS5: fallback behavior without FTS5", "[unit][metadata][fts5]") {
    FTS5Fixture fix;

    Database db;
    auto openResult = db.open(fix.dbPath.string(), ConnectionMode::Create);
    REQUIRE(openResult.has_value());

    auto result = db.hasFTS5();
    REQUIRE(result.has_value());

    if (!result.value()) {
        spdlog::info("Testing fallback behavior without FTS5");

        SECTION("Regular tables work when FTS5 unavailable") {
            auto createResult = db.execute(
                "CREATE TABLE documents (id INTEGER PRIMARY KEY, title TEXT, content TEXT)");
            REQUIRE(createResult.has_value());

            auto insertStmt = db.prepare("INSERT INTO documents(title, content) VALUES (?, ?)");
            REQUIRE(insertStmt.has_value());
            Statement stmt2 = std::move(insertStmt).value();
            stmt2.bind(1, "Test Document");
            stmt2.bind(2, "This is a test document for fallback search");
            auto insertResult = stmt2.execute();
            REQUIRE(insertResult.has_value());

            // Use LIKE for basic search
            auto searchStmt = db.prepare("SELECT * FROM documents WHERE content LIKE '%test%'");
            REQUIRE(searchStmt.has_value());
            Statement searchStmtRef2 = std::move(searchStmt).value();
            auto stepResult = searchStmtRef2.step();
            REQUIRE(stepResult.has_value());
            bool hasResults = stepResult.value();
            CHECK(hasResults); // "Fallback LIKE search returned no results"

            spdlog::info("✓ Fallback search using LIKE works correctly");
        }
    } else {
        SKIP("FTS5 is available, skipping fallback test");
    }
}

TEST_CASE("FTS5: report FTS5 configuration", "[unit][metadata][fts5]") {
    FTS5Fixture fix;

    Database db;
    auto openResult = db.open(fix.dbPath.string(), ConnectionMode::Create);
    REQUIRE(openResult.has_value());

    // Get SQLite version
    auto versionStmt = db.prepare("SELECT sqlite_version()");
    if (versionStmt.has_value()) {
        Statement stmt = std::move(versionStmt).value();
        auto stepResult = stmt.step();
        if (stepResult.has_value() && stepResult.value()) {
            auto version = stmt.getString(0);
            spdlog::info("SQLite Version: {}", version);
        }
    }

    // Check compile options
    auto compileStmt =
        db.prepare("SELECT name FROM pragma_compile_options WHERE name LIKE '%FTS%'");

    if (compileStmt.has_value()) {
        spdlog::info("FTS-related compile options:");
        Statement compileStmtRef = std::move(compileStmt).value();
        auto stepResult = compileStmtRef.step();
        while (stepResult.has_value() && stepResult.value()) {
            auto optionName = compileStmtRef.getString(0);
            spdlog::info("  - {}", optionName);
            stepResult = compileStmtRef.step();
        }
    }

    // Check if FTS5 is enabled
    auto fts5Result = db.hasFTS5();
    REQUIRE(fts5Result.has_value());

    spdlog::info("========================================");
    spdlog::info("FTS5 Support Status: {}", fts5Result.value() ? "ENABLED ✓" : "DISABLED ✗");
    spdlog::info("========================================");

    // This test always passes - it's for reporting only
    CHECK(true);
}
