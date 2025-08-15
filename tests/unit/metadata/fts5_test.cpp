#include <spdlog/spdlog.h>
#include <filesystem>
#include <gtest/gtest.h>
#include <yams/metadata/database.h>

using namespace yams::metadata;

class FTS5Test : public ::testing::Test {
protected:
    void SetUp() override {
        // Create temporary database
        auto tempDir = std::filesystem::temp_directory_path() / "yams_test_fts5";
        std::filesystem::create_directories(tempDir);
        dbPath = tempDir / "yams_fts5_test.db";
        std::filesystem::remove(dbPath); // Clean if exists
    }

    void TearDown() override {
        // Clean up
        std::filesystem::remove(dbPath);
        std::filesystem::remove_all(dbPath.parent_path());
    }

    std::filesystem::path dbPath;
};

TEST_F(FTS5Test, CheckFTS5Support) {
    // Create database
    Database db;
    auto openResult = db.open(dbPath.string());
    ASSERT_TRUE(openResult.has_value())
        << "Failed to open database: " << openResult.error().message;

    // Check if FTS5 is available
    auto result = db.hasFTS5();

    ASSERT_TRUE(result.has_value()) << "Failed to check FTS5 support: " << result.error().message;

    bool hasFTS5 = result.value();

    if (hasFTS5) {
        spdlog::info("✓ SQLite FTS5 support is ENABLED");

        // Try to create an FTS5 table
        auto createResult = db.execute("CREATE VIRTUAL TABLE test_fts USING fts5(title, content)");
        ASSERT_TRUE(createResult.has_value())
            << "Failed to create FTS5 table: " << createResult.error().message;

        // Insert test data
        // Note: Database.execute() doesn't support parameters - need to use prepare()
        auto stmtResult = db.prepare("INSERT INTO test_fts(title, content) VALUES (?, ?)");
        ASSERT_TRUE(stmtResult.has_value());
        Statement stmt = std::move(stmtResult).value();
        stmt.bind(1, "Test Document");
        stmt.bind(2, "This is a test document for FTS5 search functionality");
        auto insertResult = stmt.execute();
        ASSERT_TRUE(insertResult.has_value());

        // Test FTS5 search
        auto searchStmt = db.prepare("SELECT * FROM test_fts WHERE test_fts MATCH 'test'");
        ASSERT_TRUE(searchStmt.has_value());
        // Execute and check results
        Statement searchStmtRef = std::move(searchStmt).value();
        auto stepResult = searchStmtRef.step();
        ASSERT_TRUE(stepResult.has_value());
        bool hasResults = stepResult.value();
        ASSERT_TRUE(hasResults) << "FTS5 search returned no results";

        spdlog::info("✓ FTS5 table creation and search work correctly");
    } else {
        spdlog::warn("⚠ SQLite FTS5 support is NOT available");
        spdlog::warn("⚠ Full-text search functionality will be limited");
        spdlog::warn("⚠ To enable FTS5, recompile SQLite with -DSQLITE_ENABLE_FTS5");

        // This is not a failure - just a warning
        GTEST_SKIP() << "FTS5 not available in this SQLite build";
    }
}

TEST_F(FTS5Test, FallbackBehavior) {
    // Create database
    Database db;
    auto openResult = db.open(dbPath.string());
    ASSERT_TRUE(openResult.has_value())
        << "Failed to open database: " << openResult.error().message;

    // Check if FTS5 is available
    auto result = db.hasFTS5();
    ASSERT_TRUE(result.has_value());

    if (!result.value()) {
        // Test fallback behavior when FTS5 is not available
        spdlog::info("Testing fallback behavior without FTS5");

        // Should be able to create regular tables
        auto createResult =
            db.execute("CREATE TABLE documents (id INTEGER PRIMARY KEY, title TEXT, content TEXT)");
        ASSERT_TRUE(createResult.has_value())
            << "Failed to create regular table: " << createResult.error().message;

        // Should be able to use LIKE for basic text search
        auto insertStmt = db.prepare("INSERT INTO documents(title, content) VALUES (?, ?)");
        ASSERT_TRUE(insertStmt.has_value());
        Statement stmt2 = std::move(insertStmt).value();
        stmt2.bind(1, "Test Document");
        stmt2.bind(2, "This is a test document for fallback search");
        auto insertResult = stmt2.execute();
        ASSERT_TRUE(insertResult.has_value());

        // Use LIKE for basic search
        auto searchStmt = db.prepare("SELECT * FROM documents WHERE content LIKE '%test%'");
        ASSERT_TRUE(searchStmt.has_value());
        Statement searchStmtRef2 = std::move(searchStmt).value();
        auto stepResult = searchStmtRef2.step();
        ASSERT_TRUE(stepResult.has_value());
        bool hasResults = stepResult.value();
        ASSERT_TRUE(hasResults) << "Fallback LIKE search returned no results";

        spdlog::info("✓ Fallback search using LIKE works correctly");
    } else {
        GTEST_SKIP() << "FTS5 is available, skipping fallback test";
    }
}

TEST_F(FTS5Test, ReportFTS5Configuration) {
    Database db;
    auto openResult = db.open(dbPath.string());
    ASSERT_TRUE(openResult.has_value())
        << "Failed to open database: " << openResult.error().message;

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
    ASSERT_TRUE(fts5Result.has_value());

    spdlog::info("========================================");
    spdlog::info("FTS5 Support Status: {}", fts5Result.value() ? "ENABLED ✓" : "DISABLED ✗");
    spdlog::info("========================================");
}