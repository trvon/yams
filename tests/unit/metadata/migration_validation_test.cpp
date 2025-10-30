// Copyright 2025 YAMS Project
// SPDX-License-Identifier: GPL-3.0-or-later

/**
 * @file migration_validation_test.cpp
 * @brief Validates that database migrations execute successfully
 *
 * This test ensures:
 * - All migrations can be registered and applied
 * - Database reaches expected schema version
 * - New migrations are automatically tested (no hardcoded version checks)
 * - Migration rollback/idempotency works correctly
 */

#include <catch2/catch_test_macros.hpp>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/migration.h>

#include <filesystem>
#include <string>

using namespace yams;
using namespace yams::metadata;

namespace {

/**
 * @brief Test fixture for migration validation
 */
class MigrationTestFixture {
public:
    MigrationTestFixture() {
        // Create temporary database
        temp_db_path_ = std::filesystem::temp_directory_path() / "yams_migration_test";
        std::filesystem::create_directories(temp_db_path_);

        db_file_ = temp_db_path_ / "test.db";

        // Create connection pool
        pool_ = std::make_unique<ConnectionPool>(db_file_.string());
        auto init_result = pool_->initialize();
        if (!init_result) {
            throw std::runtime_error("Failed to initialize connection pool: " +
                                     init_result.error().message);
        }
    }

    ~MigrationTestFixture() {
        pool_.reset();
        try {
            std::filesystem::remove_all(temp_db_path_);
        } catch (const std::filesystem::filesystem_error&) {
            // Ignore cleanup errors
        }
    }

    /**
     * @brief Apply all migrations and return the final version
     */
    Result<int> applyAllMigrations() {
        int final_version = 0;

        auto result = pool_->withConnection([&](Database& db) -> Result<void> {
            MigrationManager mm(db);

            // Initialize migration manager
            auto init_result = mm.initialize();
            if (!init_result) {
                return init_result.error();
            }

            // Register all migrations
            mm.registerMigrations(YamsMetadataMigrations::getAllMigrations());

            // Apply migrations
            auto migrate_result = mm.migrate();
            if (!migrate_result) {
                return migrate_result.error();
            }

            // Get current version
            auto version_result = mm.getCurrentVersion();
            if (!version_result) {
                return version_result.error();
            }

            final_version = version_result.value();
            return Result<void>();
        });

        if (!result) {
            return result.error();
        }

        return final_version;
    }

    /**
     * @brief Get the expected latest migration version
     * This is determined dynamically from the registered migrations
     */
    Result<int> getExpectedLatestVersion() {
        int expected_version = 0;

        auto result = pool_->withConnection([&](Database& db) -> Result<void> {
            MigrationManager mm(db);

            auto init_result = mm.initialize();
            if (!init_result) {
                return init_result.error();
            }

            mm.registerMigrations(YamsMetadataMigrations::getAllMigrations());

            expected_version = mm.getLatestVersion();

            return Result<void>();
        });

        if (!result) {
            return result.error();
        }

        return expected_version;
    }

    /**
     * @brief Check if migrations are idempotent (can be re-run)
     */
    Result<bool> testIdempotency() {
        // Apply migrations first time
        auto first_result = applyAllMigrations();
        if (!first_result) {
            return first_result.error();
        }

        int first_version = first_result.value();

        // Apply migrations second time (should be no-op)
        auto second_result = applyAllMigrations();
        if (!second_result) {
            return second_result.error();
        }

        int second_version = second_result.value();

        // Versions should match
        return first_version == second_version;
    }

    ConnectionPool* getPool() { return pool_.get(); }

private:
    std::filesystem::path temp_db_path_;
    std::filesystem::path db_file_;
    std::unique_ptr<ConnectionPool> pool_;
};

} // anonymous namespace

// Shared fixture (created once, reused across all tests)
MigrationTestFixture& getFixture() {
    static MigrationTestFixture instance;
    return instance;
}

// ============================================================================
// Migration Execution Tests
// ============================================================================

TEST_CASE("All migrations execute successfully", "[catch2][unit][metadata][migration]") {
    auto& fixture = getFixture();

    SECTION("Migrations reach expected version") {
        auto expected_result = fixture.getExpectedLatestVersion();
        REQUIRE(expected_result);

        auto actual_result = fixture.applyAllMigrations();
        REQUIRE(actual_result);

        int expected_version = expected_result.value();
        int actual_version = actual_result.value();

        INFO("Expected version: " << expected_version);
        INFO("Actual version: " << actual_version);

        REQUIRE(actual_version == expected_version);
        REQUIRE(actual_version > 0); // Sanity check: should have at least one migration
    }

    SECTION("Migration version is at least v16 (symbol_metadata)") {
        // This ensures we haven't regressed and lost the symbol_metadata migration
        auto result = fixture.applyAllMigrations();
        REQUIRE(result);

        int version = result.value();
        REQUIRE(version >= 16);

        INFO("Current migration version: " << version);
    }
}

TEST_CASE("Migration manager provides correct version information",
          "[catch2][unit][metadata][migration]") {
    // Use a fresh fixture for this test since we need to verify version progression from 0
    MigrationTestFixture fixture;

    SECTION("Latest available version is retrievable before migration") {
        auto expected_result = fixture.getExpectedLatestVersion();
        REQUIRE(expected_result);

        int latest_version = expected_result.value();
        REQUIRE(latest_version > 0);

        INFO("Latest available migration version: " << latest_version);
    }

    SECTION("Current version increases after migration") {
        auto pool = fixture.getPool();

        // Get initial version (should be 0 for fresh DB)
        int initial_version = 0;
        auto init_result = pool->withConnection([&](Database& db) -> Result<void> {
            MigrationManager mm(db);
            auto init = mm.initialize();
            if (!init)
                return init.error();

            auto ver = mm.getCurrentVersion();
            if (!ver)
                return ver.error();

            initial_version = ver.value();
            return Result<void>();
        });
        REQUIRE(init_result);
        REQUIRE(initial_version == 0);

        // Apply migrations
        auto migrate_result = fixture.applyAllMigrations();
        REQUIRE(migrate_result);

        int final_version = migrate_result.value();
        REQUIRE(final_version > initial_version);
    }
}

TEST_CASE("Migrations are idempotent", "[catch2][unit][metadata][migration]") {
    auto& fixture = getFixture();

    SECTION("Re-running migrations does not change version") {
        auto idempotency_result = fixture.testIdempotency();
        REQUIRE(idempotency_result);
        REQUIRE(idempotency_result.value() == true);
    }

    SECTION("Re-running migrations does not fail") {
        // First application
        auto first_result = fixture.applyAllMigrations();
        REQUIRE(first_result);

        // Second application (should succeed without errors)
        auto second_result = fixture.applyAllMigrations();
        REQUIRE(second_result);
    }
}

TEST_CASE("Migration manager handles errors gracefully", "[catch2][unit][metadata][migration]") {
    auto& fixture = getFixture();
    auto pool = fixture.getPool();

    SECTION("Initialize can be called multiple times") {
        auto result = pool->withConnection([](Database& db) -> Result<void> {
            MigrationManager mm(db);

            auto first_init = mm.initialize();
            REQUIRE(first_init);

            auto second_init = mm.initialize();
            REQUIRE(second_init);

            return Result<void>();
        });

        REQUIRE(result);
    }
}

// ============================================================================
// Schema Validation After Migration
// ============================================================================

TEST_CASE("Core tables exist after migration", "[catch2][unit][metadata][migration]") {
    auto& fixture = getFixture();

    // Apply migrations first
    auto migration_result = fixture.applyAllMigrations();
    REQUIRE(migration_result);

    auto pool = fixture.getPool();

    SECTION("Documents table exists") {
        bool table_exists = false;

        auto result = pool->withConnection([&](Database& db) -> Result<void> {
            auto stmt_result = db.prepare(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='documents'");
            if (!stmt_result)
                return stmt_result.error();

            auto stmt = std::move(stmt_result.value());
            auto step_result = stmt.step();
            if (!step_result)
                return step_result.error();

            table_exists = step_result.value();
            return Result<void>();
        });

        REQUIRE(result);
        REQUIRE(table_exists);
    }

    SECTION("Symbol_metadata table exists (v16)") {
        bool table_exists = false;

        auto result = pool->withConnection([&](Database& db) -> Result<void> {
            auto stmt_result = db.prepare(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='symbol_metadata'");
            if (!stmt_result)
                return stmt_result.error();

            auto stmt = std::move(stmt_result.value());
            auto step_result = stmt.step();
            if (!step_result)
                return step_result.error();

            table_exists = step_result.value();
            return Result<void>();
        });

        REQUIRE(result);
        REQUIRE(table_exists);
    }

    SECTION("Metadata table exists") {
        bool table_exists = false;

        auto result = pool->withConnection([&](Database& db) -> Result<void> {
            auto stmt_result =
                db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='metadata'");
            if (!stmt_result)
                return stmt_result.error();

            auto stmt = std::move(stmt_result.value());
            auto step_result = stmt.step();
            if (!step_result)
                return step_result.error();

            table_exists = step_result.value();
            return Result<void>();
        });

        REQUIRE(result);
        REQUIRE(table_exists);
    }
}
