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

#include <chrono>
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
        // Create unique temporary database path to avoid conflicts between sections
        auto timestamp = std::chrono::steady_clock::now().time_since_epoch().count();
        temp_db_path_ = std::filesystem::temp_directory_path() /
                        ("yams_migration_test_" + std::to_string(timestamp));
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

    SECTION("Migration version is at least v34 (semantic neighbor adjacency order index)") {
        // Floor checks that all expected schema is present:
        //   v28: feedback_events
        //   v31: semantic_duplicate_groups
        //   v32: idx_kg_edges_uq (audit fix B — addEdgesUnique INSERT OR IGNORE)
        //   v33: idx_metadata_doc_key + idx_kg_doc_entities_doc_extractor
        //        (audit fixes M2 + M4)
        //   v34: idx_kg_edges_src_rel_weight_time for graph expansion reads
        auto result = fixture.applyAllMigrations();
        REQUIRE(result);

        int version = result.value();
        REQUIRE(version >= 34);

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

    SECTION("Symbol_metadata table is dropped (v40)") {
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
        REQUIRE_FALSE(table_exists);
    }

    SECTION("Document_symbol_extraction_state table is dropped (v40)") {
        bool table_exists = false;

        auto result = pool->withConnection([&](Database& db) -> Result<void> {
            auto stmt_result = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND "
                                          "name='document_symbol_extraction_state'");
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
        REQUIRE_FALSE(table_exists);
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

TEST_CASE("Migration v40 drops symbol extraction subsystem and prunes graph",
          "[catch2][unit][metadata][migration][v40]") {
    MigrationTestFixture fixture;
    auto pool = fixture.getPool();

    REQUIRE(
        pool->withConnection([](Database& db) -> Result<void> {
                MigrationManager mm(db);
                auto initRes = mm.initialize();
                if (!initRes)
                    return initRes.error();
                mm.registerMigrations(YamsMetadataMigrations::getAllMigrations());

                // Step 1: Migrate up to v39
                auto mig39 = mm.migrateTo(39);
                if (!mig39)
                    return mig39.error();

                // Step 2: Seed v39 schema with document, symbol tables, KG nodes, edges, aliases
                auto seedRes = db.execute(R"(
            INSERT INTO documents (id, file_path, file_name, file_size, sha256_hash)
            VALUES (1, '/src/main.cpp', 'main.cpp', 100, 'hash123');

            INSERT INTO symbol_metadata (
                document_hash, file_path, symbol_name, qualified_name, kind, start_line, end_line
            ) VALUES ('hash123', '/src/main.cpp', 'process', 'demo::process', 'function', 10, 20);

            INSERT INTO document_symbol_extraction_state (
                document_id, extractor_id, extracted_at, status, entity_count
            ) VALUES (1, 'symbol_extractor_v1', 12345, 'completed', 1);

            INSERT INTO kg_nodes (id, node_key, label, type, properties) VALUES
                (10, 'dir:/src', 'src', 'directory', '{}'),
                (11, 'path:file:/src/main.cpp', 'main.cpp', 'file', '{}'),
                (12, 'doc:hash123', 'doc-hash123', 'document', '{}'),
                (13, 'function:process@/src/main.cpp', 'process', 'function', '{}'),
                (14, 'function:process@/src/main.cpp@snap:hash123', 'process', 'function_version', '{}'),
                (15, 'symbol_ref:callee', 'callee', 'symbol_reference', '{}'),
                (16, 'topology:snapshot:latest', 'latest', 'topology_snapshot_pointer', '{"snapshot_id":"snap-curr"}'),
                (17, 'topology:snapshot:snap-curr', 'snap-curr', 'topology_snapshot', '{"cluster_count":5}'),
                (18, 'topology:snapshot:snap-old', 'snap-old', 'topology_snapshot', '{"cluster_count":3}'),
                (19, 'nl_entity:method:pytorch', 'pytorch', 'method', '{}');

            INSERT INTO kg_edges (src_node_id, dst_node_id, relation, weight) VALUES
                (10, 11, 'contains', 1.0),
                (11, 14, 'contains', 1.0),
                (14, 12, 'defined_in', 1.0),
                (14, 15, 'calls', 1.0);

            INSERT INTO kg_aliases (node_id, alias, source, confidence) VALUES
                (12, 'main_doc', 'doc_title', 1.0),
                (13, 'process_func', 'symbol_name', 1.0);
        )");
                if (!seedRes)
                    return seedRes.error();

                // Step 3: Apply migration v40
                auto mig40 = mm.migrateTo(40);
                if (!mig40)
                    return mig40.error();

                // Verify symbol tables are dropped
                auto tableExists = [&](const char* tableName) -> Result<bool> {
                    auto stmtR = db.prepare(
                        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?");
                    if (!stmtR)
                        return stmtR.error();
                    auto stmt = std::move(stmtR).value();
                    if (auto b = stmt.bind(1, tableName); !b)
                        return b.error();
                    if (auto s = stmt.step(); !s)
                        return s.error();
                    return stmt.getInt(0) > 0;
                };

                auto symMeta = tableExists("symbol_metadata");
                if (!symMeta || symMeta.value())
                    return Error{ErrorCode::InvalidData, "symbol_metadata still exists"};

                auto symState = tableExists("document_symbol_extraction_state");
                if (!symState || symState.value())
                    return Error{ErrorCode::InvalidData,
                                 "document_symbol_extraction_state still exists"};

                auto symFts = tableExists("symbol_metadata_fts");
                if (!symFts || symFts.value())
                    return Error{ErrorCode::InvalidData, "symbol_metadata_fts still exists"};

                // Verify symbol nodes are deleted
                auto countQuery = [&](const std::string& sql) -> Result<int> {
                    auto stmtR = db.prepare(sql);
                    if (!stmtR)
                        return stmtR.error();
                    auto stmt = std::move(stmtR).value();
                    if (auto s = stmt.step(); !s)
                        return s.error();
                    return stmt.getInt(0);
                };

                auto symNodeCount =
                    countQuery("SELECT COUNT(*) FROM kg_nodes WHERE type IN ('function', "
                               "'function_version', 'symbol_reference')");
                if (!symNodeCount || symNodeCount.value() != 0) {
                    return Error{ErrorCode::InvalidData, "Symbol nodes not cleaned"};
                }

                // Verify structural nodes remain (dir, file, doc)
                auto structNodeCount = countQuery("SELECT COUNT(*) FROM kg_nodes WHERE type IN "
                                                  "('directory', 'file', 'document')");
                if (!structNodeCount || structNodeCount.value() != 3) {
                    return Error{ErrorCode::InvalidData, "Structural nodes lost"};
                }

                // Verify nl_entity nodes are preserved even if type matches a symbol type
                auto nlNodeCount = countQuery(
                    "SELECT COUNT(*) FROM kg_nodes WHERE node_key = 'nl_entity:method:pytorch'");
                if (!nlNodeCount || nlNodeCount.value() != 1) {
                    return Error{ErrorCode::InvalidData,
                                 "nl_entity node did not survive migration 40"};
                }

                // Verify AST edges are cleaned, but directory->file 'contains' remains
                auto edgesRemaining = countQuery("SELECT COUNT(*) FROM kg_edges");
                if (!edgesRemaining || edgesRemaining.value() != 1) {
                    return Error{ErrorCode::InvalidData, "Expected 1 remaining edge (dir->file)"};
                }

                auto dirFileEdge = countQuery("SELECT COUNT(*) FROM kg_edges WHERE src_node_id = "
                                              "10 AND dst_node_id = 11 AND relation = 'contains'");
                if (!dirFileEdge || dirFileEdge.value() != 1) {
                    return Error{ErrorCode::InvalidData, "dir->file contains edge missing"};
                }

                // Verify symbol aliases cleaned, doc alias preserved
                auto aliasCount = countQuery("SELECT COUNT(*) FROM kg_aliases");
                if (!aliasCount || aliasCount.value() != 1) {
                    return Error{ErrorCode::InvalidData, "Expected 1 remaining alias"};
                }

                // Verify historical topology snapshot pruned, current preserved
                auto oldSnap = countQuery(
                    "SELECT COUNT(*) FROM kg_nodes WHERE node_key = 'topology:snapshot:snap-old'");
                if (!oldSnap || oldSnap.value() != 0) {
                    return Error{ErrorCode::InvalidData, "Historical topology snapshot not pruned"};
                }

                auto currSnap = countQuery(
                    "SELECT COUNT(*) FROM kg_nodes WHERE node_key = 'topology:snapshot:snap-curr'");
                if (!currSnap || currSnap.value() != 1) {
                    return Error{ErrorCode::InvalidData, "Current topology snapshot missing"};
                }

                auto latestPtr = countQuery(
                    "SELECT COUNT(*) FROM kg_nodes WHERE node_key = 'topology:snapshot:latest'");
                if (!latestPtr || latestPtr.value() != 1) {
                    return Error{ErrorCode::InvalidData,
                                 "Latest topology snapshot pointer missing"};
                }

                // Step 4: Test rollback to v39
                auto rollRes = mm.rollbackTo(39);
                if (!rollRes)
                    return rollRes.error();

                auto rolledSymMeta = tableExists("symbol_metadata");
                if (!rolledSymMeta || !rolledSymMeta.value()) {
                    return Error{ErrorCode::InvalidData,
                                 "Rollback failed to recreate symbol_metadata"};
                }

                return Result<void>();
            })
            .has_value());
}

namespace {

Result<int> countRows(Database& db, const std::string& sql) {
    auto stmtR = db.prepare(sql);
    if (!stmtR)
        return stmtR.error();
    auto stmt = std::move(stmtR).value();
    if (auto s = stmt.step(); !s)
        return s.error();
    return stmt.getInt(0);
}

// Migrates to v39, seeds two topology snapshots plus an optional pointer, then applies v40 and
// returns how many snapshot nodes survived.
Result<int> snapshotsSurvivingV40(Database& db, const char* pointerSql) {
    MigrationManager mm(db);
    if (auto init = mm.initialize(); !init)
        return init.error();
    mm.registerMigrations(YamsMetadataMigrations::getAllMigrations());
    if (auto to39 = mm.migrateTo(39); !to39)
        return to39.error();
    auto seeded = db.execute(R"(
        INSERT INTO kg_nodes (node_key, label, type, properties) VALUES
            ('topology:snapshot:snap-a', 'snap-a', 'topology_snapshot', '{}'),
            ('topology:snapshot:snap-b', 'snap-b', 'topology_snapshot', '{}');
    )");
    if (!seeded)
        return seeded.error();
    if (pointerSql != nullptr) {
        if (auto pointer = db.execute(pointerSql); !pointer)
            return pointer.error();
    }
    if (auto to40 = mm.migrateTo(40); !to40)
        return to40.error();
    return countRows(db, "SELECT COUNT(*) FROM kg_nodes WHERE type = 'topology_snapshot'");
}

} // namespace

TEST_CASE("Migration v40 keeps topology snapshots when the latest pointer is unusable",
          "[catch2][unit][metadata][migration][v40]") {
    MigrationTestFixture fixture;
    auto pool = fixture.getPool();

    SECTION("No latest pointer") {
        auto survived =
            pool->withConnection([](Database& db) { return snapshotsSurvivingV40(db, nullptr); });
        REQUIRE(survived.has_value());
        CHECK(survived.value() == 2);
    }

    SECTION("Malformed pointer JSON") {
        auto survived = pool->withConnection([](Database& db) {
            return snapshotsSurvivingV40(
                db, "INSERT INTO kg_nodes (node_key, label, type, properties) VALUES "
                    "('topology:snapshot:latest', 'latest', 'topology_snapshot_pointer', "
                    "'{not json');");
        });
        REQUIRE(survived.has_value());
        CHECK(survived.value() == 2);
    }

    SECTION("Pointer names a snapshot that does not exist") {
        auto survived = pool->withConnection([](Database& db) {
            return snapshotsSurvivingV40(
                db, "INSERT INTO kg_nodes (node_key, label, type, properties) VALUES "
                    "('topology:snapshot:latest', 'latest', 'topology_snapshot_pointer', "
                    "'{\"snapshot_id\":\"snap-missing\"}');");
        });
        REQUIRE(survived.has_value());
        CHECK(survived.value() == 2);
    }

    SECTION("Valid pointer keeps only the named snapshot") {
        auto survived = pool->withConnection([](Database& db) {
            return snapshotsSurvivingV40(
                db, "INSERT INTO kg_nodes (node_key, label, type, properties) VALUES "
                    "('topology:snapshot:latest', 'latest', 'topology_snapshot_pointer', "
                    "'{\"snapshot_id\":\"snap-b\"}');");
        });
        REQUIRE(survived.has_value());
        CHECK(survived.value() == 1);
    }
}

TEST_CASE("Migration v41 bypasses only lowercase topology keys and restores counts on rollback",
          "[catch2][unit][metadata][migration][v41]") {
    MigrationTestFixture fixture;
    auto pool = fixture.getPool();

    auto run = pool->withConnection([](Database& db) -> Result<void> {
        MigrationManager mm(db);
        if (auto r = mm.initialize(); !r)
            return r.error();
        mm.registerMigrations(YamsMetadataMigrations::getAllMigrations());
        if (auto r = mm.migrateTo(40); !r)
            return r.error();
        if (auto r = db.execute(R"(
                INSERT INTO documents (id, file_path, file_name, file_size, sha256_hash)
                VALUES (1, '/a.md', 'a.md', 1, 'hash-a'), (2, '/b.md', 'b.md', 1, 'hash-b');
                INSERT INTO metadata (document_id, key, value, value_type) VALUES
                    (1, 'topology.cluster_id', 'c1', 'string'),
                    (2, 'topology.cluster_id', 'c1', 'string');
            )");
            !r)
            return r.error();
        if (auto r = mm.migrateTo(41); !r)
            return r.error();

        // Up: topology counts are gone.
        auto afterUp = countRows(db, "SELECT COUNT(*) FROM metadata_value_counts "
                                     "WHERE key = 'topology.cluster_id'");
        if (!afterUp)
            return afterUp.error();
        if (afterUp.value() != 0)
            return Error{ErrorCode::InvalidData, "topology counts not cleared"};

        // A user key that only differs in case is still counted.
        if (auto r = db.execute("INSERT INTO metadata (document_id, key, value, value_type) "
                                "VALUES (1, 'Topology.Owner', 'alice', 'string');");
            !r)
            return r.error();
        auto userKey = countRows(db, "SELECT COALESCE(SUM(count), 0) FROM metadata_value_counts "
                                     "WHERE key = 'Topology.Owner'");
        if (!userKey)
            return userKey.error();
        if (userKey.value() != 1)
            return Error{ErrorCode::InvalidData, "case-different user key was not counted"};

        // Down: counts are rebuilt from the remaining rows.
        if (auto r = mm.rollbackTo(40); !r)
            return r.error();
        auto afterDown = countRows(db, "SELECT COALESCE(SUM(count), 0) FROM metadata_value_counts "
                                       "WHERE key = 'topology.cluster_id' AND value = 'c1'");
        if (!afterDown)
            return afterDown.error();
        if (afterDown.value() != 2)
            return Error{ErrorCode::InvalidData, "rollback did not restore topology counts: " +
                                                     std::to_string(afterDown.value())};
        return Result<void>();
    });
    INFO((run ? std::string{} : run.error().message));
    REQUIRE(run.has_value());
}

// Migration 40 removes what the symbol extractor wrote. User-ingested graph data (MCP / IPC
// graph ingest accepts any node type and relation) must survive even when it reuses symbol
// type names ("class", "interface") or code relation names ("implements", "includes").
TEST_CASE("Migration v40 preserves user graph data that reuses symbol names",
          "[catch2][unit][metadata][migration][v40]") {
    MigrationTestFixture fixture;
    auto pool = fixture.getPool();
    auto run = pool->withConnection([](Database& db) -> Result<void> {
        MigrationManager mm(db);
        if (auto init = mm.initialize(); !init)
            return init.error();
        mm.registerMigrations(YamsMetadataMigrations::getAllMigrations());
        if (auto to39 = mm.migrateTo(39); !to39)
            return to39.error();
        auto seeded = db.execute(R"(
            INSERT INTO documents (id, file_path, file_name, file_size, sha256_hash)
            VALUES (1, '/src/a.h', 'a.h', 10, 'hash-a');

            INSERT INTO kg_nodes (id, node_key, label, type, properties) VALUES
                (30, 'class:ServiceA', 'ServiceA', 'class', '{"source":"mcp"}'),
                (31, 'interface:ApiB', 'ApiB', 'interface', '{"source":"mcp"}'),
                (32, 'path:file:/src/a.h', 'a.h', 'file', '{}'),
                (33, 'path:file:/src/b.h', 'b.h', 'file', '{}'),
                (34, 'class:Widget@/src/a.h', 'Widget', 'class',
                     '{"qualified_name":"ui::Widget","language":"cpp"}'),
                (35, 'custom:extracted-class', 'Extracted', 'class', '{}');

            INSERT INTO kg_edges (src_node_id, dst_node_id, relation, weight, properties) VALUES
                (30, 31, 'implements', 1.0, '{"source":"mcp"}'),
                (32, 33, 'includes', 1.0, '{"extractor":"symbol_extractor_v1"}'),
                (34, 31, 'implements', 1.0, '{}');

            INSERT INTO kg_doc_entities (document_id, entity_text, node_id, extractor) VALUES
                (1, 'Extracted', 35, 'symbol_extractor_v1');
        )");
        if (!seeded)
            return seeded.error();
        if (auto to40 = mm.migrateTo(40); !to40)
            return to40.error();

        auto expect = [&](const char* sql, int want, const char* what) -> Result<void> {
            auto got = countRows(db, sql);
            if (!got)
                return got.error();
            if (got.value() != want)
                return Error{ErrorCode::InvalidData,
                             std::string(what) + ": got " + std::to_string(got.value())};
            return Result<void>();
        };
        if (auto r = expect("SELECT COUNT(*) FROM kg_nodes WHERE id IN (30, 31)", 2,
                            "user-ingested class/interface nodes");
            !r)
            return r;
        if (auto r = expect("SELECT COUNT(*) FROM kg_edges WHERE src_node_id = 30 AND "
                            "dst_node_id = 31 AND relation = 'implements'",
                            1, "user-ingested implements edge");
            !r)
            return r;
        if (auto r = expect("SELECT COUNT(*) FROM kg_nodes WHERE id IN (34, 35)", 0,
                            "extractor symbol nodes (key shape / doc-entity provenance)");
            !r)
            return r;
        if (auto r = expect("SELECT COUNT(*) FROM kg_edges WHERE relation = 'includes'", 0,
                            "extractor-tagged includes edge");
            !r)
            return r;
        return expect("SELECT COUNT(*) FROM kg_edges", 1, "remaining edges");
    });
    INFO((run ? std::string{} : run.error().message));
    REQUIRE(run.has_value());
}
