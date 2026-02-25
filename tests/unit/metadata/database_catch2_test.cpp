// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2025 YAMS Contributors

#include <chrono>
#include <filesystem>
#include <thread>

#include <catch2/catch_test_macros.hpp>

#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/migration.h>

using namespace yams;
using namespace yams::metadata;

namespace {
struct DatabaseFixture {
    DatabaseFixture() {
        const char* t = std::getenv("YAMS_TEST_TMPDIR");
        auto base = (t && *t) ? std::filesystem::path(t) : std::filesystem::temp_directory_path();
        std::error_code ec;
        std::filesystem::create_directories(base, ec);
        auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
        dbPath_ = base / (std::string("database_catch2_test_") + std::to_string(ts) + ".db");
        std::filesystem::remove(dbPath_, ec);
    }

    ~DatabaseFixture() {
        std::error_code ec;
        std::filesystem::remove(dbPath_, ec);
    }

    std::filesystem::path dbPath_;
};
} // namespace

TEST_CASE("Database: open and close", "[unit][metadata][database]") {
    DatabaseFixture fix;
    Database db;

    REQUIRE_FALSE(db.isOpen());

    SECTION("Open creates database file") {
        auto result = db.open(fix.dbPath_.string(), ConnectionMode::Create);
        REQUIRE(result.has_value());
        REQUIRE(db.isOpen());

        SECTION("Close marks database as closed") {
            db.close();
            CHECK_FALSE(db.isOpen());
        }
    }
}

TEST_CASE("Database: verify FTS5 support", "[unit][metadata][database]") {
    DatabaseFixture fix;
    Database db;
    auto openResult = db.open(fix.dbPath_.string(), ConnectionMode::Create);
    REQUIRE(openResult.has_value());

    auto fts5Result = db.hasFTS5();
    REQUIRE(fts5Result.has_value());
    CHECK(fts5Result.value()); // "FTS5 support is required"
}

TEST_CASE("Database: create table", "[unit][metadata][database]") {
    DatabaseFixture fix;
    Database db;
    auto openResult = db.open(fix.dbPath_.string(), ConnectionMode::Create);
    REQUIRE(openResult.has_value());

    auto result = db.execute(R"(
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            value REAL
        )
    )");
    REQUIRE(result.has_value());

    auto exists = db.tableExists("test_table");
    REQUIRE(exists.has_value());
    CHECK(exists.value());
}

TEST_CASE("Database: prepared statements", "[unit][metadata][database]") {
    DatabaseFixture fix;
    Database db;
    auto openResult = db.open(fix.dbPath_.string(), ConnectionMode::Create);
    REQUIRE(openResult.has_value());

    // Create table
    db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)");

    SECTION("Insert and query data") {
        auto insertStmtResult = db.prepare("INSERT INTO test (name, value) VALUES (?, ?)");
        REQUIRE(insertStmtResult.has_value());
        Statement insertStmt = std::move(insertStmtResult).value();

        auto bindResult = insertStmt.bindAll("Test1", 42);
        REQUIRE(bindResult.has_value());

        auto execResult = insertStmt.execute();
        REQUIRE(execResult.has_value());

        CHECK(db.lastInsertRowId() == 1);
        CHECK(db.changes() == 1);

        SECTION("Query the inserted data") {
            auto selectStmtResult = db.prepare("SELECT * FROM test WHERE id = ?");
            REQUIRE(selectStmtResult.has_value());
            Statement selectStmt = std::move(selectStmtResult).value();

            selectStmt.bind(1, 1);

            auto stepResult = selectStmt.step();
            REQUIRE(stepResult.has_value());
            CHECK(stepResult.value());

            CHECK(selectStmt.getInt(0) == 1);
            CHECK(selectStmt.getString(1) == "Test1");
            CHECK(selectStmt.getInt(2) == 42);
        }
    }
}

TEST_CASE("Database: transactions", "[unit][metadata][database]") {
    DatabaseFixture fix;
    Database db;
    auto openResult = db.open(fix.dbPath_.string(), ConnectionMode::Create);
    REQUIRE(openResult.has_value());

    db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER)");

    SECTION("Successful transaction") {
        auto beginResult = db.beginTransaction();
        REQUIRE(beginResult.has_value());

        db.execute("INSERT INTO test (value) VALUES (1)");
        db.execute("INSERT INTO test (value) VALUES (2)");

        auto commitResult = db.commit();
        REQUIRE(commitResult.has_value());

        auto stmtResult = db.prepare("SELECT COUNT(*) FROM test");
        REQUIRE(stmtResult.has_value());
        Statement stmt = std::move(stmtResult).value();
        stmt.step();
        CHECK(stmt.getInt(0) == 2);
    }

    SECTION("Rollback transaction") {
        // Insert baseline data
        db.execute("INSERT INTO test (value) VALUES (1)");
        db.execute("INSERT INTO test (value) VALUES (2)");

        auto beginResult = db.beginTransaction();
        REQUIRE(beginResult.has_value());

        db.execute("INSERT INTO test (value) VALUES (3)");

        auto rollbackResult = db.rollback();
        REQUIRE(rollbackResult.has_value());

        auto stmtResult = db.prepare("SELECT COUNT(*) FROM test");
        REQUIRE(stmtResult.has_value());
        Statement stmt = std::move(stmtResult).value();
        stmt.step();
        CHECK(stmt.getInt(0) == 2); // Still 2, not 3
    }
}

TEST_CASE("Database: WAL mode", "[unit][metadata][database]") {
    DatabaseFixture fix;
    Database db;
    auto openResult = db.open(fix.dbPath_.string(), ConnectionMode::Create);
    REQUIRE(openResult.has_value());

    auto walResult = db.enableWAL();
    REQUIRE(walResult.has_value());

    // Verify WAL mode is enabled
    auto stmtResult = db.prepare("PRAGMA journal_mode");
    REQUIRE(stmtResult.has_value());
    Statement stmt = std::move(stmtResult).value();

    auto stepResult = stmt.step();
    REQUIRE(stepResult.has_value());
    CHECK(stepResult.value());

    CHECK(stmt.getString(0) == "wal");
}

TEST_CASE("Database: QueryBuilder", "[unit][metadata][database]") {
    QueryBuilder qb;

    SECTION("SELECT query") {
        auto sql = qb.select({"id", "name", "value"})
                       .from("users")
                       .where("active = 1")
                       .andWhere("age > ?")
                       .orderBy("name", true)
                       .limit(10)
                       .build();

        CHECK(sql == "SELECT id, name, value FROM users WHERE active = 1 AND age > ? ORDER BY "
                     "name ASC LIMIT 10");
    }

    SECTION("INSERT query") {
        qb.reset();
        auto sql = qb.insertInto("users").values({"name", "email", "age"}).build();

        CHECK(sql == "INSERT INTO users (name, email, age) VALUES (?, ?, ?)");
    }

    SECTION("UPDATE query") {
        qb.reset();
        auto sql = qb.update("users")
                       .set("name", "?")
                       .set("updated_at", "strftime('%s', 'now')")
                       .where("id = ?")
                       .build();

        CHECK(sql == "UPDATE users SET name = ?, updated_at = strftime('%s', 'now') WHERE id = ?");
    }
}

TEST_CASE("Database: ConnectionPool", "[unit][metadata][database]") {
    DatabaseFixture fix;

    ConnectionPoolConfig config;
    config.minConnections = 2;
    config.maxConnections = 5;
    config.enableWAL = true;

    ConnectionPool pool(fix.dbPath_.string(), config);

    auto initResult = pool.initialize();
    REQUIRE(initResult.has_value());

    SECTION("Initial stats") {
        auto stats = pool.getStats();
        CHECK(stats.totalConnections == 2);
        CHECK(stats.availableConnections == 2);
        CHECK(stats.activeConnections == 0);
    }

    SECTION("Acquire and release connection") {
        {
            auto connResult = pool.acquire();
            REQUIRE(connResult.has_value());

            auto conn = std::move(connResult).value();
            REQUIRE(conn->isValid());

            // Use connection
            auto result = (*conn)->execute("CREATE TABLE test (id INTEGER)");
            REQUIRE(result.has_value());

            auto stats = pool.getStats();
            CHECK(stats.activeConnections == 1);
            // Note: availableConnections may be >= 1 due to background maintenance thread
            CHECK(stats.availableConnections >= 1);
        }

        // Connection returned to pool
        auto stats = pool.getStats();
        CHECK(stats.activeConnections == 0);
        // Note: availableConnections may be >= 2 due to background maintenance thread
        CHECK(stats.availableConnections >= 2);
    }

    pool.shutdown();
}

TEST_CASE("Database: Migrations", "[unit][metadata][database]") {
    DatabaseFixture fix;
    Database db;
    auto openResult = db.open(fix.dbPath_.string(), ConnectionMode::Create);
    REQUIRE(openResult.has_value());

    MigrationManager mm(db);

    auto initResult = mm.initialize();
    REQUIRE(initResult.has_value());

    // Register migrations
    mm.registerMigrations(YamsMetadataMigrations::getAllMigrations());

    SECTION("Check current version before migration") {
        auto currentVersion = mm.getCurrentVersion();
        REQUIRE(currentVersion.has_value());
        CHECK(currentVersion.value() == 0);
    }

    SECTION("Check if migration needed") {
        auto needsMigration = mm.needsMigration();
        REQUIRE(needsMigration.has_value());
        CHECK(needsMigration.value());
    }

    SECTION("Apply migrations") {
        auto migrateResult = mm.migrate();
        REQUIRE(migrateResult.has_value());

        // Verify final version
        auto currentVersion = mm.getCurrentVersion();
        REQUIRE(currentVersion.has_value());
        CHECK(currentVersion.value() ==
              30); // Latest schema version (keep in sync with migration.cpp)

        // Verify tables exist
        auto docExists = db.tableExists("documents");
        REQUIRE(docExists.has_value());
        CHECK(docExists.value());

        auto metaExists = db.tableExists("metadata");
        REQUIRE(metaExists.has_value());
        CHECK(metaExists.value());

        // Verify FTS table exists if FTS5 is available
        auto fts5Available = db.hasFTS5();
        if (fts5Available && fts5Available.value()) {
            auto ftsExists = db.tableExists("documents_fts");
            REQUIRE(ftsExists.has_value());
            CHECK(ftsExists.value());
        }
    }
}

TEST_CASE("Database: Concurrent access", "[unit][metadata][database][.slow]") {
    DatabaseFixture fix;

    ConnectionPool pool(fix.dbPath_.string());
    auto initResult = pool.initialize();
    REQUIRE(initResult.has_value());

    // Create table
    pool.withConnection([](Database& db) {
        return db.execute("CREATE TABLE counter (id INTEGER PRIMARY KEY, value INTEGER)");
    });

    pool.withConnection(
        [](Database& db) { return db.execute("INSERT INTO counter (id, value) VALUES (1, 0)"); });

    const int numThreads = 10;
    const int incrementsPerThread = 100;
    std::vector<std::thread> threads;

    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back([&pool, incrementsPerThread]() {
            for (int j = 0; j < incrementsPerThread; ++j) {
                pool.withConnection([](Database& db) -> Result<void> {
                    return db.transaction([&db]() -> Result<void> {
                        auto stmtResult =
                            db.prepare("UPDATE counter SET value = value + 1 WHERE id = 1");
                        if (!stmtResult)
                            return stmtResult.error();
                        Statement stmt = std::move(stmtResult).value();
                        return stmt.execute();
                    });
                });
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // Check final value
    auto result = pool.withConnection([](Database& db) -> Result<int> {
        auto stmtResult = db.prepare("SELECT value FROM counter WHERE id = 1");
        if (!stmtResult)
            return stmtResult.error();
        Statement stmt = std::move(stmtResult).value();

        auto stepResult = stmt.step();
        if (!stepResult)
            return stepResult.error();

        return stmt.getInt(0);
    });

    REQUIRE(result.has_value());
    CHECK(result.value() == numThreads * incrementsPerThread);
}

TEST_CASE("Database: Statement cache", "[unit][metadata][database]") {
    DatabaseFixture fix;
    Database db;

    auto openResult = db.open(fix.dbPath_.string(), ConnectionMode::Create);
    REQUIRE(openResult.has_value());

    // Create a test table
    auto createResult = db.execute("CREATE TABLE test_cache (id INTEGER PRIMARY KEY, value TEXT)");
    REQUIRE(createResult.has_value());

    SECTION("prepareCached returns working statement") {
        auto stmtResult = db.prepareCached("INSERT INTO test_cache (id, value) VALUES (?, ?)");
        REQUIRE(stmtResult.has_value());

        auto& stmt = *stmtResult.value();
        auto bindResult = stmt.bindAll(1, "hello");
        REQUIRE(bindResult.has_value());

        auto execResult = stmt.execute();
        REQUIRE(execResult.has_value());
    }

    SECTION("prepareCached reuses statements") {
        const std::string sql = "SELECT id, value FROM test_cache WHERE id = ?";

        // First call - cache miss
        {
            auto stmtResult = db.prepareCached(sql);
            REQUIRE(stmtResult.has_value());
        } // Statement returned to cache

        auto stats1 = db.getStatementCacheStats();
        CHECK(stats1.misses == 1);
        CHECK(stats1.hits == 0);
        CHECK(stats1.currentSize == 1);

        // Second call - cache hit
        {
            auto stmtResult = db.prepareCached(sql);
            REQUIRE(stmtResult.has_value());
        }

        auto stats2 = db.getStatementCacheStats();
        CHECK(stats2.misses == 1);
        CHECK(stats2.hits == 1);
        CHECK(stats2.currentSize == 1);

        // Third call - another cache hit
        {
            auto stmtResult = db.prepareCached(sql);
            REQUIRE(stmtResult.has_value());
        }

        auto stats3 = db.getStatementCacheStats();
        CHECK(stats3.hits == 2);
    }

    SECTION("clearStatementCache clears cache and stats") {
        auto stmtResult = db.prepareCached("SELECT 1");
        REQUIRE(stmtResult.has_value());
        stmtResult.value().release(); // Don't return to cache

        // Use it again to populate cache
        stmtResult = db.prepareCached("SELECT 1");
        REQUIRE(stmtResult.has_value());

        db.clearStatementCache();

        auto stats = db.getStatementCacheStats();
        CHECK(stats.hits == 0);
        CHECK(stats.misses == 0);
        CHECK(stats.currentSize == 0);
    }

    SECTION("CachedStatement can be released") {
        auto stmtResult = db.prepareCached("SELECT 1");
        REQUIRE(stmtResult.has_value());

        Statement stmt = stmtResult.value().release();
        auto stepResult = stmt.step();
        REQUIRE(stepResult.has_value());
        CHECK(stepResult.value() == true);
        CHECK(stmt.getInt(0) == 1);

        // Statement won't be returned to cache since it was released
    }
}
