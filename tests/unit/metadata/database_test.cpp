#include <gtest/gtest.h>
#include <yams/metadata/database.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/migration.h>
#include <filesystem>
#include <thread>

using namespace yams;
using namespace yams::metadata;

class DatabaseTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Use temporary database for tests
        dbPath_ = std::filesystem::temp_directory_path() / "kronos_test.db";
        std::filesystem::remove(dbPath_);
    }
    
    void TearDown() override {
        std::filesystem::remove(dbPath_);
    }
    
    std::filesystem::path dbPath_;
};

TEST_F(DatabaseTest, OpenClose) {
    Database db;
    ASSERT_FALSE(db.isOpen());
    
    auto result = db.open(dbPath_.string(), ConnectionMode::Create);
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(db.isOpen());
    
    db.close();
    ASSERT_FALSE(db.isOpen());
}

TEST_F(DatabaseTest, VerifyFTS5Support) {
    Database db;
    auto openResult = db.open(dbPath_.string(), ConnectionMode::Create);
    ASSERT_TRUE(openResult.has_value());
    
    auto fts5Result = db.hasFTS5();
    ASSERT_TRUE(fts5Result.has_value());
    EXPECT_TRUE(fts5Result.value()) << "FTS5 support is required";
}

TEST_F(DatabaseTest, CreateTable) {
    Database db;
    auto openResult = db.open(dbPath_.string(), ConnectionMode::Create);
    ASSERT_TRUE(openResult.has_value());
    
    auto result = db.execute(R"(
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            value REAL
        )
    )");
    ASSERT_TRUE(result.has_value());
    
    auto exists = db.tableExists("test_table");
    ASSERT_TRUE(exists.has_value());
    EXPECT_TRUE(exists.value());
}

TEST_F(DatabaseTest, PreparedStatements) {
    Database db;
    auto openResult = db.open(dbPath_.string(), ConnectionMode::Create);
    ASSERT_TRUE(openResult.has_value());
    
    // Create table
    db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)");
    
    // Insert data
    auto insertStmtResult = db.prepare("INSERT INTO test (name, value) VALUES (?, ?)");
    ASSERT_TRUE(insertStmtResult.has_value());
    Statement insertStmt = std::move(insertStmtResult).value();
    
    auto bindResult = insertStmt.bindAll("Test1", 42);
    ASSERT_TRUE(bindResult.has_value());
    
    auto execResult = insertStmt.execute();
    ASSERT_TRUE(execResult.has_value());
    
    EXPECT_EQ(db.lastInsertRowId(), 1);
    EXPECT_EQ(db.changes(), 1);
    
    // Query data
    auto selectStmtResult = db.prepare("SELECT * FROM test WHERE id = ?");
    ASSERT_TRUE(selectStmtResult.has_value());
    Statement selectStmt = std::move(selectStmtResult).value();
    
    selectStmt.bind(1, 1);
    
    auto stepResult = selectStmt.step();
    ASSERT_TRUE(stepResult.has_value());
    EXPECT_TRUE(stepResult.value());
    
    EXPECT_EQ(selectStmt.getInt(0), 1);
    EXPECT_EQ(selectStmt.getString(1), "Test1");
    EXPECT_EQ(selectStmt.getInt(2), 42);
}

TEST_F(DatabaseTest, Transactions) {
    Database db;
    auto openResult = db.open(dbPath_.string(), ConnectionMode::Create);
    ASSERT_TRUE(openResult.has_value());
    
    db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER)");
    
    // Successful transaction
    {
        auto beginResult = db.beginTransaction();
        ASSERT_TRUE(beginResult.has_value());
        
        db.execute("INSERT INTO test (value) VALUES (1)");
        db.execute("INSERT INTO test (value) VALUES (2)");
        
        auto commitResult = db.commit();
        ASSERT_TRUE(commitResult.has_value());
        
        auto stmtResult = db.prepare("SELECT COUNT(*) FROM test");
        ASSERT_TRUE(stmtResult.has_value());
        Statement stmt = std::move(stmtResult).value();
        stmt.step();
        EXPECT_EQ(stmt.getInt(0), 2);
    }
    
    // Rollback transaction
    {
        auto beginResult = db.beginTransaction();
        ASSERT_TRUE(beginResult.has_value());
        
        db.execute("INSERT INTO test (value) VALUES (3)");
        
        auto rollbackResult = db.rollback();
        ASSERT_TRUE(rollbackResult.has_value());
        
        auto stmtResult = db.prepare("SELECT COUNT(*) FROM test");
        ASSERT_TRUE(stmtResult.has_value());
        Statement stmt = std::move(stmtResult).value();
        stmt.step();
        EXPECT_EQ(stmt.getInt(0), 2); // Still 2, not 3
    }
}

TEST_F(DatabaseTest, WALMode) {
    Database db;
    auto openResult = db.open(dbPath_.string(), ConnectionMode::Create);
    ASSERT_TRUE(openResult.has_value());
    
    auto walResult = db.enableWAL();
    ASSERT_TRUE(walResult.has_value());
    
    // Verify WAL mode is enabled
    auto stmtResult = db.prepare("PRAGMA journal_mode");
    ASSERT_TRUE(stmtResult.has_value());
    Statement stmt = std::move(stmtResult).value();
    
    auto stepResult = stmt.step();
    ASSERT_TRUE(stepResult.has_value());
    EXPECT_TRUE(stepResult.value());
    
    EXPECT_EQ(stmt.getString(0), "wal");
}

TEST_F(DatabaseTest, QueryBuilder) {
    QueryBuilder qb;
    
    // SELECT query
    {
        auto sql = qb.select({"id", "name", "value"})
            .from("users")
            .where("active = 1")
            .andWhere("age > ?")
            .orderBy("name", true)
            .limit(10)
            .build();
        
        EXPECT_EQ(sql, "SELECT id, name, value FROM users WHERE active = 1 AND age > ? ORDER BY name ASC LIMIT 10");
    }
    
    // INSERT query
    {
        qb.reset();
        auto sql = qb.insertInto("users")
            .values({"name", "email", "age"})
            .build();
        
        EXPECT_EQ(sql, "INSERT INTO users (name, email, age) VALUES (?, ?, ?)");
    }
    
    // UPDATE query
    {
        qb.reset();
        auto sql = qb.update("users")
            .set("name", "?")
            .set("updated_at", "strftime('%s', 'now')")
            .where("id = ?")
            .build();
        
        EXPECT_EQ(sql, "UPDATE users SET name = ?, updated_at = strftime('%s', 'now') WHERE id = ?");
    }
}

TEST_F(DatabaseTest, ConnectionPool) {
    ConnectionPoolConfig config;
    config.minConnections = 2;
    config.maxConnections = 5;
    config.enableWAL = true;
    
    ConnectionPool pool(dbPath_.string(), config);
    
    auto initResult = pool.initialize();
    ASSERT_TRUE(initResult.has_value());
    
    // Get initial stats
    auto stats = pool.getStats();
    EXPECT_EQ(stats.totalConnections, 2);
    EXPECT_EQ(stats.availableConnections, 2);
    EXPECT_EQ(stats.activeConnections, 0);
    
    // Acquire connection
    {
        auto connResult = pool.acquire();
        ASSERT_TRUE(connResult.has_value());
        
        auto conn = std::move(connResult).value();
        ASSERT_TRUE(conn->isValid());
        
        // Use connection
        auto result = (*conn)->execute("CREATE TABLE test (id INTEGER)");
        ASSERT_TRUE(result.has_value());
        
        stats = pool.getStats();
        EXPECT_EQ(stats.activeConnections, 1);
        EXPECT_EQ(stats.availableConnections, 1);
    }
    
    // Connection returned to pool
    stats = pool.getStats();
    EXPECT_EQ(stats.activeConnections, 0);
    EXPECT_EQ(stats.availableConnections, 2);
    
    // Explicitly shutdown to avoid destructor issues
    pool.shutdown();
}

TEST_F(DatabaseTest, Migrations) {
    Database db;
    auto openResult = db.open(dbPath_.string(), ConnectionMode::Create);
    ASSERT_TRUE(openResult.has_value());
    
    MigrationManager mm(db);
    
    auto initResult = mm.initialize();
    ASSERT_TRUE(initResult.has_value());
    
    // Register migrations
    mm.registerMigrations(YamsMetadataMigrations::getAllMigrations());
    
    // Check current version
    auto currentVersion = mm.getCurrentVersion();
    ASSERT_TRUE(currentVersion.has_value());
    EXPECT_EQ(currentVersion.value(), 0);
    
    // Check if migration needed
    auto needsMigration = mm.needsMigration();
    ASSERT_TRUE(needsMigration.has_value());
    EXPECT_TRUE(needsMigration.value());
    
    // Apply migrations
    auto migrateResult = mm.migrate();
    ASSERT_TRUE(migrateResult.has_value());
    
    // Verify final version
    currentVersion = mm.getCurrentVersion();
    ASSERT_TRUE(currentVersion.has_value());
    EXPECT_EQ(currentVersion.value(), 5);
    
    // Verify tables exist
    auto docExists = db.tableExists("documents");
    ASSERT_TRUE(docExists.has_value());
    EXPECT_TRUE(docExists.value());
    
    auto metaExists = db.tableExists("metadata");
    ASSERT_TRUE(metaExists.has_value());
    EXPECT_TRUE(metaExists.value());
    
    // Verify FTS table exists if FTS5 is available
    auto fts5Available = db.hasFTS5();
    if (fts5Available && fts5Available.value()) {
        auto ftsExists = db.tableExists("documents_fts");
        ASSERT_TRUE(ftsExists.has_value());
        EXPECT_TRUE(ftsExists.value());
    }
}

TEST_F(DatabaseTest, ConcurrentAccess) {
    ConnectionPool pool(dbPath_.string());
    auto initResult = pool.initialize();
    ASSERT_TRUE(initResult.has_value());
    
    // Create table
    pool.withConnection([](Database& db) {
        return db.execute("CREATE TABLE counter (id INTEGER PRIMARY KEY, value INTEGER)");
    });
    
    pool.withConnection([](Database& db) {
        return db.execute("INSERT INTO counter (id, value) VALUES (1, 0)");
    });
    
    const int numThreads = 10;
    const int incrementsPerThread = 100;
    std::vector<std::thread> threads;
    
    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back([&pool, incrementsPerThread]() {
            for (int j = 0; j < incrementsPerThread; ++j) {
                pool.withConnection([](Database& db) -> Result<void> {
                    return db.transaction([&db]() -> Result<void> {
                        auto stmtResult = db.prepare(
                            "UPDATE counter SET value = value + 1 WHERE id = 1"
                        );
                        if (!stmtResult) return stmtResult.error();
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
        if (!stmtResult) return stmtResult.error();
        Statement stmt = std::move(stmtResult).value();
        
        auto stepResult = stmt.step();
        if (!stepResult) return stepResult.error();
        
        return stmt.getInt(0);
    });
    
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), numThreads * incrementsPerThread);
}