// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <filesystem>
#include <memory>
#include <string>

#include <catch2/catch_test_macros.hpp>

#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>

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

std::filesystem::path makeCorruptDb(std::string_view prefix) {
    auto dbPath = migrated_metadata_db_template().clone(prefix);
    corruptPage(dbPath, static_cast<std::uint64_t>(tableRootPage(dbPath, "documents")));
    return dbPath;
}

Result<void> scanDocumentsTable(Database& db) {
    auto stmt = db.prepare("SELECT COUNT(*) FROM documents NOT INDEXED");
    if (!stmt) {
        return stmt.error();
    }
    auto row = stmt.value().step();
    if (!row) {
        return row.error();
    }
    return {};
}

} // namespace

TEST_CASE("pool surfaces corruption errors and stays usable",
          "[unit][metadata][corruption][connection_pool]") {
    auto dbPath = makeCorruptDb("yams_pool_corrupt_");
    ArtifactGuard guard{dbPath};

    ConnectionPoolConfig config;
    config.minConnections = 1;
    config.maxConnections = 2;
    ConnectionPool pool(dbPath.string(), config);
    REQUIRE(pool.initialize());

    for (int i = 0; i < 6; ++i) {
        auto res = pool.withConnection(scanDocumentsTable);
        REQUIRE_FALSE(res);
        CHECK(res.error().code == ErrorCode::CorruptedData);
    }

    auto stats = pool.getStats();
    CHECK(stats.activeConnections == 0);
    CHECK(stats.totalAcquired == stats.totalReleased);

    auto healthy = pool.withConnection(
        [](Database& db) -> Result<void> { return db.execute("SELECT 1"); });
    CHECK(healthy);

    pool.shutdown();
}

TEST_CASE("pool healthCheck refills to minConnections on corrupt DB",
          "[unit][metadata][corruption][connection_pool]") {
    auto dbPath = makeCorruptDb("yams_pool_corrupt_health_");
    ArtifactGuard guard{dbPath};

    ConnectionPoolConfig config;
    config.minConnections = 2;
    config.maxConnections = 4;
    ConnectionPool pool(dbPath.string(), config);
    REQUIRE(pool.initialize());

    auto health = pool.healthCheck();
    CHECK(health);
    auto stats = pool.getStats();
    CHECK(stats.totalConnections >= config.minConnections);

    pool.shutdown();
}

TEST_CASE("pool caches connections that observed corruption (documented behavior)",
          "[unit][metadata][corruption][connection_pool]") {
    auto dbPath = makeCorruptDb("yams_pool_corrupt_cache_");
    ArtifactGuard guard{dbPath};

    ConnectionPoolConfig config;
    config.minConnections = 1;
    config.maxConnections = 1;
    ConnectionPool pool(dbPath.string(), config);
    REQUIRE(pool.initialize());

    auto first = pool.withConnection(scanDocumentsTable);
    REQUIRE_FALSE(first);

    auto second = pool.withConnection(
        [](Database& db) -> Result<void> { return db.execute("SELECT 1"); });
    INFO("Pool currently returns connections to the cache after a corruption error; eviction of "
         "poisoned connections is future work");
    CHECK(second);

    pool.shutdown();
}
// NOLINTEND(bugprone-chained-comparison)
