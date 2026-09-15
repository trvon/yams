// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Unit tests for DatabaseManager component (PBI-090)
//
// Catch2 migration from GTest (yams-3s4 / yams-zns)

#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/DatabaseManager.h>
#include <yams/daemon/components/db_integrity_stamp.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>

#include <chrono>
#include <filesystem>

using namespace yams::daemon;

namespace {

struct DatabaseManagerFixture {
    std::filesystem::path tempDir;
    std::unique_ptr<StateComponent> stateComponent;

    DatabaseManagerFixture() {
        tempDir = std::filesystem::temp_directory_path() /
                  ("yams_dbmgr_test_" +
                   std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(tempDir);

        stateComponent = std::make_unique<StateComponent>();
    }

    ~DatabaseManagerFixture() {
        stateComponent.reset();

        std::error_code ec;
        std::filesystem::remove_all(tempDir, ec);
    }

    DatabaseManager::Dependencies makeDeps() {
        DatabaseManager::Dependencies deps;
        deps.state = stateComponent.get();
        return deps;
    }
};

} // namespace

TEST_CASE_METHOD(DatabaseManagerFixture, "DatabaseManager construction",
                 "[daemon][components][database][catch2]") {
    auto deps = makeDeps();

    SECTION("construction succeeds with valid dependencies") {
        DatabaseManager mgr(deps);
        const bool hasExpectedName = std::string(mgr.getName()) == "DatabaseManager";
        CHECK(hasExpectedName);
    }
}

TEST_CASE_METHOD(DatabaseManagerFixture, "DatabaseManager initialize/shutdown lifecycle",
                 "[daemon][components][database][catch2]") {
    auto deps = makeDeps();
    DatabaseManager mgr(deps);

    SECTION("initialize returns success") {
        auto result = mgr.initialize();
        CHECK(result.has_value());
    }

    SECTION("shutdown after initialize is safe") {
        REQUIRE(mgr.initialize().has_value());
        mgr.shutdown();
        // No crash = success
    }

    SECTION("double shutdown is safe") {
        REQUIRE(mgr.initialize().has_value());
        mgr.shutdown();
        mgr.shutdown();
        // No crash = success
    }

    SECTION("shutdown without initialize is safe") {
        mgr.shutdown();
        // No crash = success
    }
}

TEST_CASE_METHOD(DatabaseManagerFixture, "DatabaseManager accessor before/after init",
                 "[daemon][components][database][catch2]") {
    auto deps = makeDeps();
    DatabaseManager mgr(deps);

    SECTION("getDatabase returns nullptr before init") {
        CHECK_FALSE(static_cast<bool>(mgr.getDatabase()));
    }

    SECTION("getConnectionPool returns nullptr before init") {
        CHECK_FALSE(static_cast<bool>(mgr.getConnectionPool()));
    }

    SECTION("getMetadataRepo returns nullptr before init") {
        CHECK_FALSE(static_cast<bool>(mgr.getMetadataRepo()));
    }

    SECTION("isReady returns false before init") {
        CHECK_FALSE(mgr.isReady());
    }
}

TEST_CASE_METHOD(DatabaseManagerFixture,
                 "DatabaseManager refuses a clean stamp while a connection lease is active",
                 "[daemon][components][database][integrity_stamp][catch2]") {
    const auto dbPath = tempDir / "yams.db";
    auto database = std::make_shared<yams::metadata::Database>();
    REQUIRE(database->open(dbPath.string(), yams::metadata::ConnectionMode::Create));

    DatabaseManager mgr(makeDeps());
    mgr.setDatabase(database);
    REQUIRE(mgr.initializePools(dbPath));

    auto pool = mgr.getConnectionPool();
    REQUIRE(pool);
    auto lease = pool->acquire();
    REQUIRE(lease);

    mgr.setIntegrityStampEligible(true);
    mgr.shutdown();

    const auto decision = consumeDbCleanShutdownStamp(dbPath);
    CHECK_FALSE(decision.trustedCleanShutdown);
}

TEST_CASE("DatabaseManager getName returns component name",
          "[daemon][components][database][catch2]") {
    StateComponent state;
    DatabaseManager::Dependencies deps;
    deps.state = &state;
    DatabaseManager mgr(deps);

    const bool hasExpectedName = std::string(mgr.getName()) == "DatabaseManager";
    CHECK(hasExpectedName);
}
