// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Unit tests for DatabaseManager component (PBI-090)
//
// Catch2 migration from GTest (yams-3s4 / yams-zns)

#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/DatabaseManager.h>
#include <yams/daemon/components/StateComponent.h>

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

TEST_CASE_METHOD(DatabaseManagerFixture,
                 "DatabaseManager construction",
                 "[daemon][components][database][catch2]") {
    auto deps = makeDeps();

    SECTION("construction succeeds with valid dependencies") {
        DatabaseManager mgr(deps);
        CHECK(mgr.getName() == std::string("DatabaseManager"));
    }
}

TEST_CASE_METHOD(DatabaseManagerFixture,
                 "DatabaseManager initialize/shutdown lifecycle",
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

TEST_CASE_METHOD(DatabaseManagerFixture,
                 "DatabaseManager accessor before/after init",
                 "[daemon][components][database][catch2]") {
    auto deps = makeDeps();
    DatabaseManager mgr(deps);

    SECTION("getDatabase returns nullptr before init") {
        CHECK(mgr.getDatabase() == nullptr);
    }

    SECTION("getConnectionPool returns nullptr before init") {
        CHECK(mgr.getConnectionPool() == nullptr);
    }

    SECTION("getMetadataRepo returns nullptr before init") {
        CHECK(mgr.getMetadataRepo() == nullptr);
    }

    SECTION("isReady returns false before init") {
        CHECK_FALSE(mgr.isReady());
    }
}

TEST_CASE("DatabaseManager getName returns component name",
          "[daemon][components][database][catch2]") {
    StateComponent state;
    DatabaseManager::Dependencies deps;
    deps.state = &state;
    DatabaseManager mgr(deps);

    CHECK(std::string(mgr.getName()) == "DatabaseManager");
}
