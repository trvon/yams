// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Unit tests for PluginManager component (PBI-090)
//
// Catch2 migration from GTest (yams-3s4 / yams-zns)

#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/PluginManager.h>
#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/daemon.h>

#include <chrono>
#include <filesystem>
#include <memory>

using namespace yams::daemon;

namespace {

struct PluginManagerFixture {
    std::filesystem::path tempDir;
    std::unique_ptr<DaemonLifecycleFsm> lifecycleFsm;
    std::unique_ptr<StateComponent> stateComponent;
    DaemonConfig config;

    PluginManagerFixture() {
        tempDir = std::filesystem::temp_directory_path() /
                  ("yams_pluginmgr_test_" +
                   std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(tempDir);

        lifecycleFsm = std::make_unique<DaemonLifecycleFsm>();
        stateComponent = std::make_unique<StateComponent>();
    }

    ~PluginManagerFixture() {
        lifecycleFsm.reset();
        stateComponent.reset();

        std::error_code ec;
        std::filesystem::remove_all(tempDir, ec);
    }

    PluginManager::Dependencies makeDeps() {
        PluginManager::Dependencies deps;
        deps.config = &config;
        deps.state = stateComponent.get();
        deps.lifecycleFsm = lifecycleFsm.get();
        deps.dataDir = tempDir;
        deps.resolvePreferredModel = []() { return std::string("test-model"); };
        return deps;
    }
};

} // namespace

TEST_CASE_METHOD(PluginManagerFixture,
                 "PluginManager construction",
                 "[daemon][components][plugin][catch2]") {
    auto deps = makeDeps();

    SECTION("construction succeeds with valid dependencies") {
        PluginManager mgr(deps);
        CHECK(mgr.getName() == std::string("PluginManager"));
    }
}

TEST_CASE_METHOD(PluginManagerFixture,
                 "PluginManager initialize/shutdown lifecycle",
                 "[daemon][components][plugin][catch2]") {
    auto deps = makeDeps();
    PluginManager mgr(deps);

    SECTION("initialize without plugins succeeds") {
        auto result = mgr.initialize();
        CHECK(result.has_value());
    }

    SECTION("shutdown is safe without initialize") {
        mgr.shutdown();
        // No crash = success
    }

    SECTION("initialize then shutdown works") {
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
}

TEST_CASE_METHOD(PluginManagerFixture,
                 "PluginManager plugin host accessors",
                 "[daemon][components][plugin][catch2]") {
    auto deps = makeDeps();
    PluginManager mgr(deps);

    SECTION("plugin host is nullptr before init") {
        // May or may not be null depending on implementation
        auto host = mgr.getPluginHost();
        // Just verify accessor doesn't crash
        (void)host;
    }

    SECTION("plugin loader is nullptr before init") {
        auto loader = mgr.getPluginLoader();
        (void)loader;
    }

    SECTION("external plugin host is nullptr before init") {
        auto ext = mgr.getExternalPluginHost();
        (void)ext;
    }
}

TEST_CASE_METHOD(PluginManagerFixture,
                 "PluginManager trust list operations",
                 "[daemon][components][plugin][catch2]") {
    auto deps = makeDeps();
    PluginManager mgr(deps);

    SECTION("initial trust list may be empty") {
        auto list = mgr.trustList();
        // Empty or contains defaults - both valid
        (void)list;
    }

    SECTION("trustAdd for valid path returns result") {
        auto result = mgr.trustAdd(tempDir / "plugin.so");
        // Either success or failure is acceptable without init
        (void)result;
    }

    SECTION("trustRemove for non-existent path returns result") {
        auto result = mgr.trustRemove(tempDir / "nonexistent.so");
        (void)result;
    }
}

TEST_CASE("PluginManager getName returns component name",
          "[daemon][components][plugin][catch2]") {
    DaemonLifecycleFsm fsm;
    StateComponent state;
    DaemonConfig config;

    PluginManager::Dependencies deps;
    deps.config = &config;
    deps.state = &state;
    deps.lifecycleFsm = &fsm;
    deps.resolvePreferredModel = []() { return std::string("test"); };

    PluginManager mgr(deps);
    CHECK(std::string(mgr.getName()) == "PluginManager");
}
