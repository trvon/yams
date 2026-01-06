// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Unit tests for VectorSystemManager component (PBI-090)
//
// Catch2 migration from GTest (yams-3s4 / yams-zns)

#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/VectorSystemManager.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>

using namespace yams::daemon;

namespace {

struct VectorSystemManagerFixture {
    std::filesystem::path tempDir;
    std::unique_ptr<StateComponent> stateComponent;

    VectorSystemManagerFixture() {
        tempDir = std::filesystem::temp_directory_path() /
                  ("yams_vectorsys_test_" +
                   std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(tempDir);

        stateComponent = std::make_unique<StateComponent>();
    }

    ~VectorSystemManagerFixture() {
        stateComponent.reset();

        std::error_code ec;
        std::filesystem::remove_all(tempDir, ec);
    }

    VectorSystemManager::Dependencies makeDeps() {
        VectorSystemManager::Dependencies deps;
        deps.state = stateComponent.get();
        deps.serviceFsm = nullptr;
        deps.resolvePreferredModel = []() { return std::string("test-model"); };
        deps.getEmbeddingDimension = []() { return static_cast<size_t>(384); };
        return deps;
    }
};

} // namespace

TEST_CASE_METHOD(VectorSystemManagerFixture, "VectorSystemManager construction",
                 "[daemon][components][vector][catch2]") {
    auto deps = makeDeps();

    SECTION("construction succeeds with valid dependencies") {
        VectorSystemManager mgr(deps);
        CHECK(mgr.getName() == std::string("VectorSystemManager"));
    }
}

TEST_CASE_METHOD(VectorSystemManagerFixture, "VectorSystemManager initialize/shutdown lifecycle",
                 "[daemon][components][vector][catch2]") {
    auto deps = makeDeps();
    VectorSystemManager mgr(deps);

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
    }

    SECTION("shutdown without initialize is safe") {
        mgr.shutdown();
    }
}

TEST_CASE_METHOD(VectorSystemManagerFixture, "VectorSystemManager initializeOnce semantics",
                 "[daemon][components][vector][catch2]") {
    auto deps = makeDeps();
    VectorSystemManager mgr(deps);

    SECTION("first call to initializeOnce performs initialization") {
        CHECK_FALSE(mgr.wasInitAttempted());
        auto result = mgr.initializeOnce(tempDir);
        CHECK(mgr.wasInitAttempted());
    }

    SECTION("second call to initializeOnce skips work") {
        // Skip this test if vectors are disabled - the semantics change
        if (std::getenv("YAMS_SQLITE_VEC_SKIP_INIT") || std::getenv("YAMS_DISABLE_VECTORS") ||
            std::getenv("YAMS_DISABLE_VECTOR_DB")) {
            SKIP("Vector initialization disabled - skipping initializeOnce semantics test");
        }

        auto result1 = mgr.initializeOnce(tempDir);
        auto result2 = mgr.initializeOnce(tempDir);

        // Both succeed but second one should indicate it didn't perform work
        if (result1.has_value() && result2.has_value()) {
            CHECK(result1.value() == true);
            CHECK(result2.value() == false);
        }
    }

    SECTION("resetInitAttempt allows re-initialization") {
        mgr.initializeOnce(tempDir);
        CHECK(mgr.wasInitAttempted());

        mgr.resetInitAttempt();
        CHECK_FALSE(mgr.wasInitAttempted());
    }
}

TEST_CASE_METHOD(VectorSystemManagerFixture, "VectorSystemManager accessors before/after init",
                 "[daemon][components][vector][catch2]") {
    auto deps = makeDeps();
    VectorSystemManager mgr(deps);

    SECTION("getVectorDatabase returns nullptr before init") {
        CHECK(mgr.getVectorDatabase() == nullptr);
    }

    // VectorIndexManager removed - SearchEngine uses VectorDatabase directly

    SECTION("getEmbeddingDimension returns 0 before init") {
        CHECK(mgr.getEmbeddingDimension() == 0);
    }
}

TEST_CASE("VectorSystemManager getName returns component name",
          "[daemon][components][vector][catch2]") {
    StateComponent state;
    VectorSystemManager::Dependencies deps;
    deps.state = &state;
    deps.resolvePreferredModel = []() { return std::string("test"); };
    deps.getEmbeddingDimension = []() { return static_cast<size_t>(384); };

    VectorSystemManager mgr(deps);
    CHECK(std::string(mgr.getName()) == "VectorSystemManager");
}
