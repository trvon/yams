// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Unit tests for VectorSystemManager component (PBI-090)

#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/VectorSystemManager.h>

#include <chrono>
#include <filesystem>

using namespace yams::daemon;

namespace {

struct VectorSystemManagerFixture {
    std::filesystem::path tempDir;

    VectorSystemManagerFixture() {
        tempDir = std::filesystem::temp_directory_path() /
            ("yams_vectorsys_test_" +
             std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(tempDir);
    }

    ~VectorSystemManagerFixture() {
        std::error_code ec;
        std::filesystem::remove_all(tempDir, ec);
    }

    VectorSystemManager::Config makeConfig() {
        VectorSystemManager::Config cfg;
        cfg.data_dir = tempDir;
        cfg.index_type = "flat";
        cfg.dimension = 384;
        cfg.metric = "cosine";
        return cfg;
    }

    VectorSystemManager::Dependencies makeDeps() {
        VectorSystemManager::Dependencies deps;
        deps.databaseManager = nullptr;
        deps.pluginManager = nullptr;
        return deps;
    }
};

} // namespace

TEST_CASE_METHOD(VectorSystemManagerFixture, "VectorSystemManager construction", "[daemon][components][vector]") {
    auto cfg = makeConfig();
    auto deps = makeDeps();

    SECTION("construction succeeds with valid config") {
        VectorSystemManager mgr(cfg, deps);
        CHECK_FALSE(mgr.isInitialized());
    }

    SECTION("construction with different index types") {
        for (const auto& indexType : {"flat", "hnsw", "ivf"}) {
            cfg.index_type = indexType;
            VectorSystemManager mgr(cfg, deps);
            CHECK_FALSE(mgr.isInitialized());
        }
    }
}

TEST_CASE_METHOD(VectorSystemManagerFixture, "VectorSystemManager initialize/shutdown lifecycle", "[daemon][components][vector]") {
    auto cfg = makeConfig();
    auto deps = makeDeps();
    VectorSystemManager mgr(cfg, deps);

    SECTION("initialize creates vector index") {
        auto result = mgr.initialize();
        REQUIRE(result);
        CHECK(mgr.isInitialized());
    }

    SECTION("shutdown after initialize works") {
        REQUIRE(mgr.initialize());
        CHECK(mgr.isInitialized());

        mgr.shutdown();
        CHECK_FALSE(mgr.isInitialized());
    }

    SECTION("double initialize is safe") {
        REQUIRE(mgr.initialize());
        REQUIRE(mgr.initialize());
        CHECK(mgr.isInitialized());
        mgr.shutdown();
    }

    SECTION("double shutdown is safe") {
        REQUIRE(mgr.initialize());
        mgr.shutdown();
        mgr.shutdown();
        CHECK_FALSE(mgr.isInitialized());
    }

    SECTION("shutdown without initialize is safe") {
        mgr.shutdown();
        CHECK_FALSE(mgr.isInitialized());
    }
}

TEST_CASE_METHOD(VectorSystemManagerFixture, "VectorSystemManager provides index handle", "[daemon][components][vector]") {
    auto cfg = makeConfig();
    auto deps = makeDeps();
    VectorSystemManager mgr(cfg, deps);

    SECTION("getIndex returns nullptr before initialize") {
        CHECK(mgr.getIndex() == nullptr);
    }

    SECTION("getIndex returns valid handle after initialize") {
        REQUIRE(mgr.initialize());
        CHECK(mgr.getIndex() != nullptr);
        mgr.shutdown();
    }

    SECTION("getIndex returns nullptr after shutdown") {
        REQUIRE(mgr.initialize());
        CHECK(mgr.getIndex() != nullptr);
        mgr.shutdown();
        CHECK(mgr.getIndex() == nullptr);
    }
}

TEST_CASE_METHOD(VectorSystemManagerFixture, "VectorSystemManager checkpoint support", "[daemon][components][vector]") {
    auto cfg = makeConfig();
    auto deps = makeDeps();
    VectorSystemManager mgr(cfg, deps);

    SECTION("checkpoint before initialize returns false") {
        CHECK_FALSE(mgr.checkpoint());
    }

    SECTION("checkpoint after initialize succeeds") {
        REQUIRE(mgr.initialize());
        CHECK(mgr.checkpoint());
        mgr.shutdown();
    }

    SECTION("multiple checkpoints succeed") {
        REQUIRE(mgr.initialize());
        for (int i = 0; i < 3; ++i) {
            CHECK(mgr.checkpoint());
        }
        mgr.shutdown();
    }
}

TEST_CASE_METHOD(VectorSystemManagerFixture, "VectorSystemManager stats", "[daemon][components][vector]") {
    auto cfg = makeConfig();
    auto deps = makeDeps();
    VectorSystemManager mgr(cfg, deps);

    SECTION("stats before initialize are zero") {
        CHECK(mgr.vectorCount() == 0);
        CHECK(mgr.indexSizeBytes() == 0);
    }

    SECTION("stats after initialize reflect empty index") {
        REQUIRE(mgr.initialize());
        CHECK(mgr.vectorCount() == 0);
        // Index may have some overhead
        mgr.shutdown();
    }
}

TEST_CASE("VectorSystemManager config defaults", "[daemon][components][vector]") {
    VectorSystemManager::Config cfg;

    CHECK_FALSE(cfg.data_dir.empty());
    CHECK_FALSE(cfg.index_type.empty());
    CHECK(cfg.dimension > 0);
    CHECK_FALSE(cfg.metric.empty());
}
