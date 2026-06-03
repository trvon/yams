// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Unit tests for VectorSystemManager component (PBI-090)
//
// Catch2 migration from GTest (yams-3s4 / yams-zns)

#include <catch2/catch_test_macros.hpp>

#include "../../../common/test_helpers_catch2.h"

#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/VectorSystemManager.h>
#include <yams/vector/vector_database.h>

#include <sqlite3.h>
#include <chrono>
#include <cstdlib>
#include <filesystem>

#ifndef _WIN32
#include <fcntl.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

using namespace yams::daemon;

namespace {

bool tableExists(sqlite3* db, const std::string& tableName) {
    sqlite3_stmt* stmt = nullptr;
    REQUIRE(sqlite3_prepare_v2(db,
                               "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
                               -1, &stmt, nullptr) == SQLITE_OK);
    sqlite3_bind_text(stmt, 1, tableName.c_str(), -1, SQLITE_TRANSIENT);
    REQUIRE(sqlite3_step(stmt) == SQLITE_ROW);
    bool exists = sqlite3_column_int64(stmt, 0) > 0;
    sqlite3_finalize(stmt);
    return exists;
}

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

    SECTION("retryable dimension deferral clears initAttempted latch") {
        yams::test::ScopedEnvVar embedDimEnv("YAMS_EMBED_DIM", std::nullopt);
        yams::test::ScopedEnvVar preferredModelEnv("YAMS_PREFERRED_MODEL", std::nullopt);
        yams::test::ScopedEnvVar configPathEnv("YAMS_CONFIG_PATH", std::nullopt);
        yams::test::ScopedEnvVar configEnv("YAMS_CONFIG",
                                           (tempDir / "missing-config.toml").string());
        auto isolatedHome = tempDir / "isolated_home";
        auto isolatedXdg = tempDir / "isolated_xdg";
        std::filesystem::create_directories(isolatedHome);
        std::filesystem::create_directories(isolatedXdg);
        yams::test::ScopedEnvVar homeEnv("HOME", isolatedHome.string());
        yams::test::ScopedEnvVar xdgEnv("XDG_CONFIG_HOME", isolatedXdg.string());

        auto unresolvedDeps = makeDeps();
        unresolvedDeps.resolvePreferredModel = {};
        unresolvedDeps.getEmbeddingDimension = {};
        VectorSystemManager unresolvedMgr(unresolvedDeps);

        CHECK_FALSE(unresolvedMgr.wasInitAttempted());
        auto result = unresolvedMgr.initializeOnce(tempDir);
        REQUIRE(result.has_value());
        CHECK_FALSE(result.value());
        CHECK_FALSE(unresolvedMgr.wasInitAttempted());
    }

#ifndef _WIN32
    SECTION("lock busy path clears initAttempted latch") {
        auto lockPath = tempDir / "vectors.lock";
        int readyPipe[2] = {-1, -1};
        int releasePipe[2] = {-1, -1};
        REQUIRE(::pipe(readyPipe) == 0);
        REQUIRE(::pipe(releasePipe) == 0);

        pid_t child = ::fork();
        REQUIRE(child >= 0);

        if (child == 0) {
            ::close(readyPipe[0]);
            ::close(releasePipe[1]);

            int lockFd = ::open(lockPath.c_str(), O_CREAT | O_RDWR, 0644);
            if (lockFd < 0) {
                _exit(2);
            }

            struct flock fl{};
            fl.l_type = F_WRLCK;
            fl.l_whence = SEEK_SET;
            if (::fcntl(lockFd, F_SETLK, &fl) != 0) {
                _exit(3);
            }

            char ready = '1';
            (void)::write(readyPipe[1], &ready, 1);
            char release = 0;
            (void)::read(releasePipe[0], &release, 1);

            fl.l_type = F_UNLCK;
            (void)::fcntl(lockFd, F_SETLK, &fl);
            ::close(lockFd);
            _exit(0);
        }

        ::close(readyPipe[1]);
        ::close(releasePipe[0]);

        char ready = 0;
        REQUIRE(::read(readyPipe[0], &ready, 1) == 1);
        REQUIRE(ready == '1');

        auto result = mgr.initializeOnce(tempDir);
        CHECK(result.has_value());
        if (result.has_value()) {
            CHECK_FALSE(result.value());
        }
        CHECK_FALSE(mgr.wasInitAttempted());

        char release = '1';
        REQUIRE(::write(releasePipe[1], &release, 1) == 1);
        ::close(readyPipe[0]);
        ::close(releasePipe[1]);

        int status = 0;
        REQUIRE(::waitpid(child, &status, 0) == child);
        REQUIRE(WIFEXITED(status));
        REQUIRE(WEXITSTATUS(status) == 0);
    }
#endif

    SECTION("initializeOnce prepares persisted search index for warm vectors") {
        yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS",
                                                std::optional<std::string>("0"));
        yams::test::ScopedEnvVar disableVectorDb("YAMS_DISABLE_VECTOR_DB",
                                                 std::optional<std::string>("0"));
        yams::test::ScopedEnvVar enableSqliteVecInit("YAMS_SQLITE_VEC_SKIP_INIT",
                                                     std::optional<std::string>("0"));
        yams::test::ScopedEnvVar disableInMemory("YAMS_VDB_IN_MEMORY", std::nullopt);

        auto warmDeps = makeDeps();
        warmDeps.getEmbeddingDimension = []() { return static_cast<size_t>(64); };
        VectorSystemManager warmMgr(warmDeps);

        yams::vector::VectorDatabaseConfig cfg;
        cfg.database_path = (tempDir / "vectors.db").string();
        cfg.embedding_dim = 64;

        {
            yams::vector::VectorDatabase db(cfg);
            REQUIRE(db.initialize());

            std::vector<float> embedding(64, 0.0f);
            embedding[0] = 1.0f;

            for (int i = 0; i < 260; ++i) {
                yams::vector::VectorRecord record;
                record.chunk_id = "chunk_" + std::to_string(i);
                record.document_hash = "doc_" + std::to_string(i);
                record.embedding = embedding;
                record.content = "content";
                REQUIRE(db.insertVector(record));
            }
            REQUIRE(db.optimizeIndex());
        }

        auto result = warmMgr.initializeOnce(tempDir);
        REQUIRE(result.has_value());
        REQUIRE(warmMgr.getVectorDatabase() != nullptr);
        CHECK(stateComponent->readiness.vectorDbReady.load());
        // VectorSystemManager now owns DB readiness only. Search-index readiness and any
        // background load/build work are managed asynchronously by VectorIndexCoordinator.
        CHECK_FALSE(stateComponent->readiness.vectorIndexReady.load());
        CHECK(stateComponent->readiness.vectorIndexProgress.load() == 0);

        CHECK(warmMgr.getVectorDatabase()->hasReusablePersistedSearchIndex());
    }

    SECTION("initializeOnce does not block on rebuilding missing persisted search index") {
        yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS",
                                                std::optional<std::string>("0"));
        yams::test::ScopedEnvVar disableVectorDb("YAMS_DISABLE_VECTOR_DB",
                                                 std::optional<std::string>("0"));
        yams::test::ScopedEnvVar enableSqliteVecInit("YAMS_SQLITE_VEC_SKIP_INIT",
                                                     std::optional<std::string>("0"));
        yams::test::ScopedEnvVar disableInMemory("YAMS_VDB_IN_MEMORY", std::nullopt);

        auto warmDeps = makeDeps();
        warmDeps.getEmbeddingDimension = []() { return static_cast<size_t>(64); };
        VectorSystemManager warmMgr(warmDeps);

        yams::vector::VectorDatabaseConfig cfg;
        cfg.database_path = (tempDir / "vectors.db").string();
        cfg.embedding_dim = 64;

        {
            yams::vector::VectorDatabase db(cfg);
            REQUIRE(db.initialize());

            std::vector<float> embedding(64, 0.0f);
            embedding[0] = 1.0f;

            for (int i = 0; i < 64; ++i) {
                yams::vector::VectorRecord record;
                record.chunk_id = "chunk_unpersisted_" + std::to_string(i);
                record.document_hash = "doc_unpersisted_" + std::to_string(i);
                record.embedding = embedding;
                record.content = "content";
                REQUIRE(db.insertVector(record));
            }
            CHECK_FALSE(db.hasReusablePersistedSearchIndex());
        }

        auto result = warmMgr.initializeOnce(tempDir);
        REQUIRE(result.has_value());
        REQUIRE(warmMgr.getVectorDatabase() != nullptr);
        CHECK(stateComponent->readiness.vectorDbReady.load());
        CHECK_FALSE(stateComponent->readiness.vectorIndexReady.load());
        CHECK(stateComponent->readiness.vectorIndexProgress.load() == 0);

        CHECK_FALSE(warmMgr.getVectorDatabase()->hasReusablePersistedSearchIndex());
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
