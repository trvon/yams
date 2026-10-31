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

#include <chrono>
#include <cstdlib>
#include <filesystem>

#ifndef _WIN32
#include <fcntl.h>
#include <unistd.h>
#include <sys/wait.h>
#endif

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
        CHECK((mgr.getName() == std::string("VectorSystemManager")));
    }
}

TEST_CASE_METHOD(VectorSystemManagerFixture,
                 "VectorSystemManager uses injected embedding dimension snapshot",
                 "[daemon][components][vector][config][snapshot][catch2]") {
    yams::test::ScopedEnvVar ambientDimension{"YAMS_EMBED_DIM", "384"};
    auto deps = makeDeps();
    deps.getEmbeddingDimension = {};
    deps.resolvePreferredModel = {};
    deps.resolveConfiguredDimension = [] { return std::optional<size_t>{16U}; };

    VectorSystemManager mgr(std::move(deps));
    const auto result = mgr.initializeOnce(tempDir);

    REQUIRE(result.has_value());
    CHECK(result.value());
    CHECK((mgr.getEmbeddingDimension() == 16U));
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
            CHECK((result1.value() == true));
            CHECK((result2.value() == false));
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
        REQUIRE((::pipe(readyPipe) == 0));
        REQUIRE((::pipe(releasePipe) == 0));

        pid_t child = ::fork();
        REQUIRE((child >= 0));

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
        REQUIRE((::read(readyPipe[0], &ready, 1) == 1));
        REQUIRE((ready == '1'));

        auto result = mgr.initializeOnce(tempDir);
        CHECK(result.has_value());
        if (result.has_value()) {
            CHECK_FALSE(result.value());
        }
        CHECK_FALSE(mgr.wasInitAttempted());

        char release = '1';
        REQUIRE((::write(releasePipe[1], &release, 1) == 1));
        ::close(readyPipe[0]);
        ::close(releasePipe[1]);

        int status = 0;
        REQUIRE((::waitpid(child, &status, 0) == child));
        REQUIRE(WIFEXITED(status));
        REQUIRE((WEXITSTATUS(status) == 0));
    }
#endif

    SECTION("initializeOnce prepares persisted search index for warm vectors") {
        yams::test::ScopedEnvVar enableVectorAlias("YAMS_DISABLE_VECTOR", "0");
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
        REQUIRE((warmMgr.getVectorDatabase() != nullptr));
        CHECK(stateComponent->readiness.vectorDbReady.load());
        // VectorSystemManager now owns DB readiness only. Search-index readiness and any
        // background load/build work are managed asynchronously by VectorIndexCoordinator.
        CHECK_FALSE(stateComponent->readiness.vectorIndexReady.load());
        CHECK((stateComponent->readiness.vectorIndexProgress.load() == 0));

        CHECK(warmMgr.getVectorDatabase()->hasReusablePersistedSearchIndex());
    }

    SECTION("initializeOnce does not block on rebuilding missing persisted search index") {
        yams::test::ScopedEnvVar enableVectorAlias("YAMS_DISABLE_VECTOR", "0");
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
        REQUIRE((warmMgr.getVectorDatabase() != nullptr));
        CHECK(stateComponent->readiness.vectorDbReady.load());
        CHECK_FALSE(stateComponent->readiness.vectorIndexReady.load());
        CHECK((stateComponent->readiness.vectorIndexProgress.load() == 0));

        CHECK_FALSE(warmMgr.getVectorDatabase()->hasReusablePersistedSearchIndex());
    }

    SECTION("initializeOnce applies the configured Simeon PQ rerank factor") {
        yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS",
                                                std::optional<std::string>("0"));
        yams::test::ScopedEnvVar disableVectorDb("YAMS_DISABLE_VECTOR_DB",
                                                 std::optional<std::string>("0"));
        yams::test::ScopedEnvVar enableSqliteVecInit("YAMS_SQLITE_VEC_SKIP_INIT",
                                                     std::optional<std::string>("0"));
        yams::test::ScopedEnvVar disableInMemory("YAMS_VDB_IN_MEMORY", std::nullopt);
        yams::test::ScopedEnvVar searchEngineEnv("YAMS_VECTOR_SEARCH_ENGINE", std::nullopt);
        const auto configPath =
            yams::test::write_file(tempDir / "config.toml", "[vector_database]\n"
                                                            "search_engine = \"simeon_pq_adc\"\n"
                                                            "simeon_pq_rerank_factor = 7\n");
        yams::test::ScopedEnvVar configPathEnv("YAMS_CONFIG_PATH", configPath.string());

        auto configuredDeps = makeDeps();
        configuredDeps.getEmbeddingDimension = []() { return static_cast<size_t>(64); };
        VectorSystemManager configuredMgr(configuredDeps);

        const auto result = configuredMgr.initializeOnce(tempDir);
        REQUIRE(result.has_value());
        REQUIRE((configuredMgr.getVectorDatabase() != nullptr));
        CHECK((configuredMgr.getVectorDatabase()->getConfig().search_engine ==
               yams::vector::VectorSearchEngine::SimeonPqAdc));
        CHECK((configuredMgr.getVectorDatabase()->getConfig().simeon_pq_rerank_factor == 7));
    }
}

TEST_CASE_METHOD(VectorSystemManagerFixture, "VectorSystemManager accessors before/after init",
                 "[daemon][components][vector][catch2]") {
    auto deps = makeDeps();
    VectorSystemManager mgr(deps);

    SECTION("getVectorDatabase returns nullptr before init") {
        CHECK((mgr.getVectorDatabase() == nullptr));
    }

    // VectorIndexManager removed - SearchEngine uses VectorDatabase directly

    SECTION("getEmbeddingDimension returns 0 before init") {
        CHECK((mgr.getEmbeddingDimension() == 0));
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
    CHECK((std::string(mgr.getName()) == "VectorSystemManager"));
}

#include <sqlite3.h>
#include <yams/daemon/components/ConfigResolver.h>
#include <yams/vector/vector_types.h>

namespace {

// Writes vectors.db at `dim` with one document vector, as an older install would have.
void seedVectorDb(const std::filesystem::path& dataDir, std::size_t dim,
                  const std::string& modelId = "old-space") {
    yams::vector::VectorDatabaseConfig cfg;
    cfg.database_path = (dataDir / "vectors.db").string();
    cfg.embedding_dim = dim;
    cfg.create_if_missing = true;
    yams::vector::VectorDatabase db(cfg);
    REQUIRE(db.initialize());
    yams::vector::VectorRecord rec;
    rec.chunk_id = "doc-a";
    rec.document_hash = "hash-a";
    rec.embedding.assign(dim, 0.0F);
    rec.embedding[0] = 1.0F;
    rec.level = yams::vector::EmbeddingLevel::DOCUMENT;
    rec.model_id = modelId;
    REQUIRE(db.insertVector(rec));
}

VectorSystemManager::MigrationTarget target(std::size_t dim, std::string identity) {
    return VectorSystemManager::MigrationTarget{dim, std::move(identity)};
}

// Adds one more document vector to an existing vectors.db.
void addVector(const std::filesystem::path& dataDir, std::size_t dim, const std::string& hash,
               const std::string& modelId) {
    yams::vector::VectorDatabaseConfig cfg;
    cfg.database_path = (dataDir / "vectors.db").string();
    cfg.embedding_dim = dim;
    yams::vector::VectorDatabase db(cfg);
    REQUIRE(db.initialize());
    yams::vector::VectorRecord rec;
    rec.chunk_id = hash + "-chunk";
    rec.document_hash = hash;
    rec.embedding.assign(dim, 0.0F);
    rec.embedding[1] = 1.0F;
    rec.level = yams::vector::EmbeddingLevel::DOCUMENT;
    rec.model_id = modelId;
    REQUIRE(db.insertVector(rec));
}

std::int64_t countVectorsWithModel(const std::filesystem::path& dataDir,
                                   const std::string& modelId) {
    sqlite3* db = nullptr;
    REQUIRE(sqlite3_open_v2((dataDir / "vectors.db").string().c_str(), &db, SQLITE_OPEN_READONLY,
                            nullptr) == SQLITE_OK);
    sqlite3_stmt* stmt = nullptr;
    REQUIRE(sqlite3_prepare_v2(db, "SELECT count(*) FROM vectors WHERE model_id = ?", -1, &stmt,
                               nullptr) == SQLITE_OK);
    sqlite3_bind_text(stmt, 1, modelId.c_str(), -1, SQLITE_TRANSIENT);
    std::int64_t n = -1;
    if (sqlite3_step(stmt) == SQLITE_ROW)
        n = sqlite3_column_int64(stmt, 0);
    sqlite3_finalize(stmt);
    sqlite3_close(db);
    return n;
}

} // namespace

TEST_CASE_METHOD(VectorSystemManagerFixture,
                 "VectorSystemManager recreates vector tables when the embedding space changes dim",
                 "[daemon][components][vector][migrate][catch2]") {
    seedVectorDb(tempDir, 1024);
    REQUIRE(ConfigResolver::readDbEmbeddingDim((tempDir / "vectors.db").string()) == 1024U);

    auto deps = makeDeps();
    deps.getEmbeddingDimension = [] { return static_cast<size_t>(256); };
    deps.migrationTarget = [] { return std::optional{target(256U, "new-space")}; };
    VectorSystemManager mgr(std::move(deps));
    const auto result = mgr.initializeOnce(tempDir);
    REQUIRE(result.has_value());
    REQUIRE(result.value());
    CHECK(mgr.getEmbeddingDimension() == 256U);
    auto vdb = mgr.getVectorDatabase();
    REQUIRE(vdb);
    CHECK(vdb->getVectorCount() == 0U);
    CHECK(ConfigResolver::readVectorSentinelDim(tempDir) == std::optional<size_t>{256U});
    CHECK(mgr.migratedEmbeddingSpace());
}

TEST_CASE_METHOD(VectorSystemManagerFixture,
                 "VectorSystemManager keeps vectors when no migration target or same dim",
                 "[daemon][components][vector][migrate][catch2]") {
    seedVectorDb(tempDir, 384);
    SECTION("same dim and same identity") {
        auto deps = makeDeps();
        deps.migrationTarget = [] { return std::optional{target(384U, "old-space")}; };
        VectorSystemManager mgr(std::move(deps));
        REQUIRE(mgr.initializeOnce(tempDir).value());
        CHECK(mgr.getEmbeddingDimension() == 384U);
        CHECK(mgr.getVectorDatabase()->getVectorCount() == 1U);
        CHECK_FALSE(mgr.migratedEmbeddingSpace());
    }
    SECTION("no target (auto-migrate off or not a training-free provider)") {
        auto deps = makeDeps();
        deps.getEmbeddingDimension = [] { return static_cast<size_t>(256); };
        deps.migrationTarget = [] { return std::optional<VectorSystemManager::MigrationTarget>{}; };
        VectorSystemManager mgr(std::move(deps));
        REQUIRE(mgr.initializeOnce(tempDir).value());
        CHECK(mgr.getEmbeddingDimension() == 384U); // existing DB wins, as before
        CHECK(mgr.getVectorDatabase()->getVectorCount() == 1U);
    }
}

TEST_CASE_METHOD(VectorSystemManagerFixture,
                 "VectorSystemManager recreates vector tables when the space identity changes",
                 "[daemon][components][vector][migrate][catch2]") {
    seedVectorDb(tempDir, 384, "simeon-config-v1:old");
    const auto vdbPath = (tempDir / "vectors.db").string();
    REQUIRE(ConfigResolver::readDbForeignEmbeddingSpace(vdbPath, "simeon-static-v1:new") ==
            std::optional<std::string>{"simeon-config-v1:old"});
    REQUIRE_FALSE(ConfigResolver::readDbForeignEmbeddingSpace(vdbPath, "simeon-config-v1:old"));

    auto deps = makeDeps();
    deps.migrationTarget = [] { return std::optional{target(384U, "simeon-static-v1:new")}; };
    VectorSystemManager mgr(std::move(deps));
    REQUIRE(mgr.initializeOnce(tempDir).value());
    CHECK(mgr.getEmbeddingDimension() == 384U);
    CHECK(mgr.getVectorDatabase()->getVectorCount() == 0U);
}

// A missing or unreadable static table makes the Simeon backend fall back to the hash encoder.
// That fallback is not a space the user chose: auto-migrate must not wipe the static vectors.
TEST_CASE_METHOD(VectorSystemManagerFixture,
                 "VectorSystemManager does not migrate a static store to a fallback encoder",
                 "[daemon][components][vector][migrate][catch2]") {
    const std::string staticSpace = "simeon-static-v1:yams-static-bge-c4-256.smstat:abc:dim=256";
    seedVectorDb(tempDir, 256, staticSpace);

    auto deps = makeDeps();
    deps.getEmbeddingDimension = [] { return static_cast<size_t>(1024); };
    deps.migrationTarget = [] {
        auto t = target(1024U, "simeon-config-v1:char_and_word:3-5:sketch=4096:output=1024");
        t.fallback = true;
        return std::optional{t};
    };
    VectorSystemManager mgr(std::move(deps));
    REQUIRE(mgr.initializeOnce(tempDir).value());
    CHECK_FALSE(mgr.migratedEmbeddingSpace());
    CHECK(mgr.getEmbeddingDimension() == 256U);
    CHECK(countVectorsWithModel(tempDir, staticSpace) == 1);
}

// The post-migration work (drop semantic_neighbor edges, rebuild topology, re-embed) runs after
// initializeOnce. A crash before it completes must not lose it: the next start still reports a
// pending migration.
TEST_CASE_METHOD(VectorSystemManagerFixture,
                 "VectorSystemManager keeps a migration pending across a restart",
                 "[daemon][components][vector][migrate][catch2]") {
    seedVectorDb(tempDir, 1024);
    auto makeMigratingDeps = [this] {
        auto deps = makeDeps();
        deps.getEmbeddingDimension = [] { return static_cast<size_t>(256); };
        deps.migrationTarget = [] { return std::optional{target(256U, "new-space")}; };
        return deps;
    };
    {
        VectorSystemManager first(makeMigratingDeps());
        REQUIRE(first.initializeOnce(tempDir).value());
        REQUIRE(first.migratedEmbeddingSpace());
        // daemon dies here, before ServiceManager runs the post-migration steps
    }
    {
        VectorSystemManager second(makeMigratingDeps());
        REQUIRE(second.initializeOnce(tempDir).value());
        CHECK(second.migratedEmbeddingSpace());
        CHECK(second.getVectorDatabase()->getVectorCount() == 0U);
        second.acknowledgeEmbeddingMigration();
        CHECK_FALSE(second.migratedEmbeddingSpace());
    }
    VectorSystemManager third(makeMigratingDeps());
    REQUIRE(third.initializeOnce(tempDir).value());
    CHECK_FALSE(third.migratedEmbeddingSpace());
}

// A store holding vectors from two spaces (auto-migrate was off, or an older bug) must not keep
// the foreign rows just because the first row happens to match the target.
TEST_CASE_METHOD(VectorSystemManagerFixture,
                 "VectorSystemManager removes vectors from other spaces when identities are mixed",
                 "[daemon][components][vector][migrate][catch2]") {
    seedVectorDb(tempDir, 384, "new-space");
    addVector(tempDir, 384, "hash-b", "old-space");
    REQUIRE(countVectorsWithModel(tempDir, "old-space") == 1);

    auto deps = makeDeps();
    deps.migrationTarget = [] { return std::optional{target(384U, "new-space")}; };
    VectorSystemManager mgr(std::move(deps));
    REQUIRE(mgr.initializeOnce(tempDir).value());
    CHECK(countVectorsWithModel(tempDir, "old-space") == 0);
}
