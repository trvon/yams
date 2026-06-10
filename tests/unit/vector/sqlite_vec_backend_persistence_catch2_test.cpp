// Regression tests for SqliteVecBackend::ensurePersistenceSchema().
//
// Locks in the self-heal that creates simeon_pq_meta + simeon_pq_codes on
// legacy vectors.db files where those tables were never added. Without the
// heal, prepareSearchIndex() reports "No reusable persisted Simeon PQ index
// for dim N - falling back to rebuild" on every cold start AND the
// subsequent persistIndex() silently fails because the tables don't exist.

#include <catch2/catch_test_macros.hpp>

#include <cmath>
#include <cstdlib>
#include <filesystem>
#include <random>
#include <string>

#include <sqlite3.h>
#include <yams/vector/sqlite_vec_backend.h>

using namespace yams::vector;

namespace {

struct PersistenceFixture {
    PersistenceFixture() {
        if (const char* skipEnv = std::getenv("YAMS_SQLITE_VEC_SKIP_INIT")) {
            std::string v(skipEnv);
            if (v == "1" || v == "true") {
                skipReason = "Skipping (YAMS_SQLITE_VEC_SKIP_INIT=1)";
            }
        }
        if (const char* disableEnv = std::getenv("YAMS_DISABLE_VECTORS")) {
            std::string v(disableEnv);
            if (v == "1" || v == "true") {
                skipReason = "Skipping (YAMS_DISABLE_VECTORS=1)";
            }
        }
    }

    ~PersistenceFixture() {
        for (const auto& p : tempFiles) {
            std::error_code ec;
            std::filesystem::remove(p, ec);
        }
    }

    void skipIfNeeded() const {
        if (!skipReason.empty()) {
            SKIP(skipReason);
        }
    }

    std::string createTempDbPath() {
        auto path = std::filesystem::temp_directory_path() /
                    ("sqlite_vec_persistence_" + std::to_string(++tempCounter) + ".db");
        tempFiles.push_back(path.string());
        return path.string();
    }

    static bool tableExists(sqlite3* db, const char* name) {
        sqlite3_stmt* stmt = nullptr;
        const char* sql = "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?";
        if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
            return false;
        }
        sqlite3_bind_text(stmt, 1, name, -1, SQLITE_TRANSIENT);
        bool present = (sqlite3_step(stmt) == SQLITE_ROW);
        sqlite3_finalize(stmt);
        return present;
    }

    std::string skipReason;
    std::vector<std::string> tempFiles;
    static inline int tempCounter = 0;
};

} // namespace

TEST_CASE_METHOD(PersistenceFixture,
                 "ensurePersistenceSchema is idempotent on a freshly created backend",
                 "[unit][vector][persistence][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config cfg;
    cfg.embedding_dim = 64;
    SqliteVecBackend backend(cfg);

    auto initRes = backend.initialize(createTempDbPath());
    REQUIRE(initRes.has_value());
    REQUIRE(backend.isInitialized());

    auto createRes = backend.createTables(cfg.embedding_dim);
    REQUIRE(createRes.has_value());

    sqlite3* db = backend.getDbHandle();
    REQUIRE(db != nullptr);
    REQUIRE(tableExists(db, "simeon_pq_meta"));
    REQUIRE(tableExists(db, "simeon_pq_codes"));

    // Idempotent: two consecutive calls leave the tables present and return success.
    REQUIRE(backend.ensurePersistenceSchema().has_value());
    REQUIRE(backend.ensurePersistenceSchema().has_value());

    REQUIRE(tableExists(db, "simeon_pq_meta"));
    REQUIRE(tableExists(db, "simeon_pq_codes"));
}

TEST_CASE_METHOD(PersistenceFixture,
                 "ensurePersistenceSchema heals a legacy DB missing simeon_pq_* tables",
                 "[unit][vector][persistence][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config cfg;
    cfg.embedding_dim = 64;
    SqliteVecBackend backend(cfg);

    auto initRes = backend.initialize(createTempDbPath());
    REQUIRE(initRes.has_value());
    auto createRes = backend.createTables(cfg.embedding_dim);
    REQUIRE(createRes.has_value());

    sqlite3* db = backend.getDbHandle();
    REQUIRE(db != nullptr);

    // Simulate a legacy schema by dropping the persistence tables.
    char* err = nullptr;
    REQUIRE(sqlite3_exec(db, "DROP TABLE simeon_pq_meta", nullptr, nullptr, &err) == SQLITE_OK);
    REQUIRE(sqlite3_exec(db, "DROP TABLE simeon_pq_codes", nullptr, nullptr, &err) == SQLITE_OK);
    REQUIRE_FALSE(tableExists(db, "simeon_pq_meta"));
    REQUIRE_FALSE(tableExists(db, "simeon_pq_codes"));

    // Self-heal recreates both tables.
    auto healRes = backend.ensurePersistenceSchema();
    REQUIRE(healRes.has_value());

    REQUIRE(tableExists(db, "simeon_pq_meta"));
    REQUIRE(tableExists(db, "simeon_pq_codes"));
}

TEST_CASE_METHOD(PersistenceFixture,
                 "corrupt vec0 HNSW shadow tables degrade to rebuild on reopen",
                 "[unit][vector][persistence][catch2]") {
    skipIfNeeded();

    constexpr size_t kDim = 32;
    const auto dbPath = createTempDbPath();

    auto makeRecord = [](size_t i) {
        VectorRecord rec;
        rec.chunk_id = "chunk_" + std::to_string(i);
        rec.document_hash = "doc_" + std::to_string(i);
        rec.content = "content " + std::to_string(i);
        std::mt19937 rng(static_cast<uint32_t>(100 + i));
        std::uniform_real_distribution<float> dist(-1.0f, 1.0f);
        rec.embedding.resize(kDim);
        float norm = 0.0f;
        for (auto& v : rec.embedding) {
            v = dist(rng);
            norm += v * v;
        }
        norm = std::sqrt(norm);
        for (auto& v : rec.embedding) {
            v /= norm;
        }
        return rec;
    };

    std::vector<float> query;
    {
        SqliteVecBackend::Config cfg;
        cfg.embedding_dim = kDim;
        cfg.search_engine = VectorSearchEngine::Vec0L2;
        SqliteVecBackend backend(cfg);

        REQUIRE(backend.initialize(dbPath).has_value());
        REQUIRE(backend.createTables(kDim).has_value());

        std::vector<VectorRecord> records;
        for (size_t i = 0; i < 64; ++i) {
            records.push_back(makeRecord(i));
        }
        query = records[0].embedding;
        REQUIRE(backend.insertVectorsBatch(records).has_value());
        REQUIRE(backend.buildIndex().has_value());

        auto results = backend.searchSimilar(query, 5);
        REQUIRE(results.has_value());
        REQUIRE_FALSE(results.value().empty());

        sqlite3* db = backend.getDbHandle();
        REQUIRE(db != nullptr);
        const std::string shadow = "vectors_" + std::to_string(kDim) + "_hnsw_nodes";
        if (tableExists(db, shadow.c_str())) {
            std::string corrupt = "UPDATE \"" + shadow + "\" SET data = x'DEADBEEFDEADBEEF'";
            char* err = nullptr;
            REQUIRE(sqlite3_exec(db, corrupt.c_str(), nullptr, nullptr, &err) == SQLITE_OK);
            sqlite3_free(err);
        }
        backend.close();
    }

    {
        SqliteVecBackend::Config cfg;
        cfg.embedding_dim = kDim;
        cfg.search_engine = VectorSearchEngine::Vec0L2;
        SqliteVecBackend backend(cfg);

        REQUIRE(backend.initialize(dbPath).has_value());
        REQUIRE(backend.createTables(kDim).has_value());

        auto results = backend.searchSimilar(query, 5);
        REQUIRE(results.has_value());
        REQUIRE_FALSE(results.value().empty());
        CHECK(results.value().front().chunk_id == "chunk_0");
    }
}
