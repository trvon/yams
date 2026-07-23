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

    static bool columnExists(sqlite3* db, const char* table, const char* column) {
        const std::string sql = "PRAGMA table_info(" + std::string(table) + ")";
        sqlite3_stmt* stmt = nullptr;
        if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK) {
            return false;
        }
        bool present = false;
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            const auto* name = sqlite3_column_text(stmt, 1);
            if (name != nullptr && std::string_view(reinterpret_cast<const char*>(name)) == column) {
                present = true;
                break;
            }
        }
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
                 "ensurePersistenceSchema migrates legacy Simeon PQ metadata in place",
                 "[unit][vector][persistence][migration][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config cfg;
    cfg.embedding_dim = 64;
    cfg.search_engine = VectorSearchEngine::SimeonPqAdc;
    SqliteVecBackend backend(cfg);
    REQUIRE(backend.initialize(createTempDbPath()).has_value());
    REQUIRE(backend.createTables(cfg.embedding_dim).has_value());

    sqlite3* db = backend.getDbHandle();
    REQUIRE(db != nullptr);
    char* err = nullptr;
    REQUIRE(sqlite3_exec(db, "DROP TABLE simeon_pq_meta", nullptr, nullptr, &err) == SQLITE_OK);
    REQUIRE(sqlite3_exec(db,
                         "CREATE TABLE simeon_pq_meta ("
                         "dim INTEGER PRIMARY KEY, format_version INTEGER NOT NULL DEFAULT 1, "
                         "m INTEGER NOT NULL, k INTEGER NOT NULL, seed INTEGER NOT NULL, "
                         "rerank_factor INTEGER NOT NULL, trained INTEGER NOT NULL DEFAULT 0, "
                         "vector_count INTEGER NOT NULL, codebooks_blob BLOB NOT NULL, "
                         "updated_at INTEGER NOT NULL DEFAULT (unixepoch()))",
                         nullptr, nullptr, &err) == SQLITE_OK);
    REQUIRE_FALSE(columnExists(db, "simeon_pq_meta", "train_limit"));
    REQUIRE_FALSE(columnExists(db, "simeon_pq_meta", "source_generation"));

    REQUIRE(backend.ensurePersistenceSchema().has_value());

    CHECK(columnExists(db, "simeon_pq_meta", "train_limit"));
    CHECK(columnExists(db, "simeon_pq_meta", "source_generation"));
    CHECK(tableExists(db, "vector_index_generation"));
}
