#include <catch2/catch_test_macros.hpp>

#include <sqlite3.h>
#include <filesystem>
#include <string>
#include <vector>

#include <yams/vector/sqlite_vec_backend.h>

using namespace yams::vector;

namespace {

struct HnswPersistenceFixture {
    ~HnswPersistenceFixture() {
        for (const auto& path : tempFiles) {
            std::error_code ec;
            std::filesystem::remove(path, ec);
        }
    }

    std::string createTempDbPath() {
        auto path = std::filesystem::temp_directory_path() /
                    ("sqlite_vec_hnsw_persistence_" + std::to_string(++counter) + ".db");
        tempFiles.push_back(path.string());
        return path.string();
    }

    std::vector<float> createEmbedding(size_t dim, float seed) {
        std::vector<float> emb(dim);
        for (size_t i = 0; i < dim; ++i) {
            emb[i] = seed + static_cast<float>(i + 1) * 0.001f;
        }
        float norm = 0.0f;
        for (float v : emb) {
            norm += v * v;
        }
        norm = std::sqrt(norm);
        for (float& v : emb) {
            v /= norm;
        }
        return emb;
    }

    VectorRecord createRecord(const std::string& id, const std::vector<float>& embedding) {
        VectorRecord rec;
        rec.chunk_id = "chunk_" + id;
        rec.document_hash = "doc_" + id;
        rec.embedding = embedding;
        rec.content = "content for " + id;
        rec.start_offset = 0;
        rec.end_offset = rec.content.size();
        return rec;
    }

    std::vector<std::string> tempFiles;
    static inline int counter = 0;
};

size_t countRows(sqlite3* db, const char* sql) {
    sqlite3_stmt* stmt = nullptr;
    REQUIRE(sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) == SQLITE_OK);
    REQUIRE(sqlite3_step(stmt) == SQLITE_ROW);
    size_t value = static_cast<size_t>(sqlite3_column_int64(stmt, 0));
    sqlite3_finalize(stmt);
    return value;
}

} // namespace

TEST_CASE_METHOD(HnswPersistenceFixture,
                 "SqliteVecBackend catches up persisted HNSW after deferred batch updates",
                 "[vector][hnsw][persistence][catch2]") {
    SqliteVecBackend::Config config;
    config.embedding_dim = 64;
    config.hnsw_ef_construction = 64;
    config.hnsw_ef_search = 32;

    const std::string dbPath = createTempDbPath();

    {
        SqliteVecBackend backend(config);
        REQUIRE(backend.initialize(dbPath).has_value());
        REQUIRE(backend.createTables(64).has_value());

        std::vector<VectorRecord> seedRecords;
        for (int i = 0; i < 32; ++i) {
            seedRecords.push_back(
                createRecord("seed_" + std::to_string(i), createEmbedding(64, 1.0f + i)));
        }
        REQUIRE(backend.insertVectorsBatch(seedRecords).has_value());
        REQUIRE(backend.buildIndex().has_value());
        CHECK(backend.testingLastHnswMaintenanceMode() ==
              SqliteVecBackend::HnswMaintenanceMode::FullRebuild);
    }

    sqlite3* db = nullptr;
    REQUIRE(sqlite3_open(dbPath.c_str(), &db) == SQLITE_OK);
    CHECK(countRows(db, "SELECT COUNT(*) FROM vectors_64_hnsw_meta") > 0);
    CHECK(countRows(db, "SELECT COUNT(*) FROM vectors_64_hnsw_nodes") > 0);
    sqlite3_close(db);

    {
        SqliteVecBackend backend(config);
        REQUIRE(backend.initialize(dbPath).has_value());

        std::vector<VectorRecord> deltaRecords;
        for (int i = 0; i < 20; ++i) {
            deltaRecords.push_back(
                createRecord("delta_" + std::to_string(i), createEmbedding(64, 100.0f + i)));
        }

        REQUIRE(backend.insertVectorsBatch(deltaRecords).has_value());

        auto query = createEmbedding(64, 100.0f + 10.0f);
        auto search = backend.searchSimilar(query, 20, -2.0f, std::nullopt, {});
        REQUIRE(search.has_value());

        CHECK(backend.testingLastHnswAddedCount() == 20);
        CHECK(backend.testingLastHnswRemovedCount() == 0);
        CHECK(backend.testingLastHnswMaintenanceMode() !=
              SqliteVecBackend::HnswMaintenanceMode::FullRebuild);
    }
}

TEST_CASE_METHOD(HnswPersistenceFixture,
                 "SqliteVecBackend prepareSearchIndex builds warm index from vectors table",
                 "[vector][hnsw][persistence][prepare][catch2]") {
    SqliteVecBackend::Config config;
    config.embedding_dim = 64;

    const std::string dbPath = createTempDbPath();

    {
        SqliteVecBackend backend(config);
        REQUIRE(backend.initialize(dbPath).has_value());
        REQUIRE(backend.createTables(64).has_value());

        std::vector<VectorRecord> seedRecords;
        for (int i = 0; i < 300; ++i) {
            seedRecords.push_back(
                createRecord("seed_prepare_" + std::to_string(i), createEmbedding(64, 10.0f + i)));
        }
        REQUIRE(backend.insertVectorsBatch(seedRecords).has_value());
    }

    {
        SqliteVecBackend backend(config);
        REQUIRE(backend.initialize(dbPath).has_value());
        REQUIRE(backend.prepareSearchIndex().has_value());

        auto query = createEmbedding(64, 25.0f);
        auto search = backend.searchSimilar(query, 10, -2.0f, std::nullopt, {});
        REQUIRE(search.has_value());
        CHECK(search.value().size() == 10);
        CHECK(backend.testingLastHnswMaintenanceMode() !=
              SqliteVecBackend::HnswMaintenanceMode::BruteForceFallback);
    }

    sqlite3* db = nullptr;
    REQUIRE(sqlite3_open(dbPath.c_str(), &db) == SQLITE_OK);
    CHECK(countRows(db, "SELECT COUNT(*) FROM vectors_64_hnsw_meta") > 0);
    CHECK(countRows(db, "SELECT COUNT(*) FROM vectors_64_hnsw_nodes") > 0);
    sqlite3_close(db);
}
