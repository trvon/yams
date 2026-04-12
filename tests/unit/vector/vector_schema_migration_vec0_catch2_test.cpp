#include <catch2/catch_test_macros.hpp>

#include <sqlite3.h>

#include <cstring>
#include <filesystem>
#include <string>
#include <vector>

#include <yams/core/types.h>

#include <yams/vector/vector_schema_migration.h>

extern "C" int sqlite3_vec_init(sqlite3* db, char** pzErrMsg, const sqlite3_api_routines* pApi);

namespace yams::vector {
namespace {

namespace fs = std::filesystem;

struct SqliteDbHandle {
    sqlite3* db = nullptr;

    explicit SqliteDbHandle(const std::string& path) {
        REQUIRE(sqlite3_open(path.c_str(), &db) == SQLITE_OK);
        REQUIRE(db != nullptr);
    }

    ~SqliteDbHandle() {
        if (db) {
            sqlite3_close(db);
        }
    }
};

void exec(sqlite3* db, const char* sql) {
    char* err = nullptr;
    const int rc = sqlite3_exec(db, sql, nullptr, nullptr, &err);
    INFO(std::string(sql));
    INFO(std::string(err ? err : ""));
    REQUIRE(rc == SQLITE_OK);
    if (err) {
        sqlite3_free(err);
    }
}

bool tableExists(sqlite3* db, const char* tableName) {
    sqlite3_stmt* stmt = nullptr;
    REQUIRE(sqlite3_prepare_v2(db, "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1", -1,
                               &stmt, nullptr) == SQLITE_OK);
    sqlite3_bind_text(stmt, 1, tableName, -1, SQLITE_TRANSIENT);
    const bool exists = (sqlite3_step(stmt) == SQLITE_ROW);
    sqlite3_finalize(stmt);
    return exists;
}

std::int64_t countRows(sqlite3* db, const char* sql) {
    sqlite3_stmt* stmt = nullptr;
    REQUIRE(sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) == SQLITE_OK);
    REQUIRE(sqlite3_step(stmt) == SQLITE_ROW);
    const std::int64_t count = sqlite3_column_int64(stmt, 0);
    sqlite3_finalize(stmt);
    return count;
}

std::vector<float> readEmbedding(sqlite3* db, const char* sql) {
    sqlite3_stmt* stmt = nullptr;
    REQUIRE(sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) == SQLITE_OK);
    REQUIRE(sqlite3_step(stmt) == SQLITE_ROW);

    const void* blob = sqlite3_column_blob(stmt, 0);
    const int bytes = sqlite3_column_bytes(stmt, 0);
    REQUIRE(blob != nullptr);
    REQUIRE(bytes % static_cast<int>(sizeof(float)) == 0);

    std::vector<float> values(static_cast<size_t>(bytes) / sizeof(float));
    std::memcpy(values.data(), blob, static_cast<size_t>(bytes));
    sqlite3_finalize(stmt);
    return values;
}

void createLegacyVec0Schema(sqlite3* db) {
    REQUIRE(sqlite3_vec_init(db, nullptr, nullptr) == SQLITE_OK);
    exec(db, "CREATE VIRTUAL TABLE doc_embeddings USING vec0(embedding float[4])");
    exec(db, R"sql(
        CREATE TABLE doc_metadata (
            chunk_id TEXT PRIMARY KEY,
            document_hash TEXT NOT NULL,
            embedding_rowid INTEGER,
            chunk_text TEXT,
            metadata TEXT,
            model_id TEXT,
            created_at INTEGER
        )
    )sql");
}

} // namespace

TEST_CASE("VectorSchemaMigration migrates readable vec0 V1 data into V2",
          "[vector][migration][vec0][catch2]") {
    SqliteDbHandle db(":memory:");
    createLegacyVec0Schema(db.db);

    exec(db.db, "INSERT INTO doc_embeddings(rowid, embedding) VALUES (1, '[1,2,3,4]')");
    exec(db.db, "INSERT INTO doc_embeddings(rowid, embedding) VALUES (2, '[4,3,2,1]')");
    exec(db.db, R"sql(
        INSERT INTO doc_metadata(chunk_id, document_hash, embedding_rowid, chunk_text, metadata,
                                 model_id, created_at)
        VALUES
          ('chunk_a', 'doc_a', 1, 'alpha text', '{"kind":"a"}', 'model-a', 111),
          ('chunk_b', 'doc_b', 2, 'beta text',  '{"kind":"b"}', 'model-a', 222)
    )sql");

    REQUIRE(VectorSchemaMigration::detectVersion(db.db) ==
            VectorSchemaMigration::SchemaVersion::V1);

    auto migration = VectorSchemaMigration::migrateV1ToV2(db.db, 4);
    REQUIRE(migration.has_value());

    CHECK(VectorSchemaMigration::detectVersion(db.db) ==
          VectorSchemaMigration::SchemaVersion::V2_1);
    CHECK(VectorSchemaMigration::hasBackupTables(db.db));
    CHECK_FALSE(tableExists(db.db, "doc_embeddings"));
    CHECK_FALSE(tableExists(db.db, "doc_metadata"));
    CHECK(tableExists(db.db, "doc_embeddings_v1_backup"));
    CHECK(tableExists(db.db, "doc_metadata_v1_backup"));
    CHECK(countRows(db.db, "SELECT COUNT(*) FROM vectors") == 2);
    CHECK(countRows(db.db, "SELECT COUNT(*) FROM vectors_hnsw_meta") >= 0);

    auto embA = readEmbedding(db.db, "SELECT embedding FROM vectors WHERE chunk_id='chunk_a'");
    auto embB = readEmbedding(db.db, "SELECT embedding FROM vectors WHERE chunk_id='chunk_b'");
    REQUIRE(embA.size() == 4);
    REQUIRE(embB.size() == 4);
    CHECK(embA[0] == 1.0f);
    CHECK(embA[3] == 4.0f);
    CHECK(embB[0] == 4.0f);
    CHECK(embB[3] == 1.0f);
    CHECK(countRows(db.db, "SELECT COUNT(*) FROM vectors WHERE embedding_dim = 4") == 2);
}

TEST_CASE("VectorSchemaMigration preserves legacy tables when vec0 embeddings are unreadable",
          "[vector][migration][vec0][fallback][catch2]") {
    SqliteDbHandle db(":memory:");

    exec(db.db, R"sql(
        CREATE TABLE doc_embeddings (
            rowid INTEGER PRIMARY KEY,
            embedding TEXT
        )
    )sql");
    exec(db.db, R"sql(
        CREATE TABLE doc_metadata (
            chunk_id TEXT PRIMARY KEY,
            document_hash TEXT NOT NULL,
            embedding_rowid INTEGER,
            chunk_text TEXT,
            metadata TEXT,
            model_id TEXT,
            created_at INTEGER
        )
    )sql");
    exec(db.db, "INSERT INTO doc_embeddings(rowid, embedding) VALUES (1, 'not-a-vec0-blob')");
    exec(db.db, R"sql(
        INSERT INTO doc_metadata(chunk_id, document_hash, embedding_rowid, chunk_text, metadata,
                                 model_id, created_at)
        VALUES ('chunk_legacy', 'doc_legacy', 1, 'legacy text', '{}', 'legacy-model', 333)
    )sql");

    REQUIRE(VectorSchemaMigration::detectVersion(db.db) ==
            VectorSchemaMigration::SchemaVersion::V1);

    auto migration = VectorSchemaMigration::migrateV1ToV2(db.db, 4);
    REQUIRE(migration.has_value());

    CHECK(VectorSchemaMigration::detectVersion(db.db) ==
          VectorSchemaMigration::SchemaVersion::V2_1);
    CHECK_FALSE(VectorSchemaMigration::hasBackupTables(db.db));
    CHECK(VectorSchemaMigration::hasV1Tables(db.db));
    CHECK(tableExists(db.db, "doc_embeddings"));
    CHECK(tableExists(db.db, "doc_metadata"));
    CHECK(countRows(db.db, "SELECT COUNT(*) FROM vectors") == 0);
    CHECK(countRows(db.db, "SELECT COUNT(*) FROM vectors_hnsw_meta") >= 0);
}

} // namespace yams::vector
