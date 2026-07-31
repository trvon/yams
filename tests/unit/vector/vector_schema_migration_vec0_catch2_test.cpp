#include <catch2/catch_test_macros.hpp>

#include <sqlite3.h>

#include <chrono>
#include <cstring>
#include <filesystem>
#include <string>
#include <vector>

#include <yams/core/types.h>

#include <yams/vector/sqlite_vec_backend.h>
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

struct TempDbPath {
    fs::path path =
        fs::temp_directory_path() /
        ("yams-vector-schema-" +
         std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".db");

    ~TempDbPath() {
        std::error_code error;
        fs::remove(path, error);
        fs::remove(path.string() + "-wal", error);
        fs::remove(path.string() + "-shm", error);
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

bool columnExists(sqlite3* db, const char* tableName, const char* columnName) {
    sqlite3_stmt* stmt = nullptr;
    REQUIRE(sqlite3_prepare_v2(db, "SELECT 1 FROM pragma_table_info(?1) WHERE name=?2", -1, &stmt,
                               nullptr) == SQLITE_OK);
    sqlite3_bind_text(stmt, 1, tableName, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, columnName, -1, SQLITE_TRANSIENT);
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

std::string readText(sqlite3* db, const char* sql) {
    sqlite3_stmt* stmt = nullptr;
    REQUIRE(sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) == SQLITE_OK);
    REQUIRE(sqlite3_step(stmt) == SQLITE_ROW);
    const auto* value = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
    std::string result = value ? value : "";
    sqlite3_finalize(stmt);
    return result;
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

void createV2_2Schema(sqlite3* db) {
    exec(db, R"sql(
        CREATE TABLE vectors (
            rowid INTEGER PRIMARY KEY,
            chunk_id TEXT UNIQUE NOT NULL,
            document_hash TEXT NOT NULL,
            embedding BLOB,
            embedding_dim INTEGER,
            content TEXT,
            start_offset INTEGER DEFAULT 0,
            end_offset INTEGER DEFAULT 0,
            metadata TEXT,
            model_id TEXT,
            model_version TEXT,
            embedding_version INTEGER DEFAULT 1,
            content_hash TEXT,
            created_at INTEGER,
            embedded_at INTEGER,
            is_stale INTEGER DEFAULT 0,
            level INTEGER DEFAULT 0,
            source_chunk_ids TEXT,
            parent_document_hash TEXT,
            child_document_hashes TEXT,
            quantized_format INTEGER DEFAULT 0,
            quantized_bits INTEGER DEFAULT 0,
            quantized_seed INTEGER DEFAULT 0,
            quantized_packed_codes BLOB
        );
        CREATE INDEX idx_vectors_chunk_id ON vectors(chunk_id);
        CREATE INDEX idx_vectors_document_hash ON vectors(document_hash);
        CREATE INDEX idx_vectors_model ON vectors(model_id, model_version);
        CREATE INDEX idx_vectors_embedding_dim ON vectors(embedding_dim);
        CREATE INDEX idx_vectors_level ON vectors(level, document_hash);
        CREATE TABLE vector_index_generation (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            generation INTEGER NOT NULL DEFAULT 0
        );
        INSERT INTO vector_index_generation(id, generation) VALUES (1, 0);
        CREATE TRIGGER vectors_index_generation_insert
        AFTER INSERT ON vectors BEGIN
            UPDATE vector_index_generation SET generation = generation + 1 WHERE id = 1;
        END;
        CREATE TRIGGER vectors_index_generation_update_v2
        AFTER UPDATE OF chunk_id, document_hash, embedding, embedding_dim, quantized_format,
                        quantized_bits, quantized_seed, quantized_packed_codes ON vectors BEGIN
            UPDATE vector_index_generation SET generation = generation + 1 WHERE id = 1;
        END;
        CREATE TRIGGER vectors_index_generation_delete
        AFTER DELETE ON vectors BEGIN
            UPDATE vector_index_generation SET generation = generation + 1 WHERE id = 1;
        END;
        CREATE TABLE turboquant_quantizer_meta (
            rowid INTEGER PRIMARY KEY,
            dim INTEGER NOT NULL,
            bits INTEGER NOT NULL,
            seed INTEGER NOT NULL,
            fit_version INTEGER NOT NULL DEFAULT 1,
            per_coord_scales BLOB,
            per_coord_centroids BLOB,
            UNIQUE(dim, bits, seed)
        );
    )sql");
}

void insertFloatV2_2Row(sqlite3* db, std::int64_t rowid = 41) {
    const std::string sql =
        "INSERT INTO vectors(rowid, chunk_id, document_hash, embedding, embedding_dim, content, "
        "start_offset, end_offset, metadata, model_id, model_version, embedding_version, "
        "content_hash, created_at, embedded_at, is_stale, level, source_chunk_ids, "
        "parent_document_hash, child_document_hashes, quantized_format, quantized_bits, "
        "quantized_seed, quantized_packed_codes) VALUES (" +
        std::to_string(rowid) +
        ", 'chunk_a', 'doc_a', X'0000803F000000400000404000008040', 4, 'alpha', 3, 9, "
        "'{\"kind\":\"kept\"}', 'model-a', 'v1', 7, 'content-hash', 111, 222, 0, 1, "
        "'[\"source\"]', 'parent', '[\"child\"]', 0, 0, 0, NULL)";
    exec(db, sql.c_str());
}

} // namespace

TEST_CASE("VectorSchemaMigration retires unused V2.2 quantized schema atomically",
          "[vector][migration][turboquant][catch2]") {
    SqliteDbHandle db(":memory:");
    createV2_2Schema(db.db);
    insertFloatV2_2Row(db.db);
    exec(db.db, "UPDATE vector_index_generation SET generation=17 WHERE id=1");

    auto migration = VectorSchemaMigration::migrateV2_2ToV2_3(db.db);
    REQUIRE(migration.has_value());

    CHECK(VectorSchemaMigration::detectVersion(db.db) ==
          VectorSchemaMigration::SchemaVersion::V2_3);
    CHECK_FALSE(columnExists(db.db, "vectors", "quantized_format"));
    CHECK_FALSE(columnExists(db.db, "vectors", "quantized_packed_codes"));
    CHECK_FALSE(tableExists(db.db, "turboquant_quantizer_meta"));
    CHECK(countRows(db.db, "SELECT COUNT(*) FROM vectors") == 1);
    CHECK(countRows(db.db, "SELECT rowid FROM vectors WHERE chunk_id='chunk_a'") == 41);
    CHECK(readText(db.db, "SELECT hex(embedding) FROM vectors WHERE rowid=41") ==
          "0000803F000000400000404000008040");
    CHECK(readText(db.db, "SELECT metadata FROM vectors WHERE rowid=41") == "{\"kind\":\"kept\"}");
    CHECK(countRows(db.db,
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND "
                    "name IN ('idx_vectors_chunk_id','idx_vectors_document_hash',"
                    "'idx_vectors_model','idx_vectors_embedding_dim','idx_vectors_level')") == 5);
    CHECK(countRows(db.db, "SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND "
                           "name IN ('vectors_index_generation_insert',"
                           "'vectors_index_generation_update_v2',"
                           "'vectors_index_generation_delete')") == 3);
    CHECK(countRows(db.db, "SELECT generation FROM vector_index_generation WHERE id=1") == 17);

    exec(db.db, "INSERT INTO vectors(chunk_id, document_hash, embedding, embedding_dim) "
                "VALUES ('chunk_b','doc_b',X'0000803F',1)");
    exec(db.db, "UPDATE vectors SET document_hash='doc_b2' WHERE chunk_id='chunk_b'");
    exec(db.db, "DELETE FROM vectors WHERE chunk_id='chunk_b'");
    CHECK(countRows(db.db, "SELECT generation FROM vector_index_generation WHERE id=1") == 20);
}

TEST_CASE("VectorSchemaMigration rejects nonzero quantized format without mutation",
          "[vector][migration][turboquant][guard][catch2]") {
    SqliteDbHandle db(":memory:");
    createV2_2Schema(db.db);
    insertFloatV2_2Row(db.db);
    exec(db.db, "UPDATE vectors SET quantized_format=1 WHERE rowid=41");

    auto migration = VectorSchemaMigration::migrateV2_2ToV2_3(db.db);
    REQUIRE_FALSE(migration.has_value());
    CHECK(migration.error().message.find("pre-removal YAMS release") != std::string::npos);
    CHECK(columnExists(db.db, "vectors", "quantized_format"));
    CHECK(tableExists(db.db, "turboquant_quantizer_meta"));
    CHECK(countRows(db.db, "SELECT quantized_format FROM vectors WHERE rowid=41") == 1);
    CHECK_FALSE(tableExists(db.db, "vectors_v2_3"));
}

TEST_CASE("VectorSchemaMigration rejects packed codes when format is zero",
          "[vector][migration][turboquant][guard][catch2]") {
    SqliteDbHandle db(":memory:");
    createV2_2Schema(db.db);
    insertFloatV2_2Row(db.db);
    exec(db.db,
         "UPDATE vectors SET quantized_format=0, quantized_packed_codes=X'0102' WHERE rowid=41");

    auto migration = VectorSchemaMigration::migrateV2_2ToV2_3(db.db);
    REQUIRE_FALSE(migration.has_value());
    CHECK(migration.error().message.find("export/regenerate float embeddings") !=
          std::string::npos);
    CHECK(columnExists(db.db, "vectors", "quantized_packed_codes"));
    CHECK(readText(db.db, "SELECT hex(quantized_packed_codes) FROM vectors WHERE rowid=41") ==
          "0102");
    CHECK_FALSE(tableExists(db.db, "vectors_v2_3"));
}

TEST_CASE("VectorSchemaMigration rolls back a failed V2.3 rebuild",
          "[vector][migration][turboquant][rollback][catch2]") {
    SqliteDbHandle db(":memory:");
    createV2_2Schema(db.db);
    insertFloatV2_2Row(db.db);
    exec(db.db, "CREATE TABLE vectors_v2_3(blocker INTEGER)");

    auto migration = VectorSchemaMigration::migrateV2_2ToV2_3(db.db);
    REQUIRE_FALSE(migration.has_value());
    CHECK(columnExists(db.db, "vectors", "quantized_format"));
    CHECK(tableExists(db.db, "turboquant_quantizer_meta"));
    CHECK(countRows(db.db, "SELECT COUNT(*) FROM vectors WHERE rowid=41") == 1);
    CHECK(tableExists(db.db, "vectors_v2_3"));
}

TEST_CASE("VectorSchemaMigration rejects custom vector schema objects",
          "[vector][migration][turboquant][guard][catch2]") {
    SqliteDbHandle db(":memory:");
    createV2_2Schema(db.db);
    insertFloatV2_2Row(db.db);
    exec(db.db, "CREATE INDEX custom_vectors_content ON vectors(content)");

    auto migration = VectorSchemaMigration::migrateV2_2ToV2_3(db.db);
    REQUIRE_FALSE(migration.has_value());
    CHECK(migration.error().message.find("custom schema object") != std::string::npos);
    CHECK(columnExists(db.db, "vectors", "quantized_format"));
    CHECK(tableExists(db.db, "turboquant_quantizer_meta"));
    CHECK(countRows(db.db, "SELECT COUNT(*) FROM sqlite_master WHERE "
                           "name='custom_vectors_content'") == 1);
}

TEST_CASE("SqliteVecBackend retires unused V2.2 storage before preparing statements",
          "[vector][migration][turboquant][backend][catch2]") {
    TempDbPath temp;
    {
        SqliteDbHandle db(temp.path.string());
        createV2_2Schema(db.db);
        insertFloatV2_2Row(db.db);
    }

    SqliteVecBackend backend;
    REQUIRE(backend.initialize(temp.path.string()).has_value());
    CHECK(VectorSchemaMigration::detectVersion(backend.getDbHandle()) ==
          VectorSchemaMigration::SchemaVersion::V2_3);
    auto record = backend.getVector("chunk_a");
    REQUIRE(record.has_value());
    REQUIRE(record.value().has_value());
    CHECK(record.value()->embedding.size() == 4);
}

TEST_CASE("SqliteVecBackend rejects packed V2.2 storage before preparing statements",
          "[vector][migration][turboquant][backend][guard][catch2]") {
    TempDbPath temp;
    {
        SqliteDbHandle db(temp.path.string());
        createV2_2Schema(db.db);
        insertFloatV2_2Row(db.db);
        exec(db.db, "UPDATE vectors SET quantized_packed_codes=X'0102' WHERE rowid=41");
    }

    SqliteVecBackend backend;
    auto initialization = backend.initialize(temp.path.string());
    REQUIRE_FALSE(initialization.has_value());
    CHECK(initialization.error().message.find("pre-removal YAMS release") != std::string::npos);

    SqliteDbHandle db(temp.path.string());
    CHECK(columnExists(db.db, "vectors", "quantized_packed_codes"));
    CHECK(readText(db.db, "SELECT hex(quantized_packed_codes) FROM vectors WHERE rowid=41") ==
          "0102");
}

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
          VectorSchemaMigration::SchemaVersion::V2_3);
    CHECK(VectorSchemaMigration::hasBackupTables(db.db));
    CHECK_FALSE(tableExists(db.db, "doc_embeddings"));
    CHECK_FALSE(tableExists(db.db, "doc_metadata"));
    CHECK(tableExists(db.db, "doc_embeddings_v1_backup"));
    CHECK(tableExists(db.db, "doc_metadata_v1_backup"));
    CHECK(countRows(db.db, "SELECT COUNT(*) FROM vectors") == 2);
    // HNSW shadow tables are no longer created by V1→V2 migration; the active
    // search-index state is owned by the current backend (vec0 or Simeon PQ).
    CHECK_FALSE(tableExists(db.db, "vectors_hnsw_meta"));

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
          VectorSchemaMigration::SchemaVersion::V2_3);
    CHECK_FALSE(VectorSchemaMigration::hasBackupTables(db.db));
    CHECK(VectorSchemaMigration::hasV1Tables(db.db));
    CHECK(tableExists(db.db, "doc_embeddings"));
    CHECK(tableExists(db.db, "doc_metadata"));
    CHECK(countRows(db.db, "SELECT COUNT(*) FROM vectors") == 0);
    CHECK_FALSE(tableExists(db.db, "vectors_hnsw_meta"));
}

} // namespace yams::vector
