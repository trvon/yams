#include <yams/vector/vector_schema_migration.h>

#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_set>

#include <sqlite3.h>
#include <spdlog/spdlog.h>

#include <yams/core/types.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/storage/sqlite_retry.h>

namespace yams::vector {

namespace {

// Check if a table exists
bool tableExists(sqlite3* db, const char* table_name) {
    const char* sql = "SELECT name FROM sqlite_master WHERE type='table' AND name=?";
    sqlite3_stmt* stmt = nullptr;

    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        return false;
    }

    sqlite3_bind_text(stmt, 1, table_name, -1, SQLITE_TRANSIENT);
    bool exists = (sqlite3_step(stmt) == SQLITE_ROW);
    sqlite3_finalize(stmt);

    return exists;
}

// Execute SQL statement with retry for transient lock errors
Result<void> executeSQL(sqlite3* db, const char* sql) {
    const auto retryPolicy = yams::storage::sqlite_retry::vectorWritePolicy();
    auto backoff = retryPolicy.initialBackoff;

    for (int attempt = 0; attempt < retryPolicy.maxRetries; ++attempt) {
        char* err_msg = nullptr;
        int rc = sqlite3_exec(db, sql, nullptr, nullptr, &err_msg);

        if (rc == SQLITE_OK) {
            return Result<void>{};
        }

        std::string err = err_msg ? err_msg : "Unknown error";
        sqlite3_free(err_msg);

        // Check for transient lock errors
        if (yams::storage::sqlite_retry::canRetry(rc, attempt, retryPolicy)) {
            spdlog::debug("[Migration] transient lock on '{}', retry {}/{}", sql, attempt + 1,
                          retryPolicy.maxRetries);
            yams::storage::sqlite_retry::sleepAndBackoff(backoff);
            continue;
        }

        return Error{ErrorCode::DatabaseError, std::move(err)};
    }

    daemon::TuneAdvisor::reportDbLockError(); // Signal contention for adaptive scaling
    return Error{ErrorCode::DatabaseError, "Max retries exceeded"};
}

// Begin transaction with backend-appropriate semantics
Result<void> beginTransaction(sqlite3* db) {
#if YAMS_LIBSQL_BACKEND
    // libsql MVCC: use regular BEGIN (deferred) for better concurrency
    return executeSQL(db, "BEGIN");
#else
    // SQLite: use BEGIN IMMEDIATE to acquire write lock immediately
    return executeSQL(db, "BEGIN IMMEDIATE");
#endif
}

class TransactionGuard {
public:
    explicit TransactionGuard(sqlite3* db) : db_(db) {}

    TransactionGuard(const TransactionGuard&) = delete;
    TransactionGuard& operator=(const TransactionGuard&) = delete;

    ~TransactionGuard() {
        if (active_) {
            (void)executeSQL(db_, "ROLLBACK");
        }
    }

    Result<void> begin() {
        auto result = beginTransaction(db_);
        active_ = result.has_value();
        return result;
    }

    Result<void> commit() {
        auto result = executeSQL(db_, "COMMIT");
        if (result) {
            active_ = false;
        }
        return result;
    }

private:
    sqlite3* db_;
    bool active_ = false;
};

Result<std::int64_t> queryInt64(sqlite3* db, const char* sql) {
    sqlite3_stmt* stmt = nullptr;
    const int prepareRc = sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr);
    if (prepareRc != SQLITE_OK) {
        return Error{ErrorCode::DatabaseError,
                     "Failed to prepare migration query: " + std::string(sqlite3_errmsg(db))};
    }

    const int stepRc = sqlite3_step(stmt);
    if (stepRc != SQLITE_ROW) {
        const std::string message = sqlite3_errmsg(db);
        sqlite3_finalize(stmt);
        return Error{ErrorCode::DatabaseError, "Failed to execute migration query: " + message};
    }

    const std::int64_t value = sqlite3_column_int64(stmt, 0);
    sqlite3_finalize(stmt);
    return value;
}

Result<void> rejectUnsupportedVectorSchemaObjects(sqlite3* db) {
    static const std::unordered_set<std::string_view> supportedObjects = {
        "idx_vectors_chunk_id",
        "idx_vectors_document_hash",
        "idx_vectors_model",
        "idx_vectors_embedding_dim",
        "idx_vectors_level",
        "vectors_index_generation_insert",
        "vectors_index_generation_update",
        "vectors_index_generation_update_v2",
        "vectors_index_generation_delete",
    };

    constexpr const char* sql = R"sql(
        SELECT type, name
        FROM sqlite_master
        WHERE sql IS NOT NULL
          AND (
              (tbl_name = 'vectors' AND type IN ('index', 'trigger'))
              OR (type = 'view' AND lower(sql) LIKE '%vectors%')
          )
    )sql";

    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        return Error{ErrorCode::DatabaseError,
                     "Failed to inspect vector schema objects: " + std::string(sqlite3_errmsg(db))};
    }

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const auto* type = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
        const auto* name = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
        const std::string_view objectType = type ? type : "";
        const std::string_view objectName = name ? name : "";
        if (objectType == "view" || !supportedObjects.contains(objectName)) {
            const std::string message =
                "Cannot retire TurboQuant because custom schema object '" +
                std::string(objectName) + "' (" + std::string(objectType) +
                ") depends on vectors; remove or migrate it explicitly, then retry";
            sqlite3_finalize(stmt);
            return Error{ErrorCode::InvalidState, message};
        }
    }

    const int rc = sqlite3_finalize(stmt);
    if (rc != SQLITE_OK) {
        return Error{ErrorCode::DatabaseError, "Failed while inspecting vector schema objects: " +
                                                   std::string(sqlite3_errmsg(db))};
    }
    return Result<void>{};
}

Result<void> copyVectorsToCanonicalTable(sqlite3* db, std::int64_t expectedRows) {
    constexpr const char* createVectors = R"sql(
        CREATE TABLE vectors_v2_3 (
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
            child_document_hashes TEXT
        )
    )sql";
    auto result = executeSQL(db, createVectors);
    if (!result) {
        return Error{ErrorCode::DatabaseError,
                     "Failed to create replacement vectors table: " + result.error().message};
    }

    constexpr const char* copyVectors = R"sql(
        INSERT INTO vectors_v2_3 (
            rowid, chunk_id, document_hash, embedding, embedding_dim, content,
            start_offset, end_offset, metadata, model_id, model_version,
            embedding_version, content_hash, created_at, embedded_at, is_stale,
            level, source_chunk_ids, parent_document_hash, child_document_hashes
        )
        SELECT
            rowid, chunk_id, document_hash, embedding, embedding_dim, content,
            start_offset, end_offset, metadata, model_id, model_version,
            embedding_version, content_hash, created_at, embedded_at, is_stale,
            level, source_chunk_ids, parent_document_hash, child_document_hashes
        FROM vectors
    )sql";
    result = executeSQL(db, copyVectors);
    if (!result) {
        return Error{ErrorCode::DatabaseError,
                     "Failed to copy vectors into replacement table: " + result.error().message};
    }

    auto copiedRows = queryInt64(db, "SELECT COUNT(*) FROM vectors_v2_3");
    if (!copiedRows) {
        return copiedRows.error();
    }
    if (copiedRows.value() != expectedRows) {
        return Error{ErrorCode::DatabaseError,
                     "Vector schema migration copied an unexpected number of rows"};
    }
    return Result<void>{};
}

Result<void> installCanonicalVectorSchema(sqlite3* db) {
    auto result = executeSQL(db, R"sql(
        DROP TABLE vectors;
        ALTER TABLE vectors_v2_3 RENAME TO vectors;
        CREATE INDEX idx_vectors_chunk_id ON vectors(chunk_id);
        CREATE INDEX idx_vectors_document_hash ON vectors(document_hash);
        CREATE INDEX idx_vectors_model ON vectors(model_id, model_version);
        CREATE INDEX idx_vectors_embedding_dim ON vectors(embedding_dim);
        CREATE INDEX idx_vectors_level ON vectors(level, document_hash);
        CREATE TABLE IF NOT EXISTS vector_index_generation (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            generation INTEGER NOT NULL DEFAULT 0
        );
        INSERT OR IGNORE INTO vector_index_generation(id, generation) VALUES (1, 0);
        CREATE TRIGGER vectors_index_generation_insert
        AFTER INSERT ON vectors BEGIN
            UPDATE vector_index_generation SET generation = generation + 1 WHERE id = 1;
        END;
        CREATE TRIGGER vectors_index_generation_update_v2
        AFTER UPDATE OF chunk_id, document_hash, embedding, embedding_dim ON vectors BEGIN
            UPDATE vector_index_generation SET generation = generation + 1 WHERE id = 1;
        END;
        CREATE TRIGGER vectors_index_generation_delete
        AFTER DELETE ON vectors BEGIN
            UPDATE vector_index_generation SET generation = generation + 1 WHERE id = 1;
        END;
        DROP TABLE IF EXISTS turboquant_quantizer_meta;
    )sql");
    if (!result) {
        return Error{ErrorCode::DatabaseError,
                     "Failed to install canonical vector schema: " + result.error().message};
    }
    return Result<void>{};
}

Result<void> verifyDatabaseIntegrity(sqlite3* db) {
    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db, "PRAGMA quick_check", -1, &stmt, nullptr) != SQLITE_OK) {
        return Error{ErrorCode::DatabaseError,
                     "Failed to prepare post-migration integrity check: " +
                         std::string(sqlite3_errmsg(db))};
    }

    const int stepRc = sqlite3_step(stmt);
    const auto* text = stepRc == SQLITE_ROW
                           ? reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0))
                           : nullptr;
    const bool integrityOk = text && std::string_view(text) == "ok";
    sqlite3_finalize(stmt);
    if (!integrityOk) {
        return Error{ErrorCode::DatabaseError,
                     "Post-migration SQLite integrity check did not return ok"};
    }
    return Result<void>{};
}

// Check if vec0 module is available (needed to read V1 embeddings)
bool isVec0Available(sqlite3* db) {
    // Check if we can read embeddings from the V1 vec0 virtual table.
    // The original sqlite-vec (C library) stores data in a chunked format with
    // shadow tables like _vector_chunks00. Our C++ implementation uses different
    // shadow tables (_vectors, _metadata).
    //
    // We can only read V1 data if:
    // 1. The original sqlite-vec extension is loaded (rarely the case in migration), OR
    // 2. We implement reading from the original shadow tables (complex)
    //
    // For now, we detect if we can actually read an embedding value. If the
    // virtual table returns NULL embeddings, the migration won't work.

    sqlite3_stmt* stmt = nullptr;
    // Query the embedding column - see if it returns actual data
    const char* sql = "SELECT embedding FROM doc_embeddings LIMIT 1";
    int rc = sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr);

    if (rc != SQLITE_OK) {
        spdlog::info("[Migration] vec0 check: prepare failed - {} (rc={})", sqlite3_errmsg(db), rc);
        return false;
    }

    rc = sqlite3_step(stmt);
    if (rc != SQLITE_ROW) {
        // No data or error
        spdlog::info("[Migration] vec0 check: no rows or error (rc={})", rc);
        sqlite3_finalize(stmt);
        return false;
    }

    // Check if embedding column has actual data
    int blob_size = sqlite3_column_bytes(stmt, 0);
    bool has_data = (blob_size > 0 && sqlite3_column_type(stmt, 0) == SQLITE_BLOB);
    spdlog::info("[Migration] vec0 check: step rc={}, blob_size={}, has_data={}", rc, blob_size,
                 has_data);
    sqlite3_finalize(stmt);

    return has_data;
}

// Check if a column exists in a table
bool columnExists(sqlite3* db, const char* table_name, const char* column_name) {
    // Use PRAGMA table_info to check for column
    std::string sql = "SELECT COUNT(*) FROM pragma_table_info('" + std::string(table_name) +
                      "') WHERE name = '" + std::string(column_name) + "'";
    sqlite3_stmt* stmt = nullptr;

    if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK) {
        return false;
    }

    bool exists = false;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        exists = (sqlite3_column_int(stmt, 0) > 0);
    }
    sqlite3_finalize(stmt);

    return exists;
}

} // namespace

VectorSchemaMigration::SchemaVersion VectorSchemaMigration::detectVersion(sqlite3* db) {
    if (!db) {
        return SchemaVersion::Unknown;
    }

    // Check for V2+ schema (unified vectors table). Legacy HNSW shadow tables
    // no longer exist on fresh databases, so detection depends on the vectors
    // table plus its column markers.
    if (tableExists(db, "vectors")) {
        if (columnExists(db, "vectors", "quantized_packed_codes")) {
            return SchemaVersion::V2_2;
        }
        if (columnExists(db, "vectors", "embedding_dim")) {
            return SchemaVersion::V2_3;
        }
        return SchemaVersion::V2;
    }

    // Check for V1 schema (dual tables)
    if (tableExists(db, "doc_embeddings") && tableExists(db, "doc_metadata")) {
        return SchemaVersion::V1;
    }

    return SchemaVersion::Unknown;
}

bool VectorSchemaMigration::hasEmbeddingDimColumn(sqlite3* db) {
    if (!db) {
        return false;
    }
    return columnExists(db, "vectors", "embedding_dim");
}

Result<void> VectorSchemaMigration::migrateV2ToV2_1(sqlite3* db) {
    if (!db) {
        return Error{ErrorCode::InvalidArgument, "Database handle is null"};
    }

    // Check if already migrated
    if (hasEmbeddingDimColumn(db)) {
        spdlog::debug("Database already has embedding_dim column, skipping migration");
        return Result<void>{};
    }

    // Check that we have a V2 schema
    auto version = detectVersion(db);
    if (version != SchemaVersion::V2) {
        return Error{ErrorCode::InvalidState,
                     "Cannot migrate to V2.1: database is not at V2 schema"};
    }

    spdlog::info("Starting V2 to V2.1 schema migration (adding embedding_dim column)...");

    // Begin transaction
    auto result = beginTransaction(db);
    if (!result) {
        return result;
    }

    // 1. Add embedding_dim column
    result = executeSQL(db, "ALTER TABLE vectors ADD COLUMN embedding_dim INTEGER");
    if (!result) {
        executeSQL(db, "ROLLBACK");
        return Error{ErrorCode::DatabaseError,
                     "Failed to add embedding_dim column: " + result.error().message};
    }

    // 2. Backfill dimension from BLOB size
    result = executeSQL(db, "UPDATE vectors SET embedding_dim = LENGTH(embedding) / 4");
    if (!result) {
        executeSQL(db, "ROLLBACK");
        return Error{ErrorCode::DatabaseError,
                     "Failed to backfill embedding_dim: " + result.error().message};
    }

    // 3. Create index for efficient dimension queries
    result = executeSQL(
        db, "CREATE INDEX IF NOT EXISTS idx_vectors_embedding_dim ON vectors(embedding_dim)");
    if (!result) {
        executeSQL(db, "ROLLBACK");
        return Error{ErrorCode::DatabaseError,
                     "Failed to create embedding_dim index: " + result.error().message};
    }

    // Get count of migrated records
    sqlite3_stmt* count_stmt = nullptr;
    if (sqlite3_prepare_v2(db, "SELECT COUNT(*), COUNT(DISTINCT embedding_dim) FROM vectors", -1,
                           &count_stmt, nullptr) == SQLITE_OK) {
        if (sqlite3_step(count_stmt) == SQLITE_ROW) {
            int64_t count = sqlite3_column_int64(count_stmt, 0);
            int64_t distinct_dims = sqlite3_column_int64(count_stmt, 1);
            spdlog::info("Backfilled embedding_dim for {} vectors ({} distinct dimensions)", count,
                         distinct_dims);
        }
        sqlite3_finalize(count_stmt);
    }

    // Commit transaction
    result = executeSQL(db, "COMMIT");
    if (!result) {
        return Error{ErrorCode::DatabaseError,
                     "Failed to commit migration: " + result.error().message};
    }

    spdlog::info("V2 to V2.1 migration completed successfully");
    return Result<void>{};
}

Result<void> VectorSchemaMigration::migrateV2_2ToV2_3(sqlite3* db) {
    if (!db) {
        return Error{ErrorCode::InvalidArgument, "Database handle is null"};
    }

    const auto version = detectVersion(db);
    if (version == SchemaVersion::V2_3) {
        return Result<void>{};
    }
    if (version != SchemaVersion::V2_2) {
        return Error{ErrorCode::InvalidState,
                     "Cannot retire TurboQuant: database is not at V2.2 schema"};
    }

    auto packedRows = queryInt64(db, "SELECT COUNT(*) FROM vectors "
                                     "WHERE COALESCE(quantized_format, 0) != 0 "
                                     "OR quantized_packed_codes IS NOT NULL");
    if (!packedRows) {
        return packedRows.error();
    }
    if (packedRows.value() != 0) {
        return Error{
            ErrorCode::InvalidState,
            "Cannot retire TurboQuant while packed vector data exists. Reopen this database "
            "with a pre-removal YAMS release, export/regenerate float embeddings, then retry."};
    }

    auto schemaCheck = rejectUnsupportedVectorSchemaObjects(db);
    if (!schemaCheck) {
        return schemaCheck;
    }

    auto rowCount = queryInt64(db, "SELECT COUNT(*) FROM vectors");
    if (!rowCount) {
        return rowCount.error();
    }

    TransactionGuard transaction(db);
    auto result = transaction.begin();
    if (!result) {
        return result;
    }

    result = copyVectorsToCanonicalTable(db, rowCount.value());
    if (!result) {
        return result;
    }

    result = installCanonicalVectorSchema(db);
    if (!result) {
        return result;
    }

    result = verifyDatabaseIntegrity(db);
    if (!result) {
        return result;
    }

    result = transaction.commit();
    if (!result) {
        return Error{ErrorCode::DatabaseError,
                     "Failed to commit TurboQuant retirement: " + result.error().message};
    }

    spdlog::info("Retired unused TurboQuant vector storage; schema is now V2.3");
    return Result<void>{};
}

Result<void> VectorSchemaMigration::migrateV1ToV2(sqlite3* db,
                                                  [[maybe_unused]] size_t embedding_dim) {
    if (!db) {
        return Error{ErrorCode::InvalidArgument, "Database handle is null"};
    }

    spdlog::info("Starting V1 to V2 schema migration...");

    // Check current version
    auto version = detectVersion(db);
    if (version == SchemaVersion::V2) {
        spdlog::info("Database already at V2 schema, upgrading to V2.1...");
        return migrateV2ToV2_1(db);
    }

    if (version == SchemaVersion::V2_3) {
        spdlog::info("Database already at canonical float-only schema, skipping migration");
        return Result<void>{};
    }

    if (version != SchemaVersion::V1) {
        return Error{ErrorCode::InvalidState, "Cannot migrate: database is not at V1 schema"};
    }

    // Begin transaction
    auto result = beginTransaction(db);
    if (!result) {
        return result;
    }

    // 1. Create V2 tables
    spdlog::info("Creating V2 schema tables...");

    const char* create_vectors = R"sql(
        CREATE TABLE IF NOT EXISTS vectors (
            rowid INTEGER PRIMARY KEY,
            chunk_id TEXT UNIQUE NOT NULL,
            document_hash TEXT NOT NULL,
            embedding BLOB NOT NULL,
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
            child_document_hashes TEXT
        )
    )sql";

    result = executeSQL(db, create_vectors);
    if (!result) {
        executeSQL(db, "ROLLBACK");
        return Error{ErrorCode::DatabaseError,
                     "Failed to create vectors table: " + result.error().message};
    }

    result = executeSQL(db, "CREATE INDEX IF NOT EXISTS idx_vectors_chunk_id ON vectors(chunk_id)");
    if (!result) {
        executeSQL(db, "ROLLBACK");
        return result;
    }

    result = executeSQL(
        db, "CREATE INDEX IF NOT EXISTS idx_vectors_document_hash ON vectors(document_hash)");
    if (!result) {
        executeSQL(db, "ROLLBACK");
        return result;
    }

    result = executeSQL(
        db, "CREATE INDEX IF NOT EXISTS idx_vectors_embedding_dim ON vectors(embedding_dim)");
    if (!result) {
        executeSQL(db, "ROLLBACK");
        return result;
    }

    // Fresh unified-schema databases no longer create HNSW shadow tables here.
    // Active search-index state is owned by the current backend (vec0 aux tables or
    // Simeon PQ persistence) and initialized later through normal backend flows.

    // Create entity_vectors table for symbol/entity embeddings
    const char* create_entity_vectors = R"sql(
        CREATE TABLE IF NOT EXISTS entity_vectors (
            rowid INTEGER PRIMARY KEY,
            node_key TEXT NOT NULL,
            embedding_type TEXT NOT NULL,
            embedding BLOB NOT NULL,
            content TEXT,
            model_id TEXT,
            model_version TEXT,
            embedded_at INTEGER,
            is_stale INTEGER DEFAULT 0,
            node_type TEXT,
            qualified_name TEXT,
            file_path TEXT,
            document_hash TEXT,
            UNIQUE(node_key, embedding_type)
        )
    )sql";
    result = executeSQL(db, create_entity_vectors);
    if (!result) {
        executeSQL(db, "ROLLBACK");
        return Error{ErrorCode::DatabaseError,
                     "Failed to create entity_vectors table: " + result.error().message};
    }

    result = executeSQL(
        db, "CREATE INDEX IF NOT EXISTS idx_entity_vectors_node_key ON entity_vectors(node_key)");
    if (!result) {
        executeSQL(db, "ROLLBACK");
        return result;
    }

    result = executeSQL(
        db,
        "CREATE INDEX IF NOT EXISTS idx_entity_vectors_document ON entity_vectors(document_hash)");
    if (!result) {
        executeSQL(db, "ROLLBACK");
        return result;
    }

    // 2. Copy data from V1 to V2 (only if vec0 module is available)
    spdlog::info("Migrating data from V1 to V2 schema...");

    // Check if vec0 module is available to read V1 data
    bool vec0_available = isVec0Available(db);

    if (!vec0_available) {
        spdlog::warn("vec0 module not available - cannot read V1 embeddings");
        spdlog::warn(
            "V1 tables will be preserved as backup; embeddings will need to be regenerated");
        // Skip data copy - V2 tables are created but empty
        // Old V1 tables remain as-is (not renamed to backup)

        // Just commit the new table creation
        result = executeSQL(db, "COMMIT");
        if (!result) {
            return Error{ErrorCode::DatabaseError,
                         "Failed to commit migration: " + result.error().message};
        }

        spdlog::info("V2 schema created (empty). Embeddings will be regenerated on next ingest.");
        spdlog::info(
            "V1 tables preserved: doc_embeddings, doc_metadata (run removeV1Tables() to clean up)");
        return Result<void>{};
    }

    // Query to copy data - join doc_metadata with doc_embeddings
    // V1 schema stores embedding in doc_embeddings virtual table (vec0)
    // We need to read the raw embedding blob

    const char* copy_sql = R"sql(
        INSERT INTO vectors (
            chunk_id, document_hash, embedding, embedding_dim, content, metadata,
            model_id, created_at, embedding_version
        )
        SELECT
            m.chunk_id,
            m.document_hash,
            e.embedding,
            LENGTH(e.embedding) / 4,
            m.chunk_text,
            m.metadata,
            m.model_id,
            m.created_at,
            1
        FROM doc_metadata m
        JOIN doc_embeddings e ON m.embedding_rowid = e.rowid
    )sql";

    result = executeSQL(db, copy_sql);
    if (!result) {
        // Try alternate approach - vec0 may store differently
        spdlog::warn("Direct copy failed, trying alternate approach: {}", result.error().message);

        // For vec0 tables, we may need to query differently
        // The embedding column in vec0 is accessed via the virtual table
        const char* copy_sql_alt = R"sql(
            INSERT INTO vectors (
                chunk_id, document_hash, embedding, embedding_dim, content, metadata,
                model_id, created_at, embedding_version
            )
            SELECT
                m.chunk_id,
                m.document_hash,
                (SELECT embedding FROM doc_embeddings WHERE rowid = m.embedding_rowid),
                (SELECT LENGTH(embedding) / 4 FROM doc_embeddings WHERE rowid = m.embedding_rowid),
                m.chunk_text,
                m.metadata,
                m.model_id,
                m.created_at,
                1
            FROM doc_metadata m
            WHERE m.embedding_rowid IS NOT NULL
        )sql";

        result = executeSQL(db, copy_sql_alt);
        if (!result) {
            executeSQL(db, "ROLLBACK");
            return Error{ErrorCode::DatabaseError,
                         "Failed to copy data: " + result.error().message};
        }
    }

    // Get count of migrated records
    sqlite3_stmt* count_stmt = nullptr;
    if (sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM vectors", -1, &count_stmt, nullptr) ==
        SQLITE_OK) {
        if (sqlite3_step(count_stmt) == SQLITE_ROW) {
            int64_t count = sqlite3_column_int64(count_stmt, 0);
            spdlog::info("Migrated {} records to V2 schema", count);
        }
        sqlite3_finalize(count_stmt);
    }

    // 3. Rename old tables as backup
    spdlog::info("Creating backup of V1 tables...");

    result = executeSQL(db, "ALTER TABLE doc_embeddings RENAME TO doc_embeddings_v1_backup");
    if (!result) {
        executeSQL(db, "ROLLBACK");
        return Error{ErrorCode::DatabaseError,
                     "Failed to backup doc_embeddings: " + result.error().message};
    }

    result = executeSQL(db, "ALTER TABLE doc_metadata RENAME TO doc_metadata_v1_backup");
    if (!result) {
        // Try to restore doc_embeddings
        executeSQL(db, "ALTER TABLE doc_embeddings_v1_backup RENAME TO doc_embeddings");
        executeSQL(db, "ROLLBACK");
        return Error{ErrorCode::DatabaseError,
                     "Failed to backup doc_metadata: " + result.error().message};
    }

    // Commit transaction
    result = executeSQL(db, "COMMIT");
    if (!result) {
        return Error{ErrorCode::DatabaseError,
                     "Failed to commit migration: " + result.error().message};
    }

    spdlog::info("V1 to V2 migration completed successfully");
    spdlog::info("Backup tables created: doc_embeddings_v1_backup, doc_metadata_v1_backup");
    spdlog::info("Run removeBackupTables() after confirming V2 works correctly");

    return Result<void>{};
}

Result<void> VectorSchemaMigration::rollbackV2ToV1(sqlite3* db) {
    if (!db) {
        return Error{ErrorCode::InvalidArgument, "Database handle is null"};
    }

    if (!hasBackupTables(db)) {
        return Error{ErrorCode::InvalidState, "Cannot rollback: backup tables not found"};
    }

    spdlog::info("Rolling back V2 to V1 schema...");

    auto result = beginTransaction(db);
    if (!result) {
        return result;
    }

    // Drop V2 tables
    result = executeSQL(db, "DROP TABLE IF EXISTS vectors");
    if (!result) {
        executeSQL(db, "ROLLBACK");
        return result;
    }

    result = executeSQL(db, "DROP TABLE IF EXISTS vectors_hnsw_meta");
    if (!result) {
        executeSQL(db, "ROLLBACK");
        return result;
    }

    result = executeSQL(db, "DROP TABLE IF EXISTS vectors_hnsw_nodes");
    if (!result) {
        executeSQL(db, "ROLLBACK");
        return result;
    }

    // Restore V1 tables
    result = executeSQL(db, "ALTER TABLE doc_embeddings_v1_backup RENAME TO doc_embeddings");
    if (!result) {
        executeSQL(db, "ROLLBACK");
        return result;
    }

    result = executeSQL(db, "ALTER TABLE doc_metadata_v1_backup RENAME TO doc_metadata");
    if (!result) {
        // Try to restore doc_embeddings backup name
        executeSQL(db, "ALTER TABLE doc_embeddings RENAME TO doc_embeddings_v1_backup");
        executeSQL(db, "ROLLBACK");
        return result;
    }

    result = executeSQL(db, "COMMIT");
    if (!result) {
        return result;
    }

    spdlog::info("Rollback to V1 schema completed");
    return Result<void>{};
}

bool VectorSchemaMigration::hasBackupTables(sqlite3* db) {
    if (!db) {
        return false;
    }

    return tableExists(db, "doc_embeddings_v1_backup") && tableExists(db, "doc_metadata_v1_backup");
}

Result<void> VectorSchemaMigration::removeBackupTables(sqlite3* db) {
    if (!db) {
        return Error{ErrorCode::InvalidArgument, "Database handle is null"};
    }

    if (!hasBackupTables(db)) {
        spdlog::info("No backup tables to remove");
        return Result<void>{};
    }

    spdlog::info("Removing V1 backup tables...");

    auto result = executeSQL(db, "DROP TABLE IF EXISTS doc_embeddings_v1_backup");
    if (!result) {
        return result;
    }

    result = executeSQL(db, "DROP TABLE IF EXISTS doc_metadata_v1_backup");
    if (!result) {
        return result;
    }

    spdlog::info("Backup tables removed");
    return Result<void>{};
}

bool VectorSchemaMigration::hasV1Tables(sqlite3* db) {
    if (!db) {
        return false;
    }

    // Check for main V1 tables and shadow tables
    return tableExists(db, "doc_embeddings") || tableExists(db, "doc_metadata") ||
           tableExists(db, "doc_embeddings_chunks") ||
           tableExists(db, "doc_embeddings_vector_chunks00");
}

Result<void> VectorSchemaMigration::removeV1Tables(sqlite3* db) {
    if (!db) {
        return Error{ErrorCode::InvalidArgument, "Database handle is null"};
    }

    if (!hasV1Tables(db)) {
        spdlog::info("No V1 tables to remove");
        return Result<void>{};
    }

    spdlog::info("Removing V1 vector tables...");

    auto result = beginTransaction(db);
    if (!result) {
        return result;
    }

    // Drop the main vec0 virtual table first (this should cascade to shadow tables)
    result = executeSQL(db, "DROP TABLE IF EXISTS doc_embeddings");
    if (!result) {
        spdlog::warn("Failed to drop doc_embeddings: {}", result.error().message);
        // Continue anyway - might be a virtual table issue
    }

    // Drop metadata table
    result = executeSQL(db, "DROP TABLE IF EXISTS doc_metadata");
    if (!result) {
        executeSQL(db, "ROLLBACK");
        return result;
    }

    // Explicitly drop vec0 shadow tables (in case virtual table drop didn't cascade)
    // These are internal tables created by sqlite-vec
    const char* shadow_tables[] = {"doc_embeddings_chunks", "doc_embeddings_rowids",
                                   "doc_embeddings_vector_chunks00", "doc_embeddings_metadata",
                                   "doc_embeddings_vectors"};

    for (const char* table : shadow_tables) {
        if (tableExists(db, table)) {
            std::string sql = "DROP TABLE IF EXISTS ";
            sql += table;
            auto drop_result = executeSQL(db, sql.c_str());
            if (!drop_result) {
                spdlog::warn("Failed to drop {}: {}", table, drop_result.error().message);
                // Continue - best effort cleanup
            } else {
                spdlog::debug("Dropped shadow table: {}", table);
            }
        }
    }

    result = executeSQL(db, "COMMIT");
    if (!result) {
        return result;
    }

    spdlog::info("V1 vector tables removed");
    return Result<void>{};
}

} // namespace yams::vector
