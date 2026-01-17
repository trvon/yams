#include <yams/vector/vector_schema_migration.h>

#include <chrono>
#include <cstdint>
#include <string>
#include <thread>

#include <sqlite3.h>
#include <spdlog/spdlog.h>

#include <yams/core/types.h>
#include <yams/daemon/components/TuneAdvisor.h>

#include <sqlite-vec-cpp/index/hnsw_persistence.hpp>

namespace yams::vector {

namespace {

// Retry parameters vary by backend:
// - libsql MVCC: fewer retries needed since concurrent writers don't block
// - SQLite: more retries with longer backoff for single-writer model
#if YAMS_LIBSQL_BACKEND
constexpr int kMaxRetries = 3;
constexpr int kInitialBackoffMs = 5;
#else
constexpr int kMaxRetries = 5;
constexpr int kInitialBackoffMs = 10;
#endif

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
    auto backoff = std::chrono::milliseconds(kInitialBackoffMs);

    for (int attempt = 0; attempt < kMaxRetries; ++attempt) {
        char* err_msg = nullptr;
        int rc = sqlite3_exec(db, sql, nullptr, nullptr, &err_msg);

        if (rc == SQLITE_OK) {
            return Result<void>{};
        }

        std::string err = err_msg ? err_msg : "Unknown error";
        sqlite3_free(err_msg);

        // Check for transient lock errors
        if ((rc == SQLITE_BUSY || rc == SQLITE_LOCKED) && attempt + 1 < kMaxRetries) {
            spdlog::debug("[Migration] transient lock on '{}', retry {}/{}", sql, attempt + 1,
                          kMaxRetries);
            std::this_thread::sleep_for(backoff);
            backoff *= 2;
            continue;
        }

        return Error{ErrorCode::DatabaseError, err};
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

    // Check for V2 schema (unified vectors table)
    if (tableExists(db, "vectors") && tableExists(db, "vectors_hnsw_meta")) {
        // Check for V2.1 (has embedding_dim column)
        if (columnExists(db, "vectors", "embedding_dim")) {
            return SchemaVersion::V2_1;
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

Result<void> VectorSchemaMigration::migrateV1ToV2(sqlite3* db, size_t embedding_dim) {
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

    if (version == SchemaVersion::V2_1) {
        spdlog::info("Database already at V2.1 schema, skipping migration");
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

    // Create HNSW shadow tables
    char* err_msg = nullptr;
    int rc = sqlite_vec_cpp::index::create_hnsw_shadow_tables(db, "main", "vectors", &err_msg);
    if (rc != SQLITE_OK) {
        std::string err = err_msg ? err_msg : "Unknown error";
        sqlite3_free(err_msg);
        executeSQL(db, "ROLLBACK");
        return Error{ErrorCode::DatabaseError, "Failed to create HNSW tables: " + err};
    }

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
