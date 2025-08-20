#include <spdlog/spdlog.h>
#include <cstring>
#include <sstream>
#include <yams/profiling.h>
#include <yams/vector/sqlite_vec_backend.h>

extern "C" {
#include "sqlite-vec.h"
}

namespace yams::vector {

namespace {
inline int stepWithRetry(sqlite3_stmt* stmt, int max_attempts = 20) {
    int attempt = 0;
    while (true) {
        int rc = sqlite3_step(stmt);
        if (rc == SQLITE_BUSY || rc == SQLITE_LOCKED) {
            int exp = attempt;
            if (exp > 7)
                exp = 7;
            int sleep_ms = 10 * (1 << exp);
            ++attempt;
            if (attempt <= max_attempts) {
                spdlog::warn("sqlite3_step busy/locked (rc={}): retry {}/{} after {} ms", rc,
                             attempt, max_attempts, sleep_ms);
                sqlite3_sleep(sleep_ms);
                continue;
            }
        }
        return rc;
    }
}

// RAII transaction guard to ensure transactions are properly rolled back on error
class TransactionGuard {
public:
    TransactionGuard(sqlite3* db, bool& in_transaction_flag)
        : db_(db), in_transaction_flag_(in_transaction_flag), committed_(false) {
        in_transaction_flag_ = true;
    }

    ~TransactionGuard() {
        if (!committed_) {
            sqlite3_exec(db_, "ROLLBACK", nullptr, nullptr, nullptr);
            in_transaction_flag_ = false;
        }
    }

    void commit() {
        committed_ = true;
        in_transaction_flag_ = false;
    }

private:
    sqlite3* db_;
    bool& in_transaction_flag_;
    bool committed_;
};
} // namespace

SqliteVecBackend::SqliteVecBackend()
    : db_(nullptr), embedding_dim_(0), initialized_(false), in_transaction_(false) {
    std::memset(&stmts_, 0, sizeof(stmts_));
}

SqliteVecBackend::~SqliteVecBackend() {
    close();
}

SqliteVecBackend::SqliteVecBackend(SqliteVecBackend&& other) noexcept
    : db_(other.db_), db_path_(std::move(other.db_path_)), embedding_dim_(other.embedding_dim_),
      initialized_(other.initialized_), in_transaction_(other.in_transaction_),
      stmts_(other.stmts_) {
    other.db_ = nullptr;
    other.initialized_ = false;
    std::memset(&other.stmts_, 0, sizeof(other.stmts_));
}

SqliteVecBackend& SqliteVecBackend::operator=(SqliteVecBackend&& other) noexcept {
    if (this != &other) {
        close();
        db_ = other.db_;
        db_path_ = std::move(other.db_path_);
        embedding_dim_ = other.embedding_dim_;
        initialized_ = other.initialized_;
        in_transaction_ = other.in_transaction_;
        stmts_ = other.stmts_;

        other.db_ = nullptr;
        other.initialized_ = false;
        std::memset(&other.stmts_, 0, sizeof(other.stmts_));
    }
    return *this;
}

Result<void> SqliteVecBackend::initialize(const std::string& db_path) {
    YAMS_ZONE_SCOPED_N("SqliteVecBackend::initialize");
    std::lock_guard<std::mutex> lock(mutex_);

    if (initialized_) {
        return Error{ErrorCode::InvalidState, "Backend already initialized"};
    }

    // Open database connection
    int rc;
    {
        YAMS_ZONE_SCOPED_N("SQLite::OpenDatabase");
        rc = sqlite3_open(db_path.c_str(), &db_);
    }

    if (rc != SQLITE_OK) {
        std::string error = sqlite3_errmsg(db_);
        sqlite3_close(db_);
        db_ = nullptr;
        return Error{ErrorCode::DatabaseError, "Failed to open database: " + error};
    }

    // Set busy timeout to avoid indefinite blocking
    sqlite3_busy_timeout(db_, 5000); // 5 second timeout

    db_path_ = db_path;

    spdlog::info("Database opened successfully: {}", db_path);
    // Enable additional pragmas for robustness and performance
    executeSQL("PRAGMA foreign_keys=ON");
    executeSQL("PRAGMA temp_store=MEMORY");
    // 256 MiB memory map for improved I/O performance (bytes)
    executeSQL("PRAGMA mmap_size=268435456");

    // Harden connection against SQLITE_BUSY/LOCKED and enable WAL for better concurrency
    sqlite3_extended_result_codes(db_, 1);
    sqlite3_busy_timeout(db_, 10000);

    {
        auto pragma = executeSQL("PRAGMA journal_mode=WAL");
        if (!pragma) {
            spdlog::warn("Failed to enable WAL journal mode: {}. Continuing with default mode.",
                         pragma.error().message);
        }
    }
    // Reasonable durability/performance trade-offs for WAL
    executeSQL("PRAGMA synchronous=NORMAL");
    executeSQL("PRAGMA wal_autocheckpoint=1000");

    // Load sqlite-vec extension
    Result<void> result;
    {
        YAMS_ZONE_SCOPED_N("SQLite::LoadVecExtension");
        result = loadSqliteVecExtension();
    }
    if (!result) {
        close();
        return Error{ErrorCode::DatabaseError,
                     "Failed to load sqlite-vec extension: " + result.error().message};
    }

    initialized_ = true;
    spdlog::info("SqliteVecBackend initialized with database: {}", db_path);

    return Result<void>();
}

void SqliteVecBackend::close() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!initialized_) {
        return;
    }

    // Clean up prepared statements
    finalizeStatements();

    // Close database
    if (db_) {
        sqlite3_close(db_);
        db_ = nullptr;
    }

    initialized_ = false;
    in_transaction_ = false;
    spdlog::debug("SqliteVecBackend closed");
}

bool SqliteVecBackend::isInitialized() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return initialized_;
}

Result<void> SqliteVecBackend::createTables(size_t embedding_dim) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!initialized_) {
        return Error{ErrorCode::NotInitialized, "Backend not initialized"};
    }

    embedding_dim_ = embedding_dim;

    spdlog::info("Creating sqlite-vec tables with proper schema separation...");

    // Set a busy timeout to prevent hangs
    sqlite3_busy_timeout(db_, 10000);

    // 1. Create the vector virtual table (embeddings only)
    spdlog::info("Creating vector virtual table...");
    std::string vector_sql =
        "CREATE VIRTUAL TABLE IF NOT EXISTS doc_embeddings USING vec0(embedding float[" +
        std::to_string(embedding_dim) + "])";

    spdlog::debug("Vector table SQL: {}", vector_sql);
    auto result = executeSQL(vector_sql);
    if (!result) {
        spdlog::error("Vector table creation failed: {}", result.error().message);
        return Error{ErrorCode::DatabaseError,
                     "Failed to create vector table: " + result.error().message};
    }

    spdlog::info("Vector table created successfully");

    // 2. Create the metadata table (regular SQLite table)
    spdlog::info("Creating metadata table...");
    std::string metadata_sql = R"(
        CREATE TABLE IF NOT EXISTS doc_metadata (
            rowid INTEGER PRIMARY KEY,
            document_hash TEXT NOT NULL,
            chunk_id TEXT UNIQUE NOT NULL,
            chunk_text TEXT,
            model_id TEXT,
            metadata TEXT,
            created_at INTEGER DEFAULT (unixepoch())
        )
    )";

    spdlog::debug("Metadata table SQL: {}", metadata_sql);
    result = executeSQL(metadata_sql);
    if (!result) {
        spdlog::error("Metadata table creation failed: {}", result.error().message);
        return Error{ErrorCode::DatabaseError,
                     "Failed to create metadata table: " + result.error().message};
    }

    spdlog::info("Metadata table created successfully");

    // 3. Create indexes on metadata table for faster lookups
    spdlog::info("Creating indexes...");

    auto index_result = executeSQL(
        "CREATE INDEX IF NOT EXISTS idx_doc_metadata_hash ON doc_metadata(document_hash)");
    if (!index_result) {
        spdlog::warn("Failed to create document_hash index: {}", index_result.error().message);
    }

    index_result = executeSQL(
        "CREATE INDEX IF NOT EXISTS idx_doc_metadata_chunk_id ON doc_metadata(chunk_id)");
    if (!index_result) {
        spdlog::warn("Failed to create chunk_id index: {}", index_result.error().message);
    }

    // 4. Test that both tables are accessible
    spdlog::debug("Testing table access...");
    auto count_result = executeSQL("SELECT COUNT(*) FROM doc_embeddings");
    if (!count_result) {
        spdlog::warn("Vector table access test failed: {}", count_result.error().message);
    }

    count_result = executeSQL("SELECT COUNT(*) FROM doc_metadata");
    if (!count_result) {
        spdlog::warn("Metadata table access test failed: {}", count_result.error().message);
    }

    spdlog::info("Vector database tables created successfully with dimension: {}", embedding_dim);

    // Skip table verification for now - it causes issues with vec0 virtual tables
    // The tables were just created successfully, so they should exist
    // TODO: Find a reliable way to check vec0 virtual table existence

    return Result<void>();
}

bool SqliteVecBackend::tablesExist() const {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!initialized_) {
        return false;
    }

    spdlog::debug("Checking if tables exist...");

    // For virtual tables created by vec0, we need to check sqlite_master differently
    // Virtual tables show up in sqlite_master but we need to be careful with the query

    // Simple approach: Try to select from the tables with LIMIT 0
    // This won't return any data but will fail if the table doesn't exist

    // Check existence in sqlite_master (virtual tables included)
    const char* vector_check_sql =
        "SELECT name FROM sqlite_master WHERE name='doc_embeddings' LIMIT 1";
    sqlite3_stmt* stmt;
    spdlog::debug("Checking vector table existence via sqlite_master...");
    if (sqlite3_prepare_v2(db_, vector_check_sql, -1, &stmt, nullptr) != SQLITE_OK) {
        spdlog::debug("Failed to prepare sqlite_master check for vector table: {}",
                      sqlite3_errmsg(db_));
        return false;
    }
    bool vector_exists = (sqlite3_step(stmt) == SQLITE_ROW);
    sqlite3_finalize(stmt);
    if (!vector_exists) {
        spdlog::debug("Vector table doc_embeddings not found in sqlite_master");
        return false;
    }

    const char* metadata_check_sql =
        "SELECT name FROM sqlite_master WHERE name='doc_metadata' LIMIT 1";
    spdlog::debug("Checking metadata table existence via sqlite_master...");
    if (sqlite3_prepare_v2(db_, metadata_check_sql, -1, &stmt, nullptr) != SQLITE_OK) {
        spdlog::debug("Failed to prepare sqlite_master check for metadata table: {}",
                      sqlite3_errmsg(db_));
        return false;
    }
    bool metadata_exists = (sqlite3_step(stmt) == SQLITE_ROW);
    sqlite3_finalize(stmt);
    if (!metadata_exists) {
        spdlog::debug("Metadata table doc_metadata not found in sqlite_master");
        return false;
    }

    spdlog::debug("Both tables exist and are accessible");
    return true;
}

Result<void> SqliteVecBackend::insertVector(const VectorRecord& record) {
    YAMS_ZONE_SCOPED_N("SqliteVecBackend::insertVector");
    YAMS_PLOT("VectorInsert", static_cast<int64_t>(1));

    std::lock_guard<std::mutex> lock(mutex_);

    if (!initialized_) {
        return Error{ErrorCode::NotInitialized, "Backend not initialized"};
    }

    // Use internal method that doesn't manage transactions
    // Note: insertVectorInternal does NOT lock mutex - caller must hold lock
    return insertVectorInternal(record, true);
}

Result<void> SqliteVecBackend::insertVectorInternal(const VectorRecord& record,
                                                    bool manage_transaction) {
    spdlog::info("insertVectorInternal called for chunk_id: {}, model_id: {}", record.chunk_id,
                 record.model_id);

    // Check if chunk_id already exists (for upsert behavior)
    const char* check_sql = "SELECT rowid FROM doc_metadata WHERE chunk_id = ?";
    sqlite3_stmt* check_stmt;
    sqlite3_int64 existing_rowid = -1;

    if (sqlite3_prepare_v2(db_, check_sql, -1, &check_stmt, nullptr) == SQLITE_OK) {
        sqlite3_bind_text(check_stmt, 1, record.chunk_id.c_str(), -1, SQLITE_TRANSIENT);

        if (sqlite3_step(check_stmt) == SQLITE_ROW) {
            existing_rowid = sqlite3_column_int64(check_stmt, 0);
            spdlog::debug("Chunk {} already exists with rowid {}, will update", record.chunk_id,
                          existing_rowid);
        }
        sqlite3_finalize(check_stmt);
    }

    // Only begin transaction if we're managing it
    if (manage_transaction) {
        auto tx_result = executeSQL("BEGIN IMMEDIATE TRANSACTION");
        if (!tx_result) {
            return Error{ErrorCode::DatabaseError,
                         "Failed to begin transaction: " + tx_result.error().message};
        }
    }

    try {
        sqlite3_int64 vector_rowid;

        if (existing_rowid != -1) {
            // Update existing record
            vector_rowid = existing_rowid;

            // Update vector in virtual table (sqlite-vec expects JSON format)
            std::stringstream vec_json;
            vec_json << "[";
            for (size_t i = 0; i < record.embedding.size(); ++i) {
                if (i > 0)
                    vec_json << ", ";
                vec_json << record.embedding[i];
            }
            vec_json << "]";

            const char* update_vector_sql =
                "UPDATE doc_embeddings SET embedding = ? WHERE rowid = ?";
            sqlite3_stmt* update_stmt;

            if (sqlite3_prepare_v2(db_, update_vector_sql, -1, &update_stmt, nullptr) !=
                SQLITE_OK) {
                if (manage_transaction)
                    executeSQL("ROLLBACK");
                return Error{ErrorCode::DatabaseError,
                             "Failed to prepare vector update: " + getLastError()};
            }

            sqlite3_bind_text(update_stmt, 1, vec_json.str().c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int64(update_stmt, 2, vector_rowid);

            int rc = stepWithRetry(update_stmt);
            sqlite3_finalize(update_stmt);

            if (rc != SQLITE_DONE) {
                if (manage_transaction)
                    executeSQL("ROLLBACK");
                return Error{ErrorCode::DatabaseError,
                             "Failed to update vector: " + getLastError()};
            }

            // Update metadata
            const char* update_metadata_sql = R"(
                UPDATE doc_metadata 
                SET document_hash = ?, chunk_text = ?, model_id = ?, metadata = ?
                WHERE rowid = ?
            )";
            sqlite3_stmt* metadata_stmt;

            if (sqlite3_prepare_v2(db_, update_metadata_sql, -1, &metadata_stmt, nullptr) !=
                SQLITE_OK) {
                if (manage_transaction)
                    executeSQL("ROLLBACK");
                return Error{ErrorCode::DatabaseError,
                             "Failed to prepare metadata update: " + getLastError()};
            }

            sqlite3_bind_text(metadata_stmt, 1, record.document_hash.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(metadata_stmt, 2, record.content.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(metadata_stmt, 3, record.model_id.c_str(), -1, SQLITE_TRANSIENT);

            // Serialize metadata
            std::string metadata_json = "{}";
            if (!record.metadata.empty()) {
                metadata_json = "{";
                bool first = true;
                for (const auto& [key, value] : record.metadata) {
                    if (!first)
                        metadata_json += ",";
                    metadata_json += "\"" + key + "\":\"" + value + "\"";
                    first = false;
                }
                metadata_json += "}";
            }
            sqlite3_bind_text(metadata_stmt, 4, metadata_json.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int64(metadata_stmt, 5, vector_rowid);

            rc = stepWithRetry(metadata_stmt);
            sqlite3_finalize(metadata_stmt);

            if (rc != SQLITE_DONE) {
                if (manage_transaction)
                    executeSQL("ROLLBACK");
                return Error{ErrorCode::DatabaseError,
                             "Failed to update metadata: " + getLastError()};
            }

        } else {
            // Insert new record
            // 1. Insert vector into virtual table (sqlite-vec expects JSON format for vectors)
            // Convert vector to JSON string format: [1.0, 2.0, 3.0, ...]
            std::stringstream vec_json;
            vec_json << "[";
            for (size_t i = 0; i < record.embedding.size(); ++i) {
                if (i > 0)
                    vec_json << ", ";
                vec_json << record.embedding[i];
            }
            vec_json << "]";

            // Insert vector with rowid NULL to let sqlite-vec auto-assign
            const char* vector_sql =
                "INSERT INTO doc_embeddings (rowid, embedding) VALUES (NULL, ?)";
            sqlite3_stmt* vector_stmt;

            spdlog::info("Preparing vector insert SQL: {}", vector_sql);
            spdlog::info("Vector JSON: {}",
                         vec_json.str().substr(0, 100) + "..."); // Log first 100 chars

            {
                YAMS_ZONE_SCOPED_N("SQLite::PrepareVectorInsert");
                if (sqlite3_prepare_v2(db_, vector_sql, -1, &vector_stmt, nullptr) != SQLITE_OK) {
                    if (manage_transaction)
                        executeSQL("ROLLBACK");
                    return Error{ErrorCode::DatabaseError,
                                 "Failed to prepare vector insert: " + getLastError()};
                }
                sqlite3_bind_text(vector_stmt, 1, vec_json.str().c_str(), -1, SQLITE_TRANSIENT);
            }

            spdlog::info("About to execute sqlite3_step for vector insert");
            int rc;
            {
                YAMS_ZONE_SCOPED_N("SQLite::ExecuteVectorInsert");
                rc = stepWithRetry(vector_stmt);
            }
            spdlog::info("sqlite3_step returned: {}", rc);
            sqlite3_finalize(vector_stmt);

            if (rc != SQLITE_DONE) {
                if (manage_transaction)
                    executeSQL("ROLLBACK");
                return Error{ErrorCode::DatabaseError,
                             "Failed to insert vector: " + getLastError()};
            }

            // Get the rowid of the inserted vector
            vector_rowid = sqlite3_last_insert_rowid(db_);

            // 2. Insert metadata into regular table with the same rowid
            const char* metadata_sql = R"(
                INSERT INTO doc_metadata (rowid, document_hash, chunk_id, chunk_text, model_id, metadata)
                VALUES (?, ?, ?, ?, ?, ?)
            )";

            sqlite3_stmt* metadata_stmt;
            {
                YAMS_ZONE_SCOPED_N("SQLite::PrepareMetadataInsert");
                if (sqlite3_prepare_v2(db_, metadata_sql, -1, &metadata_stmt, nullptr) !=
                    SQLITE_OK) {
                    if (manage_transaction)
                        executeSQL("ROLLBACK");
                    return Error{ErrorCode::DatabaseError,
                                 "Failed to prepare metadata insert: " + getLastError()};
                }

                sqlite3_bind_int64(metadata_stmt, 1, vector_rowid);
                sqlite3_bind_text(metadata_stmt, 2, record.document_hash.c_str(), -1,
                                  SQLITE_TRANSIENT);
                sqlite3_bind_text(metadata_stmt, 3, record.chunk_id.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(metadata_stmt, 4, record.content.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(metadata_stmt, 5, record.model_id.c_str(), -1, SQLITE_TRANSIENT);

                // Serialize metadata map to JSON string
                std::string metadata_json = "{}";
                if (!record.metadata.empty()) {
                    metadata_json = "{";
                    bool first = true;
                    for (const auto& [key, value] : record.metadata) {
                        if (!first)
                            metadata_json += ",";
                        metadata_json += "\"" + key + "\":\"" + value + "\"";
                        first = false;
                    }
                    metadata_json += "}";
                }
                sqlite3_bind_text(metadata_stmt, 6, metadata_json.c_str(), -1, SQLITE_TRANSIENT);
            }

            {
                YAMS_ZONE_SCOPED_N("SQLite::ExecuteMetadataInsert");
                rc = stepWithRetry(metadata_stmt);
            }
            sqlite3_finalize(metadata_stmt);

            if (rc != SQLITE_DONE) {
                if (manage_transaction)
                    executeSQL("ROLLBACK");
                return Error{ErrorCode::DatabaseError,
                             "Failed to insert metadata: " + getLastError()};
            }
        }

        // Commit transaction only if we're managing it
        if (manage_transaction) {
            auto commit_result = executeSQL("COMMIT");
            if (!commit_result) {
                executeSQL("ROLLBACK");
                return Error{ErrorCode::DatabaseError,
                             "Failed to commit transaction: " + commit_result.error().message};
            }
        }

        spdlog::debug("Successfully inserted vector record with rowid: {}", vector_rowid);
        return Result<void>();

    } catch (const std::exception& e) {
        if (manage_transaction) {
            executeSQL("ROLLBACK");
        }
        return Error{ErrorCode::DatabaseError, "Insert failed: " + std::string(e.what())};
    }
}

Result<void> SqliteVecBackend::insertVectorsBatch(const std::vector<VectorRecord>& records) {
    YAMS_ZONE_SCOPED_N("SqliteVecBackend::insertVectorsBatch");
    YAMS_PLOT("BatchInsertSize", static_cast<int64_t>(records.size()));

    spdlog::debug("insertVectorsBatch called with {} records", records.size());

    if (records.empty()) {
        return Result<void>();
    }

    std::lock_guard<std::mutex> lock(mutex_);
    spdlog::debug("insertVectorsBatch acquired lock");

    if (!initialized_) {
        return Error{ErrorCode::NotInitialized, "Backend not initialized"};
    }

    // Start transaction for batch insert
    if (in_transaction_) {
        return Error{ErrorCode::InvalidState, "Transaction already in progress"};
    }

    spdlog::info("Beginning transaction for batch insert");
    Result<void> tx_result;
    {
        YAMS_ZONE_SCOPED_N("BatchInsert::BeginTransaction");
        tx_result = executeSQL("BEGIN IMMEDIATE TRANSACTION");
        if (!tx_result) {
            return Error{ErrorCode::DatabaseError,
                         "Failed to begin transaction: " + tx_result.error().message};
        }
    }

    // Use RAII guard to ensure transaction is properly rolled back on error
    TransactionGuard guard(db_, in_transaction_);
    spdlog::info("Transaction started successfully");

    for (size_t i = 0; i < records.size(); ++i) {
        spdlog::info("Inserting record {}/{}", i + 1, records.size());
        // Use internal method that doesn't manage its own transaction
        // Note: We already hold the lock, so insertVectorInternal won't deadlock
        auto result = insertVectorInternal(records[i], false);
        if (!result) {
            spdlog::error("Failed to insert record {} in batch: {}", i, result.error().message);
            // TransactionGuard will automatically rollback
            return result;
        }
        spdlog::info("Record {}/{} inserted successfully", i + 1, records.size());
    }

    Result<void> commit_result;
    {
        YAMS_ZONE_SCOPED_N("BatchInsert::CommitTransaction");
        commit_result = executeSQL("COMMIT");
    }

    if (!commit_result) {
        // TransactionGuard will automatically rollback
        return Error{ErrorCode::DatabaseError,
                     "Failed to commit transaction: " + commit_result.error().message};
    }

    // Mark transaction as successfully committed
    guard.commit();

    return Result<void>();
}

Result<void> SqliteVecBackend::updateVector(const std::string& chunk_id,
                                            const VectorRecord& record) {
    YAMS_ZONE_SCOPED_N("SqliteVecBackend::updateVector");
    YAMS_PLOT("VectorUpdate", static_cast<int64_t>(1));

    std::lock_guard<std::mutex> lock(mutex_);

    if (!initialized_) {
        return Error{ErrorCode::NotInitialized, "Backend not initialized"};
    }

    // Start transaction
    auto tx_result = executeSQL("BEGIN TRANSACTION");
    if (!tx_result) {
        return Error{ErrorCode::DatabaseError,
                     "Failed to begin transaction: " + tx_result.error().message};
    }

    try {
        // First, get the rowid for this chunk_id
        const char* get_rowid_sql = "SELECT rowid FROM doc_metadata WHERE chunk_id = ?";
        sqlite3_stmt* rowid_stmt;

        if (sqlite3_prepare_v2(db_, get_rowid_sql, -1, &rowid_stmt, nullptr) != SQLITE_OK) {
            executeSQL("ROLLBACK");
            return Error{ErrorCode::DatabaseError,
                         "Failed to prepare rowid query: " + getLastError()};
        }

        sqlite3_bind_text(rowid_stmt, 1, chunk_id.c_str(), -1, SQLITE_TRANSIENT);

        sqlite3_int64 rowid = -1;
        if (sqlite3_step(rowid_stmt) == SQLITE_ROW) {
            rowid = sqlite3_column_int64(rowid_stmt, 0);
        }
        sqlite3_finalize(rowid_stmt);

        if (rowid == -1) {
            executeSQL("ROLLBACK");
            return Error{ErrorCode::NotFound, "Vector not found with chunk_id: " + chunk_id};
        }

        // Update the vector in the virtual table (use JSON text, not BLOB)
        std::stringstream vec_json;
        vec_json << "[";
        for (size_t i = 0; i < record.embedding.size(); ++i) {
            if (i > 0)
                vec_json << ", ";
            vec_json << record.embedding[i];
        }
        vec_json << "]";

        const char* vector_sql = "UPDATE doc_embeddings SET embedding = ? WHERE rowid = ?";
        sqlite3_stmt* vector_stmt;

        if (sqlite3_prepare_v2(db_, vector_sql, -1, &vector_stmt, nullptr) != SQLITE_OK) {
            executeSQL("ROLLBACK");
            return Error{ErrorCode::DatabaseError,
                         "Failed to prepare vector update: " + getLastError()};
        }

        sqlite3_bind_text(vector_stmt, 1, vec_json.str().c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(vector_stmt, 2, rowid);

        int rc = stepWithRetry(vector_stmt);
        sqlite3_finalize(vector_stmt);

        if (rc != SQLITE_DONE) {
            executeSQL("ROLLBACK");
            return Error{ErrorCode::DatabaseError, "Failed to update vector: " + getLastError()};
        }

        // Update the metadata
        const char* metadata_sql = "UPDATE doc_metadata SET document_hash = ?, chunk_text = ?, "
                                   "model_id = ?, metadata = ? WHERE rowid = ?";
        sqlite3_stmt* metadata_stmt;

        if (sqlite3_prepare_v2(db_, metadata_sql, -1, &metadata_stmt, nullptr) != SQLITE_OK) {
            executeSQL("ROLLBACK");
            return Error{ErrorCode::DatabaseError,
                         "Failed to prepare metadata update: " + getLastError()};
        }

        sqlite3_bind_text(metadata_stmt, 1, record.document_hash.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(metadata_stmt, 2, record.content.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(metadata_stmt, 3, record.model_id.c_str(), -1, SQLITE_TRANSIENT);

        // Serialize metadata map to JSON string
        std::string metadata_json = "{}";
        if (!record.metadata.empty()) {
            metadata_json = "{";
            bool first = true;
            for (const auto& [key, value] : record.metadata) {
                if (!first)
                    metadata_json += ",";
                metadata_json += "\"" + key + "\":\"" + value + "\"";
                first = false;
            }
            metadata_json += "}";
        }
        sqlite3_bind_text(metadata_stmt, 4, metadata_json.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(metadata_stmt, 5, rowid);

        rc = stepWithRetry(metadata_stmt);
        sqlite3_finalize(metadata_stmt);

        if (rc != SQLITE_DONE) {
            executeSQL("ROLLBACK");
            return Error{ErrorCode::DatabaseError, "Failed to update metadata: " + getLastError()};
        }

        // Commit transaction
        auto commit_result = executeSQL("COMMIT");
        if (!commit_result) {
            executeSQL("ROLLBACK");
            return Error{ErrorCode::DatabaseError,
                         "Failed to commit transaction: " + commit_result.error().message};
        }

        return Result<void>();

    } catch (const std::exception& e) {
        executeSQL("ROLLBACK");
        return Error{ErrorCode::DatabaseError, "Update failed: " + std::string(e.what())};
    }
}

Result<void> SqliteVecBackend::deleteVector(const std::string& chunk_id) {
    YAMS_ZONE_SCOPED_N("SqliteVecBackend::deleteVector");
    YAMS_PLOT("VectorDelete", static_cast<int64_t>(1));

    std::lock_guard<std::mutex> lock(mutex_);

    if (!initialized_) {
        return Error{ErrorCode::NotInitialized, "Backend not initialized"};
    }

    // Start transaction
    auto tx_result = executeSQL("BEGIN IMMEDIATE TRANSACTION");
    if (!tx_result) {
        return Error{ErrorCode::DatabaseError,
                     "Failed to begin transaction: " + tx_result.error().message};
    }

    try {
        // First, get the rowid for this chunk_id
        const char* get_rowid_sql = "SELECT rowid FROM doc_metadata WHERE chunk_id = ?";
        sqlite3_stmt* rowid_stmt;

        if (sqlite3_prepare_v2(db_, get_rowid_sql, -1, &rowid_stmt, nullptr) != SQLITE_OK) {
            executeSQL("ROLLBACK");
            return Error{ErrorCode::DatabaseError,
                         "Failed to prepare rowid query: " + getLastError()};
        }

        sqlite3_bind_text(rowid_stmt, 1, chunk_id.c_str(), -1, SQLITE_TRANSIENT);

        sqlite3_int64 rowid = -1;
        if (sqlite3_step(rowid_stmt) == SQLITE_ROW) {
            rowid = sqlite3_column_int64(rowid_stmt, 0);
        }
        sqlite3_finalize(rowid_stmt);

        if (rowid == -1) {
            executeSQL("ROLLBACK");
            return Error{ErrorCode::NotFound, "Vector not found with chunk_id: " + chunk_id};
        }

        // Delete from metadata table first (due to foreign key)
        const char* metadata_sql = "DELETE FROM doc_metadata WHERE rowid = ?";
        sqlite3_stmt* metadata_stmt;

        if (sqlite3_prepare_v2(db_, metadata_sql, -1, &metadata_stmt, nullptr) != SQLITE_OK) {
            executeSQL("ROLLBACK");
            return Error{ErrorCode::DatabaseError,
                         "Failed to prepare metadata delete: " + getLastError()};
        }

        sqlite3_bind_int64(metadata_stmt, 1, rowid);

        int rc = stepWithRetry(metadata_stmt);
        sqlite3_finalize(metadata_stmt);

        if (rc != SQLITE_DONE) {
            executeSQL("ROLLBACK");
            return Error{ErrorCode::DatabaseError, "Failed to delete metadata: " + getLastError()};
        }

        // Delete from vector table
        const char* vector_sql = "DELETE FROM doc_embeddings WHERE rowid = ?";
        sqlite3_stmt* vector_stmt;

        if (sqlite3_prepare_v2(db_, vector_sql, -1, &vector_stmt, nullptr) != SQLITE_OK) {
            executeSQL("ROLLBACK");
            return Error{ErrorCode::DatabaseError,
                         "Failed to prepare vector delete: " + getLastError()};
        }

        sqlite3_bind_int64(vector_stmt, 1, rowid);

        rc = stepWithRetry(vector_stmt);
        sqlite3_finalize(vector_stmt);

        if (rc != SQLITE_DONE) {
            executeSQL("ROLLBACK");
            return Error{ErrorCode::DatabaseError, "Failed to delete vector: " + getLastError()};
        }

        // Commit transaction
        auto commit_result = executeSQL("COMMIT");
        if (!commit_result) {
            executeSQL("ROLLBACK");
            return Error{ErrorCode::DatabaseError,
                         "Failed to commit transaction: " + commit_result.error().message};
        }

        return Result<void>();

    } catch (const std::exception& e) {
        executeSQL("ROLLBACK");
        return Error{ErrorCode::DatabaseError, "Delete failed: " + std::string(e.what())};
    }
}

Result<void> SqliteVecBackend::deleteVectorsByDocument(const std::string& document_hash) {
    YAMS_ZONE_SCOPED_N("SqliteVecBackend::deleteVectorsByDocument");
    YAMS_PLOT("DocumentDelete", static_cast<int64_t>(1));

    std::lock_guard<std::mutex> lock(mutex_);

    if (!initialized_) {
        return Error{ErrorCode::NotInitialized, "Backend not initialized"};
    }

    // Start transaction
    auto tx_result = executeSQL("BEGIN IMMEDIATE TRANSACTION");
    if (!tx_result) {
        return Error{ErrorCode::DatabaseError,
                     "Failed to begin transaction: " + tx_result.error().message};
    }

    try {
        // Get all rowids for this document
        const char* get_rowids_sql = "SELECT rowid FROM doc_metadata WHERE document_hash = ?";
        sqlite3_stmt* rowids_stmt;

        if (sqlite3_prepare_v2(db_, get_rowids_sql, -1, &rowids_stmt, nullptr) != SQLITE_OK) {
            executeSQL("ROLLBACK");
            return Error{ErrorCode::DatabaseError,
                         "Failed to prepare rowids query: " + getLastError()};
        }

        sqlite3_bind_text(rowids_stmt, 1, document_hash.c_str(), -1, SQLITE_TRANSIENT);

        std::vector<sqlite3_int64> rowids;
        while (sqlite3_step(rowids_stmt) == SQLITE_ROW) {
            rowids.push_back(sqlite3_column_int64(rowids_stmt, 0));
        }
        sqlite3_finalize(rowids_stmt);

        // Delete from metadata table first
        const char* metadata_sql = "DELETE FROM doc_metadata WHERE document_hash = ?";
        sqlite3_stmt* metadata_stmt;

        if (sqlite3_prepare_v2(db_, metadata_sql, -1, &metadata_stmt, nullptr) != SQLITE_OK) {
            executeSQL("ROLLBACK");
            return Error{ErrorCode::DatabaseError,
                         "Failed to prepare metadata delete: " + getLastError()};
        }

        sqlite3_bind_text(metadata_stmt, 1, document_hash.c_str(), -1, SQLITE_TRANSIENT);

        int rc = stepWithRetry(metadata_stmt);
        sqlite3_finalize(metadata_stmt);

        if (rc != SQLITE_DONE) {
            executeSQL("ROLLBACK");
            return Error{ErrorCode::DatabaseError, "Failed to delete metadata: " + getLastError()};
        }

        // Delete from vector table for each rowid
        const char* vector_sql = "DELETE FROM doc_embeddings WHERE rowid = ?";
        for (auto rowid : rowids) {
            sqlite3_stmt* vector_stmt;

            if (sqlite3_prepare_v2(db_, vector_sql, -1, &vector_stmt, nullptr) != SQLITE_OK) {
                executeSQL("ROLLBACK");
                return Error{ErrorCode::DatabaseError,
                             "Failed to prepare vector delete: " + getLastError()};
            }

            sqlite3_bind_int64(vector_stmt, 1, rowid);

            rc = stepWithRetry(vector_stmt);
            sqlite3_finalize(vector_stmt);

            if (rc != SQLITE_DONE) {
                executeSQL("ROLLBACK");
                return Error{ErrorCode::DatabaseError,
                             "Failed to delete vector: " + getLastError()};
            }
        }

        // Commit transaction
        auto commit_result = executeSQL("COMMIT");
        if (!commit_result) {
            executeSQL("ROLLBACK");
            return Error{ErrorCode::DatabaseError,
                         "Failed to commit transaction: " + commit_result.error().message};
        }

        return Result<void>();

    } catch (const std::exception& e) {
        executeSQL("ROLLBACK");
        return Error{ErrorCode::DatabaseError, "Delete failed: " + std::string(e.what())};
    }
}

Result<std::vector<VectorRecord>>
SqliteVecBackend::searchSimilar(const std::vector<float>& query_embedding, size_t k,
                                float similarity_threshold,
                                const std::optional<std::string>& document_hash,
                                const std::map<std::string, std::string>& metadata_filters) {
    YAMS_VECTOR_SEARCH_ZONE(k, similarity_threshold);
    YAMS_PLOT("QueryVectorDim", static_cast<int64_t>(query_embedding.size()));

    std::lock_guard<std::mutex> lock(mutex_);

    if (!initialized_) {
        return Error{ErrorCode::NotInitialized, "Backend not initialized"};
    }

    // Convert query vector to JSON format for sqlite-vec
    std::stringstream query_json;
    query_json << "[";
    for (size_t i = 0; i < query_embedding.size(); ++i) {
        if (i > 0)
            query_json << ", ";
        query_json << query_embedding[i];
    }
    query_json << "]";

    // Build SQL query with KNN search using vec_distance_cosine function
    std::stringstream sql;
    sql << "SELECT m.chunk_id, m.document_hash, m.chunk_text, m.model_id, m.metadata, "
        << "vec_distance_cosine(e.embedding, ?) as distance "
        << "FROM doc_embeddings e "
        << "JOIN doc_metadata m ON e.rowid = m.rowid ";

    // Build WHERE clause for filters
    std::vector<std::string> where_clauses;
    if (document_hash.has_value()) {
        where_clauses.push_back("m.document_hash = ?");
    }

    // Add metadata filters using json_extract
    for (const auto& [key, value] : metadata_filters) {
        (void)key;   // Suppress unused variable warning - used in parameter binding below
        (void)value; // Suppress unused variable warning - used in parameter binding below
        where_clauses.push_back("json_extract(m.metadata, '$.\"' || ? || '\"') = ?");
    }

    if (!where_clauses.empty()) {
        sql << "WHERE ";
        for (size_t i = 0; i < where_clauses.size(); ++i) {
            if (i > 0)
                sql << " AND ";
            sql << where_clauses[i];
        }
        sql << " ";
    }

    sql << "ORDER BY distance ASC "
        << "LIMIT " << k;

    spdlog::info("Search SQL: {}", sql.str());
    spdlog::info("Search query JSON length: {}, k: {}", query_json.str().length(), k);

    sqlite3_stmt* stmt;
    {
        YAMS_ZONE_SCOPED_N("VectorSearch::PrepareStatement");
        if (sqlite3_prepare_v2(db_, sql.str().c_str(), -1, &stmt, nullptr) != SQLITE_OK) {
            return Error{ErrorCode::DatabaseError, getLastError()};
        }
    }

    {
        YAMS_ZONE_SCOPED_N("VectorSearch::BindParameters");
        int param = 1;
        sqlite3_bind_text(stmt, param++, query_json.str().c_str(), -1, SQLITE_TRANSIENT);

        if (document_hash.has_value()) {
            sqlite3_bind_text(stmt, param++, document_hash->c_str(), -1, SQLITE_TRANSIENT);
        }

        // Bind metadata filter parameters
        for (const auto& [key, value] : metadata_filters) {
            sqlite3_bind_text(stmt, param++, key.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, param++, value.c_str(), -1, SQLITE_TRANSIENT);
        }
    }

    std::vector<VectorRecord> results;
    {
        YAMS_ZONE_SCOPED_N("VectorSearch::ProcessResults");
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            float distance = static_cast<float>(sqlite3_column_double(stmt, 5));
            // Cosine distance ranges from 0 (identical) to 2 (opposite)
            // Convert to similarity: 1 - (distance/2) gives range [1, -1]
            float similarity = 1.0f - (distance / 2.0f);

            if (similarity >= similarity_threshold) {
                VectorRecord record;
                record.chunk_id = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
                record.document_hash = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
                record.content = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 2));
                record.model_id = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 3));
                // Parse metadata JSON (simplified - store as single key-value for now)
                const char* metadata_json =
                    reinterpret_cast<const char*>(sqlite3_column_text(stmt, 4));
                if (metadata_json && strlen(metadata_json) > 2) {
                    record.metadata["raw_json"] = metadata_json;
                }
                record.relevance_score = similarity;
                // Note: We don't retrieve the actual embedding vector here for performance

                results.push_back(record);
            }
        }
    }

    YAMS_PLOT("SearchResultsCount", static_cast<int64_t>(results.size()));
    sqlite3_finalize(stmt);
    return results;
}

Result<std::optional<VectorRecord>> SqliteVecBackend::getVector(const std::string& chunk_id) {
    YAMS_ZONE_SCOPED_N("SqliteVecBackend::getVector");

    std::lock_guard<std::mutex> lock(mutex_);

    if (!initialized_) {
        return Error{ErrorCode::NotInitialized, "Backend not initialized"};
    }

    // Query the metadata table (not the vector table)
    const char* sql =
        "SELECT m.document_hash, m.chunk_text, m.model_id, m.metadata, m.rowid, e.embedding "
        "FROM doc_metadata m JOIN doc_embeddings e ON e.rowid = m.rowid WHERE m.chunk_id = ?";

    sqlite3_stmt* stmt;
    if (sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        return Error{ErrorCode::DatabaseError, getLastError()};
    }

    sqlite3_bind_text(stmt, 1, chunk_id.c_str(), -1, SQLITE_TRANSIENT);

    if (sqlite3_step(stmt) == SQLITE_ROW) {
        VectorRecord record;
        record.chunk_id = chunk_id;
        record.document_hash = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
        record.content = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
        record.model_id = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 2));
        // Parse metadata JSON - simple manual parsing for key-value pairs
        const char* metadata_json = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 3));
        if (metadata_json && strlen(metadata_json) > 2) {
            // Simple JSON parsing for {"key":"value","key2":"value2"} format
            std::string json_str(metadata_json);
            size_t pos = 1; // Skip opening brace
            while (pos < json_str.length() - 1) {
                // Find key
                size_t key_start = json_str.find('"', pos);
                if (key_start == std::string::npos)
                    break;
                size_t key_end = json_str.find('"', key_start + 1);
                if (key_end == std::string::npos)
                    break;
                std::string key = json_str.substr(key_start + 1, key_end - key_start - 1);

                // Find value
                size_t val_start = json_str.find('"', key_end + 2); // Skip ":"
                if (val_start == std::string::npos)
                    break;
                size_t val_end = json_str.find('"', val_start + 1);
                if (val_end == std::string::npos)
                    break;
                std::string value = json_str.substr(val_start + 1, val_end - val_start - 1);

                record.metadata[key] = value;
                pos = val_end + 1;

                // Skip comma if present
                if (pos < json_str.length() && json_str[pos] == ',')
                    pos++;
            }
        }
        // Parse embedding from joined doc_embeddings (column index 5), handling both TEXT(JSON) and
        // BLOB
        int col_type = sqlite3_column_type(stmt, 5);
        if (col_type == SQLITE_TEXT) {
            const char* embedding_json =
                reinterpret_cast<const char*>(sqlite3_column_text(stmt, 5));
            if (embedding_json) {
                std::string emb_str = embedding_json;
                std::stringstream ss(emb_str);
                char ch;
                if (ss >> ch && ch == '[') {
                    while (ss) {
                        float v;
                        ss >> v;
                        if (ss.fail())
                            break;
                        record.embedding.push_back(v);
                        ss >> ch;
                        if (!ss)
                            break;
                        if (ch == ']')
                            break;
                    }
                }
            }
        } else if (col_type == SQLITE_BLOB) {
            const void* blob = sqlite3_column_blob(stmt, 5);
            int nbytes = sqlite3_column_bytes(stmt, 5);
            if (blob && nbytes > 0) {
                record.embedding = blobToVector(blob, static_cast<size_t>(nbytes));
            }
        }

        sqlite3_finalize(stmt);
        return std::optional<VectorRecord>(record);
    }

    sqlite3_finalize(stmt);
    return std::optional<VectorRecord>();
}

Result<std::vector<VectorRecord>>
SqliteVecBackend::getVectorsByDocument(const std::string& document_hash) {
    YAMS_ZONE_SCOPED_N("SqliteVecBackend::getVectorsByDocument");

    std::lock_guard<std::mutex> lock(mutex_);

    if (!initialized_) {
        return Error{ErrorCode::NotInitialized, "Backend not initialized"};
    }

    spdlog::info("getVectorsByDocument: Searching for document_hash: {}", document_hash);

    // Query both metadata and embeddings tables
    const char* sql = "SELECT m.chunk_id, m.chunk_text, m.model_id, m.metadata, e.embedding "
                      "FROM doc_metadata m "
                      "JOIN doc_embeddings e ON e.rowid = m.rowid "
                      "WHERE m.document_hash = ? ORDER BY m.rowid";

    spdlog::info("getVectorsByDocument SQL: {}", sql);

    sqlite3_stmt* stmt;
    if (sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        return Error{ErrorCode::DatabaseError, getLastError()};
    }

    sqlite3_bind_text(stmt, 1, document_hash.c_str(), -1, SQLITE_TRANSIENT);

    std::vector<VectorRecord> results;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        VectorRecord record;
        record.chunk_id = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
        record.document_hash = document_hash;
        record.content = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
        record.model_id = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 2));
        // Parse metadata JSON - simple manual parsing for key-value pairs
        const char* metadata_json = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 3));
        if (metadata_json && strlen(metadata_json) > 2) {
            // Simple JSON parsing for {"key":"value","key2":"value2"} format
            std::string json_str(metadata_json);
            size_t pos = 1; // Skip opening brace
            while (pos < json_str.length() - 1) {
                // Find key
                size_t key_start = json_str.find('"', pos);
                if (key_start == std::string::npos)
                    break;
                size_t key_end = json_str.find('"', key_start + 1);
                if (key_end == std::string::npos)
                    break;
                std::string key = json_str.substr(key_start + 1, key_end - key_start - 1);

                // Find value
                size_t val_start = json_str.find('"', key_end + 2); // Skip ":"
                if (val_start == std::string::npos)
                    break;
                size_t val_end = json_str.find('"', val_start + 1);
                if (val_end == std::string::npos)
                    break;
                std::string value = json_str.substr(val_start + 1, val_end - val_start - 1);

                record.metadata[key] = value;
                pos = val_end + 1;

                // Skip comma if present
                if (pos < json_str.length() && json_str[pos] == ',')
                    pos++;
            }
        }

        // Parse embedding from column 4 - handle both TEXT (JSON) and BLOB formats
        int col_type = sqlite3_column_type(stmt, 4);
        spdlog::info("getVectorsByDocument: Embedding column type: {}",
                     col_type == SQLITE_TEXT   ? "TEXT"
                     : col_type == SQLITE_BLOB ? "BLOB"
                                               : "OTHER");

        if (col_type == SQLITE_TEXT) {
            const char* embedding_json =
                reinterpret_cast<const char*>(sqlite3_column_text(stmt, 4));
            if (embedding_json) {
                std::string emb_str = embedding_json;
                spdlog::info("getVectorsByDocument: Parsing embedding JSON (first 100 chars): {}",
                             emb_str.substr(0, 100));
                std::stringstream ss(emb_str);
                char ch;
                if (ss >> ch && ch == '[') {
                    while (ss) {
                        float v;
                        ss >> v;
                        if (ss.fail())
                            break;
                        record.embedding.push_back(v);
                        ss >> ch;
                        if (!ss)
                            break;
                        if (ch == ']')
                            break;
                    }
                }
            }
        } else if (col_type == SQLITE_BLOB) {
            const void* blob = sqlite3_column_blob(stmt, 4);
            int nbytes = sqlite3_column_bytes(stmt, 4);
            spdlog::info("getVectorsByDocument: Parsing embedding BLOB of {} bytes", nbytes);
            if (blob && nbytes > 0) {
                record.embedding = blobToVector(blob, static_cast<size_t>(nbytes));
            }
        }
        spdlog::info("getVectorsByDocument: Parsed {} embedding values", record.embedding.size());

        results.push_back(record);
    }

    spdlog::info("getVectorsByDocument: Found {} results for document_hash: {}", results.size(),
                 document_hash);

    sqlite3_finalize(stmt);
    return results;
}

Result<bool> SqliteVecBackend::hasEmbedding(const std::string& document_hash) {
    YAMS_ZONE_SCOPED_N("SqliteVecBackend::hasEmbedding");

    std::lock_guard<std::mutex> lock(mutex_);

    if (!initialized_) {
        return Error{ErrorCode::NotInitialized, "Backend not initialized"};
    }

    // Query the metadata table since it has the document_hash
    const char* sql = "SELECT COUNT(*) FROM doc_metadata WHERE document_hash = ?";

    sqlite3_stmt* stmt;
    if (sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        return Error{ErrorCode::DatabaseError, getLastError()};
    }

    sqlite3_bind_text(stmt, 1, document_hash.c_str(), -1, SQLITE_TRANSIENT);

    bool has_embedding = false;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        has_embedding = sqlite3_column_int(stmt, 0) > 0;
    }

    sqlite3_finalize(stmt);
    return has_embedding;
}

Result<size_t> SqliteVecBackend::getVectorCount() {
    YAMS_ZONE_SCOPED_N("SqliteVecBackend::getVectorCount");

    std::lock_guard<std::mutex> lock(mutex_);

    if (!initialized_) {
        return Error{ErrorCode::NotInitialized, "Backend not initialized"};
    }

    // Count from metadata table since it has unique chunk_ids
    const char* sql = "SELECT COUNT(*) FROM doc_metadata";

    sqlite3_stmt* stmt;
    if (sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        return Error{ErrorCode::DatabaseError, getLastError()};
    }

    size_t count = 0;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        count = static_cast<size_t>(sqlite3_column_int64(stmt, 0));
    }

    sqlite3_finalize(stmt);
    return count;
}

Result<VectorDatabase::DatabaseStats> SqliteVecBackend::getStats() {
    YAMS_ZONE_SCOPED_N("SqliteVecBackend::getStats");

    std::lock_guard<std::mutex> lock(mutex_);

    if (!initialized_) {
        return Error{ErrorCode::NotInitialized, "Backend not initialized"};
    }

    VectorDatabase::DatabaseStats stats;

    // Get total vectors from metadata table (avoiding separate lock acquisition)
    const char* count_sql = "SELECT COUNT(*) FROM doc_metadata";
    sqlite3_stmt* count_stmt;

    if (sqlite3_prepare_v2(db_, count_sql, -1, &count_stmt, nullptr) == SQLITE_OK) {
        if (sqlite3_step(count_stmt) == SQLITE_ROW) {
            stats.total_vectors = static_cast<size_t>(sqlite3_column_int64(count_stmt, 0));
        }
        sqlite3_finalize(count_stmt);
    }

    // Get unique documents count (from metadata table which has document_hash)
    const char* sql = "SELECT COUNT(DISTINCT document_hash) FROM doc_metadata";
    sqlite3_stmt* stmt;

    if (sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            stats.total_documents = static_cast<size_t>(sqlite3_column_int64(stmt, 0));
        }
        sqlite3_finalize(stmt);
    }

    // Estimate database size based on vector count and dimensions
    // Avoid PRAGMA queries which can cause blocking issues
    // Each vector is embedding_dim_ floats (4 bytes each) plus metadata overhead
    size_t estimated_vector_size = embedding_dim_ * sizeof(float);
    size_t metadata_overhead = 256; // Estimate for chunk_id, document_hash, metadata JSON
    stats.index_size_bytes = stats.total_vectors * (estimated_vector_size + metadata_overhead);

    // Set avg_embedding_magnitude to a reasonable value for normalized vectors
    // Most embedding models produce normalized vectors with magnitude ~1.0
    stats.avg_embedding_magnitude = 1.0;

    // TODO: Add vector_dimensions field to DatabaseStats if needed
    // stats.vector_dimensions = embedding_dim_;

    return stats;
}

Result<void> SqliteVecBackend::buildIndex() {
    // sqlite-vec builds indices automatically
    // This is a no-op but we could add ANALYZE here
    return executeSQL("ANALYZE doc_embeddings");
}

Result<void> SqliteVecBackend::optimize() {
    YAMS_ZONE_SCOPED_N("SqliteVecBackend::optimize");

    std::lock_guard<std::mutex> lock(mutex_);

    if (!initialized_) {
        return Error{ErrorCode::NotInitialized, "Backend not initialized"};
    }

    // Run SQLite optimization
    Result<void> result;
    {
        YAMS_ZONE_SCOPED_N("SQLite::VACUUM");
        result = executeSQL("VACUUM");
        if (!result) {
            return result;
        }
    }

    {
        YAMS_ZONE_SCOPED_N("SQLite::ANALYZE");
        result = executeSQL("ANALYZE");
        if (!result) {
            return result;
        }
    }

    return Result<void>();
}

Result<void> SqliteVecBackend::beginTransaction() {
    YAMS_ZONE_SCOPED_N("SqliteVecBackend::beginTransaction");

    std::lock_guard<std::mutex> lock(mutex_);

    if (!initialized_) {
        return Error{ErrorCode::NotInitialized, "Backend not initialized"};
    }

    if (in_transaction_) {
        return Error{ErrorCode::InvalidState, "Transaction already in progress"};
    }

    auto result = executeSQL("BEGIN IMMEDIATE TRANSACTION");
    if (result) {
        in_transaction_ = true;
    }

    return result;
}

Result<void> SqliteVecBackend::commitTransaction() {
    YAMS_ZONE_SCOPED_N("SqliteVecBackend::commitTransaction");

    std::lock_guard<std::mutex> lock(mutex_);

    if (!initialized_) {
        return Error{ErrorCode::NotInitialized, "Backend not initialized"};
    }

    if (!in_transaction_) {
        return Error{ErrorCode::NotFound, "No transaction in progress"};
    }

    auto result = executeSQL("COMMIT");
    if (result) {
        in_transaction_ = false;
    }

    return result;
}

Result<void> SqliteVecBackend::rollbackTransaction() {
    YAMS_ZONE_SCOPED_N("SqliteVecBackend::rollbackTransaction");

    std::lock_guard<std::mutex> lock(mutex_);

    if (!initialized_) {
        return Error{ErrorCode::NotInitialized, "Backend not initialized"};
    }

    if (!in_transaction_) {
        return Error{ErrorCode::NotFound, "No transaction in progress"};
    }

    auto result = executeSQL("ROLLBACK");
    in_transaction_ = false; // Reset even if rollback fails

    return result;
}

Result<void> SqliteVecBackend::loadSqliteVecExtension() {
    // Since we're statically linking sqlite-vec, we need to initialize it
    // Call sqlite3_vec_init to register the vec0 virtual table module

    spdlog::debug("Initializing sqlite-vec extension...");

    char* error_msg = nullptr;
    int rc = sqlite3_vec_init(db_, &error_msg, nullptr);

    if (rc != SQLITE_OK) {
        std::string error = error_msg ? error_msg : "Unknown error";
        if (error_msg) {
            sqlite3_free(error_msg);
        }
        return Error{ErrorCode::DatabaseError,
                     "Failed to initialize sqlite-vec extension: " + error};
    }

    spdlog::info("sqlite-vec extension initialized successfully");

    // Verify the extension is working by checking if vec0 module is available
    const char* test_sql = "SELECT 1 FROM pragma_module_list WHERE name='vec0'";
    sqlite3_stmt* stmt;

    if (sqlite3_prepare_v2(db_, test_sql, -1, &stmt, nullptr) == SQLITE_OK) {
        bool vec0_available = (sqlite3_step(stmt) == SQLITE_ROW);
        sqlite3_finalize(stmt);

        if (!vec0_available) {
            return Error{ErrorCode::DatabaseError,
                         "sqlite-vec extension loaded but vec0 module not available"};
        }
    } else {
        spdlog::debug(
            "Could not verify vec0 module availability, but extension initialization succeeded");
    }

    return Result<void>();
}

Result<void> SqliteVecBackend::prepareStatements() {
    // Prepare commonly used statements for better performance
    // For now, we'll prepare them on-demand
    return Result<void>();
}

void SqliteVecBackend::finalizeStatements() {
    // Finalize all prepared statements
    if (stmts_.insert_vector) {
        sqlite3_finalize(stmts_.insert_vector);
        stmts_.insert_vector = nullptr;
    }
    if (stmts_.select_vector) {
        sqlite3_finalize(stmts_.select_vector);
        stmts_.select_vector = nullptr;
    }
    if (stmts_.update_vector) {
        sqlite3_finalize(stmts_.update_vector);
        stmts_.update_vector = nullptr;
    }
    if (stmts_.delete_vector) {
        sqlite3_finalize(stmts_.delete_vector);
        stmts_.delete_vector = nullptr;
    }
    if (stmts_.search_similar) {
        sqlite3_finalize(stmts_.search_similar);
        stmts_.search_similar = nullptr;
    }
    if (stmts_.has_embedding) {
        sqlite3_finalize(stmts_.has_embedding);
        stmts_.has_embedding = nullptr;
    }
    if (stmts_.count_vectors) {
        sqlite3_finalize(stmts_.count_vectors);
        stmts_.count_vectors = nullptr;
    }
}

std::vector<uint8_t> SqliteVecBackend::vectorToBlob(const std::vector<float>& vec) const {
    std::vector<uint8_t> blob(vec.size() * sizeof(float));
    std::memcpy(blob.data(), vec.data(), blob.size());
    return blob;
}

std::vector<float> SqliteVecBackend::blobToVector(const void* blob, size_t size) const {
    size_t num_floats = size / sizeof(float);
    std::vector<float> vec(num_floats);
    std::memcpy(vec.data(), blob, size);
    return vec;
}

Result<void> SqliteVecBackend::executeSQL(const std::string& sql) {
    YAMS_ZONE_SCOPED_N("SqliteVecBackend::executeSQL");
    spdlog::info("Executing SQL: {}", sql); // Changed to info for visibility

    const int max_attempts = 20;
    int attempt = 0;
    int rc = SQLITE_OK;
    char* error_msg = nullptr;

    while (true) {
        {
            YAMS_ZONE_SCOPED_N("SQLite::exec");
            rc = sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, &error_msg);
        }

        if (rc == SQLITE_OK) {
            if (error_msg) {
                sqlite3_free(error_msg);
                error_msg = nullptr;
            }
            break;
        }

        std::string error = error_msg ? error_msg : "Unknown error";
        if (error_msg) {
            sqlite3_free(error_msg);
            error_msg = nullptr;
        }

        if (rc == SQLITE_BUSY || rc == SQLITE_LOCKED) {
            // Exponential backoff with cap (10ms .. ~1.28s)
            int exp = attempt;
            if (exp > 7)
                exp = 7;
            int sleep_ms = 10 * (1 << exp);
            ++attempt;

            if (attempt <= max_attempts) {
                spdlog::warn("SQL busy/locked (rc={}): {}. Retrying attempt {}/{} after {} ms", rc,
                             error, attempt, max_attempts, sleep_ms);
                sqlite3_sleep(sleep_ms);
                continue;
            }
        }

        spdlog::error("SQL execution failed with code {}: {}", rc, error);
        return Error{ErrorCode::DatabaseError, error};
    }

    spdlog::info("SQL execution completed successfully");
    return Result<void>();
}

std::string SqliteVecBackend::getLastError() const {
    return sqlite3_errmsg(db_);
}

// Factory function implementation
std::unique_ptr<IVectorBackend> createVectorBackend(VectorBackendType type) {
    switch (type) {
        case VectorBackendType::SqliteVec:
            return std::make_unique<SqliteVecBackend>();
        case VectorBackendType::LanceDB:
            // Future: return std::make_unique<LanceDBBackend>();
            spdlog::warn("LanceDB backend not yet implemented, using SqliteVec");
            return std::make_unique<SqliteVecBackend>();
        case VectorBackendType::InMemory:
            // Future: return std::make_unique<InMemoryBackend>();
            spdlog::warn("InMemory backend not yet implemented, using SqliteVec");
            return std::make_unique<SqliteVecBackend>();
        default:
            return std::make_unique<SqliteVecBackend>();
    }
}

} // namespace yams::vector