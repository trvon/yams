#include <spdlog/spdlog.h>
#include <chrono>
#include <cstring>
#include <future>
#include <sstream>
#include <yams/profiling.h>
#include <yams/vector/sqlite_vec_backend.h>

#ifdef SQLITE_VEC_CPP
#include <sqlite-vec-cpp/distances/cosine.hpp>
#include <sqlite-vec-cpp/distances/l1.hpp>
#include <sqlite-vec-cpp/distances/l2.hpp>
#include <sqlite-vec-cpp/sqlite/utility_functions.hpp>
#include <sqlite-vec-cpp/sqlite_vec.hpp>
#include <sqlite-vec-cpp/utils/array.hpp>
#include <sqlite-vec-cpp/vector_view.hpp>
#else
extern "C" {
#include "sqlite-vec.h"
}
#endif

/*
 * IMPORTANT ARCHITECTURAL DECISION: Shadow Table Usage
 *
 * This implementation uses SQLite shadow tables directly instead of vec0 virtual tables
 * for INSERT/UPDATE operations. This is intentional and necessary.
 *
 * WHY: SQLite virtual table xUpdate callback doesn't properly materialize function results
 * (like vec_f32()) that return blobs with subtypes. When vec_f32('[1,2,3]')
 * is called in an INSERT statement, the blob is created successfully with subtype=223,
 * but when passed to xUpdate via argv[2], the subtype is lost (subtype=0) and
 * blob data is NULL (blob=0x0, bytes=0). This is a fundamental SQLite virtual table
 * limitation. See: https://www.sqlite.org/vtab.html#xupdate
 *
 * SHADOW TABLE BENEFITS:
 * - Better performance (no virtual table overhead)
 * - Direct access to blob data without materialization issues
 * - Full control over indexes and constraints
 * - More reliable (tested and proven to work)
 *
 * ALTERNATIVES CONSIDERED:
 * - Fix vec0 xUpdate to handle subtypes (attempted, failed after hours of debugging)
 * - Use vec_f32_simple() without subtype (breaks other functionality)
 * - Various SQLite internal hacks (all failed, low success rate)
 *
 * DECISION: Shadow table approach is the pragmatic, maintainable choice.
 */

namespace yams::vector {

namespace {
bool columnExists(sqlite3* db, const char* table, const char* column) {
    if (!db || !table || !column)
        return false;
    std::string sql = "PRAGMA table_info(" + std::string(table) + ")";
    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK) {
        return false;
    }
    bool found = false;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const unsigned char* name = sqlite3_column_text(stmt, 1);
        if (name && std::string(reinterpret_cast<const char*>(name)) == column) {
            found = true;
            break;
        }
    }
    sqlite3_finalize(stmt);
    return found;
}
} // namespace

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
    : db_(nullptr), embedding_dim_(0), initialized_(false), in_transaction_(false), stmts_{} {
    // Initialize all statement pointers to nullptr (done by stmts_{} above)
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
    other.stmts_ = {}; // Reset all statement pointers to nullptr
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
        other.stmts_ = {}; // Reset all statement pointers to nullptr
    }
    return *this;
}

Result<void> SqliteVecBackend::validateEmbeddingDim(size_t actual_dim) const {
    if (embedding_dim_ > 0 && actual_dim != embedding_dim_) {
        std::stringstream ss;
        ss << "Embedding dimension mismatch (expected=" << embedding_dim_ << ", got=" << actual_dim
           << ")";
        return Error{ErrorCode::InvalidArgument, ss.str()};
    }
    return Result<void>();
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
        // Prefer open_v2 with FULLMUTEX to avoid cross-thread hazards
        rc = sqlite3_open_v2(db_path.c_str(), &db_,
                             SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX,
                             nullptr);
    }

    if (rc != SQLITE_OK) {
        std::string error = sqlite3_errmsg(db_);
        sqlite3_close(db_);
        db_ = nullptr;
        return Error{ErrorCode::DatabaseError, "Failed to open database: " + error};
    }

    // Set busy timeout to avoid indefinite blocking (configurable)
    int busy_ms = 5000; // default 5s
    if (const char* env_busy = std::getenv("YAMS_SQLITE_BUSY_TIMEOUT_MS")) {
        try {
            busy_ms = std::max(100, std::stoi(env_busy));
        } catch (...) {
        }
    }
    sqlite3_busy_timeout(db_, busy_ms);

    db_path_ = db_path;

    spdlog::info("Database opened successfully: {}", db_path);
    // Optional minimal mode (for doctor/maintenance) to avoid heavyweight PRAGMAs
    bool minimal_pragmas = false;
    if (const char* env_min = std::getenv("YAMS_SQLITE_MINIMAL_PRAGMAS")) {
        std::string v(env_min);
        for (auto& c : v)
            c = static_cast<char>(std::tolower(c));
        minimal_pragmas = (v == "1" || v == "true" || v == "yes" || v == "on");
    }
    // Enable additional pragmas for robustness and performance (skippable)
    if (!minimal_pragmas) {
        executeSQL("PRAGMA foreign_keys=ON");
        executeSQL("PRAGMA temp_store=MEMORY");
        // 256 MiB memory map for improved I/O performance (bytes)
        executeSQL("PRAGMA mmap_size=268435456");
    } else {
        // Minimal footprint: avoid any PRAGMA that could contend or block
        // Diagnostics-only mode relies on sane SQLite defaults.
    }

    // Harden connection against SQLITE_BUSY/LOCKED
    sqlite3_extended_result_codes(db_, 1);
    // In minimal mode, skip WAL-related PRAGMAs to minimize lock contention
    if (!minimal_pragmas) {
        auto pragma = executeSQL("PRAGMA journal_mode=WAL");
        if (!pragma) {
            spdlog::warn("Failed to enable WAL journal mode: {}. Continuing with default mode.",
                         pragma.error().message);
        }
        // Reasonable durability/performance trade-offs for WAL
        executeSQL("PRAGMA synchronous=NORMAL");
        executeSQL("PRAGMA wal_autocheckpoint=1000");
    }

    // Optionally defer sqlite-vec initialization (doctor may load it separately)
    bool skip_vec = false;
    if (const char* env = std::getenv("YAMS_SQLITE_VEC_SKIP_INIT")) {
        std::string v(env);
        for (auto& c : v)
            c = static_cast<char>(std::tolower(c));
        skip_vec = (v == "1" || v == "true" || v == "yes" || v == "on");
    }
    if (!skip_vec) {
        Result<void> result;
        {
            YAMS_ZONE_SCOPED_N("SQLite::LoadVecExtension");
            int timeout_ms = 5000; // default 5s
            if (const char* env = std::getenv("YAMS_SQLITE_VEC_INIT_TIMEOUT_MS")) {
                try {
                    timeout_ms = std::stoi(env);
                    if (timeout_ms < 500)
                        timeout_ms = 500;
                } catch (...) {
                }
            }
            auto fut =
                std::async(std::launch::async, [this]() { return loadSqliteVecExtension(); });
            if (fut.wait_for(std::chrono::milliseconds(timeout_ms)) ==
                std::future_status::timeout) {
                close();
                return Error{ErrorCode::Timeout,
                             std::string("sqlite-vec extension initialization timed out after ") +
                                 std::to_string(timeout_ms) + " ms"};
            }
            result = fut.get();
        }
        if (!result) {
            close();
            return Error{ErrorCode::DatabaseError,
                         "Failed to load sqlite-vec extension: " + result.error().message};
        }
    }

    // Configure busy timeout (env override)
    try {
        busy_ms = 10000;
        if (const char* env = std::getenv("YAMS_SQLITE_BUSY_TIMEOUT_MS")) {
            try {
                busy_ms = std::max(0, std::stoi(env));
            } catch (...) {
            }
        }
        sqlite3_busy_timeout(db_, busy_ms);
    } catch (...) {
    }

    // Pre-populating embedding_dim_ from existing schema is now handled by the caller
    // (e.g., doctor command) before initialization, to avoid queries during open.

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
            embedding_rowid INTEGER UNIQUE,
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

    index_result = executeSQL("CREATE UNIQUE INDEX IF NOT EXISTS idx_doc_metadata_embedding_rowid "
                              "ON doc_metadata(embedding_rowid)");
    if (!index_result) {
        spdlog::warn("Failed to create embedding_rowid index: {}", index_result.error().message);
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

    sqlite3_stmt* stmt;

    const char* vec0_check = "SELECT 1 FROM pragma_module_list WHERE name='vec0'";
    if (sqlite3_prepare_v2(db_, vec0_check, -1, &stmt, nullptr) == SQLITE_OK) {
        bool vec0_available = (sqlite3_step(stmt) == SQLITE_ROW);
        sqlite3_finalize(stmt);
        if (!vec0_available) {
            spdlog::warn("vec0 module not available - tables cannot be vec0 virtual tables");
            return false;
        }
    } else {
        spdlog::debug("Failed to check vec0 module: {}", sqlite3_errmsg(db_));
        return false;
    }

    const char* vector_check_sql =
        "SELECT sql FROM sqlite_master WHERE name='doc_embeddings' AND type='table' LIMIT 1";
    if (sqlite3_prepare_v2(db_, vector_check_sql, -1, &stmt, nullptr) != SQLITE_OK) {
        spdlog::debug("Failed to check vector table: {}", sqlite3_errmsg(db_));
        return false;
    }

    bool vector_exists = false;
    bool is_vec0_table = false;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        vector_exists = true;
        const unsigned char* sql_text = sqlite3_column_text(stmt, 0);
        if (sql_text) {
            std::string ddl(reinterpret_cast<const char*>(sql_text));
            is_vec0_table = (ddl.find("USING vec0") != std::string::npos);
            if (!is_vec0_table) {
                spdlog::warn("doc_embeddings exists but is NOT a vec0 virtual table");
            }
        }
    }
    sqlite3_finalize(stmt);

    if (!vector_exists || !is_vec0_table) {
        return false;
    }

    const char* metadata_check_sql =
        "SELECT name FROM sqlite_master WHERE name='doc_metadata' LIMIT 1";
    if (sqlite3_prepare_v2(db_, metadata_check_sql, -1, &stmt, nullptr) != SQLITE_OK) {
        spdlog::debug("Failed to check metadata table: {}", sqlite3_errmsg(db_));
        return false;
    }
    bool metadata_exists = (sqlite3_step(stmt) == SQLITE_ROW);
    sqlite3_finalize(stmt);

    if (!metadata_exists) {
        return false;
    }

    spdlog::debug("Tables exist and doc_embeddings is a valid vec0 virtual table");
    return true;
}

std::optional<size_t> SqliteVecBackend::getStoredEmbeddingDimension() const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!initialized_) {
        return std::nullopt;
    }
    const char* sql = "SELECT sql FROM sqlite_master WHERE name='doc_embeddings' LIMIT 1";
    sqlite3_stmt* stmt = nullptr;
    std::optional<size_t> out{};
    if (sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            const unsigned char* txt = sqlite3_column_text(stmt, 0);
            if (txt) {
                std::string ddl(reinterpret_cast<const char*>(txt));
                auto pos = ddl.find("float[");
                if (pos != std::string::npos) {
                    auto end = ddl.find(']', pos);
                    if (end != std::string::npos && end > pos + 6) {
                        std::string num = ddl.substr(pos + 6, end - (pos + 6));
                        try {
                            size_t dim = static_cast<size_t>(std::stoul(num));
                            out = dim;
                        } catch (...) {
                        }
                    }
                }
            }
        }
        sqlite3_finalize(stmt);
    }
    return out;
}

Result<void> SqliteVecBackend::dropTables() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!initialized_) {
        return Error{ErrorCode::NotInitialized, "Backend not initialized"};
    }
    // Best-effort drops; proceed even if one is missing
    auto r1 = executeSQL("DROP TABLE IF EXISTS doc_embeddings");
    if (!r1) {
        spdlog::warn("Failed to drop doc_embeddings: {}", r1.error().message);
    }
    auto r2 = executeSQL("DROP TABLE IF EXISTS doc_metadata");
    if (!r2) {
        spdlog::warn("Failed to drop doc_metadata: {}", r2.error().message);
    }
    return Result<void>();
}

Result<void> SqliteVecBackend::ensureEmbeddingRowIdColumn() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!initialized_) {
        return Error{ErrorCode::NotInitialized, "Backend not initialized"};
    }

    if (!columnExists(db_, "doc_metadata", "embedding_rowid")) {
        auto alter = executeSQL("ALTER TABLE doc_metadata ADD COLUMN embedding_rowid INTEGER");
        if (!alter) {
            return Error{ErrorCode::DatabaseError,
                         "Failed to add embedding_rowid column: " + alter.error().message};
        }
    }

    auto backfill =
        executeSQL("UPDATE doc_metadata SET embedding_rowid = rowid WHERE embedding_rowid IS NULL");
    if (!backfill) {
        return Error{ErrorCode::DatabaseError,
                     "Failed to backfill embedding_rowid: " + backfill.error().message};
    }

    auto index = executeSQL("CREATE UNIQUE INDEX IF NOT EXISTS idx_doc_metadata_embedding_rowid "
                            "ON doc_metadata(embedding_rowid)");
    if (!index) {
        return Error{ErrorCode::DatabaseError,
                     "Failed to create embedding_rowid index: " + index.error().message};
    }

    return Result<void>();
}

Result<SqliteVecBackend::OrphanCleanupStats> SqliteVecBackend::cleanupOrphanRows() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!initialized_) {
        return Error{ErrorCode::NotInitialized, "Backend not initialized"};
    }

    if (!columnExists(db_, "doc_metadata", "embedding_rowid")) {
        auto alter = executeSQL("ALTER TABLE doc_metadata ADD COLUMN embedding_rowid INTEGER");
        if (!alter) {
            return Error{ErrorCode::DatabaseError,
                         "Failed to add embedding_rowid column: " + alter.error().message};
        }
    }

    OrphanCleanupStats stats{};
    auto backfill =
        executeSQL("UPDATE doc_metadata SET embedding_rowid = rowid WHERE embedding_rowid IS NULL");
    if (!backfill) {
        return Error{ErrorCode::DatabaseError,
                     "Failed to backfill embedding_rowid: " + backfill.error().message};
    }
    stats.metadata_backfilled = static_cast<std::size_t>(sqlite3_changes(db_));

    auto tx = executeSQL("BEGIN IMMEDIATE TRANSACTION");
    if (!tx) {
        return Error{ErrorCode::DatabaseError,
                     "Failed to begin cleanup transaction: " + tx.error().message};
    }

    auto pruneMeta = executeSQL("DELETE FROM doc_metadata "
                                "WHERE embedding_rowid IS NULL "
                                "OR embedding_rowid NOT IN (SELECT rowid FROM doc_embeddings)");
    if (!pruneMeta) {
        executeSQL("ROLLBACK");
        return Error{ErrorCode::DatabaseError,
                     "Failed to prune orphan metadata: " + pruneMeta.error().message};
    }
    stats.metadata_removed = static_cast<std::size_t>(sqlite3_changes(db_));

    auto pruneEmb = executeSQL("DELETE FROM doc_embeddings "
                               "WHERE rowid NOT IN (SELECT embedding_rowid FROM doc_metadata WHERE "
                               "embedding_rowid IS NOT NULL)");
    if (!pruneEmb) {
        executeSQL("ROLLBACK");
        return Error{ErrorCode::DatabaseError,
                     "Failed to prune orphan embeddings: " + pruneEmb.error().message};
    }
    stats.embeddings_removed = static_cast<std::size_t>(sqlite3_changes(db_));

    auto commit = executeSQL("COMMIT");
    if (!commit) {
        executeSQL("ROLLBACK");
        return Error{ErrorCode::DatabaseError,
                     "Failed to commit cleanup transaction: " + commit.error().message};
    }

    return stats;
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

    if (auto vr = validateEmbeddingDim(record.embedding.size()); !vr)
        return vr;

    // Check if chunk_id already exists (for upsert behavior)
    const char* check_sql = "SELECT rowid, embedding_rowid FROM doc_metadata WHERE chunk_id = ?";
    sqlite3_stmt* check_stmt;
    sqlite3_int64 existing_meta_rowid = -1;
    sqlite3_int64 existing_embed_rowid = -1;

    if (sqlite3_prepare_v2(db_, check_sql, -1, &check_stmt, nullptr) == SQLITE_OK) {
        sqlite3_bind_text(check_stmt, 1, record.chunk_id.c_str(), -1, SQLITE_TRANSIENT);

        if (sqlite3_step(check_stmt) == SQLITE_ROW) {
            existing_meta_rowid = sqlite3_column_int64(check_stmt, 0);
            if (sqlite3_column_type(check_stmt, 1) != SQLITE_NULL) {
                existing_embed_rowid = sqlite3_column_int64(check_stmt, 1);
            }
            spdlog::debug("Chunk {} already exists with metadata_rowid {} embedding_rowid {}",
                          record.chunk_id, existing_meta_rowid, existing_embed_rowid);
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

        if (existing_meta_rowid != -1) {
            // Update existing record
            if (existing_embed_rowid == -1) {
                if (manage_transaction)
                    executeSQL("ROLLBACK");
                return Error{ErrorCode::DatabaseError,
                             "Missing embedding_rowid for chunk_id: " + record.chunk_id};
            }
            vector_rowid = existing_embed_rowid;

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
                SET document_hash = ?, chunk_text = ?, model_id = ?, metadata = ?, embedding_rowid = ?
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
            sqlite3_bind_int64(metadata_stmt, 6, existing_meta_rowid);

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
            // 1. Insert vector into shadow table directly (workaround for vec0 virtual table issue)
            // Note: SQLite virtual table xUpdate doesn't properly materialize function results
            // (like vec_f32()) that return blobs with subtypes. Using shadow tables
            // directly is more reliable and avoids this virtual table limitation.
            // See: https://www.sqlite.org/vtab.html and
            // third_party/sqlite-vec-cpp/include/sqlite-vec-cpp/sqlite/vec0_module.hpp:327-397
            // Convert vector to blob format directly
            spdlog::debug("Inserting vector into shadow table ({} dimensions)",
                          record.embedding.size());
            std::vector<uint8_t> embedding_blob = vectorToBlob(record.embedding);

            // Insert directly into the shadow table
            std::string shadow_sql =
                "INSERT INTO doc_embeddings_vectors (rowid, embedding) VALUES (NULL, ?)";
            spdlog::info("Inserting directly into shadow table with blob ({} bytes)",
                         embedding_blob.size());

            sqlite3_stmt* shadow_stmt;
            if (sqlite3_prepare_v2(db_, shadow_sql.c_str(), -1, &shadow_stmt, nullptr) !=
                SQLITE_OK) {
                if (manage_transaction)
                    executeSQL("ROLLBACK");
                return Error{ErrorCode::DatabaseError,
                             "Failed to prepare shadow insert: " + getLastError()};
            }

            sqlite3_bind_blob(shadow_stmt, 1, embedding_blob.data(),
                              static_cast<int>(embedding_blob.size()), SQLITE_TRANSIENT);

            int insert_rc = stepWithRetry(shadow_stmt);
            sqlite3_finalize(shadow_stmt);

            if (insert_rc != SQLITE_DONE) {
                spdlog::error("Failed to insert into shadow table: rc={}, error={}", insert_rc,
                              getLastError());
                if (manage_transaction)
                    executeSQL("ROLLBACK");
                return Error{ErrorCode::DatabaseError,
                             "Failed to insert vector: " + getLastError()};
            }
            spdlog::info("Vector inserted into shadow table successfully");

            // Get the rowid
            sqlite3_int64 vector_rowid = sqlite3_last_insert_rowid(db_);
            spdlog::info("Got vector_rowid: {}", vector_rowid);

            // 2. Insert metadata into regular table with embedding_rowid reference
            const char* metadata_sql = R"(
                INSERT INTO doc_metadata (embedding_rowid, document_hash, chunk_id, chunk_text, model_id, metadata)
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

            int rc;
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
    // Promote a high-visibility log so operators can confirm vectordb writes during repair jobs
    spdlog::warn("[vectordb] batch insert starting: count={} db={}", records.size(), db_path_);

    if (records.empty()) {
        return Result<void>();
    }

    std::lock_guard<std::mutex> lock(mutex_);
    spdlog::debug("insertVectorsBatch acquired lock");

    if (!initialized_) {
        return Error{ErrorCode::NotInitialized, "Backend not initialized"};
    }

    for (size_t i = 0; i < records.size(); ++i) {
        if (auto vr = validateEmbeddingDim(records[i].embedding.size()); !vr)
            return vr;
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

    // High-visibility confirmation after successful commit
    spdlog::warn("[vectordb] batch insert committed: inserted={} db={}", records.size(), db_path_);

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

    if (auto vr = validateEmbeddingDim(record.embedding.size()); !vr)
        return vr;

    // Start transaction
    auto tx_result = executeSQL("BEGIN TRANSACTION");
    if (!tx_result) {
        return Error{ErrorCode::DatabaseError,
                     "Failed to begin transaction: " + tx_result.error().message};
    }

    try {
        // First, get the rowid for this chunk_id
        const char* get_rowid_sql =
            "SELECT rowid, embedding_rowid FROM doc_metadata WHERE chunk_id = ?";
        sqlite3_stmt* rowid_stmt;

        if (sqlite3_prepare_v2(db_, get_rowid_sql, -1, &rowid_stmt, nullptr) != SQLITE_OK) {
            executeSQL("ROLLBACK");
            return Error{ErrorCode::DatabaseError,
                         "Failed to prepare rowid query: " + getLastError()};
        }

        sqlite3_bind_text(rowid_stmt, 1, chunk_id.c_str(), -1, SQLITE_TRANSIENT);

        sqlite3_int64 meta_rowid = -1;
        sqlite3_int64 embedding_rowid = -1;
        if (sqlite3_step(rowid_stmt) == SQLITE_ROW) {
            meta_rowid = sqlite3_column_int64(rowid_stmt, 0);
            if (sqlite3_column_type(rowid_stmt, 1) != SQLITE_NULL) {
                embedding_rowid = sqlite3_column_int64(rowid_stmt, 1);
            }
        }
        sqlite3_finalize(rowid_stmt);

        if (meta_rowid == -1 || embedding_rowid == -1) {
            executeSQL("ROLLBACK");
            return Error{ErrorCode::NotFound, "Vector not found with chunk_id: " + chunk_id};
        }

        // Update the vector in the shadow table directly
        std::vector<uint8_t> embedding_blob = vectorToBlob(record.embedding);

        const char* vector_sql = "UPDATE doc_embeddings_vectors SET embedding = ? WHERE rowid = ?";
        sqlite3_stmt* vector_stmt;

        if (sqlite3_prepare_v2(db_, vector_sql, -1, &vector_stmt, nullptr) != SQLITE_OK) {
            executeSQL("ROLLBACK");
            return Error{ErrorCode::DatabaseError,
                         "Failed to prepare vector update: " + getLastError()};
        }

        sqlite3_bind_blob(vector_stmt, 1, embedding_blob.data(),
                          static_cast<int>(embedding_blob.size()), SQLITE_TRANSIENT);
        sqlite3_bind_int64(vector_stmt, 2, embedding_rowid);

        int rc = stepWithRetry(vector_stmt);
        sqlite3_finalize(vector_stmt);

        if (rc != SQLITE_DONE) {
            executeSQL("ROLLBACK");
            return Error{ErrorCode::DatabaseError, "Failed to update vector: " + getLastError()};
        }

        // Update the metadata
        const char* metadata_sql = "UPDATE doc_metadata SET document_hash = ?, chunk_text = ?, "
                                   "model_id = ?, metadata = ?, "
                                   "embedding_rowid = ? WHERE rowid = ?";
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
        sqlite3_bind_int64(metadata_stmt, 5, embedding_rowid);
        sqlite3_bind_int64(metadata_stmt, 6, meta_rowid);

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
        const char* get_rowid_sql =
            "SELECT rowid, embedding_rowid FROM doc_metadata WHERE chunk_id = ?";
        sqlite3_stmt* rowid_stmt;

        if (sqlite3_prepare_v2(db_, get_rowid_sql, -1, &rowid_stmt, nullptr) != SQLITE_OK) {
            executeSQL("ROLLBACK");
            return Error{ErrorCode::DatabaseError,
                         "Failed to prepare rowid query: " + getLastError()};
        }

        sqlite3_bind_text(rowid_stmt, 1, chunk_id.c_str(), -1, SQLITE_TRANSIENT);

        sqlite3_int64 meta_rowid = -1;
        sqlite3_int64 embedding_rowid = -1;
        if (sqlite3_step(rowid_stmt) == SQLITE_ROW) {
            meta_rowid = sqlite3_column_int64(rowid_stmt, 0);
            if (sqlite3_column_type(rowid_stmt, 1) != SQLITE_NULL) {
                embedding_rowid = sqlite3_column_int64(rowid_stmt, 1);
            }
        }
        sqlite3_finalize(rowid_stmt);

        if (meta_rowid == -1 || embedding_rowid == -1) {
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

        sqlite3_bind_int64(metadata_stmt, 1, meta_rowid);

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

        sqlite3_bind_int64(vector_stmt, 1, embedding_rowid);

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
        const char* get_rowids_sql = "SELECT embedding_rowid FROM doc_metadata "
                                     "WHERE document_hash = ? AND embedding_rowid IS NOT NULL";
        sqlite3_stmt* rowids_stmt;

        if (sqlite3_prepare_v2(db_, get_rowids_sql, -1, &rowids_stmt, nullptr) != SQLITE_OK) {
            executeSQL("ROLLBACK");
            return Error{ErrorCode::DatabaseError,
                         "Failed to prepare rowids query: " + getLastError()};
        }

        sqlite3_bind_text(rowids_stmt, 1, document_hash.c_str(), -1, SQLITE_TRANSIENT);

        std::vector<sqlite3_int64> embedding_rowids;
        while (sqlite3_step(rowids_stmt) == SQLITE_ROW) {
            embedding_rowids.push_back(sqlite3_column_int64(rowids_stmt, 0));
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

        // Delete from vector table for each embedding_rowid
        const char* vector_sql = "DELETE FROM doc_embeddings WHERE rowid = ?";
        for (auto rowid : embedding_rowids) {
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
    // Embed the query vector JSON directly in the SQL
    std::stringstream sql;
    sql << "SELECT m.chunk_id, m.document_hash, m.chunk_text, m.model_id, m.metadata, "
        << "vec_distance_cosine(e.embedding, vec_f32('" << query_json.str() << "')) as distance "
        << "FROM doc_embeddings e "
        << "JOIN doc_metadata m ON e.rowid = m.embedding_rowid "
        << "ORDER BY distance ASC LIMIT " << k;

    spdlog::info("Search SQL: {}", sql.str());

    sqlite3_stmt* stmt;
    {
        YAMS_ZONE_SCOPED_N("VectorSearch::PrepareStatement");
        int prep_rc = sqlite3_prepare_v2(db_, sql.str().c_str(), -1, &stmt, nullptr);
        spdlog::info("Prepare returned: {}", prep_rc);
        if (prep_rc != SQLITE_OK) {
            spdlog::error("Prepare failed: {}", sqlite3_errmsg(db_));
            return Error{ErrorCode::DatabaseError, getLastError()};
        }
    }

    std::vector<VectorRecord> results;
    spdlog::info("About to execute search query");
    {
        YAMS_ZONE_SCOPED_N("VectorSearch::ProcessResults");
        int row_count = 0;
        int step_rc;
        while ((step_rc = sqlite3_step(stmt)) == SQLITE_ROW) {
            row_count++;
            spdlog::info("Got row {} from search", row_count);
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
    const char* sql = "SELECT m.document_hash, m.chunk_text, m.model_id, m.metadata, "
                      "m.embedding_rowid, e.embedding "
                      "FROM doc_metadata m JOIN doc_embeddings e ON e.rowid = m.embedding_rowid "
                      "WHERE m.chunk_id = ?";

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

Result<std::map<std::string, VectorRecord>>
SqliteVecBackend::getVectorsBatch(const std::vector<std::string>& chunk_ids) {
    YAMS_ZONE_SCOPED_N("SqliteVecBackend::getVectorsBatch");

    std::lock_guard<std::mutex> lock(mutex_);

    if (!initialized_) {
        return Error{ErrorCode::NotInitialized, "Backend not initialized"};
    }

    if (chunk_ids.empty()) {
        return std::map<std::string, VectorRecord>{};
    }

    // Build SQL with IN clause for batch lookup
    std::stringstream sql;
    sql << "SELECT m.chunk_id, m.document_hash, m.chunk_text, m.model_id, m.metadata "
        << "FROM doc_metadata m WHERE m.chunk_id IN (";
    for (size_t i = 0; i < chunk_ids.size(); ++i) {
        if (i > 0)
            sql << ",";
        sql << "?";
    }
    sql << ")";

    sqlite3_stmt* stmt;
    if (sqlite3_prepare_v2(db_, sql.str().c_str(), -1, &stmt, nullptr) != SQLITE_OK) {
        return Error{ErrorCode::DatabaseError, getLastError()};
    }

    // Bind all chunk_ids
    for (size_t i = 0; i < chunk_ids.size(); ++i) {
        sqlite3_bind_text(stmt, static_cast<int>(i + 1), chunk_ids[i].c_str(), -1,
                          SQLITE_TRANSIENT);
    }

    std::map<std::string, VectorRecord> results;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        VectorRecord record;
        record.chunk_id = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
        record.document_hash = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
        record.content = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 2));
        record.model_id = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 3));

        const char* metadata_json = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 4));
        if (metadata_json && strlen(metadata_json) > 2) {
            record.metadata["raw_json"] = metadata_json;
        }

        results[record.chunk_id] = std::move(record);
    }

    sqlite3_finalize(stmt);
    return results;
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
                      "JOIN doc_embeddings e ON e.rowid = m.embedding_rowid "
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
    const char* sql = "SELECT COUNT(*) FROM doc_metadata m "
                      "JOIN doc_embeddings e ON e.rowid = m.embedding_rowid "
                      "WHERE m.document_hash = ?";

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
    const char* sql = "SELECT COUNT(*) FROM doc_metadata WHERE embedding_rowid IS NOT NULL";

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
    const char* count_sql = "SELECT COUNT(*) FROM doc_metadata WHERE embedding_rowid IS NOT NULL";
    sqlite3_stmt* count_stmt;

    if (sqlite3_prepare_v2(db_, count_sql, -1, &count_stmt, nullptr) == SQLITE_OK) {
        if (sqlite3_step(count_stmt) == SQLITE_ROW) {
            stats.total_vectors = static_cast<size_t>(sqlite3_column_int64(count_stmt, 0));
        }
        sqlite3_finalize(count_stmt);
    }

    // Get unique documents count (from metadata table which has document_hash)
    const char* sql = "SELECT COUNT(DISTINCT m.document_hash) FROM doc_metadata m "
                      "JOIN doc_embeddings e ON e.rowid = m.embedding_rowid";
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
#if SQLITE_VEC_STATIC
    // Statically linked: directly initialize the extension
    spdlog::debug("Initializing sqlite-vec extension (static)...");
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
    spdlog::info("sqlite-vec extension initialized successfully (static)");
#else
    // Not statically linked. Best-effort: try dynamic loading if a module path
    // is provided via env var; otherwise, return a clear error so callers can
    // degrade gracefully.
    spdlog::debug("sqlite-vec static not present; attempting dynamic load if configured");
    const char* mod_path = std::getenv("YAMS_SQLITE_VEC_MODULE");
    if (mod_path && *mod_path) {
        sqlite3_enable_load_extension(db_, 1);
        char* err = nullptr;
        int rc = sqlite3_load_extension(db_, mod_path, nullptr, &err);
        sqlite3_enable_load_extension(db_, 0);
        if (rc != SQLITE_OK) {
            std::string error = err ? err : "Unknown error";
            if (err)
                sqlite3_free(err);
            return Error{ErrorCode::DatabaseError,
                         std::string("Failed to load sqlite-vec module ") + mod_path + ": " +
                             error};
        }
        spdlog::info("sqlite-vec module loaded: {}", mod_path);
    } else {
        return Error{ErrorCode::NotFound,
                     "sqlite-vec not linked; set YAMS_SQLITE_VEC_MODULE to a loadable module"};
    }
#endif

    // Register vec_f32 utility function (not auto-registered by sqlite3_vec_init)
    // Use flags that include SQLITE_SUBTYPE and SQLITE_RESULT_SUBTYPE for proper blob handling
    int rc_f32 = sqlite3_create_function_v2(
        db_, "vec_f32", 1,
        SQLITE_UTF8 | SQLITE_DETERMINISTIC | SQLITE_SUBTYPE | SQLITE_RESULT_SUBTYPE, nullptr,
        sqlite_vec_cpp::sqlite::vec_f32, nullptr, nullptr, nullptr);
    if (rc_f32 != SQLITE_OK) {
        spdlog::warn("Failed to register vec_f32 function: {}", rc_f32);
    } else {
        spdlog::info("vec_f32 function registered successfully with SUBTYPE flags");
    }

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

Result<void> SqliteVecBackend::ensureVecLoaded() {
    return loadSqliteVecExtension();
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
#ifdef SQLITE_VEC_CPP
    std::span<const float> span_view(vec);
    sqlite_vec_cpp::VectorView<const float> view(span_view);
    return view.to_blob();
#else
    // Fallback to raw memcpy
    std::vector<uint8_t> blob(vec.size() * sizeof(float));
    std::memcpy(blob.data(), vec.data(), blob.size());
    return blob;
#endif
}

std::vector<float> SqliteVecBackend::blobToVector(const void* blob, size_t size) const {
#ifdef SQLITE_VEC_CPP
    size_t num_floats = size / sizeof(float);
    std::vector<float> result(num_floats);
    if (num_floats > 0) {
        std::memcpy(result.data(), blob, size);
    }
    return result;
#else
    // Fallback to raw memcpy
    size_t num_floats = size / sizeof(float);
    std::vector<float> vec(num_floats);
    std::memcpy(vec.data(), blob, size);
    return vec;
#endif
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
