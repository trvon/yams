#include <yams/vector/sqlite_vec_backend.h>
#include <yams/vector/vector_schema_migration.h>

#include <yams/daemon/components/TuneAdvisor.h>

#include <sqlite3.h>
#include <cmath>
#include <chrono>
#include <cstring>
#include <algorithm>
#include <shared_mutex>
#include <span>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include <sqlite-vec-cpp/distances/cosine.hpp>
#include <sqlite-vec-cpp/distances/inner_product.hpp>
#include <sqlite-vec-cpp/index/hnsw.hpp>
#include <sqlite-vec-cpp/index/hnsw_persistence.hpp>
#include <sqlite-vec-cpp/sqlite/registration.hpp>

namespace yams::vector {

// Type aliases for sqlite-vec-cpp
using DistanceMetric = sqlite_vec_cpp::distances::CosineMetric<float>;
using HNSWIndex = sqlite_vec_cpp::index::HNSWIndex<float, DistanceMetric>;

namespace {

// Helper to safely get string from sqlite column (avoids GNU ?: extension)
inline std::string safeColumnText(sqlite3_stmt* stmt, int col) {
    const char* text = reinterpret_cast<const char*>(sqlite3_column_text(stmt, col));
    return text ? text : "";
}

// Check if an embedding is zero-norm (all zeros or negligible magnitude)
// Zero-norm vectors cannot participate in cosine similarity and become dead-ends in HNSW
inline bool isZeroNormEmbedding(const std::vector<float>& embedding) {
    constexpr double kZeroNormThreshold = 1e-10;
    double norm_sq = 0.0;
    for (float val : embedding) {
        norm_sq += static_cast<double>(val) * static_cast<double>(val);
    }
    return norm_sq < kZeroNormThreshold;
}

inline bool isFiniteEmbedding(const std::vector<float>& embedding) {
    for (float val : embedding) {
        if (!std::isfinite(val)) {
            return false;
        }
    }
    return true;
}

inline bool updateOnDuplicateEnabled() {
    return true; // default: update on duplicate for accuracy
}

// ============================================================================
// Libsql-aware database helpers
// ============================================================================

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

// Begin transaction with retry logic and backend-appropriate semantics
inline bool beginTransactionWithRetry(sqlite3* db) {
    auto backoff = std::chrono::milliseconds(kInitialBackoffMs);

    for (int attempt = 0; attempt < kMaxRetries; ++attempt) {
#if YAMS_LIBSQL_BACKEND
        // libsql MVCC: use regular BEGIN (deferred) for better concurrency
        int rc = sqlite3_exec(db, "BEGIN", nullptr, nullptr, nullptr);
#else
        // SQLite: use BEGIN IMMEDIATE to acquire write lock immediately
        int rc = sqlite3_exec(db, "BEGIN IMMEDIATE", nullptr, nullptr, nullptr);
#endif
        if (rc == SQLITE_OK) {
            return true;
        }
        // Check for transient lock errors
        if ((rc == SQLITE_BUSY || rc == SQLITE_LOCKED) && attempt + 1 < kMaxRetries) {
            std::this_thread::sleep_for(backoff);
            backoff *= 2;
            continue;
        }
        spdlog::warn("[VectorDB] beginTransaction failed: {} (attempt {}/{})", sqlite3_errstr(rc),
                     attempt + 1, kMaxRetries);
        break;
    }
    daemon::TuneAdvisor::reportDbLockError(); // Signal contention for adaptive scaling
    return false;
}

// Execute SQL with retry logic for transient lock errors
inline bool execWithRetry(sqlite3* db, const char* sql) {
    auto backoff = std::chrono::milliseconds(kInitialBackoffMs);

    for (int attempt = 0; attempt < kMaxRetries; ++attempt) {
        int rc = sqlite3_exec(db, sql, nullptr, nullptr, nullptr);
        if (rc == SQLITE_OK) {
            return true;
        }
        if ((rc == SQLITE_BUSY || rc == SQLITE_LOCKED) && attempt + 1 < kMaxRetries) {
            std::this_thread::sleep_for(backoff);
            backoff *= 2;
            continue;
        }
        spdlog::warn("[VectorDB] exec '{}' failed: {} (attempt {}/{})", sql, sqlite3_errstr(rc),
                     attempt + 1, kMaxRetries);
        break;
    }
    daemon::TuneAdvisor::reportDbLockError(); // Signal contention for adaptive scaling
    return false;
}

// Step statement with retry logic (for statements expecting SQLITE_DONE)
inline int stepWithRetry(sqlite3_stmt* stmt) {
    auto backoff = std::chrono::milliseconds(kInitialBackoffMs);

    for (int attempt = 0; attempt < kMaxRetries; ++attempt) {
        int rc = sqlite3_step(stmt);
        if (rc == SQLITE_DONE || rc == SQLITE_ROW) {
            return rc;
        }
        if ((rc == SQLITE_BUSY || rc == SQLITE_LOCKED) && attempt + 1 < kMaxRetries) {
            sqlite3_reset(stmt);
            std::this_thread::sleep_for(backoff);
            backoff *= 2;
            continue;
        }
        return rc; // Non-retryable error
    }
    daemon::TuneAdvisor::reportDbLockError(); // Signal contention for adaptive scaling
    return SQLITE_BUSY;                       // Max retries exceeded
}

// SQL statements
constexpr const char* kCreateVectorsTable = R"sql(
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
);
CREATE INDEX IF NOT EXISTS idx_vectors_chunk_id ON vectors(chunk_id);
CREATE INDEX IF NOT EXISTS idx_vectors_document_hash ON vectors(document_hash);
CREATE INDEX IF NOT EXISTS idx_vectors_model ON vectors(model_id, model_version);
CREATE INDEX IF NOT EXISTS idx_vectors_embedding_dim ON vectors(embedding_dim);
)sql";

constexpr const char* kInsertVector = R"sql(
INSERT INTO vectors (
    chunk_id, document_hash, embedding, embedding_dim, content,
    start_offset, end_offset, metadata,
    model_id, model_version, embedding_version, content_hash,
    created_at, embedded_at, is_stale, level,
    source_chunk_ids, parent_document_hash, child_document_hashes
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
)sql";

constexpr const char* kSelectByChunkId = R"sql(
SELECT rowid, chunk_id, document_hash, embedding, embedding_dim, content,
       start_offset, end_offset, metadata,
       model_id, model_version, embedding_version, content_hash,
       created_at, embedded_at, is_stale, level,
       source_chunk_ids, parent_document_hash, child_document_hashes
FROM vectors WHERE chunk_id = ?
)sql";

constexpr const char* kSelectByRowid = R"sql(
SELECT rowid, chunk_id, document_hash, embedding, embedding_dim, content,
       start_offset, end_offset, metadata,
       model_id, model_version, embedding_version, content_hash,
       created_at, embedded_at, is_stale, level,
       source_chunk_ids, parent_document_hash, child_document_hashes
FROM vectors WHERE rowid = ?
)sql";

constexpr const char* kSelectByDocumentHash = R"sql(
SELECT rowid, chunk_id, document_hash, embedding, embedding_dim, content,
       start_offset, end_offset, metadata,
       model_id, model_version, embedding_version, content_hash,
       created_at, embedded_at, is_stale, level,
       source_chunk_ids, parent_document_hash, child_document_hashes
FROM vectors WHERE document_hash = ?
)sql";

constexpr const char* kDeleteByChunkId = "DELETE FROM vectors WHERE chunk_id = ?";
constexpr const char* kDeleteByDocumentHash = "DELETE FROM vectors WHERE document_hash = ?";
constexpr const char* kGetRowidByChunkId = "SELECT rowid FROM vectors WHERE chunk_id = ?";
constexpr const char* kGetRowidsByDocumentHash =
    "SELECT rowid FROM vectors WHERE document_hash = ?";
constexpr const char* kCountVectors = "SELECT COUNT(*) FROM vectors";
constexpr const char* kHasEmbedding = "SELECT 1 FROM vectors WHERE document_hash = ? LIMIT 1";
constexpr const char* kTableExists =
    "SELECT name FROM sqlite_master WHERE type='table' AND name='vectors'";

// ============================================================================
// Entity Vectors Table (for symbols, functions, classes, etc.)
// ============================================================================
// NOTE: This table schema exists for future semantic symbol search use cases
// (e.g., "find functions similar to X"). Currently NOT populated during ingestion.
// The Knowledge Graph (KG) + FTS5 symbol index provides precise structural navigation
// (call graphs, inheritance, includes) which is preferred for code navigation.
// Embeddings would add noise where exact matches and graph traversal suffice.
// The CRUD operations below are implemented and tested, ready for when a concrete
// semantic search use case emerges.
// ============================================================================

constexpr const char* kCreateEntityVectorsTable = R"sql(
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
);
CREATE INDEX IF NOT EXISTS idx_entity_vectors_node_key ON entity_vectors(node_key);
CREATE INDEX IF NOT EXISTS idx_entity_vectors_type ON entity_vectors(embedding_type);
CREATE INDEX IF NOT EXISTS idx_entity_vectors_document ON entity_vectors(document_hash);
CREATE INDEX IF NOT EXISTS idx_entity_vectors_node_type ON entity_vectors(node_type);
)sql";

constexpr const char* kInsertEntityVector = R"sql(
INSERT OR REPLACE INTO entity_vectors (
    node_key, embedding_type, embedding, content,
    model_id, model_version, embedded_at, is_stale,
    node_type, qualified_name, file_path, document_hash
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
)sql";

constexpr const char* kSelectEntityByNodeKey = R"sql(
SELECT rowid, node_key, embedding_type, embedding, content,
       model_id, model_version, embedded_at, is_stale,
       node_type, qualified_name, file_path, document_hash
FROM entity_vectors WHERE node_key = ?
)sql";

constexpr const char* kSelectEntityByDocument = R"sql(
SELECT rowid, node_key, embedding_type, embedding, content,
       model_id, model_version, embedded_at, is_stale,
       node_type, qualified_name, file_path, document_hash
FROM entity_vectors WHERE document_hash = ?
)sql";

constexpr const char* kDeleteEntityByNodeKey = "DELETE FROM entity_vectors WHERE node_key = ?";
constexpr const char* kDeleteEntityByDocument =
    "DELETE FROM entity_vectors WHERE document_hash = ?";
constexpr const char* kCountEntityVectors = "SELECT COUNT(*) FROM entity_vectors";
constexpr const char* kHasEntityEmbedding =
    "SELECT 1 FROM entity_vectors WHERE node_key = ? LIMIT 1";
constexpr const char* kMarkEntityStale =
    "UPDATE entity_vectors SET is_stale = 1 WHERE node_key = ?";

// Helper: serialize time_point to Unix timestamp
int64_t toUnixTimestamp(const std::chrono::system_clock::time_point& tp) {
    return std::chrono::duration_cast<std::chrono::seconds>(tp.time_since_epoch()).count();
}

// Helper: deserialize Unix timestamp to time_point
std::chrono::system_clock::time_point fromUnixTimestamp(int64_t ts) {
    return std::chrono::system_clock::time_point(std::chrono::seconds(ts));
}

// Helper: serialize metadata map to JSON
std::string serializeMetadata(const std::map<std::string, std::string>& meta) {
    if (meta.empty())
        return "{}";
    return nlohmann::json(meta).dump();
}

// Helper: deserialize JSON to metadata map
std::map<std::string, std::string> deserializeMetadata(const std::string& json_str) {
    if (json_str.empty() || json_str == "null")
        return {};
    try {
        return nlohmann::json::parse(json_str).get<std::map<std::string, std::string>>();
    } catch (...) {
        return {};
    }
}

// Helper: serialize string vector to JSON
std::string serializeStringVector(const std::vector<std::string>& vec) {
    if (vec.empty())
        return "[]";
    return nlohmann::json(vec).dump();
}

// Helper: deserialize JSON to string vector
std::vector<std::string> deserializeStringVector(const std::string& json_str) {
    if (json_str.empty() || json_str == "null")
        return {};
    try {
        return nlohmann::json::parse(json_str).get<std::vector<std::string>>();
    } catch (...) {
        return {};
    }
}

} // namespace

// ============================================================================
// Implementation class
// ============================================================================

class SqliteVecBackend::Impl {
public:
    explicit Impl(const Config& config) : config_(config) {}

    ~Impl() { close(); }

    Result<void> initialize(const std::string& db_path) {
        std::unique_lock lock(mutex_);

        if (db_) {
            return Error{ErrorCode::InvalidState, "Already initialized"};
        }

        int rc = sqlite3_open(db_path.c_str(), &db_);
        if (rc != SQLITE_OK) {
            std::string err = db_ ? sqlite3_errmsg(db_) : "Unknown error";
            if (db_) {
                sqlite3_close(db_);
                db_ = nullptr;
            }
            return Error{ErrorCode::DatabaseError, "Failed to open database: " + err};
        }

        // Enable WAL mode for better concurrency
        sqlite3_exec(db_, "PRAGMA journal_mode=WAL", nullptr, nullptr, nullptr);
        sqlite3_exec(db_, "PRAGMA synchronous=NORMAL", nullptr, nullptr, nullptr);
        sqlite3_exec(db_, "PRAGMA cache_size=-64000", nullptr, nullptr, nullptr); // 64MB cache

        // Register all sqlite-vec functions: vec0 module (for V1 migration), distance functions
        // (l2, l1, cosine, hamming), utility functions (vec_length, vec_type, vec_f32, etc.),
        // and enhanced functions (vec_dot, vec_magnitude, vec_scale, vec_mean)
        auto func_result = sqlite_vec_cpp::sqlite::register_all_functions(db_);
        if (!func_result) {
            spdlog::warn("[VectorInit] Failed to register sqlite-vec functions: {}",
                         func_result.error().message);
            // Continue anyway - migration will handle this gracefully
        } else {
            spdlog::info(
                "[VectorInit] sqlite-vec functions registered (vec0, distances, enhanced)");
        }

        // Check for V1 schema and migrate if needed
        auto schema_version = VectorSchemaMigration::detectVersion(db_);
        if (schema_version == VectorSchemaMigration::SchemaVersion::V1) {
            spdlog::info("Detected V1 vector schema, migrating to V2.1...");
            auto migrate_result = VectorSchemaMigration::migrateV1ToV2(db_, config_.embedding_dim);
            if (!migrate_result) {
                spdlog::error("V1 to V2 migration failed: {}", migrate_result.error().message);
                sqlite3_close(db_);
                db_ = nullptr;
                return Error{ErrorCode::DatabaseError,
                             "Schema migration failed: " + migrate_result.error().message};
            }
            spdlog::info("V1 to V2.1 migration completed successfully");
        } else if (schema_version == VectorSchemaMigration::SchemaVersion::V2) {
            // Upgrade V2 to V2.1 (add embedding_dim column)
            spdlog::info("Detected V2 vector schema, upgrading to V2.1...");
            auto migrate_result = VectorSchemaMigration::migrateV2ToV2_1(db_);
            if (!migrate_result) {
                spdlog::error("V2 to V2.1 migration failed: {}", migrate_result.error().message);
                sqlite3_close(db_);
                db_ = nullptr;
                return Error{ErrorCode::DatabaseError,
                             "Schema migration failed: " + migrate_result.error().message};
            }
            spdlog::info("V2 to V2.1 migration completed successfully");
        }

        db_path_ = db_path;
        initialized_ = true;

        // If tables already exist, prepare statements for immediate use
        // (inline check to avoid lock contention - we already hold the lock)
        {
            sqlite3_stmt* check_stmt = nullptr;
            int rc = sqlite3_prepare_v2(db_, kTableExists, -1, &check_stmt, nullptr);
            if (rc == SQLITE_OK) {
                rc = sqlite3_step(check_stmt);
                bool exists = (rc == SQLITE_ROW);
                sqlite3_finalize(check_stmt);
                if (exists) {
                    prepareStatements();
                }
            }
        }

        return Result<void>{};
    }

    void close() {
        std::unique_lock lock(mutex_);

        // Save all dirty HNSW indices
        if (db_) {
            saveAllHnswUnlocked();
        }

        // Finalize prepared statements
        finalizeStatements();

        if (db_) {
            sqlite3_close(db_);
            db_ = nullptr;
        }

        hnsw_indices_.clear();
        hnsw_dirty_.clear();
        initialized_ = false;
        hnsw_loaded_ = false;
    }

    bool isInitialized() const {
        std::shared_lock lock(mutex_);
        return initialized_;
    }

    Result<void> createTables(size_t embedding_dim) {
        std::unique_lock lock(mutex_);

        if (!db_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        config_.embedding_dim = embedding_dim;

        char* err_msg = nullptr;
        int rc = sqlite3_exec(db_, kCreateVectorsTable, nullptr, nullptr, &err_msg);
        if (rc != SQLITE_OK) {
            std::string err = err_msg ? err_msg : "Unknown error";
            sqlite3_free(err_msg);
            return Error{ErrorCode::DatabaseError, "Failed to create tables: " + err};
        }

        // Create entity_vectors table for symbol/entity embeddings
        rc = sqlite3_exec(db_, kCreateEntityVectorsTable, nullptr, nullptr, &err_msg);
        if (rc != SQLITE_OK) {
            std::string err = err_msg ? err_msg : "Unknown error";
            sqlite3_free(err_msg);
            return Error{ErrorCode::DatabaseError, "Failed to create entity_vectors table: " + err};
        }

        // Create HNSW shadow tables
        rc = sqlite_vec_cpp::index::create_hnsw_shadow_tables(db_, "main", "vectors", &err_msg);
        if (rc != SQLITE_OK) {
            std::string err = err_msg ? err_msg : "Unknown error";
            sqlite3_free(err_msg);
            return Error{ErrorCode::DatabaseError, "Failed to create HNSW tables: " + err};
        }

        // Prepare statements
        prepareStatements();

        return Result<void>{};
    }

    bool tablesExist() const {
        std::shared_lock lock(mutex_);

        if (!db_)
            return false;

        sqlite3_stmt* stmt = nullptr;
        int rc = sqlite3_prepare_v2(db_, kTableExists, -1, &stmt, nullptr);
        if (rc != SQLITE_OK) {
            return false;
        }

        rc = sqlite3_step(stmt);
        bool exists = (rc == SQLITE_ROW);
        sqlite3_finalize(stmt);

        return exists;
    }

    Result<void> insertVector(const VectorRecord& record) {
        std::unique_lock lock(mutex_);

        if (!db_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        auto rowid_result = insertVectorUnlocked(record);
        if (!rowid_result) {
            return rowid_result.error();
        }

        int64_t rowid = rowid_result.value();

        // Skip HNSW insertion for zero-norm vectors (they become dead-ends in the graph)
        if (isZeroNormEmbedding(record.embedding)) {
            spdlog::warn("[HNSW] Skipping zero-norm vector for chunk_id={} (stored in SQLite only)",
                         record.chunk_id);
            return Result<void>{};
        }
        if (!isFiniteEmbedding(record.embedding)) {
            spdlog::warn(
                "[HNSW] Skipping non-finite vector for chunk_id={} (stored in SQLite only)",
                record.chunk_id);
            return Result<void>{};
        }

        // Insert into HNSW (dimension-specific index)
        ensureHnswLoadedUnlocked();
        size_t dim = record.embedding.size();
        if (auto* hnsw = getOrCreateHnswForDim(dim)) {
            std::span<const float> embedding_span(record.embedding.data(), record.embedding.size());
            hnsw->insert(static_cast<size_t>(rowid), embedding_span);
            hnsw_dirty_[dim] = true;
            pending_inserts_++;

            if (pending_inserts_ >= config_.checkpoint_threshold) {
                saveHnswCheckpointUnlocked();
            }
        }

        return Result<void>{};
    }

    Result<void> insertVectorsBatch(const std::vector<VectorRecord>& records) {
        std::unique_lock lock(mutex_);

        if (!db_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        if (records.empty()) {
            return Result<void>{};
        }

        // Begin transaction with libsql-aware retry logic
        if (!beginTransactionWithRetry(db_)) {
            return Error{ErrorCode::DatabaseError, "Failed to begin transaction"};
        }

        std::vector<int64_t> rowids(records.size(), -1);
        std::vector<std::optional<size_t>> old_dims(records.size(), std::nullopt);
        std::vector<std::optional<int64_t>> old_rowids(records.size(), std::nullopt);

        std::vector<size_t> unique_indices;
        unique_indices.reserve(records.size());
        std::unordered_map<std::string, size_t> chunk_to_pos;
        chunk_to_pos.reserve(records.size());

        size_t skipped_duplicates = 0;
        for (size_t i = 0; i < records.size(); ++i) {
            const auto& record = records[i];
            auto it = chunk_to_pos.find(record.chunk_id);
            if (it == chunk_to_pos.end()) {
                chunk_to_pos.emplace(record.chunk_id, unique_indices.size());
                unique_indices.push_back(i);
            } else {
                unique_indices[it->second] = i; // last write wins
                ++skipped_duplicates;
            }
        }

        size_t skipped_existing = 0;
        size_t inserted_count = 0;
        size_t updated_existing = 0;

        for (size_t idx : unique_indices) {
            const auto& record = records[idx];
            auto existing_rowid = getRowidByChunkIdUnlocked(record.chunk_id);
            if (existing_rowid) {
                if (!updateOnDuplicateEnabled()) {
                    ++skipped_existing;
                    continue;
                }

                auto old_record = getVectorByChunkIdUnlocked(record.chunk_id);
                if (old_record) {
                    old_dims[idx] = old_record->embedding.size();
                    old_rowids[idx] = *existing_rowid;
                }

                if (stmt_delete_by_chunk_id_) {
                    sqlite3_reset(stmt_delete_by_chunk_id_);
                    sqlite3_bind_text(stmt_delete_by_chunk_id_, 1, record.chunk_id.c_str(), -1,
                                      SQLITE_TRANSIENT);
                    stepWithRetry(stmt_delete_by_chunk_id_);
                }

                auto rowid_result = insertVectorUnlocked(record);
                if (!rowid_result) {
                    execWithRetry(db_, "ROLLBACK");
                    return rowid_result.error();
                }
                rowids[idx] = rowid_result.value();
                ++updated_existing;
                continue;
            }

            auto rowid_result = insertVectorUnlocked(record);
            if (!rowid_result) {
                execWithRetry(db_, "ROLLBACK");
                return rowid_result.error();
            }
            rowids[idx] = rowid_result.value();
            ++inserted_count;
        }

        // Commit transaction with retry
        if (!execWithRetry(db_, "COMMIT")) {
            return Error{ErrorCode::DatabaseError, "Failed to commit transaction"};
        }

        // Insert into HNSW (dimension-specific indices)
        ensureHnswLoadedUnlocked();

        // Group records by dimension for batch insertion into appropriate indices
        // Skip zero-norm vectors (they become dead-ends in the HNSW graph)
        std::vector<size_t> remove_only_indices;
        remove_only_indices.reserve(records.size());
        std::unordered_map<size_t, std::vector<std::pair<size_t, size_t>>>
            dim_records; // dim -> [(idx, rowid)]
        size_t skipped_zero_norm = 0;
        size_t skipped_invalid = 0;
        for (size_t i = 0; i < records.size(); ++i) {
            if (rowids[i] < 0) {
                continue;
            }
            if (isZeroNormEmbedding(records[i].embedding)) {
                if (old_rowids[i] && old_dims[i]) {
                    remove_only_indices.push_back(i);
                }
                ++skipped_zero_norm;
                continue;
            }
            if (!isFiniteEmbedding(records[i].embedding)) {
                if (old_rowids[i] && old_dims[i]) {
                    remove_only_indices.push_back(i);
                }
                ++skipped_invalid;
                continue;
            }
            size_t dim = records[i].embedding.size();
            dim_records[dim].emplace_back(i, rowids[i]);
        }

        if (skipped_zero_norm > 0) {
            spdlog::warn("[HNSW] Skipped {} zero-norm vectors in batch (stored in SQLite only)",
                         skipped_zero_norm);
        }
        if (skipped_invalid > 0) {
            spdlog::warn("[HNSW] Skipped {} non-finite vectors in batch (stored in SQLite only)",
                         skipped_invalid);
        }

        static std::atomic<uint64_t> batch_counter{0};
        uint64_t bc = batch_counter.fetch_add(1);

        // Threshold for using parallel build (sequential is faster for small batches)
        constexpr size_t kParallelBuildThreshold = 100;

        for (size_t idx : remove_only_indices) {
            if (old_rowids[idx] && old_dims[idx]) {
                if (auto* old_hnsw = getHnswForDim(*old_dims[idx])) {
                    old_hnsw->remove(static_cast<size_t>(*old_rowids[idx]));
                }
            }
        }

        size_t hnsw_adds = 0;

        for (auto& [dim, indices] : dim_records) {
            if (auto* hnsw = getOrCreateHnswForDim(dim)) {
                size_t before_size = hnsw->size();

                if (indices.size() >= kParallelBuildThreshold) {
                    // Use parallel build for large batches
                    std::vector<size_t> ids;
                    std::vector<std::span<const float>> vectors;
                    ids.reserve(indices.size());
                    vectors.reserve(indices.size());

                    for (const auto& [idx, rowid] : indices) {
                        ids.push_back(static_cast<size_t>(rowid));
                        vectors.emplace_back(records[idx].embedding.data(),
                                             records[idx].embedding.size());
                        if (old_rowids[idx] && old_dims[idx]) {
                            if (auto* old_hnsw = getHnswForDim(*old_dims[idx])) {
                                old_hnsw->remove(static_cast<size_t>(*old_rowids[idx]));
                            }
                        }
                        bool same_dim = old_dims[idx] && *old_dims[idx] == dim;
                        if (old_rowids[idx] && same_dim && *old_rowids[idx] == rowid) {
                            *hnsw = hnsw->compact();
                        }
                    }

                    hnsw->build_parallel(std::span{ids}, std::span{vectors}, 0); // 0 = auto threads
                    hnsw_adds += indices.size();

                    spdlog::debug("[HNSW] insertVectorsBatch (parallel): dim={}, added {} records, "
                                  "hnsw_size: {} -> {} (batch #{})",
                                  dim, indices.size(), before_size, hnsw->size(), bc);
                } else {
                    // Sequential insert for small batches (avoids thread overhead)
                    for (const auto& [idx, rowid] : indices) {
                        std::span<const float> embedding_span(records[idx].embedding.data(),
                                                              records[idx].embedding.size());
                        if (old_rowids[idx] && old_dims[idx]) {
                            if (auto* old_hnsw = getHnswForDim(*old_dims[idx])) {
                                old_hnsw->remove(static_cast<size_t>(*old_rowids[idx]));
                            }
                        }
                        bool same_dim = old_dims[idx] && *old_dims[idx] == dim;
                        if (old_rowids[idx] && same_dim && *old_rowids[idx] == rowid) {
                            *hnsw = hnsw->compact();
                        }
                        hnsw->insert(static_cast<size_t>(rowid), embedding_span);
                        ++hnsw_adds;
                    }

                    if (bc % 100 == 0 || indices.size() >= 10) {
                        spdlog::debug(
                            "[HNSW] insertVectorsBatch: dim={}, added {} records, hnsw_size: "
                            "{} -> {} (batch #{})",
                            dim, indices.size(), before_size, hnsw->size(), bc);
                    }
                }
                hnsw_dirty_[dim] = true;
            }
        }

        pending_inserts_ += hnsw_adds;
        if (pending_inserts_ >= config_.checkpoint_threshold) {
            saveHnswCheckpointUnlocked();
        }

        if (skipped_existing > 0 || skipped_duplicates > 0 || updated_existing > 0) {
            spdlog::warn(
                "[HNSW] Batch summary: inserted={}, updated_existing={}, skipped_existing={}, "
                "skipped_duplicates={}",
                inserted_count, updated_existing, skipped_existing, skipped_duplicates);
        }

        return Result<void>{};
    }

    Result<void> updateVector(const std::string& chunk_id, const VectorRecord& record) {
        std::unique_lock lock(mutex_);

        if (!db_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        // Get existing rowid
        auto rowid_opt = getRowidByChunkIdUnlocked(chunk_id);
        if (!rowid_opt) {
            return Error{ErrorCode::NotFound, "Vector not found: " + chunk_id};
        }

        int64_t old_rowid = *rowid_opt;

        // Get old dimension before deleting (need to know which HNSW index to remove from)
        std::optional<size_t> old_dim;
        auto old_record = getVectorByChunkIdUnlocked(chunk_id);
        if (old_record) {
            old_dim = old_record->embedding.size();
        }

        // Delete old record
        if (stmt_delete_by_chunk_id_) {
            sqlite3_reset(stmt_delete_by_chunk_id_);
            sqlite3_bind_text(stmt_delete_by_chunk_id_, 1, chunk_id.c_str(), -1, SQLITE_TRANSIENT);
            stepWithRetry(stmt_delete_by_chunk_id_);
        }

        // Remove from HNSW (soft delete from old dimension's index)
        ensureHnswLoadedUnlocked();
        if (old_dim) {
            if (auto* old_hnsw = getHnswForDim(*old_dim)) {
                old_hnsw->remove(static_cast<size_t>(old_rowid));
            }
        }

        // Insert new record
        auto rowid_result = insertVectorUnlocked(record);
        if (!rowid_result) {
            return rowid_result.error();
        }

        int64_t new_rowid = rowid_result.value();
        size_t new_dim = record.embedding.size();

        // Skip HNSW insertion for zero-norm vectors
        if (isZeroNormEmbedding(record.embedding)) {
            spdlog::warn("[HNSW] Skipping zero-norm vector update for chunk_id={}", chunk_id);
            return Result<void>{};
        }
        if (!isFiniteEmbedding(record.embedding)) {
            spdlog::warn("[HNSW] Skipping non-finite vector update for chunk_id={}", chunk_id);
            return Result<void>{};
        }

        // Insert into HNSW (new dimension's index)
        if (auto* hnsw = getOrCreateHnswForDim(new_dim)) {
            std::span<const float> embedding_span(record.embedding.data(), record.embedding.size());

            // Handle SQLite rowid reuse: if new_rowid == old_rowid AND same dimension,
            // the node still exists in HNSW (just soft-deleted).
            bool same_dim = old_dim && *old_dim == new_dim;
            if (new_rowid == old_rowid && same_dim) {
                // Compact to fully remove the deleted node, then insert fresh
                *hnsw = hnsw->compact();
                hnsw->insert(static_cast<size_t>(new_rowid), embedding_span);
            } else {
                hnsw->insert(static_cast<size_t>(new_rowid), embedding_span);
            }
            hnsw_dirty_[new_dim] = true;

            // Check if compaction needed (skip if we just compacted)
            if (!(new_rowid == old_rowid && same_dim) &&
                hnsw->needs_compaction(config_.compaction_threshold)) {
                compactHnswUnlocked();
            }
        }

        return Result<void>{};
    }

    Result<void> deleteVector(const std::string& chunk_id) {
        std::unique_lock lock(mutex_);

        if (!db_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        // Get rowid and dimension first
        auto rowid_opt = getRowidByChunkIdUnlocked(chunk_id);
        if (!rowid_opt) {
            return Result<void>{}; // Not found is OK for delete
        }

        int64_t rowid = *rowid_opt;

        // Get dimension before deleting
        std::optional<size_t> dim;
        auto record = getVectorByChunkIdUnlocked(chunk_id);
        if (record) {
            dim = record->embedding.size();
        }

        // Delete from SQLite
        if (stmt_delete_by_chunk_id_) {
            sqlite3_reset(stmt_delete_by_chunk_id_);
            sqlite3_bind_text(stmt_delete_by_chunk_id_, 1, chunk_id.c_str(), -1, SQLITE_TRANSIENT);
            stepWithRetry(stmt_delete_by_chunk_id_);
        }

        // Soft delete from HNSW (dimension-specific index)
        ensureHnswLoadedUnlocked();
        if (dim) {
            if (auto* hnsw = getHnswForDim(*dim)) {
                hnsw->remove(static_cast<size_t>(rowid));
                hnsw_dirty_[*dim] = true;

                if (hnsw->needs_compaction(config_.compaction_threshold)) {
                    compactHnswUnlocked();
                }
            }
        }

        return Result<void>{};
    }

    Result<void> deleteVectorsByDocument(const std::string& document_hash) {
        std::unique_lock lock(mutex_);

        if (!db_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        // Get all rowids and their dimensions for this document
        // We need to query rowid and embedding_dim together
        std::vector<std::pair<int64_t, size_t>> rowid_dims; // (rowid, dimension)
        const char* query_sql = "SELECT rowid, embedding_dim FROM vectors WHERE document_hash = ?";
        sqlite3_stmt* stmt = nullptr;
        if (sqlite3_prepare_v2(db_, query_sql, -1, &stmt, nullptr) == SQLITE_OK) {
            sqlite3_bind_text(stmt, 1, document_hash.c_str(), -1, SQLITE_TRANSIENT);
            while (sqlite3_step(stmt) == SQLITE_ROW) {
                int64_t rowid = sqlite3_column_int64(stmt, 0);
                int64_t dim = sqlite3_column_int64(stmt, 1);
                if (dim > 0) {
                    rowid_dims.emplace_back(rowid, static_cast<size_t>(dim));
                }
            }
            sqlite3_finalize(stmt);
        }

        // Delete from SQLite
        if (stmt_delete_by_doc_) {
            sqlite3_reset(stmt_delete_by_doc_);
            sqlite3_bind_text(stmt_delete_by_doc_, 1, document_hash.c_str(), -1, SQLITE_TRANSIENT);
            stepWithRetry(stmt_delete_by_doc_);
        }

        // Soft delete from HNSW (dimension-specific indices)
        ensureHnswLoadedUnlocked();
        if (!rowid_dims.empty()) {
            std::unordered_set<size_t> affected_dims;
            for (const auto& [rowid, dim] : rowid_dims) {
                if (auto* hnsw = getHnswForDim(dim)) {
                    hnsw->remove(static_cast<size_t>(rowid));
                    hnsw_dirty_[dim] = true;
                    affected_dims.insert(dim);
                }
            }

            // Check compaction for affected indices
            for (size_t dim : affected_dims) {
                if (auto* hnsw = getHnswForDim(dim)) {
                    if (hnsw->needs_compaction(config_.compaction_threshold)) {
                        compactHnswUnlocked();
                        break; // compactHnswUnlocked handles all indices
                    }
                }
            }
        }

        return Result<void>{};
    }

    Result<std::vector<VectorRecord>>
    searchSimilar(const std::vector<float>& query_embedding, size_t k, float similarity_threshold,
                  const std::optional<std::string>& document_hash,
                  const std::unordered_set<std::string>& candidate_hashes,
                  const std::map<std::string, std::string>& metadata_filters) {
        std::shared_lock lock(mutex_);

        if (!db_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        // Ensure HNSW is loaded (may upgrade to unique_lock)
        {
            lock.unlock();
            std::unique_lock write_lock(mutex_);
            ensureHnswLoadedUnlocked();
            write_lock.unlock();
            lock.lock();
        }

        // Route to the correct HNSW index based on query embedding dimension
        size_t query_dim = query_embedding.size();
        HNSWIndex* hnsw = nullptr;

        // Find index for this dimension (const access, no creation)
        auto it = hnsw_indices_.find(query_dim);
        if (it != hnsw_indices_.end()) {
            hnsw = it->second.get();
        }

        if (!hnsw || hnsw->empty()) {
            spdlog::warn(
                "[HNSW] searchSimilar: no index for dim={} (hnsw={}), returning empty results",
                query_dim, hnsw ? "empty" : "null");
            return std::vector<VectorRecord>{};
        }

        // Diagnostic: log HNSW size at search time (first 10 searches, then every 100)
        static std::atomic<uint64_t> search_counter{0};
        uint64_t sc = search_counter.fetch_add(1);
        if (sc < 10 || sc % 100 == 0) {
            spdlog::info("[HNSW] searchSimilar: dim={}, hnsw_size={}, k={}, search #{}", query_dim,
                         hnsw->size(), k, sc);
        }

        // Build filter function for document_hash, candidate_hashes, and metadata
        HNSWIndex::FilterFn filter = nullptr;
        if (document_hash || !candidate_hashes.empty() || !metadata_filters.empty()) {
            filter = [&](size_t node_id) {
                // Look up the record
                auto record_opt = getVectorByRowidUnlocked(static_cast<int64_t>(node_id));
                if (!record_opt)
                    return false;

                const auto& record = *record_opt;

                // Check candidate_hashes filter (tiered search narrowing)
                if (!candidate_hashes.empty() &&
                    candidate_hashes.find(record.document_hash) == candidate_hashes.end()) {
                    return false;
                }

                // Check document_hash filter (single doc)
                if (document_hash && record.document_hash != *document_hash) {
                    return false;
                }

                // Check metadata filters
                for (const auto& [key, value] : metadata_filters) {
                    auto it = record.metadata.find(key);
                    if (it == record.metadata.end() || it->second != value) {
                        return false;
                    }
                }

                return true;
            };
        }

        // Search HNSW
        std::span<const float> query_span(query_embedding.data(), query_embedding.size());

        // Calculate ef_search adaptively based on corpus size for target 95% recall
        // recommended_ef_search() uses hnsw->size() internally for corpus-aware tuning
        size_t ef_search = hnsw->recommended_ef_search(k, 0.95F);
        // Apply user-configured minimum as floor
        ef_search = std::max(ef_search, config_.hnsw_ef_search);
        size_t fetch_k = k;

        if (filter != nullptr) {
            // Estimate filter selectivity based on candidate_hashes
            // If we're filtering to N candidate docs out of M total, selectivity ≈ N/M
            // We need to explore ~k/selectivity nodes on average to find k valid results
            float selectivity = 1.0F;

            if (!candidate_hashes.empty() && hnsw->size() > 0) {
                // Rough estimate: each doc has ~N chunks on average (configurable)
                size_t chunks_per_doc =
                    std::max<size_t>(1, config_.filter_candidate_chunks_per_doc);
                size_t estimated_valid_chunks = candidate_hashes.size() * chunks_per_doc;
                selectivity = std::min(1.0F, static_cast<float>(estimated_valid_chunks) /
                                                 static_cast<float>(hnsw->size()));
            }

            // With metadata filters, assume another 50% reduction
            if (!metadata_filters.empty()) {
                selectivity *= 0.5F;
            }

            // Clamp selectivity to avoid division by zero or excessive exploration
            selectivity = std::max(0.01F, selectivity);

            // Increase ef_search inversely proportional to selectivity
            // With 10% selectivity, we need ~10x more exploration
            size_t selectivity_boost = static_cast<size_t>(1.0F / selectivity);
            ef_search = std::max(ef_search, k * selectivity_boost);

            // Also increase fetch_k to ensure we get enough results post-filter
            fetch_k = std::max(k * 5, static_cast<size_t>(static_cast<float>(k) / selectivity));

            // Cap at reasonable limits to avoid excessive computation
            ef_search = std::min(ef_search, std::max(size_t{500}, hnsw->size()));
            fetch_k = std::min(fetch_k, hnsw->size());

            spdlog::debug(
                "[HNSW] Filtered search: k={}, selectivity={:.2f}, ef_search={}, fetch_k={}", k,
                selectivity, ef_search, fetch_k);
        }

        auto results = hnsw->search_with_filter(query_span, fetch_k, ef_search, filter);

        // Convert to VectorRecords
        std::vector<VectorRecord> records;
        records.reserve(std::min(results.size(), k));

        // Debug: log HNSW search statistics
        size_t filtered_count = 0;
        float min_dist = std::numeric_limits<float>::max();
        float max_dist = std::numeric_limits<float>::lowest();
        float min_sim = std::numeric_limits<float>::max();
        float max_sim = std::numeric_limits<float>::lowest();

        for (const auto& [node_id, distance] : results) {
            min_dist = std::min(min_dist, distance);
            max_dist = std::max(max_dist, distance);

            // Convert distance to similarity (cosine distance = 1 - similarity)
            float similarity = 1.0f - distance;
            min_sim = std::min(min_sim, similarity);
            max_sim = std::max(max_sim, similarity);

            if (records.size() >= k)
                break;

            if (similarity < similarity_threshold) {
                ++filtered_count;
                continue;
            }

            auto record_opt = getVectorByRowidUnlocked(static_cast<int64_t>(node_id));
            if (record_opt) {
                record_opt->relevance_score = similarity;
                records.push_back(std::move(*record_opt));
            }
        }

        // Log search statistics for debugging retrieval quality
        if (!results.empty()) {
            spdlog::info("[HNSW Search] dim={} k={} hnsw_results={} returned={} filtered={} "
                         "dist=[{:.4f},{:.4f}] sim=[{:.4f},{:.4f}] threshold={:.4f}",
                         query_embedding.size(), k, results.size(), records.size(), filtered_count,
                         min_dist, max_dist, min_sim, max_sim, similarity_threshold);
        } else {
            spdlog::info("[HNSW Search] dim={} k={} - no results from HNSW (index may be empty or "
                         "not built)",
                         query_embedding.size(), k);
        }

        return records;
    }

    Result<std::vector<std::vector<VectorRecord>>>
    searchSimilarBatch(const std::vector<std::vector<float>>& query_embeddings, size_t k,
                       float similarity_threshold, size_t num_threads) {
        if (query_embeddings.empty()) {
            return std::vector<std::vector<VectorRecord>>{};
        }

        // Ensure all queries have the same dimension
        size_t query_dim = query_embeddings[0].size();
        for (const auto& q : query_embeddings) {
            if (q.size() != query_dim) {
                return Error{ErrorCode::InvalidArgument,
                             "All query embeddings must have the same dimension"};
            }
        }

        std::shared_lock lock(mutex_);

        if (!db_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        // Ensure HNSW is loaded (may upgrade to unique_lock)
        {
            lock.unlock();
            std::unique_lock write_lock(mutex_);
            ensureHnswLoadedUnlocked();
            write_lock.unlock();
            lock.lock();
        }

        // Find index for this dimension
        auto it = hnsw_indices_.find(query_dim);
        if (it == hnsw_indices_.end() || it->second->empty()) {
            spdlog::warn("[HNSW] searchSimilarBatch: no index for dim={}", query_dim);
            return std::vector<std::vector<VectorRecord>>(query_embeddings.size());
        }

        HNSWIndex* hnsw = it->second.get();

        // Convert to spans for HNSW batch search
        std::vector<std::span<const float>> query_spans;
        query_spans.reserve(query_embeddings.size());
        for (const auto& q : query_embeddings) {
            query_spans.emplace_back(q.data(), q.size());
        }

        // Calculate ef_search adaptively based on corpus size
        size_t ef_search = hnsw->recommended_ef_search(k, 0.95F);
        ef_search = std::max(ef_search, config_.hnsw_ef_search);

        spdlog::info(
            "[HNSW] searchSimilarBatch: {} queries, dim={}, k={}, ef_search={}, threads={}",
            query_embeddings.size(), query_dim, k, ef_search,
            num_threads ? num_threads : std::thread::hardware_concurrency());

        // Execute parallel batch search
        auto batch_results = hnsw->search_batch(
            std::span<const std::span<const float>>(query_spans.data(), query_spans.size()), k,
            ef_search, num_threads);

        // Convert HNSW results to VectorRecords
        std::vector<std::vector<VectorRecord>> results;
        results.reserve(batch_results.size());

        for (const auto& hnsw_results : batch_results) {
            std::vector<VectorRecord> records;
            records.reserve(std::min(hnsw_results.size(), k));

            for (const auto& [node_id, distance] : hnsw_results) {
                if (records.size() >= k)
                    break;

                float similarity = 1.0f - distance;
                if (similarity < similarity_threshold) {
                    continue;
                }

                auto record_opt = getVectorByRowidUnlocked(static_cast<int64_t>(node_id));
                if (record_opt) {
                    record_opt->relevance_score = similarity;
                    records.push_back(std::move(*record_opt));
                }
            }
            results.push_back(std::move(records));
        }

        return results;
    }

    Result<std::optional<VectorRecord>> getVector(const std::string& chunk_id) {
        std::shared_lock lock(mutex_);

        if (!db_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        return getVectorByChunkIdUnlocked(chunk_id);
    }

    Result<std::map<std::string, VectorRecord>>
    getVectorsBatch(const std::vector<std::string>& chunk_ids) {
        std::shared_lock lock(mutex_);

        if (!db_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        std::map<std::string, VectorRecord> results;

        for (const auto& chunk_id : chunk_ids) {
            auto record_opt = getVectorByChunkIdUnlocked(chunk_id);
            if (record_opt) {
                results[chunk_id] = std::move(*record_opt);
            }
        }

        return results;
    }

    Result<std::vector<VectorRecord>> getVectorsByDocument(const std::string& document_hash) {
        std::shared_lock lock(mutex_);

        if (!db_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        std::vector<VectorRecord> results;

        if (stmt_select_by_doc_) {
            sqlite3_reset(stmt_select_by_doc_);
            sqlite3_bind_text(stmt_select_by_doc_, 1, document_hash.c_str(), -1, SQLITE_TRANSIENT);

            while (sqlite3_step(stmt_select_by_doc_) == SQLITE_ROW) {
                results.push_back(recordFromStatement(stmt_select_by_doc_));
            }
        }

        return results;
    }

    Result<bool> hasEmbedding(const std::string& document_hash) {
        std::shared_lock lock(mutex_);

        if (!db_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        // Lazily prepare statements if not yet done (e.g., when opening existing DB)
        if (!stmt_has_embedding_) {
            lock.unlock();
            std::unique_lock write_lock(mutex_);
            if (!stmt_has_embedding_) {
                prepareStatements();
            }
            write_lock.unlock();
            lock.lock();
        }

        if (stmt_has_embedding_) {
            sqlite3_reset(stmt_has_embedding_);
            sqlite3_bind_text(stmt_has_embedding_, 1, document_hash.c_str(), -1, SQLITE_TRANSIENT);

            return sqlite3_step(stmt_has_embedding_) == SQLITE_ROW;
        }

        return false;
    }

    Result<size_t> getVectorCount() {
        std::shared_lock lock(mutex_);

        if (!db_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        if (stmt_count_) {
            sqlite3_reset(stmt_count_);
            if (sqlite3_step(stmt_count_) == SQLITE_ROW) {
                return static_cast<size_t>(sqlite3_column_int64(stmt_count_, 0));
            }
        }

        return size_t{0};
    }

    Result<VectorDatabase::DatabaseStats> getStats() {
        std::shared_lock lock(mutex_);

        if (!db_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        VectorDatabase::DatabaseStats stats;

        // Get vector count
        if (stmt_count_) {
            sqlite3_reset(stmt_count_);
            if (sqlite3_step(stmt_count_) == SQLITE_ROW) {
                stats.total_vectors = static_cast<size_t>(sqlite3_column_int64(stmt_count_, 0));
            }
        }

        // Get unique document count
        sqlite3_stmt* stmt = nullptr;
        const char* sql = "SELECT COUNT(DISTINCT document_hash) FROM vectors";
        if (sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr) == SQLITE_OK) {
            if (sqlite3_step(stmt) == SQLITE_ROW) {
                stats.total_documents = static_cast<size_t>(sqlite3_column_int64(stmt, 0));
            }
            sqlite3_finalize(stmt);
        }

        return stats;
    }

    Result<void> buildIndex() {
        std::unique_lock lock(mutex_);

        if (!db_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        // Rebuild HNSW from scratch
        rebuildHnswUnlocked();

        return Result<void>{};
    }

    Result<void> optimize() {
        std::unique_lock lock(mutex_);

        if (!db_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        // Compact all HNSW indices if needed (lower threshold for explicit optimize)
        for (auto& [dim, hnsw] : hnsw_indices_) {
            if (hnsw && hnsw->needs_compaction(0.1f)) {
                spdlog::info("Compacting HNSW index for dim={} (deleted ratio: {:.2f})", dim,
                             static_cast<float>(hnsw->deleted_count()) /
                                 static_cast<float>(hnsw->size()));
                *hnsw = hnsw->compact();
                hnsw_dirty_[dim] = true;
            }
        }

        // Save any dirty indices
        saveAllHnswUnlocked();

        // Vacuum SQLite
        sqlite3_exec(db_, "VACUUM", nullptr, nullptr, nullptr);

        return Result<void>{};
    }

    Result<void> beginTransaction() {
        std::unique_lock lock(mutex_);

        if (!db_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        if (in_transaction_) {
            return Error{ErrorCode::InvalidState, "Already in transaction"};
        }

        // Use libsql-aware transaction begin with retry
        if (!beginTransactionWithRetry(db_)) {
            return Error{ErrorCode::DatabaseError, "Failed to begin transaction"};
        }

        in_transaction_ = true;
        return Result<void>{};
    }

    Result<void> commitTransaction() {
        std::unique_lock lock(mutex_);

        if (!db_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        if (!in_transaction_) {
            return Error{ErrorCode::InvalidState, "Not in transaction"};
        }

        if (!execWithRetry(db_, "COMMIT")) {
            return Error{ErrorCode::DatabaseError, "Failed to commit transaction"};
        }

        in_transaction_ = false;
        return Result<void>{};
    }

    Result<void> rollbackTransaction() {
        std::unique_lock lock(mutex_);

        if (!db_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        if (!in_transaction_) {
            return Error{ErrorCode::InvalidState, "Not in transaction"};
        }

        if (!execWithRetry(db_, "ROLLBACK")) {
            return Error{ErrorCode::DatabaseError, "Failed to rollback transaction"};
        }

        in_transaction_ = false;
        return Result<void>{};
    }

    // Get stored embedding dimension from database
    // Probes the database to detect dimension from existing vectors
    std::optional<size_t> getStoredEmbeddingDimension() const {
        std::shared_lock lock(mutex_);
        if (!initialized_ || !db_) {
            return std::nullopt;
        }

        // Probe the database for existing vectors - don't trust config value
        // as it may differ from what's actually stored
        // First check if vectors table exists
        const char* check_table =
            "SELECT name FROM sqlite_master WHERE type='table' AND name='vectors'";
        sqlite3_stmt* stmt = nullptr;
        if (sqlite3_prepare_v2(db_, check_table, -1, &stmt, nullptr) != SQLITE_OK) {
            return std::nullopt;
        }
        bool table_exists = (sqlite3_step(stmt) == SQLITE_ROW);
        sqlite3_finalize(stmt);

        if (!table_exists) {
            return std::nullopt;
        }

        // Try to get dimension from the embedding_dim column first (V2.1+ schema)
        const char* probe_dim_col =
            "SELECT embedding_dim FROM vectors WHERE embedding_dim IS NOT NULL LIMIT 1";
        if (sqlite3_prepare_v2(db_, probe_dim_col, -1, &stmt, nullptr) == SQLITE_OK) {
            if (sqlite3_step(stmt) == SQLITE_ROW) {
                int64_t dim = sqlite3_column_int64(stmt, 0);
                sqlite3_finalize(stmt);
                if (dim > 0) {
                    return static_cast<size_t>(dim);
                }
            } else {
                sqlite3_finalize(stmt);
            }
        }

        // Fallback: infer dimension from BLOB size (V2.0 schema without embedding_dim column)
        const char* probe_sql = "SELECT LENGTH(embedding) / 4 FROM vectors LIMIT 1";
        if (sqlite3_prepare_v2(db_, probe_sql, -1, &stmt, nullptr) != SQLITE_OK) {
            return std::nullopt;
        }

        std::optional<size_t> dim;
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            int64_t blob_floats = sqlite3_column_int64(stmt, 0);
            if (blob_floats > 0) {
                dim = static_cast<size_t>(blob_floats);
            }
        }
        sqlite3_finalize(stmt);

        return dim;
    }

    // Drop all vector tables
    Result<void> dropTables() {
        std::unique_lock lock(mutex_);

        if (!db_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        // Clear all HNSW indices
        hnsw_indices_.clear();
        hnsw_dirty_.clear();
        hnsw_loaded_ = false;

        // Finalize all prepared statements
        finalizeStatements();

        // Find and drop all dimension-specific HNSW tables
        // Pattern: vectors_{dim}_hnsw_meta, vectors_{dim}_hnsw_nodes
        std::vector<std::string> tables_to_drop;
        const char* find_tables =
            "SELECT name FROM sqlite_master WHERE type='table' AND "
            "(name LIKE 'vectors_%_hnsw_meta' OR name LIKE 'vectors_%_hnsw_nodes' OR "
            " name = 'vectors_hnsw_meta' OR name = 'vectors_hnsw_nodes')";

        sqlite3_stmt* stmt = nullptr;
        if (sqlite3_prepare_v2(db_, find_tables, -1, &stmt, nullptr) == SQLITE_OK) {
            while (sqlite3_step(stmt) == SQLITE_ROW) {
                const char* table_name =
                    reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
                if (table_name) {
                    tables_to_drop.push_back(std::string("DROP TABLE IF EXISTS ") + table_name);
                }
            }
            sqlite3_finalize(stmt);
        }

        // Drop main vectors table
        tables_to_drop.push_back("DROP TABLE IF EXISTS vectors");

        for (const auto& sql : tables_to_drop) {
            char* err_msg = nullptr;
            int rc = sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, &err_msg);
            if (rc != SQLITE_OK) {
                std::string err = err_msg ? err_msg : "Unknown error";
                sqlite3_free(err_msg);
                return Error{ErrorCode::DatabaseError, "Failed to drop tables: " + err};
            }
        }

        spdlog::info("Dropped vector tables (including {} HNSW tables)", tables_to_drop.size() - 1);
        return Result<void>{};
    }

    // =========================================================================
    // Entity Vector Operations
    // =========================================================================

    Result<void> insertEntityVector(const EntityVectorRecord& record) {
        std::unique_lock lock(mutex_);

        if (!db_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        sqlite3_stmt* stmt = nullptr;
        int rc = sqlite3_prepare_v2(db_, kInsertEntityVector, -1, &stmt, nullptr);
        if (rc != SQLITE_OK) {
            return Error{ErrorCode::DatabaseError,
                         "Failed to prepare entity insert: " + std::string(sqlite3_errmsg(db_))};
        }

        // Bind parameters
        sqlite3_bind_text(stmt, 1, record.node_key.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 2,
                          utils::entityEmbeddingTypeToString(record.embedding_type).c_str(), -1,
                          SQLITE_TRANSIENT);
        sqlite3_bind_blob(stmt, 3, record.embedding.data(),
                          static_cast<int>(record.embedding.size() * sizeof(float)),
                          SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 4, record.content.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 5, record.model_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 6, record.model_version.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(stmt, 7, toUnixTimestamp(record.embedded_at));
        sqlite3_bind_int(stmt, 8, record.is_stale ? 1 : 0);
        sqlite3_bind_text(stmt, 9, record.node_type.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 10, record.qualified_name.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 11, record.file_path.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 12, record.document_hash.c_str(), -1, SQLITE_TRANSIENT);

        rc = stepWithRetry(stmt);
        sqlite3_finalize(stmt);

        if (rc != SQLITE_DONE) {
            return Error{ErrorCode::DatabaseError,
                         "Failed to insert entity vector: " + std::string(sqlite3_errmsg(db_))};
        }

        return Result<void>{};
    }

    Result<void> insertEntityVectorsBatch(const std::vector<EntityVectorRecord>& records) {
        std::unique_lock lock(mutex_);

        if (!db_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        if (records.empty()) {
            return Result<void>{};
        }

        // Begin transaction with libsql-aware retry logic
        if (!beginTransactionWithRetry(db_)) {
            return Error{ErrorCode::DatabaseError, "Failed to begin transaction"};
        }

        sqlite3_stmt* stmt = nullptr;
        int rc = sqlite3_prepare_v2(db_, kInsertEntityVector, -1, &stmt, nullptr);
        if (rc != SQLITE_OK) {
            execWithRetry(db_, "ROLLBACK");
            return Error{ErrorCode::DatabaseError, "Failed to prepare entity insert"};
        }

        for (const auto& record : records) {
            sqlite3_reset(stmt);
            sqlite3_bind_text(stmt, 1, record.node_key.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, 2,
                              utils::entityEmbeddingTypeToString(record.embedding_type).c_str(), -1,
                              SQLITE_TRANSIENT);
            sqlite3_bind_blob(stmt, 3, record.embedding.data(),
                              static_cast<int>(record.embedding.size() * sizeof(float)),
                              SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, 4, record.content.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, 5, record.model_id.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, 6, record.model_version.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int64(stmt, 7, toUnixTimestamp(record.embedded_at));
            sqlite3_bind_int(stmt, 8, record.is_stale ? 1 : 0);
            sqlite3_bind_text(stmt, 9, record.node_type.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, 10, record.qualified_name.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, 11, record.file_path.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, 12, record.document_hash.c_str(), -1, SQLITE_TRANSIENT);

            rc = stepWithRetry(stmt);
            if (rc != SQLITE_DONE) {
                sqlite3_finalize(stmt);
                execWithRetry(db_, "ROLLBACK");
                return Error{ErrorCode::DatabaseError, "Failed to insert entity vector batch"};
            }
        }

        sqlite3_finalize(stmt);
        execWithRetry(db_, "COMMIT");

        return Result<void>{};
    }

    Result<void> deleteEntityVectorsByNode(const std::string& node_key) {
        std::unique_lock lock(mutex_);

        if (!db_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        sqlite3_stmt* stmt = nullptr;
        int rc = sqlite3_prepare_v2(db_, kDeleteEntityByNodeKey, -1, &stmt, nullptr);
        if (rc != SQLITE_OK) {
            return Error{ErrorCode::DatabaseError, "Failed to prepare delete statement"};
        }

        sqlite3_bind_text(stmt, 1, node_key.c_str(), -1, SQLITE_TRANSIENT);
        rc = stepWithRetry(stmt);
        sqlite3_finalize(stmt);

        if (rc != SQLITE_DONE) {
            return Error{ErrorCode::DatabaseError, "Failed to delete entity vectors"};
        }

        return Result<void>{};
    }

    Result<void> deleteEntityVectorsByDocument(const std::string& document_hash) {
        std::unique_lock lock(mutex_);

        if (!db_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        sqlite3_stmt* stmt = nullptr;
        int rc = sqlite3_prepare_v2(db_, kDeleteEntityByDocument, -1, &stmt, nullptr);
        if (rc != SQLITE_OK) {
            return Error{ErrorCode::DatabaseError, "Failed to prepare delete statement"};
        }

        sqlite3_bind_text(stmt, 1, document_hash.c_str(), -1, SQLITE_TRANSIENT);
        rc = stepWithRetry(stmt);
        sqlite3_finalize(stmt);

        if (rc != SQLITE_DONE) {
            return Error{ErrorCode::DatabaseError, "Failed to delete entity vectors by document"};
        }

        return Result<void>{};
    }

    std::vector<EntityVectorRecord> getEntityVectorsByNode(const std::string& node_key) {
        std::shared_lock lock(mutex_);
        std::vector<EntityVectorRecord> results;

        if (!db_) {
            return results;
        }

        sqlite3_stmt* stmt = nullptr;
        int rc = sqlite3_prepare_v2(db_, kSelectEntityByNodeKey, -1, &stmt, nullptr);
        if (rc != SQLITE_OK) {
            return results;
        }

        sqlite3_bind_text(stmt, 1, node_key.c_str(), -1, SQLITE_TRANSIENT);

        while (sqlite3_step(stmt) == SQLITE_ROW) {
            results.push_back(parseEntityVectorRow(stmt));
        }

        sqlite3_finalize(stmt);
        return results;
    }

    std::vector<EntityVectorRecord> getEntityVectorsByDocument(const std::string& document_hash) {
        std::shared_lock lock(mutex_);
        std::vector<EntityVectorRecord> results;

        if (!db_) {
            return results;
        }

        sqlite3_stmt* stmt = nullptr;
        int rc = sqlite3_prepare_v2(db_, kSelectEntityByDocument, -1, &stmt, nullptr);
        if (rc != SQLITE_OK) {
            return results;
        }

        sqlite3_bind_text(stmt, 1, document_hash.c_str(), -1, SQLITE_TRANSIENT);

        while (sqlite3_step(stmt) == SQLITE_ROW) {
            results.push_back(parseEntityVectorRow(stmt));
        }

        sqlite3_finalize(stmt);
        return results;
    }

    std::vector<EntityVectorRecord> searchEntities(const std::vector<float>& query_embedding,
                                                   const EntitySearchParams& params) {
        std::shared_lock lock(mutex_);
        std::vector<EntityVectorRecord> results;

        if (!db_ || query_embedding.empty()) {
            return results;
        }

        // Build query with optional filters
        std::string sql = R"sql(
            SELECT rowid, node_key, embedding_type, embedding, content,
                   model_id, model_version, embedded_at, is_stale,
                   node_type, qualified_name, file_path, document_hash
            FROM entity_vectors WHERE 1=1
        )sql";

        if (params.embedding_type) {
            sql += " AND embedding_type = ?";
        }
        if (params.node_type) {
            sql += " AND node_type = ?";
        }
        if (params.document_hash) {
            sql += " AND document_hash = ?";
        }

        sqlite3_stmt* stmt = nullptr;
        int rc = sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt, nullptr);
        if (rc != SQLITE_OK) {
            return results;
        }

        // Bind filter parameters
        int bind_idx = 1;
        if (params.embedding_type) {
            sqlite3_bind_text(stmt, bind_idx++,
                              utils::entityEmbeddingTypeToString(*params.embedding_type).c_str(),
                              -1, SQLITE_TRANSIENT);
        }
        if (params.node_type) {
            sqlite3_bind_text(stmt, bind_idx++, params.node_type->c_str(), -1, SQLITE_TRANSIENT);
        }
        if (params.document_hash) {
            sqlite3_bind_text(stmt, bind_idx++, params.document_hash->c_str(), -1,
                              SQLITE_TRANSIENT);
        }

        // Collect all rows and compute similarities
        std::vector<std::pair<float, EntityVectorRecord>> scored_results;

        while (sqlite3_step(stmt) == SQLITE_ROW) {
            auto record = parseEntityVectorRow(stmt);

            // Compute cosine similarity
            if (record.embedding.size() == query_embedding.size()) {
                float similarity = static_cast<float>(
                    VectorDatabase::computeCosineSimilarity(query_embedding, record.embedding));

                if (similarity >= params.similarity_threshold) {
                    record.relevance_score = similarity;
                    scored_results.emplace_back(similarity, std::move(record));
                }
            }
        }

        sqlite3_finalize(stmt);

        // Sort by similarity descending
        std::sort(scored_results.begin(), scored_results.end(),
                  [](const auto& a, const auto& b) { return a.first > b.first; });

        // Take top k
        size_t count = std::min(params.k, scored_results.size());
        results.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            if (!params.include_embeddings) {
                scored_results[i].second.embedding.clear();
            }
            results.push_back(std::move(scored_results[i].second));
        }

        return results;
    }

    bool hasEntityEmbedding(const std::string& node_key) {
        std::shared_lock lock(mutex_);

        if (!db_) {
            return false;
        }

        sqlite3_stmt* stmt = nullptr;
        int rc = sqlite3_prepare_v2(db_, kHasEntityEmbedding, -1, &stmt, nullptr);
        if (rc != SQLITE_OK) {
            return false;
        }

        sqlite3_bind_text(stmt, 1, node_key.c_str(), -1, SQLITE_TRANSIENT);
        bool exists = (sqlite3_step(stmt) == SQLITE_ROW);
        sqlite3_finalize(stmt);

        return exists;
    }

    size_t getEntityVectorCount() {
        std::shared_lock lock(mutex_);

        if (!db_) {
            return 0;
        }

        sqlite3_stmt* stmt = nullptr;
        int rc = sqlite3_prepare_v2(db_, kCountEntityVectors, -1, &stmt, nullptr);
        if (rc != SQLITE_OK) {
            return 0;
        }

        size_t count = 0;
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            count = static_cast<size_t>(sqlite3_column_int64(stmt, 0));
        }
        sqlite3_finalize(stmt);

        return count;
    }

    Result<void> markEntityAsStale(const std::string& node_key) {
        std::unique_lock lock(mutex_);

        if (!db_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        sqlite3_stmt* stmt = nullptr;
        int rc = sqlite3_prepare_v2(db_, kMarkEntityStale, -1, &stmt, nullptr);
        if (rc != SQLITE_OK) {
            return Error{ErrorCode::DatabaseError, "Failed to prepare mark stale statement"};
        }

        sqlite3_bind_text(stmt, 1, node_key.c_str(), -1, SQLITE_TRANSIENT);
        rc = stepWithRetry(stmt);
        sqlite3_finalize(stmt);

        if (rc != SQLITE_DONE) {
            return Error{ErrorCode::DatabaseError, "Failed to mark entity as stale"};
        }

        return Result<void>{};
    }

private:
    // Helper to parse entity vector row
    EntityVectorRecord parseEntityVectorRow(sqlite3_stmt* stmt) const {
        EntityVectorRecord record;

        record.rowid = sqlite3_column_int64(stmt, 0);
        record.node_key = safeColumnText(stmt, 1);
        record.embedding_type = utils::stringToEntityEmbeddingType(safeColumnText(stmt, 2));

        // Parse embedding blob
        const void* blob = sqlite3_column_blob(stmt, 3);
        int blob_size = sqlite3_column_bytes(stmt, 3);
        if (blob && blob_size > 0) {
            size_t num_floats = blob_size / sizeof(float);
            record.embedding.resize(num_floats);
            std::memcpy(record.embedding.data(), blob, blob_size);
        }

        record.content = safeColumnText(stmt, 4);
        record.model_id = safeColumnText(stmt, 5);
        record.model_version = safeColumnText(stmt, 6);
        record.embedded_at = fromUnixTimestamp(sqlite3_column_int64(stmt, 7));
        record.is_stale = sqlite3_column_int(stmt, 8) != 0;
        record.node_type = safeColumnText(stmt, 9);
        record.qualified_name = safeColumnText(stmt, 10);
        record.file_path = safeColumnText(stmt, 11);
        record.document_hash = safeColumnText(stmt, 12);

        return record;
    }

private:
    // Insert vector and return rowid (assumes lock held)
    Result<int64_t> insertVectorUnlocked(const VectorRecord& record) {
        if (!stmt_insert_) {
            prepareStatements();
        }

        if (!stmt_insert_) {
            return Error{ErrorCode::DatabaseError, "Insert statement not prepared"};
        }

        sqlite3_reset(stmt_insert_);

        // Bind parameters
        sqlite3_bind_text(stmt_insert_, 1, record.chunk_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt_insert_, 2, record.document_hash.c_str(), -1, SQLITE_TRANSIENT);

        // Embedding as blob
        sqlite3_bind_blob(stmt_insert_, 3, record.embedding.data(),
                          record.embedding.size() * sizeof(float), SQLITE_TRANSIENT);

        // Embedding dimension
        sqlite3_bind_int64(stmt_insert_, 4, static_cast<int64_t>(record.embedding.size()));

        sqlite3_bind_text(stmt_insert_, 5, record.content.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(stmt_insert_, 6, static_cast<int64_t>(record.start_offset));
        sqlite3_bind_int64(stmt_insert_, 7, static_cast<int64_t>(record.end_offset));

        // Metadata as JSON
        std::string metadata_json = serializeMetadata(record.metadata);
        sqlite3_bind_text(stmt_insert_, 8, metadata_json.c_str(), -1, SQLITE_TRANSIENT);

        sqlite3_bind_text(stmt_insert_, 9, record.model_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt_insert_, 10, record.model_version.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(stmt_insert_, 11, static_cast<int>(record.embedding_version));
        sqlite3_bind_text(stmt_insert_, 12, record.content_hash_at_embedding.c_str(), -1,
                          SQLITE_TRANSIENT);

        sqlite3_bind_int64(stmt_insert_, 13, toUnixTimestamp(record.created_at));
        sqlite3_bind_int64(stmt_insert_, 14, toUnixTimestamp(record.embedded_at));
        sqlite3_bind_int(stmt_insert_, 15, record.is_stale ? 1 : 0);
        sqlite3_bind_int(stmt_insert_, 16, static_cast<int>(record.level));

        // JSON arrays
        std::string source_chunks_json = serializeStringVector(record.source_chunk_ids);
        sqlite3_bind_text(stmt_insert_, 17, source_chunks_json.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt_insert_, 18, record.parent_document_hash.c_str(), -1,
                          SQLITE_TRANSIENT);
        std::string child_hashes_json = serializeStringVector(record.child_document_hashes);
        sqlite3_bind_text(stmt_insert_, 19, child_hashes_json.c_str(), -1, SQLITE_TRANSIENT);

        int rc = stepWithRetry(stmt_insert_);
        if (rc != SQLITE_DONE) {
            std::string err = sqlite3_errmsg(db_);
            return Error{ErrorCode::DatabaseError, "Failed to insert vector: " + err};
        }

        return sqlite3_last_insert_rowid(db_);
    }

    // Get rowid by chunk_id (assumes lock held)
    std::optional<int64_t> getRowidByChunkIdUnlocked(const std::string& chunk_id) {
        if (!stmt_get_rowid_) {
            return std::nullopt;
        }

        sqlite3_reset(stmt_get_rowid_);
        sqlite3_bind_text(stmt_get_rowid_, 1, chunk_id.c_str(), -1, SQLITE_TRANSIENT);

        if (sqlite3_step(stmt_get_rowid_) == SQLITE_ROW) {
            return sqlite3_column_int64(stmt_get_rowid_, 0);
        }

        return std::nullopt;
    }

    // Get vector by chunk_id (assumes lock held)
    std::optional<VectorRecord> getVectorByChunkIdUnlocked(const std::string& chunk_id) {
        if (!stmt_select_by_chunk_id_) {
            return std::nullopt;
        }

        sqlite3_reset(stmt_select_by_chunk_id_);
        sqlite3_bind_text(stmt_select_by_chunk_id_, 1, chunk_id.c_str(), -1, SQLITE_TRANSIENT);

        if (sqlite3_step(stmt_select_by_chunk_id_) == SQLITE_ROW) {
            return recordFromStatement(stmt_select_by_chunk_id_);
        }

        return std::nullopt;
    }

    // Get vector by rowid (assumes lock held)
    std::optional<VectorRecord> getVectorByRowidUnlocked(int64_t rowid) const {
        if (!stmt_select_by_rowid_) {
            return std::nullopt;
        }

        sqlite3_reset(stmt_select_by_rowid_);
        sqlite3_bind_int64(stmt_select_by_rowid_, 1, rowid);

        if (sqlite3_step(stmt_select_by_rowid_) == SQLITE_ROW) {
            return recordFromStatement(stmt_select_by_rowid_);
        }

        return std::nullopt;
    }

    // Build VectorRecord from statement (current row)
    VectorRecord recordFromStatement(sqlite3_stmt* stmt) const {
        VectorRecord record;

        // Column indices match SELECT statement
        // 0: rowid, 1: chunk_id, 2: document_hash, 3: embedding, 4: embedding_dim, 5: content, ...

        record.chunk_id = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
        record.document_hash = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 2));

        // Embedding blob
        const void* blob = sqlite3_column_blob(stmt, 3);
        int blob_size = sqlite3_column_bytes(stmt, 3);
        size_t num_floats = blob_size / sizeof(float);
        record.embedding.resize(num_floats);
        std::memcpy(record.embedding.data(), blob, blob_size);

        // Column 4 is embedding_dim - we skip it since embedding.size() provides this
        // (useful for queries but not needed when loading record)

        const char* content = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 5));
        record.content = content ? content : "";

        record.start_offset = static_cast<size_t>(sqlite3_column_int64(stmt, 6));
        record.end_offset = static_cast<size_t>(sqlite3_column_int64(stmt, 7));

        const char* metadata_json = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 8));
        record.metadata = deserializeMetadata(metadata_json ? metadata_json : "");

        const char* model_id = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 9));
        record.model_id = model_id ? model_id : "";

        const char* model_version = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 10));
        record.model_version = model_version ? model_version : "";

        record.embedding_version = static_cast<uint32_t>(sqlite3_column_int(stmt, 11));

        const char* content_hash = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 12));
        record.content_hash_at_embedding = content_hash ? content_hash : "";

        record.created_at = fromUnixTimestamp(sqlite3_column_int64(stmt, 13));
        record.embedded_at = fromUnixTimestamp(sqlite3_column_int64(stmt, 14));
        record.is_stale = sqlite3_column_int(stmt, 15) != 0;
        record.level = static_cast<EmbeddingLevel>(sqlite3_column_int(stmt, 16));

        const char* source_chunks = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 17));
        record.source_chunk_ids = deserializeStringVector(source_chunks ? source_chunks : "");

        const char* parent_hash = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 18));
        record.parent_document_hash = parent_hash ? parent_hash : "";

        const char* child_hashes = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 19));
        record.child_document_hashes = deserializeStringVector(child_hashes ? child_hashes : "");

        return record;
    }

    // Prepare all statements
    void prepareStatements() {
        sqlite3_prepare_v2(db_, kInsertVector, -1, &stmt_insert_, nullptr);
        sqlite3_prepare_v2(db_, kSelectByChunkId, -1, &stmt_select_by_chunk_id_, nullptr);
        sqlite3_prepare_v2(db_, kSelectByRowid, -1, &stmt_select_by_rowid_, nullptr);
        sqlite3_prepare_v2(db_, kSelectByDocumentHash, -1, &stmt_select_by_doc_, nullptr);
        sqlite3_prepare_v2(db_, kDeleteByChunkId, -1, &stmt_delete_by_chunk_id_, nullptr);
        sqlite3_prepare_v2(db_, kDeleteByDocumentHash, -1, &stmt_delete_by_doc_, nullptr);
        sqlite3_prepare_v2(db_, kGetRowidByChunkId, -1, &stmt_get_rowid_, nullptr);
        sqlite3_prepare_v2(db_, kGetRowidsByDocumentHash, -1, &stmt_get_rowids_by_doc_, nullptr);
        sqlite3_prepare_v2(db_, kCountVectors, -1, &stmt_count_, nullptr);
        sqlite3_prepare_v2(db_, kHasEmbedding, -1, &stmt_has_embedding_, nullptr);
    }

    // Finalize all statements
    void finalizeStatements() {
        if (stmt_insert_)
            sqlite3_finalize(stmt_insert_);
        if (stmt_select_by_chunk_id_)
            sqlite3_finalize(stmt_select_by_chunk_id_);
        if (stmt_select_by_rowid_)
            sqlite3_finalize(stmt_select_by_rowid_);
        if (stmt_select_by_doc_)
            sqlite3_finalize(stmt_select_by_doc_);
        if (stmt_delete_by_chunk_id_)
            sqlite3_finalize(stmt_delete_by_chunk_id_);
        if (stmt_delete_by_doc_)
            sqlite3_finalize(stmt_delete_by_doc_);
        if (stmt_get_rowid_)
            sqlite3_finalize(stmt_get_rowid_);
        if (stmt_get_rowids_by_doc_)
            sqlite3_finalize(stmt_get_rowids_by_doc_);
        if (stmt_count_)
            sqlite3_finalize(stmt_count_);
        if (stmt_has_embedding_)
            sqlite3_finalize(stmt_has_embedding_);

        stmt_insert_ = nullptr;
        stmt_select_by_chunk_id_ = nullptr;
        stmt_select_by_rowid_ = nullptr;
        stmt_select_by_doc_ = nullptr;
        stmt_delete_by_chunk_id_ = nullptr;
        stmt_delete_by_doc_ = nullptr;
        stmt_get_rowid_ = nullptr;
        stmt_get_rowids_by_doc_ = nullptr;
        stmt_count_ = nullptr;
        stmt_has_embedding_ = nullptr;
    }

    // Ensure HNSW indices are loaded (lazy loading)
    // Discovers existing dimension-specific tables and loads each index
    // If force_rebuild is true, skips loading from persisted tables and rebuilds from vectors table
    void ensureHnswLoadedUnlocked(bool force_rebuild = false) {
        if (hnsw_loaded_ && !force_rebuild) {
            // Already loaded - log occasionally for diagnostics
            static std::atomic<uint64_t> skip_counter{0};
            uint64_t skc = skip_counter.fetch_add(1);
            if (skc < 5 || skc % 1000 == 0) {
                size_t total_size = 0;
                for (const auto& [dim, idx] : hnsw_indices_) {
                    if (idx)
                        total_size += idx->size();
                }
                spdlog::debug("[HNSW] ensureHnswLoadedUnlocked: already loaded, total_size={}, "
                              "num_indices={}, backend={:p} (skip #{})",
                              total_size, hnsw_indices_.size(), static_cast<void*>(this), skc);
            }
            return;
        }

        spdlog::info("[HNSW] ensureHnswLoadedUnlocked: first load, backend={:p}{}",
                     static_cast<void*>(this), force_rebuild ? " (force_rebuild)" : "");

        // If force_rebuild, skip table discovery and go straight to building from vectors
        if (force_rebuild) {
            spdlog::info("[HNSW] Force rebuilding from vectors table, skipping persisted indices");
        }

        // stmt is used both in table discovery and in build-from-vectors
        sqlite3_stmt* stmt = nullptr;

        // Discover existing dimension-specific HNSW tables (skip if force_rebuild)
        if (!force_rebuild) {
            // Pattern: vectors_{dim}_hnsw_meta (e.g., vectors_384_hnsw_meta, vectors_768_hnsw_meta)
            std::vector<size_t> existing_dims;

            // First check for legacy single-dimension tables (vectors_hnsw_meta)
            const char* check_legacy = "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND "
                                       "name='vectors_hnsw_meta'";
            if (sqlite3_prepare_v2(db_, check_legacy, -1, &stmt, nullptr) == SQLITE_OK) {
                if (sqlite3_step(stmt) == SQLITE_ROW && sqlite3_column_int64(stmt, 0) > 0) {
                    // Legacy table exists - need to migrate or load with detected dimension
                    spdlog::info(
                        "[HNSW] Found legacy vectors_hnsw_meta table, will probe for dimension");
                    // The dimension will be determined when we load from vectors table
                }
                sqlite3_finalize(stmt);
            }

            // Find all dimension-specific tables
            const char* find_dims = "SELECT name FROM sqlite_master WHERE type='table' AND name "
                                    "LIKE 'vectors_%_hnsw_meta'";
            if (sqlite3_prepare_v2(db_, find_dims, -1, &stmt, nullptr) == SQLITE_OK) {
                while (sqlite3_step(stmt) == SQLITE_ROW) {
                    const char* table_name =
                        reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
                    if (table_name) {
                        // Parse dimension from table name: vectors_384_hnsw_meta -> 384
                        std::string name(table_name);
                        auto start = name.find('_') + 1;
                        auto end = name.find('_', start);
                        if (start != std::string::npos && end != std::string::npos) {
                            try {
                                size_t dim = std::stoull(name.substr(start, end - start));
                                if (dim > 0) {
                                    existing_dims.push_back(dim);
                                    spdlog::debug("[HNSW] Found existing HNSW table for dim={}",
                                                  dim);
                                }
                            } catch (...) {
                                // Ignore malformed table names
                            }
                        }
                    }
                }
                sqlite3_finalize(stmt);
            }

            // Load existing indices
            for (size_t dim : existing_dims) {
                try {
                    std::string table_prefix = hnswTablePrefix(dim);
                    char* err = nullptr;
                    auto loaded_hnsw =
                        sqlite_vec_cpp::index::load_hnsw_index<float, DistanceMetric>(
                            db_, "main", table_prefix.c_str(), &err);
                    if (err) {
                        spdlog::warn("[HNSW] Failed to load index for dim={}: {}", dim, err);
                        sqlite3_free(err);
                    } else {
                        hnsw_indices_[dim] = std::make_unique<HNSWIndex>(std::move(loaded_hnsw));
                        hnsw_dirty_[dim] = false;
                        spdlog::info("[HNSW] Loaded index for dim={} with {} vectors", dim,
                                     hnsw_indices_[dim]->size());
                    }
                } catch (const std::exception& e) {
                    spdlog::warn("[HNSW] Exception loading index for dim={}: {}", dim, e.what());
                }
            }
        }

        // If no existing indices found (or force_rebuild), build from database grouped by dimension
        if (hnsw_indices_.empty()) {
            spdlog::info("[HNSW] No existing indices found, building from vectors table");

            // Query vectors grouped by dimension
            const char* select_by_dim =
                "SELECT rowid, embedding, embedding_dim FROM vectors ORDER BY embedding_dim";
            if (sqlite3_prepare_v2(db_, select_by_dim, -1, &stmt, nullptr) == SQLITE_OK) {
                while (sqlite3_step(stmt) == SQLITE_ROW) {
                    int64_t rowid = sqlite3_column_int64(stmt, 0);
                    const void* blob = sqlite3_column_blob(stmt, 1);
                    int blob_size = sqlite3_column_bytes(stmt, 1);
                    size_t num_floats = blob_size / sizeof(float);

                    // Get dimension from column or infer from blob
                    size_t dim = sqlite3_column_int64(stmt, 2);
                    if (dim == 0) {
                        dim = num_floats; // Fallback to blob size
                    } else if (dim != num_floats) {
                        spdlog::warn(
                            "[HNSW] embedding_dim mismatch for rowid={} (col_dim={} blob_dim={}) "
                            "- using blob_dim",
                            rowid, dim, num_floats);
                        dim = num_floats;
                    }

                    if (dim == 0 || num_floats == 0)
                        continue;

                    std::vector<float> embedding(num_floats);
                    std::memcpy(embedding.data(), blob, blob_size);

                    // Skip zero-norm vectors during rebuild (they become dead-ends in HNSW)
                    if (isZeroNormEmbedding(embedding)) {
                        continue;
                    }
                    if (!isFiniteEmbedding(embedding)) {
                        continue;
                    }

                    std::span<const float> embedding_span(embedding.data(), embedding.size());

                    // Get or create index for this dimension
                    auto* hnsw = getOrCreateHnswForDim(dim);
                    if (hnsw) {
                        hnsw->insert(static_cast<size_t>(rowid), embedding_span);
                    }
                }
                sqlite3_finalize(stmt);

                // Log what we built and save immediately
                for (const auto& [dim, idx] : hnsw_indices_) {
                    spdlog::info("[HNSW] Built index for dim={} with {} vectors", dim, idx->size());
                    hnsw_dirty_[dim] = true;
                }
                // Save indices immediately after building to ensure persistence
                saveAllHnswUnlocked();
            }
        }

        hnsw_loaded_ = true;
    }

    // Save all HNSW indices to disk
    void saveAllHnswUnlocked() {
        if (!db_)
            return;

        for (auto& [dim, hnsw] : hnsw_indices_) {
            if (!hnsw || !hnsw_dirty_[dim])
                continue;

            std::string table_prefix = hnswTablePrefix(dim);
            char* err = nullptr;
            int rc = sqlite_vec_cpp::index::save_hnsw_index<float, DistanceMetric>(
                db_, "main", table_prefix.c_str(), *hnsw, &err);
            if (rc != SQLITE_OK) {
                if (err) {
                    spdlog::error("[HNSW] Failed to save index for dim={}: {}", dim, err);
                    sqlite3_free(err);
                }
            } else {
                hnsw_dirty_[dim] = false;
                spdlog::debug("[HNSW] Saved index for dim={} with {} vectors", dim, hnsw->size());
            }
        }
        pending_inserts_ = 0;
    }

    // Save HNSW checkpoint (metadata + recent nodes) for all indices
    void saveHnswCheckpointUnlocked() {
        if (!db_)
            return;

        for (auto& [dim, hnsw] : hnsw_indices_) {
            if (!hnsw || !hnsw_dirty_[dim])
                continue;

            std::string table_prefix = hnswTablePrefix(dim);
            char* err = nullptr;
            int rc = sqlite_vec_cpp::index::save_hnsw_checkpoint<float, DistanceMetric>(
                db_, "main", table_prefix.c_str(), *hnsw, &err);
            if (rc != SQLITE_OK && err) {
                spdlog::warn("[HNSW] Failed to save checkpoint for dim={}: {}", dim, err);
                sqlite3_free(err);
            }
        }
        pending_inserts_ = 0;
    }

    // Rebuild all HNSW indices from scratch
    void rebuildHnswUnlocked() {
        hnsw_indices_.clear();
        hnsw_dirty_.clear();
        hnsw_loaded_ = false;

        // Drop persisted HNSW tables (best effort, may fail but force_rebuild handles it)
        dropHnswTablesUnlocked();

        // Force rebuild from vectors table, bypassing any stale persisted tables
        ensureHnswLoadedUnlocked(/*force_rebuild=*/true);
        saveAllHnswUnlocked();
    }

    // Drop all HNSW tables (but not the vectors table)
    void dropHnswTablesUnlocked() {
        std::vector<std::string> tables_to_drop;
        const char* find_tables =
            "SELECT name FROM sqlite_master WHERE type='table' AND "
            "(name LIKE 'vectors_%_hnsw_meta' OR name LIKE 'vectors_%_hnsw_nodes' OR "
            " name = 'vectors_hnsw_meta' OR name = 'vectors_hnsw_nodes')";

        sqlite3_stmt* stmt = nullptr;
        if (sqlite3_prepare_v2(db_, find_tables, -1, &stmt, nullptr) == SQLITE_OK) {
            while (sqlite3_step(stmt) == SQLITE_ROW) {
                const char* table_name =
                    reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
                if (table_name) {
                    tables_to_drop.push_back(std::string("DROP TABLE IF EXISTS ") + table_name);
                }
            }
            sqlite3_finalize(stmt);
        }

        for (const auto& sql : tables_to_drop) {
            char* err_msg = nullptr;
            int rc = sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, &err_msg);
            if (rc != SQLITE_OK) {
                spdlog::warn("[HNSW] Failed to drop table: {} (sql: {})",
                             err_msg ? err_msg : "unknown error", sql);
            }
            if (err_msg) {
                sqlite3_free(err_msg);
            }
        }

        if (!tables_to_drop.empty()) {
            spdlog::debug("[HNSW] Dropped {} persisted HNSW tables for rebuild",
                          tables_to_drop.size());
        }
    }

    // Compact all HNSW indices (remove soft-deleted nodes)
    void compactHnswUnlocked() {
        for (auto& [dim, hnsw] : hnsw_indices_) {
            if (!hnsw || !hnsw->needs_compaction(config_.compaction_threshold))
                continue;

            spdlog::info("[HNSW] Compacting index for dim={} (deleted ratio: {:.2f})", dim,
                         static_cast<float>(hnsw->deleted_count()) /
                             static_cast<float>(hnsw->size()));

            *hnsw = hnsw->compact();
            hnsw_dirty_[dim] = true;
        }

        saveAllHnswUnlocked();
    }

    Config config_;
    std::string db_path_;
    sqlite3* db_ = nullptr;
    bool initialized_ = false;
    bool in_transaction_ = false;

    // HNSW indices - one per embedding dimension for multi-model support
    // Key: embedding dimension (e.g., 384, 768)
    // Value: HNSW index for vectors of that dimension
    std::unordered_map<size_t, std::unique_ptr<HNSWIndex>> hnsw_indices_;
    std::unordered_map<size_t, bool> hnsw_dirty_; // Track dirty state per dimension
    bool hnsw_loaded_ = false;
    size_t pending_inserts_ = 0;

    // Helper to get table prefix for dimension-specific HNSW tables
    std::string hnswTablePrefix(size_t dim) const { return "vectors_" + std::to_string(dim); }

    // Get or create HNSW index for a specific dimension
    HNSWIndex* getOrCreateHnswForDim(size_t dim) {
        auto it = hnsw_indices_.find(dim);
        if (it != hnsw_indices_.end()) {
            return it->second.get();
        }

        // Get corpus size for optimal HNSW configuration
        // Query directly since we may not hold the lock in the same way as getVectorCount
        size_t corpus_size = 0;
        if (stmt_count_) {
            sqlite3_reset(stmt_count_);
            if (sqlite3_step(stmt_count_) == SQLITE_ROW) {
                corpus_size = static_cast<size_t>(sqlite3_column_int64(stmt_count_, 0));
            }
        }

        // Use corpus-aware config from sqlite-vec-cpp for optimal M values
        // Higher dims (384+) get M=24, M_max=48, M_max_0=96 for better recall
        HNSWIndex::Config config = HNSWIndex::Config::for_corpus(corpus_size, dim);

        // Preserve user-configured ef_construction if higher than library default
        config.ef_construction = std::max(config.ef_construction, config_.hnsw_ef_construction);

        spdlog::info("[HNSW] Creating index for dim={} corpus_size={}: M={} M_max={} M_max_0={} "
                     "ef_construction={}",
                     dim, corpus_size, config.M, config.M_max, config.M_max_0,
                     config.ef_construction);

        auto idx = std::make_unique<HNSWIndex>(config);
        auto* ptr = idx.get();
        hnsw_indices_[dim] = std::move(idx);
        hnsw_dirty_[dim] = true;

        // Create dimension-specific shadow tables
        std::string table_prefix = hnswTablePrefix(dim);
        char* err_msg = nullptr;
        int rc = sqlite_vec_cpp::index::create_hnsw_shadow_tables(db_, "main", table_prefix.c_str(),
                                                                  &err_msg);
        if (rc != SQLITE_OK) {
            spdlog::warn("[HNSW] Failed to create shadow tables for dim {}: {}", dim,
                         err_msg ? err_msg : "unknown");
            sqlite3_free(err_msg);
        } else {
            spdlog::info("[HNSW] Created shadow tables for dim {} ({})", dim, table_prefix);
        }

        return ptr;
    }

    // Get existing HNSW index for a dimension (nullptr if not exists)
    HNSWIndex* getHnswForDim(size_t dim) {
        auto it = hnsw_indices_.find(dim);
        return it != hnsw_indices_.end() ? it->second.get() : nullptr;
    }

    // Check if any HNSW index has data
    bool anyHnswHasData() const {
        for (const auto& [dim, idx] : hnsw_indices_) {
            if (idx && !idx->empty())
                return true;
        }
        return false;
    }

    // Prepared statements
    sqlite3_stmt* stmt_insert_ = nullptr;
    sqlite3_stmt* stmt_select_by_chunk_id_ = nullptr;
    sqlite3_stmt* stmt_select_by_rowid_ = nullptr;
    sqlite3_stmt* stmt_select_by_doc_ = nullptr;
    sqlite3_stmt* stmt_delete_by_chunk_id_ = nullptr;
    sqlite3_stmt* stmt_delete_by_doc_ = nullptr;
    sqlite3_stmt* stmt_get_rowid_ = nullptr;
    sqlite3_stmt* stmt_get_rowids_by_doc_ = nullptr;
    sqlite3_stmt* stmt_count_ = nullptr;
    sqlite3_stmt* stmt_has_embedding_ = nullptr;

    // Thread safety
    mutable std::shared_mutex mutex_;
};

// ============================================================================
// SqliteVecBackend public interface
// ============================================================================

SqliteVecBackend::SqliteVecBackend() : impl_(std::make_unique<Impl>(Config{})) {}

SqliteVecBackend::SqliteVecBackend(const Config& config) : impl_(std::make_unique<Impl>(config)) {}

SqliteVecBackend::~SqliteVecBackend() = default;

SqliteVecBackend::SqliteVecBackend(SqliteVecBackend&&) noexcept = default;
SqliteVecBackend& SqliteVecBackend::operator=(SqliteVecBackend&&) noexcept = default;

Result<void> SqliteVecBackend::initialize(const std::string& db_path) {
    return impl_->initialize(db_path);
}

void SqliteVecBackend::close() {
    impl_->close();
}

bool SqliteVecBackend::isInitialized() const {
    return impl_->isInitialized();
}

Result<void> SqliteVecBackend::createTables(size_t embedding_dim) {
    return impl_->createTables(embedding_dim);
}

bool SqliteVecBackend::tablesExist() const {
    return impl_->tablesExist();
}

Result<void> SqliteVecBackend::insertVector(const VectorRecord& record) {
    return impl_->insertVector(record);
}

Result<void> SqliteVecBackend::insertVectorsBatch(const std::vector<VectorRecord>& records) {
    return impl_->insertVectorsBatch(records);
}

Result<void> SqliteVecBackend::updateVector(const std::string& chunk_id,
                                            const VectorRecord& record) {
    return impl_->updateVector(chunk_id, record);
}

Result<void> SqliteVecBackend::deleteVector(const std::string& chunk_id) {
    return impl_->deleteVector(chunk_id);
}

Result<void> SqliteVecBackend::deleteVectorsByDocument(const std::string& document_hash) {
    return impl_->deleteVectorsByDocument(document_hash);
}

Result<std::vector<VectorRecord>>
SqliteVecBackend::searchSimilar(const std::vector<float>& query_embedding, size_t k,
                                float similarity_threshold,
                                const std::optional<std::string>& document_hash,
                                const std::unordered_set<std::string>& candidate_hashes,
                                const std::map<std::string, std::string>& metadata_filters) {
    return impl_->searchSimilar(query_embedding, k, similarity_threshold, document_hash,
                                candidate_hashes, metadata_filters);
}

Result<std::vector<std::vector<VectorRecord>>>
SqliteVecBackend::searchSimilarBatch(const std::vector<std::vector<float>>& query_embeddings,
                                     size_t k, float similarity_threshold, size_t num_threads) {
    return impl_->searchSimilarBatch(query_embeddings, k, similarity_threshold, num_threads);
}

Result<std::optional<VectorRecord>> SqliteVecBackend::getVector(const std::string& chunk_id) {
    return impl_->getVector(chunk_id);
}

Result<std::map<std::string, VectorRecord>>
SqliteVecBackend::getVectorsBatch(const std::vector<std::string>& chunk_ids) {
    return impl_->getVectorsBatch(chunk_ids);
}

Result<std::vector<VectorRecord>>
SqliteVecBackend::getVectorsByDocument(const std::string& document_hash) {
    return impl_->getVectorsByDocument(document_hash);
}

Result<bool> SqliteVecBackend::hasEmbedding(const std::string& document_hash) {
    return impl_->hasEmbedding(document_hash);
}

Result<size_t> SqliteVecBackend::getVectorCount() {
    return impl_->getVectorCount();
}

Result<VectorDatabase::DatabaseStats> SqliteVecBackend::getStats() {
    return impl_->getStats();
}

Result<void> SqliteVecBackend::buildIndex() {
    return impl_->buildIndex();
}

Result<void> SqliteVecBackend::optimize() {
    return impl_->optimize();
}

Result<void> SqliteVecBackend::beginTransaction() {
    return impl_->beginTransaction();
}

Result<void> SqliteVecBackend::commitTransaction() {
    return impl_->commitTransaction();
}

Result<void> SqliteVecBackend::rollbackTransaction() {
    return impl_->rollbackTransaction();
}

// ============================================================================
// Backend-specific helpers (compatibility with external code)
// ============================================================================

std::optional<size_t> SqliteVecBackend::getStoredEmbeddingDimension() const {
    return impl_->getStoredEmbeddingDimension();
}

Result<void> SqliteVecBackend::dropTables() {
    return impl_->dropTables();
}

Result<void> SqliteVecBackend::ensureEmbeddingRowIdColumn() {
    // No-op for unified schema - no separate rowid sync needed
    return Result<void>{};
}

Result<SqliteVecBackend::OrphanCleanupStats> SqliteVecBackend::cleanupOrphanRows() {
    // No-op for unified schema - no orphans possible
    return OrphanCleanupStats{};
}

Result<void> SqliteVecBackend::ensureVecLoaded() {
    // sqlite-vec-cpp doesn't require extension loading - it's statically linked
    return Result<void>{};
}

// ============================================================================
// Entity Vector Operations
// ============================================================================

Result<void> SqliteVecBackend::insertEntityVector(const EntityVectorRecord& record) {
    return impl_->insertEntityVector(record);
}

Result<void>
SqliteVecBackend::insertEntityVectorsBatch(const std::vector<EntityVectorRecord>& records) {
    return impl_->insertEntityVectorsBatch(records);
}

Result<void> SqliteVecBackend::deleteEntityVectorsByNode(const std::string& node_key) {
    return impl_->deleteEntityVectorsByNode(node_key);
}

Result<void> SqliteVecBackend::deleteEntityVectorsByDocument(const std::string& document_hash) {
    return impl_->deleteEntityVectorsByDocument(document_hash);
}

std::vector<EntityVectorRecord>
SqliteVecBackend::searchEntities(const std::vector<float>& query_embedding,
                                 const EntitySearchParams& params) {
    return impl_->searchEntities(query_embedding, params);
}

std::vector<EntityVectorRecord>
SqliteVecBackend::getEntityVectorsByNode(const std::string& node_key) {
    return impl_->getEntityVectorsByNode(node_key);
}

std::vector<EntityVectorRecord>
SqliteVecBackend::getEntityVectorsByDocument(const std::string& document_hash) {
    return impl_->getEntityVectorsByDocument(document_hash);
}

bool SqliteVecBackend::hasEntityEmbedding(const std::string& node_key) {
    return impl_->hasEntityEmbedding(node_key);
}

size_t SqliteVecBackend::getEntityVectorCount() {
    return impl_->getEntityVectorCount();
}

Result<void> SqliteVecBackend::markEntityAsStale(const std::string& node_key) {
    return impl_->markEntityAsStale(node_key);
}

} // namespace yams::vector
