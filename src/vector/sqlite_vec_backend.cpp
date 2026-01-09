#include <yams/vector/sqlite_vec_backend.h>
#include <yams/vector/vector_schema_migration.h>

#include <sqlite3.h>
#include <chrono>
#include <cstring>
#include <shared_mutex>
#include <span>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include <sqlite-vec-cpp/distances/cosine.hpp>
#include <sqlite-vec-cpp/index/hnsw.hpp>
#include <sqlite-vec-cpp/index/hnsw_persistence.hpp>
#include <sqlite-vec-cpp/sqlite/vec0_module.hpp>

namespace yams::vector {

// Type aliases for sqlite-vec-cpp
using CosineMetric = sqlite_vec_cpp::distances::CosineMetric<float>;
using HNSWIndex = sqlite_vec_cpp::index::HNSWIndex<float, CosineMetric>;

namespace {

// SQL statements
constexpr const char* kCreateVectorsTable = R"sql(
CREATE TABLE IF NOT EXISTS vectors (
    rowid INTEGER PRIMARY KEY,
    chunk_id TEXT UNIQUE NOT NULL,
    document_hash TEXT NOT NULL,
    embedding BLOB NOT NULL,
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
)sql";

constexpr const char* kInsertVector = R"sql(
INSERT INTO vectors (
    chunk_id, document_hash, embedding, content,
    start_offset, end_offset, metadata,
    model_id, model_version, embedding_version, content_hash,
    created_at, embedded_at, is_stale, level,
    source_chunk_ids, parent_document_hash, child_document_hashes
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
)sql";

constexpr const char* kSelectByChunkId = R"sql(
SELECT rowid, chunk_id, document_hash, embedding, content,
       start_offset, end_offset, metadata,
       model_id, model_version, embedding_version, content_hash,
       created_at, embedded_at, is_stale, level,
       source_chunk_ids, parent_document_hash, child_document_hashes
FROM vectors WHERE chunk_id = ?
)sql";

constexpr const char* kSelectByRowid = R"sql(
SELECT rowid, chunk_id, document_hash, embedding, content,
       start_offset, end_offset, metadata,
       model_id, model_version, embedding_version, content_hash,
       created_at, embedded_at, is_stale, level,
       source_chunk_ids, parent_document_hash, child_document_hashes
FROM vectors WHERE rowid = ?
)sql";

constexpr const char* kSelectByDocumentHash = R"sql(
SELECT rowid, chunk_id, document_hash, embedding, content,
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

        // Register vec0 module to enable reading V1 schema data during migration
        // This allows us to read embeddings from the old vec0 virtual table
        auto vec0_result = sqlite_vec_cpp::sqlite::register_vec0_module(db_);
        if (!vec0_result) {
            spdlog::warn("[VectorInit] Failed to register vec0 module: {}",
                         vec0_result.error().message);
            // Continue anyway - migration will handle this gracefully
        } else {
            spdlog::info("[VectorInit] vec0 module registered for V1 migration");
        }

        // Check for V1 schema and migrate if needed
        auto schema_version = VectorSchemaMigration::detectVersion(db_);
        if (schema_version == VectorSchemaMigration::SchemaVersion::V1) {
            spdlog::info("Detected V1 vector schema, migrating to V2...");
            auto migrate_result = VectorSchemaMigration::migrateV1ToV2(db_, config_.embedding_dim);
            if (!migrate_result) {
                spdlog::error("V1 to V2 migration failed: {}", migrate_result.error().message);
                sqlite3_close(db_);
                db_ = nullptr;
                return Error{ErrorCode::DatabaseError,
                             "Schema migration failed: " + migrate_result.error().message};
            }
            spdlog::info("V1 to V2 migration completed successfully");
        }

        db_path_ = db_path;
        initialized_ = true;

        return Result<void>{};
    }

    void close() {
        std::unique_lock lock(mutex_);

        // Save HNSW if dirty
        if (hnsw_dirty_ && hnsw_ && db_) {
            saveHnswUnlocked();
        }

        // Finalize prepared statements
        finalizeStatements();

        if (db_) {
            sqlite3_close(db_);
            db_ = nullptr;
        }

        hnsw_.reset();
        initialized_ = false;
        hnsw_loaded_ = false;
        hnsw_dirty_ = false;
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

        // Insert into HNSW
        ensureHnswLoadedUnlocked();
        if (hnsw_) {
            std::span<const float> embedding_span(record.embedding.data(), record.embedding.size());
            hnsw_->insert(static_cast<size_t>(rowid), embedding_span);
            hnsw_dirty_ = true;
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

        // Begin transaction
        int rc = sqlite3_exec(db_, "BEGIN TRANSACTION", nullptr, nullptr, nullptr);
        if (rc != SQLITE_OK) {
            return Error{ErrorCode::DatabaseError, "Failed to begin transaction"};
        }

        std::vector<int64_t> rowids;
        rowids.reserve(records.size());

        for (const auto& record : records) {
            auto rowid_result = insertVectorUnlocked(record);
            if (!rowid_result) {
                sqlite3_exec(db_, "ROLLBACK", nullptr, nullptr, nullptr);
                return rowid_result.error();
            }
            rowids.push_back(rowid_result.value());
        }

        // Commit transaction
        rc = sqlite3_exec(db_, "COMMIT", nullptr, nullptr, nullptr);
        if (rc != SQLITE_OK) {
            return Error{ErrorCode::DatabaseError, "Failed to commit transaction"};
        }

        // Insert into HNSW
        ensureHnswLoadedUnlocked();
        if (hnsw_) {
            for (size_t i = 0; i < records.size(); ++i) {
                std::span<const float> embedding_span(records[i].embedding.data(),
                                                      records[i].embedding.size());
                hnsw_->insert(static_cast<size_t>(rowids[i]), embedding_span);
            }
            hnsw_dirty_ = true;
            pending_inserts_ += records.size();

            if (pending_inserts_ >= config_.checkpoint_threshold) {
                saveHnswCheckpointUnlocked();
            }
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

        // Delete old record
        if (stmt_delete_by_chunk_id_) {
            sqlite3_reset(stmt_delete_by_chunk_id_);
            sqlite3_bind_text(stmt_delete_by_chunk_id_, 1, chunk_id.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_step(stmt_delete_by_chunk_id_);
        }

        // Remove from HNSW (soft delete)
        ensureHnswLoadedUnlocked();
        if (hnsw_) {
            hnsw_->remove(static_cast<size_t>(old_rowid));
        }

        // Insert new record
        auto rowid_result = insertVectorUnlocked(record);
        if (!rowid_result) {
            return rowid_result.error();
        }

        int64_t new_rowid = rowid_result.value();

        // Insert into HNSW
        if (hnsw_) {
            std::span<const float> embedding_span(record.embedding.data(), record.embedding.size());

            // Handle SQLite rowid reuse: if new_rowid == old_rowid, the node still exists
            // in HNSW (just soft-deleted). We need to restore it and the vector data
            // is already correct since HNSW stores the actual vector.
            if (new_rowid == old_rowid) {
                // Restore the soft-deleted node - the vector data in HNSW is stale,
                // but since HNSW doesn't support in-place update, we need to remove
                // completely and re-insert. Use a workaround: compact to remove,
                // then insert fresh.
                *hnsw_ = hnsw_->compact(); // Remove the deleted node entirely
                hnsw_->insert(static_cast<size_t>(new_rowid), embedding_span);
            } else {
                hnsw_->insert(static_cast<size_t>(new_rowid), embedding_span);
            }
            hnsw_dirty_ = true;

            // Check if compaction needed (skip if we just compacted)
            if (new_rowid != old_rowid && hnsw_->needs_compaction(config_.compaction_threshold)) {
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

        // Get rowid first
        auto rowid_opt = getRowidByChunkIdUnlocked(chunk_id);
        if (!rowid_opt) {
            return Result<void>{}; // Not found is OK for delete
        }

        int64_t rowid = *rowid_opt;

        // Delete from SQLite
        if (stmt_delete_by_chunk_id_) {
            sqlite3_reset(stmt_delete_by_chunk_id_);
            sqlite3_bind_text(stmt_delete_by_chunk_id_, 1, chunk_id.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_step(stmt_delete_by_chunk_id_);
        }

        // Soft delete from HNSW
        ensureHnswLoadedUnlocked();
        if (hnsw_) {
            hnsw_->remove(static_cast<size_t>(rowid));
            hnsw_dirty_ = true;

            if (hnsw_->needs_compaction(config_.compaction_threshold)) {
                compactHnswUnlocked();
            }
        }

        return Result<void>{};
    }

    Result<void> deleteVectorsByDocument(const std::string& document_hash) {
        std::unique_lock lock(mutex_);

        if (!db_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        // Get all rowids for this document
        std::vector<int64_t> rowids;
        if (stmt_get_rowids_by_doc_) {
            sqlite3_reset(stmt_get_rowids_by_doc_);
            sqlite3_bind_text(stmt_get_rowids_by_doc_, 1, document_hash.c_str(), -1,
                              SQLITE_TRANSIENT);

            while (sqlite3_step(stmt_get_rowids_by_doc_) == SQLITE_ROW) {
                rowids.push_back(sqlite3_column_int64(stmt_get_rowids_by_doc_, 0));
            }
        }

        // Delete from SQLite
        if (stmt_delete_by_doc_) {
            sqlite3_reset(stmt_delete_by_doc_);
            sqlite3_bind_text(stmt_delete_by_doc_, 1, document_hash.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_step(stmt_delete_by_doc_);
        }

        // Soft delete from HNSW
        ensureHnswLoadedUnlocked();
        if (hnsw_ && !rowids.empty()) {
            for (int64_t rowid : rowids) {
                hnsw_->remove(static_cast<size_t>(rowid));
            }
            hnsw_dirty_ = true;

            if (hnsw_->needs_compaction(config_.compaction_threshold)) {
                compactHnswUnlocked();
            }
        }

        return Result<void>{};
    }

    Result<std::vector<VectorRecord>>
    searchSimilar(const std::vector<float>& query_embedding, size_t k, float similarity_threshold,
                  const std::optional<std::string>& document_hash,
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

        if (!hnsw_ || hnsw_->empty()) {
            return std::vector<VectorRecord>{};
        }

        // Build filter function for document_hash and metadata
        HNSWIndex::FilterFn filter = nullptr;
        if (document_hash || !metadata_filters.empty()) {
            filter = [&](size_t node_id) {
                // Look up the record
                auto record_opt = getVectorByRowidUnlocked(static_cast<int64_t>(node_id));
                if (!record_opt)
                    return false;

                const auto& record = *record_opt;

                // Check document_hash filter
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
        size_t ef_search = std::max(config_.hnsw_ef_search, k * 2);

        // Over-fetch if filtering to ensure enough results
        size_t fetch_k = (filter != nullptr) ? k * 5 : k;

        auto results = hnsw_->search_with_filter(query_span, fetch_k, ef_search, filter);

        // Convert to VectorRecords
        std::vector<VectorRecord> records;
        records.reserve(std::min(results.size(), k));

        for (const auto& [node_id, distance] : results) {
            if (records.size() >= k)
                break;

            // Convert distance to similarity (cosine distance = 1 - similarity)
            float similarity = 1.0f - distance;

            if (similarity < similarity_threshold)
                continue;

            auto record_opt = getVectorByRowidUnlocked(static_cast<int64_t>(node_id));
            if (record_opt) {
                record_opt->relevance_score = similarity;
                records.push_back(std::move(*record_opt));
            }
        }

        return records;
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

        // Compact HNSW if needed
        if (hnsw_ && hnsw_->needs_compaction(0.1f)) { // Lower threshold for explicit optimize
            compactHnswUnlocked();
        }

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

        int rc = sqlite3_exec(db_, "BEGIN TRANSACTION", nullptr, nullptr, nullptr);
        if (rc != SQLITE_OK) {
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

        int rc = sqlite3_exec(db_, "COMMIT", nullptr, nullptr, nullptr);
        if (rc != SQLITE_OK) {
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

        int rc = sqlite3_exec(db_, "ROLLBACK", nullptr, nullptr, nullptr);
        if (rc != SQLITE_OK) {
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

        // If config already has a dimension set, return it
        if (config_.embedding_dim > 0) {
            return config_.embedding_dim;
        }

        // Otherwise, probe the database for existing vectors
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

        // Get dimension from first vector
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

        // Save and clear HNSW first
        if (hnsw_) {
            hnsw_.reset();
            hnsw_loaded_ = false;
            hnsw_dirty_ = false;
        }

        // Finalize all prepared statements
        finalizeStatements();

        // Drop tables in order (HNSW shadow tables first, then main table)
        const char* drop_statements[] = {"DROP TABLE IF EXISTS vectors_hnsw_meta",
                                         "DROP TABLE IF EXISTS vectors_hnsw_nodes",
                                         "DROP TABLE IF EXISTS vectors"};

        for (const char* sql : drop_statements) {
            char* err_msg = nullptr;
            int rc = sqlite3_exec(db_, sql, nullptr, nullptr, &err_msg);
            if (rc != SQLITE_OK) {
                std::string err = err_msg ? err_msg : "Unknown error";
                sqlite3_free(err_msg);
                return Error{ErrorCode::DatabaseError, "Failed to drop tables: " + err};
            }
        }

        spdlog::info("Dropped vector tables");
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

        rc = sqlite3_step(stmt);
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

        // Begin transaction for batch insert
        sqlite3_exec(db_, "BEGIN TRANSACTION", nullptr, nullptr, nullptr);

        sqlite3_stmt* stmt = nullptr;
        int rc = sqlite3_prepare_v2(db_, kInsertEntityVector, -1, &stmt, nullptr);
        if (rc != SQLITE_OK) {
            sqlite3_exec(db_, "ROLLBACK", nullptr, nullptr, nullptr);
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

            rc = sqlite3_step(stmt);
            if (rc != SQLITE_DONE) {
                sqlite3_finalize(stmt);
                sqlite3_exec(db_, "ROLLBACK", nullptr, nullptr, nullptr);
                return Error{ErrorCode::DatabaseError, "Failed to insert entity vector batch"};
            }
        }

        sqlite3_finalize(stmt);
        sqlite3_exec(db_, "COMMIT", nullptr, nullptr, nullptr);

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
        rc = sqlite3_step(stmt);
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
        rc = sqlite3_step(stmt);
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
        rc = sqlite3_step(stmt);
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
        record.node_key = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1)) ?: "";
        record.embedding_type = utils::stringToEntityEmbeddingType(
            reinterpret_cast<const char*>(sqlite3_column_text(stmt, 2)) ?: "");

        // Parse embedding blob
        const void* blob = sqlite3_column_blob(stmt, 3);
        int blob_size = sqlite3_column_bytes(stmt, 3);
        if (blob && blob_size > 0) {
            size_t num_floats = blob_size / sizeof(float);
            record.embedding.resize(num_floats);
            std::memcpy(record.embedding.data(), blob, blob_size);
        }

        record.content = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 4)) ?: "";
        record.model_id = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 5)) ?: "";
        record.model_version = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 6)) ?: "";
        record.embedded_at = fromUnixTimestamp(sqlite3_column_int64(stmt, 7));
        record.is_stale = sqlite3_column_int(stmt, 8) != 0;
        record.node_type = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 9)) ?: "";
        record.qualified_name = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 10)) ?: "";
        record.file_path = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 11)) ?: "";
        record.document_hash = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 12)) ?: "";

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

        sqlite3_bind_text(stmt_insert_, 4, record.content.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(stmt_insert_, 5, static_cast<int64_t>(record.start_offset));
        sqlite3_bind_int64(stmt_insert_, 6, static_cast<int64_t>(record.end_offset));

        // Metadata as JSON
        std::string metadata_json = serializeMetadata(record.metadata);
        sqlite3_bind_text(stmt_insert_, 7, metadata_json.c_str(), -1, SQLITE_TRANSIENT);

        sqlite3_bind_text(stmt_insert_, 8, record.model_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt_insert_, 9, record.model_version.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(stmt_insert_, 10, static_cast<int>(record.embedding_version));
        sqlite3_bind_text(stmt_insert_, 11, record.content_hash_at_embedding.c_str(), -1,
                          SQLITE_TRANSIENT);

        sqlite3_bind_int64(stmt_insert_, 12, toUnixTimestamp(record.created_at));
        sqlite3_bind_int64(stmt_insert_, 13, toUnixTimestamp(record.embedded_at));
        sqlite3_bind_int(stmt_insert_, 14, record.is_stale ? 1 : 0);
        sqlite3_bind_int(stmt_insert_, 15, static_cast<int>(record.level));

        // JSON arrays
        std::string source_chunks_json = serializeStringVector(record.source_chunk_ids);
        sqlite3_bind_text(stmt_insert_, 16, source_chunks_json.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt_insert_, 17, record.parent_document_hash.c_str(), -1,
                          SQLITE_TRANSIENT);
        std::string child_hashes_json = serializeStringVector(record.child_document_hashes);
        sqlite3_bind_text(stmt_insert_, 18, child_hashes_json.c_str(), -1, SQLITE_TRANSIENT);

        int rc = sqlite3_step(stmt_insert_);
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
        // 0: rowid, 1: chunk_id, 2: document_hash, 3: embedding, ...

        record.chunk_id = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
        record.document_hash = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 2));

        // Embedding blob
        const void* blob = sqlite3_column_blob(stmt, 3);
        int blob_size = sqlite3_column_bytes(stmt, 3);
        size_t num_floats = blob_size / sizeof(float);
        record.embedding.resize(num_floats);
        std::memcpy(record.embedding.data(), blob, blob_size);

        const char* content = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 4));
        record.content = content ? content : "";

        record.start_offset = static_cast<size_t>(sqlite3_column_int64(stmt, 5));
        record.end_offset = static_cast<size_t>(sqlite3_column_int64(stmt, 6));

        const char* metadata_json = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 7));
        record.metadata = deserializeMetadata(metadata_json ? metadata_json : "");

        const char* model_id = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 8));
        record.model_id = model_id ? model_id : "";

        const char* model_version = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 9));
        record.model_version = model_version ? model_version : "";

        record.embedding_version = static_cast<uint32_t>(sqlite3_column_int(stmt, 10));

        const char* content_hash = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 11));
        record.content_hash_at_embedding = content_hash ? content_hash : "";

        record.created_at = fromUnixTimestamp(sqlite3_column_int64(stmt, 12));
        record.embedded_at = fromUnixTimestamp(sqlite3_column_int64(stmt, 13));
        record.is_stale = sqlite3_column_int(stmt, 14) != 0;
        record.level = static_cast<EmbeddingLevel>(sqlite3_column_int(stmt, 15));

        const char* source_chunks = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 16));
        record.source_chunk_ids = deserializeStringVector(source_chunks ? source_chunks : "");

        const char* parent_hash = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 17));
        record.parent_document_hash = parent_hash ? parent_hash : "";

        const char* child_hashes = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 18));
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

    // Ensure HNSW is loaded (lazy loading)
    void ensureHnswLoadedUnlocked() {
        if (hnsw_loaded_)
            return;

        // Check if HNSW tables exist and have data
        sqlite3_stmt* stmt = nullptr;
        const char* check_sql =
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='vectors_hnsw_meta'";
        if (sqlite3_prepare_v2(db_, check_sql, -1, &stmt, nullptr) == SQLITE_OK) {
            if (sqlite3_step(stmt) == SQLITE_ROW && sqlite3_column_int64(stmt, 0) > 0) {
                sqlite3_finalize(stmt);

                // Try to load existing HNSW
                try {
                    char* err = nullptr;
                    hnsw_ = std::make_unique<HNSWIndex>(
                        sqlite_vec_cpp::index::load_hnsw_index<float, CosineMetric>(
                            db_, "main", "vectors", &err));
                    if (err) {
                        sqlite3_free(err);
                        hnsw_.reset();
                    } else {
                        spdlog::info("Loaded HNSW index with {} vectors", hnsw_->size());
                        hnsw_loaded_ = true;
                        return;
                    }
                } catch (const std::exception& e) {
                    spdlog::warn("Failed to load HNSW index: {}. Will rebuild.", e.what());
                }
            } else {
                sqlite3_finalize(stmt);
            }
        }

        // No existing HNSW or load failed - create new
        HNSWIndex::Config config;
        config.M = config_.hnsw_m;
        config.M_max = config_.hnsw_m * 2;
        config.M_max_0 = config_.hnsw_m * 2;
        config.ef_construction = config_.hnsw_ef_construction;

        hnsw_ = std::make_unique<HNSWIndex>(config);

        // Populate from database
        const char* select_all = "SELECT rowid, embedding FROM vectors";
        if (sqlite3_prepare_v2(db_, select_all, -1, &stmt, nullptr) == SQLITE_OK) {
            while (sqlite3_step(stmt) == SQLITE_ROW) {
                int64_t rowid = sqlite3_column_int64(stmt, 0);
                const void* blob = sqlite3_column_blob(stmt, 1);
                int blob_size = sqlite3_column_bytes(stmt, 1);
                size_t num_floats = blob_size / sizeof(float);

                std::vector<float> embedding(num_floats);
                std::memcpy(embedding.data(), blob, blob_size);

                std::span<const float> embedding_span(embedding.data(), embedding.size());
                hnsw_->insert(static_cast<size_t>(rowid), embedding_span);
            }
            sqlite3_finalize(stmt);

            spdlog::info("Built HNSW index with {} vectors", hnsw_->size());
        }

        hnsw_loaded_ = true;
        hnsw_dirty_ = true; // Needs to be saved
    }

    // Save HNSW to disk
    void saveHnswUnlocked() {
        if (!hnsw_ || !db_)
            return;

        char* err = nullptr;
        int rc = sqlite_vec_cpp::index::save_hnsw_index<float, CosineMetric>(db_, "main", "vectors",
                                                                             *hnsw_, &err);
        if (rc != SQLITE_OK) {
            if (err) {
                spdlog::error("Failed to save HNSW index: {}", err);
                sqlite3_free(err);
            }
        } else {
            hnsw_dirty_ = false;
            pending_inserts_ = 0;
        }
    }

    // Save HNSW checkpoint (metadata + recent nodes)
    void saveHnswCheckpointUnlocked() {
        if (!hnsw_ || !db_)
            return;

        char* err = nullptr;
        int rc = sqlite_vec_cpp::index::save_hnsw_checkpoint<float, CosineMetric>(
            db_, "main", "vectors", *hnsw_, &err);
        if (rc != SQLITE_OK && err) {
            spdlog::warn("Failed to save HNSW checkpoint: {}", err);
            sqlite3_free(err);
        } else {
            pending_inserts_ = 0;
        }
    }

    // Rebuild HNSW from scratch
    void rebuildHnswUnlocked() {
        hnsw_.reset();
        hnsw_loaded_ = false;
        hnsw_dirty_ = false;

        ensureHnswLoadedUnlocked();
        saveHnswUnlocked();
    }

    // Compact HNSW (remove soft-deleted nodes)
    void compactHnswUnlocked() {
        if (!hnsw_)
            return;

        spdlog::info("Compacting HNSW index (deleted ratio: {:.2f})",
                     static_cast<float>(hnsw_->deleted_count()) /
                         static_cast<float>(hnsw_->size()));

        *hnsw_ = hnsw_->compact();
        hnsw_dirty_ = true;

        saveHnswUnlocked();
    }

    Config config_;
    std::string db_path_;
    sqlite3* db_ = nullptr;
    bool initialized_ = false;
    bool in_transaction_ = false;

    // HNSW index
    std::unique_ptr<HNSWIndex> hnsw_;
    bool hnsw_loaded_ = false;
    bool hnsw_dirty_ = false;
    size_t pending_inserts_ = 0;

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
                                const std::map<std::string, std::string>& metadata_filters) {
    return impl_->searchSimilar(query_embedding, k, similarity_threshold, document_hash,
                                metadata_filters);
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
