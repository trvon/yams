#include <yams/vector/sqlite_vec_backend.h>
#include <yams/vector/turboquant.h>
#include <yams/vector/vector_database.h>
#include <yams/vector/vector_schema_migration.h>
#include <yams/vector/vector_utils.h>

#include <yams/daemon/components/TuneAdvisor.h>

#include "simeon_pq_persistence.h"

#include <sqlite3.h>
#if defined(__APPLE__)
#include <malloc/malloc.h>
#elif defined(__GLIBC__)
#include <malloc.h>
#endif
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <shared_mutex>
#include <span>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include <simeon/pq.hpp>
#include <sqlite-vec-cpp/distances/cosine.hpp>
#include <sqlite-vec-cpp/distances/inner_product.hpp>
#include <sqlite-vec-cpp/distances/l2.hpp>
#include <sqlite-vec-cpp/sqlite/registration.hpp>

namespace yams::vector {

struct SimeonPqIndexState {
    simeon::ProductQuantizer pq;
    std::vector<std::size_t> rowids;
    std::vector<std::uint8_t> codes;
    std::size_t rerank_factor = 2;

    explicit SimeonPqIndexState(simeon::PQConfig cfg) : pq(cfg) {}
};

namespace {

bool db_lifetime_trace_enabled() {
    static std::atomic<int> cached{-1};
    int cachedValue = cached.load(std::memory_order_relaxed);
    if (cachedValue >= 0) {
        return cachedValue == 1;
    }

    const char* env = std::getenv("YAMS_TRACE_DB_LIFETIME");
    bool enabled = env && *env && std::string_view(env) != "0";
    cached.store(enabled ? 1 : 0, std::memory_order_relaxed);
    return enabled;
}

void trace_vector_db_lifetime(const char* event, const void* self, std::string_view path,
                              sqlite3* db, int rc = SQLITE_OK, int liveStatements = -1) {
    if (!db_lifetime_trace_enabled()) {
        return;
    }
    std::fprintf(stderr, "[VectorDB:%s] this=%p sqlite=%p rc=%d stmts=%d path=%.*s\n", event, self,
                 static_cast<void*>(db), rc, liveStatements, static_cast<int>(path.size()),
                 path.data());
    std::fflush(stderr);
}

int count_live_statements(sqlite3* db) {
    if (!db) {
        return 0;
    }
    int count = 0;
    sqlite3_stmt* stmt = sqlite3_next_stmt(db, nullptr);
    while (stmt) {
        ++count;
        stmt = sqlite3_next_stmt(db, stmt);
    }
    return count;
}

thread_local std::vector<sqlite3_stmt*> tl_deferred_finalize;

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

inline bool normalizeEmbeddingInPlace(std::vector<float>& embedding) {
    double norm_sq = 0.0;
    for (float val : embedding) {
        norm_sq += static_cast<double>(val) * static_cast<double>(val);
    }
    if (norm_sq <= 1e-20) {
        return false;
    }
    float inv_norm = 1.0f / std::sqrt(static_cast<float>(norm_sq));
    for (float& val : embedding) {
        val *= inv_norm;
    }
    return true;
}

// Persist per-coord scales to the DB. Returns true on success.
inline bool saveTurboQuantPerCoordScales(sqlite3* db, size_t dim, uint8_t bits, uint64_t seed,
                                         const std::vector<float>& scales) {
    if (scales.size() != dim || dim == 0) {
        return false;
    }
    // Serialize scales as binary blob (little-endian IEEE-754 floats)
    std::vector<uint8_t> blob(scales.size() * sizeof(float));
    for (size_t i = 0; i < scales.size(); ++i) {
        float val = scales[i];
        std::memcpy(&blob[i * sizeof(float)], &val, sizeof(float));
    }

    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(db,
                                "INSERT OR REPLACE INTO turboquant_quantizer_meta (dim, bits, "
                                "seed, fit_version, per_coord_scales) "
                                "VALUES (?, ?, ?, ?, ?)",
                                -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        return false;
    }
    sqlite3_bind_int64(stmt, 1, static_cast<sqlite3_int64>(dim));
    sqlite3_bind_int(stmt, 2, static_cast<int>(bits));
    sqlite3_bind_int64(stmt, 3, static_cast<sqlite3_int64>(seed));
    sqlite3_bind_int(stmt, 4, 1); // fit_version = 1 (scales only)
    sqlite3_bind_blob(stmt, 5, blob.data(), static_cast<int>(blob.size()), SQLITE_TRANSIENT);
    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    return rc == SQLITE_DONE;
}

// Save the full fitted model (v2: scales + per-coord centroids).
// Returns false if dim==0 or if centroids size doesn't match dim*num_centroids.
inline bool saveTurboQuantFittedModel(sqlite3* db, size_t dim, uint8_t bits, uint64_t seed,
                                      const std::vector<float>& scales,
                                      const std::vector<float>& centroids) {
    if (dim == 0 || scales.size() != dim) {
        return false;
    }
    size_t num_centroids = 1u << bits;
    if (!centroids.empty() && centroids.size() != dim * num_centroids) {
        return false; // Mismatch
    }

    // Serialize scales
    std::vector<uint8_t> scales_blob(scales.size() * sizeof(float));
    for (size_t i = 0; i < scales.size(); ++i) {
        float val = scales[i];
        std::memcpy(&scales_blob[i * sizeof(float)], &val, sizeof(float));
    }

    // Serialize centroids (may be empty for v1 fallback)
    std::vector<uint8_t> centroids_blob(centroids.size() * sizeof(float));
    for (size_t i = 0; i < centroids.size(); ++i) {
        float val = centroids[i];
        std::memcpy(&centroids_blob[i * sizeof(float)], &val, sizeof(float));
    }

    int fit_version = centroids.empty() ? 1 : 2;

    sqlite3_stmt* stmt = nullptr;
    int rc =
        sqlite3_prepare_v2(db,
                           "INSERT OR REPLACE INTO turboquant_quantizer_meta "
                           "(dim, bits, seed, fit_version, per_coord_scales, per_coord_centroids) "
                           "VALUES (?, ?, ?, ?, ?, ?)",
                           -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        return false;
    }
    sqlite3_bind_int64(stmt, 1, static_cast<sqlite3_int64>(dim));
    sqlite3_bind_int(stmt, 2, static_cast<int>(bits));
    sqlite3_bind_int64(stmt, 3, static_cast<sqlite3_int64>(seed));
    sqlite3_bind_int(stmt, 4, fit_version);
    sqlite3_bind_blob(stmt, 5, scales_blob.data(), static_cast<int>(scales_blob.size()),
                      SQLITE_TRANSIENT);
    sqlite3_bind_blob(stmt, 6, centroids_blob.data(), static_cast<int>(centroids_blob.size()),
                      SQLITE_TRANSIENT);
    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    return rc == SQLITE_DONE;
}

// Load per-coord scales from the DB. Returns empty vector if not found.
inline std::vector<float> loadTurboQuantPerCoordScales(sqlite3* db, size_t dim, uint8_t bits,
                                                       uint64_t seed) {
    std::vector<float> scales;
    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(db,
                                "SELECT per_coord_scales FROM turboquant_quantizer_meta "
                                "WHERE dim = ? AND bits = ? AND seed = ?",
                                -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        return scales;
    }
    sqlite3_bind_int64(stmt, 1, static_cast<sqlite3_int64>(dim));
    sqlite3_bind_int(stmt, 2, static_cast<int>(bits));
    sqlite3_bind_int64(stmt, 3, static_cast<sqlite3_int64>(seed));
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        const void* blob = sqlite3_column_blob(stmt, 0);
        int blob_bytes = sqlite3_column_bytes(stmt, 0);
        if (blob && blob_bytes > 0) {
            size_t num_floats = blob_bytes / sizeof(float);
            if (num_floats == dim) {
                scales.resize(dim);
                std::memcpy(scales.data(), blob, blob_bytes);
            }
        }
    }
    sqlite3_finalize(stmt);
    return scales;
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

// RAII guard to ensure prepared statements are reset after use.
// Un-reset statements can hold shared WAL read locks, preventing checkpoints.
struct StmtResetGuard {
    sqlite3_stmt* stmt;
    explicit StmtResetGuard(sqlite3_stmt* s) : stmt(s) {}
    ~StmtResetGuard() {
        if (stmt)
            sqlite3_reset(stmt);
    }
    StmtResetGuard(const StmtResetGuard&) = delete;
    StmtResetGuard& operator=(const StmtResetGuard&) = delete;
};

// SQL statements
constexpr const char* kCreateVectorsTable = R"sql(
CREATE TABLE IF NOT EXISTS vectors (
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

CREATE INDEX IF NOT EXISTS idx_vectors_chunk_id ON vectors(chunk_id);
CREATE INDEX IF NOT EXISTS idx_vectors_document_hash ON vectors(document_hash);
CREATE INDEX IF NOT EXISTS idx_vectors_model ON vectors(model_id, model_version);
CREATE INDEX IF NOT EXISTS idx_vectors_embedding_dim ON vectors(embedding_dim);
CREATE INDEX IF NOT EXISTS idx_vectors_level ON vectors(level, document_hash);
)sql";

// Global quantizer metadata table: stores per-coord scales once per quantizer config.
// Per-coord scales are global (same for all vectors with the same seed+dim).
// Fitted quantizer model metadata (v1: scales only; v2: scales + per-coord centroids).
// The fit_version field enables forward-compatible loading:
//   v1: per_coord_scales is non-null, per_coord_centroids is null
//   v2: per_coord_scales is non-null, per_coord_centroids is non-null
// Centroid storage: dim * num_centroids floats (little-endian IEEE-754), row-major per coord.
constexpr const char* kCreateTurboQuantMeta = R"sql(
CREATE TABLE IF NOT EXISTS turboquant_quantizer_meta (
    rowid INTEGER PRIMARY KEY,
    dim INTEGER NOT NULL,
    bits INTEGER NOT NULL,
    seed INTEGER NOT NULL,
    fit_version INTEGER NOT NULL DEFAULT 1,
    per_coord_scales BLOB,
    per_coord_centroids BLOB,
    UNIQUE(dim, bits, seed)
))sql";

constexpr const char* kCreateSimeonPqMeta = R"sql(
CREATE TABLE IF NOT EXISTS simeon_pq_meta (
    dim INTEGER PRIMARY KEY,
    format_version INTEGER NOT NULL DEFAULT 1,
    m INTEGER NOT NULL,
    k INTEGER NOT NULL,
    seed INTEGER NOT NULL,
    rerank_factor INTEGER NOT NULL,
    trained INTEGER NOT NULL DEFAULT 0,
    vector_count INTEGER NOT NULL,
    codebooks_blob BLOB NOT NULL,
    updated_at INTEGER NOT NULL DEFAULT (unixepoch())
))sql";

constexpr const char* kCreateSimeonPqCodes = R"sql(
CREATE TABLE IF NOT EXISTS simeon_pq_codes (
    dim INTEGER NOT NULL,
    rowid INTEGER NOT NULL,
    code BLOB NOT NULL,
    PRIMARY KEY (dim, rowid)
))sql";

constexpr const char* kInsertVector = R"sql(
INSERT INTO vectors (
    chunk_id, document_hash, embedding, embedding_dim, content,
    start_offset, end_offset, metadata,
    model_id, model_version, embedding_version, content_hash,
    created_at, embedded_at, is_stale, level,
    source_chunk_ids, parent_document_hash, child_document_hashes,
    quantized_format, quantized_bits, quantized_seed, quantized_packed_codes
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
)sql";

constexpr const char* kSelectByChunkId = R"sql(
SELECT rowid, chunk_id, document_hash, embedding, embedding_dim, content,
       start_offset, end_offset, metadata,
       model_id, model_version, embedding_version, content_hash,
       created_at, embedded_at, is_stale, level,
       source_chunk_ids, parent_document_hash, child_document_hashes,
       quantized_format, quantized_bits, quantized_seed, quantized_packed_codes
FROM vectors WHERE chunk_id = ?
)sql";

constexpr const char* kSelectByRowid = R"sql(
SELECT rowid, chunk_id, document_hash, embedding, embedding_dim, content,
       start_offset, end_offset, metadata,
       model_id, model_version, embedding_version, content_hash,
       created_at, embedded_at, is_stale, level,
       source_chunk_ids, parent_document_hash, child_document_hashes,
       quantized_format, quantized_bits, quantized_seed, quantized_packed_codes
FROM vectors WHERE rowid = ?
)sql";

constexpr const char* kSelectByDocumentHash = R"sql(
SELECT rowid, chunk_id, document_hash, embedding, embedding_dim, content,
       start_offset, end_offset, metadata,
       model_id, model_version, embedding_version, content_hash,
       created_at, embedded_at, is_stale, level,
       source_chunk_ids, parent_document_hash, child_document_hashes,
       quantized_format, quantized_bits, quantized_seed, quantized_packed_codes
FROM vectors WHERE document_hash = ?
)sql";

constexpr const char* kDeleteByChunkId = "DELETE FROM vectors WHERE chunk_id = ?";
constexpr const char* kDeleteByDocumentHash = "DELETE FROM vectors WHERE document_hash = ?";
constexpr const char* kGetRowidByChunkId = "SELECT rowid FROM vectors WHERE chunk_id = ?";
constexpr const char* kGetRowidsByDocumentHash =
    "SELECT rowid FROM vectors WHERE document_hash = ?";
constexpr const char* kCountVectors = "SELECT COUNT(*) FROM vectors";
constexpr const char* kHasEmbedding = "SELECT 1 FROM vectors WHERE document_hash = ? LIMIT 1";
constexpr const char* kSelectFilterByRowid =
    "SELECT document_hash, metadata FROM vectors WHERE rowid = ?";
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

    /// Get raw SQLite handle (for migration/testing only)
    sqlite3* dbHandle() const { return db_; }

    bool usesVec0SearchEngine() const {
        return config_.search_engine == VectorSearchEngine::Vec0L2;
    }

    bool usesSimeonPqSearchEngine() const {
        return config_.search_engine == VectorSearchEngine::SimeonPqAdc;
    }

    std::uint32_t normalizedSimeonPqSubquantizers(size_t dim) const {
        const std::uint32_t requested =
            static_cast<std::uint32_t>(std::max<size_t>(1, config_.simeon_pq_subquantizers));
        if (dim == 0) {
            return requested;
        }
        if (dim % requested == 0) {
            return requested;
        }
        for (std::uint32_t candidate = requested; candidate > 1; --candidate) {
            if (dim % candidate == 0) {
                return candidate;
            }
        }
        return 1;
    }

    std::string vec0TableName(size_t dim) const {
        return "vectors_" + std::to_string(dim) + "_vec0";
    }

    void markVec0DimDirtyUnlocked(size_t dim) {
        if (dim == 0) {
            return;
        }
        vec0_dirty_dims_.insert(dim);
        vec0_ready_dims_.erase(dim);
    }

    void markVec0DimsDirtyUnlocked(const std::unordered_set<size_t>& dims) {
        for (size_t dim : dims) {
            markVec0DimDirtyUnlocked(dim);
        }
    }

    void markSimeonPqDimDirtyUnlocked(size_t dim) {
        if (dim == 0) {
            return;
        }
        simeon_pq_dirty_dims_.insert(dim);
        simeon_pq_ready_dims_.erase(dim);
    }

    void markSimeonPqDimsDirtyUnlocked(const std::unordered_set<size_t>& dims) {
        for (size_t dim : dims) {
            markSimeonPqDimDirtyUnlocked(dim);
        }
    }

    Result<void> initialize(const std::string& db_path) {
        std::unique_lock lock(mutex_);

        if (db_) {
            return Error{ErrorCode::InvalidState, "Already initialized"};
        }

        // Contract: quantized-primary storage requires TurboQuant sidecar for reconstruction
        if (config_.quantized_primary_storage && !config_.enable_turboquant_storage) {
            return Error{ErrorCode::InvalidArgument,
                         "quantized_primary_storage=true requires enable_turboquant_storage=true"};
        }

        query_dim_counts_.clear();

        int rc = sqlite3_open(db_path.c_str(), &db_);
        if (rc != SQLITE_OK) {
            std::string err = db_ ? sqlite3_errmsg(db_) : "Unknown error";
            if (db_) {
                sqlite3_close(db_);
                db_ = nullptr;
            }
            return Error{ErrorCode::DatabaseError, "Failed to open database: " + err};
        }

        // Set busy timeout so lock contention waits rather than failing immediately.
        // 5 seconds matches the retry budget used elsewhere in the vector backend.
        sqlite3_busy_timeout(db_, 5000);

        // Enable WAL mode for better concurrency
        sqlite3_exec(db_, "PRAGMA journal_mode=WAL", nullptr, nullptr, nullptr);
        sqlite3_exec(db_, "PRAGMA synchronous=NORMAL", nullptr, nullptr, nullptr);
        sqlite3_exec(db_, "PRAGMA cache_size=-2048", nullptr, nullptr, nullptr);
        sqlite3_exec(db_, "PRAGMA temp_store=MEMORY", nullptr, nullptr, nullptr);
        sqlite3_exec(db_, "PRAGMA mmap_size=268435456", nullptr, nullptr, nullptr); // 256MB

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
        } else if (schema_version == VectorSchemaMigration::SchemaVersion::V2_1) {
            // Upgrade V2.1 to V2.2 (add quantized sidecar columns)
            spdlog::info("Detected V2.1 vector schema, upgrading to V2.2...");
            auto migrate_result = VectorSchemaMigration::migrateV2_1ToV2_2(db_);
            if (!migrate_result) {
                spdlog::error("V2.1 to V2.2 migration failed: {}", migrate_result.error().message);
                sqlite3_close(db_);
                db_ = nullptr;
                return Error{ErrorCode::DatabaseError,
                             "Schema migration failed: " + migrate_result.error().message};
            }
            spdlog::info("V2.1 to V2.2 migration completed successfully");
        }

        db_path_ = db_path;
        initialized_.store(true, std::memory_order_release);
        refreshQueryDimCountsUnlocked();
        trace_vector_db_lifetime("open", this, db_path_, db_, SQLITE_OK,
                                 count_live_statements(db_));

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
        trace_vector_db_lifetime("close.begin", this, db_path_, db_, SQLITE_OK,
                                 count_live_statements(db_));

        // Finalize prepared statements
        finalizeStatements();

        if (db_) {
            sqlite3* db = db_;
            const int liveStatementsBeforeClose = count_live_statements(db);
            int rc = sqlite3_close_v2(db);
            if (rc != SQLITE_OK) {
                spdlog::warn("[VectorDB] close_v2 deferred/failed for '{}': {}", db_path_,
                             sqlite3_errstr(rc));
            }
            trace_vector_db_lifetime("close.sqlite", this, db_path_, db, rc,
                                     liveStatementsBeforeClose);
            db_ = nullptr;
        }

        initialized_.store(false, std::memory_order_release);
        query_dim_counts_.clear();
        vec0_ready_dims_.clear();
        vec0_dirty_dims_.clear();
        simeon_pq_indices_.clear();
        simeon_pq_ready_dims_.clear();
        simeon_pq_dirty_dims_.clear();
        trace_vector_db_lifetime("close.end", this, db_path_, db_);
    }

    bool isInitialized() const noexcept {
        // Lock-free: readiness flag is published with release after init, read with
        // acquire. Prevents status/health paths from blocking on the long-held
        // unique_lock acquired by buildIndex/rebuild paths.
        return initialized_.load(std::memory_order_acquire);
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

        // Create TurboQuant quantizer metadata table for global per-coord scales
        rc = sqlite3_exec(db_, kCreateTurboQuantMeta, nullptr, nullptr, &err_msg);
        if (rc != SQLITE_OK) {
            std::string err = err_msg ? err_msg : "Unknown error";
            sqlite3_free(err_msg);
            return Error{ErrorCode::DatabaseError,
                         "Failed to create turboquant_quantizer_meta table: " + err};
        }

        rc = sqlite3_exec(db_, kCreateSimeonPqMeta, nullptr, nullptr, &err_msg);
        if (rc != SQLITE_OK) {
            std::string err = err_msg ? err_msg : "Unknown error";
            sqlite3_free(err_msg);
            return Error{ErrorCode::DatabaseError, "Failed to create simeon_pq_meta table: " + err};
        }
        rc = sqlite3_exec(db_, kCreateSimeonPqCodes, nullptr, nullptr, &err_msg);
        if (rc != SQLITE_OK) {
            std::string err = err_msg ? err_msg : "Unknown error";
            sqlite3_free(err_msg);
            return Error{ErrorCode::DatabaseError,
                         "Failed to create simeon_pq_codes table: " + err};
        }

        // NOTE: Legacy vectors_hnsw_meta/vectors_hnsw_nodes and per-dim
        // vectors_{dim}_hnsw_* tables are no longer created. Existing databases
        // may still contain them; a future migration can drop them.

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
        (void)rowid;
        const size_t record_dim = record.embedding.size();
        query_dim_counts_[record_dim] += 1;
        if (usesVec0SearchEngine()) {
            markVec0DimDirtyUnlocked(record_dim);
        }
        if (usesSimeonPqSearchEngine()) {
            markSimeonPqDimDirtyUnlocked(record_dim);
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
        std::unordered_set<size_t> vec0_affected_dims;

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
                    old_dims[idx] = !old_record->embedding.empty() ? old_record->embedding.size()
                                                                   : old_record->embedding_dim;
                    old_rowids[idx] = *existing_rowid;
                    if (old_dims[idx]) {
                        vec0_affected_dims.insert(*old_dims[idx]);
                    }
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
                size_t new_dim =
                    !record.embedding.empty() ? record.embedding.size() : record.embedding_dim;
                if (new_dim > 0) {
                    vec0_affected_dims.insert(new_dim);
                }
                ++updated_existing;
                continue;
            }

            auto rowid_result = insertVectorUnlocked(record);
            if (!rowid_result) {
                execWithRetry(db_, "ROLLBACK");
                return rowid_result.error();
            }
            rowids[idx] = rowid_result.value();
            size_t new_dim =
                !record.embedding.empty() ? record.embedding.size() : record.embedding_dim;
            if (new_dim > 0) {
                vec0_affected_dims.insert(new_dim);
            }
            ++inserted_count;
        }

        if (usesVec0SearchEngine()) {
            markVec0DimsDirtyUnlocked(vec0_affected_dims);
        }
        if (usesSimeonPqSearchEngine()) {
            markSimeonPqDimsDirtyUnlocked(vec0_affected_dims);
        }

        // Commit transaction with retry
        if (!execWithRetry(db_, "COMMIT")) {
            return Error{ErrorCode::DatabaseError, "Failed to commit transaction"};
        }

        for (size_t idx : unique_indices) {
            if (rowids[idx] >= 0) {
                query_dim_counts_[records[idx].embedding.size()] += 1;
            }
        }

        if (skipped_existing > 0 || skipped_duplicates > 0 || updated_existing > 0) {
            spdlog::debug(
                "[VectorDB] Batch summary: inserted={}, updated_existing={}, skipped_existing={}, "
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
        // Use embedding_dim when embedding blob is absent (quantized-primary mode).
        std::optional<size_t> old_dim;
        auto old_record = getVectorByChunkIdUnlocked(chunk_id);
        if (old_record) {
            old_dim = !old_record->embedding.empty() ? old_record->embedding.size()
                                                     : old_record->embedding_dim;
        }

        // Delete old record
        if (stmt_delete_by_chunk_id_) {
            sqlite3_reset(stmt_delete_by_chunk_id_);
            sqlite3_bind_text(stmt_delete_by_chunk_id_, 1, chunk_id.c_str(), -1, SQLITE_TRANSIENT);
            stepWithRetry(stmt_delete_by_chunk_id_);
        }

        // Insert new record
        auto rowid_result = insertVectorUnlocked(record);
        if (!rowid_result) {
            return rowid_result.error();
        }

        (void)old_rowid;
        (void)rowid_result.value();
        size_t new_dim = !record.embedding.empty()
                             ? record.embedding.size()
                             : (record.embedding_dim > 0 ? record.embedding_dim : 0);

        if (old_dim) {
            auto oldIt = query_dim_counts_.find(*old_dim);
            if (oldIt != query_dim_counts_.end() && oldIt->second > 0) {
                --oldIt->second;
            }
        }
        query_dim_counts_[new_dim] += 1;
        if (usesVec0SearchEngine()) {
            if (old_dim) {
                markVec0DimDirtyUnlocked(*old_dim);
            }
            markVec0DimDirtyUnlocked(new_dim);
        }
        if (usesSimeonPqSearchEngine()) {
            if (old_dim) {
                markSimeonPqDimDirtyUnlocked(*old_dim);
            }
            markSimeonPqDimDirtyUnlocked(new_dim);
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

        // Get dimension before deleting (use embedding_dim when blob is absent)
        std::optional<size_t> dim;
        auto record = getVectorByChunkIdUnlocked(chunk_id);
        if (record) {
            dim = !record->embedding.empty() ? record->embedding.size() : record->embedding_dim;
        }

        if (dim) {
            auto dimIt = query_dim_counts_.find(*dim);
            if (dimIt != query_dim_counts_.end() && dimIt->second > 0) {
                --dimIt->second;
            }
            if (usesVec0SearchEngine()) {
                markVec0DimDirtyUnlocked(*dim);
            }
            if (usesSimeonPqSearchEngine()) {
                markSimeonPqDimDirtyUnlocked(*dim);
            }
        }

        // Delete from SQLite
        if (stmt_delete_by_chunk_id_) {
            sqlite3_reset(stmt_delete_by_chunk_id_);
            sqlite3_bind_text(stmt_delete_by_chunk_id_, 1, chunk_id.c_str(), -1, SQLITE_TRANSIENT);
            stepWithRetry(stmt_delete_by_chunk_id_);
        }

        (void)rowid;
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

        for (const auto& [_, dim] : rowid_dims) {
            auto dimIt = query_dim_counts_.find(dim);
            if (dimIt != query_dim_counts_.end() && dimIt->second > 0) {
                --dimIt->second;
            }
        }
        if (usesVec0SearchEngine()) {
            std::unordered_set<size_t> affected_dims;
            for (const auto& [_, dim] : rowid_dims) {
                affected_dims.insert(dim);
            }
            markVec0DimsDirtyUnlocked(affected_dims);
        }
        if (usesSimeonPqSearchEngine()) {
            std::unordered_set<size_t> affected_dims;
            for (const auto& [_, dim] : rowid_dims) {
                affected_dims.insert(dim);
            }
            markSimeonPqDimsDirtyUnlocked(affected_dims);
        }

        // Delete from SQLite
        if (stmt_delete_by_doc_) {
            sqlite3_reset(stmt_delete_by_doc_);
            sqlite3_bind_text(stmt_delete_by_doc_, 1, document_hash.c_str(), -1, SQLITE_TRANSIENT);
            stepWithRetry(stmt_delete_by_doc_);
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

        if (usesVec0SearchEngine()) {
            const size_t query_dim = query_embedding.size();

            if (document_hash || !candidate_hashes.empty() || !metadata_filters.empty()) {
                spdlog::debug("[vec0] filtered search falling back to exact cosine scan");
                return bruteForceSearchUnlocked(query_embedding, k, similarity_threshold,
                                                document_hash, candidate_hashes, metadata_filters);
            }

            if (!vec0_ready_dims_.contains(query_dim) || vec0_dirty_dims_.contains(query_dim)) {
                lock.unlock();
                std::unique_lock write_lock(mutex_);
                auto ready = ensureVec0ReadyUnlocked(query_dim);
                write_lock.unlock();
                for (auto* s : tl_deferred_finalize) {
                    sqlite3_finalize(s);
                }
                tl_deferred_finalize.clear();
                lock.lock();
                if (!ready) {
                    return ready.error();
                }
            }

            return vec0SearchUnlocked(query_embedding, k, similarity_threshold);
        }

        if (usesSimeonPqSearchEngine()) {
            const size_t query_dim = query_embedding.size();

            if (document_hash || !candidate_hashes.empty() || !metadata_filters.empty()) {
                spdlog::debug("[SPQ] filtered search falling back to exact cosine scan");
                return bruteForceSearchUnlocked(query_embedding, k, similarity_threshold,
                                                document_hash, candidate_hashes, metadata_filters);
            }

            if (!simeon_pq_ready_dims_.contains(query_dim) ||
                simeon_pq_dirty_dims_.contains(query_dim) ||
                !simeon_pq_indices_.contains(query_dim)) {
                lock.unlock();
                std::unique_lock write_lock(mutex_);
                auto ready = ensureSimeonPqReadyUnlocked(query_dim);
                write_lock.unlock();
                for (auto* s : tl_deferred_finalize) {
                    sqlite3_finalize(s);
                }
                tl_deferred_finalize.clear();
                lock.lock();
                if (!ready) {
                    return ready.error();
                }
            }

            return simeonPqSearchUnlocked(query_embedding, k, similarity_threshold);
        }

        return Error{ErrorCode::InvalidOperation, "no search engine configured"};
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
        (void)num_threads;

        std::shared_lock lock(mutex_);

        if (!db_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        if (usesVec0SearchEngine()) {
            if (!vec0_ready_dims_.contains(query_dim) || vec0_dirty_dims_.contains(query_dim)) {
                lock.unlock();
                std::unique_lock write_lock(mutex_);
                auto ready = ensureVec0ReadyUnlocked(query_dim);
                write_lock.unlock();
                for (auto* s : tl_deferred_finalize) {
                    sqlite3_finalize(s);
                }
                tl_deferred_finalize.clear();
                lock.lock();
                if (!ready) {
                    return ready.error();
                }
            }

            std::vector<std::vector<VectorRecord>> results;
            results.reserve(query_embeddings.size());
            for (const auto& query_embedding : query_embeddings) {
                auto result = vec0SearchUnlocked(query_embedding, k, similarity_threshold);
                if (!result) {
                    return result.error();
                }
                results.push_back(std::move(result.value()));
            }
            return results;
        }

        if (usesSimeonPqSearchEngine()) {
            if (!simeon_pq_ready_dims_.contains(query_dim) ||
                simeon_pq_dirty_dims_.contains(query_dim) ||
                !simeon_pq_indices_.contains(query_dim)) {
                lock.unlock();
                std::unique_lock write_lock(mutex_);
                auto ready = ensureSimeonPqReadyUnlocked(query_dim);
                write_lock.unlock();
                for (auto* s : tl_deferred_finalize) {
                    sqlite3_finalize(s);
                }
                tl_deferred_finalize.clear();
                lock.lock();
                if (!ready) {
                    return ready.error();
                }
            }

            std::vector<std::vector<VectorRecord>> results;
            results.reserve(query_embeddings.size());
            for (const auto& query_embedding : query_embeddings) {
                auto result = simeonPqSearchUnlocked(query_embedding, k, similarity_threshold);
                if (!result) {
                    return result.error();
                }
                results.push_back(std::move(result.value()));
            }
            return results;
        }

        return Error{ErrorCode::InvalidOperation, "no search engine configured"};
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
            std::lock_guard stmt_lock(stmt_mutex_);
            sqlite3_reset(stmt_select_by_doc_);
            StmtResetGuard guard(stmt_select_by_doc_);
            sqlite3_bind_text(stmt_select_by_doc_, 1, document_hash.c_str(), -1, SQLITE_TRANSIENT);

            while (sqlite3_step(stmt_select_by_doc_) == SQLITE_ROW) {
                results.push_back(recordFromStatement(stmt_select_by_doc_));
            }
        }

        return results;
    }

    Result<std::unordered_map<std::string, VectorRecord>> getDocumentLevelVectorsAll() {
        std::shared_lock lock(mutex_);

        if (!db_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        static constexpr const char* kSelectAllDocLevel = R"sql(
SELECT rowid, chunk_id, document_hash, embedding, embedding_dim, content,
       start_offset, end_offset, metadata,
       model_id, model_version, embedding_version, content_hash,
       created_at, embedded_at, is_stale, level,
       source_chunk_ids, parent_document_hash, child_document_hashes,
       quantized_format, quantized_bits, quantized_seed, quantized_packed_codes
FROM vectors WHERE level = ?
)sql";

        std::unordered_map<std::string, VectorRecord> results;

        std::lock_guard stmt_lock(stmt_mutex_);
        sqlite3_stmt* stmt = nullptr;
        if (sqlite3_prepare_v2(db_, kSelectAllDocLevel, -1, &stmt, nullptr) != SQLITE_OK) {
            return Error{ErrorCode::DatabaseError,
                         std::string{"prepare getDocumentLevelVectorsAll: "} + sqlite3_errmsg(db_)};
        }
        sqlite3_bind_int(stmt, 1, static_cast<int>(EmbeddingLevel::DOCUMENT));

        while (sqlite3_step(stmt) == SQLITE_ROW) {
            auto record = recordFromStatement(stmt);
            auto hash = record.document_hash;
            if (!hash.empty()) {
                results.emplace(std::move(hash), std::move(record));
            }
        }
        lock.unlock();
        sqlite3_finalize(stmt);

        return results;
    }

    Result<size_t> forEachDocumentLevelVector(const std::function<bool(VectorRecord&&)>& visitor) {
        if (!visitor) {
            return Error{ErrorCode::InvalidArgument, "forEachDocumentLevelVector visitor is empty"};
        }

        std::shared_lock lock(mutex_);

        if (!db_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        static constexpr const char* kSelectAllDocLevel = R"sql(
SELECT rowid, chunk_id, document_hash, embedding, embedding_dim, content,
       start_offset, end_offset, metadata,
       model_id, model_version, embedding_version, content_hash,
       created_at, embedded_at, is_stale, level,
       source_chunk_ids, parent_document_hash, child_document_hashes,
       quantized_format, quantized_bits, quantized_seed, quantized_packed_codes
FROM vectors WHERE level = ?
)sql";

        std::lock_guard stmt_lock(stmt_mutex_);
        sqlite3_stmt* stmt = nullptr;
        if (sqlite3_prepare_v2(db_, kSelectAllDocLevel, -1, &stmt, nullptr) != SQLITE_OK) {
            return Error{ErrorCode::DatabaseError,
                         std::string{"prepare forEachDocumentLevelVector: "} + sqlite3_errmsg(db_)};
        }

        sqlite3_bind_int(stmt, 1, static_cast<int>(EmbeddingLevel::DOCUMENT));

        size_t delivered = 0;
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            auto record = recordFromStatement(stmt);
            if (record.document_hash.empty()) {
                continue;
            }
            ++delivered;
            if (!visitor(std::move(record))) {
                break;
            }
        }

        lock.unlock();
        sqlite3_finalize(stmt);
        return delivered;
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
            std::lock_guard stmt_lock(stmt_mutex_);
            sqlite3_reset(stmt_has_embedding_);
            StmtResetGuard guard(stmt_has_embedding_);
            sqlite3_bind_text(stmt_has_embedding_, 1, document_hash.c_str(), -1, SQLITE_TRANSIENT);

            return sqlite3_step(stmt_has_embedding_) == SQLITE_ROW;
        }

        return false;
    }

    Result<std::unordered_set<std::string>> getEmbeddedDocumentHashes() {
        std::shared_lock lock(mutex_);

        if (!db_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        std::unordered_set<std::string> hashes;
        const char* sql = "SELECT DISTINCT document_hash FROM vectors";
        sqlite3_stmt* stmt = nullptr;
        int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
        if (rc != SQLITE_OK) {
            return Error{ErrorCode::DatabaseError,
                         std::string("Failed to prepare getEmbeddedDocumentHashes: ") +
                             sqlite3_errmsg(db_)};
        }

        while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
            const char* hash = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
            if (hash) {
                hashes.emplace(hash);
            }
        }
        sqlite3_finalize(stmt);

        if (rc != SQLITE_DONE) {
            return Error{ErrorCode::DatabaseError,
                         std::string("getEmbeddedDocumentHashes iteration failed: ") +
                             sqlite3_errmsg(db_)};
        }

        return hashes;
    }

    Result<size_t> getVectorCount() {
        std::shared_lock lock(mutex_);

        if (!db_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        if (stmt_count_) {
            sqlite3_reset(stmt_count_);
            StmtResetGuard guard(stmt_count_);
            if (sqlite3_step(stmt_count_) == SQLITE_ROW) {
                return static_cast<size_t>(sqlite3_column_int64(stmt_count_, 0));
            }
        }

        return size_t{0};
    }

    Result<VectorDatabaseStats> getStats() {
        std::shared_lock lock(mutex_);

        if (!db_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        VectorDatabaseStats stats;

        // Get vector count
        if (stmt_count_) {
            sqlite3_reset(stmt_count_);
            StmtResetGuard countGuard(stmt_count_);
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
        if (config_.suppress_search_index_builds) {
            spdlog::warn("[VectorIndex] buildIndex suppressed by memory instrumentation profile");
            return Result<void>{};
        }

        const auto start = std::chrono::steady_clock::now();
        if (usesVec0SearchEngine()) {
            auto result = rebuildVec0IndicesUnlocked();
            if (!result) {
                return result;
            }
            const auto durMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                                   std::chrono::steady_clock::now() - start)
                                   .count();
            spdlog::info("[vec0] buildIndex completed in {} ms", durMs);
            return Result<void>{};
        }
        if (usesSimeonPqSearchEngine()) {
            auto result = rebuildSimeonPqIndicesUnlocked();
            if (!result) {
                return result;
            }
            const auto durMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                                   std::chrono::steady_clock::now() - start)
                                   .count();
            spdlog::info("[SPQ] buildIndex completed in {} ms", durMs);
            return Result<void>{};
        }

        return Error{ErrorCode::InvalidOperation, "no search engine configured"};
    }

    Result<void> prepareSearchIndex() {
        std::unique_lock lock(mutex_);

        if (!db_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }
        if (config_.suppress_search_index_builds) {
            spdlog::warn(
                "[VectorIndex] prepareSearchIndex suppressed by memory instrumentation profile");
            return Result<void>{};
        }

        if (usesVec0SearchEngine()) {
            auto rebuild = rebuildVec0IndicesUnlocked();
            if (!rebuild) {
                return rebuild;
            }
            return warmVec0IndicesUnlocked();
        }
        if (usesSimeonPqSearchEngine()) {
            auto dims = queryVectorDimsUnlocked();
            if (dims.empty()) {
                return Result<void>{};
            }
            for (size_t dim : dims) {
                if (simeon_pq_ready_dims_.contains(dim) && !simeon_pq_dirty_dims_.contains(dim) &&
                    simeon_pq_indices_.contains(dim)) {
                    continue;
                }
                if (simeon_pq_dirty_dims_.contains(dim)) {
                    return Error{ErrorCode::InvalidState, "Simeon PQ index for dim " +
                                                              std::to_string(dim) +
                                                              " is dirty and requires rebuild"};
                }
                if (!loadPersistedSimeonPqDimUnlocked(dim)) {
                    return Error{ErrorCode::NotFound,
                                 "No reusable persisted Simeon PQ index for dim " +
                                     std::to_string(dim)};
                }
            }
            return Result<void>{};
        }

        return Error{ErrorCode::InvalidOperation, "no search engine configured"};
    }

    Result<bool> hasReusablePersistedSearchIndex() {
        std::shared_lock lock(mutex_);

        if (!db_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        if (usesVec0SearchEngine()) {
            return Result<bool>(false);
        }
        if (usesSimeonPqSearchEngine()) {
            auto dims = queryVectorDimsUnlocked();
            if (dims.empty()) {
                return Result<bool>(false);
            }
            for (size_t dim : dims) {
                if (!hasReusablePersistedSimeonPqUnlocked(dim)) {
                    return Result<bool>(false);
                }
            }
            return Result<bool>(true);
        }

        return Result<bool>(false);
    }

    Result<void> optimize() {
        std::unique_lock lock(mutex_);

        if (!db_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        if (usesVec0SearchEngine()) {
            if (!vec0_dirty_dims_.empty()) {
                if (config_.suppress_search_index_builds) {
                    spdlog::warn(
                        "[vec0] optimize rebuild suppressed by memory instrumentation profile");
                } else {
                    auto rebuild = rebuildVec0IndicesUnlocked();
                    if (!rebuild) {
                        return rebuild;
                    }
                }
            }
            return Result<void>{};
        }
        if (usesSimeonPqSearchEngine()) {
            if (!simeon_pq_dirty_dims_.empty()) {
                if (config_.suppress_search_index_builds) {
                    spdlog::warn(
                        "[SPQ] optimize rebuild suppressed by memory instrumentation profile");
                } else {
                    auto rebuild = rebuildSimeonPqIndicesUnlocked();
                    if (!rebuild) {
                        return rebuild;
                    }
                }
            }
            for (size_t dim : simeon_pq_ready_dims_) {
                auto saved = saveSimeonPqDimUnlocked(dim);
                if (!saved) {
                    return saved;
                }
            }
            return checkpointWalUnlocked();
        }

        return Error{ErrorCode::InvalidOperation, "no search engine configured"};
    }

    Result<void> persistIndex() {
        std::unique_lock lock(mutex_);

        if (!db_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        if (usesVec0SearchEngine()) {
            if (!vec0_dirty_dims_.empty()) {
                if (config_.suppress_search_index_builds) {
                    spdlog::warn(
                        "[vec0] persistIndex rebuild suppressed by memory instrumentation profile");
                } else {
                    auto rebuild = rebuildVec0IndicesUnlocked();
                    if (!rebuild) {
                        return rebuild;
                    }
                }
            }
            return checkpointWalUnlocked();
        }
        if (usesSimeonPqSearchEngine()) {
            if (!simeon_pq_dirty_dims_.empty()) {
                if (config_.suppress_search_index_builds) {
                    spdlog::warn(
                        "[SPQ] persistIndex rebuild suppressed by memory instrumentation profile");
                } else {
                    auto rebuild = rebuildSimeonPqIndicesUnlocked();
                    if (!rebuild) {
                        return rebuild;
                    }
                }
            }
            for (size_t dim : simeon_pq_ready_dims_) {
                auto saved = saveSimeonPqDimUnlocked(dim);
                if (!saved) {
                    return saved;
                }
            }
            return checkpointWalUnlocked();
        }

        return Error{ErrorCode::InvalidOperation, "no search engine configured"};
    }

    /// Checkpoint vectors.db WAL using PASSIVE mode (non-blocking).
    /// If the WAL exceeds a size threshold, escalate to TRUNCATE to reclaim disk.
    Result<void> checkpointWal() {
        std::unique_lock lock(mutex_);

        if (!db_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        return checkpointWalUnlocked();
    }

    Result<void> beginBulkLoad() { return beginBulkLoadUnlocked(); }

    Result<void> finalizeBulkLoad() { return finalizeBulkLoadUnlocked(); }

private:
    Result<void> checkpointWalUnlocked() {
        int walLog = 0, walCkpt = 0;

        // First try PASSIVE (non-blocking, won't interfere with readers)
        int rc =
            sqlite3_wal_checkpoint_v2(db_, nullptr, SQLITE_CHECKPOINT_PASSIVE, &walLog, &walCkpt);
        if (rc != SQLITE_OK) {
            return Error{ErrorCode::DatabaseError,
                         std::string("WAL checkpoint failed: ") + sqlite3_errmsg(db_)};
        }

        spdlog::debug("[VectorDB] WAL checkpoint PASSIVE: log={} checkpointed={}", walLog, walCkpt);

        // If significant un-checkpointed pages remain, try TRUNCATE to reclaim disk.
        // Threshold: 50K pages (~200MB at 4KB page size).
        constexpr int kTruncateThreshold = 50000;
        if (walLog > kTruncateThreshold && walLog > walCkpt) {
            spdlog::info("[VectorDB] WAL has {} uncheckpointed pages, attempting TRUNCATE",
                         walLog - walCkpt);
            rc = sqlite3_wal_checkpoint_v2(db_, nullptr, SQLITE_CHECKPOINT_TRUNCATE, &walLog,
                                           &walCkpt);
            if (rc == SQLITE_OK) {
                spdlog::info("[VectorDB] WAL TRUNCATE succeeded");
            } else {
                // TRUNCATE requires exclusive access; PASSIVE fallback is fine
                spdlog::debug("[VectorDB] WAL TRUNCATE not possible (busy): {}",
                              sqlite3_errmsg(db_));
            }
        }

        return Result<void>{};
    }

public:
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

        // Finalize all prepared statements
        finalizeStatements();

        // Drop any legacy HNSW shadow tables (orphaned by HNSW removal),
        // plus SimeonPQ metadata tables and any per-dim vec0 virtual tables.
        std::vector<std::string> tables_to_drop;
        const char* find_tables =
            "SELECT name FROM sqlite_master WHERE type='table' AND "
            "(name LIKE 'vectors_%_hnsw_meta' OR name LIKE 'vectors_%_hnsw_nodes' OR "
            " name = 'vectors_hnsw_meta' OR name = 'vectors_hnsw_nodes' OR "
            " name = 'simeon_pq_meta' OR "
            " name = 'simeon_pq_codes')";

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

        // Drop main vectors and auxiliary metadata tables
        tables_to_drop.push_back("DROP TABLE IF EXISTS vectors");
        tables_to_drop.push_back("DROP TABLE IF EXISTS turboquant_quantizer_meta");

        for (const auto& sql : tables_to_drop) {
            char* err_msg = nullptr;
            int rc = sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, &err_msg);
            if (rc != SQLITE_OK) {
                std::string err = err_msg ? err_msg : "Unknown error";
                sqlite3_free(err_msg);
                return Error{ErrorCode::DatabaseError, "Failed to drop tables: " + err};
            }
        }

        spdlog::info("Dropped {} vector-related tables", tables_to_drop.size());
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

        lock.unlock();
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
            sqlite3_bind_text(stmt, bind_idx, params.document_hash->c_str(), -1, SQLITE_TRANSIENT);
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

        lock.unlock();
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

        // Embedding as blob: skip when quantized-primary storage is enabled
        // (embedding is reconstructed from quantized sidecar on read)
        if (config_.quantized_primary_storage && !record.embedding.empty()) {
            sqlite3_bind_null(stmt_insert_, 3);
        } else {
            sqlite3_bind_blob(stmt_insert_, 3, record.embedding.data(),
                              record.embedding.size() * sizeof(float), SQLITE_TRANSIENT);
        }

        // Embedding dimension: always populate it so HNSW/search maintenance can use it
        // even when the float blob is absent in quantized-primary mode.
        size_t effective_dim =
            record.embedding_dim > 0 ? record.embedding_dim : record.embedding.size();
        sqlite3_bind_int64(stmt_insert_, 4, static_cast<int64_t>(effective_dim));

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

        // Quantized sidecar columns (packed TurboQuant codes)
        sqlite3_bind_int(stmt_insert_, 20, static_cast<int>(record.quantized.format));
        sqlite3_bind_int(stmt_insert_, 21, static_cast<int>(record.quantized.bits_per_channel));
        sqlite3_bind_int64(stmt_insert_, 22, static_cast<int64_t>(record.quantized.seed));
        if (!record.quantized.packed_codes.empty()) {
            sqlite3_bind_blob(stmt_insert_, 23, record.quantized.packed_codes.data(),
                              static_cast<int>(record.quantized.packed_codes.size()),
                              SQLITE_TRANSIENT);
        } else {
            sqlite3_bind_null(stmt_insert_, 23);
        }

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
        StmtResetGuard guard(stmt_get_rowid_);
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
        StmtResetGuard guard(stmt_select_by_chunk_id_);
        sqlite3_bind_text(stmt_select_by_chunk_id_, 1, chunk_id.c_str(), -1, SQLITE_TRANSIENT);

        if (sqlite3_step(stmt_select_by_chunk_id_) == SQLITE_ROW) {
            return recordFromStatement(stmt_select_by_chunk_id_);
        }

        return std::nullopt;
    }

    // Get vector by rowid (assumes lock held)
    std::optional<VectorRecord> getVectorByRowidUnlocked(int64_t rowid) const {
        std::lock_guard stmt_lock(stmt_mutex_);
        if (!stmt_select_by_rowid_) {
            return std::nullopt;
        }

        sqlite3_reset(stmt_select_by_rowid_);
        StmtResetGuard guard(stmt_select_by_rowid_);
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

        const char* chunk_id_txt = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
        record.chunk_id = chunk_id_txt ? chunk_id_txt : "";
        const char* doc_hash_txt = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 2));
        record.document_hash = doc_hash_txt ? doc_hash_txt : "";

        // Embedding blob — may be NULL in quantized-primary mode
        const void* blob = sqlite3_column_blob(stmt, 3);
        int blob_size = sqlite3_column_bytes(stmt, 3);
        if (blob && blob_size > 0) {
            size_t num_floats = static_cast<size_t>(blob_size) / sizeof(float);
            record.embedding.resize(num_floats);
            std::memcpy(record.embedding.data(), blob, blob_size);
            record.embedding_dim = num_floats;
        } else {
            // Quantized-primary row: embedding blob is absent; use embedding_dim column
            record.embedding_dim = static_cast<size_t>(sqlite3_column_int64(stmt, 4));
            // Note: embedding will be empty; caller (VectorDatabase) dequantizes on read
        }

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

        // Quantized sidecar columns (packed TurboQuant codes)
        record.quantized.format =
            static_cast<VectorRecord::QuantizedFormat>(sqlite3_column_int(stmt, 20));
        record.quantized.bits_per_channel = static_cast<uint8_t>(sqlite3_column_int(stmt, 21));
        record.quantized.seed = static_cast<uint64_t>(sqlite3_column_int64(stmt, 22));

        const void* qblob = sqlite3_column_blob(stmt, 23);
        int qblob_size = sqlite3_column_bytes(stmt, 23);
        if (qblob && qblob_size > 0) {
            record.quantized.packed_codes.resize(static_cast<size_t>(qblob_size));
            std::memcpy(record.quantized.packed_codes.data(), qblob, qblob_size);
        }

        return record;
    }

    // Prepare all statements
    void prepareStatements() {
        std::lock_guard stmt_lock(stmt_mutex_);
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
        sqlite3_prepare_v2(db_, kSelectFilterByRowid, -1, &stmt_filter_by_rowid_, nullptr);
    }

    // Finalize all statements
    void finalizeStatements() {
        std::lock_guard stmt_lock(stmt_mutex_);
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
        if (stmt_filter_by_rowid_)
            sqlite3_finalize(stmt_filter_by_rowid_);

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
        stmt_filter_by_rowid_ = nullptr;
    }

    std::vector<size_t> queryVectorDimsUnlocked() {
        std::vector<size_t> dims;
        sqlite3_stmt* stmt = nullptr;
        // Include both float-blob rows and quantized-primary rows (where embedding_dim > 0).
        // embedding_dim is now always populated even when embedding blob is NULL.
        const char* sql = "SELECT DISTINCT embedding_dim FROM vectors WHERE embedding_dim > 0 "
                          "ORDER BY embedding_dim";
        if (sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr) == SQLITE_OK) {
            while (sqlite3_step(stmt) == SQLITE_ROW) {
                int64_t dim = sqlite3_column_int64(stmt, 0);
                if (dim > 0) {
                    dims.push_back(static_cast<size_t>(dim));
                }
            }
            sqlite3_reset(stmt);
            tl_deferred_finalize.push_back(stmt);
        }
        return dims;
    }

    Result<void> ensureVec0TableUnlocked(size_t dim) {
        const std::string sql = "CREATE VIRTUAL TABLE IF NOT EXISTS \"" + vec0TableName(dim) +
                                "\" USING vec0(embedding float[" + std::to_string(dim) + "])";
        char* err_msg = nullptr;
        int rc = sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, &err_msg);
        if (rc != SQLITE_OK) {
            std::string err = err_msg ? err_msg : "Unknown error";
            sqlite3_free(err_msg);
            spdlog::warn("[vec0] Failed to create aux table for dim {}: {}", dim, err);
            return Error{ErrorCode::DatabaseError,
                         "Failed to create vec0 table for dim " + std::to_string(dim) + ": " + err};
        }
        return Result<void>{};
    }

    std::unique_ptr<TurboQuantMSE> makeTurboQuantForDimUnlocked(size_t dim) const {
        std::unique_ptr<TurboQuantMSE> tq;
        if (config_.enable_turboquant_storage || config_.quantized_primary_storage) {
            TurboQuantConfig cfg;
            cfg.dimension = dim;
            cfg.bits_per_channel = config_.turboquant_bits;
            cfg.seed = config_.turboquant_seed;
            tq = std::make_unique<TurboQuantMSE>(cfg);
            auto scales = loadTurboQuantPerCoordScales(db_, dim, config_.turboquant_bits,
                                                       config_.turboquant_seed);
            if (!scales.empty()) {
                tq->setPerCoordScales(std::move(scales));
            }
        }
        return tq;
    }

    std::optional<std::pair<size_t, std::vector<float>>>
    decodeVectorForDimRowUnlocked(sqlite3_stmt* stmt, size_t dim, TurboQuantMSE* tq) const {
        size_t rowid = static_cast<size_t>(sqlite3_column_int64(stmt, 0));
        const void* blob = sqlite3_column_blob(stmt, 1);
        int blob_size = sqlite3_column_bytes(stmt, 1);

        std::vector<float> embedding;
        if (blob && blob_size > 0 && (blob_size % static_cast<int>(sizeof(float))) == 0) {
            embedding.resize(static_cast<size_t>(blob_size) / sizeof(float));
            std::memcpy(embedding.data(), blob, static_cast<size_t>(blob_size));
        } else if (tq) {
            auto fmt = static_cast<VectorRecord::QuantizedFormat>(sqlite3_column_int(stmt, 2));
            if (fmt == VectorRecord::QuantizedFormat::TURBOquant_1) {
                const void* qblob = sqlite3_column_blob(stmt, 5);
                int qblob_size = sqlite3_column_bytes(stmt, 5);
                if (qblob && qblob_size > 0) {
                    std::vector<uint8_t> packed(static_cast<size_t>(qblob_size));
                    std::memcpy(packed.data(), qblob, static_cast<size_t>(qblob_size));
                    embedding = vector_utils::packedDequantizeVector(packed, dim, tq);
                }
            }
        }

        if (embedding.empty() || !isFiniteEmbedding(embedding)) {
            return std::nullopt;
        }
        return std::pair<size_t, std::vector<float>>{rowid, std::move(embedding)};
    }

    std::vector<std::pair<size_t, std::vector<float>>> queryVectorsForDimUnlocked(size_t dim) {
        std::vector<std::pair<size_t, std::vector<float>>> rows;
        const char* sql =
            "SELECT rowid, embedding, quantized_format, quantized_bits, quantized_seed, "
            "quantized_packed_codes FROM vectors WHERE embedding_dim = ? ORDER BY rowid";
        sqlite3_stmt* stmt = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr) != SQLITE_OK) {
            return rows;
        }

        auto tq = makeTurboQuantForDimUnlocked(dim);

        sqlite3_bind_int64(stmt, 1, static_cast<sqlite3_int64>(dim));
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            if (auto decoded = decodeVectorForDimRowUnlocked(stmt, dim, tq.get())) {
                rows.push_back(std::move(*decoded));
            }
        }

        sqlite3_reset(stmt);
        tl_deferred_finalize.push_back(stmt);
        return rows;
    }

    Result<void> rebuildVec0DimUnlocked(size_t dim) {
        auto table_result = ensureVec0TableUnlocked(dim);
        if (!table_result) {
            return table_result;
        }

        const std::string delete_sql = "DELETE FROM \"" + vec0TableName(dim) + "\"";
        char* err_msg = nullptr;
        int rc = sqlite3_exec(db_, delete_sql.c_str(), nullptr, nullptr, &err_msg);
        if (rc != SQLITE_OK) {
            std::string err = err_msg ? err_msg : "Unknown error";
            sqlite3_free(err_msg);
            spdlog::warn("[vec0] Failed to clear aux table for dim {}: {}", dim, err);
            return Error{ErrorCode::DatabaseError,
                         "Failed to clear vec0 table for dim " + std::to_string(dim) + ": " + err};
        }

        sqlite3_stmt* insert_stmt = nullptr;
        const std::string insert_sql =
            "INSERT INTO \"" + vec0TableName(dim) + "\" (rowid, embedding) VALUES (?, ?)";
        rc = sqlite3_prepare_v2(db_, insert_sql.c_str(), -1, &insert_stmt, nullptr);
        if (rc != SQLITE_OK) {
            spdlog::warn("[vec0] Failed to prepare aux insert for dim {}: {}", dim,
                         sqlite3_errmsg(db_));
            return Error{ErrorCode::DatabaseError,
                         "Failed to prepare vec0 insert for dim " + std::to_string(dim)};
        }

        const char* select_sql =
            "SELECT rowid, embedding, quantized_format, quantized_bits, quantized_seed, "
            "quantized_packed_codes FROM vectors "
            "WHERE (CASE WHEN embedding_dim IS NULL OR embedding_dim = 0 "
            "THEN LENGTH(embedding) / 4 ELSE embedding_dim END) = ? ORDER BY rowid";
        sqlite3_stmt* select_stmt = nullptr;
        rc = sqlite3_prepare_v2(db_, select_sql, -1, &select_stmt, nullptr);
        if (rc != SQLITE_OK) {
            sqlite3_finalize(insert_stmt);
            return Error{ErrorCode::DatabaseError,
                         "Failed to prepare vec0 source scan for dim " + std::to_string(dim)};
        }

        auto tq = makeTurboQuantForDimUnlocked(dim);
        sqlite3_bind_int64(select_stmt, 1, static_cast<sqlite3_int64>(dim));
        while (sqlite3_step(select_stmt) == SQLITE_ROW) {
            auto decoded = decodeVectorForDimRowUnlocked(select_stmt, dim, tq.get());
            if (!decoded) {
                continue;
            }

            auto& [rowid, embedding] = *decoded;
            sqlite3_bind_int64(insert_stmt, 1, static_cast<sqlite3_int64>(rowid));
            sqlite3_bind_blob(insert_stmt, 2, embedding.data(),
                              static_cast<int>(embedding.size() * sizeof(float)), SQLITE_TRANSIENT);
            rc = stepWithRetry(insert_stmt);
            if (rc != SQLITE_DONE) {
                std::string err = sqlite3_errmsg(db_);
                sqlite3_finalize(select_stmt);
                sqlite3_finalize(insert_stmt);
                spdlog::warn("[vec0] Failed to populate aux table for dim {} rowid {}: {}", dim,
                             rowid, err);
                return Error{ErrorCode::DatabaseError, "Failed to populate vec0 table for dim " +
                                                           std::to_string(dim) + ": " + err};
            }
            sqlite3_reset(insert_stmt);
            sqlite3_clear_bindings(insert_stmt);
        }
        sqlite3_finalize(select_stmt);
        sqlite3_finalize(insert_stmt);

        vec0_dirty_dims_.erase(dim);
        vec0_ready_dims_.insert(dim);
        return Result<void>{};
    }

    Result<void> rebuildVec0IndicesUnlocked(std::optional<size_t> focus_dim = std::nullopt) {
        auto dims = queryVectorDimsUnlocked();
        std::unordered_set<size_t> available_dims(dims.begin(), dims.end());

        if (focus_dim) {
            if (!available_dims.contains(*focus_dim)) {
                vec0_dirty_dims_.erase(*focus_dim);
                vec0_ready_dims_.erase(*focus_dim);
                return Result<void>{};
            }
            dims = {*focus_dim};
        }

        for (size_t dim : dims) {
            auto result = rebuildVec0DimUnlocked(dim);
            if (!result) {
                return result;
            }
        }

        return Result<void>{};
    }

    Result<void> ensureVec0ReadyUnlocked(size_t dim) {
        if (vec0_ready_dims_.contains(dim) && !vec0_dirty_dims_.contains(dim)) {
            return Result<void>{};
        }
        if (config_.suppress_search_index_builds) {
            if (vec0_ready_dims_.contains(dim)) {
                return Result<void>{};
            }
            return Error{ErrorCode::InvalidOperation,
                         "vec0 search index build suppressed by memory instrumentation profile"};
        }
        return rebuildVec0IndicesUnlocked(dim);
    }

    Result<void> warmVec0DimUnlocked(size_t dim) {
        auto ready = ensureVec0ReadyUnlocked(dim);
        if (!ready) {
            return ready;
        }

        auto count_it = query_dim_counts_.find(dim);
        if (count_it == query_dim_counts_.end() || count_it->second == 0) {
            return Result<void>{};
        }

        sqlite3_stmt* stmt = nullptr;
        const std::string sql = "SELECT rowid FROM \"" + vec0TableName(dim) +
                                "\" WHERE embedding MATCH ?1 AND k = ?2 ORDER BY distance";
        int rc = sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt, nullptr);
        if (rc != SQLITE_OK) {
            return Error{ErrorCode::DatabaseError,
                         "Failed to prepare vec0 warm query for dim " + std::to_string(dim)};
        }

        std::vector<float> zero_query(dim, 0.0f);
        sqlite3_bind_blob(stmt, 1, zero_query.data(),
                          static_cast<int>(zero_query.size() * sizeof(float)), SQLITE_TRANSIENT);
        sqlite3_bind_int64(stmt, 2, 1);
        rc = sqlite3_step(stmt);
        sqlite3_finalize(stmt);
        if (rc != SQLITE_ROW && rc != SQLITE_DONE) {
            return Error{ErrorCode::DatabaseError,
                         "Failed to warm vec0 query path for dim " + std::to_string(dim)};
        }
        return Result<void>{};
    }

    Result<void> warmVec0IndicesUnlocked() {
        auto dims = queryVectorDimsUnlocked();
        for (size_t dim : dims) {
            auto result = warmVec0DimUnlocked(dim);
            if (!result) {
                return result;
            }
        }
        return Result<void>{};
    }

    Result<void> rebuildSimeonPqDimUnlocked(size_t dim) {
        auto rows = queryVectorsForDimUnlocked(dim);
        simeon_pq_indices_.erase(dim);

        if (rows.empty()) {
            simeon_pq_dirty_dims_.erase(dim);
            simeon_pq_ready_dims_.insert(dim);
            return Result<void>{};
        }

        const std::uint32_t m = normalizedSimeonPqSubquantizers(dim);
        const std::uint32_t k = static_cast<std::uint32_t>(
            std::clamp<std::size_t>(config_.simeon_pq_centroids, 2, 256));
        simeon::PQConfig pqConfig{
            .dim = static_cast<std::uint32_t>(dim),
            .m = m,
            .k = k,
            .seed = config_.simeon_pq_seed,
        };

        auto state = std::make_unique<SimeonPqIndexState>(pqConfig);
        const std::size_t trainCap = std::min(rows.size(), config_.simeon_pq_train_limit);
        state->rowids.reserve(rows.size());

        // Pass 1: normalize in place, collect rowids, copy a bounded prefix
        // into the training buffer. Normalized embeddings stay in `rows` so
        // pass 2 can encode without re-reading SQLite.
        std::vector<float> training;
        training.reserve(trainCap * dim);
        std::size_t trainSamples = 0;
        for (auto& [rowid, embedding] : rows) {
            if (!normalizeEmbeddingInPlace(embedding)) {
                embedding.clear();
                embedding.shrink_to_fit();
                continue;
            }
            if (trainSamples < trainCap) {
                training.insert(training.end(), embedding.begin(), embedding.end());
                ++trainSamples;
            }
            state->rowids.push_back(rowid);
        }

        if (state->rowids.empty()) {
            simeon_pq_dirty_dims_.erase(dim);
            simeon_pq_ready_dims_.insert(dim);
            return Result<void>{};
        }

        try {
            if (trainSamples >= k) {
                state->pq.train(training.data(), static_cast<std::uint32_t>(trainSamples));
            } else {
                state->pq.init_random_gaussian();
            }
        } catch (const std::exception&) {
            state->pq.init_random_gaussian();
        }
        // Training buffer is no longer needed; release before encoding so
        // peak footprint during encode == size(rows) rather than size(rows)
        // + size(training).
        std::vector<float>().swap(training);

        state->codes.resize(state->rowids.size() * state->pq.m());
        std::size_t codeOffset = 0;
        // Pass 2: encode and progressively release each embedding so the
        // ~dim*4 bytes per row are returned to the allocator as we go.
        // Skips entries whose embedding was zeroed above (normalize failed).
        for (auto& [rowid, embedding] : rows) {
            (void)rowid;
            if (embedding.empty()) {
                continue;
            }
            state->pq.encode(embedding.data(), state->codes.data() + codeOffset);
            codeOffset += state->pq.m();
            std::vector<float>().swap(embedding);
        }
        std::vector<std::pair<size_t, std::vector<float>>>().swap(rows);
        state->rerank_factor = std::max<std::size_t>(1, config_.simeon_pq_rerank_factor);
        simeon_pq_indices_[dim] = std::move(state);
        simeon_pq_dirty_dims_.erase(dim);
        simeon_pq_ready_dims_.insert(dim);
        spdlog::info(
            "[SPQ] Built persisted PQ index for dim={} with {} vectors m={} k={} rerank={}", dim,
            simeon_pq_indices_[dim]->rowids.size(), m, k, config_.simeon_pq_rerank_factor);
        // Return the bulk training/encode scratch pages to the OS.
        // The SPQ rebuild churns up to ~1 GB of transient float buffers
        // (per-row vectors + codebook training set) which macOS's
        // DefaultMallocZone otherwise keeps parked in MALLOC_LARGE regions.
#if defined(__APPLE__)
        ::malloc_zone_pressure_relief(nullptr, 0);
#elif defined(__GLIBC__)
        ::malloc_trim(0);
#endif
        return Result<void>{};
    }

    Result<void> rebuildSimeonPqIndicesUnlocked(std::optional<size_t> focus_dim = std::nullopt) {
        auto dims = queryVectorDimsUnlocked();
        std::unordered_set<size_t> available_dims(dims.begin(), dims.end());
        if (focus_dim) {
            if (!available_dims.contains(*focus_dim)) {
                simeon_pq_dirty_dims_.erase(*focus_dim);
                simeon_pq_ready_dims_.erase(*focus_dim);
                simeon_pq_indices_.erase(*focus_dim);
                return Result<void>{};
            }
            dims = {*focus_dim};
        }
        for (size_t dim : dims) {
            auto result = rebuildSimeonPqDimUnlocked(dim);
            if (!result) {
                return result;
            }
        }
        return Result<void>{};
    }

    bool loadPersistedSimeonPqDimUnlocked(size_t dim) {
        std::uint32_t m = 0;
        std::uint32_t k = 0;
        std::uint64_t seed = 0;
        std::size_t rerankFactor = 0;
        bool trained = false;
        std::size_t vectorCount = 0;
        std::vector<float> codebooks;
        if (!detail::loadPersistedSimeonPqMeta(db_, dim, m, k, seed, rerankFactor, trained,
                                               vectorCount, codebooks)) {
            return false;
        }
        if (m == 0 || dim % m != 0) {
            return false;
        }
        simeon::PQConfig pqConfig{
            .dim = static_cast<std::uint32_t>(dim),
            .m = m,
            .k = k,
            .seed = seed,
        };
        auto state = std::make_unique<SimeonPqIndexState>(pqConfig);
        state->pq.import_codebooks(codebooks, trained);
        state->rerank_factor = std::max<std::size_t>(1, rerankFactor);
        if (!detail::loadPersistedSimeonPqCodes(db_, dim, m, state->rowids, state->codes) ||
            state->rowids.size() != vectorCount) {
            return false;
        }
        simeon_pq_indices_[dim] = std::move(state);
        simeon_pq_dirty_dims_.erase(dim);
        simeon_pq_ready_dims_.insert(dim);
        spdlog::info("[SPQ] Loaded persisted PQ index for dim={} with {} vectors m={} k={}", dim,
                     simeon_pq_indices_[dim]->rowids.size(), m, k);
        return true;
    }

    Result<void> saveSimeonPqDimUnlocked(size_t dim) {
        auto it = simeon_pq_indices_.find(dim);
        if (it == simeon_pq_indices_.end() || !it->second) {
            return Result<void>{};
        }
        std::string errorMessage;
        if (!detail::savePersistedSimeonPq(db_, dim, it->second->pq, config_.simeon_pq_seed,
                                           it->second->rerank_factor, it->second->rowids,
                                           it->second->codes, errorMessage)) {
            return Error{ErrorCode::DatabaseError, "Failed to persist Simeon PQ index for dim " +
                                                       std::to_string(dim) + ": " + errorMessage};
        }
        return Result<void>{};
    }

    bool hasReusablePersistedSimeonPqUnlocked(size_t dim) {
        return detail::hasPersistedSimeonPqMeta(db_, dim);
    }

    Result<void> ensureSimeonPqReadyUnlocked(size_t dim) {
        if (simeon_pq_ready_dims_.contains(dim) && !simeon_pq_dirty_dims_.contains(dim) &&
            simeon_pq_indices_.contains(dim)) {
            return Result<void>{};
        }
        if (config_.suppress_search_index_builds) {
            if (simeon_pq_indices_.contains(dim)) {
                return Result<void>{};
            }
            if (!simeon_pq_dirty_dims_.contains(dim) && loadPersistedSimeonPqDimUnlocked(dim)) {
                return Result<void>{};
            }
            spdlog::debug("[SPQ] ensure ready skipped: build suppressed by memory instrumentation "
                          "profile");
            return Result<void>{};
        }
        if (!simeon_pq_dirty_dims_.contains(dim) && loadPersistedSimeonPqDimUnlocked(dim)) {
            return Result<void>{};
        }
        return rebuildSimeonPqIndicesUnlocked(dim);
    }

    Result<std::vector<VectorRecord>>
    simeonPqSearchUnlocked(const std::vector<float>& query_embedding, size_t k,
                           float similarity_threshold) {
        if (!db_ || query_embedding.empty() || k == 0) {
            return std::vector<VectorRecord>{};
        }
        const size_t query_dim = query_embedding.size();
        auto it = simeon_pq_indices_.find(query_dim);
        if (it == simeon_pq_indices_.end() || !it->second || it->second->rowids.empty()) {
            return std::vector<VectorRecord>{};
        }

        std::vector<float> normalized_query = query_embedding;
        if (!normalizeEmbeddingInPlace(normalized_query)) {
            return std::vector<VectorRecord>{};
        }

        simeon::PQQuery pqQuery(it->second->pq, normalized_query.data());
        const size_t m = it->second->pq.m();
        const size_t approxK = std::min(it->second->rowids.size(),
                                        std::max<std::size_t>(k, k * it->second->rerank_factor));

        std::vector<std::pair<float, std::size_t>> scores;
        scores.reserve(it->second->rowids.size());
        for (std::size_t i = 0; i < it->second->rowids.size(); ++i) {
            const float approxScore = pqQuery.inner_product(it->second->codes.data() + (i * m));
            scores.emplace_back(approxScore, i);
        }
        const auto cmp = [](const auto& a, const auto& b) { return a.first > b.first; };
        if (scores.size() > approxK) {
            std::nth_element(scores.begin(), scores.begin() + approxK, scores.end(), cmp);
            scores.resize(approxK);
        }
        std::sort(scores.begin(), scores.end(), cmp);

        std::vector<VectorRecord> records;
        records.reserve(k);
        for (const auto& [approxScore, idx] : scores) {
            if (records.size() >= k) {
                break;
            }
            auto record_opt =
                getVectorByRowidUnlocked(static_cast<int64_t>(it->second->rowids[idx]));
            if (!record_opt) {
                continue;
            }
            float similarity = approxScore;
            if (!record_opt->embedding.empty()) {
                similarity = static_cast<float>(VectorDatabase::computeCosineSimilarity(
                    query_embedding, record_opt->embedding));
            }
            if (similarity < similarity_threshold) {
                continue;
            }
            record_opt->relevance_score = similarity;
            records.push_back(std::move(*record_opt));
        }
        return records;
    }

    // beginBulkLoad/finalizeBulkLoad are kept in the public API for ABI compatibility
    // and to gate future bulk-mode optimizations. With HNSW removed there is no index
    // state to pause/rebuild here; Vec0 and SimeonPQ indices are maintained lazily.
    Result<void> beginBulkLoadUnlocked() {
        std::unique_lock lock(mutex_);
        return Result<void>{};
    }

    Result<void> finalizeBulkLoadUnlocked() {
        std::unique_lock lock(mutex_);
        return Result<void>{};
    }

    void refreshQueryDimCountsUnlocked() {
        query_dim_counts_.clear();

        const char* count_sql =
            "SELECT embedding, embedding_dim, quantized_format, quantized_bits, quantized_seed, "
            "quantized_packed_codes FROM vectors";
        sqlite3_stmt* stmt = nullptr;
        if (sqlite3_prepare_v2(db_, count_sql, -1, &stmt, nullptr) != SQLITE_OK) {
            return;
        }

        std::unique_ptr<TurboQuantMSE> count_tq;
        if (config_.enable_turboquant_storage || config_.quantized_primary_storage) {
            TurboQuantConfig cfg;
            cfg.dimension = config_.embedding_dim;
            cfg.bits_per_channel = config_.turboquant_bits;
            cfg.seed = config_.turboquant_seed;
            count_tq = std::make_unique<TurboQuantMSE>(cfg);
            auto scales = loadTurboQuantPerCoordScales(
                db_, config_.embedding_dim, config_.turboquant_bits, config_.turboquant_seed);
            if (!scales.empty()) {
                count_tq->setPerCoordScales(std::move(scales));
            }
        }

        while (sqlite3_step(stmt) == SQLITE_ROW) {
            const void* blob = sqlite3_column_blob(stmt, 0);
            int blob_size = sqlite3_column_bytes(stmt, 0);
            size_t num_floats =
                (blob && blob_size > 0) ? static_cast<size_t>(blob_size) / sizeof(float) : 0;

            size_t dim = static_cast<size_t>(sqlite3_column_int64(stmt, 1));
            if (dim == 0) {
                dim = num_floats;
            } else if (num_floats > 0 && dim != num_floats) {
                dim = num_floats;
            }

            if (dim == 0) {
                continue;
            }

            std::vector<float> embedding;
            if (blob && blob_size > 0 && (blob_size % static_cast<int>(sizeof(float))) == 0) {
                embedding.resize(num_floats);
                std::memcpy(embedding.data(), blob, static_cast<size_t>(blob_size));
            } else {
                if (!count_tq) {
                    continue;
                }
                auto fmt = static_cast<VectorRecord::QuantizedFormat>(sqlite3_column_int(stmt, 2));
                if (fmt != VectorRecord::QuantizedFormat::TURBOquant_1) {
                    continue;
                }
                const void* qblob = sqlite3_column_blob(stmt, 5);
                int qblob_size = sqlite3_column_bytes(stmt, 5);
                if (!qblob || qblob_size <= 0) {
                    continue;
                }
                std::vector<uint8_t> packed(static_cast<size_t>(qblob_size));
                std::memcpy(packed.data(), qblob, static_cast<size_t>(qblob_size));
                embedding = vector_utils::packedDequantizeVector(packed, dim, count_tq.get());
            }

            if (embedding.empty() || isZeroNormEmbedding(embedding) ||
                !isFiniteEmbedding(embedding)) {
                continue;
            }

            query_dim_counts_[dim] += 1;
        }

        sqlite3_finalize(stmt);
    }

    Result<std::vector<VectorRecord>>
    bruteForceSearchUnlocked(const std::vector<float>& query_embedding, size_t k,
                             float similarity_threshold,
                             const std::optional<std::string>& document_hash,
                             const std::unordered_set<std::string>& candidate_hashes,
                             const std::map<std::string, std::string>& metadata_filters) {
        if (!db_ || query_embedding.empty() || k == 0) {
            return std::vector<VectorRecord>{};
        }

        // Extended SQL to include quantized sidecar columns for dequantization in
        // quantized-primary mode (where the float blob is NULL).
        const char* sql = R"sql(
SELECT rowid, chunk_id, document_hash, embedding, embedding_dim, content,
       start_offset, end_offset, metadata,
       model_id, model_version, embedding_version, content_hash,
       created_at, embedded_at, is_stale, level,
       source_chunk_ids, parent_document_hash, child_document_hashes,
       quantized_format, quantized_bits, quantized_seed, quantized_packed_codes
FROM vectors
WHERE embedding_dim = ?
ORDER BY rowid
)sql";

        sqlite3_stmt* stmt = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr) != SQLITE_OK) {
            return Error{ErrorCode::DatabaseError, "Failed to prepare brute-force vector search"};
        }

        sqlite3_bind_int64(stmt, 1, static_cast<sqlite3_int64>(query_embedding.size()));

        // Dequantizer for quantized-primary rows (when float blob is absent)
        std::unique_ptr<TurboQuantMSE> bf_tq;
        if (config_.enable_turboquant_storage || config_.quantized_primary_storage) {
            TurboQuantConfig cfg;
            cfg.dimension = query_embedding.size();
            cfg.bits_per_channel = config_.turboquant_bits;
            cfg.seed = config_.turboquant_seed;
            bf_tq = std::make_unique<TurboQuantMSE>(cfg);
            // Load fitted per-coord scales from DB if available
            auto scales = loadTurboQuantPerCoordScales(
                db_, query_embedding.size(), config_.turboquant_bits, config_.turboquant_seed);
            if (!scales.empty()) {
                bf_tq->setPerCoordScales(std::move(scales));
            }
        }

        std::vector<std::pair<float, VectorRecord>> scored_results;
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            auto record = recordFromStatement(stmt);

            // Dequantize if embedding is absent but quantized sidecar is present
            if (record.embedding.empty() && !record.quantized.packed_codes.empty() && bf_tq) {
                record.embedding = vector_utils::packedDequantizeVector(
                    record.quantized.packed_codes, query_embedding.size(), bf_tq.get());
            }

            if (!candidate_hashes.empty() &&
                candidate_hashes.find(record.document_hash) == candidate_hashes.end()) {
                continue;
            }
            if (document_hash && record.document_hash != *document_hash) {
                continue;
            }

            bool metadata_match = true;
            for (const auto& [key, value] : metadata_filters) {
                auto it = record.metadata.find(key);
                if (it == record.metadata.end() || it->second != value) {
                    metadata_match = false;
                    break;
                }
            }
            if (!metadata_match) {
                continue;
            }

            if (!record.embedding.empty() && record.embedding.size() != query_embedding.size()) {
                continue;
            }
            if (record.embedding.empty() && record.embedding_dim != query_embedding.size()) {
                continue; // Quantized-primary row: use embedding_dim for dimension check
            }
            if (isZeroNormEmbedding(record.embedding) || !isFiniteEmbedding(record.embedding)) {
                continue;
            }

            float similarity = static_cast<float>(
                VectorDatabase::computeCosineSimilarity(query_embedding, record.embedding));
            if (similarity < similarity_threshold) {
                continue;
            }

            record.relevance_score = similarity;
            scored_results.emplace_back(similarity, std::move(record));
        }
        sqlite3_finalize(stmt);

        std::sort(scored_results.begin(), scored_results.end(),
                  [](const auto& a, const auto& b) { return a.first > b.first; });

        size_t count = std::min(k, scored_results.size());
        std::vector<VectorRecord> records;
        records.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            records.push_back(std::move(scored_results[i].second));
        }

        return records;
    }

    Result<std::vector<VectorRecord>> vec0SearchUnlocked(const std::vector<float>& query_embedding,
                                                         size_t k, float similarity_threshold) {
        if (!db_ || query_embedding.empty() || k == 0) {
            return std::vector<VectorRecord>{};
        }

        const size_t query_dim = query_embedding.size();

        sqlite3_stmt* stmt = nullptr;
        std::string sql = "SELECT rowid, distance FROM \"" + vec0TableName(query_dim) +
                          "\" WHERE embedding MATCH ?1 AND k = ?2";
        if (config_.vec0_phss_enabled) {
            sql += " AND phss = ?3 AND phss_candidates = ?4";
        }
        sql += " ORDER BY distance";
        int rc = sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt, nullptr);
        if (rc != SQLITE_OK) {
            spdlog::warn("[vec0] Failed to prepare search query for dim {}: {}", query_dim,
                         sqlite3_errmsg(db_));
            return Error{ErrorCode::DatabaseError, "Failed to prepare vec0 search query: " +
                                                       std::string(sqlite3_errmsg(db_))};
        }

        sqlite3_bind_blob(stmt, 1, query_embedding.data(),
                          static_cast<int>(query_embedding.size() * sizeof(float)),
                          SQLITE_TRANSIENT);
        sqlite3_bind_int64(stmt, 2, static_cast<sqlite3_int64>(k));
        if (config_.vec0_phss_enabled) {
            sqlite3_bind_int(stmt, 3, 1);
            sqlite3_bind_int64(
                stmt, 4, static_cast<sqlite3_int64>(std::max(k, config_.vec0_phss_candidates)));
        }

        std::vector<VectorRecord> records;
        records.reserve(k);
        while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
            const auto node_id = sqlite3_column_int64(stmt, 0);
            auto record_opt = getVectorByRowidUnlocked(node_id);
            if (!record_opt) {
                continue;
            }

            float similarity = static_cast<float>(
                VectorDatabase::computeCosineSimilarity(query_embedding, record_opt->embedding));
            if (similarity < similarity_threshold) {
                continue;
            }

            record_opt->relevance_score = similarity;
            records.push_back(std::move(*record_opt));
            if (records.size() >= k) {
                rc = SQLITE_DONE;
                break;
            }
        }
        sqlite3_finalize(stmt);

        if (rc != SQLITE_DONE) {
            spdlog::warn("[vec0] Search iteration failed for dim {} rc={} err={}", query_dim, rc,
                         sqlite3_errmsg(db_));
            return Error{ErrorCode::DatabaseError,
                         "vec0 search iteration failed rc=" + std::to_string(rc) + ": " +
                             std::string(sqlite3_errmsg(db_))};
        }

        return records;
    }

    Result<std::vector<std::vector<VectorRecord>>>
    bruteForceSearchBatchUnlocked(const std::vector<std::vector<float>>& query_embeddings, size_t k,
                                  float similarity_threshold) {
        std::vector<std::vector<VectorRecord>> results;
        results.reserve(query_embeddings.size());
        for (const auto& query_embedding : query_embeddings) {
            auto result = bruteForceSearchUnlocked(query_embedding, k, similarity_threshold,
                                                   std::nullopt, {}, {});
            if (!result) {
                return result.error();
            }
            results.push_back(std::move(result.value()));
        }
        return results;
    }

    Config config_;
    std::string db_path_;
    sqlite3* db_ = nullptr;
    std::atomic<bool> initialized_{false};
    bool in_transaction_ = false;

    std::unordered_map<size_t, std::unique_ptr<SimeonPqIndexState>> simeon_pq_indices_;
    std::unordered_set<size_t> simeon_pq_ready_dims_;
    std::unordered_set<size_t> simeon_pq_dirty_dims_;
    std::unordered_map<size_t, size_t> query_dim_counts_;
    std::unordered_set<size_t> vec0_ready_dims_;
    std::unordered_set<size_t> vec0_dirty_dims_;

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
    sqlite3_stmt* stmt_filter_by_rowid_ = nullptr;

    // Thread safety
    mutable std::shared_mutex mutex_;
    mutable std::mutex stmt_mutex_;
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

sqlite3* SqliteVecBackend::getDbHandle() const {
    return impl_->dbHandle();
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

Result<std::unordered_map<std::string, VectorRecord>>
SqliteVecBackend::getDocumentLevelVectorsAll() {
    return impl_->getDocumentLevelVectorsAll();
}

Result<size_t>
SqliteVecBackend::forEachDocumentLevelVector(const std::function<bool(VectorRecord&&)>& visitor) {
    return impl_->forEachDocumentLevelVector(visitor);
}

Result<bool> SqliteVecBackend::hasEmbedding(const std::string& document_hash) {
    return impl_->hasEmbedding(document_hash);
}

Result<std::unordered_set<std::string>> SqliteVecBackend::getEmbeddedDocumentHashes() {
    return impl_->getEmbeddedDocumentHashes();
}

Result<size_t> SqliteVecBackend::getVectorCount() {
    return impl_->getVectorCount();
}

Result<VectorDatabaseStats> SqliteVecBackend::getStats() {
    return impl_->getStats();
}

Result<void> SqliteVecBackend::buildIndex() {
    return impl_->buildIndex();
}

Result<void> SqliteVecBackend::prepareSearchIndex() {
    return impl_->prepareSearchIndex();
}

Result<bool> SqliteVecBackend::hasReusablePersistedSearchIndex() {
    return impl_->hasReusablePersistedSearchIndex();
}

Result<void> SqliteVecBackend::optimize() {
    return impl_->optimize();
}

Result<void> SqliteVecBackend::persistIndex() {
    return impl_->persistIndex();
}

Result<void> SqliteVecBackend::checkpointWal() {
    return impl_->checkpointWal();
}

Result<void> SqliteVecBackend::beginBulkLoad() {
    return impl_->beginBulkLoad();
}

Result<void> SqliteVecBackend::finalizeBulkLoad() {
    return impl_->finalizeBulkLoad();
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

Result<void> SqliteVecBackend::persistTurboQuantPerCoordScales(size_t dim, uint8_t bits,
                                                               uint64_t seed,
                                                               const std::vector<float>& scales) {
    if (scales.size() != dim) {
        return Error{ErrorCode::InvalidArgument, "Scale dimension mismatch"};
    }
    auto* db = impl_->dbHandle();
    if (!db) {
        return Error{ErrorCode::NotInitialized, "Database not initialized"};
    }
    bool ok = saveTurboQuantPerCoordScales(db, dim, bits, seed, scales);
    if (!ok) {
        return Error{ErrorCode::DatabaseError, "Failed to persist per-coord scales"};
    }
    return {};
}

Result<void> SqliteVecBackend::persistTurboQuantFittedModel(size_t dim, uint8_t bits, uint64_t seed,
                                                            const std::vector<float>& scales,
                                                            const std::vector<float>& centroids) {
    if (scales.size() != dim) {
        return Error{ErrorCode::InvalidArgument, "Scales dimension mismatch"};
    }
    auto* db = impl_->dbHandle();
    if (!db) {
        return Error{ErrorCode::NotInitialized, "Database not initialized"};
    }
    bool ok = saveTurboQuantFittedModel(db, dim, bits, seed, scales, centroids);
    if (!ok) {
        return Error{ErrorCode::DatabaseError, "Failed to persist fitted quantizer model"};
    }
    return {};
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
