#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <fstream>
#include <yams/core/atomic_utils.h>
#include <yams/profiling.h>
#include <yams/vector/sqlite_vec_backend.h>
#include <yams/vector/vector_backend.h>
#include <yams/vector/vector_database.h>

#include <algorithm>
#include <cmath>
#include <filesystem>
#include <iomanip>
#include <mutex>
#include <random>
#include <sstream>
#include <unordered_map>
#include <unordered_set>

namespace yams::vector {

/**
 * Private implementation class (PIMPL pattern)
 * Uses vector backend abstraction for storage
 */
class VectorDatabase::Impl {
public:
    explicit Impl(const VectorDatabaseConfig& config)
        : config_(config), initialized_(false), has_error_(false) {
        // Create backend based on configuration
        // For now, always use sqlite-vec for persistence
        backend_ = std::make_unique<SqliteVecBackend>();
    }

    bool initialize() {
        std::lock_guard<std::mutex> lock(mutex_);

        if (initialized_) {
            return true;
        }

        try {
            // Do not create or touch the DB file when create_if_missing is false
            // and the target path doesn't exist.
            try {
                namespace fs = std::filesystem;
                if (!config_.create_if_missing) {
                    fs::path pth = config_.database_path;
                    if (!pth.empty() && !fs::exists(pth)) {
                        setError("Vector database does not exist and create_if_missing=false");
                        return false;
                    }
                }
            } catch (...) {
                // best-effort: continue
            }

            auto envTruthy = [](const char* env) {
                if (!env || !*env)
                    return false;
                std::string v(env);
                std::transform(v.begin(), v.end(), v.begin(), ::tolower);
                return v == "1" || v == "true" || v == "yes" || v == "on";
            };

            // Allow test/CI override to force in-memory vector DB
            std::string db_path = config_.database_path;
            bool force_in_memory = config_.use_in_memory;
            if (const char* env_mem = std::getenv("YAMS_VDB_IN_MEMORY")) {
                std::string v(env_mem);
                std::transform(v.begin(), v.end(), v.begin(),
                               [](unsigned char c) { return (char)std::tolower(c); });
                force_in_memory =
                    force_in_memory || (v == "1" || v == "true" || v == "yes" || v == "on");
            }
            if (force_in_memory) {
                db_path = ":memory:";
            }

            // Initialize backend with chosen database path
            auto result = backend_->initialize(db_path);
            if (!result) {
                setError("Failed to initialize backend: " + result.error().message);
                return false;
            }

            // Respect test/CI bypass: when sqlite-vec init is skipped, do not attempt to create
            // virtual tables (avoids 'no such module: vec0'). Allow test harnesses to override so
            // unit tests can exercise vector flows even with YAMS_DISABLE_VECTORS.
            bool vec_bypass = false;
            bool vec_bypass_protected = false; // do not auto-enable during tests if set via env
            try {
                if (const char* env = std::getenv("YAMS_DISABLE_VECTORS")) {
                    std::string v(env);
                    std::transform(v.begin(), v.end(), v.begin(), ::tolower);
                    vec_bypass = (v == "1" || v == "true" || v == "yes" || v == "on");
                    vec_bypass_protected = vec_bypass_protected || vec_bypass;
                }
                if (!vec_bypass) {
                    if (const char* env = std::getenv("YAMS_SQLITE_VEC_SKIP_INIT")) {
                        std::string v(env);
                        std::transform(v.begin(), v.end(), v.begin(), ::tolower);
                        vec_bypass = (v == "1" || v == "true" || v == "yes" || v == "on");
                        vec_bypass_protected = vec_bypass_protected || vec_bypass;
                    }
                }
                if (vec_bypass && !vec_bypass_protected) {
                    if (const char* testEnv = std::getenv("YAMS_TESTING")) {
                        std::string v(testEnv);
                        std::transform(v.begin(), v.end(), v.begin(), ::tolower);
                        if (v == "1" || v == "true" || v == "yes" || v == "on") {
                            spdlog::debug(
                                "[VectorDB] override disable flag during testing; creating tables");
                            vec_bypass = false;
                        }
                    }
                }
            } catch (...) {
            }

            // Create tables only when explicitly allowed (and not bypassed)
            if (!backend_->tablesExist()) {
                if (vec_bypass) {
                    spdlog::warn("Vector tables missing but sqlite-vec init bypassed; continuing "
                                 "without vector support");
                } else {
                    if (!config_.create_if_missing) {
                        setError("Vector database tables missing and create_if_missing=false");
                        return false;
                    }
                    auto createResult = backend_->createTables(config_.embedding_dim);
                    if (!createResult) {
                        setError("Failed to create tables: " + createResult.error().message);
                        return false;
                    }
                    spdlog::info("Created vector tables with dimension {}", config_.embedding_dim);
                }
            } else {
                // Tables exist; verify stored dimension vs configured and optionally self-heal.
                try {
                    if (auto* sqliteBackend = dynamic_cast<SqliteVecBackend*>(backend_.get())) {
                        if (auto er = sqliteBackend->ensureEmbeddingRowIdColumn(); !er) {
                            spdlog::warn("Vector DB embedding_rowid migration failed: {}",
                                         er.error().message);
                        }
                        auto storedDimOpt = sqliteBackend->getStoredEmbeddingDimension();
                        if (storedDimOpt && *storedDimOpt != config_.embedding_dim) {
                            // Check sentinel to decide whether to suppress warning and adopt stored
                            // dim
                            auto readSentinelDim =
                                [&](const std::string& dbPath) -> std::optional<size_t> {
                                try {
                                    namespace fs = std::filesystem;
                                    fs::path p =
                                        fs::path(dbPath).parent_path() / "vectors_sentinel.json";
                                    if (!fs::exists(p))
                                        return std::nullopt;
                                    std::ifstream in(p);
                                    if (!in)
                                        return std::nullopt;
                                    nlohmann::json j;
                                    in >> j;
                                    if (j.contains("embedding_dim"))
                                        return j["embedding_dim"].get<size_t>();
                                } catch (...) {
                                }
                                return std::nullopt;
                            };
                            auto sdim = readSentinelDim(config_.database_path);
                            if (sdim && *sdim == *storedDimOpt) {
                                spdlog::info("Vector DB dim matches sentinel ({}). Updating "
                                             "configured dim from {}.",
                                             *storedDimOpt, config_.embedding_dim);
                                config_.embedding_dim = *storedDimOpt;
                            } else {
                                spdlog::warn(
                                    "Vector table dimension mismatch: stored={} configured={}",
                                    *storedDimOpt, config_.embedding_dim);
                            }

                            // Heuristic: if DB is empty, or explicit env flag set, recreate schema.
                            bool allow_autofix = false;
                            try {
                                // No vectors yet? Safe to rebuild.
                                auto count = backend_->getVectorCount();
                                allow_autofix = (count && count.value() == 0);
                            } catch (...) {
                            }
                            // explicit env flag removed; rely on empty DB condition only
                            if (allow_autofix) {
                                spdlog::info("Vector DB empty or autofix enabled — recreating vec "
                                             "schema to dim {}",
                                             config_.embedding_dim);
                                auto dr = sqliteBackend->dropTables();
                                if (!dr) {
                                    spdlog::warn("Schema drop failed: {}", dr.error().message);
                                } else {
                                    auto cr = sqliteBackend->createTables(config_.embedding_dim);
                                    if (!cr) {
                                        spdlog::warn("Schema create failed: {}",
                                                     cr.error().message);
                                    } else {
                                        spdlog::info("Vector tables recreated with dimension {}",
                                                     config_.embedding_dim);
                                    }
                                }
                            }
                        }
                    }
                } catch (...) {
                    // Non-fatal: continue with existing schema
                }
            }

            initialized_ = true;
            has_error_ = false;
            return true;

        } catch (const std::exception& e) {
            setError("Initialization failed: " + std::string(e.what()));
            return false;
        }
    }

    bool isInitialized() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return initialized_;
    }

    void close() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (backend_) {
            backend_->close();
        }
        initialized_ = false;
        has_error_ = false;
        last_error_.clear();
    }

    bool createTable() {
        // Tables are created during initialization
        return backend_->tablesExist();
    }

    bool tableExists() const { return backend_->tablesExist(); }

    void dropTable() {
        std::lock_guard<std::mutex> lock(mutex_);
        // Note: We don't actually drop tables, just clear them
        // This preserves the schema but removes all data
        if (backend_->isInitialized()) {
            // Could implement a clearAll() method in backend if needed
            spdlog::warn("Drop table requested but not implemented for safety");
        }
    }

    size_t getVectorCount() const {
        // Return cached count (no DB query)
        return cachedVectorCount_.load(std::memory_order_relaxed);
    }

    void initializeCounter() {
        if (counterInitialized_.exchange(true, std::memory_order_acquire)) {
            return; // Already initialized
        }

        // Query actual count from DB once at startup
        std::lock_guard<std::mutex> lock(mutex_);
        if (auto result = backend_->getVectorCount(); result) {
            cachedVectorCount_.store(result.value(), std::memory_order_release);
            spdlog::info("VectorDatabase: initialized counter - total_vectors={}", result.value());
        }
    }

    bool insertVector(const VectorRecord& record) {
        std::lock_guard<std::mutex> lock(mutex_);

        if (!initialized_) {
            setError("Database not initialized");
            return false;
        }

        if (!utils::validateVectorRecord(record, config_.embedding_dim)) {
            setError("Invalid vector record");
            return false;
        }

        try {
            auto result = backend_->insertVector(record);
            if (!result) {
                setError("Insert failed: " + result.error().message);
                return false;
            }

            // Update component-owned metrics
            cachedVectorCount_.fetch_add(1, std::memory_order_relaxed);

            has_error_ = false;
            return true;

        } catch (const std::exception& e) {
            setError("Insert failed: " + std::string(e.what()));
            return false;
        }
    }

    bool insertVectorsBatch(const std::vector<VectorRecord>& records) {
        spdlog::debug("VectorDatabase::insertVectorsBatch called with {} records", records.size());

        if (records.empty()) {
            return true;
        }

        // Only hold mutex for validation and state check
        {
            std::lock_guard<std::mutex> lock(mutex_);

            if (!initialized_) {
                setError("Database not initialized");
                return false;
            }

            // Validate all records first; capture first mismatch for diagnostics.
            // If a mismatch is detected, attempt a one-time reconciliation with the backend's
            // stored schema dimension to protect against config drift (e.g., 384 vs 768).
            bool validated = true;
            for (const auto& record : records) {
                if (!utils::validateVectorRecord(record, config_.embedding_dim)) {
                    validated = false;
                    // Attempt reconciliation using backend's stored dimension (sqlite-vec)
                    try {
                        size_t got = record.embedding.size();
                        size_t want = config_.embedding_dim;
                        // Try to discover stored schema dimension via sqlite-vec backend
                        if (auto* sqliteBackend = dynamic_cast<SqliteVecBackend*>(backend_.get())) {
                            auto storedDimOpt = sqliteBackend->getStoredEmbeddingDimension();
                            if (storedDimOpt && *storedDimOpt > 0 && *storedDimOpt == got) {
                                // Update expected dimension to match storage schema
                                config_.embedding_dim = *storedDimOpt;
                                validated = true;
                                break; // re-run validation loop below
                            }
                        }
                        // If reconciliation not possible, report the original mismatch
                        std::stringstream ss;
                        ss << "Invalid vector record in batch (expected_dim=" << want
                           << ", got_dim=" << got << ")";
                        setError(ss.str());
                        return false;
                    } catch (...) {
                        std::stringstream ss;
                        ss << "Invalid vector record in batch (expected_dim="
                           << config_.embedding_dim << ", got_dim=" << record.embedding.size()
                           << ")";
                        setError(ss.str());
                        return false;
                    }
                }
            }
            if (!validated) {
                // Re-validate after reconciliation
                for (const auto& record : records) {
                    if (!utils::validateVectorRecord(record, config_.embedding_dim)) {
                        std::stringstream ss;
                        ss << "Invalid vector record in batch (expected_dim="
                           << config_.embedding_dim << ", got_dim=" << record.embedding.size()
                           << ")";
                        setError(ss.str());
                        return false;
                    }
                }
            }
        }

        try {
            // Don't hold our mutex while calling backend to avoid potential deadlock
            auto result = backend_->insertVectorsBatch(records);
            if (!result) {
                std::lock_guard<std::mutex> lock(mutex_);
                setError("Batch insert failed: " + result.error().message);
                return false;
            }

            // Update component-owned metrics
            cachedVectorCount_.fetch_add(records.size(), std::memory_order_relaxed);

            std::lock_guard<std::mutex> lock(mutex_);
            has_error_ = false;
            return true;

        } catch (const std::exception& e) {
            std::lock_guard<std::mutex> lock(mutex_);
            setError("Batch insert failed: " + std::string(e.what()));
            return false;
        }
    }

    bool updateVector(const std::string& chunk_id, const VectorRecord& record) {
        std::lock_guard<std::mutex> lock(mutex_);

        if (!initialized_) {
            setError("Database not initialized");
            return false;
        }

        if (!utils::validateVectorRecord(record, config_.embedding_dim)) {
            setError("Invalid vector record");
            return false;
        }

        try {
            auto result = backend_->updateVector(chunk_id, record);
            if (!result) {
                setError("Update failed: " + result.error().message);
                return false;
            }

            has_error_ = false;
            return true;

        } catch (const std::exception& e) {
            setError("Update failed: " + std::string(e.what()));
            return false;
        }
    }

    bool deleteVector(const std::string& chunk_id) {
        std::lock_guard<std::mutex> lock(mutex_);

        if (!initialized_) {
            setError("Database not initialized");
            return false;
        }

        try {
            auto result = backend_->deleteVector(chunk_id);
            if (!result) {
                setError("Delete failed: " + result.error().message);
                return false;
            }

            // Update component-owned metrics atomically (avoid underflow)
            core::decrement_if_positive(cachedVectorCount_);

            has_error_ = false;
            return true;

        } catch (const std::exception& e) {
            setError("Delete failed: " + std::string(e.what()));
            return false;
        }
    }

    bool deleteVectorsByDocument(const std::string& document_hash) {
        std::lock_guard<std::mutex> lock(mutex_);

        if (!initialized_) {
            setError("Database not initialized");
            return false;
        }

        try {
            // Count vectors before deletion to update metrics
            size_t countToDelete = 0;
            if (auto vectors = backend_->getVectorsByDocument(document_hash); vectors) {
                countToDelete = vectors.value().size();
            }

            auto result = backend_->deleteVectorsByDocument(document_hash);
            if (!result) {
                setError("Batch delete failed: " + result.error().message);
                return false;
            }

            // Update component-owned metrics atomically (avoid underflow)
            if (countToDelete > 0) {
                core::saturating_sub(cachedVectorCount_, static_cast<size_t>(countToDelete));
            }

            has_error_ = false;
            return true;

        } catch (const std::exception& e) {
            setError("Batch delete failed: " + std::string(e.what()));
            return false;
        }
    }

    std::vector<VectorRecord> searchSimilar(const std::vector<float>& query_embedding,
                                            const VectorSearchParams& params) const {
        std::lock_guard<std::mutex> lock(mutex_);

        if (!initialized_) {
            return {};
        }

        if (query_embedding.size() != config_.embedding_dim) {
            return {};
        }

        try {
            auto result = backend_->searchSimilar(query_embedding, params.k,
                                                  params.similarity_threshold, params.document_hash,
                                                  params.candidate_hashes, params.metadata_filters);
            if (!result) {
                // Can't modify has_error_ from const method
                return {};
            }

            return result.value();

        } catch (const std::exception& e) {
            // Can't modify has_error_ from const method
            return {};
        }
    }

    std::optional<VectorRecord> getVector(const std::string& chunk_id) const {
        std::lock_guard<std::mutex> lock(mutex_);

        auto result = backend_->getVector(chunk_id);
        if (!result) {
            return std::nullopt;
        }

        return result.value();
    }

    std::map<std::string, VectorRecord>
    getVectorsBatch(const std::vector<std::string>& chunk_ids) const {
        std::lock_guard<std::mutex> lock(mutex_);

        if (!initialized_) {
            return {};
        }

        auto result = backend_->getVectorsBatch(chunk_ids);
        if (!result) {
            return {};
        }

        return result.value();
    }

    std::vector<VectorRecord> getVectorsByDocument(const std::string& document_hash) const {
        std::lock_guard<std::mutex> lock(mutex_);

        auto result = backend_->getVectorsByDocument(document_hash);
        if (!result) {
            return {};
        }

        return result.value();
    }

    bool hasEmbedding(const std::string& document_hash) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto result = backend_->hasEmbedding(document_hash);
        return result && result.value();
    }

    Result<VectorDatabase::OrphanCleanupStats> cleanupOrphanRows() {
        std::lock_guard<std::mutex> lock(mutex_);

        if (!initialized_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        auto* sqliteBackend = dynamic_cast<SqliteVecBackend*>(backend_.get());
        if (!sqliteBackend) {
            return Error{ErrorCode::InvalidState, "Orphan cleanup not supported by backend"};
        }

        auto res = sqliteBackend->cleanupOrphanRows();
        if (!res) {
            setError("Orphan cleanup failed: " + res.error().message);
            return res.error();
        }

        VectorDatabase::OrphanCleanupStats stats;
        stats.metadata_removed = res.value().metadata_removed;
        stats.embeddings_removed = res.value().embeddings_removed;
        stats.metadata_backfilled = res.value().metadata_backfilled;
        return stats;
    }

    // =========================================================================
    // Entity Vector Operations
    // =========================================================================

    Result<void> insertEntityVector(const EntityVectorRecord& record) {
        std::lock_guard<std::mutex> lock(mutex_);

        if (!initialized_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        return backend_->insertEntityVector(record);
    }

    Result<void> insertEntityVectorsBatch(const std::vector<EntityVectorRecord>& records) {
        if (records.empty()) {
            return Result<void>{};
        }

        std::lock_guard<std::mutex> lock(mutex_);

        if (!initialized_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        return backend_->insertEntityVectorsBatch(records);
    }

    Result<void> updateEntityVector(const std::string& node_key, EntityEmbeddingType type,
                                    const EntityVectorRecord& record) {
        std::lock_guard<std::mutex> lock(mutex_);

        if (!initialized_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        // Delete existing and insert new (simple upsert)
        // First try to delete any existing record with this node_key + type
        // Then insert the new record
        auto deleteRes = backend_->deleteEntityVectorsByNode(node_key);
        if (!deleteRes) {
            return deleteRes;
        }

        return backend_->insertEntityVector(record);
    }

    Result<void> deleteEntityVectorsByNode(const std::string& node_key) {
        std::lock_guard<std::mutex> lock(mutex_);

        if (!initialized_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        return backend_->deleteEntityVectorsByNode(node_key);
    }

    Result<void> deleteEntityVectorsByDocument(const std::string& document_hash) {
        std::lock_guard<std::mutex> lock(mutex_);

        if (!initialized_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        return backend_->deleteEntityVectorsByDocument(document_hash);
    }

    std::vector<EntityVectorRecord> searchEntities(const std::vector<float>& query_embedding,
                                                   const EntitySearchParams& params) const {
        std::lock_guard<std::mutex> lock(mutex_);

        if (!initialized_) {
            return {};
        }

        return backend_->searchEntities(query_embedding, params);
    }

    std::vector<EntityVectorRecord> getEntityVectorsByNode(const std::string& node_key) const {
        std::lock_guard<std::mutex> lock(mutex_);

        if (!initialized_) {
            return {};
        }

        return backend_->getEntityVectorsByNode(node_key);
    }

    std::vector<EntityVectorRecord>
    getEntityVectorsByDocument(const std::string& document_hash) const {
        std::lock_guard<std::mutex> lock(mutex_);

        if (!initialized_) {
            return {};
        }

        return backend_->getEntityVectorsByDocument(document_hash);
    }

    bool hasEntityEmbedding(const std::string& node_key) const {
        std::lock_guard<std::mutex> lock(mutex_);

        if (!initialized_) {
            return false;
        }

        return backend_->hasEntityEmbedding(node_key);
    }

    size_t getEntityVectorCount() const {
        std::lock_guard<std::mutex> lock(mutex_);

        if (!initialized_) {
            return 0;
        }

        return backend_->getEntityVectorCount();
    }

    Result<void> markEntityAsStale(const std::string& node_key) {
        std::lock_guard<std::mutex> lock(mutex_);

        if (!initialized_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        return backend_->markEntityAsStale(node_key);
    }

    bool buildIndex() {
        std::lock_guard<std::mutex> lock(mutex_);

        if (!initialized_) {
            setError("Database not initialized");
            return false;
        }

        try {
            // TODO: Build actual LanceDB index (IVF_PQ)
            has_error_ = false;
            return true;

        } catch (const std::exception& e) {
            setError("Index build failed: " + std::string(e.what()));
            return false;
        }
    }

    bool optimizeIndex() {
        std::lock_guard<std::mutex> lock(mutex_);

        if (!initialized_) {
            setError("Database not initialized");
            return false;
        }

        try {
            auto result = backend_->optimize();
            if (!result) {
                setError("Backend optimize failed: " + result.error().message);
                return false;
            }
            has_error_ = false;
            return true;

        } catch (const std::exception& e) {
            setError("Index optimization failed: " + std::string(e.what()));
            return false;
        }
    }

    VectorDatabase::DatabaseStats getStats() const {
        VectorDatabase::DatabaseStats stats;

        // Get basic stats from backend - don't hold our mutex while calling backend
        // to avoid potential deadlock with backend's mutex
        auto countResult = backend_->getVectorCount();
        stats.total_vectors = countResult ? countResult.value() : 0;

        // Get stats from backend if available
        auto backendStats = backend_->getStats();
        if (backendStats) {
            stats.total_documents = backendStats.value().total_documents;
            stats.avg_embedding_magnitude = backendStats.value().avg_embedding_magnitude;
            stats.index_size_bytes = backendStats.value().index_size_bytes;
        } else {
            // Estimate if backend doesn't provide stats
            stats.total_documents = 0;
            stats.index_size_bytes = stats.total_vectors * config_.embedding_dim * sizeof(float);
        }

        stats.last_optimized = std::chrono::system_clock::now();

        return stats;
    }

    const VectorDatabaseConfig& getConfig() const { return config_; }

    std::string getLastError() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return last_error_;
    }

    bool hasError() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return has_error_;
    }

private:
    void setError(const std::string& error) const {
        last_error_ = error;
        has_error_ = true;
    }

    double computeCosineSimilarity(const std::vector<float>& a, const std::vector<float>& b) const {
        if (a.size() != b.size()) {
            return 0.0;
        }

        double dot_product = 0.0;
        double norm_a = 0.0;
        double norm_b = 0.0;

        for (size_t i = 0; i < a.size(); ++i) {
            dot_product += static_cast<double>(a[i]) * static_cast<double>(b[i]);
            norm_a += static_cast<double>(a[i]) * static_cast<double>(a[i]);
            norm_b += static_cast<double>(b[i]) * static_cast<double>(b[i]);
        }

        norm_a = std::sqrt(norm_a);
        norm_b = std::sqrt(norm_b);

        if (norm_a == 0.0 || norm_b == 0.0) {
            return 0.0;
        }

        return dot_product / (norm_a * norm_b);
    }

    VectorDatabaseConfig config_;
    std::unique_ptr<IVectorBackend> backend_;
    bool initialized_;
    mutable bool has_error_;
    mutable std::string last_error_;
    mutable std::mutex mutex_;

    // Component-owned metrics (updated on insert/delete, read by DaemonMetrics)
    mutable std::atomic<size_t> cachedVectorCount_{0};
    mutable std::atomic<bool> counterInitialized_{false};
};

// VectorDatabase implementation

VectorDatabase::VectorDatabase(const VectorDatabaseConfig& config)
    : pImpl(std::make_unique<Impl>(config)) {}

VectorDatabase::~VectorDatabase() = default;

VectorDatabase::VectorDatabase(VectorDatabase&&) noexcept = default;
VectorDatabase& VectorDatabase::operator=(VectorDatabase&&) noexcept = default;

bool VectorDatabase::initialize() {
    return pImpl->initialize();
}

bool VectorDatabase::isInitialized() const {
    return pImpl->isInitialized();
}

void VectorDatabase::initializeCounter() {
    pImpl->initializeCounter();
}

void VectorDatabase::close() {
    pImpl->close();
}

bool VectorDatabase::createTable() {
    return pImpl->createTable();
}

bool VectorDatabase::tableExists() const {
    return pImpl->tableExists();
}

void VectorDatabase::dropTable() {
    pImpl->dropTable();
}

size_t VectorDatabase::getVectorCount() const {
    return pImpl->getVectorCount();
}

bool VectorDatabase::insertVector(const VectorRecord& record) {
    YAMS_ZONE_SCOPED_N("VectorDB::insertVector");
    return pImpl->insertVector(record);
}

bool VectorDatabase::insertVectorsBatch(const std::vector<VectorRecord>& records) {
    YAMS_ZONE_SCOPED_N("VectorDB::insertVectorsBatch");
    return pImpl->insertVectorsBatch(records);
}

bool VectorDatabase::updateVector(const std::string& chunk_id, const VectorRecord& record) {
    return pImpl->updateVector(chunk_id, record);
}

bool VectorDatabase::deleteVector(const std::string& chunk_id) {
    return pImpl->deleteVector(chunk_id);
}

bool VectorDatabase::deleteVectorsByDocument(const std::string& document_hash) {
    return pImpl->deleteVectorsByDocument(document_hash);
}

std::vector<VectorRecord> VectorDatabase::searchSimilar(const std::vector<float>& query_embedding,
                                                        const VectorSearchParams& params) const {
    YAMS_ZONE_SCOPED_N("VectorDB::searchSimilar");
    return pImpl->searchSimilar(query_embedding, params);
}

std::vector<VectorRecord>
VectorDatabase::searchSimilarToDocument(const std::string& document_hash,
                                        const VectorSearchParams& params) const {
    auto document_vectors = pImpl->getVectorsByDocument(document_hash);
    spdlog::info("searchSimilarToDocument: Found {} vectors for document {}",
                 document_vectors.size(), document_hash);

    if (document_vectors.empty()) {
        return {};
    }

    spdlog::info("searchSimilarToDocument: Using embedding of size {} from first chunk",
                 document_vectors[0].embedding.size());

    // Use the first chunk's embedding as the query
    // TODO: Could implement more sophisticated document-level embeddings
    return searchSimilar(document_vectors[0].embedding, params);
}

std::vector<VectorRecord> VectorDatabase::search(const std::vector<float>& query_embedding,
                                                 const VectorSearchParams& params) const {
    YAMS_ZONE_SCOPED_N("VectorDB::search");

    // All search uses HNSW - O(log n) approximate nearest neighbor
    // HNSW achieves 100% recall vs brute-force in benchmarks (see bench_results/hnsw_*.json)
    return searchSimilar(query_embedding, params);
}

std::optional<VectorRecord> VectorDatabase::getVector(const std::string& chunk_id) const {
    return pImpl->getVector(chunk_id);
}

std::map<std::string, VectorRecord>
VectorDatabase::getVectorsBatch(const std::vector<std::string>& chunk_ids) const {
    return pImpl->getVectorsBatch(chunk_ids);
}

std::vector<VectorRecord>
VectorDatabase::getVectorsByDocument(const std::string& document_hash) const {
    return pImpl->getVectorsByDocument(document_hash);
}

bool VectorDatabase::hasEmbedding(const std::string& document_hash) const {
    return pImpl->hasEmbedding(document_hash);
}

Result<VectorDatabase::OrphanCleanupStats> VectorDatabase::cleanupOrphanRows() {
    return pImpl->cleanupOrphanRows();
}

bool VectorDatabase::buildIndex() {
    YAMS_ZONE_SCOPED_N("VectorDB::buildIndex");
    return pImpl->buildIndex();
}

bool VectorDatabase::optimizeIndex() {
    return pImpl->optimizeIndex();
}

void VectorDatabase::compactDatabase() {
    pImpl->optimizeIndex(); // For now, optimization serves as compaction
}

bool VectorDatabase::rebuildIndex() {
    return pImpl->buildIndex();
}

VectorDatabase::DatabaseStats VectorDatabase::getStats() const {
    return pImpl->getStats();
}

const VectorDatabaseConfig& VectorDatabase::getConfig() const {
    return pImpl->getConfig();
}

std::string VectorDatabase::getLastError() const {
    return pImpl->getLastError();
}

bool VectorDatabase::hasError() const {
    return pImpl->hasError();
}

Result<void> VectorDatabase::updateEmbeddings(const std::vector<VectorRecord>& records) {
    // TODO: Implement batch update of embeddings
    for (const auto& record : records) {
        if (!updateVector(record.chunk_id, record)) {
            return Error{ErrorCode::DatabaseError,
                         "Failed to update embedding: " + record.chunk_id};
        }
    }
    return {};
}

Result<std::vector<std::string>>
VectorDatabase::getStaleEmbeddings(const std::string& /*model_id*/,
                                   const std::string& /*model_version*/) {
    // TODO: Implement stale embedding detection
    return std::vector<std::string>{};
}

Result<std::vector<VectorRecord>>
VectorDatabase::getEmbeddingsByVersion(const std::string& /*model_version*/, size_t /*limit*/) {
    // TODO: Implement version filtering
    return std::vector<VectorRecord>{};
}

Result<void> VectorDatabase::markAsStale(const std::string& /*chunk_id*/) {
    // TODO: Implement stale marking
    return {};
}

Result<void> VectorDatabase::markAsDeleted(const std::string& /*chunk_id*/) {
    // TODO: Implement soft delete
    return {};
}

Result<size_t> VectorDatabase::purgeDeleted(std::chrono::hours /*age_threshold*/) {
    // TODO: Implement purge of soft-deleted records
    return size_t{0};
}

// =========================================================================
// Entity Vector Operations (Public API)
// =========================================================================

Result<void> VectorDatabase::insertEntityVector(const EntityVectorRecord& record) {
    return pImpl->insertEntityVector(record);
}

Result<void>
VectorDatabase::insertEntityVectorsBatch(const std::vector<EntityVectorRecord>& records) {
    return pImpl->insertEntityVectorsBatch(records);
}

Result<void> VectorDatabase::updateEntityVector(const std::string& node_key,
                                                EntityEmbeddingType type,
                                                const EntityVectorRecord& record) {
    return pImpl->updateEntityVector(node_key, type, record);
}

Result<void> VectorDatabase::deleteEntityVectorsByNode(const std::string& node_key) {
    return pImpl->deleteEntityVectorsByNode(node_key);
}

Result<void> VectorDatabase::deleteEntityVectorsByDocument(const std::string& document_hash) {
    return pImpl->deleteEntityVectorsByDocument(document_hash);
}

std::vector<EntityVectorRecord>
VectorDatabase::searchEntities(const std::vector<float>& query_embedding,
                               const EntitySearchParams& params) const {
    return pImpl->searchEntities(query_embedding, params);
}

std::vector<EntityVectorRecord>
VectorDatabase::getEntityVectorsByNode(const std::string& node_key) const {
    return pImpl->getEntityVectorsByNode(node_key);
}

std::vector<EntityVectorRecord>
VectorDatabase::getEntityVectorsByDocument(const std::string& document_hash) const {
    return pImpl->getEntityVectorsByDocument(document_hash);
}

bool VectorDatabase::hasEntityEmbedding(const std::string& node_key) const {
    return pImpl->hasEntityEmbedding(node_key);
}

size_t VectorDatabase::getEntityVectorCount() const {
    return pImpl->getEntityVectorCount();
}

Result<void> VectorDatabase::markEntityAsStale(const std::string& node_key) {
    return pImpl->markEntityAsStale(node_key);
}

bool VectorDatabase::isValidEmbedding(const std::vector<float>& embedding, size_t expected_dim) {
    if (embedding.size() != expected_dim) {
        return false;
    }

    // Check for NaN or infinite values
    for (float val : embedding) {
        if (!std::isfinite(val)) {
            return false;
        }
    }

    return true;
}

double VectorDatabase::computeCosineSimilarity(const std::vector<float>& a,
                                               const std::vector<float>& b) {
    if (a.size() != b.size()) {
        return 0.0;
    }

    double dot_product = 0.0;
    double norm_a = 0.0;
    double norm_b = 0.0;

    for (size_t i = 0; i < a.size(); ++i) {
        dot_product += static_cast<double>(a[i]) * static_cast<double>(b[i]);
        norm_a += static_cast<double>(a[i]) * static_cast<double>(a[i]);
        norm_b += static_cast<double>(b[i]) * static_cast<double>(b[i]);
    }

    norm_a = std::sqrt(norm_a);
    norm_b = std::sqrt(norm_b);

    if (norm_a == 0.0 || norm_b == 0.0) {
        return 0.0;
    }

    return dot_product / (norm_a * norm_b);
}

// Factory function
std::unique_ptr<VectorDatabase> createVectorDatabase(const VectorDatabaseConfig& config) {
    auto db = std::make_unique<VectorDatabase>(config);
    if (!db->initialize()) {
        return nullptr;
    }
    return db;
}

// Utility functions
namespace utils {

std::vector<float> normalizeVector(const std::vector<float>& vec) {
    double norm = 0.0;
    for (float val : vec) {
        norm += static_cast<double>(val) * static_cast<double>(val);
    }
    norm = std::sqrt(norm);

    if (norm == 0.0) {
        return vec; // Return original vector if zero
    }

    std::vector<float> normalized;
    normalized.reserve(vec.size());
    for (float val : vec) {
        normalized.push_back(static_cast<float>(static_cast<double>(val) / norm));
    }

    return normalized;
}

std::string generateChunkId(const std::string& document_hash, size_t chunk_index) {
    // Generate a deterministic but unique chunk ID
    std::stringstream ss;
    ss << document_hash << "_chunk_" << std::setfill('0') << std::setw(6) << chunk_index;
    return ss.str();
}

bool validateVectorRecord(const VectorRecord& record, size_t expected_dim) {
    if (record.chunk_id.empty() || record.document_hash.empty()) {
        return false;
    }

    if (!VectorDatabase::isValidEmbedding(record.embedding, expected_dim)) {
        return false;
    }

    if (record.start_offset > record.end_offset && record.end_offset != 0) {
        return false;
    }

    return true;
}

double similarityToDistance(double similarity) {
    // Convert cosine similarity [-1, 1] to distance [0, 2]
    return 1.0 - similarity;
}

double distanceToSimilarity(double distance) {
    // Convert distance [0, 2] to cosine similarity [-1, 1]
    return 1.0 - distance;
}

} // namespace utils

} // namespace yams::vector
