#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <fstream>
#include <yams/core/assert.hpp>
#include <yams/core/atomic_utils.h>
#include <yams/profiling.h>
#include <yams/vector/sqlite_vec_backend.h>
#include <yams/vector/turboquant.h>
#include <yams/vector/vector_backend.h>
#include <yams/vector/vector_database.h>
#include <yams/vector/vector_utils.h>

#ifdef YAMS_HAS_FAISS
#include <yams/vector/faiss_backend.h>
#endif

#include <algorithm>
#include <atomic>
#include <cmath>
#include <filesystem>
#include <iomanip>
#include <mutex>
#include <random>
#include <shared_mutex>
#include <sstream>
#include <unordered_map>
#include <unordered_set>

namespace yams::vector {

namespace {
std::optional<std::string> getenvCopy(const char* name) {
    static std::mutex envMutex;
    std::lock_guard<std::mutex> lock(envMutex);
    if (const char* value = std::getenv(name)) { // NOLINT(concurrency-mt-unsafe)
        return std::string(value);
    }
    return std::nullopt;
}
} // namespace

/**
 * Private implementation class (PIMPL pattern)
 * Uses vector backend abstraction for storage
 */
class VectorDatabase::Impl {
public:
    explicit Impl(const VectorDatabaseConfig& config)
        : config_(config), initialized_{false}, has_error_(false) {
        if (config_.backend_type == VectorBackendType::Faiss) {
#ifdef YAMS_HAS_FAISS
            FaissBackendConfig faissConfig;
            faissConfig.embeddingDim = config_.embedding_dim;
            backend_ = std::make_unique<FaissBackend>(faissConfig);
            return;
#endif
            // Fall through to SqliteVec when faiss not built
        }

        // Default: SqliteVec backend
        SqliteVecBackend::Config backend_config;
        backend_config.embedding_dim = config_.embedding_dim;
        backend_config.enable_turboquant_storage = config_.enable_turboquant_storage;
        backend_config.quantized_primary_storage = config_.quantized_primary_storage;
        backend_config.turboquant_bits = config_.turboquant_bits;
        backend_config.turboquant_seed = config_.turboquant_seed;
        backend_config.search_engine = config_.search_engine;
        backend_config.vec0_phss_enabled = config_.vec0_phss_enabled;
        backend_config.vec0_phss_candidates = config_.vec0_phss_candidates;
        backend_config.simeon_pq_subquantizers = config_.simeon_pq_subquantizers;
        backend_config.simeon_pq_centroids = config_.simeon_pq_centroids;
        backend_config.simeon_pq_train_limit = config_.simeon_pq_train_limit;
        backend_config.simeon_pq_rerank_factor = config_.simeon_pq_rerank_factor;
        backend_config.simeon_pq_seed = config_.simeon_pq_seed;
        backend_config.suppress_search_index_builds = config_.suppress_search_index_builds;
        backend_ = std::make_unique<SqliteVecBackend>(backend_config);
    }

    bool initialize() {
        std::unique_lock<std::shared_mutex> lock(mutex_);

        if (initialized_) {
            return true;
        }

        // Contract: quantized-primary storage requires a TurboQuant sidecar to be present.
        // Without it, there is no way to reconstruct embeddings from packed codes on read.
        if (config_.quantized_primary_storage && !config_.enable_turboquant_storage) {
            setError(
                "quantized_primary_storage=true requires enable_turboquant_storage=true: "
                "cannot reconstruct embeddings from packed codes without TurboQuant configuration");
            return false;
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
                spdlog::debug("[VectorDB] best-effort create_if_missing check failed");
            }

            // Allow test/CI override to force in-memory vector DB
            std::string db_path = config_.database_path;
            bool force_in_memory = config_.use_in_memory;
            if (auto env_mem = getenvCopy("YAMS_VDB_IN_MEMORY"); env_mem) {
                std::string v(*env_mem);
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
                if (auto env = getenvCopy("YAMS_DISABLE_VECTORS"); env) {
                    std::string v(*env);
                    std::transform(v.begin(), v.end(), v.begin(), ::tolower);
                    vec_bypass = (v == "1" || v == "true" || v == "yes" || v == "on");
                    vec_bypass_protected = vec_bypass_protected || vec_bypass;
                }
                if (!vec_bypass) {
                    if (auto env = getenvCopy("YAMS_SQLITE_VEC_SKIP_INIT"); env) {
                        std::string v(*env);
                        std::transform(v.begin(), v.end(), v.begin(), ::tolower);
                        vec_bypass = (v == "1" || v == "true" || v == "yes" || v == "on");
                        vec_bypass_protected = vec_bypass_protected || vec_bypass;
                    }
                }
                if (vec_bypass && !vec_bypass_protected) {
                    if (auto testEnv = getenvCopy("YAMS_TESTING"); testEnv) {
                        std::string v(*testEnv);
                        std::transform(v.begin(), v.end(), v.begin(), ::tolower);
                        if (v == "1" || v == "true" || v == "yes" || v == "on") {
                            spdlog::debug(
                                "[VectorDB] override disable flag during testing; creating tables");
                            vec_bypass = false;
                        }
                    }
                }
            } catch (...) {
                spdlog::debug("[VectorDB] env-based vector bypass probe failed");
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
                        if (auto er = sqliteBackend->ensurePersistenceSchema(); !er) {
                            spdlog::warn("Vector DB persistence schema ensure failed: {}",
                                         er.error().message);
                        }
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
                                    spdlog::debug("[VectorDB] sentinel read failed");
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
                                spdlog::debug("[VectorDB] vector count probe for autofix failed");
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
                    spdlog::debug("[VectorDB] schema verification/healing probe failed");
                }
            }

            initialized_.store(true, std::memory_order_release);
            clearError();
            return true;

        } catch (const std::exception& e) {
            setError("Initialization failed: " + std::string(e.what()));
            return false;
        }
    }

    bool isInitialized() const noexcept {
        // Lock-free: readiness flag is published with release after init, read with
        // acquire. Keeps status/health paths from blocking behind long-held rebuild
        // writer locks on mutex_.
        return initialized_.load(std::memory_order_acquire);
    }

    void close() {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        if (backend_) {
            backend_->close();
        }
        initialized_.store(false, std::memory_order_release);
        clearError();
    }

    bool createTable() {
        // Tables are created during initialization
        return backend_->tablesExist();
    }

    bool tableExists() const { return backend_->tablesExist(); }

    void dropTable() {
        std::unique_lock<std::shared_mutex> lock(mutex_);
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

    size_t getEmbeddingDim() const { return config_.embedding_dim; }

    void initializeCounter() {
        if (counterInitialized_.exchange(true, std::memory_order_acquire)) {
            return; // Already initialized
        }

        // Query actual count from DB once at startup
        std::unique_lock<std::shared_mutex> lock(mutex_);
        if (auto result = backend_->getVectorCount(); result) {
            cachedVectorCount_.store(result.value(), std::memory_order_release);
            spdlog::info("VectorDatabase: initialized counter - total_vectors={}", result.value());
        }
    }

    bool insertVector(const VectorRecord& record) {
        std::unique_lock<std::shared_mutex> lock(mutex_);

        if (!initialized_) {
            setError("Database not initialized");
            return false;
        }

        if (!utils::validateVectorRecord(record, config_.embedding_dim)) {
            setError("Invalid vector record");
            return false;
        }

        try {
            // TurboQuant compression if enabled
            VectorRecord to_insert = record;
            if (config_.enable_turboquant_storage &&
                record.quantized.format == VectorRecord::QuantizedFormat::NONE &&
                record.embedding.size() == config_.embedding_dim) {
                // Get owned TurboQuantMSE (creates/configures on first call)
                TurboQuantMSE* tq = ensureTurboQuant();
                YAMS_ASSERT(tq != nullptr,
                            "TurboQuant must be initialized before packed quantization");

                // Use packed format for storage
                to_insert.quantized.format = VectorRecord::QuantizedFormat::TURBOquant_1;
                to_insert.quantized.bits_per_channel = config_.turboquant_bits;
                to_insert.quantized.seed = config_.turboquant_seed;
                to_insert.quantized.packed_codes =
                    vector_utils::packedQuantizeVector(record.embedding, tq);
            }

            auto result = backend_->insertVector(to_insert);
            if (!result) {
                setError("Insert failed: " + result.error().message);
                return false;
            }

            // Update component-owned metrics
            cachedVectorCount_.fetch_add(1, std::memory_order_relaxed);

            clearError();
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
            std::unique_lock<std::shared_mutex> lock(mutex_);

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
            // Apply TurboQuant compression if enabled
            std::unique_lock<std::shared_mutex> compressionLock(mutex_);
            const bool useTurboQuant = config_.enable_turboquant_storage;
            const size_t embeddingDim = config_.embedding_dim;
            const auto turboquantBits = config_.turboquant_bits;
            const auto turboquantSeed = config_.turboquant_seed;
            TurboQuantMSE* tq = useTurboQuant ? ensureTurboQuant() : nullptr;

            if (useTurboQuant) {
                YAMS_ASSERT(tq != nullptr,
                            "TurboQuant must be initialized before batch packed quantization");
                // Get owned TurboQuantMSE (creates/configures on first call)
                std::vector<VectorRecord> compressed_records;
                compressed_records.reserve(records.size());

                for (const auto& record : records) {
                    VectorRecord compressed = record;
                    if (record.quantized.format == VectorRecord::QuantizedFormat::NONE &&
                        record.embedding.size() == embeddingDim) {
                        compressed.quantized.format = VectorRecord::QuantizedFormat::TURBOquant_1;
                        compressed.quantized.bits_per_channel = turboquantBits;
                        compressed.quantized.seed = turboquantSeed;
                        compressed.quantized.packed_codes =
                            vector_utils::packedQuantizeVector(record.embedding, tq);
                    }
                    compressed_records.push_back(std::move(compressed));
                }

                compressionLock.unlock();

                // Don't hold our mutex while calling backend to avoid potential deadlock
                auto result = backend_->insertVectorsBatch(compressed_records);
                if (!result) {
                    std::unique_lock<std::shared_mutex> lock(mutex_);
                    setError("Batch insert failed: " + result.error().message);
                    return false;
                }
            } else {
                compressionLock.unlock();
                // Don't hold our mutex while calling backend to avoid potential deadlock
                auto result = backend_->insertVectorsBatch(records);
                if (!result) {
                    std::unique_lock<std::shared_mutex> lock(mutex_);
                    setError("Batch insert failed: " + result.error().message);
                    return false;
                }
            }

            // Update component-owned metrics
            cachedVectorCount_.fetch_add(records.size(), std::memory_order_relaxed);

            std::unique_lock<std::shared_mutex> lock(mutex_);
            clearError();
            return true;

        } catch (const std::exception& e) {
            std::unique_lock<std::shared_mutex> lock(mutex_);
            setError("Batch insert failed: " + std::string(e.what()));
            return false;
        }
    }

    bool updateVector(const std::string& chunk_id, const VectorRecord& record) {
        std::unique_lock<std::shared_mutex> lock(mutex_);

        if (!initialized_) {
            setError("Database not initialized");
            return false;
        }

        if (!utils::validateVectorRecord(record, config_.embedding_dim)) {
            setError("Invalid vector record");
            return false;
        }

        try {
            // Apply TurboQuant compression if enabled
            VectorRecord to_update = record;
            if (config_.enable_turboquant_storage &&
                record.quantized.format == VectorRecord::QuantizedFormat::NONE &&
                record.embedding.size() == config_.embedding_dim) {
                TurboQuantMSE* tq = ensureTurboQuant();
                YAMS_ASSERT(tq != nullptr,
                            "TurboQuant must be initialized before packed quantization");
                to_update.quantized.format = VectorRecord::QuantizedFormat::TURBOquant_1;
                to_update.quantized.bits_per_channel = config_.turboquant_bits;
                to_update.quantized.seed = config_.turboquant_seed;
                to_update.quantized.packed_codes =
                    vector_utils::packedQuantizeVector(record.embedding, tq);
            }

            auto result = backend_->updateVector(chunk_id, to_update);
            if (!result) {
                setError("Update failed: " + result.error().message);
                return false;
            }

            clearError();
            return true;

        } catch (const std::exception& e) {
            setError("Update failed: " + std::string(e.what()));
            return false;
        }
    }

    bool deleteVector(const std::string& chunk_id) {
        std::unique_lock<std::shared_mutex> lock(mutex_);

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

            clearError();
            return true;

        } catch (const std::exception& e) {
            setError("Delete failed: " + std::string(e.what()));
            return false;
        }
    }

    bool deleteVectorsByDocument(const std::string& document_hash) {
        std::unique_lock<std::shared_mutex> lock(mutex_);

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

            clearError();
            return true;

        } catch (const std::exception& e) {
            setError("Batch delete failed: " + std::string(e.what()));
            return false;
        }
    }

    std::vector<VectorRecord> searchSimilar(const std::vector<float>& query_embedding,
                                            const VectorSearchParams& params) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);

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
                setError("Vector search failed: " + result.error().message);
                return {};
            }

            clearError();
            return result.value();

        } catch (const std::exception& e) {
            setError("Vector search failed: " + std::string(e.what()));
            return {};
        }
    }

    std::vector<std::vector<VectorRecord>>
    searchSimilarBatch(const std::vector<std::vector<float>>& query_embeddings,
                       const VectorSearchParams& params, size_t num_threads) const {
        auto result = searchSimilarBatchChecked(query_embeddings, params, num_threads);
        if (!result) {
            return {};
        }
        return std::move(result.value());
    }

    Result<std::vector<VectorRecord>>
    searchSimilarChecked(const std::vector<float>& query_embedding,
                         const VectorSearchParams& params) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);

        if (!initialized_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        if (query_embedding.size() != config_.embedding_dim) {
            return Error{ErrorCode::InvalidArgument,
                         "Query embedding dimension mismatch (expected=" +
                             std::to_string(config_.embedding_dim) +
                             ", got=" + std::to_string(query_embedding.size()) + ")"};
        }

        try {
            return backend_->searchSimilar(query_embedding, params.k, params.similarity_threshold,
                                           params.document_hash, params.candidate_hashes,
                                           params.metadata_filters);
        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, "Vector search failed: " + std::string(e.what())};
        }
    }

    Result<std::vector<std::vector<VectorRecord>>>
    searchSimilarBatchChecked(const std::vector<std::vector<float>>& query_embeddings,
                              const VectorSearchParams& params, size_t num_threads) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);

        if (!initialized_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }
        if (query_embeddings.empty()) {
            return std::vector<std::vector<VectorRecord>>{};
        }
        for (const auto& query : query_embeddings) {
            if (query.size() != config_.embedding_dim) {
                return Error{ErrorCode::InvalidArgument,
                             "Batch query embedding dimension mismatch (expected=" +
                                 std::to_string(config_.embedding_dim) +
                                 ", got=" + std::to_string(query.size()) + ")"};
            }
        }

        try {
            return backend_->searchSimilarBatch(query_embeddings, params.k,
                                                params.similarity_threshold, num_threads);
        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown,
                         "Batch vector search failed: " + std::string(e.what())};
        }
    }

    Result<std::optional<VectorRecord>> getVector(const std::string& chunk_id) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);

        auto result = backend_->getVector(chunk_id);
        if (!result) {
            return result;
        }

        auto& opt_record = result.value();
        if (!opt_record.has_value()) {
            return result;
        }

        // TurboQuant decompression: dequantize if enabled OR if quantized-primary storage is active
        // (embedding blob is NULL, so it needs reconstruction from packed codes)
        if ((config_.enable_turboquant_storage || config_.quantized_primary_storage) &&
            opt_record->quantized.format == VectorRecord::QuantizedFormat::TURBOquant_1 &&
            !opt_record->quantized.packed_codes.empty()) {
            // Dequantize from packed storage using owned quantizer
            TurboQuantMSE* tq = ensureTurboQuant();
            YAMS_ASSERT(tq != nullptr,
                        "TurboQuant must be initialized before packed dequantization");
            opt_record->embedding = vector_utils::packedDequantizeVector(
                opt_record->quantized.packed_codes, config_.embedding_dim, tq);
        }

        return result;
    }

    std::map<std::string, VectorRecord>
    getVectorsBatch(const std::vector<std::string>& chunk_ids) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);

        if (!initialized_) {
            return {};
        }

        auto result = backend_->getVectorsBatch(chunk_ids);
        if (!result) {
            return {};
        }

        auto records = std::move(result.value());
        if (config_.enable_turboquant_storage || config_.quantized_primary_storage) {
            TurboQuantMSE* tq = ensureTurboQuant();
            YAMS_ASSERT(tq != nullptr,
                        "TurboQuant must be initialized before packed dequantization");
            for (auto& [id, rec] : records) {
                if (rec.quantized.format == VectorRecord::QuantizedFormat::TURBOquant_1 &&
                    !rec.quantized.packed_codes.empty() && rec.embedding.empty()) {
                    rec.embedding = vector_utils::packedDequantizeVector(rec.quantized.packed_codes,
                                                                         config_.embedding_dim, tq);
                }
            }
        }

        return records;
    }

    std::vector<VectorRecord> getVectorsByDocument(const std::string& document_hash) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);

        auto result = backend_->getVectorsByDocument(document_hash);
        if (!result) {
            return {};
        }

        auto records = std::move(result.value());
        if (config_.enable_turboquant_storage || config_.quantized_primary_storage) {
            TurboQuantMSE* tq = ensureTurboQuant();
            YAMS_ASSERT(tq != nullptr,
                        "TurboQuant must be initialized before packed dequantization");
            for (auto& rec : records) {
                if (rec.quantized.format == VectorRecord::QuantizedFormat::TURBOquant_1 &&
                    !rec.quantized.packed_codes.empty() && rec.embedding.empty()) {
                    rec.embedding = vector_utils::packedDequantizeVector(rec.quantized.packed_codes,
                                                                         config_.embedding_dim, tq);
                }
            }
        }

        return records;
    }

    std::unordered_map<std::string, VectorRecord> getDocumentLevelVectorsAll() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);

        auto result = backend_->getDocumentLevelVectorsAll();
        if (!result) {
            return {};
        }

        auto records = std::move(result.value());
        if (config_.enable_turboquant_storage || config_.quantized_primary_storage) {
            TurboQuantMSE* tq = ensureTurboQuant();
            YAMS_ASSERT(tq != nullptr,
                        "TurboQuant must be initialized before packed dequantization");
            for (auto& [_hash, rec] : records) {
                if (rec.quantized.format == VectorRecord::QuantizedFormat::TURBOquant_1 &&
                    !rec.quantized.packed_codes.empty() && rec.embedding.empty()) {
                    rec.embedding = vector_utils::packedDequantizeVector(rec.quantized.packed_codes,
                                                                         config_.embedding_dim, tq);
                }
            }
        }

        return records;
    }

    Result<size_t>
    forEachDocumentLevelVector(const std::function<bool(VectorRecord&&)>& visitor) const {
        if (!visitor) {
            return Error{ErrorCode::InvalidArgument, "forEachDocumentLevelVector visitor is empty"};
        }

        std::shared_lock<std::shared_mutex> lock(mutex_);
        TurboQuantMSE* tq = (config_.enable_turboquant_storage || config_.quantized_primary_storage)
                                ? ensureTurboQuant()
                                : nullptr;
        if (config_.enable_turboquant_storage || config_.quantized_primary_storage) {
            YAMS_ASSERT(tq != nullptr,
                        "TurboQuant must be initialized before packed dequantization");
        }

        return backend_->forEachDocumentLevelVector([&](VectorRecord&& rec) {
            if ((config_.enable_turboquant_storage || config_.quantized_primary_storage) &&
                rec.quantized.format == VectorRecord::QuantizedFormat::TURBOquant_1 &&
                !rec.quantized.packed_codes.empty() && rec.embedding.empty()) {
                rec.embedding = vector_utils::packedDequantizeVector(rec.quantized.packed_codes,
                                                                     config_.embedding_dim, tq);
            }
            return visitor(std::move(rec));
        });
    }

    bool hasEmbedding(const std::string& document_hash) const {
        auto result = hasEmbeddingChecked(document_hash);
        return result && result.value();
    }

    Result<bool> hasEmbeddingChecked(const std::string& document_hash) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        if (!initialized_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }
        return backend_->hasEmbedding(document_hash);
    }

    std::unordered_set<std::string> getEmbeddedDocumentHashes() const {
        auto result = getEmbeddedDocumentHashesChecked();
        if (result) {
            return std::move(result.value());
        }
        return {};
    }

    Result<std::unordered_set<std::string>> getEmbeddedDocumentHashesChecked() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        if (!initialized_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }
        return backend_->getEmbeddedDocumentHashes();
    }

    Result<VectorDatabase::OrphanCleanupStats> cleanupOrphanRows() {
        std::unique_lock<std::shared_mutex> lock(mutex_);

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
        std::unique_lock<std::shared_mutex> lock(mutex_);

        if (!initialized_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        return backend_->insertEntityVector(record);
    }

    Result<void> insertEntityVectorsBatch(const std::vector<EntityVectorRecord>& records) {
        if (records.empty()) {
            return Result<void>{};
        }

        std::unique_lock<std::shared_mutex> lock(mutex_);

        if (!initialized_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        return backend_->insertEntityVectorsBatch(records);
    }

    Result<void> updateEntityVector(const std::string& node_key, EntityEmbeddingType type,
                                    const EntityVectorRecord& record) {
        (void)type;
        std::unique_lock<std::shared_mutex> lock(mutex_);

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
        std::unique_lock<std::shared_mutex> lock(mutex_);

        if (!initialized_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        return backend_->deleteEntityVectorsByNode(node_key);
    }

    Result<void> deleteEntityVectorsByDocument(const std::string& document_hash) {
        std::unique_lock<std::shared_mutex> lock(mutex_);

        if (!initialized_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        return backend_->deleteEntityVectorsByDocument(document_hash);
    }

    Result<size_t>
    fitAndPersistPerCoordScales(const std::vector<std::vector<float>>& training_vectors) {
        std::unique_lock<std::shared_mutex> lock(mutex_);

        if (!initialized_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        if (!config_.enable_turboquant_storage && !config_.quantized_primary_storage) {
            return Error{ErrorCode::InvalidState, "TurboQuant is not enabled"};
        }

        if (training_vectors.empty()) {
            return Error{ErrorCode::InvalidState, "No training vectors provided"};
        }

        // Fit scales
        yams::vector::TurboQuantConfig cfg;
        cfg.dimension = config_.embedding_dim;
        cfg.bits_per_channel = config_.turboquant_bits;
        cfg.seed = config_.turboquant_seed;
        yams::vector::TurboQuantMSE tq(cfg);
        tq.fitPerCoordScales(training_vectors, training_vectors.size());

        // Persist to DB using the full-fitted-model path (v2: scales + centroids)
        // Note: fitPerCoordCentroids() is available but Milestone 9 benchmarks show it does NOT
        // improve scoring on random/unit-sphere data. Keeping scales-only for now.
        auto scales = tq.perCoordScales();
        backend_->persistTurboQuantPerCoordScales(config_.embedding_dim, config_.turboquant_bits,
                                                  config_.turboquant_seed, scales);

        spdlog::info("Fitted and persisted {} per-coord scales for dim={} bits={}", scales.size(),
                     config_.embedding_dim, static_cast<int>(config_.turboquant_bits));

        return scales.size();
    }

    Result<std::vector<EntityVectorRecord>>
    searchEntitiesChecked(const std::vector<float>& query_embedding,
                          const EntitySearchParams& params) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);

        if (!initialized_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        return backend_->searchEntities(query_embedding, params);
    }

    Result<std::vector<EntityVectorRecord>>
    getEntityVectorsByNodeChecked(const std::string& node_key) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);

        if (!initialized_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        return backend_->getEntityVectorsByNode(node_key);
    }

    Result<std::vector<EntityVectorRecord>>
    getEntityVectorsByDocumentChecked(const std::string& document_hash) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);

        if (!initialized_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        return backend_->getEntityVectorsByDocument(document_hash);
    }

    Result<bool> hasEntityEmbeddingChecked(const std::string& node_key) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);

        if (!initialized_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        return backend_->hasEntityEmbedding(node_key);
    }

    Result<size_t> getEntityVectorCountChecked() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);

        if (!initialized_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        return backend_->getEntityVectorCount();
    }

    Result<void> markEntityAsStale(const std::string& node_key) {
        std::unique_lock<std::shared_mutex> lock(mutex_);

        if (!initialized_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        return backend_->markEntityAsStale(node_key);
    }

    bool buildIndex() { return buildIndexChecked().has_value(); }

    Result<void> buildIndexChecked() {
        std::unique_lock<std::shared_mutex> lock(mutex_);

        if (!initialized_) {
            setError("Database not initialized");
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        try {
            auto result = backend_->buildIndex();
            if (!result) {
                setError("Backend buildIndex failed: " + result.error().message);
                return result.error();
            }
            clearError();
            return Result<void>{};

        } catch (const std::exception& e) {
            setError("Index build failed: " + std::string(e.what()));
            return Error{ErrorCode::Unknown, "Index build failed: " + std::string(e.what())};
        }
    }

    bool prepareSearchIndex() { return prepareSearchIndexChecked().has_value(); }

    Result<void> prepareSearchIndexChecked() {
        std::unique_lock<std::shared_mutex> lock(mutex_);

        if (!initialized_) {
            setError("Database not initialized");
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        try {
            auto result = backend_->prepareSearchIndex();
            if (!result) {
                setError("Backend prepareSearchIndex failed: " + result.error().message);
                return result.error();
            }
            clearError();
            return Result<void>{};

        } catch (const std::exception& e) {
            setError("Index prepare failed: " + std::string(e.what()));
            return Error{ErrorCode::Unknown, "Index prepare failed: " + std::string(e.what())};
        }
    }

    bool hasReusablePersistedSearchIndex() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);

        if (!initialized_) {
            return false;
        }

        try {
            auto result = backend_->hasReusablePersistedSearchIndex();
            return result && result.value();
        } catch (...) {
            spdlog::debug("[VectorDB] hasReusablePersistedSearchIndex probe failed");
            return false;
        }
    }

    bool optimizeIndex() { return optimizeIndexChecked().has_value(); }

    Result<void> optimizeIndexChecked() {
        std::unique_lock<std::shared_mutex> lock(mutex_);

        if (!initialized_) {
            setError("Database not initialized");
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        try {
            auto result = backend_->optimize();
            if (!result) {
                setError("Backend optimize failed: " + result.error().message);
                return result.error();
            }
            clearError();
            return Result<void>{};

        } catch (const std::exception& e) {
            setError("Index optimization failed: " + std::string(e.what()));
            return Error{ErrorCode::Unknown, "Index optimization failed: " + std::string(e.what())};
        }
    }

    bool persistIndex() { return persistIndexChecked().has_value(); }

    Result<void> persistIndexChecked() {
        std::unique_lock<std::shared_mutex> lock(mutex_);

        if (!initialized_) {
            setError("Database not initialized");
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        try {
            auto result = backend_->persistIndex();
            if (!result) {
                setError("Backend persistIndex failed: " + result.error().message);
                return result.error();
            }
            clearError();
            return Result<void>{};

        } catch (const std::exception& e) {
            setError("Index persistence failed: " + std::string(e.what()));
            return Error{ErrorCode::Unknown, "Index persistence failed: " + std::string(e.what())};
        }
    }

    bool beginBulkLoad() { return beginBulkLoadChecked().has_value(); }

    Result<void> beginBulkLoadChecked() {
        std::unique_lock<std::shared_mutex> lock(mutex_);

        if (!initialized_) {
            setError("Database not initialized");
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        auto* sqliteBackend = dynamic_cast<SqliteVecBackend*>(backend_.get());
        if (!sqliteBackend) {
            return Result<void>{};
        }

        auto result = sqliteBackend->beginBulkLoad();
        if (!result) {
            setError("Bulk-load begin failed: " + result.error().message);
            return result.error();
        }
        clearError();
        return Result<void>{};
    }

    bool finalizeBulkLoad() { return finalizeBulkLoadChecked().has_value(); }

    Result<void> finalizeBulkLoadChecked() {
        std::unique_lock<std::shared_mutex> lock(mutex_);

        if (!initialized_) {
            setError("Database not initialized");
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        auto* sqliteBackend = dynamic_cast<SqliteVecBackend*>(backend_.get());
        if (!sqliteBackend) {
            return Result<void>{};
        }

        auto result = sqliteBackend->finalizeBulkLoad();
        if (!result) {
            setError("Bulk-load finalize failed: " + result.error().message);
            return result.error();
        }
        clearError();
        return Result<void>{};
    }

    Result<void> checkpointWal() {
        std::shared_lock<std::shared_mutex> lock(mutex_);

        if (!initialized_) {
            return Error{ErrorCode::NotInitialized, "Database not initialized"};
        }

        // Downcast to SqliteVecBackend — safe because we always create it as such
        auto* sqliteBackend = dynamic_cast<SqliteVecBackend*>(backend_.get());
        if (!sqliteBackend) {
            return Result<void>{}; // Non-SQLite backend, nothing to do
        }

        return sqliteBackend->checkpointWal();
    }

    VectorDatabase::DatabaseStats getStats() const {
        VectorDatabase::DatabaseStats stats;
        size_t embeddingDim = 0;
        {
            std::shared_lock<std::shared_mutex> lock(mutex_);
            embeddingDim = config_.embedding_dim;
        }

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
            stats.index_size_bytes = stats.total_vectors * embeddingDim * sizeof(float);
        }

        stats.last_optimized = std::chrono::system_clock::now();

        return stats;
    }

    const VectorDatabaseConfig& getConfig() const { return config_; }

    std::string getLastError() const {
        std::lock_guard<std::mutex> lock(error_mutex_);
        return last_error_;
    }

    bool hasError() const { return has_error_.load(std::memory_order_acquire); }

private:
    void setError(const std::string& error) const {
        {
            std::lock_guard<std::mutex> lock(error_mutex_);
            last_error_ = error;
        }
        has_error_.store(true, std::memory_order_release);
    }

    void clearError() const { has_error_.store(false, std::memory_order_release); }

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
    std::atomic<bool> initialized_;
    mutable std::atomic<bool> has_error_;
    mutable std::string last_error_;
    mutable std::mutex error_mutex_;
    mutable std::shared_mutex mutex_;

    // Component-owned metrics (updated on insert/delete, read by DaemonMetrics)
    mutable std::atomic<size_t> cachedVectorCount_{0};
    mutable std::atomic<bool> counterInitialized_{false};

    // Owned TurboQuantMSE instance - replaces global/TLS plumbing.
    // Initialized lazily on first use when enable_turboquant_storage is true.
    mutable std::unique_ptr<TurboQuantMSE> turboquant_;
    mutable std::mutex turboquant_mutex_;

    /**
     * Lazily initialize (or re-configure) the owned TurboQuantMSE member.
     * Returns the raw pointer for use with vector_utils overloads.
     * Idempotent: subsequent calls with the same config return the existing instance.
     */
    TurboQuantMSE* ensureTurboQuant() const {
        if (!config_.enable_turboquant_storage) {
            return nullptr;
        }

        std::lock_guard<std::mutex> lock(turboquant_mutex_);
        if (!turboquant_ || turboquant_->config().dimension != config_.embedding_dim ||
            turboquant_->config().bits_per_channel != config_.turboquant_bits) {
            TurboQuantConfig cfg;
            cfg.dimension = config_.embedding_dim;
            cfg.bits_per_channel = config_.turboquant_bits;
            cfg.seed = config_.turboquant_seed;
            turboquant_ = std::make_unique<TurboQuantMSE>(cfg);
            spdlog::debug("VectorDB: TurboQuant configured: dim={} bits={}", cfg.dimension,
                          cfg.bits_per_channel);
        }
        return turboquant_.get();
    }
};

// VectorDatabase implementation

VectorDatabase::VectorDatabase(const VectorDatabaseConfig& config)
    : pImpl(std::make_unique<Impl>(config)) {}

VectorDatabase::~VectorDatabase() {
    if (pImpl) {
        pImpl->close();
    }
}

VectorDatabase::VectorDatabase(VectorDatabase&&) noexcept = default;
VectorDatabase& VectorDatabase::operator=(VectorDatabase&&) noexcept = default;

bool VectorDatabase::initialize() {
    return initializeChecked().has_value();
}

Result<void> VectorDatabase::initializeChecked() {
    if (pImpl->initialize()) {
        return Result<void>{};
    }
    const auto message = pImpl->getLastError();
    return Error{ErrorCode::Unknown,
                 message.empty() ? "Vector database initialization failed" : message};
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
    return createTableChecked().has_value();
}

Result<void> VectorDatabase::createTableChecked() {
    if (pImpl->createTable()) {
        return Result<void>{};
    }
    const auto message = pImpl->getLastError();
    return Error{ErrorCode::Unknown, message.empty() ? "Vector table creation failed" : message};
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

size_t VectorDatabase::getEmbeddingDim() const {
    return pImpl->getEmbeddingDim();
}

bool VectorDatabase::insertVector(const VectorRecord& record) {
    return insertVectorChecked(record).has_value();
}

Result<void> VectorDatabase::insertVectorChecked(const VectorRecord& record) {
    YAMS_ZONE_SCOPED_N("VectorDB::insertVector");
    YAMS_PLOT("vector_db::insert_embedding_dim", static_cast<int64_t>(record.embedding.size()));
    if (pImpl->insertVector(record)) {
        return Result<void>{};
    }
    const auto message = pImpl->getLastError();
    return Error{ErrorCode::Unknown, message.empty() ? "Vector insert failed" : message};
}

bool VectorDatabase::insertVectorsBatch(const std::vector<VectorRecord>& records) {
    return insertVectorsBatchChecked(records).has_value();
}

Result<void> VectorDatabase::insertVectorsBatchChecked(const std::vector<VectorRecord>& records) {
    YAMS_ZONE_SCOPED_N("VectorDB::insertVectorsBatch");
    YAMS_PLOT("vector_db::insert_batch_size", static_cast<int64_t>(records.size()));
    if (pImpl->insertVectorsBatch(records)) {
        return Result<void>{};
    }
    const auto message = pImpl->getLastError();
    return Error{ErrorCode::Unknown, message.empty() ? "Vector batch insert failed" : message};
}

bool VectorDatabase::updateVector(const std::string& chunk_id, const VectorRecord& record) {
    return updateVectorChecked(chunk_id, record).has_value();
}

Result<void> VectorDatabase::updateVectorChecked(const std::string& chunk_id,
                                                 const VectorRecord& record) {
    if (pImpl->updateVector(chunk_id, record)) {
        return Result<void>{};
    }
    const auto message = pImpl->getLastError();
    return Error{ErrorCode::Unknown, message.empty() ? "Vector update failed" : message};
}

bool VectorDatabase::deleteVector(const std::string& chunk_id) {
    return deleteVectorChecked(chunk_id).has_value();
}

Result<void> VectorDatabase::deleteVectorChecked(const std::string& chunk_id) {
    if (pImpl->deleteVector(chunk_id)) {
        return Result<void>{};
    }
    const auto message = pImpl->getLastError();
    return Error{ErrorCode::Unknown, message.empty() ? "Vector delete failed" : message};
}

bool VectorDatabase::deleteVectorsByDocument(const std::string& document_hash) {
    return deleteVectorsByDocumentChecked(document_hash).has_value();
}

Result<void> VectorDatabase::deleteVectorsByDocumentChecked(const std::string& document_hash) {
    if (pImpl->deleteVectorsByDocument(document_hash)) {
        return Result<void>{};
    }
    const auto message = pImpl->getLastError();
    return Error{ErrorCode::Unknown, message.empty() ? "Document vector delete failed" : message};
}

std::vector<VectorRecord> VectorDatabase::searchSimilar(const std::vector<float>& query_embedding,
                                                        const VectorSearchParams& params) const {
    auto results = searchSimilarChecked(query_embedding, params);
    if (!results) {
        return {};
    }
    return std::move(results.value());
}

Result<std::vector<VectorRecord>>
VectorDatabase::searchSimilarChecked(const std::vector<float>& query_embedding,
                                     const VectorSearchParams& params) const {
    YAMS_ZONE_SCOPED_N("VectorDB::searchSimilar");
    YAMS_PLOT("vector_db::search_query_dim", static_cast<int64_t>(query_embedding.size()));
    YAMS_PLOT("vector_db::search_k", static_cast<int64_t>(params.k));
    auto results = pImpl->searchSimilarChecked(query_embedding, params);
    if (results) {
        YAMS_PLOT("vector_db::search_results", static_cast<int64_t>(results->size()));
    }
    return results;
}

std::vector<std::vector<VectorRecord>>
VectorDatabase::searchSimilarBatch(const std::vector<std::vector<float>>& query_embeddings,
                                   const VectorSearchParams& params, size_t num_threads) const {
    auto results = searchSimilarBatchChecked(query_embeddings, params, num_threads);
    if (!results) {
        return {};
    }
    return std::move(results.value());
}

Result<std::vector<std::vector<VectorRecord>>>
VectorDatabase::searchSimilarBatchChecked(const std::vector<std::vector<float>>& query_embeddings,
                                          const VectorSearchParams& params,
                                          size_t num_threads) const {
    YAMS_ZONE_SCOPED_N("VectorDB::searchSimilarBatch");
    YAMS_PLOT("vector_db::batch_search_queries", static_cast<int64_t>(query_embeddings.size()));
    YAMS_PLOT("vector_db::batch_search_k", static_cast<int64_t>(params.k));
    auto results = pImpl->searchSimilarBatchChecked(query_embeddings, params, num_threads);
    if (results) {
        YAMS_PLOT("vector_db::batch_search_result_sets", static_cast<int64_t>(results->size()));
    }
    return results;
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
    YAMS_PLOT("vector_db::search_dispatch_query_dim", static_cast<int64_t>(query_embedding.size()));
    YAMS_PLOT("vector_db::search_dispatch_k", static_cast<int64_t>(params.k));

    // All search dispatches through the configured search engine (vec0 or Simeon PQ).
    auto results = searchSimilar(query_embedding, params);
    YAMS_PLOT("vector_db::search_dispatch_results", static_cast<int64_t>(results.size()));
    return results;
}

std::optional<VectorRecord> VectorDatabase::getVector(const std::string& chunk_id) const {
    YAMS_ZONE_SCOPED_N("VectorDB::getVector");
    auto result = pImpl->getVector(chunk_id);
    YAMS_PLOT("vector_db::get_vector_found", result.has_value() ? 1 : 0);
    if (!result) {
        return std::nullopt;
    }
    return result.value();
}

std::map<std::string, VectorRecord>
VectorDatabase::getVectorsBatch(const std::vector<std::string>& chunk_ids) const {
    YAMS_ZONE_SCOPED_N("VectorDB::getVectorsBatch");
    YAMS_PLOT("vector_db::get_vectors_batch_requested", static_cast<int64_t>(chunk_ids.size()));
    auto result = pImpl->getVectorsBatch(chunk_ids);
    YAMS_PLOT("vector_db::get_vectors_batch_found", static_cast<int64_t>(result.size()));
    return result;
}

std::vector<VectorRecord>
VectorDatabase::getVectorsByDocument(const std::string& document_hash) const {
    YAMS_ZONE_SCOPED_N("VectorDB::getVectorsByDocument");
    auto result = pImpl->getVectorsByDocument(document_hash);
    YAMS_PLOT("vector_db::get_vectors_by_document_count", static_cast<int64_t>(result.size()));
    return result;
}

std::unordered_map<std::string, VectorRecord> VectorDatabase::getDocumentLevelVectorsAll() const {
    YAMS_ZONE_SCOPED_N("VectorDB::getDocumentLevelVectorsAll");
    auto result = pImpl->getDocumentLevelVectorsAll();
    YAMS_PLOT("vector_db::document_level_vectors_all", static_cast<int64_t>(result.size()));
    return result;
}

Result<size_t> VectorDatabase::forEachDocumentLevelVector(
    const std::function<bool(VectorRecord&&)>& visitor) const {
    YAMS_ZONE_SCOPED_N("VectorDB::forEachDocumentLevelVector");
    auto result = pImpl->forEachDocumentLevelVector(visitor);
    if (result) {
        YAMS_PLOT("vector_db::document_level_vectors_streamed",
                  static_cast<int64_t>(result.value()));
    }
    return result;
}

bool VectorDatabase::hasEmbedding(const std::string& document_hash) const {
    auto result = hasEmbeddingChecked(document_hash);
    return result && result.value();
}

Result<bool> VectorDatabase::hasEmbeddingChecked(const std::string& document_hash) const {
    YAMS_ZONE_SCOPED_N("VectorDB::hasEmbedding");
    auto has = pImpl->hasEmbeddingChecked(document_hash);
    if (has) {
        YAMS_PLOT("vector_db::has_embedding", has.value() ? 1 : 0);
    }
    return has;
}

std::unordered_set<std::string> VectorDatabase::getEmbeddedDocumentHashes() const {
    auto result = getEmbeddedDocumentHashesChecked();
    if (!result) {
        return {};
    }
    return std::move(result.value());
}

Result<std::unordered_set<std::string>> VectorDatabase::getEmbeddedDocumentHashesChecked() const {
    YAMS_ZONE_SCOPED_N("VectorDB::getEmbeddedDocumentHashes");
    auto hashes = pImpl->getEmbeddedDocumentHashesChecked();
    if (hashes) {
        YAMS_PLOT("vector_db::embedded_document_hashes", static_cast<int64_t>(hashes->size()));
    }
    return hashes;
}

Result<VectorDatabase::OrphanCleanupStats> VectorDatabase::cleanupOrphanRows() {
    return pImpl->cleanupOrphanRows();
}

bool VectorDatabase::buildIndex() {
    return buildIndexChecked().has_value();
}

Result<void> VectorDatabase::buildIndexChecked() {
    YAMS_ZONE_SCOPED_N("VectorDB::buildIndex");
    return pImpl->buildIndexChecked();
}

bool VectorDatabase::prepareSearchIndex() {
    return prepareSearchIndexChecked().has_value();
}

Result<void> VectorDatabase::prepareSearchIndexChecked() {
    YAMS_ZONE_SCOPED_N("VectorDB::prepareSearchIndex");
    return pImpl->prepareSearchIndexChecked();
}

bool VectorDatabase::hasReusablePersistedSearchIndex() const {
    return pImpl->hasReusablePersistedSearchIndex();
}

bool VectorDatabase::optimizeIndex() {
    return optimizeIndexChecked().has_value();
}

Result<void> VectorDatabase::optimizeIndexChecked() {
    return pImpl->optimizeIndexChecked();
}

bool VectorDatabase::persistIndex() {
    return persistIndexChecked().has_value();
}

Result<void> VectorDatabase::persistIndexChecked() {
    return pImpl->persistIndexChecked();
}

void VectorDatabase::compactDatabase() {
    pImpl->optimizeIndex(); // For now, optimization serves as compaction
}

bool VectorDatabase::rebuildIndex() {
    return pImpl->buildIndex();
}

bool VectorDatabase::beginBulkLoad() {
    return beginBulkLoadChecked().has_value();
}

Result<void> VectorDatabase::beginBulkLoadChecked() {
    return pImpl->beginBulkLoadChecked();
}

bool VectorDatabase::finalizeBulkLoad() {
    return finalizeBulkLoadChecked().has_value();
}

Result<void> VectorDatabase::finalizeBulkLoadChecked() {
    return pImpl->finalizeBulkLoadChecked();
}

Result<void> VectorDatabase::checkpointWal() {
    return pImpl->checkpointWal();
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
    YAMS_ZONE_SCOPED_N("VectorDB::insertEntityVector");
    YAMS_PLOT("vector_db::entity_insert_embedding_dim",
              static_cast<int64_t>(record.embedding.size()));
    return pImpl->insertEntityVector(record);
}

Result<void>
VectorDatabase::insertEntityVectorsBatch(const std::vector<EntityVectorRecord>& records) {
    YAMS_ZONE_SCOPED_N("VectorDB::insertEntityVectorsBatch");
    YAMS_PLOT("vector_db::entity_insert_batch_size", static_cast<int64_t>(records.size()));
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
    auto results = searchEntitiesChecked(query_embedding, params);
    if (!results) {
        return {};
    }
    return std::move(results.value());
}

Result<std::vector<EntityVectorRecord>>
VectorDatabase::searchEntitiesChecked(const std::vector<float>& query_embedding,
                                      const EntitySearchParams& params) const {
    YAMS_ZONE_SCOPED_N("VectorDB::searchEntities");
    YAMS_PLOT("vector_db::entity_search_query_dim", static_cast<int64_t>(query_embedding.size()));
    YAMS_PLOT("vector_db::entity_search_k", static_cast<int64_t>(params.k));
    auto results = pImpl->searchEntitiesChecked(query_embedding, params);
    if (results) {
        YAMS_PLOT("vector_db::entity_search_results", static_cast<int64_t>(results->size()));
    }
    return results;
}

std::vector<EntityVectorRecord>
VectorDatabase::getEntityVectorsByNode(const std::string& node_key) const {
    auto results = getEntityVectorsByNodeChecked(node_key);
    if (!results) {
        return {};
    }
    return std::move(results.value());
}

Result<std::vector<EntityVectorRecord>>
VectorDatabase::getEntityVectorsByNodeChecked(const std::string& node_key) const {
    YAMS_ZONE_SCOPED_N("VectorDB::getEntityVectorsByNode");
    auto results = pImpl->getEntityVectorsByNodeChecked(node_key);
    if (results) {
        YAMS_PLOT("vector_db::entity_vectors_by_node", static_cast<int64_t>(results->size()));
    }
    return results;
}

std::vector<EntityVectorRecord>
VectorDatabase::getEntityVectorsByDocument(const std::string& document_hash) const {
    auto results = getEntityVectorsByDocumentChecked(document_hash);
    if (!results) {
        return {};
    }
    return std::move(results.value());
}

Result<std::vector<EntityVectorRecord>>
VectorDatabase::getEntityVectorsByDocumentChecked(const std::string& document_hash) const {
    YAMS_ZONE_SCOPED_N("VectorDB::getEntityVectorsByDocument");
    auto results = pImpl->getEntityVectorsByDocumentChecked(document_hash);
    if (results) {
        YAMS_PLOT("vector_db::entity_vectors_by_document", static_cast<int64_t>(results->size()));
    }
    return results;
}

bool VectorDatabase::hasEntityEmbedding(const std::string& node_key) const {
    auto result = hasEntityEmbeddingChecked(node_key);
    return result && result.value();
}

Result<bool> VectorDatabase::hasEntityEmbeddingChecked(const std::string& node_key) const {
    return pImpl->hasEntityEmbeddingChecked(node_key);
}

size_t VectorDatabase::getEntityVectorCount() const {
    auto result = getEntityVectorCountChecked();
    return result ? result.value() : 0;
}

Result<size_t> VectorDatabase::getEntityVectorCountChecked() const {
    return pImpl->getEntityVectorCountChecked();
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
