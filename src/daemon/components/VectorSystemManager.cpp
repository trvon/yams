// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <yams/core/assert.hpp>
#include <yams/daemon/components/ConfigResolver.h>
#include <yams/daemon/components/ServiceManagerFsm.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/VectorSystemManager.h>
#include <yams/daemon/resource/model_provider.h>
#include <yams/vector/dim_resolver.h>
#include <yams/vector/vector_database.h>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdlib>
#include <optional>
#include <string_view>
#include <thread>

#ifdef _WIN32
#include <io.h>
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

namespace yams::daemon {

namespace {

template <typename T> std::optional<T> parseUnsigned(std::string_view raw) {
    if (raw.empty() || raw.front() == '-') {
        return std::nullopt;
    }
    T value{};
    const char* begin = raw.data();
    const char* end = begin + raw.size();
    auto [ptr, ec] = std::from_chars(begin, end, value);
    if (ec != std::errc{} || ptr != end) {
        return std::nullopt;
    }
    return value;
}

long long currentProcessId() noexcept {
#ifdef _WIN32
    return static_cast<long long>(::_getpid());
#else
    return static_cast<long long>(::getpid());
#endif
}

std::optional<int> parseInt(std::string_view raw) {
    if (raw.empty()) {
        return std::nullopt;
    }
    int value{};
    const char* begin = raw.data();
    const char* end = begin + raw.size();
    auto [ptr, ec] = std::from_chars(begin, end, value);
    if (ec != std::errc{} || ptr != end) {
        return std::nullopt;
    }
    return value;
}

std::optional<std::string> getenvCopy(const char* name) {
    static std::mutex envMutex;
    std::lock_guard<std::mutex> lock(envMutex);
    if (const char* value = std::getenv(name)) { // NOLINT(concurrency-mt-unsafe)
        return std::string(value);
    }
    return std::nullopt;
}

bool isTruthyValue(std::string_view raw) {
    if (raw.empty()) {
        return false;
    }

    std::string normalized(raw);
    std::transform(normalized.begin(), normalized.end(), normalized.begin(), [](char c) {
        return static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    });
    return normalized == "1" || normalized == "true" || normalized == "yes" || normalized == "on";
}

void markVectorInitAttempted(StateComponent* state, bool attempted) noexcept {
    if (!state) {
        return;
    }
    try {
        state->readiness.vectorDbInitAttempted = attempted;
    } catch (...) {
        spdlog::debug("[VectorInit] failed to publish initAttempted readiness state");
    }
}

} // namespace

VectorSystemManager::VectorSystemManager(Dependencies deps) : deps_(std::move(deps)) {}

VectorSystemManager::~VectorSystemManager() {
    shutdown();
}

Result<void> VectorSystemManager::initialize() {
    // Initialization is deferred to initializeOnce() which takes dataDir
    return Result<void>{};
}

void VectorSystemManager::shutdown() {
    auto existing = std::atomic_load_explicit(&vectorDatabase_, std::memory_order_acquire);
    if (existing) {
        spdlog::debug("[VectorSystemManager] Shutting down vector database");
        try {
            existing->close();
        } catch (const std::exception& e) {
            spdlog::debug("[VectorSystemManager] Vector database close failed: {}", e.what());
        } catch (...) {
            spdlog::debug("[VectorSystemManager] Vector database close failed: unknown exception");
        }
        std::atomic_store_explicit(&vectorDatabase_, std::shared_ptr<vector::VectorDatabase>{},
                                   std::memory_order_release);
    }
}

Result<bool> VectorSystemManager::initializeOnce(const std::filesystem::path& dataDir) {
    const auto clearRetryableInitAttempt = [this]() {
        markVectorInitAttempted(deps_.state, false);
        initAttempted_.store(false, std::memory_order_release);
    };

    // In-process guard (first-wins)
    if (initAttempted_.exchange(true, std::memory_order_acq_rel)) {
        spdlog::debug("[VectorInit] skipped (already attempted in this process)");
        markVectorInitAttempted(deps_.state, true);
        return Result<bool>(false);
    }

    auto existing = std::atomic_load_explicit(&vectorDatabase_, std::memory_order_acquire);
    if (existing) {
        spdlog::debug("[VectorInit] vectorDatabase_ already present; nothing to do");
        return Result<bool>(false);
    }

    namespace fs = std::filesystem;
    vector::VectorDatabaseConfig cfg;
    cfg.database_path = (dataDir / "vectors.db").string();

    // Resolve vector backend selection via ConfigResolver (TOML + env).
    auto backendPolicy = ConfigResolver::resolveVectorBackendPolicy();
    if (backendPolicy.backend && *backendPolicy.backend == "faiss") {
        cfg.backend_type = vector::VectorBackendType::Faiss;
        spdlog::info("[VectorInit] backend=faiss via ConfigResolver");
    }
    bool exists = fs::exists(cfg.database_path);
    cfg.create_if_missing = true;

    // Resolve embedding dimension with precedence:
    // 1. Existing DB DDL (existing data takes precedence)
    // 2. getEmbeddingDimension callback (actual model dimension via ServiceManager)
    // 3. Provider directly (if weak_ptr is set)
    // 4. Config file / env (fallback only)
    std::optional<size_t> dim;

    // 1. Existing DB DDL
    if (exists) {
        try {
            auto ddlDim = ConfigResolver::readDbEmbeddingDim(cfg.database_path);
            if (ddlDim && *ddlDim > 0) {
                dim = ddlDim;
                spdlog::info("[VectorInit] probe: ddl dim={}", *ddlDim);
            }
        } catch (...) {
            spdlog::debug("[VectorInit] failed reading embedding dim from existing DB DDL");
        }
    }

    // 2. Embedding generator callback (ServiceManager::getEmbeddingDimension)
    // This is preferred over config because it returns the actual model dimension
    if (!dim && deps_.getEmbeddingDimension) {
        try {
            size_t g = deps_.getEmbeddingDimension();
            if (g > 0) {
                dim = g;
                spdlog::info("[VectorInit] probe: generator dim={}", g);
            }
        } catch (...) {
            spdlog::debug("[VectorInit] getEmbeddingDimension callback failed");
        }
    }

    // 3. Ask provider directly (if weak_ptr is set)
    auto modelProvider = deps_.modelProvider.lock();
    // Training-free providers (e.g. simeon) don't use named models; ask for the
    // fixed output dimension directly before falling through to the name-based
    // resolution paths below.
    if (!dim && modelProvider && modelProvider->isAvailable() && modelProvider->isTrainingFree()) {
        try {
            size_t prov = modelProvider->getEmbeddingDim("");
            if (prov > 0) {
                dim = prov;
                spdlog::info("[VectorInit] using training-free provider dim={} ({})", *dim,
                             modelProvider->getProviderName());
            }
        } catch (...) {
            spdlog::debug("[VectorInit] training-free provider dimension probe failed");
        }
    }
    if (!dim && deps_.resolvePreferredModel) {
        try {
            std::string preferred = deps_.resolvePreferredModel();
            if (!preferred.empty() && modelProvider && modelProvider->isAvailable()) {
                size_t prov = modelProvider->getEmbeddingDim(preferred);
                if (prov > 0) {
                    dim = prov;
                    spdlog::info("[VectorInit] using provider dim={} from '{}'", *dim, preferred);
                }
            }
        } catch (...) {
            spdlog::debug("[VectorInit] preferred model provider dimension probe failed");
        }
    }

    // 4. Config file / env (fallback only when model dimension unknown)
    if (!dim) {
        auto cfgPath = ConfigResolver::resolveDefaultConfigPath();
        if (!cfgPath.empty()) {
            try {
                auto kv = ConfigResolver::parseSimpleTomlFlat(cfgPath);
                auto it = kv.find("embeddings.embedding_dim");
                if (it != kv.end() && !it->second.empty()) {
                    if (auto parsed = parseUnsigned<size_t>(it->second)) {
                        dim = parsed;
                        spdlog::info("[VectorInit] probe: config dim={}", *dim);
                    }
                }
            } catch (...) {
                spdlog::debug("[VectorInit] config embedding dim probe failed");
            }
        }

        if (!dim) {
            if (auto envd = getenvCopy("YAMS_EMBED_DIM"); envd) {
                if (auto parsed = parseUnsigned<size_t>(*envd)) {
                    dim = parsed;
                }
            }
        }
    }

    // 5. Fallback: try centralized model name/config lookup for preferred model
    if (!dim && deps_.resolvePreferredModel) {
        try {
            std::string preferred = deps_.resolvePreferredModel();
            if (!preferred.empty()) {
                namespace fs = std::filesystem;
                fs::path modelsDir =
                    fs::path(cfg.database_path).parent_path() / "models" / preferred;
                // Try config file first (most accurate)
                if (auto cfgDim = vector::dimres::dim_from_model_config(modelsDir)) {
                    dim = cfgDim;
                    spdlog::info("[VectorInit] probe: model config dim={} for '{}'", *dim,
                                 preferred);
                } else if (auto nameDim = vector::dimres::dim_from_model_name(preferred)) {
                    // Name-based heuristic (jina, nomic, minilm, etc.)
                    dim = nameDim;
                    spdlog::info("[VectorInit] probe: model name heuristic dim={} for '{}'", *dim,
                                 preferred);
                }
            }
        } catch (...) {
            spdlog::debug("[VectorInit] preferred model config/name heuristic probe failed");
        }
    }

    // 6. Last resort: try YAMS_PREFERRED_MODEL env var directly for model name heuristic
    // This handles cases where resolvePreferredModel callback isn't ready yet
    if (!dim) {
        if (auto envModel = getenvCopy("YAMS_PREFERRED_MODEL"); envModel) {
            std::string modelName(*envModel);
            if (!modelName.empty()) {
                if (auto nameDim = vector::dimres::dim_from_model_name(modelName)) {
                    dim = nameDim;
                    spdlog::info("[VectorInit] probe: env model name heuristic dim={} for '{}'",
                                 *dim, modelName);
                }
            }
        }
    }

    // 7. Final fallback: check preload_models from config for model name heuristic
    if (!dim) {
        auto cfgPath = ConfigResolver::resolveDefaultConfigPath();
        if (!cfgPath.empty()) {
            try {
                auto kv = ConfigResolver::parseSimpleTomlFlat(cfgPath);
                // Check preload_models list for known model names
                auto preload = kv.find("daemon.models.preload_models");
                if (preload != kv.end() && !preload->second.empty()) {
                    // Try known models in order of commonality
                    const std::vector<std::string> knownModels = {
                        "all-MiniLM-L6-v2",    "all-mpnet-base-v2", "jina-embeddings-v2-small-en",
                        "nomic-embed-text-v1", "bge-small-en-v1.5", "bge-base-en-v1.5"};
                    for (const auto& model : knownModels) {
                        if (preload->second.find(model) != std::string::npos) {
                            if (auto nameDim = vector::dimres::dim_from_model_name(model)) {
                                dim = nameDim;
                                spdlog::info(
                                    "[VectorInit] probe: preload list heuristic dim={} for '{}'",
                                    *dim, model);
                                break;
                            }
                        }
                    }
                }
            } catch (...) {
                spdlog::debug("[VectorInit] preload model list heuristic probe failed");
            }
        }
    }

    if (!dim) {
        spdlog::info("[VectorInit] deferring initialization (provider dim unresolved)");
        clearRetryableInitAttempt();
        return Result<bool>(false);
    }

    cfg.embedding_dim = dim.value();
    cfg.suppress_search_index_builds = deps_.suppressVectorIndexBuild;
    if (cfg.suppress_search_index_builds) {
        spdlog::warn("[VectorInit] search index build/load suppressed by memory instrumentation "
                     "profile");
    }

    if (auto env = getenvCopy("YAMS_VECTOR_ENABLE_TURBOQUANT_STORAGE"); env) {
        cfg.enable_turboquant_storage = isTruthyValue(*env);
        spdlog::info("[VectorInit] turboquant storage overridden to {} via env",
                     cfg.enable_turboquant_storage);
    }
    if (auto env = getenvCopy("YAMS_VECTOR_QUANTIZED_PRIMARY_STORAGE"); env) {
        cfg.quantized_primary_storage = isTruthyValue(*env);
        spdlog::info("[VectorInit] quantized primary storage overridden to {} via env",
                     cfg.quantized_primary_storage);
    }
    if (auto env = getenvCopy("YAMS_VECTOR_TURBOQUANT_BITS"); env) {
        if (auto bits = parseInt(*env)) {
            cfg.turboquant_bits = static_cast<uint8_t>(std::clamp(*bits, 1, 8));
            spdlog::info("[VectorInit] turboquant bits overridden to {} via env",
                         static_cast<int>(cfg.turboquant_bits));
        }
    }
    if (auto env = getenvCopy("YAMS_VECTOR_TURBOQUANT_SEED"); env) {
        if (auto seed = parseUnsigned<uint64_t>(*env)) {
            cfg.turboquant_seed = seed.value();
            spdlog::info("[VectorInit] turboquant seed overridden to {} via env",
                         cfg.turboquant_seed);
        }
    }

    auto cfgPath = ConfigResolver::resolveDefaultConfigPath();
    if (!cfgPath.empty()) {
        try {
            auto kv = ConfigResolver::parseSimpleTomlFlat(cfgPath);
            if (auto it = kv.find("vector_database.search_engine");
                it != kv.end() && !it->second.empty()) {
                std::string normalized(it->second);
                std::transform(
                    normalized.begin(), normalized.end(), normalized.begin(), [](char c) {
                        return static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
                    });
                if (auto parsed = vector::parseVectorSearchEngine(normalized)) {
                    cfg.search_engine = parsed.value();
                    spdlog::info("[VectorInit] search engine configured as {} from {}",
                                 vector::vectorSearchEngineName(cfg.search_engine),
                                 cfgPath.string());
                }
            }
            if (auto it = kv.find("vector_database.vec0_phss_enabled");
                it != kv.end() && !it->second.empty()) {
                cfg.vec0_phss_enabled = isTruthyValue(it->second);
                spdlog::info("[VectorInit] vec0 PHSS configured as {} from {}",
                             cfg.vec0_phss_enabled ? "enabled" : "disabled", cfgPath.string());
            }
            if (auto it = kv.find("vector_database.vec0_phss_candidates");
                it != kv.end() && !it->second.empty()) {
                if (auto candidates = parseInt(it->second)) {
                    cfg.vec0_phss_candidates = static_cast<size_t>(std::max(1, *candidates));
                    spdlog::info("[VectorInit] vec0 PHSS candidates configured as {} from {}",
                                 cfg.vec0_phss_candidates, cfgPath.string());
                }
            }
            if (auto it = kv.find("vector_database.simeon_pq_rerank_factor");
                it != kv.end() && !it->second.empty()) {
                if (auto factor = parseUnsigned<size_t>(it->second)) {
                    cfg.simeon_pq_rerank_factor = std::max<size_t>(1, *factor);
                    spdlog::info("[VectorInit] Simeon PQ rerank factor configured as {} from {}",
                                 cfg.simeon_pq_rerank_factor, cfgPath.string());
                }
            }
        } catch (...) {
            spdlog::debug("[VectorInit] optional vector config overrides parse failed");
        }
    }

    if (auto env = getenvCopy("YAMS_VECTOR_SEARCH_ENGINE"); env) {
        std::string normalized(*env);
        std::transform(normalized.begin(), normalized.end(), normalized.begin(), [](char c) {
            return static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        });
        if (auto parsed = vector::parseVectorSearchEngine(normalized)) {
            cfg.search_engine = parsed.value();
            spdlog::info("[VectorInit] search engine overridden to {} via env",
                         vector::vectorSearchEngineName(cfg.search_engine));
        } else {
            spdlog::warn("[VectorInit] invalid YAMS_VECTOR_SEARCH_ENGINE='{}'; using default {}",
                         *env, vector::vectorSearchEngineName(cfg.search_engine));
        }
    }
    if (auto env = getenvCopy("YAMS_VECTOR_VEC0_PHSS_ENABLED"); env) {
        cfg.vec0_phss_enabled = isTruthyValue(*env);
        spdlog::info("[VectorInit] vec0 PHSS overridden to {} via env",
                     cfg.vec0_phss_enabled ? "enabled" : "disabled");
    }
    if (auto env = getenvCopy("YAMS_VECTOR_VEC0_PHSS_CANDIDATES"); env) {
        if (auto candidates = parseInt(*env)) {
            cfg.vec0_phss_candidates = static_cast<size_t>(std::max(1, *candidates));
            spdlog::info("[VectorInit] vec0 PHSS candidates overridden to {} via env",
                         cfg.vec0_phss_candidates);
        }
    }

    // Log start
    auto tid = std::this_thread::get_id();
    spdlog::info("[VectorInit] start pid={} tid={} path={} exists={} create={} dim={} engine={}",
                 currentProcessId(), static_cast<const void*>(&tid), cfg.database_path,
                 exists ? "yes" : "no", cfg.create_if_missing ? "yes" : "no", cfg.embedding_dim,
                 vector::vectorSearchEngineName(cfg.search_engine));

    // Cross-process advisory lock
#ifdef _WIN32
    HANDLE hLock = INVALID_HANDLE_VALUE;
#else
    int lock_fd = -1;
#endif
    std::filesystem::path lockPath =
        std::filesystem::path(cfg.database_path).replace_extension(".lock");

    try {
#ifdef _WIN32
        hLock = CreateFileA(lockPath.string().c_str(), GENERIC_READ | GENERIC_WRITE,
                            FILE_SHARE_READ | FILE_SHARE_WRITE, NULL, OPEN_ALWAYS,
                            FILE_ATTRIBUTE_NORMAL, NULL);
        if (hLock != INVALID_HANDLE_VALUE) {
            OVERLAPPED ov = {0};
            if (!LockFileEx(hLock, LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0,
                            &ov)) {
                spdlog::info("[VectorInit] skipped (lock busy by another process)");
                CloseHandle(hLock);
                clearRetryableInitAttempt();
                return Result<bool>(false);
            }
        }
#else
        lock_fd = ::open(lockPath.c_str(), O_CREAT | O_RDWR, 0644);
        if (lock_fd >= 0) {
            struct flock fl{};
            fl.l_type = F_WRLCK;
            fl.l_whence = SEEK_SET;
            if (fcntl(lock_fd, F_SETLK, &fl) == -1) {
                spdlog::info("[VectorInit] skipped (lock busy by another process)");
                ::close(lock_fd);
                clearRetryableInitAttempt();
                return Result<bool>(false);
            }
        }
#endif
    } catch (...) {
        spdlog::warn("[VectorInit] lock setup error (continuing without lock)");
    }

    // Initialize with retries
    const int maxAttempts = 3;
    bool success = false;

    for (int attempt = 0; attempt < maxAttempts; ++attempt) {
        if (attempt > 0) {
            int backoff_ms = (attempt == 1 ? 100 : 300);
            std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
            spdlog::info("[VectorInit] retrying attempt {} of {}", attempt + 1, maxAttempts);
        }

        try {
            auto vdb = std::make_shared<vector::VectorDatabase>(cfg);
            if (vdb->initialize()) {
                std::atomic_store_explicit(&vectorDatabase_, vdb, std::memory_order_release);

                // Initialize counter
                try {
                    vdb->initializeCounter();
                } catch (...) {
                    spdlog::debug("[VectorInit] initializeCounter best-effort step failed");
                }

                bool vectorDbReady = false;
                try {
                    const auto rows = vdb->getVectorCount();
                    vectorDbReady = (rows > 0);
                    if (vectorDbReady) {
                        // Eagerly prepare the search index so first query doesn't
                        // pay the O(n log n) ANN build cost (profiler: 99% CPU).
                        spdlog::info("[VectorInit] preparing search index for {} vectors", rows);
                        if (vdb->prepareSearchIndex()) {
                            spdlog::info("[VectorInit] search index ready");
                        } else {
                            spdlog::warn("[VectorInit] search index prepare failed: {}",
                                         vdb->getLastError());
                        }
                    } else {
                        spdlog::info("[VectorInit] Empty vector DB; index will be built on first "
                                     "embedding batch (coordinator owns index readiness)");
                    }
                } catch (...) {
                    spdlog::debug(
                        "[VectorInit] failed probing vector count/search index readiness");
                }

                // Update state (DB readiness only; index readiness is managed by coordinator)
                if (deps_.state) {
                    try {
                        deps_.state->readiness.vectorDbInitAttempted = true;
                        deps_.state->readiness.vectorDbReady = vectorDbReady;
                        deps_.state->readiness.vectorDbDim =
                            static_cast<uint32_t>(cfg.embedding_dim);
                    } catch (...) {
                        spdlog::debug("[VectorInit] failed publishing vector readiness state");
                    }
                }

                // Dispatch FSM event
                if (deps_.serviceFsm) {
                    try {
                        deps_.serviceFsm->dispatch(VectorsInitializedEvent{cfg.embedding_dim});
                    } catch (...) {
                        spdlog::debug("[VectorInit] failed dispatching VectorsInitializedEvent");
                    }
                }

                // Write sentinel
                ConfigResolver::writeVectorSentinel(dataDir, cfg.embedding_dim, "vec0", 1);

                success = true;
                break;
            } else {
                auto err = vdb->getLastError();
                spdlog::warn("[VectorInit] initialization attempt {} failed: {}", attempt + 1, err);

                // Check if transient error
                std::string el = err;
                std::transform(el.begin(), el.end(), el.begin(), ::tolower);
                bool transient = (el.find("busy") != std::string::npos) ||
                                 (el.find("lock") != std::string::npos) ||
                                 (el.find("timeout") != std::string::npos);
                if (!transient)
                    break;
            }
        } catch (const std::exception& e) {
            spdlog::warn("[VectorInit] exception attempt {}: {}", attempt + 1, e.what());
        }
    }

    // Release lock
#ifdef _WIN32
    if (hLock != INVALID_HANDLE_VALUE) {
        OVERLAPPED ov = {0};
        UnlockFileEx(hLock, 0, 1, 0, &ov);
        CloseHandle(hLock);
    }
#else
    if (lock_fd >= 0) {
        struct flock fl{};
        fl.l_type = F_UNLCK;
        fl.l_whence = SEEK_SET;
        (void)fcntl(lock_fd, F_SETLK, &fl);
        ::close(lock_fd);
    }
#endif

    if (!success) {
        spdlog::error("[VectorInit] all {} attempt(s) failed; continuing without vector DB",
                      maxAttempts);
        clearRetryableInitAttempt();
        return Error{ErrorCode::DatabaseError, "vector database init failed after retries"};
    }

    YAMS_POSTCONDITION(getVectorDatabase() != nullptr,
                       "VectorSystemManager success path must publish a vector database");
    return Result<bool>(true);
}

size_t VectorSystemManager::getEmbeddingDimension() const {
    auto db = std::atomic_load_explicit(&vectorDatabase_, std::memory_order_acquire);
    if (db) {
        return db->getConfig().embedding_dim;
    }
    return 0;
}

} // namespace yams::daemon
