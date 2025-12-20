// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: Apache-2.0

#include <yams/daemon/components/ConfigResolver.h>
#include <yams/daemon/components/ServiceManagerFsm.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/VectorSystemManager.h>
#include <yams/daemon/resource/model_provider.h>
#include <yams/vector/vector_database.h>
#include <yams/vector/vector_index_manager.h>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <thread>

#ifdef _WIN32
#include <io.h>
#include <windows.h>
#define getpid _getpid
#else
#include <fcntl.h>
#include <unistd.h>
#endif

namespace yams::daemon {

VectorSystemManager::VectorSystemManager(Dependencies deps) : deps_(std::move(deps)) {}

VectorSystemManager::~VectorSystemManager() {
    shutdown();
}

Result<void> VectorSystemManager::initialize() {
    // Initialization is deferred to initializeOnce() which takes dataDir
    return Result<void>{};
}

void VectorSystemManager::shutdown() {
    // Save index if present
    if (vectorIndexManager_) {
        spdlog::debug("[VectorSystemManager] Shutting down vector index manager");
        vectorIndexManager_.reset();
    }

    if (vectorDatabase_) {
        spdlog::debug("[VectorSystemManager] Shutting down vector database");
        vectorDatabase_.reset();
    }
}

Result<bool> VectorSystemManager::initializeOnce(const std::filesystem::path& dataDir) {
    // In-process guard (first-wins)
    if (initAttempted_.exchange(true, std::memory_order_acq_rel)) {
        spdlog::debug("[VectorInit] skipped (already attempted in this process)");
        if (deps_.state) {
            try {
                deps_.state->readiness.vectorDbInitAttempted = true;
            } catch (...) {
            }
        }
        return Result<bool>(false);
    }

    if (vectorDatabase_) {
        spdlog::debug("[VectorInit] vectorDatabase_ already present; nothing to do");
        return Result<bool>(false);
    }

    namespace fs = std::filesystem;
    vector::VectorDatabaseConfig cfg;
    cfg.database_path = (dataDir / "vectors.db").string();
    bool exists = fs::exists(cfg.database_path);
    cfg.create_if_missing = true;

    // Resolve embedding dimension with precedence
    std::optional<size_t> dim;

    // 1. Existing DB DDL
    if (exists) {
        try {
            auto ddlDim = ConfigResolver::readDbEmbeddingDim(cfg.database_path);
            if (ddlDim && *ddlDim > 0) {
                dim = *ddlDim;
                spdlog::info("[VectorInit] probe: ddl dim={}", *ddlDim);
            }
        } catch (...) {
        }
    }

    // 2. Config file / env (only if no provider available)
    auto modelProvider = deps_.modelProvider.lock();
    if (!dim && (!modelProvider || !modelProvider->isAvailable())) {
        auto cfgPath = ConfigResolver::resolveDefaultConfigPath();
        if (!cfgPath.empty()) {
            try {
                auto kv = ConfigResolver::parseSimpleTomlFlat(cfgPath);
                auto it = kv.find("embeddings.embedding_dim");
                if (it != kv.end() && !it->second.empty()) {
                    dim = static_cast<size_t>(std::stoul(it->second));
                    spdlog::info("[VectorInit] probe: config dim={}", *dim);
                }
            } catch (...) {
            }
        }

        if (!dim) {
            if (const char* envd = std::getenv("YAMS_EMBED_DIM")) {
                try {
                    dim = static_cast<size_t>(std::stoul(envd));
                } catch (...) {
                }
            }
        }
    }

    // 3. Ask provider
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
        }
    }

    // 4. Embedding generator
    if (!dim && (!modelProvider || !modelProvider->isAvailable()) && deps_.getEmbeddingDimension) {
        try {
            size_t g = deps_.getEmbeddingDimension();
            if (g > 0)
                dim = g;
        } catch (...) {
        }
    }

    if (!dim) {
        spdlog::info("[VectorInit] deferring initialization (provider dim unresolved)");
        if (deps_.state) {
            try {
                deps_.state->readiness.vectorDbInitAttempted = false;
            } catch (...) {
            }
        }
        initAttempted_.store(false, std::memory_order_release);
        return Result<bool>(false);
    }

    cfg.embedding_dim = *dim;

    // Log start
    auto tid = std::this_thread::get_id();
    spdlog::info("[VectorInit] start pid={} tid={} path={} exists={} create={} dim={}",
                 static_cast<long long>(::getpid()), (void*)(&tid), cfg.database_path,
                 exists ? "yes" : "no", cfg.create_if_missing ? "yes" : "no", cfg.embedding_dim);

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
                vectorDatabase_ = std::move(vdb);

                // Initialize counter
                try {
                    vectorDatabase_->initializeCounter();
                } catch (...) {
                }

                // Update state
                if (deps_.state) {
                    try {
                        deps_.state->readiness.vectorDbReady = true;
                        deps_.state->readiness.vectorDbDim =
                            static_cast<uint32_t>(cfg.embedding_dim);
                    } catch (...) {
                    }
                }

                // Dispatch FSM event
                if (deps_.serviceFsm) {
                    try {
                        deps_.serviceFsm->dispatch(VectorsInitializedEvent{cfg.embedding_dim});
                    } catch (...) {
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
        return Error{ErrorCode::DatabaseError, "vector database init failed after retries"};
    }

    return Result<bool>(true);
}

bool VectorSystemManager::initializeIndexManager(const std::filesystem::path& dataDir,
                                                 size_t dimension) {
    if (dimension == 0) {
        // Try to get from database config
        if (vectorDatabase_) {
            dimension = vectorDatabase_->getConfig().embedding_dim;
        }
    }

    if (dimension == 0) {
        spdlog::warn("[VectorSystemManager] Cannot init index manager: dimension unknown");
        return false;
    }

    try {
        vector::IndexConfig indexConfig;
        indexConfig.dimension = dimension;
        indexConfig.max_elements = ConfigResolver::readVectorMaxElements();
        indexConfig.hnsw_ef_construction = 200;
        indexConfig.hnsw_m = 16;

        vectorIndexManager_ = std::make_shared<vector::VectorIndexManager>(indexConfig);
        auto initRes = vectorIndexManager_->initialize();

        if (!initRes) {
            spdlog::warn("[VectorSystemManager] Failed to initialize VectorIndexManager: {}",
                         initRes.error().message);
            vectorIndexManager_.reset();
            return false;
        }

        spdlog::info(
            "[VectorSystemManager] VectorIndexManager initialized with dim={}, max_elements={}",
            dimension, indexConfig.max_elements);
        return true;
    } catch (const std::exception& e) {
        spdlog::error("[VectorSystemManager] Exception initializing index manager: {}", e.what());
        return false;
    }
}

bool VectorSystemManager::loadPersistedIndex(const std::filesystem::path& indexPath) {
    if (!vectorIndexManager_) {
        spdlog::warn("[VectorSystemManager] Cannot load index: manager not initialized");
        return false;
    }

    namespace fs = std::filesystem;
    if (!fs::exists(indexPath)) {
        spdlog::debug("[VectorSystemManager] No persisted index at {}", indexPath.string());
        return false;
    }

    auto loadRes = vectorIndexManager_->loadIndex(indexPath.string());
    if (!loadRes) {
        spdlog::warn("[VectorSystemManager] Failed to load index: {}", loadRes.error().message);
        return false;
    }

    auto stats = vectorIndexManager_->getStats();
    spdlog::info("[VectorSystemManager] Loaded vector index with {} vectors", stats.num_vectors);
    return stats.num_vectors > 0;
}

bool VectorSystemManager::saveIndex(const std::filesystem::path& indexPath) {
    if (!vectorIndexManager_) {
        return false;
    }

    auto stats = vectorIndexManager_->getStats();
    if (stats.num_vectors == 0) {
        spdlog::debug("[VectorSystemManager] No vectors to save");
        return true;
    }

    auto saveRes = vectorIndexManager_->saveIndex(indexPath.string());
    if (!saveRes) {
        spdlog::warn("[VectorSystemManager] Failed to save index: {}", saveRes.error().message);
        return false;
    }

    spdlog::info("[VectorSystemManager] Saved {} vectors to {}", stats.num_vectors,
                 indexPath.string());
    return true;
}

size_t VectorSystemManager::getEmbeddingDimension() const {
    if (vectorDatabase_) {
        return vectorDatabase_->getConfig().embedding_dim;
    }
    return 0;
}

void VectorSystemManager::alignDimensions() {
    // Called after embedding generator is initialized
    // Currently a no-op - dimensions are set at init time
    spdlog::debug("[VectorSystemManager] alignDimensions called");
}

} // namespace yams::daemon
