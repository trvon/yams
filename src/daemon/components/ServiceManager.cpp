#include <sqlite3.h>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <ctime>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <future>
#include <optional>
#include <thread>
#include <unistd.h>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/as_tuple.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/experimental/channel.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_future.hpp>
#include <yams/api/content_store_builder.h>
#include <yams/compat/thread_stop_compat.h>
#include <yams/core/types.h>
#include <yams/daemon/components/DaemonMetrics.h>
#include <yams/daemon/components/init_utils.hpp>
#include <yams/daemon/components/PoolManager.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/WorkerPool.h>
#include <yams/daemon/ipc/retrieval_session.h>
#include <yams/daemon/resource/abi_content_extractor_adapter.h>
#include <yams/daemon/resource/abi_model_provider_adapter.h>
#include <yams/daemon/resource/abi_plugin_loader.h>
#include <yams/daemon/resource/plugin_host.h>
#include <yams/daemon/resource/plugin_loader.h>
#include <yams/detection/file_type_detector.h>
#include <yams/metadata/migration.h>
#include <yams/search/search_engine_builder.h>
#include <yams/vector/sqlite_vec_backend.h>
#include <yams/vector/vector_database.h>

namespace {
std::optional<size_t> read_db_embedding_dim(const std::filesystem::path& dbPath) {
    try {
        namespace fs = std::filesystem;
        if (dbPath.empty() || !fs::exists(dbPath))
            return std::nullopt;
        yams::vector::SqliteVecBackend backend;
        auto r = backend.initialize(dbPath.string());
        if (!r)
            return std::nullopt;
        auto dimOpt = backend.getStoredEmbeddingDimension();
        backend.close();
        if (dimOpt && *dimOpt > 0)
            return dimOpt;
    } catch (...) {
    }
    return std::nullopt;
}
void write_vector_sentinel(const std::filesystem::path& dataDir, size_t dim,
                           const std::string& /*tableName*/, int schemaVersion) {
    try {
        namespace fs = std::filesystem;
        fs::create_directories(dataDir);
        nlohmann::json j;
        j["embedding_dim"] = dim;
        j["schema_version"] = schemaVersion;
        j["written_at"] = std::time(nullptr);
        std::ofstream out(dataDir / "vectors_sentinel.json");
        if (out)
            out << j.dump(2);
    } catch (...) {
    }
}
bool env_truthy(const char* value) {
    if (!value || !*value) {
        return false;
    }
    std::string v(value);
    std::transform(v.begin(), v.end(), v.begin(), [](unsigned char c) { return std::tolower(c); });
    return !(v == "0" || v == "false" || v == "off" || v == "no");
}

std::optional<size_t> read_vector_sentinel_dim(const std::filesystem::path& dataDir) {
    try {
        namespace fs = std::filesystem;
        auto p = dataDir / "vectors_sentinel.json";
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
}
} // namespace

// Open the daemon namespace for all following member definitions.
namespace yams::daemon {
using yams::Error;
using yams::ErrorCode;
using yams::Result;
namespace search = yams::search;

ServiceManager::ServiceManager(const DaemonConfig& config, StateComponent& state)
    : config_(config), state_(state) {
    try {
        spdlog::debug("ServiceManager constructor start");
        // In test builds, prefer mock embedding provider unless explicitly disabled.
        // This avoids heavy ONNX runtime usage and platform-specific crashes on CI/macOS.
#ifdef YAMS_TESTING
        // Default to auto-embed on AddDocument in tests unless explicitly turned off
        try {
            auto falsy = [](const char* s) {
                if (!s)
                    return false;
                std::string v(s);
                std::transform(v.begin(), v.end(), v.begin(), ::tolower);
                return v == "0" || v == "false" || v == "off" || v == "no";
            };
            if (!falsy(std::getenv("YAMS_EMBED_ON_ADD"))) {
                embeddingsAutoOnAdd_ = true;
                spdlog::debug("YAMS_TESTING: defaulting embeddingsAutoOnAdd_=true");
            }
        } catch (...) {
        }
#endif
        // Initialize worker pool early at a conservative minimum; TuningManager will scale up.
        try {
            if (!workerPool_) {
                auto threads = static_cast<std::size_t>(TuneAdvisor::poolMinSizeIpc());
                if (threads < 1)
                    threads = 1;
                workerPool_ = std::make_shared<WorkerPool>(threads);
                poolThreads_ = threads;
                spdlog::info("WorkerPool initialized with {} threads", threads);
            }
        } catch (const std::exception& e) {
            spdlog::warn("Failed to initialize WorkerPool: {} (will use system executor)",
                         e.what());
        }
        // Initialize plugin hosts early so that environment-driven trust (YAMS_PLUGIN_DIR)
        // can be applied before autoload attempts. Previously abiHost_ was never constructed,
        // causing autoloadPluginsNow() to scan zero ABI roots and load 0 plugins.
        try {
            if (!abiHost_) {
                // Use dataDir (may be empty here) for trust file persistence; it will be
                // created later during initialize(). If empty, omit trust file persistence.
                std::filesystem::path trustFile;
                if (!config_.dataDir.empty()) {
                    trustFile = config_.dataDir / "plugins.trust";
                }
                abiHost_ = std::make_unique<AbiPluginHost>(this, trustFile);
                spdlog::debug("ServiceManager: AbiPluginHost initialized (trustFile='{}')",
                              trustFile.string());
            }
        } catch (const std::exception& e) {
            spdlog::warn("ServiceManager: failed to initialize AbiPluginHost: {}", e.what());
        } catch (...) {
            spdlog::warn("ServiceManager: unknown error initializing AbiPluginHost");
        }
        // Perform single-attempt guarded vector DB initialization early so dimension is known.
        {
            spdlog::debug("[Startup] invoking initializeVectorDatabaseOnce guard");
            auto vres = initializeVectorDatabaseOnce(config_.dataDir);
            if (!vres) {
                spdlog::warn(
                    "[Startup] vector database unavailable ({}). Continuing without vectors.",
                    vres.error().message);
            } else {
                if (vres.value()) {
                    spdlog::debug("[Startup] vector database initialized (dim={})",
                                  state_.readiness.vectorDbDim.load());
                } else {
                    spdlog::debug("[Startup] vector database init skipped (already attempted)");
                }
            }
        }
        // Auto-trust plugin directory from env if provided.
        try {
            if (const char* env = std::getenv("YAMS_PLUGIN_DIR")) {
                std::filesystem::path penv(env);
                if (!penv.empty()) {
                    if (abiHost_) {
                        if (auto tr1 = abiHost_->trustAdd(penv); !tr1) {
                            spdlog::warn("Failed to auto-trust YAMS_PLUGIN_DIR {}: {}",
                                         penv.string(), tr1.error().message);
                        }
                    }
                    if (abiPluginLoader_) {
                        if (auto tr2 = abiPluginLoader_->trustAdd(penv); !tr2) {
                            spdlog::warn("Failed to auto-trust YAMS_PLUGIN_DIR for loader {}: {}",
                                         penv.string(), tr2.error().message);
                        }
                    }
                }
            }
        } catch (const std::exception& e) {
            spdlog::warn("Exception during auto-trust setup: {}", e.what());
        }

        // Auto-trust configured plugin directory (config_.pluginDir) so that specifying
        // pluginDir in DaemonConfig is sufficient without relying on YAMS_PLUGIN_DIR env.
        try {
            if (!config_.pluginDir.empty()) {
                std::filesystem::path pconf = config_.pluginDir;
                if (abiHost_) {
                    if (auto trc = abiHost_->trustAdd(pconf); !trc) {
                        spdlog::warn("Failed to trust configured pluginDir {}: {}", pconf.string(),
                                     trc.error().message);
                    } else {
                        spdlog::debug("Trusted configured pluginDir {} for ABI host",
                                      pconf.string());
                    }
                }
                if (abiPluginLoader_) {
                    if (auto trc2 = abiPluginLoader_->trustAdd(pconf); !trc2) {
                        spdlog::warn(
                            "Failed to trust configured pluginDir for legacy loader {}: {}",
                            pconf.string(), trc2.error().message);
                    } else {
                        spdlog::debug("Trusted configured pluginDir {} for legacy plugin loader",
                                      pconf.string());
                    }
                }
            }
        } catch (const std::exception& e) {
            spdlog::warn("Exception trusting configured pluginDir: {}", e.what());
        }
    } catch (const std::exception& e) {
        spdlog::warn("Exception during ServiceManager constructor setup: {}", e.what());
    }
}

ServiceManager::~ServiceManager() {
    shutdown();
}

yams::Result<void> ServiceManager::initialize() {
    // Validate data directory synchronously to fail fast if unwritable
    namespace fs = std::filesystem;
    fs::path dataDir = config_.dataDir;
    if (dataDir.empty()) {
        if (const char* xdgDataHome = std::getenv("XDG_DATA_HOME")) {
            dataDir = fs::path(xdgDataHome) / "yams";
        } else if (const char* homeEnv = std::getenv("HOME")) {
            dataDir = fs::path(homeEnv) / ".local" / "share" / "yams";
        } else {
            dataDir = fs::path(".") / "yams_data";
        }
    }
    std::error_code ec;
    fs::create_directories(dataDir, ec);
    spdlog::info("ServiceManager: resolved data directory: {}", dataDir.string());
    if (ec) {
        return Error{ErrorCode::IOError,
                     std::string("Failed to create storage directory: ") + ec.message()};
    }
    // Probe write access
    const auto probe = dataDir / ".yams-write-test";
    {
        std::ofstream f(probe);
        if (!f.good()) {
            return Error{ErrorCode::IOError, "Data directory is not writable: " + dataDir.string()};
        }
        f << "ok";
        f.close();
    }
    fs::remove(probe, ec);

    // Persist resolved dataDir for downstream components/telemetry
    resolvedDataDir_ = dataDir;

    // Log plugin scan directories for troubleshooting
    try {
        std::string dirs;
        for (const auto& d : PluginLoader::getDefaultPluginDirectories()) {
            if (!dirs.empty())
                dirs += ";";
            dirs += d.string();
        }
        spdlog::info("Plugin scan directories: {}", dirs);
    } catch (...) {
    }

    // Ensure file type detector is initialized once before background workers start.
    try {
        (void)yams::detection::FileTypeDetector::initializeWithMagicNumbers();
    } catch (...) {
        // Non-fatal: detector will remain with built-in fallbacks
    }

    // Start background resource initialization (coroutine-based)
    initThread_ = yams::compat::jthread([this](yams::compat::stop_token token) {
        spdlog::info("Starting async resource initialization (coroutine)...");
        // Launch coroutine on system executor and wait for completion in this thread
        auto fut =
            boost::asio::co_spawn(boost::asio::system_executor(),
                                  this->initializeAsyncAwaitable(token), boost::asio::use_future);
        auto result = fut.get();
        if (!result) {
            spdlog::error("Async resource initialization failed: {}", result.error().message);
            if (initCompleteCallback_) {
                initCompleteCallback_(false, result.error().message);
            }
        } else {
            spdlog::info("All daemon services initialized successfully");
            if (initCompleteCallback_) {
                initCompleteCallback_(true, "");
            }
        }
    });

    // Wait a short time for critical services to come up
    // This ensures the daemon can at least respond to status requests
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Configure PoolManager defaults from TuneAdvisor for known components
    try {
        PoolManager::Config ipcCfg{};
        ipcCfg.min_size = TuneAdvisor::poolMinSizeIpc();
        ipcCfg.max_size = TuneAdvisor::poolMaxSizeIpc();
        ipcCfg.cooldown_ms = TuneAdvisor::poolCooldownMs();
        ipcCfg.low_watermark = TuneAdvisor::poolLowWatermarkPercent();
        ipcCfg.high_watermark = TuneAdvisor::poolHighWatermarkPercent();
        PoolManager::instance().configure("ipc", ipcCfg);

        PoolManager::Config ioCfg{};
        ioCfg.min_size = TuneAdvisor::poolMinSizeIpcIo();
        // Bound IO max by both configured max and a dynamic cap from CPU budget
        try {
            auto dynCap = TuneAdvisor::recommendedThreads(0.5 /*backgroundFactor*/);
            ioCfg.max_size = std::min(TuneAdvisor::poolMaxSizeIpcIo(), dynCap);
        } catch (...) {
            ioCfg.max_size = TuneAdvisor::poolMaxSizeIpcIo();
        }
        ioCfg.cooldown_ms = TuneAdvisor::poolCooldownMs();
        ioCfg.low_watermark = TuneAdvisor::poolLowWatermarkPercent();
        ioCfg.high_watermark = TuneAdvisor::poolHighWatermarkPercent();
        PoolManager::instance().configure("ipc_io", ioCfg);
        spdlog::info("PoolManager defaults configured: ipc[min={},max={}] io[min={},max={}]",
                     ipcCfg.min_size, ipcCfg.max_size, ioCfg.min_size, ioCfg.max_size);
    } catch (const std::exception& e) {
        spdlog::debug("PoolManager configure error: {}", e.what());
    }

    // Sanity check: if dependencies are ready but searchExecutor_ not initialized
    if (state_.readiness.databaseReady.load() && state_.readiness.metadataRepoReady.load() &&
        !searchExecutor_) {
        spdlog::warn("SearchExecutor not initialized despite database and metadata repo ready");
    }
    return Result<void>();
}

void ServiceManager::shutdown() {
    // Stop pool reconciler thread first
    try {
        if (poolReconThread_.joinable()) {
            poolReconThread_.request_stop();
        }
    } catch (...) {
    }
    if (initThread_.joinable()) {
        initThread_.request_stop();
        initThread_.join();
    }

    spdlog::debug("ServiceManager: Shutting down daemon resources");

    // Persist vector index when ready
    if (vectorIndexManager_ && state_.readiness.vectorIndexReady.load()) {
        try {
            auto indexPath = config_.dataDir / "vector_index.bin";

            // Create directory if needed
            std::error_code ec;
            std::filesystem::create_directories(indexPath.parent_path(), ec);

            spdlog::info("Saving vector index to '{}'", indexPath.string());
            auto saveRes = vectorIndexManager_->saveIndex(indexPath.string());

            if (!saveRes) {
                spdlog::warn("Failed to save vector index: {}", saveRes.error().message);
            } else {
                auto stats = vectorIndexManager_->getStats();
                spdlog::info("Vector index saved successfully ({} vectors)", stats.num_vectors);
            }
        } catch (const std::exception& e) {
            spdlog::warn("Vector index save exception: {}", e.what());
        }
    }

    // Shutdown embedding generator (if any)
    if (embeddingGenerator_) {
        embeddingGenerator_->shutdown();
        embeddingGenerator_.reset();
    }

    // Shutdown model provider (unload models first, then shutdown)
    if (modelProvider_) {
        try {
            auto loaded = modelProvider_->getLoadedModels();
            for (const auto& name : loaded) {
                auto ur = modelProvider_->unloadModel(name);
                if (!ur) {
                    spdlog::debug("Unload model {} failed: {}", name, ur.error().message);
                }
            }
        } catch (...) {
        }
        modelProvider_->shutdown();
        modelProvider_.reset();
    }

    // Shutdown search engine
    if (searchEngine_) {
        searchEngine_.reset();
    }

    // Shutdown retrieval sessions
    if (retrievalSessions_) {
        retrievalSessions_.reset();
    }

    // Shutdown plugins (prefer ABI host)
    try {
        if (abiHost_) {
            for (const auto& d : abiHost_->listLoaded()) {
                (void)abiHost_->unload(d.name);
            }
        }
    } catch (...) {
    }

    // Shutdown connection pool and database
    if (connectionPool_) {
        connectionPool_->shutdown();
        connectionPool_.reset();
    }
    if (database_) {
        database_->close();
        database_.reset();
    }

    // Release all remaining resources
    searchExecutor_.reset();
    metadataRepo_.reset();
    vectorIndexManager_.reset();
    searchBuilder_.reset();
    contentStore_.reset();

    spdlog::info("ServiceManager: All services have been shut down.");
}

// Single-attempt vector database initialization. Safe to call multiple times; only
// the first invocation performs work. Subsequent calls are cheap no-ops.
yams::Result<bool>
ServiceManager::initializeVectorDatabaseOnce(const std::filesystem::path& dataDir) {
    // In-process guard
    if (vectorDbInitAttempted_.exchange(true, std::memory_order_acq_rel)) {
        spdlog::debug("[VectorInit] skipped (already attempted in this process)");
        try {
            state_.readiness.vectorDbInitAttempted = true;
        } catch (...) {
        }
        return Result<bool>(false);
    }
    try {
        state_.readiness.vectorDbInitAttempted = true;
    } catch (...) {
    }

    // Honor global disable flags
    auto is_on = [](const char* v) {
        if (!v)
            return false;
        std::string s(v);
        std::transform(s.begin(), s.end(), s.begin(), ::tolower);
        return s == "1" || s == "true" || s == "yes" || s == "on";
    };
    if (is_on(std::getenv("YAMS_DISABLE_VECTORS")) ||
        is_on(std::getenv("YAMS_DISABLE_VECTOR_DB"))) {
        spdlog::warn("[VectorInit] disabled via env flag");
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
    cfg.create_if_missing = !exists; // permit creation on first run

    // Resolve embedding dimension with precedence:
    // 1. Existing DB DDL (if present)
    // 2. Config file ~/.config/yams/config.toml
    // 3. Env YAMS_EMBED_DIM
    // 4. Embedding generator (if already available)
    // 5. Fallback heuristic (384)
    size_t dim = 0;
    if (exists) {
        try {
            auto ddlDim = read_db_embedding_dim(cfg.database_path);
            if (ddlDim && *ddlDim > 0)
                dim = *ddlDim;
        } catch (...) {
        }
    }
    // Config file
    if (dim == 0) {
        try {
            fs::path cfgHome;
            if (const char* xdg = std::getenv("XDG_CONFIG_HOME"))
                cfgHome = fs::path(xdg);
            else if (const char* home = std::getenv("HOME"))
                cfgHome = fs::path(home) / ".config";
            fs::path cpath = cfgHome / "yams" / "config.toml";
            if (!cpath.empty() && fs::exists(cpath)) {
                std::ifstream in(cpath);
                std::string line;
                auto trim = [](std::string& t) {
                    if (t.empty())
                        return;
                    t.erase(0, t.find_first_not_of(" \t"));
                    auto p = t.find_last_not_of(" \t");
                    if (p != std::string::npos)
                        t.erase(p + 1);
                };
                while (std::getline(in, line)) {
                    std::string l = line;
                    trim(l);
                    if (l.empty() || l[0] == '#')
                        continue;
                    if (l.find("embeddings.embedding_dim") != std::string::npos) {
                        auto eq = l.find('=');
                        if (eq != std::string::npos) {
                            std::string v = l.substr(eq + 1);
                            trim(v);
                            if (!v.empty() && v.front() == '"' && v.back() == '"')
                                v = v.substr(1, v.size() - 2);
                            try {
                                dim = static_cast<size_t>(std::stoul(v));
                            } catch (...) {
                            }
                        }
                        break;
                    }
                }
            }
        } catch (...) {
        }
    }
    // Env
    if (dim == 0) {
        try {
            if (const char* envd = std::getenv("YAMS_EMBED_DIM"))
                dim = static_cast<size_t>(std::stoul(envd));
        } catch (...) {
        }
    }
    // Generator
    if (dim == 0) {
        try {
            if (embeddingGenerator_)
                dim = embeddingGenerator_->getEmbeddingDimension();
        } catch (...) {
        }
    }
    if (dim == 0)
        dim = 384;
    cfg.embedding_dim = dim;

    // Log start with PID/TID
    auto tid = std::this_thread::get_id();
    spdlog::info("[VectorInit] start pid={} tid={} path={} exists={} create={} dim={}",
                 static_cast<long long>(::getpid()), (void*)(&tid), cfg.database_path,
                 exists ? "yes" : "no", cfg.create_if_missing ? "yes" : "no", cfg.embedding_dim);

    // Cross-process advisory lock to avoid concurrent init/extension loads
    int lock_fd = -1;
    std::filesystem::path lockPath =
        std::filesystem::path(cfg.database_path).replace_extension(".lock");
    try {
        lock_fd = ::open(lockPath.c_str(), O_CREAT | O_RDWR, 0644);
        if (lock_fd >= 0) {
            struct flock fl{};
            fl.l_type = F_WRLCK;
            fl.l_whence = SEEK_SET;
            fl.l_start = 0;
            fl.l_len = 0;
            if (fcntl(lock_fd, F_SETLK, &fl) == -1) {
                spdlog::info("[VectorInit] skipped (lock busy by another process)");
                ::close(lock_fd);
                lock_fd = -1;
                return Result<bool>(false);
            } else {
                // Stamp pid for diagnostics
                try {
                    (void)ftruncate(lock_fd, 0);
                    std::string stamp = std::to_string(static_cast<long long>(::getpid())) + "\n";
                    (void)::write(lock_fd, stamp.data(), stamp.size());
                    (void)lseek(lock_fd, 0, SEEK_SET);
                } catch (...) {
                }
            }
        } else {
            spdlog::debug("[VectorInit] could not open lock file (continuing without lock)");
        }
    } catch (...) {
        spdlog::debug("[VectorInit] lock setup error (continuing without lock)");
    }

    const int maxAttempts = 3;
    int attempt = 0;
    for (; attempt < maxAttempts; ++attempt) {
        if (attempt > 0) {
            // Exponential-ish backoff: 100ms, 300ms
            int backoff_ms = (attempt == 1 ? 100 : 300);
            std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
            spdlog::info("[VectorInit] retrying attempt {} of {}", attempt + 1, maxAttempts);
        }
        try {
            auto vdb = std::make_shared<vector::VectorDatabase>(cfg);
            if (!vdb->initialize()) {
                auto err = vdb->getLastError();
                spdlog::warn("[VectorInit] initialization attempt {} failed: {}", attempt + 1, err);
                // Heuristic: retry on lock/busy/timeout errors; otherwise abort early
                std::string el = err;
                std::transform(el.begin(), el.end(), el.begin(), ::tolower);
                bool transient = (el.find("busy") != std::string::npos) ||
                                 (el.find("lock") != std::string::npos) ||
                                 (el.find("locked") != std::string::npos) ||
                                 (el.find("timeout") != std::string::npos);
                if (!transient && attempt + 1 < maxAttempts) {
                    // Non-transient: break out without further retries
                    attempt = maxAttempts - 1; // signal final
                }
            } else {
                vectorDatabase_ = std::move(vdb);
                spdlog::info("[VectorInit] end pid={} tid={} path={} dim={} attempts={}",
                             static_cast<long long>(::getpid()), (void*)(&tid), cfg.database_path,
                             cfg.embedding_dim, attempt + 1);
                try {
                    state_.readiness.vectorDbReady = true;
                    state_.readiness.vectorDbDim = static_cast<uint32_t>(cfg.embedding_dim);
                } catch (...) {
                }
                // Sentinel write & quick health probes (best-effort)
                try {
                    write_vector_sentinel(dataDir, cfg.embedding_dim, "vec0", 1);
                } catch (...) {
                }
                try {
                    std::size_t rows = vectorDatabase_->getVectorCount();
                    spdlog::debug("[VectorInit] current row count={} (initial)", rows);
                } catch (...) {
                }
                try {
                    auto sdim = read_vector_sentinel_dim(dataDir);
                    if (sdim && *sdim != cfg.embedding_dim) {
                        spdlog::warn("[VectorInit] sentinel dimension mismatch sentinel={} "
                                     "actual={} — run 'yams doctor' if needed",
                                     *sdim, cfg.embedding_dim);
                    }
                } catch (...) {
                }
                break; // success
            }
        } catch (const std::exception& e) {
            spdlog::warn("[VectorInit] exception attempt {}: {}", attempt + 1, e.what());
        } catch (...) {
            spdlog::warn("[VectorInit] unknown exception attempt {}", attempt + 1);
        }
    }
    // Release advisory lock if held
    if (lock_fd >= 0) {
        struct flock fl{};
        fl.l_type = F_UNLCK;
        fl.l_whence = SEEK_SET;
        fl.l_start = 0;
        fl.l_len = 0;
        (void)fcntl(lock_fd, F_SETLK, &fl);
        ::close(lock_fd);
        lock_fd = -1;
    }
    if (!vectorDatabase_) {
        spdlog::error("[VectorInit] all {} attempt(s) failed; continuing without vector DB",
                      maxAttempts);
        return Error{ErrorCode::DatabaseError, "vector database init failed after retries"};
    }
    return Result<bool>(true);
}

// Best-effort: write bootstrap status JSON so CLI can show progress before IPC is ready
static void writeBootstrapStatusFile(const yams::daemon::DaemonConfig& cfg,
                                     const yams::daemon::StateComponent& state) {
    try {
        namespace fs = std::filesystem;
        fs::path dir = yams::daemon::YamsDaemon::getXDGRuntimeDir();
        if (dir.empty())
            return;
        fs::create_directories(dir);
        fs::path path = dir / "yams-daemon.status.json";
        nlohmann::json j;
        j["ready"] = state.readiness.fullyReady();
        // Normalize overall to lowercase for consistency with IPC lifecycle strings
        {
            std::string ov = state.readiness.overallStatus();
            for (auto& c : ov)
                c = static_cast<char>(std::tolower(c));
            j["overall"] = ov;
        }
        nlohmann::json rd;
        rd["ipc_server"] = state.readiness.ipcServerReady.load();
        rd["content_store"] = state.readiness.contentStoreReady.load();
        rd["database"] = state.readiness.databaseReady.load();
        rd["metadata_repo"] = state.readiness.metadataRepoReady.load();
        rd["search_engine"] = state.readiness.searchEngineReady.load();
        rd["model_provider"] = state.readiness.modelProviderReady.load();
        rd["vector_index"] = state.readiness.vectorIndexReady.load();
        rd["plugins"] = state.readiness.pluginsReady.load();
        // Extended vector DB readiness fields
        rd["vector_db_init_attempted"] = state.readiness.vectorDbInitAttempted.load();
        rd["vector_db_ready"] = state.readiness.vectorDbReady.load();
        rd["vector_db_dim"] = state.readiness.vectorDbDim.load();
        j["readiness"] = rd;
        nlohmann::json pr;
        pr["search_engine"] = state.readiness.searchProgress.load();
        pr["vector_index"] = state.readiness.vectorIndexProgress.load();
        pr["model_provider"] = state.readiness.modelLoadProgress.load();
        j["progress"] = pr;
        auto sec_since_start = std::chrono::duration_cast<std::chrono::seconds>(
                                   std::chrono::steady_clock::now() - state.stats.startTime)
                                   .count();
        std::map<std::string, int> expected_s{
            {"plugins", 1},      {"content_store", 2}, {"database", 2},       {"metadata_repo", 2},
            {"vector_index", 3}, {"search_engine", 4}, {"model_provider", 20}};
        nlohmann::json eta;
        auto add_eta = [&](const std::string& key, bool ready, int progress) {
            if (ready)
                return;
            int exp = expected_s.count(key) ? expected_s[key] : 5;
            try {
                if (state.initDurationsMs.count(key)) {
                    int hist = static_cast<int>((state.initDurationsMs.at(key) + 999) / 1000);
                    if (hist > 0)
                        exp = hist;
                }
            } catch (...) {
            }
            int remain_by_pct = std::max(0, exp - (exp * progress) / 100);
            int remain_by_elapsed = std::max(0, exp - static_cast<int>(sec_since_start));
            int remain = std::max(remain_by_pct, remain_by_elapsed);
            eta[key] = remain;
        };
        add_eta("plugins", state.readiness.pluginsReady.load(), 100);
        add_eta("content_store", state.readiness.contentStoreReady.load(), 100);
        add_eta("database", state.readiness.databaseReady.load(), 100);
        add_eta("metadata_repo", state.readiness.metadataRepoReady.load(), 100);
        add_eta("vector_index", state.readiness.vectorIndexReady.load(),
                state.readiness.vectorIndexProgress.load());
        add_eta("search_engine", state.readiness.searchEngineReady.load(),
                state.readiness.searchProgress.load());
        add_eta("model_provider", state.readiness.modelProviderReady.load(),
                state.readiness.modelLoadProgress.load());
        j["eta_seconds"] = eta;
        if (!state.initDurationsMs.empty()) {
            nlohmann::json dur;
            for (const auto& [k, v] : state.initDurationsMs) {
                dur[k] = v;
            }
            j["durations_ms"] = dur;
            std::vector<std::pair<std::string, uint64_t>> items(state.initDurationsMs.begin(),
                                                                state.initDurationsMs.end());
            std::sort(items.begin(), items.end(),
                      [](const auto& a, const auto& b) { return a.second > b.second; });
            nlohmann::json top;
            size_t count = std::min<size_t>(3, items.size());
            for (size_t i = 0; i < count; ++i) {
                nlohmann::json entry;
                entry["name"] = items[i].first;
                entry["elapsed_ms"] = items[i].second;
                top.push_back(entry);
            }
            if (!top.empty())
                j["top_slowest"] = top;
        }
        auto uptime = std::chrono::steady_clock::now() - state.stats.startTime;
        j["uptime_seconds"] = std::chrono::duration_cast<std::chrono::seconds>(uptime).count();
        j["data_dir"] = cfg.dataDir.string();
        std::ofstream out(path);
        if (out)
            out << j.dump(2);
    } catch (...) {
        // ignore
    }
}

boost::asio::awaitable<Result<void>>
ServiceManager::initializeAsyncAwaitable(yams::compat::stop_token token) {
    // Vector DB initialization (single guarded attempt invoked earlier in constructor)
    // Idempotent: safe if already attempted.
    (void)initializeVectorDatabaseOnce(config_.dataDir);
    spdlog::debug("ServiceManager(co): Initializing daemon resources");
    writeBootstrapStatusFile(config_, state_);

    auto read_timeout_ms = [](const char* env_name, int def_ms, int min_ms = 100) -> int {
        int ms = def_ms;
        if (const char* env = std::getenv(env_name)) {
            try {
                ms = std::stoi(env);
            } catch (...) {
            }
        }
        if (ms < min_ms)
            ms = min_ms;
        return ms;
    };

    using namespace std::chrono_literals;
    auto ex = co_await boost::asio::this_coro::executor;

    // Plugins step: mark ready (host scaffolding) and record duration uniformly
    try {
        (void)init::record_duration(
            "plugins",
            [&]() -> yams::Result<void> {
                state_.readiness.pluginsReady = true;
                return yams::Result<void>();
            },
            state_.initDurationsMs);
    } catch (...) {
        state_.readiness.pluginsReady = true;
    }
    writeBootstrapStatusFile(config_, state_);

    if (token.stop_requested())
        co_return Error{ErrorCode::OperationCancelled, "Shutdown requested"};

    // Resolve data dir strictly from config with XDG/HOME default only (no env overrides)
    namespace fs = std::filesystem;
    fs::path dataDir = config_.dataDir;
    if (dataDir.empty()) {
        if (const char* xdgDataHome = std::getenv("XDG_DATA_HOME")) {
            dataDir = fs::path(xdgDataHome) / "yams";
        } else if (const char* homeEnv = std::getenv("HOME")) {
            dataDir = fs::path(homeEnv) / ".local" / "share" / "yams";
        } else {
            dataDir = fs::path(".") / "yams_data";
        }
    }
    {
        std::error_code ec;
        fs::create_directories(dataDir, ec);
        resolvedDataDir_ = dataDir;
    }
    spdlog::info("ServiceManager[co]: using data directory: {}", dataDir.string());

    // Content store (synchronous, quick) using init helpers
    auto storeRoot = dataDir / "storage";
    spdlog::info("ContentStore root: {}", storeRoot.string());
    {
        using T = std::unique_ptr<yams::api::IContentStore>;
        auto storeRes = init::record_duration(
            "content_store",
            [&]() -> yams::Result<T> {
                return yams::api::ContentStoreBuilder::createDefault(storeRoot);
            },
            state_.initDurationsMs);
        if (storeRes) {
            auto& uniqueStore = const_cast<T&>(storeRes.value());
            contentStore_ = std::shared_ptr<yams::api::IContentStore>(uniqueStore.release());
            state_.readiness.contentStoreReady = true;
            writeBootstrapStatusFile(config_, state_);
        } else {
            spdlog::warn("ContentStore initialization failed: {}", storeRes.error().message);
            // Record error and fail initialization so lifecycle reflects the failure
            try {
                contentStoreError_ = storeRes.error().message;
            } catch (...) {
            }
            co_return Error{ErrorCode::IOError,
                            std::string("ContentStore initialization failed: ") +
                                storeRes.error().message};
        }
    }

    if (token.stop_requested())
        co_return Error{ErrorCode::OperationCancelled, "Shutdown requested"};

    // Phase: Open metadata DB with timeout (awaitable helper)
    auto dbPath = dataDir / "yams.db";
    database_ = std::make_shared<metadata::Database>();
    int open_timeout = read_timeout_ms("YAMS_DB_OPEN_TIMEOUT_MS", 5000, 250);
    bool db_ok = co_await init::await_record_duration(
        "database",
        [&]() -> boost::asio::awaitable<bool> {
            co_return co_await co_openDatabase(dbPath, open_timeout, token);
        },
        state_.initDurationsMs);
    writeBootstrapStatusFile(config_, state_);

    // Phase: Migrations (if DB ok)
    if (db_ok) {
        int mig_timeout = read_timeout_ms("YAMS_DB_MIGRATE_TIMEOUT_MS", 7000, 250);
        (void)co_await init::await_record_duration(
            "migrations",
            [&]() -> boost::asio::awaitable<bool> {
                co_return co_await co_migrateDatabase(mig_timeout, token);
            },
            state_.initDurationsMs);
    }

    // Phase: Connection pool + repo
    if (db_ok) {
        metadata::ConnectionPoolConfig dbPoolCfg;
        // Size DB pool based on centralized tuning (avoid large bursts at startup)
        size_t rec = 4;
        try {
            rec = std::max<size_t>(1, yams::daemon::TuneAdvisor::recommendedThreads(0.25));
        } catch (...) {
            size_t hw = std::max<size_t>(1, std::thread::hardware_concurrency());
            rec = std::max<size_t>(1, hw / 2);
        }
        dbPoolCfg.minConnections = std::min<size_t>(std::max<size_t>(2, rec), 8);
        dbPoolCfg.maxConnections = 32;
        if (const char* envMax = std::getenv("YAMS_DB_POOL_MAX"); envMax && *envMax) {
            try {
                auto v = static_cast<size_t>(std::stoul(envMax));
                if (v >= dbPoolCfg.minConnections)
                    dbPoolCfg.maxConnections = v;
            } catch (...) {
            }
        }
        if (const char* envMin = std::getenv("YAMS_DB_POOL_MIN"); envMin && *envMin) {
            try {
                auto v = static_cast<size_t>(std::stoul(envMin));
                if (v > 0)
                    dbPoolCfg.minConnections = v;
            } catch (...) {
            }
        }
        connectionPool_ = std::make_shared<metadata::ConnectionPool>(dbPath.string(), dbPoolCfg);
        auto poolInit = init::record_duration(
            "db_pool", [&]() { return connectionPool_->initialize(); }, state_.initDurationsMs);
        if (!poolInit) {
            spdlog::warn("Connection pool init failed: {} — continuing degraded",
                         poolInit.error().message);
        } else {
            auto repoRes = init::record_duration(
                "metadata_repo",
                [&]() -> yams::Result<void> {
                    metadataRepo_ =
                        std::make_shared<metadata::MetadataRepository>(*connectionPool_);
                    state_.readiness.metadataRepoReady = true;
                    spdlog::info("Metadata repository initialized successfully");
                    return yams::Result<void>();
                },
                state_.initDurationsMs);
            if (!repoRes) {
                spdlog::warn("Metadata repository init failed: {}", repoRes.error().message);
            }
        }
        writeBootstrapStatusFile(config_, state_);
    }

    // Executors and sessions
    if (database_ && metadataRepo_)
        searchExecutor_ = std::make_shared<search::SearchExecutor>(database_, metadataRepo_);
    retrievalSessions_ = std::make_unique<RetrievalSessionManager>();

    // Initialize post-ingest queue (decouple extraction/index/graph from add paths)
    try {
        using TA = yams::daemon::TuneAdvisor;
        uint32_t taThreads = 0;
        try {
            taThreads = TA::postIngestThreads();
        } catch (...) {
        }
        std::size_t threads = taThreads ? static_cast<std::size_t>(taThreads)
                                        : static_cast<std::size_t>(TA::postIngestThreads());
        // Initialize KG store on daemon side using connection pool if available
        std::shared_ptr<metadata::KnowledgeGraphStore> kgStore;
        try {
            if (connectionPool_) {
                metadata::KnowledgeGraphStoreConfig kgCfg;
                kgCfg.enable_alias_fts = true;
                kgCfg.enable_wal = true;
                auto kgRes = metadata::makeSqliteKnowledgeGraphStore(*connectionPool_, kgCfg);
                if (kgRes) {
                    auto uniqueKg = std::move(kgRes).value();
                    // Promote to shared_ptr for broader use
                    kgStore = std::shared_ptr<metadata::KnowledgeGraphStore>(std::move(uniqueKg));
                }
            }
        } catch (...) {
        }
        postIngest_ = std::make_unique<PostIngestQueue>(contentStore_, metadataRepo_,
                                                        contentExtractors_, kgStore, threads);
        spdlog::info("Post-ingest queue initialized (threads={})", threads);
    } catch (const std::exception& e) {
        spdlog::warn("Post-ingest queue init failed: {}", e.what());
    } catch (...) {
        spdlog::warn("Post-ingest queue init failed (unknown)");
    }

    // Vector DB initialization (single guarded attempt invoked earlier in constructor)
    // Idempotent: safe if already attempted.
    (void)initializeVectorDatabaseOnce(dataDir);

    // Vector index manager (using init helpers)
    try {
        // Determine if vector index should be disabled (environment flags)
        const bool disableVecIndex = []() {
            auto is_on = [](const char* v) {
                if (!v)
                    return false;
                std::string s(v);
                std::transform(s.begin(), s.end(), s.begin(), ::tolower);
                return s == "1" || s == "true" || s == "yes" || s == "on";
            };
            return is_on(std::getenv("YAMS_DISABLE_VECTORS")) ||
                   is_on(std::getenv("YAMS_DISABLE_VECTOR_INDEX")) ||
                   is_on(std::getenv("YAMS_DISABLE_VECTOR_DB"));
        }();
        vector::IndexConfig indexConfig;
        // Derive index dimension from Vector DB config if available; else fallback to
        // generator/heuristic
        size_t derivedIdxDim = 0;
        try {
            if (vectorDatabase_)
                derivedIdxDim = vectorDatabase_->getConfig().embedding_dim;
        } catch (...) {
        }
        if (derivedIdxDim == 0) {
            try {
                if (embeddingGenerator_)
                    derivedIdxDim = embeddingGenerator_->getEmbeddingDimension();
            } catch (...) {
            }
        }
        if (derivedIdxDim == 0)
            derivedIdxDim = 384;
        indexConfig.dimension = derivedIdxDim;
        indexConfig.type = vector::IndexType::FLAT;
        if (!disableVecIndex) {
            vectorIndexManager_ = std::make_shared<vector::VectorIndexManager>(indexConfig);
            auto initRes = init::record_duration(
                "vector_index", [&]() { return vectorIndexManager_->initialize(); },
                state_.initDurationsMs);
            if (!initRes) {
                spdlog::warn("Failed to initialize VectorIndexManager: {}",
                             initRes.error().message);
                vectorIndexManager_.reset();
            } else {
                state_.readiness.vectorIndexReady = true;
                writeBootstrapStatusFile(config_, state_);
            }
        } else {
            spdlog::warn("Vector index initialization disabled by YAMS_DISABLE_VECTOR_DB");
        }
    } catch (const std::exception& e) {
        spdlog::warn("Exception initializing VectorIndexManager: {}", e.what());
    }

    // Embedding generator will be initialized after plugin adoption
    spdlog::debug("Embedding generator initialization deferred to plugin adoption phase");

    // AUTOLOAD PLUGINS (MOVED UP)
    try {
        bool enableAutoload = config_.autoLoadPlugins;
        if (const char* env = std::getenv("YAMS_AUTOLOAD_PLUGINS")) {
            std::string v(env);
            for (auto& c : v)
                c = static_cast<char>(std::tolower(c));
            if (v == "0" || v == "false" || v == "off")
                enableAutoload = false;
        }
        if (enableAutoload) {
            auto loadResult =
                init::step<size_t>("plugin_autoload_now", [&]() { return autoloadPluginsNow(); });
            if (loadResult) {
                spdlog::info("ServiceManager: Autoloaded {} plugins.", loadResult.value());
            }
            auto adoptResult = co_await init::await_with_retry<bool>(
                [&]() -> boost::asio::awaitable<yams::Result<bool>> {
                    // Wrap synchronous adoption in an awaitable
                    co_return adoptModelProviderFromHosts();
                },
                /*attempts=*/3,
                /*backoff*/ [](int n) { return n == 1 ? 100 : 300; });
            // Log result uniformly
            if (adoptResult) {
                spdlog::info("[InitStep] adopt_model_provider: ok");
            } else {
                spdlog::warn("[InitStep] adopt_model_provider: failed: {}",
                             adoptResult.error().message);
            }
            if (adoptResult && adoptResult.value()) {
                spdlog::info("ServiceManager: Adopted model provider from plugins, starting "
                             "background initialization.");
                // Non-blocking initialization of embedding generator and search engine rebuild
                auto executor = getWorkerExecutor();
                // Initialize embedding generator now that we have a model provider
                auto initResult = ensureEmbeddingGeneratorReady();
                if (!initResult) {
                    spdlog::warn(
                        "Failed to initialize embedding generator after plugin adoption: {}",
                        initResult.error().message);
                } else {
                    spdlog::info(
                        "Embedding generator initialized successfully after plugin adoption");

                    // Now rebuild search engine to enable vector search
                    try {
                        auto self = shared_from_this();
                        boost::asio::co_spawn(
                            executor,
                            [self]() -> boost::asio::awaitable<void> {
                                co_await self->co_enableEmbeddingsAndRebuild();
                            },
                            boost::asio::detached);
                    } catch (const std::bad_weak_ptr& e) {
                        // If shared_from_this() fails, log and continue
                        spdlog::debug("Cannot rebuild search engine - ServiceManager not yet "
                                      "managed by shared_ptr");
                    }
                }
            } else {
                spdlog::info("ServiceManager: No model provider adopted from plugins.");
            }
            auto extractorResult = init::step<size_t>(
                "adopt_extractors", [&]() { return adoptContentExtractorsFromHosts(); });
            if (extractorResult) {
                spdlog::info("ServiceManager: Adopted {} content extractors.",
                             extractorResult.value());
            }
        }
        // If autoload is disabled but model provider is enabled, initialize a provider/generator
        // now so embedding requests and LoadModel work under mocks/local.
        if (!enableAutoload && config_.enableModelProvider) {
            spdlog::info(
                "Model provider enabled with autoload disabled; initializing provider now");
            auto initRes = ensureEmbeddingGeneratorReady();
            if (!initRes) {
                spdlog::warn("Provider/generator init (no-autoload) failed: {}",
                             initRes.error().message);
            } else {
                spdlog::info("Provider/generator ready (no-autoload)");
                // Rebuild search engine to enable vector search now that embeddings are ready
                try {
                    auto executor = getWorkerExecutor();
                    auto self = shared_from_this();
                    boost::asio::co_spawn(
                        executor,
                        [self]() -> boost::asio::awaitable<void> {
                            co_await self->co_enableEmbeddingsAndRebuild();
                        },
                        boost::asio::detached);
                } catch (const std::exception& e) {
                    spdlog::debug("Deferred rebuild scheduling failed: {}", e.what());
                }
            }
        }
    } catch (const std::exception& e) {
        spdlog::warn("Plugin autoload failed: {}", e.what());
    }

    // Build HybridSearchEngine with timeout
    try {
        state_.readiness.searchProgress = 10;
        writeBootstrapStatusFile(config_, state_);
        if (metadataRepo_) {
            state_.readiness.searchProgress = 40;
            writeBootstrapStatusFile(config_, state_);
        }
        if (vectorIndexManager_) {
            state_.readiness.searchProgress = 70;
            writeBootstrapStatusFile(config_, state_);
        }
        int build_timeout = read_timeout_ms("YAMS_SEARCH_BUILD_TIMEOUT_MS", 5000, 250);
        spdlog::info("[SearchBuild] scheduling initial build (vector_enabled hint={})",
                     (embeddingGenerator_ && embeddingGenerator_->isInitialized()) ? "true"
                                                                                   : "false");
        // Nudge progress to indicate we're in the final build step
        try {
            state_.readiness.searchProgress =
                std::max<int>(state_.readiness.searchProgress.load(), 90);
        } catch (...) {
        }
        auto built = co_await co_buildEngine(build_timeout, token, false);
        if (built) {
            std::lock_guard<std::mutex> lk(searchEngineMutex_);
            searchEngine_ = built;
            state_.readiness.searchEngineReady = true;
            state_.readiness.searchProgress = 100;
            writeBootstrapStatusFile(config_, state_);
            spdlog::info("HybridSearchEngine initialized and published to AppContext");
        } else {
            // Do not leave UI stuck below 100% when we are running degraded.
            try {
                state_.readiness.searchProgress = 100;
            } catch (...) {
            }
            writeBootstrapStatusFile(config_, state_);
            spdlog::warn("[SearchBuild] initial engine build not ready; continuing degraded");
        }
    } catch (const std::exception& e) {
        spdlog::warn("Exception wiring HybridSearchEngine: {}", e.what());
    }

    // Watchdog: promote lifecycle if core infra is ready
    try {
        std::thread([this]() {
            std::this_thread::sleep_for(std::chrono::milliseconds(1200));
            if (initCompleteCallback_ && state_.readiness.databaseReady.load() &&
                state_.readiness.metadataRepoReady.load()) {
                spdlog::info("Lifecycle Ready watchdog: promoting state based on core readiness");
                try {
                    initCompleteCallback_(true, "");
                } catch (...) {
                }
                initCompleteCallback_ = nullptr;
            }
        }).detach();
    } catch (...) {
    }

    co_return Result<void>();
}

boost::asio::awaitable<bool> ServiceManager::co_openDatabase(const std::filesystem::path& dbPath,
                                                             int timeout_ms,
                                                             yams::compat::stop_token token) {
    using namespace boost::asio::experimental::awaitable_operators;
    auto ex = co_await boost::asio::this_coro::executor;

    // Channel to receive completion from worker (wrap Result in shared_ptr for
    // default-construct path)
    boost::asio::experimental::channel<void(boost::system::error_code,
                                            std::shared_ptr<Result<void>>)>
        ch(ex, 1);
    try {
        boost::asio::post(getWorkerExecutor(), [this, dbPath, &ch]() mutable {
            try {
                auto r = database_->open(dbPath.string(), metadata::ConnectionMode::Create);
                ch.try_send(boost::system::error_code{},
                            std::make_shared<Result<void>>(std::move(r)));
            } catch (...) {
                ch.try_send(make_error_code(std::errc::operation_canceled),
                            std::make_shared<Result<void>>(
                                Error{ErrorCode::InternalError, "DB open exception"}));
            }
        });
    } catch (...) {
        // Fallback: run inline if posting fails
        auto r = database_->open(dbPath.string(), metadata::ConnectionMode::Create);
        co_return (r && (state_.readiness.databaseReady = true,
                         spdlog::info("Database opened successfully"), true));
    }

    boost::asio::steady_timer timer(ex);
    timer.expires_after(std::chrono::milliseconds(timeout_ms));

    auto which = co_await (ch.async_receive(boost::asio::as_tuple(boost::asio::use_awaitable)) ||
                           timer.async_wait(boost::asio::as_tuple(boost::asio::use_awaitable)));

    if (which.index() == 1 || token.stop_requested()) {
        spdlog::warn("Database open timed out after {} ms; continuing in degraded mode",
                     timeout_ms);
        co_return false;
    }

    auto tup0 = std::move(std::get<0>(which));
    auto ec = std::get<0>(tup0);
    auto pres = std::get<1>(tup0);
    if (ec) {
        spdlog::warn("Database open failed (channel/ec): {}", ec.message());
        co_return false;
    }
    if (!pres || !(*pres)) {
        std::string msg = (!pres ? std::string("no result") : (*pres).error().message);
        spdlog::warn("Database open failed: {} — continuing in degraded mode", msg);
        co_return false;
    }
    state_.readiness.databaseReady = true;
    spdlog::info("Database opened successfully");
    co_return true;
}

boost::asio::awaitable<bool> ServiceManager::co_migrateDatabase(int timeout_ms,
                                                                yams::compat::stop_token token) {
    using namespace boost::asio::experimental::awaitable_operators;
    auto ex = co_await boost::asio::this_coro::executor;
    metadata::MigrationManager mm(*database_);
    mm.initialize();
    mm.registerMigrations(metadata::YamsMetadataMigrations::getAllMigrations());

    boost::asio::experimental::channel<void(boost::system::error_code,
                                            std::shared_ptr<Result<void>>)>
        ch(ex, 1);
    try {
        boost::asio::post(getWorkerExecutor(), [&mm, &ch]() mutable {
            try {
                auto r = mm.migrate();
                ch.try_send(boost::system::error_code{},
                            std::make_shared<Result<void>>(std::move(r)));
            } catch (...) {
                ch.try_send(make_error_code(std::errc::operation_canceled),
                            std::make_shared<Result<void>>(
                                Error{ErrorCode::InternalError, "Migration exception"}));
            }
        });
    } catch (...) {
        auto r = mm.migrate();
        co_return r;
    }

    boost::asio::steady_timer timer(ex);
    timer.expires_after(std::chrono::milliseconds(timeout_ms));
    auto which = co_await (ch.async_receive(boost::asio::as_tuple(boost::asio::use_awaitable)) ||
                           timer.async_wait(boost::asio::as_tuple(boost::asio::use_awaitable)));
    if (which.index() == 1 || token.stop_requested()) {
        spdlog::warn("Database migration timed out after {} ms; proceeding", timeout_ms);
        co_return false;
    }
    auto tup1 = std::move(std::get<0>(which));
    auto ec1 = std::get<0>(tup1);
    auto pres1 = std::get<1>(tup1);
    if (ec1)
        co_return false;
    co_return static_cast<bool>(pres1 && (*pres1));
}

boost::asio::awaitable<std::shared_ptr<search::HybridSearchEngine>>
ServiceManager::co_buildEngine(int timeout_ms, yams::compat::stop_token token,
                               bool includeEmbeddingGenerator) {
    using namespace boost::asio::experimental::awaitable_operators;
    auto ex = co_await boost::asio::this_coro::executor;
    bool vector_enabled = false;
    try {
        vector_enabled = includeEmbeddingGenerator && embeddingGenerator_ &&
                         embeddingGenerator_->isInitialized();
    } catch (...) {
    }
    // Record intended reason: initial when not including embeddings; rebuild otherwise.
    try {
        this->lastSearchBuildReason_ = includeEmbeddingGenerator ? "rebuild" : "initial";
    } catch (...) {
    }
    spdlog::info("[SearchBuild] start include_embeddings={} vector_enabled={} timeout_ms={}",
                 includeEmbeddingGenerator ? "true" : "false", vector_enabled ? "true" : "false",
                 timeout_ms);
    searchBuilder_ = std::make_shared<search::SearchEngineBuilder>();
    searchBuilder_->withMetadataRepo(metadataRepo_);
    if (vectorIndexManager_)
        searchBuilder_->withVectorIndex(vectorIndexManager_);
    if (includeEmbeddingGenerator && embeddingGenerator_)
        searchBuilder_->withEmbeddingGenerator(embeddingGenerator_);
    auto opts = search::SearchEngineBuilder::BuildOptions::makeDefault();
    using RetT = Result<std::shared_ptr<search::HybridSearchEngine>>;

    boost::asio::experimental::channel<void(boost::system::error_code, std::shared_ptr<RetT>)> ch(
        ex, 1);
    try {
        boost::asio::post(getWorkerExecutor(), [this, opts, &ch]() mutable {
            try {
                auto r = searchBuilder_->buildEmbedded(opts);
                ch.try_send(boost::system::error_code{}, std::make_shared<RetT>(std::move(r)));
            } catch (...) {
                ch.try_send(make_error_code(std::errc::operation_canceled),
                            std::make_shared<RetT>(
                                Error{ErrorCode::InternalError, "Engine build exception"}));
            }
        });
    } catch (...) {
        auto r = searchBuilder_->buildEmbedded(opts);
        if (r) {
            spdlog::info("[SearchBuild] completed synchronously (fallback) ok vector_enabled={}",
                         vector_enabled ? "true" : "false");
            co_return r.value();
        }
        spdlog::warn("[SearchBuild] failed synchronously (fallback)");
        co_return nullptr;
    }

    boost::asio::steady_timer timer(ex);
    timer.expires_after(std::chrono::milliseconds(timeout_ms));
    auto which = co_await (ch.async_receive(boost::asio::as_tuple(boost::asio::use_awaitable)) ||
                           timer.async_wait(boost::asio::as_tuple(boost::asio::use_awaitable)));
    if (which.index() == 1 || token.stop_requested()) {
        spdlog::warn("[SearchBuild] timeout after {} ms; continuing degraded", timeout_ms);
        try {
            this->lastSearchBuildReason_ = "degraded";
        } catch (...) {
        }
        co_return nullptr;
    }
    auto tup2 = std::move(std::get<0>(which));
    auto ec2 = std::get<0>(tup2);
    auto pres2 = std::get<1>(tup2);
    if (ec2 || !pres2 || !(*pres2)) {
        if (pres2 && (*pres2))
            spdlog::warn("[SearchBuild] failed: {}", pres2->error().message);
        else
            spdlog::warn("[SearchBuild] failed: unknown error");
        try {
            this->lastSearchBuildReason_ = "degraded";
        } catch (...) {
        }
        co_return nullptr;
    }
    spdlog::info("[SearchBuild] end ok vector_enabled={}", vector_enabled ? "true" : "false");
    try {
        this->lastVectorEnabled_ = vector_enabled;
    } catch (...) {
    }
    co_return pres2->value();
}

Result<bool> ServiceManager::adoptModelProviderFromHosts(const std::string& preferredName) {
    try {
        if (abiHost_) {
            auto loaded = abiHost_->listLoaded();

            auto path_for = [&](const std::string& name) -> std::string {
                for (const auto& d : loaded) {
                    if (d.name == name)
                        return d.path.string();
                    try {
                        auto stem = std::filesystem::path(d.path).stem().string();
                        if (stem == name)
                            return d.path.string();
                    } catch (...) {
                    }
                }
                return std::string{};
            };

            auto try_adopt = [&](const std::string& pluginName) -> bool {
                auto ifaceRes = abiHost_->getInterface(pluginName, "model_provider_v1", 1);
                if (!ifaceRes) {
                    spdlog::debug("Model provider iface not found for plugin '{}' (path='{}') : {}",
                                  pluginName, path_for(pluginName), ifaceRes.error().message);
                    return false;
                }
                auto* table = reinterpret_cast<yams_model_provider_v1*>(ifaceRes.value());
                if (!table) {
                    spdlog::debug("Null model provider table for plugin '{}' (path='{}')",
                                  pluginName, path_for(pluginName));
                    return false;
                }
                if (table->abi_version != YAMS_IFACE_MODEL_PROVIDER_V1_VERSION) {
                    spdlog::debug(
                        "ABI mismatch for '{}' (path='{}'): got v{}, expected v{} — skipping",
                        pluginName, path_for(pluginName), table->abi_version,
                        (int)YAMS_IFACE_MODEL_PROVIDER_V1_VERSION);
                    return false;
                }
                modelProvider_ = std::make_shared<AbiModelProviderAdapter>(table);
                state_.readiness.modelProviderReady = (modelProvider_ != nullptr);
                spdlog::info("Adopted model provider from plugin: {} (path='{}', abi={})",
                             pluginName, path_for(pluginName), (int)table->abi_version);
                adoptedProviderPluginName_ = pluginName;
                clearModelProviderError();
                // Embedding generator initialization happens in the caller after all plugins are
                // processed

                // Safeguard: set preferred embedding model to ONNX default when user hasn't
                // chosen one
                try {
                    namespace fs = std::filesystem;
                    fs::path cfgPath;
                    if (const char* xdg = std::getenv("XDG_CONFIG_HOME"))
                        cfgPath = fs::path(xdg) / "yams" / "config.toml";
                    else if (const char* home = std::getenv("HOME"))
                        cfgPath = fs::path(home) / ".config" / "yams" / "config.toml";
                    if (!cfgPath.empty()) {
                        std::string content;
                        if (fs::exists(cfgPath)) {
                            std::ifstream in(cfgPath);
                            std::ostringstream ss;
                            ss << in.rdbuf();
                            content = ss.str();
                        }
                        auto hasKey =
                            content.find("embeddings.preferred_model") != std::string::npos ||
                            content.find("[embeddings]") != std::string::npos;
                        auto nomicDefault =
                            content.find("nomic-embed-text-v1.5") != std::string::npos;
                        // Only write when not set or set to known non-ONNX default
                        if (!hasKey || nomicDefault) {
                            spdlog::info("Selecting ONNX preferred model 'all-MiniLM-L6-v2' (auto) "
                                         "since user preference not set");
                            std::map<std::string, std::map<std::string, std::string>> sections;
                            // Minimal TOML writer: parse existing into sections map
                            {
                                std::istringstream iss(content);
                                std::string line;
                                std::string section;
                                auto trim = [](std::string& s) {
                                    if (s.empty())
                                        return;
                                    s.erase(0, s.find_first_not_of(" \t"));
                                    auto p = s.find_last_not_of(" \t");
                                    if (p != std::string::npos)
                                        s.erase(p + 1);
                                };
                                while (std::getline(iss, line)) {
                                    std::string l = line;
                                    trim(l);
                                    if (l.empty() || l[0] == '#')
                                        continue;
                                    if (!l.empty() && l.front() == '[') {
                                        auto end = l.find(']');
                                        if (end != std::string::npos)
                                            section = l.substr(1, end - 1);
                                        else
                                            section.clear();
                                        continue;
                                    }
                                    auto eq = l.find('=');
                                    if (eq == std::string::npos)
                                        continue;
                                    std::string key = l.substr(0, eq);
                                    std::string val = l.substr(eq + 1);
                                    trim(key);
                                    trim(val);
                                    if (!val.empty() && val.front() == '"' && val.back() == '"')
                                        val = val.substr(1, val.size() - 2);
                                    sections[section][key] = val;
                                }
                            }
                            sections["embeddings"]["preferred_model"] = "all-MiniLM-L6-v2";
                            fs::create_directories(cfgPath.parent_path());
                            std::ofstream out(cfgPath);
                            if (out) {
                                for (const auto& [sec, kv] : sections) {
                                    if (!sec.empty())
                                        out << "[" << sec << "]\n";
                                    for (const auto& [k, v] : kv)
                                        out << k << " = \"" << v << "\"\n";
                                    out << "\n";
                                }
                            }
                        }
                    }
                } catch (...) {
                    // non-fatal
                }
                return true;
            };
            if (!preferredName.empty()) {
                if (try_adopt(preferredName))
                    return Result<bool>(true);
            }
            if (try_adopt("onnx"))
                return Result<bool>(true);
            for (const auto& d : loaded) {
                bool hasIface = false;
                for (const auto& id : d.interfaces)
                    if (id == std::string("model_provider_v1")) {
                        hasIface = true;
                        break;
                    }
                if (!hasIface)
                    continue;
                spdlog::debug("Trying model provider adoption from loaded plugin {} (path='{}')",
                              d.name, d.path.string());
                if (try_adopt(d.name))
                    return Result<bool>(true);
                // Try stem of path as alternate plugin name
                try {
                    std::string alt = std::filesystem::path(d.path).stem().string();
                    if (!alt.empty() && alt != d.name) {
                        spdlog::debug(
                            "Trying model provider adoption from plugin path stem {} (path='{}')",
                            alt, d.path.string());
                        if (try_adopt(alt))
                            return Result<bool>(true);
                    }
                } catch (...) {
                }
                // Try alternate ABI versions if available
                for (int vv : {1, 0, 2}) {
                    try {
                        auto ifaceAlt = abiHost_->getInterface(d.name, "model_provider_v1", vv);
                        if (!ifaceAlt) {
                            spdlog::debug(
                                "getInterface(model_provider_v1,{}) failed for {} (path='{}')", vv,
                                d.name, d.path.string());
                            continue;
                        }
                        auto* table = reinterpret_cast<yams_model_provider_v1*>(ifaceAlt.value());
                        if (!table) {
                            spdlog::debug("Null provider table for {} (path='{}')", d.name,
                                          d.path.string());
                            continue;
                        }
                        spdlog::info(
                            "Adopted model provider (alt ABI v{}) from plugin: {} (path='{}')", vv,
                            d.name, d.path.string());
                        modelProvider_ = std::make_shared<AbiModelProviderAdapter>(table);
                        state_.readiness.modelProviderReady = (modelProvider_ != nullptr);
                        adoptedProviderPluginName_ = d.name;
                        clearModelProviderError();
                        return Result<bool>(true);
                    } catch (...) {
                    }
                }
            }
        }
    } catch (const std::exception& e) {
        return Error{ErrorCode::Unknown, e.what()};
    }
    return Result<bool>(false);
}

Result<size_t> ServiceManager::adoptContentExtractorsFromHosts() {
    size_t adopted = 0;
    try {
        if (abiHost_) {
            // Scan loaded plugins for content_extractor_v1 and adopt as extractors
            for (const auto& d : abiHost_->listLoaded()) {
                bool hasIface = false;
                for (const auto& id : d.interfaces) {
                    if (id == std::string("content_extractor_v1")) {
                        hasIface = true;
                        break;
                    }
                }
                if (!hasIface)
                    continue;
                auto ifaceRes = abiHost_->getInterface(d.name, "content_extractor_v1", 1);
                if (!ifaceRes)
                    continue;
                auto* table = reinterpret_cast<yams_content_extractor_v1*>(ifaceRes.value());
                if (!table || table->abi_version != YAMS_IFACE_CONTENT_EXTRACTOR_V1_VERSION)
                    continue;
                try {
                    auto adapter = std::make_shared<AbiContentExtractorAdapter>(table);
                    contentExtractors_.push_back(std::move(adapter));
                    ++adopted;
                    spdlog::info("Adopted content extractor from plugin: {}", d.name);
                } catch (...) {
                }
            }
        }
    } catch (const std::exception& e) {
        return Error{ErrorCode::Unknown, e.what()};
    }
    return Result<size_t>(adopted);
}

boost::asio::any_io_executor ServiceManager::getWorkerExecutor() const {
    if (workerPool_)
        return workerPool_->executor();
    return boost::asio::system_executor();
}

bool ServiceManager::resizeWorkerPool(std::size_t target) {
    try {
        if (target == 0)
            target = 1;
        if (!workerPool_) {
            workerPool_ = std::make_shared<WorkerPool>(target);
            poolThreads_ = target;
            spdlog::info("WorkerPool created with {} threads", target);
            return true;
        }
        bool changed = workerPool_->resize(target);
        if (changed) {
            poolThreads_ = target;
            spdlog::info("WorkerPool resized to {} threads", target);
        }
        return changed;
    } catch (const std::exception& e) {
        spdlog::warn("resizeWorkerPool error: {}", e.what());
        return false;
    }
}

Result<size_t> ServiceManager::autoloadPluginsNow() {
    try {
        // In mock/test mode, skip scanning/loading ABI plugins entirely to avoid
        // platform-specific crashes from dlopen or missing runtimes. The embedding
        // stack will use the mock provider instead.
        if (config_.useMockModelProvider || env_truthy(std::getenv("YAMS_USE_MOCK_PROVIDER"))) {
            spdlog::info("Plugin autoload skipped (mock provider in use)");
            return Result<size_t>(0);
        }
        if (const char* d = std::getenv("YAMS_DISABLE_ABI_PLUGINS"); d && *d) {
            spdlog::info("Plugin autoload disabled by YAMS_DISABLE_ABI_PLUGINS");
            return Result<size_t>(0);
        }
        std::vector<std::filesystem::path> roots;
        if (abiHost_) {
            for (const auto& p : abiHost_->trustList())
                roots.push_back(p);
        }
        if (wasmHost_) {
            for (const auto& p : wasmHost_->trustList())
                roots.push_back(p);
        }
        // Prefer explicit env override before default directories to avoid stale system plugins
        try {
            if (const char* env = std::getenv("YAMS_PLUGIN_DIR")) {
                std::filesystem::path penv(env);
                if (!penv.empty())
                    roots.push_back(penv);
            }
        } catch (...) {
        }
        try {
            for (const auto& d : PluginLoader::getDefaultPluginDirectories())
                roots.push_back(d);
        } catch (...) {
        }

        std::sort(roots.begin(), roots.end());
        roots.erase(std::unique(roots.begin(), roots.end()), roots.end());

        spdlog::info("Plugin autoload(now): {} roots to scan", roots.size());
        for (const auto& r : roots) {
            spdlog::debug("Plugin autoload(now): root {}", r.string());
        }
        size_t loaded_count = 0;
        for (const auto& r : roots) {
            try {
                if (abiHost_) {
                    if (auto sr = abiHost_->scanDirectory(r)) {
                        if (sr.value().empty()) {
                            spdlog::debug("Plugin autoload(now): no candidates in {}", r.string());
                        }
                        for (const auto& d : sr.value()) {
                            spdlog::debug(
                                "Plugin autoload(now): candidate '{}' path='{}' ifaces=[{}]",
                                d.name, d.path.string(), [&]() {
                                    std::string s;
                                    for (size_t i = 0; i < d.interfaces.size(); ++i) {
                                        if (i)
                                            s += ",";
                                        s += d.interfaces[i];
                                    }
                                    return s;
                                }());
                            auto lr = abiHost_->load(d.path, "");
                            if (lr) {
                                ++loaded_count;
                                spdlog::info("Plugin autoload(now): loaded '{}' (ifaces=[{}])",
                                             d.name, [&]() {
                                                 std::string s;
                                                 for (size_t i = 0; i < d.interfaces.size(); ++i) {
                                                     if (i)
                                                         s += ",";
                                                     s += d.interfaces[i];
                                                 }
                                                 return s;
                                             }());
                            } else {
                                spdlog::warn("Plugin autoload(now): load failed '{}' : {}", d.name,
                                             lr.error().message);
                            }
                        }
                    }
                }
                if (wasmHost_) {
                    if (auto sr = wasmHost_->scanDirectory(r)) {
                        for (const auto& d : sr.value()) {
                            if (wasmHost_->load(d.path, ""))
                                ++loaded_count;
                        }
                    }
                }
            } catch (const std::exception& e) {
                spdlog::warn("Plugin autoload(now): scan/load error at {}: {}", r.string(),
                             e.what());
            } catch (...) {
                spdlog::warn("Plugin autoload(now): unknown error at {}", r.string());
            }
        }
        spdlog::info("Plugin autoload(now): loaded {} plugin(s)", loaded_count);
        auto adopted = adoptModelProviderFromHosts();
        if (adopted && adopted.value()) {
            spdlog::info("Plugin autoload(now): model provider adopted");
        } else {
            spdlog::info("Plugin autoload(now): no model provider adopted");
        }
        (void)adoptContentExtractorsFromHosts();
        // Try to preload a preferred model if configured; do not gate on FSM readiness
        if (!preferredPreloadStarted_.load()) {
            preloadPreferredModelIfConfigured();
        }
        writeBootstrapStatusFile(config_, state_);
        return Result<size_t>(loaded_count);
    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError, e.what()};
    }
}

void ServiceManager::preloadPreferredModelIfConfigured() {
    if (preferredPreloadStarted_.exchange(true)) {
        return; // Already started
    }

    try {
        (void)init::step<void>("schedule_model_preload", [&]() -> yams::Result<void> {
            spdlog::info("Scheduling preferred model preload (async)");
            return yams::Result<void>();
        });
        // Skip if embedding generator is already initialized
        if (embeddingGenerator_ && embeddingGenerator_->isInitialized()) {
            spdlog::debug("preloadPreferredModelIfConfigured: embedding generator already "
                          "initialized, skipping");
            return;
        }

        if (!modelProvider_) {
            spdlog::debug("preloadPreferredModelIfConfigured: no model provider available");
            return;
        }
        // Resolve preferred model via env or disk
        std::string preferred;
        if (const char* env = std::getenv("YAMS_PREFERRED_MODEL")) {
            preferred = env;
        }
        if (preferred.empty()) {
            // Best-effort scan: ~/.yams/models/*/model.onnx
            const char* home = std::getenv("HOME");
            if (home) {
                namespace fs = std::filesystem;
                fs::path base = fs::path(home) / ".yams" / "models";
                std::error_code ec;
                if (fs::exists(base, ec) && fs::is_directory(base, ec)) {
                    for (const auto& e : fs::directory_iterator(base, ec)) {
                        if (!e.is_directory())
                            continue;
                        if (fs::exists(e.path() / "model.onnx", ec)) {
                            preferred = e.path().filename().string();
                            break;
                        }
                    }
                }
            }
        }
        if (preferred.empty()) {
            spdlog::info("Model preload skipped: no preferred model configured (set "
                         "YAMS_PREFERRED_MODEL or install a model)");
            return;
        }
        spdlog::info("Preloading preferred model: {}", preferred);

        // Use worker executor instead of raw std::thread for unified cancellation
        auto executor = getWorkerExecutor();

        // Safely handle shared_from_this - it may not be available during early initialization
        try {
            auto self = shared_from_this();
            boost::asio::co_spawn(
                executor,
                [self, preferred]() -> boost::asio::awaitable<void> {
                    try {
                        auto r = self->modelProvider_->loadModel(preferred);
                        if (r) {
                            self->state_.readiness.modelProviderReady.store(
                                true, std::memory_order_relaxed);
                            self->state_.readiness.modelLoadProgress.store(
                                100, std::memory_order_relaxed);
                            spdlog::info("Preferred model '{}' preloaded", preferred);
                            self->clearModelProviderError();
                        } else {
                            self->state_.readiness.modelLoadProgress.store(
                                0, std::memory_order_relaxed);
                            spdlog::warn("Preferred model '{}' preload failed: {}", preferred,
                                         r.error().message);
                            self->modelProviderDegraded_.store(true, std::memory_order_relaxed);
                            self->lastModelError_ =
                                std::string("preload failed: ") + r.error().message;
                            try {
                                if (self->modelProvider_)
                                    (void)self->modelProvider_->unloadModel(preferred);
                            } catch (...) {
                            }
                            // Retry once after a short delay to tolerate slow filesystems
                            boost::asio::steady_timer timer(
                                co_await boost::asio::this_coro::executor);
                            timer.expires_after(std::chrono::milliseconds(500));
                            co_await timer.async_wait(boost::asio::use_awaitable);

                            auto r_retry = self->modelProvider_->loadModel(preferred);
                            if (r_retry) {
                                self->state_.readiness.modelProviderReady.store(
                                    true, std::memory_order_relaxed);
                                self->state_.readiness.modelLoadProgress.store(
                                    100, std::memory_order_relaxed);
                                spdlog::info("Preferred model '{}' preloaded on retry", preferred);
                                self->clearModelProviderError();
                            } else {
#ifndef YAMS_USE_ONNX_RUNTIME
                                spdlog::warn("ONNX runtime disabled in this build; model "
                                             "preloading not supported by daemon binary");
#endif
                                spdlog::warn("Preferred model '{}' failed twice: {}", preferred,
                                             r_retry.error().message);
                            }
                        }
                    } catch (const std::exception& e) {
                        spdlog::warn("preloadPreferredModelIfConfigured error: {}", e.what());
                    } catch (...) {
                        spdlog::warn("preloadPreferredModelIfConfigured: unknown error");
                    }
                },
                boost::asio::detached);
        } catch (const std::bad_weak_ptr& e) {
            // Fall back to synchronous loading if shared_from_this() is not available
            spdlog::debug(
                "shared_from_this() not available, falling back to synchronous model load");

            // Capture necessary members by value/pointer for the lambda
            auto* provider = modelProvider_.get();
            auto* readiness = &state_.readiness;

            boost::asio::co_spawn(
                executor,
                [provider, readiness, preferred]() -> boost::asio::awaitable<void> {
                    try {
                        auto r = provider->loadModel(preferred);
                        if (r) {
                            readiness->modelProviderReady.store(true, std::memory_order_relaxed);
                            readiness->modelLoadProgress.store(100, std::memory_order_relaxed);
                            spdlog::info("Preferred model '{}' preloaded (fallback)", preferred);
                        } else {
                            readiness->modelLoadProgress.store(0, std::memory_order_relaxed);
                            spdlog::warn("Preferred model '{}' preload failed: {}", preferred,
                                         r.error().message);
                        }
                    } catch (const std::exception& fallbackEx) {
                        spdlog::warn("preloadPreferredModelIfConfigured fallback error: {}",
                                     fallbackEx.what());
                    }
                    co_return;
                },
                boost::asio::detached);
        }
    } catch (const std::exception& e) {
        spdlog::warn("preloadPreferredModelIfConfigured error: {}", e.what());
    }
}

boost::asio::awaitable<void> ServiceManager::co_enableEmbeddingsAndRebuild() {
    if (embedInitStarted_.exchange(true)) {
        spdlog::debug("[Rebuild] skip: embedding init already started");
        co_return; // Already started
    }

    try {
        spdlog::info("[Rebuild] start reason=embeddings_ready");
        auto res = ensureEmbeddingGeneratorReady();
        if (!res) {
            spdlog::warn("[Rebuild] embedding init failed: {}", res.error().message);
            co_return;
        }

        embedInitCompleted_ = true;
        // Model is already loaded by ensureEmbeddingGeneratorReady(), no need to preload again

        // Protect against concurrent rebuilds
        if (!rebuildInProgress_.exchange(true)) {
            spdlog::info("[Rebuild] search engine rebuild begin (enable vector scoring)");
            int build_timeout = 15000; // Generous timeout for rebuild
            auto rebuilt = co_await co_buildEngine(build_timeout, {}, true);

            if (rebuilt) {
                {
                    std::lock_guard<std::mutex> lk(searchEngineMutex_);
                    searchEngine_ = rebuilt;
                }

                // Update readiness indicators after successful rebuild
                state_.readiness.searchEngineReady = true;
                state_.readiness.searchProgress = 100;
                state_.readiness.vectorIndexReady = true;
                writeBootstrapStatusFile(config_, state_);

                spdlog::info("[Rebuild] done ok: vector scoring enabled");
            } else {
                spdlog::warn("[Rebuild] failed: engine rebuild unsuccessful");
            }

            rebuildInProgress_ = false;
        } else {
            spdlog::debug("[Rebuild] skip: rebuild already in progress");
        }
    } catch (const std::exception& e) {
        spdlog::warn("[Rebuild] error: {}", e.what());
        rebuildInProgress_ = false;
    }
}
std::function<void(bool)> ServiceManager::getWorkerJobSignal() {
    return [this](bool start) {
        if (start) {
            poolActive_.fetch_add(1, std::memory_order_relaxed);
            poolPosted_.fetch_add(1, std::memory_order_relaxed);
        } else {
            poolActive_.fetch_sub(1, std::memory_order_relaxed);
            poolCompleted_.fetch_add(1, std::memory_order_relaxed);
        }
    };
}

std::shared_ptr<search::HybridSearchEngine> ServiceManager::getSearchEngineSnapshot() const {
    std::lock_guard<std::mutex> lock(searchEngineMutex_);
    return searchEngine_;
}

yams::app::services::AppContext ServiceManager::getAppContext() const {
    app::services::AppContext ctx;
    ctx.service_manager = const_cast<ServiceManager*>(this);
    ctx.store = contentStore_;
    ctx.searchExecutor = searchExecutor_;
    ctx.metadataRepo = metadataRepo_;
    ctx.hybridEngine = getSearchEngineSnapshot();
    ctx.contentExtractors = contentExtractors_;

    // Log vector capability status
    bool vectorCapable = (embeddingGenerator_ != nullptr);
    spdlog::debug("AppContext: vector_capabilities={}", vectorCapable ? "active" : "unavailable");

    // Populate degraded/repair flags for search.
    // Do NOT degrade just because embeddings are missing; hybrid falls back to keyword/KG.
    // Only degrade when core metadata repository is unavailable or when explicitly forced.
    try {
        bool degraded = (metadataRepo_ == nullptr);
        int prog = 0;
        std::string details;

        // Use readiness progress when available
        try {
            prog = static_cast<int>(state_.readiness.searchProgress.load());
        } catch (...) {
        }

        // Environment overrides to force degraded mode and provide detail (non-blocking)
        if (const char* env = std::getenv("YAMS_SEARCH_DEGRADED")) {
            std::string v(env);
            if (!v.empty() && v != "0" && v != "false" && v != "False" && v != "FALSE")
                degraded = true;
        }
        if (const char* reason = std::getenv("YAMS_SEARCH_DEGRADED_REASON")) {
            details = reason;
        }
        if (degraded && details.empty()) {
            details = "Metadata repository unavailable";
        }

        if (prog < 0)
            prog = 0;
        if (prog > 100)
            prog = 100;

        ctx.searchRepairInProgress = degraded;
        ctx.searchRepairDetails = details;
        ctx.searchRepairProgress = prog;
    } catch (...) {
        // best-effort only
    }

    return ctx;
}

size_t ServiceManager::getWorkerQueueDepth() const {
    if (!workerPool_)
        return 0;
    // A simple estimate of the queue depth.
    long posted = poolPosted_.load();
    long completed = poolCompleted_.load();
    long active = poolActive_.load();
    if (posted > completed + active) {
        return posted - completed - active;
    }
    return 0;
}

// (Namespace yams::daemon remains open for subsequent member definitions)

Result<void> ServiceManager::ensureEmbeddingGeneratorReady() {
    try {
        // Check if already initialized
        if (embeddingGenerator_ && embeddingGenerator_->isInitialized()) {
            spdlog::debug("EmbeddingGenerator already initialized");
            return Result<void>();
        }

        // If we have a stale generator, reset it
        if (embeddingGenerator_) {
            spdlog::debug("Resetting uninitialized EmbeddingGenerator");
            embeddingGenerator_.reset();
        }

        const bool preferMockProvider =
            config_.useMockModelProvider || env_truthy(std::getenv("YAMS_USE_MOCK_PROVIDER"));

        // Try to get or create a model provider
        if (!modelProvider_ || !modelProvider_->isAvailable()) {
            // In config-driven mock mode, bypass plugin loading entirely.
            if (preferMockProvider) {
                spdlog::info("Using mock model provider (config/env preference)");
                try {
                    modelProvider_ = std::shared_ptr<IModelProvider>(createModelProvider(
                        config_.modelPoolConfig, /*preferredProvider=*/"", true));
                    state_.readiness.modelProviderReady = (modelProvider_ != nullptr);
                } catch (const std::exception& e) {
                    spdlog::warn("Failed to instantiate mock provider: {}", e.what());
                }
            }
            if (modelProvider_ && modelProvider_->isAvailable()) {
                spdlog::debug("Mock provider ready; skipping plugin adoption");
            } else {
                spdlog::debug("Model provider not available, attempting to load plugins");
                (void)autoloadPluginsNow();
                auto adopted = adoptModelProviderFromHosts();
                if (!adopted || !adopted.value()) {
                    spdlog::debug("No model provider adopted from plugins, trying local fallback");
                    // Fallback to local model generator when provider not available
                    try {
                        // Use unified selector that honors env, config preferred_model, preload
                        // list, and DB-aligned auto-detection
                        std::string preferred_local = resolvePreferredModel();
                        if (preferred_local.empty()) {
                            namespace fs = std::filesystem;
                            if (const char* home = std::getenv("HOME")) {
                                fs::path models = fs::path(home) / ".yams" / "models";
                                std::error_code ec;
                                if (fs::exists(models, ec) && fs::is_directory(models, ec)) {
                                    // Prefer a model matching existing DB dim if available
                                    size_t dbDim = 0;
                                    try {
                                        if (auto s = read_vector_sentinel_dim(getResolvedDataDir()))
                                            dbDim = *s;
                                    } catch (...) {
                                    }
                                    auto exists = [&](const char* n) {
                                        return fs::exists(models / n / "model.onnx", ec);
                                    };
                                    if (dbDim == 384 && exists("all-MiniLM-L6-v2"))
                                        preferred_local = "all-MiniLM-L6-v2";
                                    else if (dbDim == 768 && exists("all-mpnet-base-v2"))
                                        preferred_local = "all-mpnet-base-v2";
                                    // Fallback: first available
                                    if (preferred_local.empty()) {
                                        for (const auto& e : fs::directory_iterator(models, ec)) {
                                            if (e.is_directory() &&
                                                fs::exists(e.path() / "model.onnx", ec)) {
                                                preferred_local = e.path().filename().string();
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        if (!preferred_local.empty()) {
                            vector::EmbeddingConfig ecfg;
                            if (const char* home = std::getenv("HOME")) {
                                ecfg.model_path = (std::filesystem::path(home) / ".yams" /
                                                   "models" / preferred_local / "model.onnx")
                                                      .string();
                            }
                            ecfg.model_name = preferred_local;
                            ecfg.backend = vector::EmbeddingConfig::Backend::Hybrid;
                            ecfg.daemon_auto_start = false;
                            auto eg = std::make_shared<vector::EmbeddingGenerator>(ecfg);
                            if (eg->initialize()) {
                                embeddingGenerator_ = eg;
                                embeddingModelName_ = preferred_local;
                                spdlog::info("EmbeddingGenerator initialized from local model: {}",
                                             preferred_local);
                                alignVectorComponentDimensions();
                                return Result<void>();
                            } else {
                                spdlog::warn(
                                    "Failed to initialize local EmbeddingGenerator for model: {}",
                                    preferred_local);
                            }
                        }
                    } catch (const std::exception& e) {
                        spdlog::warn("Exception during local model fallback: {}", e.what());
                    }
                    return Error{ErrorCode::NotInitialized,
                                 "Model provider not available and local fallback failed"};
                }
            }
        }

        // At this point we have a model provider
        // Determine preferred model
        std::string preferred = resolvePreferredModel();
        if (preferred.empty()) {
            return Error{ErrorCode::NotFound, "No preferred model configured or installed"};
        }

        spdlog::debug("Attempting to load preferred model: {}", preferred);

        // Try to load the model through the provider
        auto r = modelProvider_->loadModel(preferred);
        if (!r) {
            spdlog::warn("Model provider failed to load '{}': {}", preferred, r.error().message);
            // Best-effort: unload the model if partially loaded and mark provider degraded
            try {
                (void)modelProvider_->unloadModel(preferred);
            } catch (...) {
            }
            modelProviderDegraded_.store(true, std::memory_order_relaxed);
            lastModelError_ = std::string("load '") + preferred + "' failed: " + r.error().message;
            // Fallback to local model generator
            try {
                vector::EmbeddingConfig ecfg;
                if (const char* home = std::getenv("HOME")) {
                    ecfg.model_path = (std::filesystem::path(home) / ".yams" / "models" /
                                       preferred / "model.onnx")
                                          .string();
                }
                ecfg.model_name = preferred;
                ecfg.backend = vector::EmbeddingConfig::Backend::Hybrid;
                ecfg.daemon_auto_start = false;
                auto eg = std::make_shared<vector::EmbeddingGenerator>(ecfg);
                if (eg->initialize()) {
                    embeddingGenerator_ = eg;
                    embeddingModelName_ = preferred;
                    spdlog::info("EmbeddingGenerator initialized from local model (fallback): {}",
                                 preferred);
                    alignVectorComponentDimensions();
                    return Result<void>();
                }
            } catch (const std::exception& e) {
                spdlog::warn("Local fallback failed: {}", e.what());
            }
            return Error{ErrorCode::InternalError, std::string("Failed to load model '") +
                                                       preferred + "': " + r.error().message};
        }

        clearModelProviderError();
        // Ensure only the selected model remains loaded in the provider
        try {
            if (modelProvider_) {
                auto loaded = modelProvider_->getLoadedModels();
                for (const auto& name : loaded) {
                    if (name != preferred) {
                        auto ur = modelProvider_->unloadModel(name);
                        if (!ur) {
                            spdlog::debug("Unload extra model '{}' failed: {}", name,
                                          ur.error().message);
                        }
                    }
                }
            }
        } catch (...) {
        }

        // Create and initialize embedding generator, bound to the provider-loaded model via daemon
        // backend. Use provider-reported dimensions to ensure downstream vector DB/index alignment.
        try {
            size_t providerDim = 0;
            size_t providerMaxSeq = 0;
            try {
                if (modelProvider_) {
                    providerDim = modelProvider_->getEmbeddingDim(preferred);
                    if (auto mi = modelProvider_->getModelInfo(preferred)) {
                        providerMaxSeq = mi.value().maxSequenceLength;
                    }
                }
            } catch (...) {
            }

            vector::EmbeddingConfig
                ecfg; // default Hybrid -> override to Daemon explicitly in daemon
            ecfg.backend = vector::EmbeddingConfig::Backend::Daemon;
            ecfg.model_name = preferred;
            if (providerDim > 0)
                ecfg.embedding_dim = providerDim; // seed until backend reports
            if (providerMaxSeq > 0)
                ecfg.max_sequence_length = providerMaxSeq;
            ecfg.daemon_auto_start = false; // we are in-daemon; avoid self-spawn paths

            auto eg = std::make_shared<vector::EmbeddingGenerator>(ecfg);
            if (!eg->initialize()) {
                spdlog::warn("EmbeddingGenerator (daemon backend) init failed; falling back to "
                             "default config");
                // Fallback to previous behavior
                eg = std::make_shared<vector::EmbeddingGenerator>();
                if (!eg->initialize()) {
                    embeddingGenerator_.reset();
                    return Error{ErrorCode::InternalError,
                                 "Failed to initialize embedding generator"};
                }
            }
            // Gracefully shutdown any previous generator before replacement
            if (embeddingGenerator_) {
                try {
                    embeddingGenerator_->shutdown();
                } catch (...) {
                }
            }
            embeddingGenerator_ = std::move(eg);
            embeddingModelName_ = preferred;
        } catch (const std::exception& egEx) {
            spdlog::warn("EmbeddingGenerator init exception: {}", egEx.what());
            embeddingGenerator_.reset();
            return Error{ErrorCode::InternalError, "Failed to initialize embedding generator"};
        }

        spdlog::info("EmbeddingGenerator initialized successfully (model='{}', backend=Daemon)",
                     preferred);
        alignVectorComponentDimensions();

        return Result<void>();
    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError, e.what()};
    }
}

Result<void> ServiceManager::ensureEmbeddingGeneratorFor(const std::string& modelName) {
    try {
        if (modelName.empty()) {
            return Error{ErrorCode::InvalidArgument, "Model name is empty"};
        }
        if (!modelProvider_ || !modelProvider_->isAvailable()) {
            return Error{ErrorCode::NotInitialized, "Model provider not available"};
        }
        // Create and initialize embedding generator bound to the provider-loaded model via daemon
        size_t providerDim = 0;
        size_t providerMaxSeq = 0;
        try {
            providerDim = modelProvider_->getEmbeddingDim(modelName);
            if (auto mi = modelProvider_->getModelInfo(modelName)) {
                providerMaxSeq = mi.value().maxSequenceLength;
            }
        } catch (...) {
        }
        // Ensure only the selected model remains loaded in the provider
        try {
            auto loaded = modelProvider_->getLoadedModels();
            for (const auto& name : loaded) {
                if (name != modelName) {
                    auto ur = modelProvider_->unloadModel(name);
                    if (!ur) {
                        spdlog::debug("Unload extra model '{}' failed: {}", name,
                                      ur.error().message);
                    }
                }
            }
        } catch (...) {
        }

        vector::EmbeddingConfig ecfg;
        ecfg.backend = vector::EmbeddingConfig::Backend::Daemon;
        ecfg.model_name = modelName;
        if (providerDim > 0)
            ecfg.embedding_dim = providerDim;
        if (providerMaxSeq > 0)
            ecfg.max_sequence_length = providerMaxSeq;
        ecfg.daemon_auto_start = false;
        auto eg = std::make_shared<vector::EmbeddingGenerator>(ecfg);
        if (!eg->initialize()) {
            modelProviderDegraded_.store(true, std::memory_order_relaxed);
            lastModelError_ = "embedding_generator_init_failed";
            return Error{ErrorCode::InternalError, "Failed to initialize embedding generator"};
        }
        // Gracefully shutdown any previous generator before replacement
        if (embeddingGenerator_) {
            try {
                embeddingGenerator_->shutdown();
            } catch (...) {
            }
        }
        embeddingGenerator_ = std::move(eg);
        clearModelProviderError();
        alignVectorComponentDimensions();
        return Result<void>();
    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError, e.what()};
    }
}

std::string ServiceManager::resolvePreferredModel() const {
    std::string preferred;

    // 1. Check environment variable first (highest priority)
    if (const char* envp = std::getenv("YAMS_PREFERRED_MODEL")) {
        preferred = envp;
        if (!preferred.empty()) {
            spdlog::debug("Preferred model from environment: {}", preferred);
            return preferred;
        }
    }

    // 2. Check config file
    try {
        namespace fs = std::filesystem;
        fs::path cfgHome;
        if (const char* xdg = std::getenv("XDG_CONFIG_HOME")) {
            cfgHome = fs::path(xdg);
        } else if (const char* home = std::getenv("HOME")) {
            cfgHome = fs::path(home) / ".config";
        }

        fs::path cfgPath = cfgHome / "yams" / "config.toml";
        if (!cfgPath.empty() && fs::exists(cfgPath)) {
            std::ifstream in(cfgPath);
            std::string line;
            auto trim = [&](std::string& t) {
                if (t.empty())
                    return;
                t.erase(0, t.find_first_not_of(" \t"));
                auto p = t.find_last_not_of(" \t");
                if (p != std::string::npos)
                    t.erase(p + 1);
            };

            while (std::getline(in, line)) {
                std::string l = line;
                trim(l);
                if (l.empty() || l[0] == '#')
                    continue;

                if (l.find("embeddings.preferred_model") != std::string::npos) {
                    auto eq = l.find('=');
                    if (eq != std::string::npos) {
                        std::string v = l.substr(eq + 1);
                        trim(v);
                        if (!v.empty() && v.front() == '"' && v.back() == '"') {
                            v = v.substr(1, v.size() - 2);
                        }
                        preferred = v;
                    }
                    if (!preferred.empty()) {
                        spdlog::debug("Preferred model from config: {}", preferred);
                        return preferred;
                    }
                }
                // daemon.models.preload_models -> take the first
                if (l.find("daemon.models.preload_models") != std::string::npos) {
                    auto eq = l.find('=');
                    if (eq != std::string::npos) {
                        std::string v = l.substr(eq + 1);
                        trim(v);
                        // crude parse: if contains MiniLM or mpnet, prefer ordering
                        if (v.find("all-MiniLM-L6-v2") != std::string::npos) {
                            preferred = "all-MiniLM-L6-v2";
                        } else if (v.find("all-mpnet-base-v2") != std::string::npos) {
                            preferred = "all-mpnet-base-v2";
                        }
                    }
                    if (!preferred.empty()) {
                        spdlog::debug("Preferred model from config preload list: {}", preferred);
                        return preferred;
                    }
                }
            }
        }
    } catch (const std::exception& e) {
        spdlog::debug("Error reading config for preferred model: {}", e.what());
    }

    // 3. Auto-detect from available models (prefer models matching existing DB dim)
    try {
        if (const char* home = std::getenv("HOME")) {
            namespace fs = std::filesystem;
            fs::path models = fs::path(home) / ".yams" / "models";
            std::error_code ec;
            if (fs::exists(models, ec) && fs::is_directory(models, ec)) {
                size_t dbDim = 0;
                try {
                    // Prefer sentinel dim when available
                    if (auto s = read_vector_sentinel_dim(getResolvedDataDir()))
                        dbDim = *s;
                } catch (...) {
                }
                std::vector<std::string> preferences;
                if (dbDim == 384) {
                    preferences = {"all-MiniLM-L6-v2", "all-mpnet-base-v2", "nomic-embed-text-v1.5",
                                   "nomic-embed-text-v1"};
                } else if (dbDim == 768) {
                    preferences = {"all-mpnet-base-v2", "nomic-embed-text-v1.5",
                                   "nomic-embed-text-v1", "all-MiniLM-L6-v2"};
                } else {
                    preferences = {"all-MiniLM-L6-v2", "all-mpnet-base-v2", "nomic-embed-text-v1.5",
                                   "nomic-embed-text-v1"};
                }

                for (const auto& pref : preferences) {
                    fs::path modelPath = models / pref;
                    if (fs::exists(modelPath / "model.onnx", ec)) {
                        spdlog::debug("Auto-detected preferred model: {}", pref);
                        return pref;
                    }
                }

                // If no preferred model found, use the first available
                for (const auto& e : fs::directory_iterator(models, ec)) {
                    if (e.is_directory() && fs::exists(e.path() / "model.onnx", ec)) {
                        preferred = e.path().filename().string();
                        spdlog::debug("Using first available model: {}", preferred);
                        return preferred;
                    }
                }
            }
        }
    } catch (const std::exception& e) {
        spdlog::debug("Error auto-detecting models: {}", e.what());
    }

    return preferred;
}

void ServiceManager::alignVectorComponentDimensions() {
    try {
        if (!embeddingGenerator_)
            return;

        // Prefer the persistent Vector DB dimension when present to avoid downshifts.
        size_t genDim = embeddingGenerator_->getEmbeddingDimension();
        if (genDim == 0)
            genDim = 768; // fallback default
        size_t dbDim = 0;
        try {
            if (vectorDatabase_)
                dbDim = vectorDatabase_->getConfig().embedding_dim;
        } catch (...) {
        }
        size_t targetDim = dbDim > 0 ? dbDim : genDim;

        spdlog::debug("Aligning vector components to dimension: {} (generator={}, db={})",
                      targetDim, genDim, dbDim);

        // Align VectorIndexManager dimension
        if (vectorIndexManager_) {
            if (vectorIndexManager_->getConfig().dimension != targetDim) {
                auto cfg = vectorIndexManager_->getConfig();
                cfg.dimension = targetDim;
                vectorIndexManager_->setConfig(cfg);
                auto rr = vectorIndexManager_->rebuildIndex();
                if (!rr) {
                    spdlog::warn("VectorIndexManager rebuild with dim {} failed: {}", targetDim,
                                 rr.error().message);
                } else {
                    spdlog::info("VectorIndexManager dimension aligned to {}", targetDim);
                }
            }
        }

        // Log vector database dimension for diagnostics
        if (vectorDatabase_) {
            spdlog::info("Vector database dimension check: generator={}, database={}", genDim,
                         vectorDatabase_->getConfig().embedding_dim);
        }
    } catch (const std::exception& e) {
        spdlog::warn("Error aligning vector dimensions: {}", e.what());
    }
}

} // namespace yams::daemon
