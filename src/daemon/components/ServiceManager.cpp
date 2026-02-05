#include <sqlite3.h>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <atomic>
#include <cctype>
#include <cstdlib>
#include <ctime>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <map>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <system_error>
#include <thread>
#include <yams/config/config_helpers.h>
#include <yams/config/config_migration.h>

#ifdef _WIN32
#include <io.h>
#include <windows.h>
#define getpid _getpid
#else
#include <unistd.h>
#endif

// Platform-specific malloc pressure relief for macOS
#ifdef __APPLE__
#include <malloc/malloc.h>
#endif

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/use_future.hpp>
#include <tl/expected.hpp>
#include <yams/api/content_store_builder.h>
#include <yams/app/services/services.hpp>
#include <yams/app/services/session_service.hpp>
#include <yams/compat/thread_stop_compat.h>
#include <yams/config/config_helpers.h>
#include <yams/core/types.h>
#include <yams/daemon/components/BackgroundTaskManager.h>
#include <yams/daemon/components/CheckpointManager.h>
#include <yams/daemon/components/ConfigResolver.h>
#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/DaemonMetrics.h>
#include <yams/daemon/components/DatabaseManager.h>
#include <yams/daemon/components/EmbeddingService.h>
#include <yams/daemon/components/EntityGraphService.h>
#include <yams/daemon/components/gliner_query_extractor.h>
#include <yams/daemon/components/GraphComponent.h>
#include <yams/daemon/components/IngestService.h>
#include <yams/daemon/components/init_utils.hpp>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/PluginManager.h>
#include <yams/daemon/components/PoolManager.h>
#include <yams/daemon/components/ResourceGovernor.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/VectorSystemManager.h>
#include <yams/daemon/ipc/fsm_metrics_registry.h>
#include <yams/daemon/ipc/retrieval_session.h>

#include <yams/daemon/resource/abi_content_extractor_adapter.h>
#include <yams/daemon/resource/abi_model_provider_adapter.h>
#include <yams/daemon/resource/abi_plugin_loader.h>
#include <yams/daemon/resource/abi_symbol_extractor_adapter.h>
#include <yams/daemon/resource/external_plugin_host.h>
#include <yams/daemon/resource/model_provider.h>
#include <yams/daemon/resource/plugin_host.h>
#include <yams/extraction/extraction_util.h>
#include <yams/integrity/repair_manager.h>
#include <yams/metadata/migration.h>
#include <yams/plugins/symbol_extractor_v1.h>
#include <yams/repair/embedding_repair_util.h>
#include <yams/search/reranker_adapter.h>
#include <yams/search/search_engine_builder.h>
#include <yams/vector/sqlite_vec_backend.h>
#include <yams/vector/vector_database.h>

namespace {
// Convenience alias for ConfigResolver timeouts
inline int read_timeout_ms(const char* envName, int defaultMs, int minMs) {
    return yams::daemon::ConfigResolver::readTimeoutMs(envName, defaultMs, minMs);
}

// Template-based plugin adoption helper to reduce code duplication
template <typename AbiTableType, typename AdapterType, typename ContainerValueType>
size_t
adoptPluginInterface(yams::daemon::AbiPluginHost* host, const std::string& interfaceName,
                     int interfaceVersion,
                     std::vector<std::shared_ptr<ContainerValueType>>& targetContainer,
                     const std::function<bool(const AbiTableType*)>& validateTable = nullptr) {
    size_t adopted = 0;
    if (!host)
        return adopted;

    for (const auto& descriptor : host->listLoaded()) {
        // Check if plugin exposes the requested interface
        bool hasInterface = false;
        for (const auto& id : descriptor.interfaces) {
            if (id == interfaceName) {
                hasInterface = true;
                break;
            }
        }
        if (!hasInterface)
            continue;

        // Get the interface table
        auto ifaceRes = host->getInterface(descriptor.name, interfaceName, interfaceVersion);
        if (!ifaceRes)
            continue;

        auto* table = reinterpret_cast<AbiTableType*>(ifaceRes.value());
        if (!table)
            continue;

        // Optional validation
        if (validateTable && !validateTable(table))
            continue;

        // Create adapter and add to container
        try {
            auto adapter = std::make_shared<AdapterType>(table);
            targetContainer.push_back(std::move(adapter));
            ++adopted;
            spdlog::info("Adopted {} from plugin: {}", interfaceName, descriptor.name);
        } catch (const std::exception& e) {
            spdlog::warn("Failed to create adapter for {} from plugin {}: {}", interfaceName,
                         descriptor.name, e.what());
        } catch (...) {
            spdlog::warn("Failed to create adapter for {} from plugin {} (unknown error)",
                         interfaceName, descriptor.name);
        }
    }
    return adopted;
}

} // namespace

// Open the daemon namespace for all following member definitions.
namespace yams::daemon {

using yams::Error;
using yams::ErrorCode;
using yams::Result;
namespace search = yams::search;

ServiceManager::PluginStatusSnapshot ServiceManager::getPluginStatusSnapshot() const {
    // Use try_lock to avoid blocking indefinitely during plugin refresh operations
    std::shared_lock lk(pluginStatusMutex_, std::try_to_lock);
    if (!lk.owns_lock()) {
        spdlog::debug(
            "getPluginStatusSnapshot: mutex busy (refresh in progress?), returning empty");
        return PluginStatusSnapshot{};
    }
    return pluginStatusSnapshot_;
}

void ServiceManager::refreshPluginStatusSnapshot() {
    PluginStatusSnapshot snapshot;
    try {
        // PBI-088: Use delegated FSM snapshot (PluginManager owns the FSM)
        snapshot.host = getPluginHostFsmSnapshot();
        bool providerDegraded = false;
        try {
            auto es = embeddingFsm_.snapshot();
            providerDegraded = (es.state == EmbeddingProviderState::Degraded ||
                                es.state == EmbeddingProviderState::Failed);
        } catch (const std::exception& e) {
            spdlog::debug("Failed to snapshot embedding FSM state: {}", e.what());
        } catch (...) {
            spdlog::debug("Failed to snapshot embedding FSM state: unknown error");
        }
        const bool providerReady = state_.readiness.modelProviderReady.load();
        const auto providerError =
            lifecycleFsm_.degradationReason("embeddings"); // Use lifecycleFsm instead
        const auto modelsLoaded = cachedModelProviderModelCount_.load(std::memory_order_relaxed);
        // Helper lambda to add plugin records
        auto addPluginRecords = [&](const std::vector<PluginDescriptor>& loaded,
                                    [[maybe_unused]] const std::string& pluginType) {
            for (const auto& d : loaded) {
                PluginStatusRecord rec;
                rec.name = d.name;
                rec.interfaces = d.interfaces;
                rec.isProvider =
                    (!adoptedProviderPluginName_.empty() && adoptedProviderPluginName_ == d.name);
                if (rec.isProvider) {
                    rec.ready = providerReady;
                    rec.degraded = providerDegraded;
                    rec.error = providerError;
                    rec.modelsLoaded = modelsLoaded;
                } else {
                    rec.ready = (snapshot.host.state == PluginHostState::Ready);
                    rec.degraded = (snapshot.host.state == PluginHostState::Failed);
                    if (rec.degraded)
                        rec.error = snapshot.host.lastError;
                }
                snapshot.records.push_back(std::move(rec));
            }
        };

        // ABI (native) plugins
        if (abiHost_) {
            auto loaded = abiHost_->listLoaded();
            snapshot.records.reserve(loaded.size());
            addPluginRecords(loaded, "native");
        }

        // PBI-096: External (Python/JS) plugins
        spdlog::info("[refreshPluginStatusSnapshot] Checking external plugins, pluginManager_={}",
                     static_cast<void*>(pluginManager_.get()));
        if (auto* external = getExternalPluginHost()) {
            auto externalLoaded = external->listLoaded();
            spdlog::info("[refreshPluginStatusSnapshot] External host returned {} plugins",
                         externalLoaded.size());
            // Reserve additional space for external plugins
            snapshot.records.reserve(snapshot.records.size() + externalLoaded.size());
            addPluginRecords(externalLoaded, "external");
        }
    } catch (...) {
        // leave snapshot empty on failure
    }
    {
        std::unique_lock lk(pluginStatusMutex_);
        pluginStatusSnapshot_ = std::move(snapshot);
    }
}

ServiceManager::ServiceManager(const DaemonConfig& config, StateComponent& state,
                               DaemonLifecycleFsm& lifecycleFsm)
    : config_(config), state_(state), lifecycleFsm_(lifecycleFsm) {
    spdlog::debug("[ServiceManager] Constructor start");
    tuningConfig_ = config_.tuning;

    ingestWorkerTarget_.store(1, std::memory_order_relaxed);

    // If post_ingest_threads_max is still the default, calculate a better one.
    if (tuningConfig_.postIngestThreadsMax == 8) {
        try {
            // Use 75% of budgeted background threads for post-ingest max.
            auto rec = yams::daemon::TuneAdvisor::recommendedThreads(0.75);
            if (rec > tuningConfig_.postIngestThreadsMax) {
                tuningConfig_.postIngestThreadsMax = rec;
                spdlog::info("Auto-adjusting post-ingest max threads to {}", rec);
            }
        } catch (...) {
            // Ignore errors and proceed with default.
        }
    }

    // Initialize WorkCoordinator (Phase 0c): Unified async work coordination
    spdlog::debug("[ServiceManager] Creating WorkCoordinator...");
    try {
        workCoordinator_ = std::make_unique<WorkCoordinator>();
        auto threadCount = yams::daemon::TuneAdvisor::recommendedThreads();
        workCoordinator_->start(threadCount);
        spdlog::info("[ServiceManager] WorkCoordinator created with {} worker threads (budget {}%)",
                     workCoordinator_->getWorkerCount(),
                     yams::daemon::TuneAdvisor::cpuBudgetPercent());

        // Initialize strands for logical separation
        spdlog::debug("[ServiceManager] Creating strands...");
        auto executor = workCoordinator_->getExecutor();
        initStrand_.emplace(executor);
        pluginStrand_.emplace(executor);
        modelStrand_.emplace(executor);
        spdlog::debug("[ServiceManager] Strands created");
    } catch (const std::exception& e) {
        spdlog::error("Failed to initialize WorkCoordinator: {}", e.what());
        throw;
    }

    try {
        spdlog::debug("ServiceManager constructor start");
        if (!cliRequestPool_) {
            cliRequestPool_ = std::make_unique<boost::asio::thread_pool>(2);
            spdlog::info("[ServiceManager] CLI request pool created with 2 threads");
        }
        refreshPluginStatusSnapshot();
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
            // Intentionally ignored - test-only environment variable parsing
        }
#endif

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

        // NOTE: ExternalPluginHost should be initialized in PluginManager, not ServiceManager.
        // See PBI-093 / RFC-EPH for future integration.

        // Defer vector DB initialization to async phase to avoid blocking daemon startup (PBI-057).
        spdlog::debug("[Startup] deferring vector DB init to async phase");

        // Auto-trust plugin directories (env, config, system) via AbiPluginHost
        // Note: Use abiHost_ directly since PluginManager is created later
        if (abiHost_) {
            // Trust from env
            if (const char* env = std::getenv("YAMS_PLUGIN_DIR")) {
                std::filesystem::path penv(env);
                if (!penv.empty()) {
                    if (auto tr = abiHost_->trustAdd(penv); !tr) {
                        spdlog::warn("Failed to auto-trust YAMS_PLUGIN_DIR {}: {}", penv.string(),
                                     tr.error().message);
                    }
                }
            }

            // Trust from config
            if (!config_.pluginDir.empty()) {
                std::filesystem::path pconf = config_.pluginDir;
                if (auto tr = abiHost_->trustAdd(pconf); !tr) {
                    spdlog::warn("Failed to trust configured pluginDir {}: {}", pconf.string(),
                                 tr.error().message);
                } else {
                    spdlog::debug("Trusted configured pluginDir {}", pconf.string());
                }
            }

            // Trust explicit entries from config ([plugins].trusted_paths or daemon.trusted_paths)
            for (const auto& p : config_.trustedPluginPaths) {
                if (auto tr = abiHost_->trustAdd(p); !tr) {
                    spdlog::warn("Failed to trust configured plugin path {}: {}", p.string(),
                                 tr.error().message);
                } else {
                    spdlog::debug("Trusted configured plugin path {}", p.string());
                }
            }

            // Trust system install location
#ifdef YAMS_INSTALL_PREFIX
            namespace fs = std::filesystem;
            fs::path system_plugins = fs::path(YAMS_INSTALL_PREFIX) / "lib" / "yams" / "plugins";
            if (fs::exists(system_plugins) && fs::is_directory(system_plugins)) {
                if (auto tr = abiHost_->trustAdd(system_plugins)) {
                    spdlog::info("Auto-trusted system plugin directory: {}",
                                 system_plugins.string());
                } else {
                    spdlog::warn("Failed to auto-trust system plugins: {}", tr.error().message);
                }
            }
#endif
        } else {
            spdlog::debug("[ServiceManager] AbiPluginHost not available for trust setup");
        }

        try {
            if (!ingestService_) {
                ingestService_ = std::make_unique<IngestService>(this, workCoordinator_.get());
            }
        } catch (const std::exception& e) {
            spdlog::warn("ServiceManager: failed to initialize IngestService scaffold: {}",
                         e.what());
        } catch (...) {
            spdlog::warn("ServiceManager: unknown error initializing IngestService scaffold");
        }

        // PBI-088: Create extracted managers (wiring in progress)
        try {
            // Create PluginManager
            PluginManager::Dependencies pluginDeps;
            pluginDeps.config = &config_;
            pluginDeps.state = &state_;
            pluginDeps.lifecycleFsm = &lifecycleFsm_;
            pluginDeps.dataDir = config_.dataDir;
            pluginDeps.resolvePreferredModel = [this]() { return this->resolvePreferredModel(); };
            pluginDeps.sharedPluginHost = abiHost_.get();
            pluginManager_ = std::make_unique<PluginManager>(pluginDeps);
            if (auto initResult = pluginManager_->initialize(); !initResult) {
                spdlog::warn("[ServiceManager] PluginManager init failed: {}",
                             initResult.error().message);
            }
            spdlog::debug("[ServiceManager] PluginManager created");

            // Create VectorSystemManager
            VectorSystemManager::Dependencies vectorDeps;
            vectorDeps.state = &state_;
            vectorDeps.serviceFsm = &serviceFsm_;
            vectorDeps.resolvePreferredModel = [this]() { return this->resolvePreferredModel(); };
            vectorDeps.getEmbeddingDimension = [this]() { return this->getEmbeddingDimension(); };
            vectorSystemManager_ = std::make_unique<VectorSystemManager>(vectorDeps);
            spdlog::debug("[ServiceManager] VectorSystemManager created");

            // Create DatabaseManager
            DatabaseManager::Dependencies dbDeps;
            dbDeps.state = &state_;
            databaseManager_ = std::make_unique<DatabaseManager>(dbDeps);
            spdlog::debug("[ServiceManager] DatabaseManager created");

            // Create WALManager (will be initialized later with resolved dataDir)
            try {
                walManager_ = std::make_shared<yams::wal::WALManager>();
                spdlog::debug("[ServiceManager] WALManager created");
            } catch (const std::exception& e) {
                spdlog::warn("[ServiceManager] Failed to create WALManager: {}", e.what());
                // Continue without WAL - metrics will return zeros
            }

            // Create CheckpointManager
            CheckpointManager::Config checkpointConfig;
            checkpointConfig.checkpoint_interval =
                std::chrono::seconds(TuneAdvisor::checkpointIntervalSeconds());
            checkpointConfig.vector_index_insert_threshold =
                TuneAdvisor::checkpointInsertThreshold();
            checkpointConfig.enable_hotzone_persistence = TuneAdvisor::enableHotzoneCheckpoint();
            checkpointConfig.data_dir = config_.dataDir;

            CheckpointManager::Dependencies checkpointDeps;
            checkpointDeps.vectorSystemManager = vectorSystemManager_.get();
            checkpointDeps.hotzoneManager = nullptr;
            checkpointDeps.executor = workCoordinator_->getExecutor();
            checkpointDeps.stopRequested = std::make_shared<std::atomic<bool>>(false);

            checkpointManager_ = std::make_unique<CheckpointManager>(std::move(checkpointConfig),
                                                                     std::move(checkpointDeps));
            spdlog::debug("[ServiceManager] CheckpointManager created");

            // Create SearchComponent for corpus monitoring and auto-rebuild
            searchComponent_ = std::make_unique<SearchComponent>(*this, state_);
            spdlog::debug("[ServiceManager] SearchComponent created");
        } catch (const std::exception& e) {
            spdlog::warn("[ServiceManager] Failed to create extracted managers: {}", e.what());
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

    // Initialize WALManager (after dataDir is resolved)
    if (walManager_) {
        yams::wal::WALManager::Config walConfig;
        walConfig.walDirectory = resolvedDataDir_ / "wal";
        try {
            if (auto result = walManager_->initialize(); !result) {
                spdlog::warn("[ServiceManager] WALManager initialization failed: {}",
                             result.error().message);
                walManager_.reset();
            } else {
                spdlog::info("[ServiceManager] WALManager initialized");
                // Attach to WalMetricsProvider for metrics collection
                attachWalManager(walManager_);
                spdlog::debug("[ServiceManager] WALManager attached to metrics provider");
            }
        } catch (const std::exception& e) {
            spdlog::warn("[ServiceManager] WALManager initialization threw: {}", e.what());
            walManager_.reset();
        }
    }

    // Log plugin scan directories for troubleshooting
    try {
        std::string dirs;
        std::vector<std::filesystem::path> pluginDirs;
#ifdef _WIN32
        // Windows: use LOCALAPPDATA for user plugins
        if (const char* localAppData = std::getenv("LOCALAPPDATA"))
            pluginDirs.push_back(std::filesystem::path(localAppData) / "yams" / "plugins");
        else if (const char* userProfile = std::getenv("USERPROFILE"))
            pluginDirs.push_back(std::filesystem::path(userProfile) / "AppData" / "Local" / "yams" /
                                 "plugins");
#else
        if (const char* home = std::getenv("HOME"))
            pluginDirs.push_back(std::filesystem::path(home) / ".local" / "lib" / "yams" /
                                 "plugins");
        pluginDirs.push_back(std::filesystem::path("/usr/local/lib/yams/plugins"));
        pluginDirs.push_back(std::filesystem::path("/usr/lib/yams/plugins"));
#endif
#ifdef YAMS_INSTALL_PREFIX
        pluginDirs.push_back(std::filesystem::path(YAMS_INSTALL_PREFIX) / "lib" / "yams" /
                             "plugins");
#endif
        for (const auto& d : pluginDirs) {
            if (!dirs.empty())
                dirs += ";";
            dirs += d.string();
        }
        spdlog::info("Plugin scan directories: {}", dirs);
    } catch (const std::exception& e) {
        spdlog::debug("Failed to log plugin directories: {}", e.what());
    } catch (...) {
        spdlog::debug("Failed to log plugin directories: unknown error");
    }

    // File type detector init skipped to reduce compile-time deps; non-fatal fallback remains.

    if (initThread_.joinable()) {
        spdlog::debug("Previous init thread still active; requesting stop before restart");
        initThread_.request_stop();
        initThread_.join();
    }

    // io_context and workers already created in constructor; proceed with initialization
    spdlog::debug("ServiceManager: Using io_context from constructor");

    // Async initialization is now triggered explicitly via startAsyncInit()
    // to allow the daemon main loop to start first.

    // Configure PoolManager defaults from TuneAdvisor for known components
    try {
        PoolManager::Config ipcCfg{};
        ipcCfg.min_size = TuneAdvisor::poolMinSizeIpc();
        if (ipcCfg.min_size < 4) {
            ipcCfg.min_size = 4;
        }
        ipcCfg.max_size = TuneAdvisor::poolMaxSizeIpc();
        ipcCfg.cooldown_ms = TuneAdvisor::poolCooldownMs();
        ipcCfg.low_watermark = TuneAdvisor::poolLowWatermarkPercent();
        ipcCfg.high_watermark = TuneAdvisor::poolHighWatermarkPercent();
        PoolManager::instance().configure("ipc", ipcCfg);

        PoolManager::Config ioCfg{};
        ioCfg.min_size = TuneAdvisor::poolMinSizeIpcIo();
        if (ioCfg.min_size < 2) {
            ioCfg.min_size = 2;
        }
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

        // Seed FsmMetricsRegistry with initial pool sizes for immediate visibility in status
        FsmMetricsRegistry::instance().setIpcPoolSize(static_cast<uint32_t>(ipcCfg.min_size));
        FsmMetricsRegistry::instance().setIoPoolSize(static_cast<uint32_t>(ioCfg.min_size));
    } catch (const std::exception& e) {
        spdlog::debug("PoolManager configure error: {}", e.what());
    }

    // SearchEngine initialization is handled separately via searchEngineManager_
    // (SearchExecutor has been deprecated and removed)
    return Result<void>();
}

void ServiceManager::startAsyncInit(std::promise<void>* barrierPromise,
                                    std::atomic<bool>* barrierSet) {
    bool expected = false;
    if (!asyncInitStarted_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        spdlog::warn("ServiceManager::startAsyncInit() called more than once, ignoring");
        return;
    }

    spdlog::info("ServiceManager: Triggering deferred async initialization");

    if (barrierSet) {
        barrierSet->store(true, std::memory_order_release);
    }

    if (!workCoordinator_ || !workCoordinator_->isRunning()) {
        spdlog::error("ServiceManager: WorkCoordinator not ready, cannot start async init");
        if (barrierPromise) {
            barrierPromise->set_value();
        }
        serviceFsm_.dispatch(InitializationFailedEvent{"WorkCoordinator not ready"});
        return;
    }

    std::shared_ptr<ServiceManager> self;
    try {
        self = shared_from_this();
    } catch (const std::bad_weak_ptr& e) {
        spdlog::error(
            "ServiceManager: shared_from_this() failed - object not managed by shared_ptr: {}",
            e.what());
        if (barrierPromise) {
            barrierPromise->set_value();
        }
        serviceFsm_.dispatch(InitializationFailedEvent{"shared_from_this() failed"});
        return;
    }

    boost::asio::post(workCoordinator_->getExecutor(), [self, barrierPromise]() {
        spdlog::debug("ServiceManager: Async init sync point reached, spawning coroutine");

        boost::asio::co_spawn(
            self->workCoordinator_->getExecutor(),
            [self, barrierPromise]() -> boost::asio::awaitable<void> {
                auto localSelf = self;
                auto localBarrierPromise = barrierPromise;

                spdlog::info("Starting async resource initialization (coroutine)...");

                if (localBarrierPromise) {
                    try {
                        localBarrierPromise->set_value();
                        spdlog::debug("ServiceManager: Async init barrier signaled");
                    } catch (...) {
                    }
                }

                auto token = localSelf->asyncInitStopSource_.get_token();

                try {
                    auto result = co_await localSelf->initializeAsyncAwaitable(token);

                    if (!result) {
                        spdlog::error("Async resource initialization failed: {}",
                                      result.error().message);
                        if (!token.stop_requested()) {
                            localSelf->serviceFsm_.dispatch(
                                InitializationFailedEvent{result.error().message});
                        }
                    } else {
                        spdlog::info("All daemon services initialized successfully");
                    }
                } catch (const std::exception& e) {
                    spdlog::error("Async resource initialization exception: {}", e.what());
                    if (!token.stop_requested()) {
                        localSelf->serviceFsm_.dispatch(InitializationFailedEvent{e.what()});
                    }
                }
            },
            boost::asio::detached);
    });
}

void ServiceManager::shutdown() {
    // FSM-first guard: avoid duplicate shutdown
    try {
        auto ss = serviceFsm_.snapshot();
        if (ss.state == ServiceManagerState::ShuttingDown ||
            ss.state == ServiceManagerState::Stopped) {
            return;
        }
        serviceFsm_.dispatch(ShutdownEvent{});
    } catch (const std::exception& e) {
        spdlog::debug("FSM dispatch failed for ShutdownEvent: {}", e.what());
    } catch (...) {
        spdlog::debug("FSM dispatch failed for ShutdownEvent: unknown error");
    }
    // Ensure shutdown is executed at most once to avoid double-free/use-after-free
    if (shutdownInvoked_.exchange(true, std::memory_order_acq_rel)) {
        spdlog::debug("ServiceManager: shutdown already invoked; skipping.");
        return;
    }

    spdlog::info("[ServiceManager] Shutdown initiated");
    auto shutdownStart = std::chrono::steady_clock::now();

    // Hold components that must outlive WorkCoordinator shutdown.
    // We move these out of member storage during early shutdown phases to prevent
    // accidental reuse, while keeping the objects alive until we finish draining threads.
    std::unique_ptr<CheckpointManager> checkpointManagerHold;

    // Phase 0: Signal async init coroutine to stop and wait for it to complete
    // This prevents the coroutine from accessing resources we're about to tear down
    spdlog::info("[ServiceManager] Phase 0: Requesting async init stop");
    if (asyncInitStopSource_.stop_possible()) {
        asyncInitStopSource_.request_stop();
    }

    // Phase 1: Stop background task consumers FIRST (before io_context stop)
    // This signals coroutines to exit gracefully before we stop the io_context
    spdlog::info("[ServiceManager] Phase 1: Stopping background task manager");
    auto phase1Start = std::chrono::steady_clock::now();
    if (backgroundTaskManager_) {
        try {
            backgroundTaskManager_->stop();
            auto phase1Duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - phase1Start);
            spdlog::info("[ServiceManager] Phase 1: Background task manager stopped ({}ms)",
                         phase1Duration.count());
        } catch (const std::exception& e) {
            spdlog::warn("[ServiceManager] Phase 1: Background task manager stop failed: {}",
                         e.what());
        }
    } else {
        spdlog::info("[ServiceManager] Phase 1: No background task manager to stop");
    }

    // Phase 2: Signal stop to session watcher and wait for it to complete
    // We must wait for the session watcher BEFORE stopping the io_context, because:
    // - The session watcher is a coroutine suspended on a timer
    // - When we request stop, it will exit on its next timer wake-up
    // - If we stop io_context first, worker threads exit and the coroutine can never resume
    spdlog::info("[ServiceManager] Phase 2: Signaling session watcher stop");
    try {
        if (sessionWatchStopSource_.stop_possible())
            sessionWatchStopSource_.request_stop();
        spdlog::info("[ServiceManager] Phase 2: Session watcher stop requested");
    } catch (const std::exception& e) {
        spdlog::warn("[ServiceManager] Phase 2: Session watcher stop signal failed: {}", e.what());
    } catch (...) {
        spdlog::warn("[ServiceManager] Phase 2: Session watcher stop signal failed");
    }

    // Phase 2.5: Wait for session watcher to complete (it checks stop_requested on timer wake)
    // Timer interval is 2s by default, so wait up to 3s for graceful completion
    spdlog::info("[ServiceManager] Phase 2.5: Waiting for session watcher to complete");
    try {
        if (sessionWatcherFuture_.valid()) {
            auto status = sessionWatcherFuture_.wait_for(std::chrono::seconds(3));
            if (status == std::future_status::timeout) {
                spdlog::warn("[ServiceManager] Phase 2.5: Session watcher future timed out");
            } else {
                sessionWatcherFuture_.get();
                spdlog::info("[ServiceManager] Phase 2.5: Session watcher completed");
            }
            sessionWatcherFuture_ = std::future<void>();
        }
    } catch (const std::exception& e) {
        spdlog::warn("[ServiceManager] Phase 2.5: Session watcher stop failed: {}", e.what());
    } catch (...) {
        spdlog::warn("[ServiceManager] Phase 2.5: Session watcher stop failed");
    }

    // Phase 3: (Removed - work guard now managed by WorkCoordinator)
    spdlog::info("[ServiceManager] Phase 3: Skipped (work guard managed by WorkCoordinator)");

    // Phase 3.5: Stop CLI request pool to avoid starving shutdown
    if (cliRequestPool_) {
        try {
            cliRequestPool_->stop();
            cliRequestPool_->join();
            cliRequestPool_.reset();
            spdlog::info("[ServiceManager] Phase 3.5: CLI request pool stopped");
        } catch (const std::exception& e) {
            spdlog::warn("[ServiceManager] Phase 3.5: CLI request pool stop failed: {}", e.what());
        } catch (...) {
            spdlog::warn("[ServiceManager] Phase 3.5: CLI request pool stop failed");
        }
    }

    // Phase 3.6: Stop CheckpointManager before WorkCoordinator
    spdlog::info("[ServiceManager] Phase 3.6: Stopping CheckpointManager");
    if (checkpointManager_) {
        checkpointManager_->stop();
        checkpointManagerHold = std::move(checkpointManager_);
        spdlog::info("[ServiceManager] Phase 3.6: CheckpointManager stopped");
    }

    // Phase 4: Cancel all asynchronous operations and stop WorkCoordinator io_context
    spdlog::info("[ServiceManager] Phase 4: Cancelling async operations");
    shutdownSignal_.emit(boost::asio::cancellation_type::terminal);
    if (workCoordinator_) {
        workCoordinator_->stop();
        spdlog::info("[ServiceManager] Phase 4: WorkCoordinator stop() called");
    }

    // Phase 5: Join worker threads with timeout to ensure no threads are accessing
    // shared resources when we start resetting them. This prevents race conditions
    // during shutdown. Use timeout to avoid hanging on long-running operations.
    spdlog::info("[ServiceManager] Phase 5: Joining WorkCoordinator threads");
    if (workCoordinator_) {
        try {
            constexpr auto kShutdownTimeout = std::chrono::seconds(5);
            if (!workCoordinator_->joinWithTimeout(kShutdownTimeout)) {
                spdlog::warn("[ServiceManager] Phase 5: WorkCoordinator timed out after 5s, "
                             "force-stopping remaining workers");
            } else {
                spdlog::info("[ServiceManager] Phase 5: WorkCoordinator threads joined");
            }
        } catch (const std::exception& e) {
            spdlog::warn("[ServiceManager] Phase 5: WorkCoordinator join failed: {}", e.what());
        }
    }

    // Phase 6: Stop services in reverse dependency order
    spdlog::info("[ServiceManager] Phase 6: Shutting down daemon services");

    spdlog::info("[ServiceManager] Phase 6.1: Stopping ingest service");
    if (ingestService_) {
        try {
            ingestService_->stop();
            ingestService_.reset();
            spdlog::info("[ServiceManager] Phase 6.1: Ingest service stopped");
        } catch (const std::exception& e) {
            spdlog::warn("[ServiceManager] Phase 6.1: IngestService shutdown failed: {}", e.what());
        }
    } else {
        spdlog::info("[ServiceManager] Phase 6.1: No ingest service to stop");
    }

    spdlog::info("[ServiceManager] Phase 6.2: Shutting down graph component");
    if (graphComponent_) {
        try {
            graphComponent_->shutdown();
            graphComponent_.reset();
            spdlog::info("[ServiceManager] Phase 6.2: Graph component shut down");
        } catch (const std::exception& e) {
            spdlog::warn("[ServiceManager] Phase 6.2: GraphComponent shutdown failed: {}",
                         e.what());
        }
    }

    spdlog::info("[ServiceManager] Phase 6.3: Resetting post-ingest queue");
    if (postIngest_) {
        postIngest_.reset();
        spdlog::info("[ServiceManager] Phase 6.3: Post-ingest queue reset");
    } else {
        spdlog::info("[ServiceManager] Phase 6.3: No post-ingest queue to reset");
    }

    spdlog::info("[ServiceManager] Phase 6.3.5: Shutting down embedding service");
    if (embeddingService_) {
        embeddingService_->shutdown();
        embeddingService_.reset();
        spdlog::info("[ServiceManager] Phase 6.3.5: Embedding service shutdown complete");
    } else {
        spdlog::info("[ServiceManager] Phase 6.3.5: No embedding service to shutdown");
    }

    spdlog::info("[ServiceManager] Phase 6.3.6: Shutting down KG write queue");
    if (kgWriteQueue_) {
        kgWriteQueue_->shutdown();
        kgWriteQueue_.reset();
        spdlog::info("[ServiceManager] Phase 6.3.6: KG write queue shutdown complete");
    } else {
        spdlog::info("[ServiceManager] Phase 6.3.6: No KG write queue to shutdown");
    }

    // No vector index to save - using VectorDatabase directly
    spdlog::info("[ServiceManager] Phase 6.4: Vector search uses VectorDatabase directly");

    // Model provider manages embedding lifecycle, no separate shutdown needed
    spdlog::info("[ServiceManager] Phase 6.5: Embedding lifecycle managed by model provider");

    spdlog::info("[ServiceManager] Phase 6.6: Shutting down model provider");
    if (modelProvider_) {
        try {
            auto loaded = modelProvider_->getLoadedModels();
            for (const auto& name : loaded) {
                (void)modelProvider_->unloadModel(name);
            }
            modelProvider_->shutdown();
            modelProvider_.reset();
        } catch (const std::exception& e) {
            spdlog::warn("[ServiceManager] Phase 6.6: Model provider shutdown failed: {}",
                         e.what());
            modelProvider_.reset();
        }
    } else {
        spdlog::info("[ServiceManager] Phase 6.6: No model provider to shut down");
    }

    // Shutdown search engine
    spdlog::info("[ServiceManager] Phase 6.7: Resetting search engine");
    if (searchEngine_) {
        searchEngine_.reset();
        spdlog::info("[ServiceManager] Phase 6.7: Search engine reset");
    } else {
        spdlog::info("[ServiceManager] Phase 6.7: No search engine to reset");
    }

    // Shutdown retrieval sessions
    spdlog::info("[ServiceManager] Phase 6.8: Resetting retrieval sessions");
    if (retrievalSessions_) {
        retrievalSessions_.reset();
        spdlog::info("[ServiceManager] Phase 6.8: Retrieval sessions reset");
    } else {
        spdlog::info("[ServiceManager] Phase 6.8: No retrieval sessions to reset");
    }

    // Shutdown plugins (prefer ABI host)
    spdlog::info("[ServiceManager] Phase 6.9: Unloading plugins");
    try {
        if (abiHost_) {
            auto loaded = abiHost_->listLoaded();
            spdlog::info("[ServiceManager] Phase 6.9: Unloading {} plugins", loaded.size());
            for (const auto& d : loaded) {
                (void)abiHost_->unload(d.name);
            }
            spdlog::info("[ServiceManager] Phase 6.9: All plugins unloaded");
        } else {
            spdlog::info("[ServiceManager] Phase 6.9: No ABI host, no plugins to unload");
        }
    } catch (...) {
        spdlog::warn("[ServiceManager] Phase 6.9: Exception during plugin unloading");
    }

    spdlog::info("[ServiceManager] Phase 7: Shutting down database");
    try {
        if (connectionPool_) {
            connectionPool_->shutdown();
            connectionPool_.reset();
        }
        if (database_) {
            database_->close();
            database_.reset();
        } else {
            spdlog::info("[ServiceManager] Phase 7.2: No database to close");
        }
        spdlog::info("[ServiceManager] Phase 7: Database shutdown complete");
    } catch (const std::exception& e) {
        spdlog::error("[ServiceManager] Phase 7: Database shutdown failed: {}", e.what());
    }

    // Release all remaining resources
    spdlog::info("[ServiceManager] Phase 8: Releasing remaining resources");
    metadataRepo_.reset();
    spdlog::info("[ServiceManager] Phase 9.2: Metadata repository reset");
    spdlog::info("[ServiceManager] Phase 8.3: Vector search uses VectorDatabase directly");
    contentStore_.reset();
    spdlog::info("[ServiceManager] Phase 8.4: Content store reset");

    spdlog::info("[ServiceManager] Phase 8.5: Releasing WorkCoordinator");
    workCoordinator_.reset(); // WorkCoordinator destructor will join threads

#ifdef __APPLE__
    malloc_zone_pressure_relief(nullptr, 0);
#endif

    // PBI-088: Shutdown extracted managers BEFORE plugin infrastructure
    // (PluginManager holds raw pointer to abiHost_ via sharedPluginHost_)
    spdlog::info("[ServiceManager] Phase 9: Releasing extracted managers");
    try {
        if (pluginManager_) {
            pluginManager_->shutdown();
            pluginManager_.reset();
            spdlog::info("[ServiceManager] Phase 9.1: PluginManager reset");
        }
    } catch (...) {
        spdlog::warn("[ServiceManager] Phase 9.1: Exception resetting PluginManager");
    }
    try {
        if (vectorSystemManager_) {
            vectorSystemManager_->shutdown();
            vectorSystemManager_.reset();
            spdlog::info("[ServiceManager] Phase 9.2: VectorSystemManager reset");
        }
    } catch (...) {
        spdlog::warn("[ServiceManager] Phase 9.2: Exception resetting VectorSystemManager");
    }
    try {
        if (databaseManager_) {
            databaseManager_->shutdown();
            databaseManager_.reset();
            spdlog::info("[ServiceManager] Phase 9.3: DatabaseManager reset");
        }
    } catch (...) {
        spdlog::warn("[ServiceManager] Phase 9.3: Exception resetting DatabaseManager");
    }
    try {
        if (walManager_) {
            auto result = walManager_->shutdown();
            if (!result) {
                spdlog::warn("[ServiceManager] Phase 9.4: WALManager shutdown failed: {}",
                             result.error().message);
            } else {
                spdlog::info("[ServiceManager] Phase 9.4: WALManager shutdown complete");
            }
            walManager_.reset();
        }
    } catch (...) {
        spdlog::warn("[ServiceManager] Phase 9.4: Exception resetting WALManager");
    }

    spdlog::info("[ServiceManager] Phase 10: Releasing plugin infrastructure");
    try {
        abiPluginLoader_.reset();
        spdlog::info("[ServiceManager] Phase 10.1: ABI plugin loader reset");
    } catch (...) {
        spdlog::warn("[ServiceManager] Phase 10.1: Exception resetting ABI plugin loader");
    }
    try {
        abiHost_.reset();
        spdlog::info("[ServiceManager] Phase 10.2: ABI host reset");
    } catch (...) {
        spdlog::warn("[ServiceManager] Phase 10.2: Exception resetting ABI host");
    }

    auto shutdownDuration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - shutdownStart);
    spdlog::info("[ServiceManager] Shutdown complete ({}ms total)", shutdownDuration.count());

    try {
        serviceFsm_.dispatch(ServiceManagerStoppedEvent{});
    } catch (...) {
        spdlog::warn("[ServiceManager] Failed to dispatch ServiceManagerStoppedEvent");
    }
}

// Single-attempt vector database initialization. Safe to call multiple times; only
// the first invocation performs work. Subsequent calls are cheap no-ops.
// NOTE: Implementation delegated to VectorSystemManager (PBI-088 decomposition)
yams::Result<bool>
ServiceManager::initializeVectorDatabaseOnce(const std::filesystem::path& dataDir) {
    if (vectorSystemManager_) {
        auto result = vectorSystemManager_->initializeOnce(dataDir);
        if (result && result.value()) {
            // Sync local members from VectorSystemManager for backward compatibility
            vectorDatabase_ = vectorSystemManager_->getVectorDatabase();
        }
        return result;
    }
    // Fallback if VectorSystemManager not available (shouldn't happen in normal flow)
    spdlog::warn("[VectorInit] VectorSystemManager not initialized");
    return Result<bool>(false);
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
            int remain_by_pct = ServiceManager::computeEtaRemaining(exp, progress);
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
    spdlog::info("[ServiceManager] Async initialization started.");
    // Defer Vector DB init to post-plugins phase; skip here
    spdlog::info("[ServiceManager] Phase: Vector DB Init (skipped pre-plugins).");
    spdlog::debug("ServiceManager(co): Initializing daemon resources");
    spdlog::default_logger()->flush(); // Flush before potentially crashing
    writeBootstrapStatusFile(config_, state_);
    spdlog::debug("ServiceManager(co): writeBootstrapStatusFile done");
    spdlog::default_logger()->flush();

    // read_timeout_ms is now provided by ConfigResolver alias above

    using namespace std::chrono_literals;
    spdlog::debug("ServiceManager(co): about to co_await executor");
    spdlog::default_logger()->flush();
    auto ex = co_await boost::asio::this_coro::executor;
    spdlog::debug("ServiceManager(co): co_await executor done");
    spdlog::default_logger()->flush();

    // Set up SearchEngineManager rebuild callback for FSM-driven rebuilds
    // This callback is invoked when the FSM determines it's time to rebuild
    // (e.g., after indexing drains when in AwaitingDrain state)
    searchEngineManager_.setRebuildCallback([this, ex](const std::string& reason,
                                                       bool includeVector) {
        spdlog::info("[ServiceManager] FSM triggered rebuild: reason={} includeVector={}", reason,
                     includeVector);

        // Post the async rebuild to the executor
        boost::asio::co_spawn(
            ex,
            [this, reason, includeVector]() -> boost::asio::awaitable<void> {
                try {
                    std::shared_ptr<vector::EmbeddingGenerator> embGen;
                    if (includeVector && modelProvider_) {
                        try {
                            embGen = modelProvider_->getEmbeddingGenerator();
                        } catch (...) {
                        }
                    }

                    int build_timeout = 30000; // 30s timeout
                    auto result = co_await searchEngineManager_.buildEngine(
                        metadataRepo_, vectorDatabase_, embGen, reason, build_timeout,
                        getWorkerExecutor());

                    if (result.has_value()) {
                        state_.readiness.searchEngineReady.store(true);
                        spdlog::info("[ServiceManager] FSM-triggered rebuild succeeded");
                    } else {
                        spdlog::error("[ServiceManager] FSM-triggered rebuild failed: {}",
                                      result.error().message);
                    }
                } catch (const std::exception& e) {
                    spdlog::error("[ServiceManager] FSM-triggered rebuild exception: {}", e.what());
                }
                co_return;
            },
            boost::asio::detached);
    });

    // Plugins step: mark ready (host scaffolding) and record duration uniformly
    spdlog::info("[ServiceManager] Phase: Plugins Ready.");
    try {
        (void)init::record_duration(
            "plugins",
            [&]() -> yams::Result<void> {
                try {
                    const auto ps = getPluginHostFsmSnapshot();
                    state_.readiness.pluginsReady = (ps.state == PluginHostState::Ready);
                } catch (...) {
                    // Best-effort legacy flag; plugin readiness is authoritative via PluginHostFsm.
                    state_.readiness.pluginsReady = true;
                }
                return yams::Result<void>();
            },
            state_.initDurationsMs);
    } catch (...) {
        try {
            const auto ps = getPluginHostFsmSnapshot();
            state_.readiness.pluginsReady = (ps.state == PluginHostState::Ready);
        } catch (...) {
            state_.readiness.pluginsReady = true;
        }
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
    spdlog::info("[ServiceManager] Phase: Data Dir Resolved.");
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
            // Use atomic_store for thread-safe write (read by main thread via getContentStore())
            std::atomic_store_explicit(
                &contentStore_, std::shared_ptr<yams::api::IContentStore>(uniqueStore.release()),
                std::memory_order_release);
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
    spdlog::info("[ServiceManager] Phase: Content Store Initialized.");

    if (token.stop_requested())
        co_return Error{ErrorCode::OperationCancelled, "Shutdown requested"};

    // Phase: Open metadata DB with timeout (awaitable helper)
    auto dbPath = dataDir / "yams.db";
    database_ = std::make_shared<metadata::Database>();
    int open_timeout = read_timeout_ms("YAMS_DB_OPEN_TIMEOUT_MS", 5000, 250);

    // Check stop again before FSM transition
    if (token.stop_requested())
        co_return Error{ErrorCode::OperationCancelled, "Shutdown requested"};

    // FSM: opening database (catch exceptions during shutdown race)
    try {
        serviceFsm_.dispatch(OpeningDatabaseEvent{});
    } catch (const std::exception& e) {
        spdlog::warn("[ServiceManager] FSM dispatch during shutdown: {}", e.what());
        co_return Error{ErrorCode::OperationCancelled, "FSM dispatch failed during shutdown"};
    } catch (...) {
        spdlog::warn("[ServiceManager] FSM dispatch during shutdown (unknown exception)");
        co_return Error{ErrorCode::OperationCancelled, "FSM dispatch failed during shutdown"};
    }
    bool db_ok = co_await init::await_record_duration(
        "database",
        [&]() -> boost::asio::awaitable<bool> {
            co_return co_await co_openDatabase(dbPath, open_timeout, token);
        },
        state_.initDurationsMs);
    writeBootstrapStatusFile(config_, state_);
    spdlog::info("[ServiceManager] Phase: Database Opened.");
    if (db_ok) {
        try {
            serviceFsm_.dispatch(DatabaseOpenedEvent{});
        } catch (...) {
        }
    }

    // Phase: Migrations (if DB ok)
    if (db_ok) {
        int mig_timeout = read_timeout_ms("YAMS_DB_MIGRATE_TIMEOUT_MS", 7000, 250);
        try {
            serviceFsm_.dispatch(MigrationStartedEvent{});
        } catch (...) {
        }
        bool mig_ok = co_await init::await_record_duration(
            "migrations",
            [&]() -> boost::asio::awaitable<bool> {
                co_return co_await co_migrateDatabase(mig_timeout, token);
            },
            state_.initDurationsMs);
        if (mig_ok) {
            try {
                serviceFsm_.dispatch(MigrationCompletedEvent{});
            } catch (...) {
            }
        }
    }
    spdlog::info("[ServiceManager] Phase: Database Migrated.");

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
        dbPoolCfg.maxConnections = 64; // Increased from 32 to handle heavy concurrent indexing
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
        TuneAdvisor::setStoragePoolSize(static_cast<uint32_t>(dbPoolCfg.maxConnections));
        TuneAdvisor::setEnableParallelIngest(true);
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
                    // Initialize component-owned metrics (sync with DB once at startup)
                    metadataRepo_->initializeCounters();
                    metadataRepo_->warmValueCountsCache(); // Pre-warm common queries
                    spdlog::info("Metadata repository initialized successfully");

                    // Note: RepairManager initialization deferred to RepairCoordinator
                    // since it needs access to storage engine which is not directly
                    // exposed by IContentStore interface

                    return yams::Result<void>();
                },
                state_.initDurationsMs);
            if (!repoRes) {
                spdlog::warn("Metadata repository init failed: {}", repoRes.error().message);
            } else {
                auto journalRes = connectionPool_->withConnection(
                    [](metadata::Database& db) -> Result<std::string> {
                        auto stmtRes = db.prepare("PRAGMA journal_mode");
                        if (!stmtRes) {
                            return stmtRes.error();
                        }
                        auto stmt = std::move(stmtRes).value();
                        auto stepRes = stmt.step();
                        if (!stepRes) {
                            return stepRes.error();
                        }
                        if (!stepRes.value()) {
                            return Error{ErrorCode::NotFound,
                                         "PRAGMA journal_mode returned no rows"};
                        }
                        return stmt.getString(0);
                    });
                if (journalRes) {
                    spdlog::info("Metadata DB journal_mode={}", journalRes.value());
                } else {
                    spdlog::warn("Failed to read Metadata DB journal_mode: {}",
                                 journalRes.error().message);
                }
            }
        }
        writeBootstrapStatusFile(config_, state_);
    }
    spdlog::info("[ServiceManager] Phase: DB Pool and Repo Initialized.");

    // Phase: mark vectors ready (vector backend initialization is opportunistic)
    try {
        serviceFsm_.dispatch(VectorsInitializedEvent{});
    } catch (...) {
    }

    // Executors and sessions
    // Lightweight session directory watcher (polling), reacts to SessionService config.
    auto isTruthy = [](const char* s) {
        if (!s)
            return false;
        std::string v(s);
        std::transform(v.begin(), v.end(), v.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        return v == "1" || v == "true" || v == "yes" || v == "on";
    };
    // Session watcher is opt-in: it should not run by default in daemon mode.
    // Enable explicitly via `YAMS_ENABLE_SESSION_WATCHER=1`.
    const bool enableSessionWatcher = isTruthy(std::getenv("YAMS_ENABLE_SESSION_WATCHER"));
    if (!enableSessionWatcher) {
        spdlog::info("[ServiceManager] Session watcher disabled (default); set "
                     "YAMS_ENABLE_SESSION_WATCHER=1 to enable");
    } else {
        try {
            auto exec = getWorkerExecutor();
            sessionWatchStopSource_ = yams::compat::stop_source{};
            auto watcherToken = sessionWatchStopSource_.get_token();
            sessionWatcherFuture_ = boost::asio::co_spawn(
                exec,
                [this, watcherToken]() -> boost::asio::awaitable<void> {
                    co_await co_runSessionWatcher(watcherToken);
                },
                boost::asio::use_future);
        } catch (...) {
        }
    }

    retrievalSessions_ = std::make_unique<RetrievalSessionManager>();
    spdlog::info("[ServiceManager] Phase: Sessions Initialized.");

    // Initialize post-ingest queue (decouple extraction/index/graph from add paths)
    try {
        using TA = yams::daemon::TuneAdvisor;
        uint32_t taThreads = 0;
        try {
            taThreads = TA::postIngestThreads();
        } catch (...) {
        }
        (void)taThreads; // Retrieved for future use in post-ingest configuration
        // Initialize KG store on daemon side using connection pool if available
        try {
            if (connectionPool_) {
                metadata::KnowledgeGraphStoreConfig kgCfg;
                kgCfg.enable_alias_fts = true;
                kgCfg.enable_wal = true;
                auto kgRes = metadata::makeSqliteKnowledgeGraphStore(*connectionPool_, kgCfg);
                if (kgRes) {
                    auto uniqueKg = std::move(kgRes).value();
                    // Promote to shared_ptr for broader use and store as member
                    kgStore_ = std::shared_ptr<metadata::KnowledgeGraphStore>(std::move(uniqueKg));
                    // PBI-043-12: Wire KG store to metadata repository for tree diff integration
                    if (metadataRepo_) {
                        metadataRepo_->setKnowledgeGraphStore(kgStore_);
                        spdlog::info(
                            "KG store wired to metadata repository for tree diff integration");
                    }
                    // PBI-009: Initialize GraphComponent after KG store is ready
                    try {
                        graphComponent_ =
                            std::make_shared<GraphComponent>(metadataRepo_, kgStore_, this);
                        auto initResult = graphComponent_->initialize();
                        if (!initResult) {
                            spdlog::warn("GraphComponent initialization failed: {}",
                                         initResult.error().message);
                            graphComponent_.reset();
                        } else {
                            spdlog::info("GraphComponent initialized successfully");
                            if (metadataRepo_) {
                                metadataRepo_->setGraphComponent(graphComponent_);
                                spdlog::info("GraphComponent wired to metadata repository");
                            }
                        }
                    } catch (const std::exception& e) {
                        spdlog::warn("GraphComponent init failed: {}", e.what());
                    }
                }
            }
        } catch (...) {
        }
        auto qcap = static_cast<std::size_t>(TA::postIngestQueueMax());
        postIngest_ = std::make_unique<PostIngestQueue>(
            contentStore_, metadataRepo_, contentExtractors_, kgStore_, graphComponent_,
            workCoordinator_.get(), nullptr, qcap);
        postIngest_->start();

        try {
            if (config_.tuning.postIngestCapacity > 0)
                postIngest_->setCapacity(config_.tuning.postIngestCapacity);
        } catch (...) {
        }
        spdlog::info("Post-ingest queue initialized (capacity={})", qcap);

        // Wire PostIngestQueue drain to SearchEngineManager FSM
        postIngest_->setDrainCallback([this]() {
            spdlog::debug(
                "[ServiceManager] PostIngestQueue drained, signaling SearchEngineManager");
            searchEngineManager_.signalIndexingDrained();
        });
    } catch (const std::exception& e) {
        spdlog::warn("Post-ingest queue init failed: {}", e.what());
    } catch (...) {
        spdlog::warn("Post-ingest queue init failed (unknown)");
    }
    spdlog::info("[ServiceManager] Phase: Post-Ingest Queue Initialized.");

    // Initialize EmbeddingService for async embedding generation
    try {
        using TA = yams::daemon::TuneAdvisor;
        uint32_t taThreads = 0;
        try {
            taThreads = TA::postIngestThreads();
        } catch (...) {
        }
        (void)taThreads; // Retrieved for future use in embedding service configuration
        embeddingService_ = std::make_unique<EmbeddingService>(contentStore_, metadataRepo_,
                                                               workCoordinator_.get());

        auto initRes = embeddingService_->initialize();
        if (initRes) {
            embeddingService_->setProviders([this]() { return this->modelProvider_; },
                                            [this]() { return this->resolvePreferredModel(); },
                                            [this]() { return this->vectorDatabase_; });
            embeddingService_->start();
            spdlog::info("EmbeddingService initialized");
        } else {
            spdlog::warn("EmbeddingService initialization failed: {}", initRes.error().message);
            embeddingService_.reset();
        }
    } catch (const std::exception& e) {
        spdlog::warn("EmbeddingService init failed: {}", e.what());
        embeddingService_.reset();
    } catch (...) {
        spdlog::warn("EmbeddingService init failed (unknown)");
        embeddingService_.reset();
    }
    spdlog::info("[ServiceManager] Phase: EmbeddingService Initialized.");

    // Initialize KGWriteQueue for serialized KG writes (internal infrastructure, not a phase)
    try {
        auto kgStore = getKgStore();
        if (kgStore && workCoordinator_) {
            KGWriteQueue::Config queueConfig;
            queueConfig.maxBatchSize = 50;
            queueConfig.maxBatchDelayMs = std::chrono::milliseconds(100);
            queueConfig.channelCapacity = 1000;

            kgWriteQueue_ = std::make_unique<KGWriteQueue>(*workCoordinator_->getIOContext(),
                                                           kgStore, queueConfig);
            kgWriteQueue_->start();
            if (postIngest_) {
                postIngest_->setKgWriteQueue(kgWriteQueue_.get());
            }
            spdlog::debug("[ServiceManager] KGWriteQueue started");
        }
    } catch (const std::exception& e) {
        spdlog::warn("[ServiceManager] KGWriteQueue init failed: {}", e.what());
    } catch (...) {
    }

    // Cross-encoder reranker is initialized later after plugins are loaded
    // See the reranker wiring after search engine build in searchEngine setup

    // Defer Vector DB initialization until after plugin adoption (provider dim)
    spdlog::info("[ServiceManager] Phase: Vector DB Init (deferred until after plugins).");

    // VectorIndexManager removed - using VectorDatabase directly for vector search
    // Mark vector index as ready since VectorDatabase handles all vector operations
    if (vectorDatabase_) {
        state_.readiness.vectorIndexReady = true;
        writeBootstrapStatusFile(config_, state_);
    }
    spdlog::info("[ServiceManager] Phase: Vector search uses VectorDatabase directly.");

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

        // Detect embedding preload flag early so we can use it during plugin adoption
        embeddingPreloadOnStartup_ = detectEmbeddingPreloadFlag();
        spdlog::info("ServiceManager: embeddingPreloadOnStartup={}", embeddingPreloadOnStartup_);

        if (enableAutoload) {
            auto loadResult = co_await init::await_step(
                "plugin_autoload_now",
                [&]() -> boost::asio::awaitable<Result<size_t>> { return autoloadPluginsNow(); });
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
                spdlog::info("ServiceManager: Adopted model provider from plugins.");
                spdlog::info(
                    "ServiceManager: Model provider ready, embeddings will be generated on-demand");
            } else {
                spdlog::warn("ServiceManager: No model provider adopted from plugins.");
                if (config_.enableModelProvider) {
                    co_return Error{ErrorCode::NotInitialized,
                                    "Failed to adopt a model provider from plugins. Check "
                                    "plugin paths and trust settings."};
                }
            }
            auto extractorResult = init::step<size_t>(
                "adopt_extractors", [&]() { return adoptContentExtractorsFromHosts(); });
            if (extractorResult) {
                spdlog::info("ServiceManager: Adopted {} content extractors from plugins.",
                             extractorResult.value());
                // Note: Binary extraction via Ghidra is provided by the yams_ghidra plugin
                // which implements content_extractor_v1 and is adopted above
            }

            // Respect config flag plugins.symbol_extraction.enable (default: true)
            bool enableSymbols = ConfigResolver::isSymbolExtractionEnabled(config_);
            if (enableSymbols) {
                auto symRes = init::step<size_t>(
                    "adopt_symbol_extractors", [&]() { return adoptSymbolExtractorsFromHosts(); });
                if (symRes) {
                    spdlog::info("ServiceManager: Adopted {} symbol extractors.", symRes.value());
                }
            } else {
                spdlog::info("ServiceManager: symbol extractor plugins disabled by config");
            }

            auto entityRes = init::step<size_t>("adopt_entity_extractors",
                                                [&]() { return adoptEntityExtractorsFromHosts(); });
            if (entityRes) {
                spdlog::info("ServiceManager: Adopted {} entity extractors.", entityRes.value());
            }
        }
        // If autoload is disabled but model provider is enabled, defer initialization
        // until after daemon reaches Ready state (see daemon.cpp main loop)
        if (!enableAutoload && config_.enableModelProvider) {
            spdlog::info("Model provider enabled with autoload disabled; deferring initialization "
                         "until Ready");
        }
    } catch (const std::exception& e) {
        spdlog::warn("Plugin autoload failed: {}", e.what());
    }
    spdlog::info("[ServiceManager] Phase: Plugins Autoloaded.");
    // Update pluginsReady flag after actual loading completes
    try {
        const auto ps = getPluginHostFsmSnapshot();
        state_.readiness.pluginsReady = (ps.state == PluginHostState::Ready);
    } catch (...) {
        state_.readiness.pluginsReady = true;
    }
    refreshPluginStatusSnapshot();

    spdlog::info("[ServiceManager] Phase: Vector DB Init (post-plugins, sync).");
    {
        auto vdbRes = initializeVectorDatabaseOnce(dataDir);
        if (!vdbRes) {
            spdlog::warn("[ServiceManager] Vector DB init failed: {}", vdbRes.error().message);
        } else if (vdbRes.value()) {
            spdlog::info("[ServiceManager] Vector DB initialized successfully");
            // Log vector count from database
            if (vectorDatabase_) {
                auto dbVectorCount = vectorDatabase_->getVectorCount();
                if (dbVectorCount > 0) {
                    spdlog::info("[VectorInit] Found {} vectors in database", dbVectorCount);
                }
            }
        } else {
            spdlog::info("[ServiceManager] Vector DB init deferred (dim unresolved)");
        }
    }

    // Only schedule warmup if vector DB is present with non-zero dim
    if (embeddingPreloadOnStartup_) {
        size_t vdim = 0;
        try {
            if (vectorDatabase_)
                vdim = vectorDatabase_->getConfig().embedding_dim;
        } catch (...) {
        }
        if (vdim == 0) {
            spdlog::info("[Warmup] deferred: vector DB not ready or dim=0");
            embeddingPreloadOnStartup_ = false;
        } else {
            spdlog::info("[Warmup] embeddings.preload_on_startup detected -> background warmup "
                         "will run after Ready");
        }
    }

    // Build SearchEngine with timeout
    try {
        state_.readiness.searchProgress = 10;
        writeBootstrapStatusFile(config_, state_);
        if (metadataRepo_) {
            state_.readiness.searchProgress = 40;
            writeBootstrapStatusFile(config_, state_);
        }
        if (vectorDatabase_) {
            state_.readiness.searchProgress = 70;
            writeBootstrapStatusFile(config_, state_);
        }
        int build_timeout = read_timeout_ms("YAMS_SEARCH_BUILD_TIMEOUT_MS", 5000, 250);

        // Determine vector readiness: honor env disables and presence of vector infra
        const bool vectorsDisabled =
            ConfigResolver::envTruthy(std::getenv("YAMS_DISABLE_VECTORS")) ||
            ConfigResolver::envTruthy(std::getenv("YAMS_DISABLE_VECTOR_DB"));
        bool vectorEnabled = false;
        if (vectorsDisabled) {
            spdlog::info(
                "[SearchBuild] Vector search disabled via env flag; building text-only engine");
        } else if (vectorDatabase_) {
            try {
                // Use VectorDatabase directly - it knows the actual DB size
                auto vectorCount = vectorDatabase_->getVectorCount();
                vectorEnabled = (vectorCount > 0);
                spdlog::info("[SearchBuild] Vector DB has {} vectors, vector_enabled={}",
                             vectorCount, vectorEnabled);
            } catch (const std::exception& e) {
                spdlog::warn("[SearchBuild] Could not check vector count: {}", e.what());
            }
        } else {
            spdlog::info(
                "[SearchBuild] Vector components not available; building text-only engine");
        }

        spdlog::info("[SearchBuild] scheduling initial build (vector_enabled hint={})",
                     vectorEnabled);
        // Nudge progress to indicate we're in the final build step
        try {
            state_.readiness.searchProgress =
                std::max<int>(state_.readiness.searchProgress.load(), 90);
        } catch (...) {
        }

        // Phase 2.4: Use SearchEngineManager instead of co_buildEngine
        // Get embedding generator from model provider if available
        std::shared_ptr<vector::EmbeddingGenerator> embGen;
        spdlog::info("[SearchBuild] Checking embedding generator: modelProvider_={} isAvailable={} "
                     "modelName='{}'",
                     modelProvider_ != nullptr,
                     modelProvider_ ? modelProvider_->isAvailable() : false, embeddingModelName_);
        if (modelProvider_ && modelProvider_->isAvailable() && !embeddingModelName_.empty()) {
            try {
                embGen = modelProvider_->getEmbeddingGenerator(embeddingModelName_);
                spdlog::info("[SearchBuild] Got embedding generator: {}", embGen != nullptr);
            } catch (const std::exception& e) {
                spdlog::warn("[SearchBuild] Failed to get embedding generator: {}", e.what());
            }
        }
        auto buildResult = co_await searchEngineManager_.buildEngine(
            metadataRepo_, vectorDatabase_, embGen, "initial", build_timeout, getWorkerExecutor());

        if (buildResult.has_value()) {
            const auto& built = buildResult.value();
            {
                std::unique_lock lk(searchEngineMutex_); // Exclusive write
                searchEngine_ = built;
            }

            std::call_once(queryConceptExtractorOnce_, [this]() {
                cachedQueryConceptExtractor_ = createGlinerExtractionFunc(getEntityExtractors());
            });
            if (cachedQueryConceptExtractor_) {
                built->setConceptExtractor(cachedQueryConceptExtractor_);
                spdlog::info("[SearchBuild] GLiNER concept extractor wired to search engine");
            }

            // Wire cross-encoder reranker if available
            if (rerankerAdapter_ && rerankerAdapter_->isReady()) {
                built->setReranker(rerankerAdapter_);
                spdlog::info("[SearchBuild] Cross-encoder reranker wired to search engine");
            }

            // Update readiness indicators after successful rebuild
            state_.readiness.searchEngineReady = true;
            state_.readiness.searchProgress = 100;
            state_.readiness.vectorIndexReady = (vectorDatabase_ != nullptr);

            // Track doc count at build time for re-tuning decisions
            if (metadataRepo_) {
                auto countRes = metadataRepo_->getDocumentCount();
                if (countRes) {
                    if (searchComponent_) {
                        searchComponent_->recordSuccessfulBuild(countRes.value());
                    } else {
                        state_.readiness.searchEngineDocCount.store(countRes.value());
                    }
                }
            }

            writeBootstrapStatusFile(config_, state_);

            spdlog::info("SearchEngine initialized and published to AppContext (docs={})",
                         state_.readiness.searchEngineDocCount.load());
            try {
                serviceFsm_.dispatch(SearchEngineBuiltEvent{});
            } catch (...) {
            }
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
        spdlog::warn("Exception wiring SearchEngine: {}", e.what());
    }
    spdlog::info("[ServiceManager] Phase: Search Engine Built.");
    if (ingestService_) {
        ingestService_->start();
    }
    spdlog::info("[ServiceManager] Phase: Ingest Service Started.");

    co_return Result<void>();
}

boost::asio::awaitable<void> ServiceManager::co_runSessionWatcher(yams::compat::stop_token token) {
    auto executor = co_await boost::asio::this_coro::executor;

    auto read_ms = [](const char* env, int def) {
        try {
            if (const char* v = std::getenv(env))
                return std::max(100, std::stoi(v));
        } catch (...) {
        }
        return def;
    };

    const int interval_ms = read_ms("YAMS_SESSION_WATCH_INTERVAL_MS", 2000);
    auto wait_duration = std::chrono::milliseconds(interval_ms);
    boost::asio::steady_timer timer(executor);

    while (!token.stop_requested()) {
        try {
            yams::app::services::AppContext appCtx = getAppContext();
            auto sess = yams::app::services::makeSessionService(&appCtx);
            auto current = sess->current();
            if (current && sess->watchEnabled(*current)) {
                auto indexingService = yams::app::services::makeIndexingService(appCtx);
                if (!indexingService) {
                    continue;
                }
                auto patterns = sess->getPinnedPatterns(*current);
                for (const auto& pat : patterns) {
                    std::error_code ec;
                    std::filesystem::path p(pat);
                    if (!p.empty() && std::filesystem::is_directory(p, ec)) {
                        auto& dirMap = sessionWatch_.dirFiles[p.string()];
                        std::unordered_map<std::string, std::pair<std::uint64_t, std::uint64_t>>
                            cur;
                        std::vector<std::string> changed;
                        for (auto it = std::filesystem::recursive_directory_iterator(p, ec);
                             !ec && it != std::filesystem::recursive_directory_iterator(); ++it) {
                            if (!it->is_regular_file())
                                continue;
                            auto fp = it->path().string();
                            auto fsz = static_cast<std::uint64_t>(it->file_size(ec));
                            auto fmt = static_cast<std::uint64_t>(
                                std::chrono::duration_cast<std::chrono::seconds>(
                                    it->last_write_time().time_since_epoch())
                                    .count());
                            cur[fp] = {fmt, fsz};
                            auto old = dirMap.find(fp);
                            if (old == dirMap.end() || old->second != cur[fp]) {
                                std::error_code rel_ec;
                                auto relPath = std::filesystem::relative(it->path(), p, rel_ec);
                                std::string relStr = rel_ec ? it->path().filename().string()
                                                            : relPath.generic_string();
                                if (!relStr.empty()) {
                                    changed.emplace_back(std::move(relStr));
                                }
                            }
                        }
                        dirMap.swap(cur);
                        if (!changed.empty()) {
                            yams::app::services::AddDirectoryRequest req;
                            req.directoryPath = p.string();
                            req.includePatterns = std::move(changed);
                            req.recursive = true;
                            req.sessionId = *current;
                            req.noEmbeddings = true;
                            req.noGitignore = false;
                            (void)indexingService->addDirectory(req);
                        }
                    }
                }
            }
        } catch (...) {
        }

        boost::system::error_code ec;
        timer.expires_after(wait_duration);
        co_await timer.async_wait(boost::asio::redirect_error(boost::asio::use_awaitable, ec));
        if (token.stop_requested() || ec == boost::asio::error::operation_aborted)
            break;
    }

    co_return;
}

boost::asio::awaitable<bool> ServiceManager::co_openDatabase(const std::filesystem::path& dbPath,
                                                             int timeout_ms,
                                                             yams::compat::stop_token token) {
    auto ex = co_await boost::asio::this_coro::executor;

    try {
        // Use async_initiate pattern with timeout racing (no experimental APIs)
        co_return co_await boost::asio::async_initiate<decltype(boost::asio::use_awaitable),
                                                       void(std::exception_ptr, bool)>(
            [this, dbPath, ex, timeout_ms](auto handler) mutable {
                // Shared state for race coordination
                auto completed = std::make_shared<std::atomic<bool>>(false);
                auto timer = std::make_shared<boost::asio::steady_timer>(ex);
                timer->expires_after(std::chrono::milliseconds(timeout_ms));

                // Capture handler in shared_ptr for safe sharing between timer and work
                using HandlerT = std::decay_t<decltype(handler)>;
                auto handlerPtr = std::make_shared<HandlerT>(std::move(handler));
                auto completion_exec = boost::asio::get_associated_executor(*handlerPtr, ex);

                // Set up timeout
                timer->async_wait([completed, handlerPtr, completion_exec,
                                   timeout_ms](const boost::system::error_code& ec) mutable {
                    if (ec)
                        return; // Timer cancelled
                    if (!completed->exchange(true, std::memory_order_acq_rel)) {
                        // Timeout won
                        spdlog::warn(
                            "Database open timed out after {} ms — continuing in degraded mode",
                            timeout_ms);
                        boost::asio::post(completion_exec, [h = std::move(*handlerPtr)]() mutable {
                            std::move(h)(std::exception_ptr{}, false);
                        });
                    }
                });

                // Post blocking work to executor
                boost::asio::post(ex, [this, dbPath, timer, completed, handlerPtr,
                                       completion_exec]() mutable {
                    bool success = false;
                    std::exception_ptr ep;
                    try {
                        auto r = database_->open(dbPath.string(), metadata::ConnectionMode::Create);
                        success = static_cast<bool>(r);
                        if (!success) {
                            spdlog::warn("Database open failed: {} — continuing in degraded mode",
                                         r.error().message);
                        } else {
                            state_.readiness.databaseReady = true;
                            spdlog::info("Database opened successfully");
                        }
                    } catch (const std::exception& e) {
                        spdlog::warn(
                            "Database open threw exception: {} — continuing in degraded mode",
                            e.what());
                        ep = std::current_exception();
                    } catch (...) {
                        spdlog::warn("Database open failed (unknown exception) — continuing in "
                                     "degraded mode");
                        ep = std::current_exception();
                    }

                    if (!completed->exchange(true, std::memory_order_acq_rel)) {
                        // Work won
                        timer->cancel();
                        boost::asio::post(completion_exec,
                                          [h = std::move(*handlerPtr), ep, success]() mutable {
                                              std::move(h)(ep, success);
                                          });
                    }
                });
            },
            boost::asio::use_awaitable);

    } catch (const std::exception& e) {
        spdlog::warn("Database open failed (exception): {} — continuing in degraded mode",
                     e.what());
        co_return false;
    } catch (...) {
        spdlog::warn("Database open failed (unknown exception) — continuing in degraded mode");
        co_return false;
    }
}

boost::asio::awaitable<bool> ServiceManager::co_migrateDatabase(int timeout_ms,
                                                                yams::compat::stop_token token) {
    auto ex = co_await boost::asio::this_coro::executor;

    // Create migration manager on heap so it survives async operations
    auto mm = std::make_shared<metadata::MigrationManager>(*database_);
    auto initResult = mm->initialize();
    if (!initResult) {
        spdlog::error("[ServiceManager] Failed to initialize migration system: {}",
                      initResult.error().message);
        co_return false;
    }
    mm->registerMigrations(metadata::YamsMetadataMigrations::getAllMigrations());

    try {
        // Use async_initiate pattern with timeout racing (no experimental APIs)
        co_return co_await boost::asio::async_initiate<decltype(boost::asio::use_awaitable),
                                                       void(std::exception_ptr, bool)>(
            [mm, ex, timeout_ms](auto handler) mutable {
                // Shared state for race coordination
                auto completed = std::make_shared<std::atomic<bool>>(false);
                auto timer = std::make_shared<boost::asio::steady_timer>(ex);
                timer->expires_after(std::chrono::milliseconds(timeout_ms));

                // Capture handler in shared_ptr for safe sharing between timer and work
                using HandlerT = std::decay_t<decltype(handler)>;
                auto handlerPtr = std::make_shared<HandlerT>(std::move(handler));
                auto completion_exec = boost::asio::get_associated_executor(*handlerPtr, ex);

                // Set up timeout
                timer->async_wait([completed, handlerPtr, completion_exec,
                                   timeout_ms](const boost::system::error_code& ec) mutable {
                    if (ec)
                        return; // Timer cancelled
                    if (!completed->exchange(true, std::memory_order_acq_rel)) {
                        // Timeout won
                        spdlog::warn("Database migration timed out after {} ms", timeout_ms);
                        boost::asio::post(completion_exec, [h = std::move(*handlerPtr)]() mutable {
                            std::move(h)(std::exception_ptr{}, false);
                        });
                    }
                });

                // Post blocking work to executor
                boost::asio::post(
                    ex, [mm, timer, completed, handlerPtr, completion_exec]() mutable {
                        bool success = false;
                        std::exception_ptr ep;
                        try {
                            auto r = mm->migrate();
                            success = static_cast<bool>(r);
                            if (!success) {
                                spdlog::warn("Database migration failed: {}", r.error().message);
                            } else {
                                spdlog::info("Database migrations completed");
                            }
                        } catch (const std::exception& e) {
                            spdlog::warn("Database migration threw exception: {}", e.what());
                            ep = std::current_exception();
                        } catch (...) {
                            spdlog::warn("Database migration failed (unknown exception)");
                            ep = std::current_exception();
                        }

                        if (!completed->exchange(true, std::memory_order_acq_rel)) {
                            // Work won
                            timer->cancel();
                            boost::asio::post(completion_exec,
                                              [h = std::move(*handlerPtr), ep, success]() mutable {
                                                  std::move(h)(ep, success);
                                              });
                        }
                    });
            },
            boost::asio::use_awaitable);

    } catch (const std::exception& e) {
        spdlog::warn("Database migration failed (exception): {}", e.what());
        co_return false;
    } catch (...) {
        spdlog::warn("Database migration failed (unknown exception)");
        co_return false;
    }
}

/// NOTE: Implementation delegated to PluginManager (PBI-088 decomposition)
Result<bool> ServiceManager::adoptModelProviderFromHosts(const std::string& preferredName) {
    if (pluginManager_) {
        auto result = pluginManager_->adoptModelProvider(preferredName);
        if (result && result.value()) {
            // Sync local members from PluginManager for backward compatibility
            modelProvider_ = pluginManager_->getModelProvider();
            embeddingModelName_ = pluginManager_->getEmbeddingModelName();
            state_.readiness.modelProviderReady = (modelProvider_ != nullptr);
            spdlog::info("[ServiceManager] Synced model provider: model='{}', provider={}",
                         embeddingModelName_, modelProvider_ ? "valid" : "null");

            // Initialize reranker adapter using model provider's scoreDocuments capability
            // This is a lazy-init adapter - it fetches the provider on each call
            if (!rerankerAdapter_) {
                rerankerAdapter_ = std::make_shared<yams::search::ModelProviderRerankerAdapter>(
                    [this]() { return this->modelProvider_; });
                spdlog::info("[Reranker] Initialized model provider reranker adapter");
            }
        }
        return result;
    }
    spdlog::warn("[Plugin] PluginManager not initialized");
    return Result<bool>(false);
}

// NOTE: Implementation delegated to PluginManager (PBI-088 decomposition)
Result<size_t> ServiceManager::adoptContentExtractorsFromHosts() {
    if (pluginManager_) {
        auto result = pluginManager_->adoptContentExtractors();
        if (result) {
            // Sync local copy for PostIngestQueue and AppContext
            contentExtractors_ = pluginManager_->getContentExtractors();
            spdlog::info("[ServiceManager] Synced {} content extractors from PluginManager",
                         contentExtractors_.size());

            // Update PostIngestQueue with the newly adopted extractors
            if (postIngest_) {
                postIngest_->setExtractors(contentExtractors_);
                spdlog::info("[ServiceManager] Updated PostIngestQueue with {} extractors",
                             contentExtractors_.size());
            }
        }
        return result;
    }
    spdlog::warn("[Plugin] PluginManager not initialized");
    return Result<size_t>(0);
}

// NOTE: Implementation delegated to PluginManager (PBI-088 decomposition)
Result<size_t> ServiceManager::adoptSymbolExtractorsFromHosts() {
    if (pluginManager_) {
        auto result = pluginManager_->adoptSymbolExtractors();
        if (result && result.value() > 0) {
            // Build extension-to-language map from symbol extractors and pass to PostIngestQueue
            if (postIngest_) {
                std::unordered_map<std::string, std::string> extMap;
                const auto& extractors = pluginManager_->getSymbolExtractors();
                for (const auto& extractor : extractors) {
                    if (!extractor)
                        continue;
                    auto supported = extractor->getSupportedExtensions();
                    for (const auto& [ext, lang] : supported) {
                        extMap[ext] = lang;
                    }
                }
                auto mapSize = extMap.size();
                postIngest_->setSymbolExtensionMap(std::move(extMap));
                spdlog::info(
                    "[ServiceManager] Updated PostIngestQueue with {} symbol extension mappings",
                    mapSize);
            }
        }
        return result;
    }
    spdlog::warn("[Plugin] PluginManager not initialized");
    return Result<size_t>(0);
}

// NOTE: Implementation delegated to PluginManager (PBI-088 decomposition)
Result<size_t> ServiceManager::adoptEntityExtractorsFromHosts() {
    if (pluginManager_) {
        auto result = pluginManager_->adoptEntityExtractors();
        if (result) {
            if (postIngest_) {
                auto extractor = createGlinerExtractionFunc(pluginManager_->getEntityExtractors());
                if (!extractor) {
                    spdlog::warn("[ServiceManager] GLiNER extraction func is null — title "
                                 "extraction will be disabled (no entity extractors found)");
                }
                postIngest_->setTitleExtractor(std::move(extractor));
                spdlog::info("[ServiceManager] Updated PostIngestQueue title extractor using {} "
                             "entity extractors",
                             pluginManager_->getEntityExtractors().size());
            }
        }
        return result;
    }
    spdlog::warn("[Plugin] PluginManager not initialized");
    return Result<size_t>(0);
}

boost::asio::any_io_executor ServiceManager::getWorkerExecutor() const {
    if (workCoordinator_)
        return workCoordinator_->getExecutor();
    return boost::asio::system_executor();
}

boost::asio::any_io_executor ServiceManager::getCliExecutor() const {
    if (cliRequestPool_) {
        return cliRequestPool_->get_executor();
    }
    return getWorkerExecutor();
}

bool ServiceManager::resizeWorkerPool(std::size_t target) {
    try {
        if (target == 0)
            target = 1;
        // With the new architecture, we don't dynamically resize worker pools
        // The worker count is fixed at construction time
        spdlog::debug("resizeWorkerPool called with target {} (ignored in new architecture)",
                      target);
        return false; // No change made
    } catch (const std::exception& e) {
        spdlog::warn("resizeWorkerPool error: {}", e.what());
        return false;
    }
}

boost::asio::awaitable<Result<size_t>> ServiceManager::autoloadPluginsNow() {
    // PBI-088: Delegate to PluginManager which owns the plugin host lifecycle and FSM
    if (!pluginManager_) {
        spdlog::error("[ServiceManager] autoloadPluginsNow: PluginManager not initialized");
        co_return Error{ErrorCode::InvalidState, "PluginManager not initialized"};
    }

    auto executor = getWorkerExecutor();
    auto result = co_await pluginManager_->autoloadPlugins(executor);

    if (result) {
        spdlog::info("ServiceManager: Autoloaded {} plugins via PluginManager", result.value());

        // Adopt model provider from PluginManager's hosts
        auto adopted = adoptModelProviderFromHosts();
        if (adopted && adopted.value()) {
            spdlog::info("[ServiceManager] Model provider adopted after autoload");
        }

        // Adopt extractors
        (void)adoptContentExtractorsFromHosts();
        (void)adoptSymbolExtractorsFromHosts();
        (void)adoptEntityExtractorsFromHosts();

        refreshPluginStatusSnapshot();
        writeBootstrapStatusFile(config_, state_);
    }

    co_return result;
}

boost::asio::awaitable<void> ServiceManager::co_enableEmbeddingsAndRebuild() {
    spdlog::info("[ServiceManager] co_enableEmbeddingsAndRebuild: starting");

    // Protect against concurrent rebuilds.
    // NOTE: ServiceManagerFsm is a startup lifecycle FSM and is not reliably driven for
    // model-load-triggered rebuilds. Use SearchEngineManager/SearchEngineFsm as the source of
    // truth.
    try {
        const auto snap = searchEngineManager_.getSnapshot();
        if (snap.state == SearchEngineState::Building ||
            snap.state == SearchEngineState::AwaitingDrain) {
            spdlog::info("[ServiceManager] co_enableEmbeddingsAndRebuild: rebuild already in "
                         "progress (SearchEngineFsm), skipping");
            co_return;
        }
    } catch (...) {
    }

    try {
        // Phase 2.4: Use SearchEngineManager
        auto graphService = graphComponent_ ? graphComponent_->getQueryService() : nullptr;

        // Get embedding generator from model provider if available
        std::shared_ptr<vector::EmbeddingGenerator> embGen;
        if (modelProvider_ && modelProvider_->isAvailable() && !embeddingModelName_.empty()) {
            try {
                embGen = modelProvider_->getEmbeddingGenerator(embeddingModelName_);
            } catch (...) {
            }
        }

        int build_timeout = 30000; // 30s timeout
        auto rebuildResult = co_await searchEngineManager_.buildEngine(
            metadataRepo_, vectorDatabase_, embGen, "rebuild_enabled", build_timeout,
            getWorkerExecutor());

        if (rebuildResult.has_value()) {
            const auto& rebuilt = rebuildResult.value();
            {
                std::unique_lock lk(searchEngineMutex_);
                searchEngine_ = rebuilt;
            }

            // Wire cross-encoder reranker if available
            if (rerankerAdapter_ && rerankerAdapter_->isReady()) {
                rebuilt->setReranker(rerankerAdapter_);
                spdlog::debug("[Rebuild] Cross-encoder reranker wired to search engine");
            }

            // Update readiness indicators
            state_.readiness.searchEngineReady = true;
            state_.readiness.vectorIndexReady = (vectorDatabase_ != nullptr);

            // Track doc count at build time for re-tuning decisions
            if (metadataRepo_) {
                auto countRes = metadataRepo_->getDocumentCount();
                if (countRes) {
                    if (searchComponent_) {
                        searchComponent_->recordSuccessfulBuild(countRes.value());
                    } else {
                        state_.readiness.searchEngineDocCount.store(countRes.value());
                    }
                }
            }

            spdlog::info("[ServiceManager] co_enableEmbeddingsAndRebuild: success (docs={})",
                         state_.readiness.searchEngineDocCount.load());
        } else {
            spdlog::warn("[ServiceManager] co_enableEmbeddingsAndRebuild: failed");
        }
    } catch (const std::exception& e) {
        spdlog::error("[ServiceManager] co_enableEmbeddingsAndRebuild: exception: {}", e.what());
    }
}

bool ServiceManager::triggerSearchEngineRebuildIfNeeded() {
    // Delegate to SearchComponent for corpus monitoring and rebuild triggering
    if (searchComponent_) {
        return searchComponent_->checkAndTriggerRebuildIfNeeded();
    }
    return false;
}

boost::asio::awaitable<void> ServiceManager::preloadPreferredModelIfConfigured() {
    // FSM-based idempotence: skip if already loading or ready
    if (embeddingFsm_.isLoadingOrReady()) {
        spdlog::debug("preloadPreferredModelIfConfigured: already loading or ready (FSM)");
        co_return;
    }

    // Signal started
    try {
        embeddingFsm_.dispatch(ModelLoadStartedEvent{resolvePreferredModel()});
    } catch (...) {
    }

    try {
        spdlog::info("[Rebuild] Embedding generator will initialize on first use (lazy)");

        // Don't initialize embeddings here - let it happen lazily on first search request
        // This avoids blocking the rebuild process

        // Protect against concurrent rebuilds
        bool buildingAlready = false;
        try {
            buildingAlready =
                (serviceFsm_.snapshot().state == ServiceManagerState::BuildingSearchEngine);
        } catch (...) {
        }
        if (!buildingAlready) {
            spdlog::info("[Rebuild] search engine rebuild begin (enable vector scoring)");
            int build_timeout = 15000; // Generous timeout for rebuild

            // Get embedding generator from model provider if available
            std::shared_ptr<vector::EmbeddingGenerator> embGen;
            if (modelProvider_ && modelProvider_->isAvailable() && !embeddingModelName_.empty()) {
                try {
                    embGen = modelProvider_->getEmbeddingGenerator(embeddingModelName_);
                } catch (...) {
                }
            }

            // Phase 2.4: Use SearchEngineManager instead of co_buildEngine
            auto rebuildResult = co_await searchEngineManager_.buildEngine(
                metadataRepo_, vectorDatabase_, embGen, "rebuild", build_timeout,
                getWorkerExecutor());

            if (rebuildResult.has_value()) {
                const auto& rebuilt = rebuildResult.value();
                {
                    std::unique_lock lk(searchEngineMutex_); // Exclusive write
                    searchEngine_ = rebuilt;
                }

                // Wire cross-encoder reranker if available
                if (rerankerAdapter_ && rerankerAdapter_->isReady()) {
                    rebuilt->setReranker(rerankerAdapter_);
                    spdlog::debug("[Rebuild] Cross-encoder reranker wired to search engine");
                }

                // Update readiness indicators after successful rebuild
                state_.readiness.searchEngineReady = true;
                state_.readiness.searchProgress = 100;
                state_.readiness.vectorIndexReady = (vectorDatabase_ != nullptr);

                // Track doc count at build time for re-tuning decisions
                if (metadataRepo_) {
                    auto countRes = metadataRepo_->getDocumentCount();
                    if (countRes) {
                        if (searchComponent_) {
                            searchComponent_->recordSuccessfulBuild(countRes.value());
                        } else {
                            state_.readiness.searchEngineDocCount.store(countRes.value());
                        }
                    }
                }

                writeBootstrapStatusFile(config_, state_);

                spdlog::info("[Rebuild] done ok: vector scoring enabled (docs={})",
                             state_.readiness.searchEngineDocCount.load());
            } else {
                spdlog::warn("[Rebuild] failed: engine rebuild unsuccessful");
            }

        } else {
            spdlog::debug("[Rebuild] skip: rebuild already in progress");
        }
    } catch (const std::exception& e) {
        spdlog::warn("[Rebuild] error: {}", e.what());
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

std::shared_ptr<search::SearchEngine> ServiceManager::getSearchEngineSnapshot() const {
    // Use try_lock to avoid blocking indefinitely during search engine rebuilds
    std::shared_lock lock(searchEngineMutex_, std::try_to_lock);
    if (!lock.owns_lock()) {
        spdlog::debug(
            "getSearchEngineSnapshot: mutex busy (rebuild in progress?), returning cached");
        return std::atomic_load(&searchEngine_);
    }
    return searchEngine_;
}

yams::app::services::AppContext ServiceManager::getAppContext() const {
    app::services::AppContext ctx;
    ctx.service_manager = const_cast<ServiceManager*>(this);
    ctx.store = getContentStore(); // Thread-safe via atomic_load
    ctx.metadataRepo = metadataRepo_;
    ctx.searchEngine = getSearchEngineSnapshot();
    ctx.vectorDatabase = getVectorDatabase();
    ctx.kgStore = this->kgStore_; // PBI-043: tree diff KG integration
    ctx.graphQueryService = graphComponent_ ? graphComponent_->getQueryService()
                                            : nullptr; // PBI-009: centralized graph queries
    ctx.contentExtractors = contentExtractors_;

    // Log vector capability status
    bool vectorCapable = (modelProvider_ && modelProvider_->isAvailable());
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
    // A simple estimate of the queue depth based on job tracking counters.
    long posted = poolPosted_.load(std::memory_order_relaxed);
    long completed = poolCompleted_.load(std::memory_order_relaxed);
    long active = poolActive_.load(std::memory_order_relaxed);

    if (posted > completed + active) {
        return static_cast<size_t>(posted - completed - active);
    }
    return 0;
}

ServiceManager::SearchLoadMetrics ServiceManager::getSearchLoadMetrics() const {
    SearchLoadMetrics metrics;

    // Get search engine statistics if available
    auto engine = getSearchEngineSnapshot();
    if (engine) {
        const auto& stats = engine->getStatistics();

        // Map SearchEngine::Statistics to SearchLoadMetrics
        // Note: SearchEngine is synchronous, so "active" and "queued" are conceptual
        // "active" = 1 if engine is available and serving queries
        // "queued" = 0 (no queue in synchronous model)
        metrics.active = 1; // Engine is available
        metrics.queued = 0; // No queue in synchronous model
        metrics.executed = stats.totalQueries.load();
        metrics.avgLatencyUs = stats.avgQueryTimeMicros.load();

        // Calculate cache hit rate from successful vs total queries
        auto total = stats.totalQueries.load();
        auto successful = stats.successfulQueries.load();
        if (total > 0) {
            metrics.cacheHitRate = static_cast<double>(successful) / static_cast<double>(total);
        } else {
            metrics.cacheHitRate = 0.0;
        }

        // Concurrency limit is not applicable in new model, set to 1 (synchronous)
        metrics.concurrencyLimit = 1;
    }

    return metrics;
}

bool ServiceManager::detectEmbeddingPreloadFlag() const {
    return ConfigResolver::detectEmbeddingPreloadFlag(config_);
}

size_t ServiceManager::getEmbeddingDimension() const {
    if (!modelProvider_ || !modelProvider_->isAvailable())
        return 0;
    try {
        std::string modelName = resolvePreferredModel();
        if (modelName.empty())
            return 0;
        return modelProvider_->getEmbeddingDim(modelName);
    } catch (...) {
        return 0;
    }
}

std::size_t ServiceManager::getEmbeddingInFlightJobs() const {
    if (embeddingService_) {
        return embeddingService_->inFlightJobs();
    }
    return 0;
}

std::size_t ServiceManager::getEmbeddingQueuedJobs() const {
    if (embeddingService_) {
        return embeddingService_->queuedJobs();
    }
    return 0;
}

std::string ServiceManager::resolvePreferredModel() const {
    return ConfigResolver::resolvePreferredModel(config_, resolvedDataDir_);
}

// Async phase helpers implementation
boost::asio::awaitable<yams::Result<void>>
ServiceManager::co_initContentStore(boost::asio::any_io_executor exec,
                                    const boost::asio::cancellation_state& token) {
    // Check cancellation
    if (token.cancelled() != boost::asio::cancellation_type::none) {
        co_return yams::Result<void>(
            Error{ErrorCode::OperationCancelled, "Content store initialization cancelled"});
    }

    try {
        spdlog::info("[ServiceManager::co_initContentStore] Creating content store");

        // Create content store using existing pattern
        yams::api::ContentStoreConfig storeConfig;
        storeConfig.storagePath = resolvedDataDir_ / "storage";

        auto store = yams::api::createContentStore(storeConfig);
        if (!store) {
            co_return yams::Result<void>(
                Error{ErrorCode::IOError, "Failed to create content store"});
        }

        auto uniqueStore = std::move(store).value();
        // Use atomic_store for thread-safe write (read by main thread via getContentStore())
        std::atomic_store_explicit(&contentStore_,
                                   std::shared_ptr<yams::api::IContentStore>(uniqueStore.release()),
                                   std::memory_order_release);
        spdlog::info("[ServiceManager::co_initContentStore] Content store initialized");
        co_return yams::Result<void>{};

    } catch (const std::exception& e) {
        spdlog::error("[ServiceManager::co_initContentStore] Exception: {}", e.what());
        co_return yams::Result<void>(
            Error{std::string("Content store initialization failed: ") + e.what()});
    }
}

boost::asio::awaitable<yams::Result<void>>
ServiceManager::co_initDatabase(boost::asio::any_io_executor exec,
                                const boost::asio::cancellation_state& token) {
    // Check cancellation
    if (token.cancelled() != boost::asio::cancellation_type::none) {
        co_return yams::Result<void>(
            Error{ErrorCode::OperationCancelled, "Database initialization cancelled"});
    }

    try {
        spdlog::info("[ServiceManager::co_initDatabase] Opening database");

        // Open database using existing pattern
        auto db = std::make_shared<yams::metadata::Database>();
        database_ = db;
        const auto dbPath = resolvedDataDir_ / "yams.db";

        // Bridge cancellation_state to stop_token for helpers
        yams::compat::stop_source src;
        if (token.cancelled() != boost::asio::cancellation_type::none)
            src.request_stop();
        bool opened = co_await co_openDatabase(dbPath, 5000, src.get_token());
        if (!opened) {
            co_return yams::Result<void>(
                Error{ErrorCode::DatabaseError, "Failed to open database"});
        }

        // Migrate database
        bool migrated = co_await co_migrateDatabase(5000, src.get_token());
        if (!migrated) {
            co_return yams::Result<void>(
                Error{ErrorCode::DatabaseError, "Failed to migrate database"});
        }

        // Create connection pool and metadata repository
        yams::metadata::ConnectionPoolConfig dbCfg{};
        connectionPool_ = std::make_shared<yams::metadata::ConnectionPool>(dbPath.string(), dbCfg);
        metadataRepo_ = std::make_shared<yams::metadata::MetadataRepository>(*connectionPool_);

        // Mark readiness
        state_.readiness.databaseReady.store(true);
        state_.readiness.metadataRepoReady.store(true);

        spdlog::info("[ServiceManager::co_initDatabase] Database initialized");
        co_return yams::Result<void>{};

    } catch (const std::exception& e) {
        spdlog::error("[ServiceManager::co_initDatabase] Exception: {}", e.what());

        co_return yams::Result<void>(
            Error{std::string("Database initialization failed: ") + e.what()});
    }
}

boost::asio::awaitable<std::shared_ptr<yams::search::SearchEngine>>
ServiceManager::co_buildEngine(int timeout_ms, const boost::asio::cancellation_state& /*token*/,
                               bool includeEmbeddingGenerator) {
    auto exec = getWorkerExecutor();
    std::shared_ptr<vector::EmbeddingGenerator> gen;
    if (includeEmbeddingGenerator && modelProvider_ && modelProvider_->isAvailable() &&
        !embeddingModelName_.empty()) {
        try {
            gen = modelProvider_->getEmbeddingGenerator(embeddingModelName_);
        } catch (...) {
        }
    }
    auto res = co_await searchEngineManager_.buildEngine(metadataRepo_, vectorDatabase_, gen,
                                                         "co_buildEngine", timeout_ms, exec);
    if (res.has_value()) {
        co_return res.value();
    }
    co_return std::shared_ptr<yams::search::SearchEngine>{};
}

boost::asio::awaitable<yams::Result<void>>
ServiceManager::co_initSearchEngine(boost::asio::any_io_executor exec,
                                    const boost::asio::cancellation_state& token) {
    // Check cancellation
    if (token.cancelled() != boost::asio::cancellation_type::none) {
        co_return yams::Result<void>(
            Error{ErrorCode::OperationCancelled, "Search engine initialization cancelled"});
    }

    try {
        spdlog::info("[ServiceManager::co_initSearchEngine] Building search engine");

        // Build search engine using existing pattern
        auto engine = co_await co_buildEngine(5000, token, true);
        if (!engine) {
            co_return yams::Result<void>(
                Error{ErrorCode::InternalError, "Failed to build search engine"});
        }

        searchEngine_ = engine;

        // Mark readiness
        state_.readiness.searchEngineReady.store(true);

        spdlog::info("[ServiceManager::co_initSearchEngine] Search engine initialized");
        co_return yams::Result<void>{};

    } catch (const std::exception& e) {
        spdlog::error("[ServiceManager::co_initSearchEngine] Exception: {}", e.what());
        co_return yams::Result<void>(
            Error{std::string("Search engine initialization failed: ") + e.what()});
    }
}

boost::asio::awaitable<yams::Result<void>>
ServiceManager::co_initVectorSystem(boost::asio::any_io_executor exec,
                                    const boost::asio::cancellation_state& token) {
    // Check cancellation
    if (token.cancelled() != boost::asio::cancellation_type::none) {
        co_return yams::Result<void>(
            Error{ErrorCode::OperationCancelled, "Vector system initialization cancelled"});
    }

    try {
        spdlog::info("[ServiceManager::co_initVectorSystem] Initializing vector system");

        // Create vector database using existing pattern
        const auto dbPath = resolvedDataDir_ / "vectors.db";

        yams::vector::VectorDatabaseConfig cfg;
        cfg.database_path = dbPath.string();
        cfg.create_if_missing = true;

        // Get embedding dimension - use VectorSystemManager if available
        size_t dim = 0;
        if (vectorSystemManager_) {
            dim = vectorSystemManager_->getEmbeddingDimension();
        }
        if (dim == 0) {
            dim = getEmbeddingDimension(); // Try model provider
        }
        if (dim == 0) {
            spdlog::warn(
                "[ServiceManager::co_initVectorSystem] Cannot determine embedding dimension");
            co_return yams::Result<void>(Error{"Cannot determine embedding dimension"});
        }
        cfg.embedding_dim = dim;

        auto vectorDb = std::make_shared<yams::vector::VectorDatabase>(cfg);
        auto initRes = vectorDb->initialize();
        if (!initRes) {
            co_return yams::Result<void>(Error{"Failed to initialize vector database"});
        }

        vectorDatabase_ = vectorDb;
        state_.readiness.vectorDbReady.store(true);

        spdlog::info("[ServiceManager::co_initVectorSystem] Vector system initialized with dim={}",
                     dim);
        co_return yams::Result<void>{};

    } catch (const std::exception& e) {
        spdlog::error("[ServiceManager::co_initVectorSystem] Exception: {}", e.what());
        co_return yams::Result<void>(
            Error{std::string("Vector system initialization failed: ") + e.what()});
    }
}

boost::asio::awaitable<yams::Result<void>>
ServiceManager::co_initPluginSystem(boost::asio::any_io_executor exec,
                                    const boost::asio::cancellation_state& token) {
    // Check cancellation
    if (token.cancelled() != boost::asio::cancellation_type::none) {
        co_return yams::Result<void>(Error{"Plugin system initialization cancelled"});
    }

    try {
        spdlog::info("[ServiceManager::co_initPluginSystem] Initializing plugin system");

        // Create plugin loader and host using existing patterns
        abiPluginLoader_ = std::make_unique<AbiPluginLoader>();
        abiHost_ = std::make_unique<AbiPluginHost>(this);

        if (true /* simulate success - actual implementation would co_await scan_plugins */) {
            try {
                const auto ps = getPluginHostFsmSnapshot();
                state_.readiness.pluginsReady.store(ps.state == PluginHostState::Ready);
            } catch (...) {
                state_.readiness.pluginsReady.store(true);
            }
        }

        spdlog::info("[ServiceManager::co_initPluginSystem] Plugin system initialized");
        co_return yams::Result<void>{};

    } catch (const std::exception& e) {
        spdlog::error("[ServiceManager::co_initPluginSystem] Exception: {}", e.what());
        co_return yams::Result<void>(
            Error{std::string("Plugin system initialization failed: ") + e.what()});
    }
}

} // namespace yams::daemon

namespace yams::daemon {

// Start background task coroutines (EmbedJob/Fts5Job consumers, OrphanScan)
// Must be called after shared_ptr construction so shared_from_this() works.
void ServiceManager::startBackgroundTasks() {
    spdlog::debug("[ServiceManager] Starting background task coroutines via BackgroundTaskManager");

    // Acquire shared_from_this once - fail fast if unavailable
    std::shared_ptr<ServiceManager> self;
    try {
        self = shared_from_this();
    } catch (const std::bad_weak_ptr& e) {
        spdlog::error("[ServiceManager] Cannot launch consumers: shared_from_this() failed: {}",
                      e.what());
        return;
    }

    // Create BackgroundTaskManager with dependencies
    BackgroundTaskManager::Dependencies deps{
        .serviceManager = self, .lifecycleFsm = lifecycleFsm_, .executor = getWorkerExecutor()};

    try {
        backgroundTaskManager_ = std::make_unique<BackgroundTaskManager>(std::move(deps));
        backgroundTaskManager_->start();
        spdlog::info("[ServiceManager] Background tasks delegated to BackgroundTaskManager");
    } catch (const std::exception& e) {
        spdlog::error("[ServiceManager] Failed to start BackgroundTaskManager: {}", e.what());
        lifecycleFsm_.setSubsystemDegraded("background_tasks", true, e.what());
    }
}

std::string ServiceManager::lastModelError() const {
    return lifecycleFsm_.degradationReason("embeddings");
}

void ServiceManager::clearModelProviderError() {
    lifecycleFsm_.setSubsystemDegraded("embeddings", false);
}

void ServiceManager::__test_setModelProviderDegraded(bool degraded, const std::string& error) {
    try {
        lifecycleFsm_.setSubsystemDegraded("embeddings", degraded, error);
        if (pluginManager_) {
            pluginManager_->__test_setEmbeddingDegraded(degraded, error);
        } else {
            if (degraded) {
                embeddingFsm_.dispatch(
                    ProviderDegradedEvent{error.empty() ? std::string{"test"} : error});
            } else {
                embeddingFsm_.dispatch(ModelLoadedEvent{embeddingModelName_, 0});
            }
        }
    } catch (...) {
    }
}

void ServiceManager::enqueuePostIngest(const std::string& hash, const std::string& mime) {
    if (!postIngest_) {
        return;
    }

    // Check admission control - document is stored, post-processing can be retried later
    if (!ResourceGovernor::instance().canAdmitWork()) {
        spdlog::debug("[ServiceManager] PostIngest rejected: admission control blocked");
        return;
    }

    PostIngestQueue::Task task{hash, mime,
                               "", // session
                               std::chrono::steady_clock::now(),
                               PostIngestQueue::Task::Stage::Metadata};
    postIngest_->tryEnqueue(std::move(task));
}

} // namespace yams::daemon
