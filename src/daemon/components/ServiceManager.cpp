#include <sqlite3.h>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <atomic>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <map>
#include <optional>
#include <queue>
#include <sstream>
#include <string>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <yams/common/fs_utils.h>
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
#include <yams/common/fs_utils.h>
#include <yams/compat/thread_stop_compat.h>
#include <yams/config/config_helpers.h>
#include <yams/core/types.h>
#include <yams/daemon/components/BackgroundTaskManager.h>
#include <yams/daemon/components/CheckpointManager.h>
#include <yams/daemon/components/ConfigResolver.h>
#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/DaemonMetrics.h>
#include <yams/daemon/components/DatabaseManager.h>
#include <yams/daemon/components/dispatch_utils.hpp>
#include <yams/daemon/components/EmbeddingService.h>
#include <yams/daemon/components/EntityGraphService.h>
#include <yams/daemon/components/gliner_query_extractor.h>
#include <yams/daemon/components/GraphComponent.h>
#include <yams/daemon/components/IngestService.h>
#include <yams/daemon/components/init_utils.hpp>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/PluginManager.h>
#include <yams/daemon/components/ResourceGovernor.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/TopologyTuner.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/VectorIndexCoordinator.h>
#include <yams/daemon/components/VectorSystemManager.h>
#include <yams/daemon/ipc/fsm_metrics_registry.h>
#include <yams/daemon/ipc/retrieval_session.h>
#include <yams/daemon/metric_keys.h>
#include <yams/topology/topology_factory.h>

#include <yams/daemon/components/RepairService.h>
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
#include <yams/search/search_engine_builder.h>
#include <yams/storage/storage_runtime_resolver.h>
#include <yams/vector/sqlite_vec_backend.h>
#include <yams/vector/vector_database.h>

namespace {

bool isEphemeralDataDir(const std::filesystem::path& path) {
    namespace fs = std::filesystem;

    auto normalize = [](const fs::path& in) {
        std::error_code ec;
        auto canonical = fs::weakly_canonical(in, ec);
        return ec ? in.lexically_normal() : canonical;
    };

    const fs::path normalized = normalize(path);
    std::error_code ec;
    const fs::path tmpRoot = fs::temp_directory_path(ec);
    if (!ec) {
        const fs::path normalizedTmp = normalize(tmpRoot);
        auto rel = normalized.lexically_relative(normalizedTmp);
        if (rel.empty() || rel == "." || (!rel.empty() && *rel.begin() != "..")) {
            return true;
        }
    }

    const std::string generic = normalized.generic_string();
    return generic == "/tmp" || generic.rfind("/tmp/", 0) == 0 || generic == "/private/tmp" ||
           generic.rfind("/private/tmp/", 0) == 0;
}

// Convenience alias for ConfigResolver timeouts
inline int read_timeout_ms(const char* envName, int defaultMs, int minMs) {
    return yams::daemon::ConfigResolver::readTimeoutMs(envName, defaultMs, minMs);
}

inline void setOnnxShutdownMarker(bool enabled) {
#ifdef _WIN32
    _putenv_s("YAMS_ONNX_SHUTDOWN_IN_PROGRESS", enabled ? "1" : "");
#else
    if (enabled) {
        ::setenv("YAMS_ONNX_SHUTDOWN_IN_PROGRESS", "1", 1);
    } else {
        ::unsetenv("YAMS_ONNX_SHUTDOWN_IN_PROGRESS");
    }
#endif
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

std::uint64_t nowUnixMillis() {
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                          std::chrono::system_clock::now().time_since_epoch())
                                          .count());
}

} // namespace

// Open the daemon namespace for all following member definitions.
namespace yams::daemon {

namespace {
constexpr auto kTopologyOverlayRebuildMinAge = std::chrono::minutes(5);
constexpr std::size_t kTopologyOverlayDirtyThreshold = 64;
} // namespace

using yams::Error;
using yams::ErrorCode;
using yams::Result;
namespace search = yams::search;

ServiceManager::PluginStatusSnapshot ServiceManager::getPluginStatusSnapshot() const {
    auto snap = std::atomic_load_explicit(&pluginStatusSnapshot_, std::memory_order_acquire);
    return snap ? *snap : PluginStatusSnapshot{};
}

std::shared_ptr<const ServiceManager::PluginStatusSnapshot>
ServiceManager::getPluginStatusSnapshotPtr() const {
    return std::atomic_load_explicit(&pluginStatusSnapshot_, std::memory_order_acquire);
}

void ServiceManager::refreshPluginStatusSnapshot() {
    PluginStatusSnapshot snapshot;
    try {
        // PBI-088: Use delegated FSM snapshot (PluginManager owns the FSM)
        snapshot.host = getPluginHostFsmSnapshot();
        bool providerDegraded = false;
        try {
            auto es = embeddingLifecycle_.fsmSnapshot();
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
        const std::uint32_t modelsLoaded = 0;
        // Helper lambda to add plugin records
        auto addPluginRecords = [&](const std::vector<PluginDescriptor>& loaded,
                                    [[maybe_unused]] const std::string& pluginType) {
            for (const auto& d : loaded) {
                PluginStatusRecord rec;
                rec.name = d.name;
                rec.interfaces = d.interfaces;
                rec.isProvider = (!embeddingLifecycle_.adoptedPluginName().empty() &&
                                  embeddingLifecycle_.adoptedPluginName() == d.name);
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
        spdlog::debug("[refreshPluginStatusSnapshot] Checking external plugins, pluginManager_={}",
                      static_cast<void*>(pluginManager_.get()));
        if (auto* external = getExternalPluginHost()) {
            auto externalLoaded = external->listLoaded();
            spdlog::debug("[refreshPluginStatusSnapshot] External host returned {} plugins",
                          externalLoaded.size());
            // Reserve additional space for external plugins
            snapshot.records.reserve(snapshot.records.size() + externalLoaded.size());
            addPluginRecords(externalLoaded, "external");
        }
    } catch (...) {
        // leave snapshot empty on failure
    }
    auto shared = std::make_shared<PluginStatusSnapshot>(std::move(snapshot));
    std::atomic_store_explicit(&pluginStatusSnapshot_, std::move(shared),
                               std::memory_order_release);
}

ServiceManager::ServiceManager(const DaemonConfig& config, StateComponent& state,
                               DaemonLifecycleFsm& lifecycleFsm)
    : config_(config), state_(state),
      embeddingLifecycle_(EmbeddingLifecycleManager::Dependencies{
          [this]() { return loadModelProvider(); },
          [this]() -> std::shared_ptr<EmbeddingService> {
              return std::atomic_load_explicit(&embeddingService_, std::memory_order_acquire);
          },
          &config_, &resolvedDataDir_}),
      topologyManager_(TopologyManager::Dependencies{[this]() { return getMetadataRepo(); },
                                                     [this]() { return getKgStore(); },
                                                     [this]() { return getVectorDatabase(); }}),
      lifecycleFsm_(lifecycleFsm) {
    spdlog::debug("[ServiceManager] Constructor start");
    tuningConfig_ = config_.tuning;

    {
        auto enginePolicy = ConfigResolver::resolveTopologyEnginePolicy();
        if (enginePolicy.engine) {
            const auto resolved = std::string{topology::resolveFactoryKey(*enginePolicy.engine)};
            tuningConfig_.topologyAlgorithm = resolved;
            spdlog::info("Topology engine applied via config: {} (resolved={})",
                         *enginePolicy.engine, resolved);
        }
        if (enginePolicy.hdbscanMinPoints) {
            topologyManager_.setHdbscanMinPoints(*enginePolicy.hdbscanMinPoints);
            spdlog::info("Topology hdbscan_min_points applied via config: {}",
                         *enginePolicy.hdbscanMinPoints);
        }
        if (enginePolicy.hdbscanMinClusterSize) {
            topologyManager_.setHdbscanMinClusterSize(*enginePolicy.hdbscanMinClusterSize);
            spdlog::info("Topology hdbscan_min_cluster_size applied via config: {}",
                         *enginePolicy.hdbscanMinClusterSize);
        }
        if (enginePolicy.featureSmoothingHops) {
            topologyManager_.setFeatureSmoothingHops(*enginePolicy.featureSmoothingHops);
            spdlog::info("Topology feature_smoothing_hops applied via config: {}",
                         *enginePolicy.featureSmoothingHops);
        }
    }

    // Phase G: optional adaptive topology tuner. Disabled by default;
    // opt-in via [topology.tuner].enabled=true in the daemon config.
    {
        auto tunerPolicy = ConfigResolver::resolveTopologyTunerPolicy();
        const bool tunerEnabled = tunerPolicy.enabled.value_or(false);
        if (tunerEnabled) {
            TopologyTunerConfig tcfg;
            tcfg.enabled = true;
            if (tunerPolicy.cooldownMinutes) {
                tcfg.cooldown = std::chrono::minutes{*tunerPolicy.cooldownMinutes};
            }
            if (tunerPolicy.docCountDelta) {
                tcfg.docCountDelta = *tunerPolicy.docCountDelta;
            }
            if (tunerPolicy.rewardAlphaSingleton) {
                tcfg.weights.alphaSingleton = *tunerPolicy.rewardAlphaSingleton;
            }
            if (tunerPolicy.rewardBetaGiantCluster) {
                tcfg.weights.betaGiantCluster = *tunerPolicy.rewardBetaGiantCluster;
            }
            if (tunerPolicy.rewardGammaGiniDeviation) {
                tcfg.weights.gammaGiniDeviation = *tunerPolicy.rewardGammaGiniDeviation;
            }
            if (tunerPolicy.rewardDeltaIntraEdge) {
                tcfg.weights.deltaIntraEdge = *tunerPolicy.rewardDeltaIntraEdge;
            }
            // Persist MAB state under data_dir so arm-pull history survives
            // daemon restarts. Without this, UCB1 always picks the alphabetically
            // first arm on each fresh daemon spawn (no pulls, infinite UCB).
            if (!config_.dataDir.empty()) {
                tcfg.statePath = config_.dataDir / "topology_tuner_state.json";
            }
            // V1: arm grid is built for a typical corpus (~5k docs); adaptive
            // resizing as the corpus grows is a future iteration.
            constexpr std::size_t kInitialCorpusEstimate = 5000;
            auto tuner = std::make_shared<TopologyTuner>(tcfg);
            tuner->setArms(defaultArmGrid(kInitialCorpusEstimate));
            if (tcfg.statePath) {
                if (auto r = tuner->loadState(*tcfg.statePath); !r) {
                    spdlog::debug("[ServiceManager] topology tuner: no prior state at {} ({})",
                                  tcfg.statePath->string(), r.error().message);
                } else {
                    spdlog::info("[ServiceManager] topology tuner loaded state from {}",
                                 tcfg.statePath->string());
                }
            }
            topologyManager_.setTopologyTuner(tuner);
            spdlog::info("[ServiceManager] topology tuner enabled (cooldown={}min "
                         "doc_delta={} arms={} state={})",
                         std::chrono::duration_cast<std::chrono::minutes>(tcfg.cooldown).count(),
                         tcfg.docCountDelta, tuner->arms().size(),
                         tcfg.statePath ? tcfg.statePath->string() : std::string{"<ephemeral>"});
        }
    }

    metricsPublisher_.setWorkerTarget(1);

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
        auto threadCount = yams::daemon::TuneAdvisor::workCoordinatorThreads();
        workCoordinator_->start(threadCount);
        spdlog::info("[ServiceManager] WorkCoordinator created with {} worker threads (budget {}%, "
                     "override={})",
                     workCoordinator_->getWorkerCount(),
                     yams::daemon::TuneAdvisor::cpuBudgetPercent(), threadCount);

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
        requestExecutor_ = std::make_unique<RequestExecutor>();
        requestExecutor_->start();
    } catch (const std::exception& e) {
        spdlog::error("Failed to initialize RequestExecutor: {}", e.what());
        throw;
    }

    // Dedicated blocking pool for synchronous SQLite I/O (open, migrations).
    // Kept separate from the event-loop pool so heavy disk ops never stall async work.
    try {
        blockingPool_ = std::make_unique<boost::asio::thread_pool>(1);
        spdlog::debug("[ServiceManager] Blocking thread pool created (1 worker)");
    } catch (const std::exception& e) {
        spdlog::error("Failed to initialize blocking thread pool: {}", e.what());
        throw;
    }

    try {
        spdlog::debug("ServiceManager constructor start");
        refreshPluginStatusSnapshot();

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
                embeddingLifecycle_.setAutoOnAdd(true);
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
                } else {
                    trustFile = yams::config::get_daemon_plugin_trust_file();
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

        spdlog::debug("[Startup] deferring vector DB init to async phase");

        if (abiHost_) {
            bool strictPluginDirMode = config_.pluginDirStrict;
            if (const char* envStrict = std::getenv("YAMS_PLUGIN_DIR_STRICT")) {
                strictPluginDirMode = ConfigResolver::envTruthy(envStrict);
            }

            // Trust from env
            if (const char* env = std::getenv("YAMS_PLUGIN_DIR")) {
                try {
                    std::string raw(env);
                    std::vector<std::string> parts;
                    parts.reserve(4);
                    std::string cur;
                    for (char ch : raw) {
                        if (ch == ':' || ch == ';') {
                            if (!cur.empty()) {
                                parts.push_back(cur);
                                cur.clear();
                            }
                            continue;
                        }
                        cur.push_back(ch);
                    }
                    if (!cur.empty()) {
                        parts.push_back(cur);
                    }

                    if (parts.empty()) {
                        parts.push_back(raw);
                    }

                    for (const auto& p : parts) {
                        std::filesystem::path penv(p);
                        if (penv.empty()) {
                            continue;
                        }
                        if (auto tr = abiHost_->trustAdd(penv); !tr) {
                            spdlog::warn("Failed to auto-trust YAMS_PLUGIN_DIR {}: {}",
                                         penv.string(), tr.error().message);
                        }
                    }
                } catch (...) {
                    std::filesystem::path penv(env);
                    if (!penv.empty()) {
                        if (auto tr = abiHost_->trustAdd(penv); !tr) {
                            spdlog::warn("Failed to auto-trust YAMS_PLUGIN_DIR {}: {}",
                                         penv.string(), tr.error().message);
                        }
                    }
                }
            }

            // Trust from config
            if (!config_.pluginDir.empty()) {
                const auto& pconf = config_.pluginDir;
                if (auto tr = abiHost_->trustAdd(pconf); !tr) {
                    spdlog::warn("Failed to trust configured pluginDir {}: {}", pconf.string(),
                                 tr.error().message);
                } else {
                    spdlog::debug("Trusted configured pluginDir {}", pconf.string());
                }
            }

            // Trust explicit entries from config ([plugins].trusted_paths or daemon.trusted_paths)
            // unless strict plugin-dir mode is enabled, in which case only the explicit pluginDir
            // / YAMS_PLUGIN_DIR roots should be trusted.
            if (!strictPluginDirMode) {
                for (const auto& p : config_.trustedPluginPaths) {
                    if (auto tr = abiHost_->trustAdd(p); !tr) {
                        spdlog::warn("Failed to trust configured plugin path {}: {}", p.string(),
                                     tr.error().message);
                    } else {
                        spdlog::debug("Trusted configured plugin path {}", p.string());
                    }
                }
            } else if (!config_.trustedPluginPaths.empty()) {
                spdlog::info(
                    "Strict plugin-dir mode enabled; skipping {} configured trusted plugin "
                    "path(s)",
                    config_.trustedPluginPaths.size());
            }

            // Trust system install location unless strict plugin-dir mode is enabled.
#ifdef YAMS_INSTALL_PREFIX
            namespace fs = std::filesystem;
            if (!strictPluginDirMode) {
                fs::path system_plugins =
                    fs::path(YAMS_INSTALL_PREFIX) / "lib" / "yams" / "plugins";
                if (fs::exists(system_plugins) && fs::is_directory(system_plugins)) {
                    if (auto tr = abiHost_->trustAdd(system_plugins)) {
                        spdlog::info("Auto-trusted system plugin directory: {}",
                                     system_plugins.string());
                    } else {
                        spdlog::warn("Failed to auto-trust system plugins: {}", tr.error().message);
                    }
                }
            } else {
                spdlog::info("Strict plugin-dir mode enabled; skipping system plugin trust");
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
            spdlog::warn("ServiceManager: failed to initialize IngestService: {}", e.what());
        } catch (...) {
            spdlog::warn("ServiceManager: unknown error initializing IngestService");
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

            // Create VectorIndexCoordinator (owns all vector-index mutations)
            vectorIndexCoordinator_ = std::make_shared<VectorIndexCoordinator>(
                workCoordinator_->getExecutor(),
                nullptr, // VectorDatabase wired below after DB init
                &state_);
            spdlog::debug("[ServiceManager] VectorIndexCoordinator created");

            // Create DatabaseManager
            DatabaseManager::Dependencies dbDeps;
            dbDeps.state = &state_;
            databaseManager_ = std::make_unique<DatabaseManager>(dbDeps);
            spdlog::debug("[ServiceManager] DatabaseManager created");

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
            checkpointDeps.vectorIndexCoordinator = vectorIndexCoordinator_.get();
            checkpointDeps.state = &state_;
            checkpointDeps.hotzoneManager = nullptr;
            checkpointDeps.metadataRepository = getMetadataRepo().get();
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
    if (ingestService_) {
        try {
            ingestService_->stop();
        } catch (...) {
        }
        ingestService_.reset();
    }
}

yams::Result<void> ServiceManager::initialize() {
    // Clear any stale shutdown marker from prior daemon lifecycles in this process.
    setOnnxShutdownMarker(false);

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
    yams::common::ensureDirectories(dataDir);
    spdlog::info("ServiceManager: resolved data directory: {}", dataDir.string());
    if (isEphemeralDataDir(dataDir)) {
        spdlog::warn("ServiceManager: resolved data directory appears ephemeral: {}. "
                     "This is allowed, but status/repair results will reflect only this temporary "
                     "store.",
                     dataDir.string());
    }
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
    resolvedDataDir_ = std::move(dataDir);

    // Wire the adaptive SearchTuner's state file so EWMA counters survive daemon restarts.
    searchEngineManager_.setTunerStatePath(resolvedDataDir_ / "tuner_state.json");

    // Initialize WALManager via DatabaseManager (owns lifecycle + metrics provider)
    if (databaseManager_) {
        databaseManager_->initializeWal(resolvedDataDir_);
    }

    // Log plugin scan directories for troubleshooting
    try {
        std::string dirs;
        std::vector<std::filesystem::path> pluginDirs;
        bool strictPluginDirMode = config_.pluginDirStrict;
        if (const char* envStrict = std::getenv("YAMS_PLUGIN_DIR_STRICT")) {
            strictPluginDirMode = ConfigResolver::envTruthy(envStrict);
        }
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
        spdlog::info("Plugin default scan directories (strict={}): {}", strictPluginDirMode, dirs);
        spdlog::info("Plugin trust file: {}",
                     yams::config::get_daemon_plugin_trust_file().string());
    } catch (const std::exception& e) {
        spdlog::debug("Failed to log plugin directories: {}", e.what());
    } catch (...) {
        spdlog::debug("Failed to log plugin directories: unknown error");
    }

    // File type detector init skipped to reduce compile-time deps; non-fatal fallback remains.

    // io_context and workers already created in constructor; proceed with initialization
    spdlog::debug("ServiceManager: Using io_context from constructor");

    // Async initialization is now triggered explicitly via startAsyncInit()
    // to allow the daemon main loop to start first.

    // Configure pool defaults via ResourceGovernor from TuneAdvisor for known components
    try {
        ResourceGovernor::PoolConfig ipcCfg{};
        ipcCfg.min_size = TuneAdvisor::poolMinSizeIpc();
        if (ipcCfg.min_size < 4) {
            ipcCfg.min_size = 4;
        }
        ipcCfg.max_size = TuneAdvisor::poolMaxSizeIpc();
        ipcCfg.cooldown_ms = TuneAdvisor::poolCooldownMs();
        ipcCfg.low_watermark = TuneAdvisor::poolLowWatermarkPercent();
        ipcCfg.high_watermark = TuneAdvisor::poolHighWatermarkPercent();
        ResourceGovernor::instance().configurePool("ipc", ipcCfg);

        ResourceGovernor::PoolConfig ioCfg{};
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
        ResourceGovernor::instance().configurePool("ipc_io", ioCfg);
        spdlog::info("Pool defaults configured: ipc[min={},max={}] io[min={},max={}]",
                     ipcCfg.min_size, ipcCfg.max_size, ioCfg.min_size, ioCfg.max_size);

        // Seed FsmMetricsRegistry with initial pool sizes for immediate visibility in status
        FsmMetricsRegistry::instance().setIpcPoolSize(static_cast<uint32_t>(ipcCfg.min_size));
        FsmMetricsRegistry::instance().setIoPoolSize(static_cast<uint32_t>(ioCfg.min_size));
    } catch (const std::exception& e) {
        spdlog::debug("Pool configure error: {}", e.what());
    }

    // Search engine initialization is handled separately via searchEngineManager_.
    return Result<void>();
}

void ServiceManager::startAsyncInit(std::promise<void>* barrierPromise,
                                    std::atomic<bool>* barrierSet) {
    if (!asyncInit_.tryStart()) {
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

        self->asyncInit_.setFuture(boost::asio::co_spawn(
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

                auto token = localSelf->asyncInit_.getStopToken();

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
            boost::asio::use_future));
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

    // Signal plugin providers to fast-fail long acquire paths while shutdown drains work.
    setOnnxShutdownMarker(true);

    // Hold components that must outlive WorkCoordinator shutdown.
    // We move these out of member storage during early shutdown phases to prevent
    // accidental reuse, while keeping the objects alive until we finish draining threads.
    std::unique_ptr<CheckpointManager> checkpointManagerHold;

    // Phase 0: Signal async init coroutine to stop and wait for it to complete
    // This prevents the coroutine from accessing resources we're about to tear down
    spdlog::info("[ServiceManager] Phase 0: Requesting async init stop");
    if (asyncInit_.requestStopAndWait(std::chrono::seconds(5))) {
        spdlog::info("[ServiceManager] Phase 0: Async init completed");
    } else {
        spdlog::warn("[ServiceManager] Phase 0: Async init future timed out");
    }

    // Phase 1: Stop background task consumers FIRST (before io_context stop)
    // This signals coroutines to exit gracefully before we stop the io_context
    spdlog::info("[ServiceManager] Phase 1: Stopping background task manager");
    auto phase1Start = std::chrono::steady_clock::now();
    if (backgroundTaskManager_) {
        try {
            backgroundTaskManager_->stop();
            backgroundTaskManager_.reset();
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

    // Phase 3.5: Stop request executor and CLI request pool
    if (requestExecutor_) {
        try {
            requestExecutor_->stop();
            requestExecutor_->join();
            requestExecutor_.reset();
            spdlog::info("[ServiceManager] Phase 3.5: Request executor stopped");
        } catch (const std::exception& e) {
            spdlog::warn("[ServiceManager] Phase 3.5: Request executor stop failed: {}", e.what());
        }
    }
    // Phase 3.6: Stop CheckpointManager before WorkCoordinator
    spdlog::info("[ServiceManager] Phase 3.6: Stopping CheckpointManager");
    if (checkpointManager_) {
        checkpointManager_->stop();
        checkpointManagerHold = std::move(checkpointManager_);
        spdlog::info("[ServiceManager] Phase 3.6: CheckpointManager stopped");
    }

    // Phase 3.6.5: Stop post-ingest queue before quiescing embedding workers.
    // This prevents new extraction/embedding dispatch during shutdown and reduces metadata lock
    // contention while in-flight embed jobs finish.
    spdlog::info("[ServiceManager] Phase 3.6.5: Quiescing post-ingest queue");
    {
        auto postIngestHold = std::atomic_exchange_explicit(
            &postIngest_, std::shared_ptr<PostIngestQueue>{}, std::memory_order_acq_rel);
        if (postIngestHold) {
            try {
                postIngestHold.reset();
                spdlog::info("[ServiceManager] Phase 3.6.5: Post-ingest queue quiesced");
            } catch (const std::exception& e) {
                spdlog::warn("[ServiceManager] Phase 3.6.5: Post-ingest quiesce failed: {}",
                             e.what());
            } catch (...) {
                spdlog::warn("[ServiceManager] Phase 3.6.5: Post-ingest quiesce failed");
            }
        } else {
            spdlog::info("[ServiceManager] Phase 3.6.5: No post-ingest queue to quiesce");
        }
    }

    // Phase 3.7: Quiesce embedding service before stopping WorkCoordinator threads.
    // Embedding jobs run on WorkCoordinator executors; stopping/joining workers first can leave
    // in-flight embed tasks stranded and trigger shutdown detaches/timeouts.
    spdlog::info("[ServiceManager] Phase 3.7: Quiescing embedding service");
    {
        auto embeddingService =
            std::atomic_load_explicit(&embeddingService_, std::memory_order_acquire);
        if (embeddingService) {
            try {
                embeddingService->shutdown();
                spdlog::info("[ServiceManager] Phase 3.7: Embedding service quiesced");
            } catch (const std::exception& e) {
                spdlog::warn("[ServiceManager] Phase 3.7: Embedding service quiesce failed: {}",
                             e.what());
            } catch (...) {
                spdlog::warn("[ServiceManager] Phase 3.7: Embedding service quiesce failed");
            }
        } else {
            spdlog::info("[ServiceManager] Phase 3.7: No embedding service to quiesce");
        }
    }

    // Phase 3.8: Stop ingest service before WorkCoordinator shutdown.
    // Ingest processing dispatches work onto WorkCoordinator executors and waits on futures;
    // stopping the coordinator first can destroy pending promises and leave workers wedged.
    spdlog::info("[ServiceManager] Phase 3.8: Quiescing ingest service");
    if (ingestService_) {
        try {
            ingestService_->stop();
            ingestService_.reset();
            spdlog::info("[ServiceManager] Phase 3.8: Ingest service quiesced");
        } catch (const std::exception& e) {
            spdlog::warn("[ServiceManager] Phase 3.8: IngestService quiesce failed: {}", e.what());
        }
    } else {
        spdlog::info("[ServiceManager] Phase 3.8: No ingest service to quiesce");
    }

    // Phase 3.9: Stop the dedicated blocking I/O pool before tearing down shared state.
    // SQLite open/migrate run here; we must join so no worker touches database_ after
    // DatabaseManager is destroyed.
    spdlog::info("[ServiceManager] Phase 3.9: Stopping blocking I/O pool");
    if (blockingPool_) {
        try {
            blockingPool_->stop();
            blockingPool_->join();
            spdlog::info("[ServiceManager] Phase 3.9: Blocking I/O pool stopped");
        } catch (const std::exception& e) {
            spdlog::warn("[ServiceManager] Phase 3.9: Blocking pool stop failed: {}", e.what());
        }
        blockingPool_.reset();
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
            const bool benchmarkFastShutdown = std::getenv("YAMS_BENCH_OPT_LOOP") != nullptr ||
                                               std::getenv("YAMS_BENCH_DATASET") != nullptr;
            if (!workCoordinator_->joinWithTimeout(kShutdownTimeout)) {
                spdlog::warn("[ServiceManager] Phase 5: WorkCoordinator timed out after 5s; "
                             "retrying with extended timeout to avoid unsafe teardown races");
                constexpr auto kExtendedShutdownTimeout = std::chrono::seconds(30);
                if (!workCoordinator_->joinWithTimeout(kExtendedShutdownTimeout)) {
                    if (benchmarkFastShutdown) {
                        spdlog::warn("[ServiceManager] Phase 5: Extended timeout expired during "
                                     "benchmark shutdown; detaching remaining workers to avoid "
                                     "losing completed benchmark results");
                        workCoordinator_->abandonWorkersForShutdown();
                    } else {
                        spdlog::warn("[ServiceManager] Phase 5: Extended timeout expired; "
                                     "falling back to blocking join() to ensure clean teardown");
                        workCoordinator_->join();
                    }
                }
            }
            spdlog::info("[ServiceManager] Phase 5: WorkCoordinator threads joined");
        } catch (const std::exception& e) {
            spdlog::warn("[ServiceManager] Phase 5: WorkCoordinator join failed: {}", e.what());
        }
    }

    if (checkpointManagerHold) {
        checkpointManagerHold.reset();
        spdlog::info("[ServiceManager] Phase 5.1: CheckpointManager destroyed");
    }

    // Phase 6: Stop services in reverse dependency order
    spdlog::info("[ServiceManager] Phase 6: Shutting down daemon services");

    spdlog::info("[ServiceManager] Phase 6.0.5: Stopping repair service");
    {
        auto rs = getRepairServiceShared();
        if (rs) {
            try {
                stopRepairService();
                spdlog::info("[ServiceManager] Phase 6.0.5: Repair service stopped");
            } catch (const std::exception& e) {
                spdlog::warn("[ServiceManager] Phase 6.0.5: RepairService shutdown failed: {}",
                             e.what());
            }
        }
    }

    spdlog::info("[ServiceManager] Phase 6.1: Ingest service already quiesced");

    spdlog::info("[ServiceManager] Phase 6.2: Shutting down graph component");
    if (auto gc = loadGraphComponent()) {
        try {
            gc->shutdown();
            storeGraphComponent(std::shared_ptr<GraphComponent>{});
            spdlog::info("[ServiceManager] Phase 6.2: Graph component shut down");
        } catch (const std::exception& e) {
            spdlog::warn("[ServiceManager] Phase 6.2: GraphComponent shutdown failed: {}",
                         e.what());
        }
    }

    spdlog::info("[ServiceManager] Phase 6.3: Resetting post-ingest queue");
    {
        auto postIngestHold = std::atomic_exchange_explicit(
            &postIngest_, std::shared_ptr<PostIngestQueue>{}, std::memory_order_acq_rel);
        if (postIngestHold) {
            postIngestHold.reset();
            spdlog::info("[ServiceManager] Phase 6.3: Post-ingest queue reset");
        } else {
            spdlog::info("[ServiceManager] Phase 6.3: No post-ingest queue to reset");
        }
    }

    spdlog::info("[ServiceManager] Phase 6.3.5: Resetting embedding service");
    {
        auto embeddingServiceHold = std::atomic_exchange_explicit(
            &embeddingService_, std::shared_ptr<EmbeddingService>{}, std::memory_order_acq_rel);
        if (embeddingServiceHold) {
            embeddingServiceHold.reset();
            spdlog::info("[ServiceManager] Phase 6.3.5: Embedding service reset complete");
        } else {
            spdlog::info("[ServiceManager] Phase 6.3.5: No embedding service to reset");
        }
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
    auto modelProvider = loadModelProvider();
    if (modelProvider) {
        try {
            // Avoid unloading individual models during shutdown for ABI-backed providers.
            // The ONNX plugin can have a background preload/warmup thread; unloading models here
            // can race with that thread and lead to use-after-free (observed as
            // std::system_error("mutex lock failed: Invalid argument") during teardown).
            // For ABI providers, rely on plugin shutdown (Phase 6.9) to join background threads
            // and release resources safely.
            if (dynamic_cast<AbiModelProviderAdapter*>(modelProvider.get()) == nullptr) {
                auto loaded = modelProvider->getLoadedModels();
                for (const auto& name : loaded) {
                    (void)modelProvider->unloadModel(name);
                }
            }
            modelProvider->shutdown();
            storeModelProvider(nullptr);
        } catch (const std::exception& e) {
            spdlog::warn("[ServiceManager] Phase 6.6: Model provider shutdown failed: {}",
                         e.what());
            storeModelProvider(nullptr);
        }
    } else {
        spdlog::info("[ServiceManager] Phase 6.6: No model provider to shut down");
    }
    embeddingLifecycle_.resetWarmupState();

    // Shutdown search engine
    spdlog::info("[ServiceManager] Phase 6.7: Resetting search engine");
    searchEngineManager_.clearEngine();

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

    // Release DB-owning service graph before pool shutdown so outstanding shared_ptr owners do not
    // keep old SQLite handles alive across daemon cycles.
    spdlog::info("[ServiceManager] Phase 6.9.5: Releasing repo/content holders before DB shutdown");
    if (auto metadataRepo = getMetadataRepo()) {
        try {
            metadataRepo->shutdown();
        } catch (const std::exception& e) {
            spdlog::warn("[ServiceManager] Phase 6.9.5: MetadataRepository shutdown failed: {}",
                         e.what());
        } catch (...) {
            spdlog::warn(
                "[ServiceManager] Phase 6.9.5: MetadataRepository shutdown failed: unknown");
        }
    }
    storeGraphComponent(std::shared_ptr<GraphComponent>{});
    graphQueryServiceOverride_.reset();
    repairManager_.reset();
    contentExtractors_.clear();
    symbolExtractors_.clear();
    cachedQueryConceptExtractor_ = {};
    searchEngineManager_.clearEngine();
    if (databaseManager_) {
        databaseManager_->setContentStore(nullptr);
    }
    searchComponent_.reset();
    try {
        if (databaseManager_) {
            databaseManager_->shutdown();
            databaseManager_.reset();
            spdlog::info("[ServiceManager] Phase 6.9.5: DatabaseManager reset");
        }
    } catch (...) {
        spdlog::warn("[ServiceManager] Phase 6.9.5: Exception resetting DatabaseManager");
    }

    spdlog::info("[ServiceManager] Phase 7: Database shutdown delegated to DatabaseManager");
    try {
        if (database_) {
            database_->close();
            database_.reset();
        }
    } catch (const std::exception& e) {
        spdlog::error("[ServiceManager] Phase 7: Legacy database close failed: {}", e.what());
    }

    // Release all remaining resources
    spdlog::info("[ServiceManager] Phase 8: Releasing remaining resources");
    spdlog::info("[ServiceManager] Phase 8.3: Vector search uses VectorDatabase directly");
    spdlog::info("[ServiceManager] Phase 8.4: Content store owned by DatabaseManager");
    searchComponent_.reset();
    spdlog::info("[ServiceManager] Phase 8.4.1: Search component reset");
    graphQueryServiceOverride_.reset();
    repairManager_.reset();
    contentExtractors_.clear();
    symbolExtractors_.clear();
    cachedQueryConceptExtractor_ = {};

    spdlog::info("[ServiceManager] Phase 8.4.2: Releasing vector index coordinator");
    vectorIndexCoordinator_.reset();

    spdlog::info("[ServiceManager] Phase 8.4.5: Releasing async strands");
    initStrand_.reset();
    pluginStrand_.reset();
    modelStrand_.reset();

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
        } else {
            spdlog::info("[ServiceManager] Phase 9.3: DatabaseManager already reset");
        }
    } catch (...) {
        spdlog::warn("[ServiceManager] Phase 9.3: Exception resetting DatabaseManager");
    }
    // Phase 9.4: WALManager shutdown is now handled by DatabaseManager::shutdown()
    // (invoked in Phase 9.3 above).

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

    spdlog::info("[ServiceManager] Phase 10.5: Releasing WorkCoordinator");
    workCoordinator_.reset(); // WorkCoordinator destructor will join threads

    auto shutdownDuration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - shutdownStart);
    spdlog::info("[ServiceManager] Shutdown complete ({}ms total)", shutdownDuration.count());

    try {
        serviceFsm_.dispatch(ServiceManagerStoppedEvent{});
    } catch (...) {
        spdlog::warn("[ServiceManager] Failed to dispatch ServiceManagerStoppedEvent");
    }

    setOnnxShutdownMarker(false);
}

// Best-effort: write bootstrap status JSON so CLI can show progress before IPC is ready
static void writeBootstrapStatusFile(const yams::daemon::DaemonConfig& cfg,
                                     const yams::daemon::StateComponent& state) {
    try {
        namespace fs = std::filesystem;
        fs::path dir = yams::daemon::YamsDaemon::getXDGRuntimeDir();
        if (dir.empty())
            return;
        yams::common::ensureDirectories(dir);
        fs::path path = dir / "yams-daemon.status.json";
        nlohmann::json j;
        j["ready"] = state.readiness.bootstrapReady();
        // Normalize overall to lowercase for consistency with IPC lifecycle strings
        {
            std::string ov = state.readiness.bootstrapStatus();
            for (auto& c : ov)
                c = static_cast<char>(std::tolower(c));
            j["overall"] = ov;
        }
        nlohmann::json rd;
        rd[std::string(readiness::kIpcServer)] = state.readiness.ipcServerReady.load();
        rd[std::string(readiness::kContentStore)] = state.readiness.contentStoreReady.load();
        rd[std::string(readiness::kDatabase)] = state.readiness.databaseReady.load();
        rd[std::string(readiness::kMetadataRepo)] = state.readiness.metadataRepoReady.load();
        rd[std::string(readiness::kSearchEngine)] = state.readiness.searchEngineReady.load();
        rd[std::string(readiness::kModelProvider)] = state.readiness.modelProviderReady.load();
        rd[std::string(readiness::kVectorIndex)] = state.readiness.vectorIndexReady.load();
        rd[std::string(readiness::kPlugins)] = state.readiness.pluginsReady.load();
        // Extended vector DB readiness fields
        rd[std::string(readiness::kVectorDbInitAttempted)] =
            state.readiness.vectorDbInitAttempted.load();
        rd[std::string(readiness::kVectorDbReady)] = state.readiness.vectorDbReady.load();
        rd[std::string(readiness::kVectorDbDim)] = state.readiness.vectorDbDim.load();
        j["readiness"] = std::move(rd);
        nlohmann::json pr;
        pr[std::string(readiness::kSearchEngine)] = state.readiness.searchProgress.load();
        pr[std::string(readiness::kVectorIndex)] = state.readiness.vectorIndexProgress.load();
        pr[std::string(readiness::kModelProvider)] = state.readiness.modelLoadProgress.load();
        j["progress"] = std::move(pr);
        auto sec_since_start = std::chrono::duration_cast<std::chrono::seconds>(
                                   std::chrono::steady_clock::now() - state.stats.startTime)
                                   .count();
        std::map<std::string, int> expected_s{
            {std::string(readiness::kPlugins), 1},       {std::string(readiness::kContentStore), 2},
            {std::string(readiness::kDatabase), 2},      {std::string(readiness::kMetadataRepo), 2},
            {std::string(readiness::kVectorIndex), 3},   {std::string(readiness::kSearchEngine), 4},
            {std::string(readiness::kModelProvider), 20}};
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
        add_eta(std::string(readiness::kPlugins), state.readiness.pluginsReady.load(), 100);
        add_eta(std::string(readiness::kContentStore), state.readiness.contentStoreReady.load(),
                100);
        add_eta(std::string(readiness::kDatabase), state.readiness.databaseReady.load(), 100);
        add_eta(std::string(readiness::kMetadataRepo), state.readiness.metadataRepoReady.load(),
                100);
        add_eta(std::string(readiness::kVectorIndex), state.readiness.vectorIndexReady.load(),
                state.readiness.vectorIndexProgress.load());
        add_eta(std::string(readiness::kSearchEngine), state.readiness.searchEngineReady.load(),
                state.readiness.searchProgress.load());
        add_eta(std::string(readiness::kModelProvider), state.readiness.modelProviderReady.load(),
                state.readiness.modelLoadProgress.load());
        j["eta_seconds"] = std::move(eta);
        if (!state.initDurationsMs.empty()) {
            nlohmann::json dur;
            for (const auto& [k, v] : state.initDurationsMs) {
                dur[k] = v;
            }
            j["durations_ms"] = std::move(dur);
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
                j["top_slowest"] = std::move(top);
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
                    auto modelProvider = loadModelProvider();
                    if (includeVector && modelProvider) {
                        try {
                            embGen = embeddingLifecycle_.modelName().empty()
                                         ? modelProvider->getEmbeddingGenerator()
                                         : modelProvider->getEmbeddingGenerator(
                                               embeddingLifecycle_.modelName());
                        } catch (...) {
                        }
                    }

                    int build_timeout = 30000; // 30s timeout
                    auto result = co_await searchEngineManager_.buildEngine(
                        getMetadataRepo(), getKgStore(), getVectorDatabase(), std::move(embGen),
                        reason, build_timeout, getWorkerExecutor());

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

    // Plugins step: record readiness and duration uniformly.
    spdlog::info("[ServiceManager] Phase: Plugins Ready.");
    try {
        (void)init::record_duration(
            std::string(readiness::kPlugins),
            [&]() -> yams::Result<void> {
                try {
                    const auto ps = getPluginHostFsmSnapshot();
                    state_.readiness.pluginsReady = (ps.state == PluginHostState::Ready);
                } catch (...) {
                    // Fall back to ready when the host snapshot is unavailable.
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
        yams::common::ensureDirectories(dataDir);
        resolvedDataDir_ = dataDir;
    }

    auto storageDecision =
        yams::storage::resolveStorageBootstrapDecision(config_.configFilePath, dataDir);
    if (!storageDecision) {
        co_return Error{storageDecision.error().code,
                        std::string("Storage bootstrap resolution failed: ") +
                            storageDecision.error().message};
    }

    if (storageDecision.value().activeDataDir != dataDir) {
        dataDir = storageDecision.value().activeDataDir;
        yams::common::ensureDirectories(dataDir);
        resolvedDataDir_ = dataDir;
    }

    if (storageDecision.value().fallbackTriggered) {
        spdlog::warn("[ServiceManager] Storage fallback activated (policy={}): {}. "
                     "Using local data dir: {}",
                     yams::storage::toString(storageDecision.value().fallbackPolicy),
                     storageDecision.value().fallbackReason, dataDir.string());
    } else if (storageDecision.value().activeEngine == "s3") {
        spdlog::info("[ServiceManager] Storage engine active: s3");
    }

    spdlog::info("[ServiceManager] Phase: Data Dir Resolved.");
    spdlog::info("ServiceManager[co]: using data directory: {}", dataDir.string());

    // Content store (synchronous, quick) using init helpers
    auto storeRoot = dataDir / "storage";
    spdlog::info("ContentStore root: {}", storeRoot.string());
    {
        using T = std::unique_ptr<yams::api::IContentStore>;
        auto storeRes = init::record_duration(
            std::string(readiness::kContentStore),
            [&]() -> yams::Result<T> {
                if (storageDecision.value().storageEngineOverride) {
                    yams::api::ContentStoreBuilder builder;
                    builder.withStoragePath(storeRoot)
                        .withStorageEngine(storageDecision.value().storageEngineOverride)
                        .withCompression(false)
                        .withDeduplication(true)
                        .withIntegrityChecks(true);
                    return builder.build();
                }
                return yams::api::ContentStoreBuilder::createDefault(storeRoot);
            },
            state_.initDurationsMs);
        if (storeRes) {
            auto& uniqueStore = const_cast<T&>(storeRes.value());
            if (databaseManager_) {
                databaseManager_->setContentStore(
                    std::shared_ptr<yams::api::IContentStore>(uniqueStore.release()));
            }
            state_.readiness.contentStoreReady = true;
            writeBootstrapStatusFile(config_, state_);
        } else {
            spdlog::warn("ContentStore initialization failed: {}", storeRes.error().message);
            try {
                if (databaseManager_) {
                    databaseManager_->setContentStoreError(storeRes.error().message);
                }
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
    // Hard-cap on the blocking SQLite open. 0 = no cap (await real completion).
    // A non-zero value is an escape hatch for CI/operators; in normal operation the
    // coroutine awaits the actual open event and a watchdog emits progress logs.
    int open_timeout = read_timeout_ms("YAMS_DB_OPEN_TIMEOUT_MS", 0, 0);

    // Re-check shutdown before transitioning the database FSM.
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
        std::string(readiness::kDatabase),
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
        int mig_timeout = read_timeout_ms("YAMS_DB_MIGRATE_TIMEOUT_MS", 0, 0);
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

    // Phase: Connection pool + repo (owned by DatabaseManager)
    if (db_ok && databaseManager_) {
        databaseManager_->setDatabase(database_);
        bool poolsOk = databaseManager_->initializePools(dbPath);
        if (!poolsOk) {
            spdlog::warn("[ServiceManager] DatabaseManager pool initialization failed — degraded");
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
                    co_await co_runSessionWatcher(std::move(watcherToken));
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
            auto writePool = getWriteConnectionPool();
            if (writePool) {
                metadata::KnowledgeGraphStoreConfig kgCfg;
                kgCfg.enable_alias_fts = true;
                kgCfg.enable_wal = true;
                auto kgRes = metadata::makeSqliteKnowledgeGraphStore(*writePool, kgCfg);
                if (kgRes) {
                    auto uniqueKg = std::move(kgRes).value();
                    // Promote to shared_ptr for broader use and store as member
                    auto kgStore =
                        std::shared_ptr<metadata::KnowledgeGraphStore>(std::move(uniqueKg));
                    if (databaseManager_) {
                        databaseManager_->setKgStore(kgStore);
                    }
                    // PBI-043-12: Wire KG store to metadata repository for tree diff integration
                    auto metadataRepo = getMetadataRepo();
                    if (metadataRepo) {
                        metadataRepo->setKnowledgeGraphStore(kgStore);
                        spdlog::info(
                            "KG store wired to metadata repository for tree diff integration");
                    }
                    // PBI-009: Initialize GraphComponent after KG store is ready
                    try {
                        auto graphComponent =
                            std::make_shared<GraphComponent>(metadataRepo, kgStore, this);
                        auto initResult = graphComponent->initialize();
                        if (!initResult) {
                            spdlog::warn("GraphComponent initialization failed: {}",
                                         initResult.error().message);
                            storeGraphComponent(std::shared_ptr<GraphComponent>{});
                        } else {
                            spdlog::info("GraphComponent initialized successfully");
                            storeGraphComponent(graphComponent);
                            if (metadataRepo) {
                                metadataRepo->setGraphComponent(graphComponent);
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
        auto newPostIngest = std::make_shared<PostIngestQueue>(
            getContentStore(), getMetadataRepo(), contentExtractors_, getKgStore(),
            loadGraphComponent(), workCoordinator_.get(), nullptr, qcap);
        newPostIngest->start();

        try {
            if (config_.tuning.postIngestCapacity > 0)
                newPostIngest->setCapacity(config_.tuning.postIngestCapacity);
        } catch (...) {
        }

        std::atomic_store_explicit(&postIngest_, newPostIngest, std::memory_order_release);
        spdlog::info("Post-ingest queue initialized (capacity={})", qcap);

        // Wire PluginManager to PIQ so adoptEntityProviders() can reach it
        if (pluginManager_) {
            pluginManager_->setPostIngestQueue(newPostIngest.get());
        }

        // Wire PostIngestQueue drain to SearchEngineManager FSM
        {
            auto piq = std::atomic_load_explicit(&postIngest_, std::memory_order_acquire);
            if (piq) {
                piq->setDrainCallback([this]() {
                    const bool disableDrainTopologyRebuild = []() {
                        if (const char* env = std::getenv("YAMS_DISABLE_DRAIN_TOPOLOGY_REBUILD")) {
                            return std::string_view(env) == "1";
                        }
                        return false;
                    }();
                    const bool repairActive =
                        state_.stats.repairInProgress.load(std::memory_order_relaxed);
                    if (repairActive) {
                        spdlog::info("[ServiceManager] Skipping drain-triggered graph maintenance "
                                     "while repair RPC is active");
                    } else if (auto graphComponent = getGraphComponent()) {
                        auto maintenance = graphComponent->maintainSemanticTopology(false);
                        if (!maintenance) {
                            spdlog::warn(
                                "[ServiceManager] Semantic topology maintenance failed: {}",
                                maintenance.error().message);
                        } else if (maintenance.value().semanticEdgesPruned > 0) {
                            spdlog::info("[ServiceManager] Pruned {} one-way semantic_neighbor "
                                         "edges after drain",
                                         maintenance.value().semanticEdgesPruned);
                        }
                    }
                    if (!disableDrainTopologyRebuild && !repairActive) {
                        requestTopologyRebuild("post_ingest_drain");
                    }
                    const auto lexicalDelta = searchEngineManager_.getLexicalDeltaSnapshot();
                    if (lexicalDelta.pendingDocs > 0) {
                        searchEngineManager_.noteLexicalDeltaPublished(
                            static_cast<std::size_t>(lexicalDelta.pendingDocs));
                    }
                    spdlog::debug(
                        "[ServiceManager] PostIngestQueue drained, signaling SearchEngineManager");
                    searchEngineManager_.signalIndexingDrained();
                });
            }
        }
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
        auto embeddingService = std::make_shared<EmbeddingService>(
            getContentStore(), getMetadataRepo(), workCoordinator_.get());

        auto initRes = embeddingService->initialize();
        if (initRes) {
            embeddingService->setProviders(
                [this]() { return this->loadModelProvider(); },
                [this]() { return this->resolvePreferredModel(); },
                [this]() { return this->getVectorDatabase(); },
                [this]() { return this->getKgStore(); },
                [this](const std::string& model,
                       std::function<void(const ModelLoadEvent&)> progress) {
                    return this->ensureEmbeddingModelReadySync(model, std::move(progress),
                                                               /*timeoutMs=*/0,
                                                               /*keepHot=*/true, /*warmup=*/true);
                });
            embeddingService->setTopologyRebuildRequester(
                [this](const std::vector<std::string>& hashes) {
                    this->requestTopologyRebuild("embedding_batch_complete", hashes);
                });
            embeddingService->start();
            std::atomic_store_explicit(&embeddingService_, std::move(embeddingService),
                                       std::memory_order_release);
            spdlog::info("EmbeddingService initialized");
        } else {
            spdlog::warn("EmbeddingService initialization failed: {}", initRes.error().message);
            std::atomic_store_explicit(&embeddingService_, std::shared_ptr<EmbeddingService>{},
                                       std::memory_order_release);
        }
    } catch (const std::exception& e) {
        spdlog::warn("EmbeddingService init failed: {}", e.what());
        std::atomic_store_explicit(&embeddingService_, std::shared_ptr<EmbeddingService>{},
                                   std::memory_order_release);
    } catch (...) {
        spdlog::warn("EmbeddingService init failed (unknown)");
        std::atomic_store_explicit(&embeddingService_, std::shared_ptr<EmbeddingService>{},
                                   std::memory_order_release);
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
            auto piq = std::atomic_load_explicit(&postIngest_, std::memory_order_acquire);
            if (piq) {
                piq->setKgWriteQueue(kgWriteQueue_.get());
            }
            spdlog::debug("[ServiceManager] KGWriteQueue started");
        }
    } catch (const std::exception& e) {
        spdlog::warn("[ServiceManager] KGWriteQueue init failed: {}", e.what());
    } catch (...) {
    }

    // Cross-encoder reranker initialization happens after plugin loading.

    // Defer Vector DB initialization until after plugin adoption (provider dim)
    spdlog::info("[ServiceManager] Phase: Vector DB Init (deferred until after plugins).");

    // Vector search uses VectorDatabase directly. VectorSystemManager determines readiness
    // after preparing any persisted or rebuilt HNSW structures.
    if (getVectorDatabase()) {
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
        embeddingLifecycle_.setPreloadOnStartup(detectEmbeddingPreloadFlag());
        spdlog::info("ServiceManager: embeddingPreloadOnStartup={}",
                     embeddingLifecycle_.preloadOnStartup());

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
                state_.readiness.modelProviderReady.store(false, std::memory_order_release);
                if (config_.enableModelProvider && config_.modelProviderRequired) {
                    co_return Error{ErrorCode::NotInitialized,
                                    "Failed to adopt a model provider from plugins. Check "
                                    "plugin paths and trust settings."};
                }
                if (config_.enableModelProvider) {
                    spdlog::warn("ServiceManager: continuing startup without model provider "
                                 "(degraded mode; embeddings unavailable)");
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
        // If autoload is disabled but model provider is enabled, try the in-process
        // registry before deferring. This covers training-free backends like simeon
        // that don't need ABI plugin infrastructure.
        if (!enableAutoload && config_.enableModelProvider) {
            auto adoptResult = init::step<bool>("adopt_in_process_model_provider",
                                                [&]() { return adoptModelProviderFromHosts(); });
            if (!adoptResult || !adoptResult.value()) {
                spdlog::info("Model provider enabled with autoload disabled; deferring "
                             "initialization until Ready");
            }
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
        auto vdbRes = vectorSystemManager_ ? vectorSystemManager_->initializeOnce(dataDir)
                                           : Result<bool>(false);
        if (!vdbRes) {
            spdlog::warn("[ServiceManager] Vector DB init failed: {}", vdbRes.error().message);
        } else if (vdbRes.value()) {
            spdlog::info("[ServiceManager] Vector DB initialized successfully");
            // Wire the VDB into the coordinator now that it's ready.
            if (vectorIndexCoordinator_) {
                if (auto vdb = getVectorDatabase()) {
                    vectorIndexCoordinator_->setVectorDatabase(vdb);
                }
                // Async: load or build the initial index (sets vectorIndexReady).
                boost::asio::co_spawn(
                    workCoordinator_->getExecutor(),
                    [coord = vectorIndexCoordinator_]() -> boost::asio::awaitable<void> {
                        auto res = co_await coord->initialBuildIfNeeded();
                        if (!res) {
                            spdlog::warn("[ServiceManager] initialBuildIfNeeded failed: {}",
                                         res.error().message);
                        }
                    },
                    boost::asio::detached);
            }
            // Log vector count from database
            if (auto vectorDatabase = getVectorDatabase()) {
                auto dbVectorCount = vectorDatabase->getVectorCount();
                if (dbVectorCount > 0) {
                    spdlog::info("[VectorInit] Found {} vectors in database", dbVectorCount);
                }

                if (auto metadataRepo = getMetadataRepo()) {
                    auto embeddedHashes = vectorDatabase->getEmbeddedDocumentHashes();
                    const auto metadataEmbeddedCount =
                        static_cast<int64_t>(metadataRepo->getCachedEmbeddedCount());
                    const auto actualEmbeddedCount = static_cast<int64_t>(embeddedHashes.size());
                    if (metadataEmbeddedCount != actualEmbeddedCount) {
                        spdlog::warn("[Embeddings] Metadata/vector status mismatch: "
                                     "documents_embedded={} vector_docs={}. Reconciling "
                                     "document_embeddings_status from vector rows.",
                                     metadataEmbeddedCount, actualEmbeddedCount);
                        std::vector<std::string> reconciledHashes;
                        reconciledHashes.reserve(embeddedHashes.size());
                        for (const auto& hash : embeddedHashes) {
                            reconciledHashes.push_back(hash);
                        }
                        auto reconcileResult =
                            metadataRepo->reconcileDocumentEmbeddingStatusByHashes(
                                reconciledHashes);
                        if (!reconcileResult) {
                            spdlog::warn("[Embeddings] Failed to reconcile embedded-doc status "
                                         "from vector rows: {}",
                                         reconcileResult.error().message);
                        } else {
                            spdlog::info("[Embeddings] Reconciled embedded-doc status: {} -> {}",
                                         metadataEmbeddedCount, actualEmbeddedCount);
                        }
                    }
                }

                if (auto modelProvider = loadModelProvider();
                    modelProvider && modelProvider->isAvailable()) {
                    std::string activeModelName = embeddingLifecycle_.modelName();
                    if (activeModelName.empty()) {
                        auto loadedModels = modelProvider->getLoadedModels();
                        if (!loadedModels.empty()) {
                            activeModelName = loadedModels.front();
                        }
                    }
                    const auto dbEmbeddingDim = vectorDatabase->getConfig().embedding_dim;
                    const auto modelEmbeddingDim = modelProvider->getEmbeddingDim(activeModelName);
                    if (dbEmbeddingDim > 0 && modelEmbeddingDim > 0 &&
                        dbEmbeddingDim != modelEmbeddingDim) {
                        spdlog::warn("[Embeddings] Vector DB/model dimension mismatch: db={} "
                                     "model={} ('{}'). Vector search will be skipped until you "
                                     "select a matching model or rebuild embeddings.",
                                     dbEmbeddingDim, modelEmbeddingDim,
                                     activeModelName.empty() ? "<default>" : activeModelName);
                    }
                }
            }
        } else {
            spdlog::info("[ServiceManager] Vector DB init deferred (dim unresolved)");
        }
    }

    // Only schedule warmup if vector DB is present with non-zero dim.
    // Training-free providers have nothing to warm; skip entirely.
    if (embeddingLifecycle_.preloadOnStartup()) {
        auto provider = loadModelProvider();
        if (provider && provider->isTrainingFree()) {
            spdlog::info("[Warmup] skipped: provider '{}' is training-free",
                         provider->getProviderName());
            embeddingLifecycle_.setPreloadOnStartup(false);
        } else {
            size_t vdim = 0;
            try {
                if (auto vectorDatabase = getVectorDatabase())
                    vdim = vectorDatabase->getConfig().embedding_dim;
            } catch (...) {
            }
            if (vdim == 0) {
                spdlog::info("[Warmup] deferred: vector DB not ready or dim=0");
                embeddingLifecycle_.setPreloadOnStartup(false);
            } else {
                spdlog::info("[Warmup] embeddings.preload_on_startup detected -> background warmup "
                             "will run after Ready");
            }
        }
    }

    // Build SearchEngine with timeout
    try {
        state_.readiness.searchProgress = 10;
        writeBootstrapStatusFile(config_, state_);
        if (getMetadataRepo()) {
            state_.readiness.searchProgress = 40;
            writeBootstrapStatusFile(config_, state_);
        }
        if (getVectorDatabase()) {
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
        } else if (auto vectorDatabase = getVectorDatabase()) {
            try {
                // Use VectorDatabase directly - it knows the actual DB size
                auto vectorCount = vectorDatabase->getVectorCount();
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
        auto modelProvider = loadModelProvider();
        spdlog::info("[SearchBuild] Checking embedding generator: modelProvider_={} isAvailable={} "
                     "modelName='{}'",
                     modelProvider != nullptr, modelProvider ? modelProvider->isAvailable() : false,
                     embeddingLifecycle_.modelName());
        if (modelProvider && modelProvider->isAvailable()) {
            try {
                embGen =
                    embeddingLifecycle_.modelName().empty()
                        ? modelProvider->getEmbeddingGenerator()
                        : modelProvider->getEmbeddingGenerator(embeddingLifecycle_.modelName());
                spdlog::info("[SearchBuild] Got embedding generator: {}", embGen != nullptr);

                if (auto vectorDatabase = getVectorDatabase()) {
                    std::string activeModelName = embeddingLifecycle_.modelName();
                    if (activeModelName.empty()) {
                        auto loadedModels = modelProvider->getLoadedModels();
                        if (!loadedModels.empty()) {
                            activeModelName = loadedModels.front();
                        }
                    }
                    const auto dbEmbeddingDim = vectorDatabase->getConfig().embedding_dim;
                    const auto modelEmbeddingDim = modelProvider->getEmbeddingDim(activeModelName);
                    if (dbEmbeddingDim > 0 && modelEmbeddingDim > 0 &&
                        dbEmbeddingDim != modelEmbeddingDim) {
                        spdlog::warn("[SearchBuild] Vector DB/model dimension mismatch: db={} "
                                     "model={} ('{}'). Search vector tiers will be skipped until "
                                     "you select a matching model or rebuild embeddings.",
                                     dbEmbeddingDim, modelEmbeddingDim,
                                     activeModelName.empty() ? "<default>" : activeModelName);
                    }
                }
            } catch (const std::exception& e) {
                spdlog::warn("[SearchBuild] Failed to get embedding generator: {}", e.what());
            }
        }
        try {
            serviceFsm_.dispatch(SearchEngineBuildStartedEvent{});
        } catch (...) {
        }
        auto buildResult = co_await searchEngineManager_.buildEngine(
            getMetadataRepo(), getKgStore(), getVectorDatabase(), embGen, "initial", build_timeout,
            getWorkerExecutor());

        if (buildResult.has_value()) {
            const auto& built = buildResult.value();
            wireSearchEngineRuntimeAdapters(built, "SearchBuild");

            // Update readiness indicators after successful rebuild
            state_.readiness.searchEngineReady = true;
            state_.readiness.searchProgress = 100;
            try {
                lifecycleFsm_.setSubsystemDegraded("search", false);
            } catch (...) {
            }
            // Track doc count at build time for re-tuning decisions
            if (auto metadataRepo = getMetadataRepo()) {
                auto countRes = metadataRepo->getDocumentCount();
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
            // Drain any topology-rebuild requests that accumulated during
            // startup (embedding preload fires setTopologyRebuildRequester
            // before Ready; requestTopologyRebuild short-circuits until now).
            try {
                requestTopologyRebuild("ready_drain");
            } catch (...) {
            }
        } else {
            // Do not leave UI stuck below 100% when we are running degraded.
            try {
                state_.readiness.searchEngineReady = false;
                state_.readiness.searchProgress = 100;
            } catch (...) {
            }
            const auto reason = buildResult.error().message.empty()
                                    ? std::string{"initial search engine build not ready"}
                                    : buildResult.error().message;
            try {
                lifecycleFsm_.setSubsystemDegraded("search", true, reason);
            } catch (...) {
            }
            writeBootstrapStatusFile(config_, state_);
            spdlog::warn("[SearchBuild] initial engine build not ready; continuing degraded: {}",
                         reason);
            try {
                serviceFsm_.dispatch(SearchEngineBuiltEvent{});
            } catch (...) {
            }
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

void ServiceManager::startDeferredMetadataWarmup() {
    if (!asyncInit_.tryBeginMetadataWarmup()) {
        return;
    }

    auto metadataRepo = getMetadataRepo();
    if (!metadataRepo) {
        spdlog::debug("[ServiceManager] Deferred metadata warmup skipped: metadata repository "
                      "unavailable");
        return;
    }

    auto weakSelf = weak_from_this();
    boost::asio::post(getWorkerExecutor(), [weakSelf, metadataRepo = std::move(metadataRepo)]() {
        auto self = weakSelf.lock();
        if (!self) {
            return;
        }

        try {
            spdlog::info("[ServiceManager] Deferred metadata warmup started");
            metadataRepo->warmValueCountsCache();
            spdlog::info("[ServiceManager] Deferred metadata warmup finished");

            auto lexicalResult = metadataRepo->ensureSymSpellInitialized();
            if (!lexicalResult) {
                spdlog::warn("[ServiceManager] Deferred lexical warmup failed: {}",
                             lexicalResult.error().message);
            } else {
                spdlog::info("[ServiceManager] Deferred lexical warmup finished");
            }
        } catch (const std::exception& e) {
            spdlog::warn("[ServiceManager] Deferred metadata warmup failed: {}", e.what());
        } catch (...) {
            spdlog::warn("[ServiceManager] Deferred metadata warmup failed");
        }
    });
}

boost::asio::awaitable<void>
ServiceManager::co_runSessionWatcher(const yams::compat::stop_token& token) {
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

        try {
            if (retrievalSessions_) {
                retrievalSessions_->cleanupExpired(std::chrono::seconds(60));
            }
        } catch (const std::exception& e) {
            spdlog::debug("[ServiceManager] retrieval-session cleanup failed: {}", e.what());
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
                                                             int /*timeout_ms*/,
                                                             yams::compat::stop_token token) {
    auto ex = co_await boost::asio::this_coro::executor;

    if (token.stop_requested())
        co_return false;
    if (!blockingPool_) {
        spdlog::error("[ServiceManager] blockingPool_ not available; cannot open database");
        co_return false;
    }

    auto completed = std::make_shared<std::atomic<bool>>(false);
    const auto startedAt = std::chrono::steady_clock::now();

    // Liveness watchdog: emits periodic progress logs so a slow cold-open on external
    // volumes doesn't look like a hang. Terminates as soon as `completed` flips.
    boost::asio::co_spawn(
        ex,
        [completed, dbPath, startedAt, ex]() -> boost::asio::awaitable<void> {
            using namespace std::chrono_literals;
            boost::asio::steady_timer timer(ex);
            while (!completed->load(std::memory_order_acquire)) {
                timer.expires_after(5s);
                boost::system::error_code ec;
                co_await timer.async_wait(
                    boost::asio::redirect_error(boost::asio::use_awaitable, ec));
                if (completed->load(std::memory_order_acquire))
                    break;
                auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                                   std::chrono::steady_clock::now() - startedAt)
                                   .count();
                spdlog::info("[ServiceManager] still opening database '{}' ({}s elapsed)",
                             dbPath.string(), elapsed);
            }
            co_return;
        },
        boost::asio::detached);

    // Package the blocking SQLite open and run it off the event loop.
    auto task = std::make_shared<std::packaged_task<bool()>>([this, dbPath]() {
        bool ok = false;
        try {
            auto r = database_->open(dbPath.string(), metadata::ConnectionMode::Create);
            ok = static_cast<bool>(r);
            if (!ok) {
                spdlog::warn("Database open failed: {}", r.error().message);
            } else {
                state_.readiness.databaseReady = true;
                spdlog::info("Database opened successfully");
            }
        } catch (const std::exception& e) {
            spdlog::warn("Database open threw exception: {}", e.what());
        } catch (...) {
            spdlog::warn("Database open failed (unknown exception)");
        }
        return ok;
    });
    auto future = task->get_future();
    boost::asio::post(blockingPool_->get_executor(), [task]() { (*task)(); });

    // Suspend the coroutine until the real work finishes — no race, no fabricated events.
    bool ok = co_await init::co_await_future(future, ex);
    completed->store(true, std::memory_order_release);

    if (token.stop_requested()) {
        spdlog::warn("[ServiceManager] Database open completed but shutdown was requested; "
                     "treating as failure");
        co_return false;
    }
    co_return ok;
}

boost::asio::awaitable<bool> ServiceManager::co_migrateDatabase(int /*timeout_ms*/,
                                                                yams::compat::stop_token token) {
    auto ex = co_await boost::asio::this_coro::executor;

    if (token.stop_requested())
        co_return false;
    if (!blockingPool_) {
        spdlog::error("[ServiceManager] blockingPool_ not available; cannot migrate database");
        co_return false;
    }

    auto mm = std::make_shared<metadata::MigrationManager>(*database_);
    auto initResult = mm->initialize();
    if (!initResult) {
        spdlog::error("[ServiceManager] Failed to initialize migration system: {}",
                      initResult.error().message);
        co_return false;
    }
    mm->registerMigrations(metadata::YamsMetadataMigrations::getAllMigrations());

    auto completed = std::make_shared<std::atomic<bool>>(false);
    const auto startedAt = std::chrono::steady_clock::now();

    // Progress watchdog
    boost::asio::co_spawn(
        ex,
        [completed, startedAt, ex]() -> boost::asio::awaitable<void> {
            using namespace std::chrono_literals;
            boost::asio::steady_timer timer(ex);
            while (!completed->load(std::memory_order_acquire)) {
                timer.expires_after(5s);
                boost::system::error_code ec;
                co_await timer.async_wait(
                    boost::asio::redirect_error(boost::asio::use_awaitable, ec));
                if (completed->load(std::memory_order_acquire))
                    break;
                auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                                   std::chrono::steady_clock::now() - startedAt)
                                   .count();
                spdlog::info("[ServiceManager] still migrating database ({}s elapsed)", elapsed);
            }
            co_return;
        },
        boost::asio::detached);

    // Package the blocking migration and run it off the event loop.
    auto task = std::make_shared<std::packaged_task<bool()>>([mm]() {
        bool ok = false;
        try {
            auto r = mm->migrate();
            ok = static_cast<bool>(r);
            if (!ok) {
                spdlog::warn("Database migration failed: {}", r.error().message);
            } else {
                spdlog::info("Database migrations completed");
            }
        } catch (const std::exception& e) {
            spdlog::warn("Database migration threw exception: {}", e.what());
        } catch (...) {
            spdlog::warn("Database migration failed (unknown exception)");
        }
        return ok;
    });
    auto future = task->get_future();
    boost::asio::post(blockingPool_->get_executor(), [task]() { (*task)(); });

    bool ok = co_await init::co_await_future(future, ex);
    completed->store(true, std::memory_order_release);

    if (token.stop_requested()) {
        spdlog::warn("[ServiceManager] Database migration completed but shutdown was requested; "
                     "treating as failure");
        co_return false;
    }
    co_return ok;
}

/// NOTE: Implementation delegated to PluginManager (PBI-088 decomposition)
Result<bool> ServiceManager::adoptModelProviderFromHosts(const std::string& preferredName) {
    if (pluginManager_) {
        auto result = pluginManager_->adoptModelProvider(preferredName);
        if (result && result.value()) {
            // Sync local members from PluginManager for backward compatibility
            auto modelProvider = pluginManager_->getModelProvider();
            storeModelProvider(modelProvider);
            embeddingLifecycle_.setModelName(pluginManager_->getEmbeddingModelName());
            state_.readiness.modelProviderReady = (modelProvider != nullptr);
            spdlog::info("[ServiceManager] Synced model provider: model='{}', provider={}",
                         embeddingLifecycle_.modelName(), modelProvider ? "valid" : "null");
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
            auto piq = std::atomic_load_explicit(&postIngest_, std::memory_order_acquire);
            if (piq) {
                piq->setExtractors(contentExtractors_);
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
            auto piq = std::atomic_load_explicit(&postIngest_, std::memory_order_acquire);
            if (piq) {
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
                piq->setSymbolExtensionMap(std::move(extMap));
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
            auto piq = std::atomic_load_explicit(&postIngest_, std::memory_order_acquire);
            if (piq) {
                auto extractor = createGlinerExtractionFunc(pluginManager_->getEntityExtractors());
                if (!extractor) {
                    spdlog::warn("[ServiceManager] GLiNER extraction func is null — title "
                                 "extraction will be disabled (no entity extractors found)");
                }
                piq->setTitleExtractor(std::move(extractor));
                spdlog::info("[ServiceManager] Updated PostIngestQueue title extractor using {} "
                             "entity extractors",
                             pluginManager_->getEntityExtractors().size());
            }

            if (auto engine = getSearchEngineSnapshot()) {
                wireSearchEngineRuntimeAdapters(engine, "ServiceManager");
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
    if (requestExecutor_) {
        return requestExecutor_->getExecutor();
    }
    return getWorkerExecutor();
}

void ServiceManager::wireSearchEngineRuntimeAdapters(
    const std::shared_ptr<search::SearchEngine>& engine, const char* contextLabel) {
    if (!engine) {
        return;
    }

    cachedQueryConceptExtractor_ = createGlinerExtractionFunc(getEntityExtractors());
    engine->setConceptExtractor(cachedQueryConceptExtractor_);
    if (cachedQueryConceptExtractor_) {
        spdlog::info("[{}] GLiNER concept extractor wired to search engine", contextLabel);
    } else {
        spdlog::debug("[{}] GLiNER concept extractor unavailable", contextLabel);
    }
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
        auto graphComponent = loadGraphComponent();
        auto graphService = graphComponent ? graphComponent->getQueryService() : nullptr;

        // Get embedding generator from model provider if available
        std::shared_ptr<vector::EmbeddingGenerator> embGen;
        auto modelProvider = loadModelProvider();
        if (modelProvider && modelProvider->isAvailable()) {
            try {
                embGen =
                    embeddingLifecycle_.modelName().empty()
                        ? modelProvider->getEmbeddingGenerator()
                        : modelProvider->getEmbeddingGenerator(embeddingLifecycle_.modelName());
            } catch (...) {
            }
        }

        int build_timeout = 30000; // 30s timeout
        auto rebuildResult = co_await searchEngineManager_.buildEngine(
            getMetadataRepo(), getKgStore(), getVectorDatabase(), std::move(embGen),
            "rebuild_enabled", build_timeout, getWorkerExecutor());

        if (rebuildResult.has_value()) {
            const auto& rebuilt = rebuildResult.value();
            wireSearchEngineRuntimeAdapters(rebuilt, "Rebuild");

            // Update readiness indicators
            state_.readiness.searchEngineReady = true;
            try {
                lifecycleFsm_.setSubsystemDegraded("search", false);
            } catch (...) {
            }
            // Track doc count at build time for re-tuning decisions
            if (auto metadataRepo = getMetadataRepo()) {
                auto countRes = metadataRepo->getDocumentCount();
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
            try {
                lifecycleFsm_.setSubsystemDegraded("search", true, rebuildResult.error().message);
            } catch (...) {
            }
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
    // Skip if the active provider already reached an operational state. This prevents
    // lazy/on-demand providers from being moved back into a synthetic loading state after plugin
    // adoption.
    try {
        auto providerSnapshot = getEmbeddingProviderFsmSnapshot();
        if (providerSnapshot.state == EmbeddingProviderState::ProviderAdopted ||
            providerSnapshot.state == EmbeddingProviderState::ModelLoading ||
            providerSnapshot.state == EmbeddingProviderState::ModelReady) {
            spdlog::debug(
                "preloadPreferredModelIfConfigured: provider already adopted/loading/ready");
            co_return;
        }
    } catch (...) {
    }

    if (embeddingLifecycle_.isLoadingOrReady()) {
        spdlog::debug("preloadPreferredModelIfConfigured: local FSM already loading or ready");
        co_return;
    }

    // Signal started
    try {
        embeddingLifecycle_.fsm().dispatch(ModelLoadStartedEvent{resolvePreferredModel()});
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
            auto modelProvider = loadModelProvider();
            if (modelProvider && modelProvider->isAvailable()) {
                try {
                    embGen =
                        embeddingLifecycle_.modelName().empty()
                            ? modelProvider->getEmbeddingGenerator()
                            : modelProvider->getEmbeddingGenerator(embeddingLifecycle_.modelName());
                } catch (...) {
                }
            }

            // Phase 2.4: Use SearchEngineManager instead of co_buildEngine
            auto rebuildResult = co_await searchEngineManager_.buildEngine(
                getMetadataRepo(), getKgStore(), getVectorDatabase(), std::move(embGen), "rebuild",
                build_timeout, getWorkerExecutor());

            if (rebuildResult.has_value()) {
                const auto& rebuilt = rebuildResult.value();
                wireSearchEngineRuntimeAdapters(rebuilt, "Rebuild");

                // Update readiness indicators after successful rebuild
                state_.readiness.searchEngineReady = true;
                state_.readiness.searchProgress = 100;
                try {
                    lifecycleFsm_.setSubsystemDegraded("search", false);
                } catch (...) {
                }
                // Track doc count at build time for re-tuning decisions
                if (auto metadataRepo = getMetadataRepo()) {
                    auto countRes = metadataRepo->getDocumentCount();
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
                try {
                    lifecycleFsm_.setSubsystemDegraded("search", true,
                                                       rebuildResult.error().message);
                } catch (...) {
                }
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
    return metricsPublisher_.workerJobSignal();
}

std::shared_ptr<search::SearchEngine> ServiceManager::getSearchEngineSnapshot() const {
    return searchEngineManager_.getEngine();
}

yams::app::services::AppContext ServiceManager::getAppContext() const {
    app::services::AppContext ctx;
    ctx.service_manager = const_cast<ServiceManager*>(this);
    ctx.store = getContentStore(); // Thread-safe via atomic_load
    auto metadataRepo = getMetadataRepo();
    ctx.metadataRepo = metadataRepo;
    ctx.searchEngine = getSearchEngineSnapshot();
    ctx.vectorDatabase = getVectorDatabase();
    ctx.kgStore = getKgStore(); // PBI-043: tree diff KG integration
    auto graphComponent = loadGraphComponent();
    ctx.graphQueryService = graphQueryServiceOverride_
                                ? graphQueryServiceOverride_
                                : (graphComponent ? graphComponent->getQueryService() : nullptr);
    ctx.contentExtractors = contentExtractors_;

    // Log vector capability status
    auto modelProvider = loadModelProvider();
    bool vectorCapable = (modelProvider && modelProvider->isAvailable());
    spdlog::debug("AppContext: vector_capabilities={}", vectorCapable ? "active" : "unavailable");

    // Populate degraded/repair flags for search.
    // Do NOT degrade just because embeddings are missing; hybrid falls back to keyword/KG.
    // Only degrade when core metadata repository is unavailable.
    try {
        bool degraded = (metadataRepo == nullptr);
        int prog = 0;
        std::string details;

        // Use readiness progress when available
        try {
            prog = static_cast<int>(state_.readiness.searchProgress.load());
        } catch (...) {
        }

        if (degraded && details.empty()) {
            details = "Metadata repository unavailable";
        }

        if (prog < 0)
            prog = 0;
        if (prog > 100)
            prog = 100;

        ctx.searchRepairInProgress = degraded;
        ctx.searchRepairDetails = std::move(details);
        ctx.searchRepairProgress = prog;
    } catch (...) {
        // best-effort only
    }

    return ctx;
}

size_t ServiceManager::getWorkerQueueDepth() const {
    // A simple estimate of the queue depth based on job tracking counters.
    auto posted = static_cast<int64_t>(metricsPublisher_.workerPosted());
    auto completed = static_cast<int64_t>(metricsPublisher_.workerCompleted());
    auto active = static_cast<int64_t>(metricsPublisher_.workerActive());

    if (posted > completed + active) {
        return static_cast<size_t>(posted - completed - active);
    }
    return 0;
}

ServiceManager::SearchLoadMetrics ServiceManager::getSearchLoadMetrics() const {
    SearchLoadMetrics metrics;

    metrics.active = searchAdmission_.active();
    metrics.queued = searchAdmission_.queued();
    metrics.concurrencyLimit = ResourceGovernor::instance().maxSearchConcurrency();

    // Get search engine statistics if available
    auto engine = getSearchEngineSnapshot();
    if (engine) {
        const auto& stats = engine->getStatistics();

        // Map SearchEngine::Statistics to SearchLoadMetrics
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

    } else {
        metrics.cacheHitRate = 0.0;
    }

    return metrics;
}

boost::asio::awaitable<Result<std::string>>
ServiceManager::co_ensureEmbeddingModelReady(const std::string& requestedModel,
                                             std::function<void(const ModelLoadEvent&)> progress,
                                             int timeoutMs, bool keepHot, bool warmup) {
    co_return co_await yams::daemon::dispatch::offload_to_worker(
        this, [this, requestedModel, progress = std::move(progress), timeoutMs, keepHot,
               warmup]() mutable {
            return ensureEmbeddingModelReadySync(requestedModel, std::move(progress), timeoutMs,
                                                 keepHot, warmup);
        });
}

bool ServiceManager::startEmbeddingWarmupIfConfigured() {
    if (!embeddingLifecycle_.preloadOnStartup()) {
        return false;
    }
    embeddingLifecycle_.setPreloadOnStartup(false);

    std::shared_ptr<ServiceManager> self;
    try {
        self = shared_from_this();
    } catch (const std::bad_weak_ptr& e) {
        spdlog::warn("[Warmup] failed to start background warmup: {}", e.what());
        return false;
    }

    boost::asio::co_spawn(
        getWorkerExecutor(),
        [self]() -> boost::asio::awaitable<void> {
            auto result = co_await self->co_ensureEmbeddingModelReady(
                "",
                [](const ModelLoadEvent& ev) {
                    spdlog::info("[Warmup] model='{}' phase={} {}", ev.modelName, ev.phase,
                                 ev.message);
                },
                /*timeoutMs=*/60000, /*keepHot=*/true, /*warmup=*/true);
            if (result) {
                spdlog::info("[Warmup] embedding model ready: {}", result.value());
            } else {
                spdlog::warn("[Warmup] embedding model warmup failed: {}", result.error().message);
            }
            co_return;
        },
        boost::asio::detached);
    return true;
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
                embeddingLifecycle_.fsm().dispatch(
                    ProviderDegradedEvent{error.empty() ? std::string{"test"} : error});
            } else {
                embeddingLifecycle_.fsm().dispatch(
                    ModelLoadedEvent{embeddingLifecycle_.modelName(), 0});
            }
        }
    } catch (...) {
    }
}

void ServiceManager::enqueuePostIngest(const std::string& hash, const std::string& mime) {
    auto piq = std::atomic_load_explicit(&postIngest_, std::memory_order_acquire);
    if (!piq) {
        return;
    }

    topologyManager_.markDirty(hash);

    PostIngestQueue::Task task{hash, mime};
    searchEngineManager_.noteLexicalDeltaQueued();
    piq->enqueue(std::move(task));
}

void ServiceManager::enqueuePostIngestBatch(const std::vector<std::string>& hashes,
                                            const std::string& mime) {
    auto piq = std::atomic_load_explicit(&postIngest_, std::memory_order_acquire);
    if (!piq || hashes.empty()) {
        return;
    }

    // Keep ingestion durable even when pressure is high: documents are already stored,
    // so this stage should backpressure instead of dropping.
    std::vector<PostIngestQueue::Task> tasks;
    tasks.reserve(hashes.size());
    for (const auto& hash : hashes) {
        if (hash.empty()) {
            continue;
        }
        tasks.push_back(PostIngestQueue::Task{hash, mime});
    }
    if (!tasks.empty()) {
        searchEngineManager_.noteLexicalDeltaQueued(tasks.size());
        topologyManager_.markDirtyBatch(hashes);
        piq->enqueueBatch(std::move(tasks));
    }
}

Result<ServiceManager::TopologyRebuildStats>
ServiceManager::rebuildTopologyArtifacts(const std::string& reason, bool dryRun,
                                         const std::vector<std::string>& documentHashes) {
    return topologyManager_.rebuildArtifacts(reason, dryRun, documentHashes,
                                             tuningConfig_.topologyAlgorithm);
}

Result<std::size_t> ServiceManager::rebuildSemanticNeighborGraph(const std::string& reason) {
    auto embSvc = std::atomic_load_explicit(&embeddingService_, std::memory_order_acquire);
    if (!embSvc) {
        return Error{ErrorCode::InvalidState,
                     "rebuildSemanticNeighborGraph: embedding service unavailable"};
    }
    std::string modelName = embeddingLifecycle_.modelName();
    if (modelName.empty()) {
        modelName = "simeon-default";
    }
    spdlog::info("ServiceManager: corpus-wide semantic neighbor rebuild (reason={}, model={})",
                 reason, modelName);
    return embSvc->rebuildSemanticNeighborGraphForCorpus(modelName);
}

void ServiceManager::requestTopologyRebuild(const std::string& reason,
                                            const std::vector<std::string>& documentHashes) {
    if (shutdownInvoked_.load(std::memory_order_acquire))
        return;

    topologyManager_.markDirtyBatch(documentHashes);
    if (!topologyManager_.hasDirtyHashes())
        return;

    if (!serviceFsm_.isReady())
        return;

    if (!topologyManager_.tryScheduleRebuild())
        return;

    auto weakSelf = weak_from_this();
    auto executor = getWorkerExecutor();
    auto debounceTimer = std::make_shared<boost::asio::steady_timer>(executor);
    debounceTimer->expires_after(std::chrono::milliseconds(1000));
    debounceTimer->async_wait([weakSelf, reason,
                               debounceTimer](const boost::system::error_code& ec) mutable {
        if (ec) {
            if (auto self = weakSelf.lock())
                self->topologyManager_.clearScheduled();
            return;
        }
        if (auto self = weakSelf.lock()) {
            const auto ingestMetrics = self->getIngestMetricsSnapshot();
            auto piq = std::atomic_load_explicit(&self->postIngest_, std::memory_order_acquire);
            const auto postQueued = piq ? piq->size() : 0U;
            const auto postInFlight = piq ? piq->totalInFlight() : 0U;
            auto embeddingService =
                std::atomic_load_explicit(&self->embeddingService_, std::memory_order_acquire);
            const auto embedQueued = embeddingService ? embeddingService->queuedJobs() : 0U;
            const auto embedInFlight = embeddingService ? embeddingService->inFlightJobs() : 0U;
            auto* kgWriteQueue = self->getKgWriteQueue();
            const auto kgQueued = kgWriteQueue ? kgWriteQueue->queuedBatches() : 0U;
            const auto kgInFlight = kgWriteQueue ? kgWriteQueue->inFlight() : 0U;

            if (ingestMetrics.queued > 0 || ingestMetrics.active > 0 || postQueued > 0 ||
                postInFlight > 0 || embedQueued > 0 || embedInFlight > 0 || kgQueued > 0 ||
                kgInFlight > 0) {
                self->topologyManager_.clearScheduled();
                self->requestTopologyRebuild(reason);
                return;
            }

            auto rebuildHashes = self->topologyManager_.drainDirtyHashes();
            self->topologyManager_.clearScheduled();
            if (rebuildHashes.empty())
                return;

            if (auto metadataRepo = self->getMetadataRepo()) {
                auto statsResult = metadataRepo->getCorpusStats();
                if (statsResult && statsResult.value().usedOnlineOverlay) {
                    const auto freshness = self->getIndexFreshnessSnapshot();
                    const auto nowMs = nowUnixMillis();
                    const auto overlayAgeMs =
                        statsResult.value().reconciledComputedAtMs > 0 &&
                                nowMs > static_cast<std::uint64_t>(
                                            statsResult.value().reconciledComputedAtMs)
                            ? nowMs - static_cast<std::uint64_t>(
                                          statsResult.value().reconciledComputedAtMs)
                            : 0;
                    const bool overlayAged =
                        overlayAgeMs >= static_cast<std::uint64_t>(
                                            std::chrono::duration_cast<std::chrono::milliseconds>(
                                                kTopologyOverlayRebuildMinAge)
                                                .count());
                    const bool overlayHeavy =
                        rebuildHashes.size() >= kTopologyOverlayDirtyThreshold ||
                        freshness.lexicalDeltaRecentDocs >= kTopologyOverlayDirtyThreshold;
                    const bool forceImmediate = []() {
                        if (const char* v = std::getenv("YAMS_TEST_FORCE_TOPOLOGY_REBUILD"))
                            return v[0] != '\0' && v[0] != '0';
                        return false;
                    }();
                    if (!overlayHeavy && !overlayAged && !forceImmediate) {
                        self->topologyManager_.restoreDirtyHashes(rebuildHashes);
                        self->requestTopologyRebuild(reason);
                        return;
                    }
                }
            }

            auto result = self->rebuildTopologyArtifacts(reason, false, rebuildHashes);
            if (!result) {
                spdlog::warn("[ServiceManager] Async topology rebuild failed (reason={}): {}",
                             reason, result.error().message);
                self->topologyManager_.restoreDirtyHashes(rebuildHashes);
            } else if (result.value().skipped) {
                self->topologyManager_.restoreDirtyHashes(rebuildHashes);
            }
        }
    });
}

void ServiceManager::startRepairService(std::function<size_t()> activeConnFn) {
    RepairServiceHost::Config rcfg;
    rcfg.enable = true;
    rcfg.dataDir = resolvedDataDir_;
    rcfg.maxBatch = static_cast<std::uint32_t>(config_.autoRepairBatchSize);
    rcfg.autoRebuildOnDimMismatch = config_.autoRebuildOnDimMismatch;
    rcfg.maxPendingRepairs = config_.maxPendingRepairs;
    repairServiceHost_.start(std::move(rcfg), &state_, std::move(activeConnFn),
                             makeRepairServiceContext(this));
}

void ServiceManager::stopRepairService() {
    repairServiceHost_.stop();
}

} // namespace yams::daemon
