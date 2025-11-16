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
#include <string>
#include <system_error>
#include <thread>

// Platform-specific malloc pressure relief for macOS
#ifdef __APPLE__
#include <malloc/malloc.h>
#endif

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/experimental/channel.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_future.hpp>
#include <tl/expected.hpp>
#include <yams/api/content_store_builder.h>
#include <yams/app/services/session_service.hpp>
#include <yams/compat/thread_stop_compat.h>
#include <yams/core/types.h>
#include <yams/daemon/components/BackgroundTaskManager.h>
#include <yams/daemon/components/DaemonMetrics.h>
#include <yams/daemon/components/EmbeddingService.h>
#include <yams/daemon/components/EntityGraphService.h>
#include <yams/daemon/components/GraphComponent.h>
#include <yams/daemon/components/IngestService.h>
#include <yams/daemon/components/init_utils.hpp>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/PoolManager.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/ipc/retrieval_session.h>

#include <yams/daemon/resource/abi_content_extractor_adapter.h>
#include <yams/daemon/resource/abi_model_provider_adapter.h>
#include <yams/daemon/resource/abi_plugin_loader.h>
#include <yams/daemon/resource/abi_symbol_extractor_adapter.h>
#include <yams/daemon/resource/model_provider.h>
#include <yams/daemon/resource/plugin_host.h>
#include <yams/extraction/extraction_util.h>
#include <yams/integrity/repair_manager.h>
#include <yams/metadata/migration.h>
#include <yams/plugins/symbol_extractor_v1.h>
#include <yams/repair/embedding_repair_util.h>
#include <yams/search/search_engine_builder.h>
#include <yams/vector/sqlite_vec_backend.h>
#include <yams/vector/vector_database.h>

namespace {
// Minimal helpers - prefer using components directly
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
    } catch (const std::exception& e) {
        spdlog::debug("Failed to read embedding dimension from {}: {}", dbPath.string(), e.what());
    } catch (...) {
        spdlog::debug("Failed to read embedding dimension from {}: unknown error", dbPath.string());
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
    } catch (const std::exception& e) {
        spdlog::debug("Failed to write vector sentinel: {}", e.what());
    } catch (...) {
        spdlog::debug("Failed to write vector sentinel: unknown error");
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
    } catch (const std::exception& e) {
        spdlog::debug("Failed to read vector sentinel: {}", e.what());
    } catch (...) {
        spdlog::debug("Failed to read vector sentinel: unknown error");
    }
    return std::nullopt;
}

std::filesystem::path resolveDefaultConfigPath() {
    if (const char* explicitPath = std::getenv("YAMS_CONFIG_PATH")) {
        std::filesystem::path p{explicitPath};
        if (std::filesystem::exists(p))
            return p;
    }
    if (const char* xdg = std::getenv("XDG_CONFIG_HOME")) {
        std::filesystem::path p = std::filesystem::path(xdg) / "yams" / "config.toml";
        if (std::filesystem::exists(p))
            return p;
    }
    if (const char* home = std::getenv("HOME")) {
        std::filesystem::path p = std::filesystem::path(home) / ".config" / "yams" / "config.toml";
        if (std::filesystem::exists(p))
            return p;
    }
    return {};
}

std::map<std::string, std::string> parseSimpleTomlFlat(const std::filesystem::path& path) {
    std::map<std::string, std::string> config;
    std::ifstream file(path);
    if (!file)
        return config;

    std::string line;
    std::string currentSection;
    auto trim = [](std::string s) {
        auto issp = [](unsigned char c) { return std::isspace(c) != 0; };
        while (!s.empty() && issp(static_cast<unsigned char>(s.front())))
            s.erase(s.begin());
        while (!s.empty() && issp(static_cast<unsigned char>(s.back())))
            s.pop_back();
        return s;
    };

    while (std::getline(file, line)) {
        auto comment = line.find('#');
        if (comment != std::string::npos)
            line = line.substr(0, comment);
        line = trim(line);
        if (line.empty())
            continue;

        if (line.front() == '[' && line.back() == ']') {
            currentSection = line.substr(1, line.size() - 2);
            continue;
        }

        auto eq = line.find('=');
        if (eq == std::string::npos)
            continue;
        std::string key = trim(line.substr(0, eq));
        std::string value = trim(line.substr(eq + 1));
        if (!value.empty() && value.front() == '"' && value.back() == '"') {
            value = value.substr(1, value.size() - 2);
        }
        if (!currentSection.empty()) {
            config[currentSection + "." + key] = value;
        } else {
            config[key] = value;
        }
    }
    return config;
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
    std::shared_lock lk(pluginStatusMutex_);
    return pluginStatusSnapshot_;
}

void ServiceManager::refreshPluginStatusSnapshot() {
    PluginStatusSnapshot snapshot;
    try {
        snapshot.host = pluginHostFsm_.snapshot();
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
        if (abiHost_) {
            auto loaded = abiHost_->listLoaded();
            snapshot.records.reserve(loaded.size());
            for (const auto& d : loaded) {
                PluginStatusRecord rec;
                rec.name = d.name;
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
        workCoordinator_->start();
        spdlog::info("[ServiceManager] WorkCoordinator created with {} worker threads",
                     workCoordinator_->getWorkerCount());

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
        // Defer vector DB initialization to async phase to avoid blocking daemon startup (PBI-057).
        spdlog::debug("[Startup] deferring vector DB init to async phase");
        // Auto-trust plugin directory from env if provided.
        try {
            if (const char* env = std::getenv("YAMS_PLUGIN_DIR")) {
                std::filesystem::path penv(env);
                if (!penv.empty()) {
                    if (abiHost_) {
                        if (auto tr1 = abiHost_->trustAdd(penv); !tr1) {
                            spdlog::warn("Failed to auto-trust YAMS_PLUGIN_DIR {}: {}",
                                         penv.string(), tr1.error().message);
                            try {
                                pluginHostFsm_.dispatch(PluginLoadFailedEvent{tr1.error().message});
                            } catch (const std::exception& e) {
                                spdlog::debug("FSM dispatch failed for PluginLoadFailedEvent: {}",
                                              e.what());
                            } catch (...) {
                                spdlog::debug(
                                    "FSM dispatch failed for PluginLoadFailedEvent: unknown error");
                            }
                        }
                    }
                    if (abiPluginLoader_) {
                        if (auto tr2 = abiPluginLoader_->trustAdd(penv); !tr2) {
                            spdlog::warn("Failed to auto-trust YAMS_PLUGIN_DIR for loader {}: {}",
                                         penv.string(), tr2.error().message);
                            try {
                                pluginHostFsm_.dispatch(PluginLoadFailedEvent{tr2.error().message});
                            } catch (const std::exception& e) {
                                spdlog::debug("FSM dispatch failed for PluginLoadFailedEvent: {}",
                                              e.what());
                            } catch (...) {
                                spdlog::debug(
                                    "FSM dispatch failed for PluginLoadFailedEvent: unknown error");
                            }
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
                        try {
                            pluginHostFsm_.dispatch(PluginLoadFailedEvent{trc.error().message});
                        } catch (const std::exception& e) {
                            spdlog::debug("FSM dispatch failed for PluginLoadFailedEvent: {}",
                                          e.what());
                        } catch (...) {
                            spdlog::debug(
                                "FSM dispatch failed for PluginLoadFailedEvent: unknown error");
                        }
                    } else {
                        spdlog::debug("Trusted configured pluginDir {} for ABI host",
                                      pconf.string());
                        try {
                            pluginHostFsm_.dispatch(PluginTrustVerifiedEvent{});
                        } catch (const std::exception& e) {
                            spdlog::debug("FSM dispatch failed for PluginTrustVerifiedEvent: {}",
                                          e.what());
                        } catch (...) {
                            spdlog::debug(
                                "FSM dispatch failed for PluginTrustVerifiedEvent: unknown error");
                        }
                    }
                }
                if (abiPluginLoader_) {
                    if (auto trc2 = abiPluginLoader_->trustAdd(pconf); !trc2) {
                        spdlog::warn(
                            "Failed to trust configured pluginDir for legacy loader {}: {}",
                            pconf.string(), trc2.error().message);
                        try {
                            pluginHostFsm_.dispatch(PluginLoadFailedEvent{trc2.error().message});
                        } catch (const std::exception& e) {
                            spdlog::debug("FSM dispatch failed for PluginLoadFailedEvent: {}",
                                          e.what());
                        } catch (...) {
                            spdlog::debug(
                                "FSM dispatch failed for PluginLoadFailedEvent: unknown error");
                        }
                    } else {
                        spdlog::debug("Trusted configured pluginDir {} for legacy plugin loader",
                                      pconf.string());
                    }
                }
            }
        } catch (const std::exception& e) {
            spdlog::warn("Exception trusting configured pluginDir: {}", e.what());
        }

        // Auto-trust system install location for plugins
#ifdef YAMS_INSTALL_PREFIX
        try {
            namespace fs = std::filesystem;
            fs::path system_plugins = fs::path(YAMS_INSTALL_PREFIX) / "lib" / "yams" / "plugins";
            if (fs::exists(system_plugins) && fs::is_directory(system_plugins)) {
                spdlog::debug("Found system plugin directory: {}", system_plugins.string());
                if (abiHost_) {
                    if (auto trc = abiHost_->trustAdd(system_plugins)) {
                        spdlog::info("Auto-trusted system plugin directory: {}",
                                     system_plugins.string());
                        try {
                            pluginHostFsm_.dispatch(PluginTrustVerifiedEvent{});
                        } catch (...) {
                        }
                    } else {
                        spdlog::warn("Failed to auto-trust system plugins: {}",
                                     trc.error().message);
                    }
                }
                if (abiPluginLoader_) {
                    if (auto trc2 = abiPluginLoader_->trustAdd(system_plugins)) {
                        spdlog::debug("Auto-trusted system plugins for legacy loader");
                    } else {
                        spdlog::warn("Failed to auto-trust system plugins for legacy loader: {}",
                                     trc2.error().message);
                    }
                }
            } else {
                spdlog::debug("System plugin directory not found: {}", system_plugins.string());
            }
        } catch (const std::exception& e) {
            spdlog::warn("Exception auto-trusting system plugins: {}", e.what());
        }
#else
        spdlog::debug("YAMS_INSTALL_PREFIX not defined; skipping system plugin auto-trust");
#endif

        try {
            if (!ingestService_) {
                ingestService_ = std::make_unique<IngestService>(this, workCoordinator_.get());
                // Initialize EntityGraphService skeleton
                try {
                    if (!entityGraphService_) {
                        entityGraphService_ = std::make_shared<EntityGraphService>(this, 1);
                        entityGraphService_->start();
                        spdlog::info("EntityGraphService initialized (workers=1)");
                    }
                } catch (const std::exception& e2) {
                    spdlog::warn("Failed to initialize EntityGraphService: {}", e2.what());
                } catch (...) {
                    spdlog::warn("Unknown error initializing EntityGraphService");
                }
            }
        } catch (const std::exception& e) {
            spdlog::warn("ServiceManager: failed to initialize IngestService scaffold: {}",
                         e.what());
        } catch (...) {
            spdlog::warn("ServiceManager: unknown error initializing IngestService scaffold");
        }
    } catch (const std::exception& e) {
        spdlog::warn("Exception during ServiceManager constructor setup: {}", e.what());
    }
}

bool ServiceManager::invokeInitCompleteOnce(bool success, const std::string& error) {
    bool expected = false;
    if (!initCompleteInvoked_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        return false; // already invoked
    }
    try {
        if (initCompleteCallback_) {
            initCompleteCallback_(success, error);
        }
        // Do not null out the callback pointer here; tests may introspect it.
    } catch (...) {
        // Swallow to avoid terminating threads
    }
    return true;
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
        for (const auto& d : std::vector<std::filesystem::path>{
                 (std::getenv("HOME") ? std::filesystem::path(std::getenv("HOME")) / ".local" /
                                            "lib" / "yams" / "plugins"
                                      : std::filesystem::path()),
                 std::filesystem::path("/usr/local/lib/yams/plugins"),
                 std::filesystem::path("/usr/lib/yams/plugins")
#ifdef YAMS_INSTALL_PREFIX
                     ,
                 std::filesystem::path(YAMS_INSTALL_PREFIX) / "lib" / "yams" / "plugins"
#endif
             }) {
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

    // Launch async initialization without blocking or using futures
    // Use detached coroutine with completion callback
    boost::asio::co_spawn(
        workCoordinator_->getExecutor(),
        [this]() -> boost::asio::awaitable<void> {
            spdlog::info("Starting async resource initialization (coroutine)...");

            // Create a dummy stop_token since we're not using initThread_ anymore
            yams::compat::stop_source stop_src;
            auto token = stop_src.get_token();

            try {
                auto result = co_await this->initializeAsyncAwaitable(token);

                if (!result) {
                    spdlog::error("Async resource initialization failed: {}",
                                  result.error().message);
                    try {
                        serviceFsm_.dispatch(InitializationFailedEvent{result.error().message});
                    } catch (...) {
                    }
                    (void)invokeInitCompleteOnce(false, result.error().message);
                } else {
                    spdlog::info("All daemon services initialized successfully");
                    (void)invokeInitCompleteOnce(true, "");
                }
            } catch (const std::exception& e) {
                spdlog::error("Async resource initialization exception: {}", e.what());
                try {
                    serviceFsm_.dispatch(InitializationFailedEvent{e.what()});
                } catch (...) {
                }
                (void)invokeInitCompleteOnce(false, e.what());
            }
        }(),
        boost::asio::detached);

    // Removed: defensive 100ms sleep (unnecessary with proper async initialization)

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
        // Post-ingest pool (background CPU) — derive bounds from TuningConfig
        PoolManager::Config piCfg{};
        try {
            const auto& cfg = tuningConfig_;
            piCfg.min_size =
                static_cast<uint32_t>(std::max<std::size_t>(1, cfg.postIngestThreadsMin));
            piCfg.max_size =
                static_cast<uint32_t>(std::max(cfg.postIngestThreadsMin, cfg.postIngestThreadsMax));
        } catch (...) {
            piCfg.min_size = 1;
            piCfg.max_size = 8;
        }
        piCfg.cooldown_ms = TuneAdvisor::poolCooldownMs();
        piCfg.low_watermark = TuneAdvisor::poolLowWatermarkPercent();
        piCfg.high_watermark = TuneAdvisor::poolHighWatermarkPercent();
        PoolManager::instance().configure("post_ingest", piCfg);
        spdlog::info("PoolManager defaults configured: ipc[min={},max={}] io[min={},max={}] "
                     "post_ingest[min={},max={}]",
                     ipcCfg.min_size, ipcCfg.max_size, ioCfg.min_size, ioCfg.max_size,
                     piCfg.min_size, piCfg.max_size);
    } catch (const std::exception& e) {
        spdlog::debug("PoolManager configure error: {}", e.what());
    }

    // Sanity check: if dependencies are ready but searchExecutor_ not initialized
    if (state_.readiness.databaseReady.load() && state_.readiness.metadataRepoReady.load() &&
        !std::atomic_load(&searchExecutor_)) {
        spdlog::warn("SearchExecutor not initialized despite database and metadata repo ready");
    }
    return Result<void>();
}

// Modern async initialization (Phase 0b): Sequential coroutine-based initialization
// with structured concurrency and automatic rollback on error
boost::asio::awaitable<yams::Result<void>>
ServiceManager::initialize_async(boost::asio::any_io_executor exec) {
    // Get cancellation state for checking cancellation requests
    auto token = co_await boost::asio::this_coro::cancellation_state;
    spdlog::info("[ServiceManager] Starting async initialization with structured concurrency");

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

    // Check cancellation before each phase
    if (token.cancelled() != boost::asio::cancellation_type::none) {
        co_return yams::Result<void>(
            Error{ErrorCode::OperationCancelled, "Initialization cancelled"});
    }

    // Phase 1: Content Store
    spdlog::info("[ServiceManager] Phase: Content Store Init");
    if (auto r = co_await co_initContentStore(exec, token); !r) {
        spdlog::error("[ServiceManager] Content Store init failed: {}", r.error().message);
        co_return yams::Result<void>(r.error());
    }

    // Check cancellation
    if (token.cancelled() != boost::asio::cancellation_type::none) {
        co_return yams::Result<void>(
            Error{ErrorCode::OperationCancelled, "Initialization cancelled"});
    }

    // Phase 2: Database
    spdlog::info("[ServiceManager] Phase: Database Init");
    if (auto r = co_await co_initDatabase(exec, token); !r) {
        spdlog::error("[ServiceManager] Database init failed: {}", r.error().message);
        // Automatic cleanup: previous phases' RAII handles rollback
        co_return yams::Result<void>(r.error());
    }

    // Check cancellation
    if (token.cancelled() != boost::asio::cancellation_type::none) {
        co_return yams::Result<void>(
            Error{ErrorCode::OperationCancelled, "Initialization cancelled"});
    }

    // Phase 3: Search Engine
    spdlog::info("[ServiceManager] Phase: Search Engine Init");
    if (auto r = co_await co_initSearchEngine(exec, token); !r) {
        spdlog::error("[ServiceManager] Search engine init failed: {}", r.error().message);
        co_return yams::Result<void>(r.error());
    }

    // Check cancellation
    if (token.cancelled() != boost::asio::cancellation_type::none) {
        co_return yams::Result<void>(
            Error{ErrorCode::OperationCancelled, "Initialization cancelled"});
    }

    // Phase 4: Vector System
    spdlog::info("[ServiceManager] Phase: Vector System Init");
    if (auto r = co_await co_initVectorSystem(exec, token); !r) {
        spdlog::error("[ServiceManager] Vector system init failed: {}", r.error().message);
        co_return yams::Result<void>(r.error());
    }

    // Check cancellation
    if (token.cancelled() != boost::asio::cancellation_type::none) {
        co_return yams::Result<void>(
            Error{ErrorCode::OperationCancelled, "Initialization cancelled"});
    }

    // Phase 5: Plugin System
    spdlog::info("[ServiceManager] Phase: Plugin System Init");
    if (auto r = co_await co_initPluginSystem(exec, token); !r) {
        spdlog::error("[ServiceManager] Plugin system init failed: {}", r.error().message);
        co_return yams::Result<void>(r.error());
    }

    // Check cancellation
    if (token.cancelled() != boost::asio::cancellation_type::none) {
        co_return yams::Result<void>(
            Error{ErrorCode::OperationCancelled, "Initialization cancelled"});
    }

    // Success: all phases completed
    spdlog::info("[ServiceManager] All phases completed successfully");

    // Invoke legacy callback if registered (compatibility with existing callback-based system)
    if (initCompleteCallback_) {
        try {
            initCompleteCallback_(true, "");
        } catch (...) {
            spdlog::warn("Init complete callback threw exception; continuing");
        }
    }

    co_return yams::Result<void>{};
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

    // Phase 2: Signal stop to session watcher
    spdlog::info("[ServiceManager] Phase 2: Stopping session watcher");
    try {
        if (sessionWatchStopSource_.stop_possible())
            sessionWatchStopSource_.request_stop();
        if (sessionWatcherFuture_.valid()) {
            sessionWatcherFuture_.get();
            sessionWatcherFuture_ = std::future<void>();
        }
        spdlog::info("[ServiceManager] Phase 2: Session watcher stopped");
    } catch (const std::exception& e) {
        spdlog::warn("[ServiceManager] Phase 2: Session watcher stop failed: {}", e.what());
    } catch (...) {
        spdlog::warn("[ServiceManager] Phase 2: Session watcher stop failed");
    }

    // Phase 3: (Removed - work guard now managed by WorkCoordinator)
    spdlog::info("[ServiceManager] Phase 3: Skipped (work guard managed by WorkCoordinator)");

    // Phase 4: Cancel all asynchronous operations and stop WorkCoordinator
    spdlog::info("[ServiceManager] Phase 4: Cancelling async operations");
    shutdownSignal_.emit(boost::asio::cancellation_type::terminal);
    if (workCoordinator_) {
        workCoordinator_->stop();
        spdlog::info("[ServiceManager] Phase 4: WorkCoordinator stop() called");
    }

    // Phase 5: Join worker threads (delegated to WorkCoordinator destructor)
    spdlog::info("[ServiceManager] Phase 5: WorkCoordinator will join threads on destruction");

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

    spdlog::info("[ServiceManager] Phase 6.3: Stopping entity graph service");
    if (entityGraphService_) {
        try {
            entityGraphService_->stop();
            entityGraphService_.reset();
            spdlog::info("[ServiceManager] Phase 6.3: Entity graph service stopped");
        } catch (const std::exception& e) {
            spdlog::warn("[ServiceManager] Phase 6.3: EntityGraphService shutdown failed: {}",
                         e.what());
        }
    } else {
        spdlog::info("[ServiceManager] Phase 6.2: No entity graph service to stop");
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

    // Persist vector index when ready
    spdlog::info("[ServiceManager] Phase 6.4: Saving vector index");
    if (vectorIndexManager_ && state_.readiness.vectorIndexReady.load()) {
        try {
            auto indexPath = config_.dataDir / "vector_index.bin";

            // Create directory if needed
            std::error_code ec;
            std::filesystem::create_directories(indexPath.parent_path(), ec);

            spdlog::info("[ServiceManager] Phase 6.4: Saving vector index to '{}'",
                         indexPath.string());
            auto saveRes = vectorIndexManager_->saveIndex(indexPath.string());

            if (!saveRes) {
                spdlog::warn("[ServiceManager] Phase 6.4: Failed to save vector index: {}",
                             saveRes.error().message);
            } else {
                auto stats = vectorIndexManager_->getStats();
                spdlog::info(
                    "[ServiceManager] Phase 6.4: Vector index saved successfully ({} vectors)",
                    stats.num_vectors);
            }
        } catch (const std::exception& e) {
            spdlog::warn("[ServiceManager] Phase 6.4: Vector index save exception: {}", e.what());
        }
    } else {
        spdlog::info("[ServiceManager] Phase 6.4: No vector index to save or not ready");
    }

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
    std::atomic_store(&searchExecutor_, std::shared_ptr<search::SearchExecutor>());
    spdlog::info("[ServiceManager] Phase 9.1: Search executor reset");
    metadataRepo_.reset();
    spdlog::info("[ServiceManager] Phase 9.2: Metadata repository reset");
    vectorIndexManager_.reset();
    spdlog::info("[ServiceManager] Phase 8.3: Vector index manager reset");
    contentStore_.reset();
    spdlog::info("[ServiceManager] Phase 8.4: Content store reset");

    spdlog::info("[ServiceManager] Phase 8.5: Releasing WorkCoordinator");
    workCoordinator_.reset(); // WorkCoordinator destructor will join threads

#ifdef __APPLE__
    malloc_zone_pressure_relief(nullptr, 0);
#endif

    spdlog::info("[ServiceManager] Phase 9: Releasing plugin infrastructure");
    try {
        abiPluginLoader_.reset();
        spdlog::info("[ServiceManager] Phase 9.1: ABI plugin loader reset");
    } catch (...) {
        spdlog::warn("[ServiceManager] Phase 9.1: Exception resetting ABI plugin loader");
    }
    try {
        abiHost_.reset();
        spdlog::info("[ServiceManager] Phase 9.2: ABI host reset");
    } catch (...) {
        spdlog::warn("[ServiceManager] Phase 9.2: Exception resetting ABI host");
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
yams::Result<bool>
ServiceManager::initializeVectorDatabaseOnce(const std::filesystem::path& dataDir) {
    // In-process guard
    // Guard (first-wins); may be reset if we intentionally defer
    if (vectorDbInitAttempted_.exchange(true, std::memory_order_acq_rel)) {
        spdlog::debug("[VectorInit] skipped (already attempted in this process)");
        try {
            state_.readiness.vectorDbInitAttempted = true;
        } catch (...) {
            // Intentionally ignored - best-effort state update
        }
        return Result<bool>(false);
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
    // Always allow table creation (DB file may exist without virtual tables)
    cfg.create_if_missing = true;

    // Resolve embedding dimension with precedence:
    // 1. Existing DB DDL (if present)
    // 2. Config file ~/.config/yams/config.toml
    // 3. Env YAMS_EMBED_DIM
    // 4. Embedding generator (if already available)
    // 5. Provider preferred model (no hardcoded fallback)
    std::optional<size_t> dim;
    if (exists) {
        try {
            auto ddlDim = read_db_embedding_dim(cfg.database_path);
            if (ddlDim && *ddlDim > 0)
                dim = *ddlDim;
            try {
                spdlog::info("[VectorInit] probe: ddl dim={}", ddlDim ? *ddlDim : 0);
            } catch (const std::exception& e) {
                spdlog::debug("Failed to log DDL dimension: {}", e.what());
            } catch (...) {
                spdlog::debug("Failed to log DDL dimension: unknown error");
            }
        } catch (const std::exception& e) {
            spdlog::debug("Failed to read DDL dimension: {}", e.what());
        } catch (...) {
            spdlog::debug("Failed to read DDL dimension: unknown error");
        }
    }
    // Config file (only if no provider available)
    if (!dim && (!modelProvider_ || !modelProvider_->isAvailable())) {
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
                                spdlog::info("[VectorInit] probe: config dim={}", *dim);

                            } catch (const std::exception& e) {
                                spdlog::debug("Failed to parse config embedding_dim: {}", e.what());
                            } catch (...) {
                                spdlog::debug(
                                    "Failed to parse config embedding_dim: unknown error");
                            }
                        }
                        break;
                    }
                }
            }
        } catch (const std::exception& e) {
            spdlog::debug("Failed to read config file for embedding dimension: {}", e.what());
        } catch (...) {
            spdlog::debug("Failed to read config file for embedding dimension: unknown error");
        }
    }
    // Env (only if no provider available)
    if (!dim && (!modelProvider_ || !modelProvider_->isAvailable())) {
        try {
            if (const char* envd = std::getenv("YAMS_EMBED_DIM"))
                dim = static_cast<size_t>(std::stoul(envd));
        } catch (...) {
            spdlog::info("[VectorInit] probe: config/env dim={}", (dim ? *dim : 0));
        }
    }
    // Ask provider preferred model first; optionally try a short load if dim remains 0
    if (!dim) {
        try {
            std::string preferred = resolvePreferredModel();
            if (!preferred.empty() && modelProvider_ && modelProvider_->isAvailable()) {
                size_t prov = modelProvider_->getEmbeddingDim(preferred);
                spdlog::info("[VectorInit] probe: provider preferred='{}' prov_dim={} (pre-load)",
                             preferred, prov);
                if (prov == 0) {
                    // Attempt a bounded model load to obtain authoritative dim
                    int load_ms = 0;
                    if (const char* s = std::getenv("YAMS_PROVIDER_LOAD_DIM_TIMEOUT_MS")) {
                        try {
                            load_ms = std::max(0, std::stoi(s));
                        } catch (const std::exception& e) {
                            spdlog::debug("Failed to parse YAMS_PROVIDER_LOAD_DIM_TIMEOUT_MS: {}",
                                          e.what());
                        } catch (...) {
                            spdlog::debug(
                                "Failed to parse YAMS_PROVIDER_LOAD_DIM_TIMEOUT_MS: unknown error");
                        }
                    }
                    if (load_ms > 0) {
                        auto r = modelProvider_->loadModel(preferred);
                        spdlog::info("[VectorInit] provider loadModel('{}') status={}", preferred,
                                     r ? 0 : -1);
                        prov = modelProvider_->getEmbeddingDim(preferred);
                        spdlog::info(
                            "[VectorInit] probe: provider preferred='{}' prov_dim={} (post-load)",
                            preferred, prov);
                    }
                }
                if (prov > 0) {
                    dim = prov;
                    spdlog::info("[VectorInit] using provider dim={} from '{}'", *dim, preferred);
                } else {
                    spdlog::info("[VectorInit] provider did not report dim for '{}' yet; will "
                                 "fallback/defer",
                                 preferred);
                }
            }
        } catch (const std::exception& e) {
            spdlog::debug("Failed to probe model provider dimension: {}", e.what());
        } catch (...) {
            spdlog::debug("Failed to probe model provider dimension: unknown error");
        }
    }
    // Generator (only if no provider available)
    if (!dim && (!modelProvider_ || !modelProvider_->isAvailable())) {
        try {
            spdlog::info("[VectorInit] probe: generator dim={}", (dim ? *dim : 0));

            {
                size_t g = getEmbeddingDimension();
                if (g > 0)
                    dim = g;
            }
        } catch (const std::exception& e) {
            spdlog::debug("Failed to get embedding dimension from generator: {}", e.what());
        } catch (...) {
            spdlog::debug("Failed to get embedding dimension from generator: unknown error");
        }
    }
    if (!dim) {
        spdlog::info("[VectorInit] deferring initialization (provider dim unresolved)");
        try {
            state_.readiness.vectorDbInitAttempted = false;
        } catch (...) {
            // Intentionally ignored - best-effort state reset
        }
        try {
            vectorDbInitAttempted_.store(false, std::memory_order_release);
        } catch (...) {
            // Intentionally ignored - best-effort atomic reset
        }
        return Result<bool>(false);
    }

    if (!dim) {
        spdlog::warn("[VectorInit] embedding_dim unresolved (DB/config/env/provider). Vector DB "
                     "will initialize without embeddings.");
    }
    cfg.embedding_dim = *dim;

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
        spdlog::info("[VectorInit] Opening lock file: {}", lockPath.string());
        lock_fd = ::open(lockPath.c_str(), O_CREAT | O_RDWR, 0644);
        if (lock_fd >= 0) {
            spdlog::info("[VectorInit] Acquiring lock on: {}", lockPath.string());
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
                spdlog::info("[VectorInit] Lock acquired.");
                // Stamp pid for diagnostics
                try {
                    (void)ftruncate(lock_fd, 0);
                    std::string stamp = std::to_string(static_cast<long long>(::getpid())) + "\n";
                    (void)::write(lock_fd, stamp.data(), stamp.size());
                    (void)lseek(lock_fd, 0, SEEK_SET);
                } catch (...) {
                    // Intentionally ignored - best-effort lock file update
                }
            }
        } else {
            spdlog::warn("[VectorInit] could not open lock file (continuing without lock)");
        }
    } catch (...) {
        spdlog::warn("[VectorInit] lock setup error (continuing without lock)");
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
            try {
                if (!vdb->initialize()) {
                    auto err = vdb->getLastError();
                    spdlog::warn("[VectorInit] initialization attempt {} failed: {}", attempt + 1,
                                 err);
                    goto init_failed_path;
                }
                spdlog::info("[VectorInit] vdb->initialize() succeeded.");
                vectorDatabase_ = std::move(vdb);
                goto init_success_path;
            } catch (...) {
                spdlog::warn("[VectorInit] initialization attempt {} raised exception",
                             attempt + 1);
                goto init_failed_path;
            }

        init_failed_path:
            spdlog::info("[VectorInit] Calling vdb->initialize() attempt {}", attempt + 1);
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
                spdlog::info("[VectorInit] vdb->initialize() succeeded.");
                vectorDatabase_ = std::move(vdb);

            init_success_path:
                // Initialize component-owned metrics (sync with DB once at startup)
                try {
                    vectorDatabase_->initializeCounter();
                } catch (const std::exception& e) {
                    spdlog::debug("Failed to initialize vector database counter: {}", e.what());
                } catch (...) {
                    spdlog::debug("Failed to initialize vector database counter: unknown error");
                }
                spdlog::info("[VectorInit] end pid={} tid={} path={} dim={} attempts={}",
                             static_cast<long long>(::getpid()), (void*)(&tid), cfg.database_path,
                             cfg.embedding_dim, attempt + 1);
                try {
                    state_.readiness.vectorDbReady = true;
                    state_.readiness.vectorDbDim = static_cast<uint32_t>(cfg.embedding_dim);
                } catch (...) {
                    // Intentionally ignored - best-effort state update
                }
                try {
                    serviceFsm_.dispatch(VectorsInitializedEvent{cfg.embedding_dim});
                } catch (const std::exception& e) {
                    spdlog::debug("FSM dispatch failed for VectorsInitializedEvent: {}", e.what());
                } catch (...) {
                    spdlog::debug("FSM dispatch failed for VectorsInitializedEvent: unknown error");
                }
                // Sentinel write & quick health probes (best-effort)
                try {
                    write_vector_sentinel(dataDir, cfg.embedding_dim, "vec0", 1);
                } catch (...) {
                    // Intentionally ignored - best-effort sentinel write
                }
                try {
                    std::size_t rows = vectorDatabase_->getVectorCount();
                    spdlog::debug("[VectorInit] current row count={} (initial, cached)", rows);
                } catch (...) {
                    // Intentionally ignored - best-effort row count probe
                }
                try {
                    auto sdim = read_vector_sentinel_dim(dataDir);
                    if (sdim && *sdim != cfg.embedding_dim) {
                        spdlog::warn("[VectorInit] sentinel dimension mismatch sentinel={} "
                                     "actual={} — run 'yams doctor' if needed",
                                     *sdim, cfg.embedding_dim);
                    }
                } catch (...) {
                    // Intentionally ignored - best-effort sentinel dimension check
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
        spdlog::info("[VectorInit] Releasing lock.");
        struct flock fl{};
        fl.l_type = F_UNLCK;
        fl.l_whence = SEEK_SET;
        fl.l_start = 0;
        fl.l_len = 0;
        (void)fcntl(lock_fd, F_SETLK, &fl);
        ::close(lock_fd);
        lock_fd = -1;
        spdlog::info("[VectorInit] Lock released.");
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
    spdlog::info("[ServiceManager] Async initialization started.");
    // Defer Vector DB init to post-plugins phase; skip here
    spdlog::info("[ServiceManager] Phase: Vector DB Init (skipped pre-plugins).");
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
    spdlog::info("[ServiceManager] Phase: Plugins Ready.");
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
                    spdlog::info("Metadata repository initialized successfully");

                    // Note: RepairManager initialization deferred to RepairCoordinator
                    // since it needs access to storage engine which is not directly
                    // exposed by IContentStore interface

                    return yams::Result<void>();
                },
                state_.initDurationsMs);
            if (!repoRes) {
                spdlog::warn("Metadata repository init failed: {}", repoRes.error().message);
            }
        }
        writeBootstrapStatusFile(config_, state_);
    }
    spdlog::info("[ServiceManager] Phase: DB Pool and Repo Initialized.");

    // Executors and sessions
    // Lightweight session directory watcher (polling), reacts to SessionService config.
    try {
        auto exec = getWorkerExecutor();
        sessionWatchStopSource_ = yams::compat::stop_source{};
        auto token = sessionWatchStopSource_.get_token();
        sessionWatcherFuture_ = boost::asio::co_spawn(
            exec,
            [this, token]() -> boost::asio::awaitable<void> {
                co_await co_runSessionWatcher(token);
            },
            boost::asio::use_future);
    } catch (...) {
    }

    if (database_ && metadataRepo_)
        std::atomic_store(&searchExecutor_,
                          std::make_shared<search::SearchExecutor>(database_, metadataRepo_));
    retrievalSessions_ = std::make_unique<RetrievalSessionManager>();
    spdlog::info("[ServiceManager] Phase: Executors and Sessions Initialized.");

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
                        graphComponent_ = std::make_shared<GraphComponent>(metadataRepo_, kgStore_);
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
            workCoordinator_.get(), qcap);
        postIngest_->start();

        try {
            if (config_.tuning.postIngestCapacity > 0)
                postIngest_->setCapacity(config_.tuning.postIngestCapacity);
        } catch (...) {
        }
        spdlog::info("Post-ingest queue initialized (capacity={})", qcap);
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
        std::size_t postIngestThreads = taThreads
                                            ? static_cast<std::size_t>(taThreads)
                                            : static_cast<std::size_t>(TA::postIngestThreads());
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

    // Defer Vector DB initialization until after plugin adoption (provider dim)
    spdlog::info("[ServiceManager] Phase: Vector DB Init (deferred until after plugins).");

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
                derivedIdxDim = getEmbeddingDimension();
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

                // Load persisted vector index if it exists
                auto indexPath = config_.dataDir / "vector_index.bin";
                bool indexLoaded = false;
                if (std::filesystem::exists(indexPath)) {
                    spdlog::info("[VectorInit] Loading persisted vector index from '{}'",
                                 indexPath.string());
                    auto loadRes = vectorIndexManager_->loadIndex(indexPath.string());
                    if (!loadRes) {
                        spdlog::warn("[VectorInit] Failed to load vector index: {}. Will rebuild "
                                     "from database.",
                                     loadRes.error().message);
                    } else {
                        auto stats = vectorIndexManager_->getStats();
                        spdlog::info("[VectorInit] Loaded vector index with {} vectors",
                                     stats.num_vectors);
                        indexLoaded = (stats.num_vectors > 0);
                    }
                } else {
                    spdlog::debug("[VectorInit] No persisted vector index found at '{}'",
                                  indexPath.string());
                }

                // If index wasn't loaded or is empty, attempt to populate from VectorDatabase
                if (!indexLoaded && vectorDatabase_) {
                    spdlog::info("[VectorInit] Populating vector index from database...");
                    try {
                        // Get all unique document hashes from the database
                        auto stats = vectorDatabase_->getStats();
                        if (stats.total_vectors > 0) {
                            spdlog::info("[VectorInit] Found {} vectors in database, loading into "
                                         "search index",
                                         stats.total_vectors);

                            spdlog::warn(
                                "[VectorInit] Direct database->index population not yet "
                                "implemented. "
                                "Vectors will be available after first document is added or after "
                                "running 'yams repair --embeddings'");
                        } else {
                            spdlog::debug("[VectorInit] Database is empty, no vectors to load");
                        }
                    } catch (const std::exception& e) {
                        spdlog::warn("[VectorInit] Failed to populate from database: {}", e.what());
                    }
                }
            }
        } else {
            spdlog::warn("Vector index initialization disabled by YAMS_DISABLE_VECTOR_DB");
        }
    } catch (const std::exception& e) {
        spdlog::warn("Exception initializing VectorIndexManager: {}", e.what());
    }
    spdlog::info("[ServiceManager] Phase: Vector Index Manager Initialized.");

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
            bool enableSymbols = true;
            try {
                auto cfgPath = resolveDefaultConfigPath();
                if (!cfgPath.empty()) {
                    auto flat = parseSimpleTomlFlat(cfgPath);
                    auto it = flat.find("plugins.symbol_extraction.enable");
                    if (it != flat.end()) {
                        std::string v = it->second;
                        std::transform(v.begin(), v.end(), v.begin(), ::tolower);
                        enableSymbols = !(v == "0" || v == "false" || v == "off" || v == "no");
                    }
                }
            } catch (...) {
            }
            if (enableSymbols) {
                auto symRes = init::step<size_t>(
                    "adopt_symbol_extractors", [&]() { return adoptSymbolExtractorsFromHosts(); });
                if (symRes) {
                    spdlog::info("ServiceManager: Adopted {} symbol extractors.", symRes.value());
                }
            } else {
                spdlog::info("ServiceManager: symbol extractor plugins disabled by config");
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
    state_.readiness.pluginsReady = true;
    refreshPluginStatusSnapshot();

    spdlog::info("[ServiceManager] Phase: Vector DB Init (post-plugins, sync).");
    {
        auto vdbRes = initializeVectorDatabaseOnce(dataDir);
        if (!vdbRes) {
            spdlog::warn("[ServiceManager] Vector DB init failed: {}", vdbRes.error().message);
        } else if (vdbRes.value()) {
            spdlog::info("[ServiceManager] Vector DB initialized successfully");

            // Now that VectorDatabase is initialized, load vectors into VectorIndexManager if
            // needed
            if (vectorIndexManager_ && vectorDatabase_) {
                auto stats = vectorIndexManager_->getStats();
                if (stats.num_vectors == 0) {
                    // Index is empty - check if we have vectors in the database
                    auto dbStats = vectorDatabase_->getStats();
                    if (dbStats.total_vectors > 0) {
                        spdlog::info(
                            "[VectorInit] Found {} vectors in database but search index is empty. "
                            "Run 'yams repair --embeddings' to rebuild the search index from "
                            "database.",
                            dbStats.total_vectors);
                    }
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

        // Determine vector_enabled: check if vector DB has usable data
        bool vectorEnabled = false;
        if (vectorDatabase_) {
            try {
                // Use VectorDatabase directly - it knows the actual DB size
                auto vectorCount = vectorDatabase_->getVectorCount();
                vectorEnabled = (vectorCount > 0);
                spdlog::info("[SearchBuild] Vector DB has {} vectors, vector_enabled={}",
                             vectorCount, vectorEnabled);
            } catch (const std::exception& e) {
                spdlog::warn("[SearchBuild] Could not check vector count: {}", e.what());
            }
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
        if (modelProvider_ && modelProvider_->isAvailable() && !embeddingModelName_.empty()) {
            try {
                embGen = modelProvider_->getEmbeddingGenerator(embeddingModelName_);
            } catch (...) {
            }
        }
        auto graphService = graphComponent_ ? graphComponent_->getQueryService() : nullptr;
        auto buildResult = co_await searchEngineManager_.buildEngine(
            metadataRepo_, vectorIndexManager_, embGen, graphService, "initial", build_timeout,
            getWorkerExecutor());

        if (buildResult.has_value()) {
            const auto& built = buildResult.value();
            std::lock_guard<std::shared_mutex> lk(searchEngineMutex_); // Exclusive write
            searchEngine_ = built;
            state_.readiness.searchEngineReady = true;
            state_.readiness.searchProgress = 100;
            writeBootstrapStatusFile(config_, state_);
            spdlog::info("HybridSearchEngine initialized and published to AppContext");
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
        spdlog::warn("Exception wiring HybridSearchEngine: {}", e.what());
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
                auto patterns = sess->getPinnedPatterns(*current);
                for (const auto& pat : patterns) {
                    std::error_code ec;
                    std::filesystem::path p(pat);
                    if (!p.empty() && std::filesystem::is_directory(p, ec)) {
                        auto& dirMap = sessionWatch_.dirFiles[p.string()];
                        std::unordered_map<std::string, std::pair<std::uint64_t, std::uint64_t>>
                            cur;
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
                                InternalEventBus::StoreDocumentTask t;
                                t.request.path = fp;
                                t.request.recursive = false;
                                t.request.noEmbeddings = true;
                                static std::shared_ptr<
                                    SpscQueue<InternalEventBus::StoreDocumentTask>>
                                    q = InternalEventBus::instance()
                                            .get_or_create_channel<
                                                InternalEventBus::StoreDocumentTask>(
                                                "store_document_tasks", 4096);
                                if (q)
                                    (void)q->try_push(std::move(t));
                            }
                        }
                        dirMap.swap(cur);
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

    // Use pure awaitable pattern with timeout timer (no futures!)
    boost::asio::steady_timer timeout_timer(ex);
    timeout_timer.expires_after(std::chrono::milliseconds(timeout_ms));

    try {
        using ResultPtr = std::shared_ptr<Result<void>>;
        using Channel = boost::asio::experimental::basic_channel<
            boost::asio::any_io_executor, boost::asio::experimental::channel_traits<std::mutex>,
            void(std::exception_ptr, ResultPtr)>;
        auto resultChannel = std::make_shared<Channel>(ex, 2);

        boost::asio::co_spawn(
            ex,
            [this, dbPath, resultChannel]() -> boost::asio::awaitable<void> {
                std::exception_ptr opException;
                ResultPtr opResult;
                try {
                    auto r = database_->open(dbPath.string(), metadata::ConnectionMode::Create);
                    opResult = std::make_shared<Result<void>>(std::move(r));
                } catch (...) {
                    opException = std::current_exception();
                }
                resultChannel->try_send(opException, std::move(opResult));
                co_return;
            },
            boost::asio::detached);

        using namespace boost::asio::experimental::awaitable_operators;

        auto race = co_await (
            resultChannel->async_receive(boost::asio::as_tuple(boost::asio::use_awaitable)) ||
            timeout_timer.async_wait(boost::asio::as_tuple(boost::asio::use_awaitable)));

        if (race.index() == 1) {
            spdlog::warn("Database open timed out after {} ms — continuing in degraded mode",
                         timeout_ms);
            resultChannel->close();
            co_return false;
        }

        timeout_timer.cancel();

        const auto channelTuple = std::get<0>(race);
        auto opException = std::get<0>(channelTuple);
        auto opResult = std::get<1>(channelTuple);

        if (opException) {
            try {
                std::rethrow_exception(opException);
            } catch (const std::exception& e) {
                spdlog::warn("Database open threw exception: {} — continuing in degraded mode",
                             e.what());
            }
            co_return false;
        }

        if (!opResult) {
            spdlog::warn("Database open failed: unknown error — continuing in degraded mode");
            co_return false;
        }

        if (*opResult) {
            state_.readiness.databaseReady = true;
            spdlog::info("Database opened successfully");
            co_return true;
        }

        spdlog::warn("Database open failed: {} — continuing in degraded mode",
                     opResult->error().message);
        co_return false;
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
    metadata::MigrationManager mm(*database_);
    auto initResult = mm.initialize();
    if (!initResult) {
        spdlog::error("[ServiceManager] Failed to initialize migration system: {}",
                      initResult.error().message);
        co_return false;
    }
    mm.registerMigrations(metadata::YamsMetadataMigrations::getAllMigrations());

    // Use pure awaitable pattern with timeout timer (no futures!)
    boost::asio::steady_timer timeout_timer(ex);
    timeout_timer.expires_after(std::chrono::milliseconds(timeout_ms));

    try {
        using ResultPtr = std::shared_ptr<Result<void>>;
        using Channel = boost::asio::experimental::basic_channel<
            boost::asio::any_io_executor, boost::asio::experimental::channel_traits<std::mutex>,
            void(std::exception_ptr, ResultPtr)>;
        auto resultChannel = std::make_shared<Channel>(ex, 2);

        boost::asio::co_spawn(
            ex,
            [&mm, resultChannel]() -> boost::asio::awaitable<void> {
                std::exception_ptr opException;
                ResultPtr opResult;
                try {
                    auto r = mm.migrate();
                    opResult = std::make_shared<Result<void>>(std::move(r));
                } catch (...) {
                    opException = std::current_exception();
                }
                resultChannel->try_send(opException, std::move(opResult));
                co_return;
            },
            boost::asio::detached);

        using namespace boost::asio::experimental::awaitable_operators;

        auto race = co_await (
            resultChannel->async_receive(boost::asio::as_tuple(boost::asio::use_awaitable)) ||
            timeout_timer.async_wait(boost::asio::as_tuple(boost::asio::use_awaitable)));

        if (race.index() == 1) {
            spdlog::warn("Database migration timed out after {} ms", timeout_ms);
            resultChannel->close();
            co_return false;
        }

        timeout_timer.cancel();

        const auto channelTuple = std::get<0>(race);
        auto opException = std::get<0>(channelTuple);
        auto opResult = std::get<1>(channelTuple);

        if (opException) {
            try {
                std::rethrow_exception(opException);
            } catch (const std::exception& e) {
                spdlog::warn("Database migration threw exception: {}", e.what());
            }
            co_return false;
        }

        if (!opResult) {
            spdlog::warn("Database migration failed before producing a result");
            co_return false;
        }

        co_return static_cast<bool>(*opResult);
    } catch (const std::exception& e) {
        spdlog::warn("Database migration failed (exception): {}", e.what());
        co_return false;
    } catch (...) {
        spdlog::warn("Database migration failed (unknown exception)");
        co_return false;
    }
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
                auto ifaceRes = abiHost_->getInterface(pluginName, "model_provider_v1", 2);
                if (!ifaceRes) {
                    spdlog::warn("Model provider iface not found for plugin '{}' (path='{}') : {}",
                                 pluginName, path_for(pluginName), ifaceRes.error().message);
                    try {
                        embeddingFsm_.dispatch(
                            ProviderDegradedEvent{std::string("iface not found: ") + pluginName});
                    } catch (...) {
                    }
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
                    try {
                        embeddingFsm_.dispatch(
                            ProviderDegradedEvent{std::string("abi mismatch: ") + pluginName});
                    } catch (...) {
                    }
                    return false;
                }
                modelProvider_ = std::make_shared<AbiModelProviderAdapter>(table);
                state_.readiness.modelProviderReady = (modelProvider_ != nullptr);
                spdlog::info("Adopted model provider from plugin: {} (path='{}', abi={})",
                             pluginName, path_for(pluginName), (int)table->abi_version);
                adoptedProviderPluginName_ = pluginName;
                clearModelProviderError();
                try {
                    auto count = static_cast<std::uint32_t>(modelProvider_->getLoadedModelCount());
                    setCachedModelProviderModelCount(count);
                } catch (...) {
                    setCachedModelProviderModelCount(0);
                }
                refreshPluginStatusSnapshot();
                try {
                    embeddingFsm_.dispatch(ProviderAdoptedEvent{pluginName});
                } catch (...) {
                }

                try {
                    if (modelProvider_ && modelProvider_->isAvailable()) {
                        std::string modelName = resolvePreferredModel();
                        size_t dimension = modelProvider_->getEmbeddingDim(modelName);
                        spdlog::info("[Provider] Model provider ready: model='{}', dim={}",
                                     modelName, dimension);
                        embeddingFsm_.dispatch(ModelLoadedEvent{modelName, dimension});
                        lifecycleFsm_.setSubsystemDegraded("embeddings", false);
                    } else {
                        spdlog::warn("[Provider] Model provider not available after adoption");
                    }
                } catch (const std::exception& e) {
                    spdlog::warn("[Provider] Failed to check provider availability: {}", e.what());
                }

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

            for (const auto& d : loaded) {
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
                        try {
                            auto count =
                                static_cast<std::uint32_t>(modelProvider_->getLoadedModelCount());
                            setCachedModelProviderModelCount(count);
                        } catch (...) {
                            setCachedModelProviderModelCount(0);
                        }
                        refreshPluginStatusSnapshot();
                        return Result<bool>(true);
                    } catch (...) {
                    }
                }
            }
        }
    } catch (const std::exception& e) {
        return Error{ErrorCode::Unknown, e.what()};
    }
    // No suitable provider was adopted. Surface a degraded state so clients/tests
    // can detect and present actionable diagnostics.
    try {
        embeddingFsm_.dispatch(ProviderDegradedEvent{"no provider adopted"});
    } catch (...) {
        // best-effort: FSM dispatch should not interfere with result propagation
    }
    refreshPluginStatusSnapshot();
    return Result<bool>(false);
}

Result<size_t> ServiceManager::adoptContentExtractorsFromHosts() {
    try {
        size_t adopted = adoptPluginInterface<yams_content_extractor_v1, AbiContentExtractorAdapter,
                                              yams::extraction::IContentExtractor>(
            abiHost_.get(), "content_extractor_v1", YAMS_IFACE_CONTENT_EXTRACTOR_V1_VERSION,
            contentExtractors_, [](const yams_content_extractor_v1* table) {
                return table->abi_version == YAMS_IFACE_CONTENT_EXTRACTOR_V1_VERSION;
            });
        return Result<size_t>(adopted);
    } catch (const std::exception& e) {
        return Error{ErrorCode::Unknown, e.what()};
    }
}

boost::asio::any_io_executor ServiceManager::getWorkerExecutor() const {
    if (workCoordinator_)
        return workCoordinator_->getExecutor();
    return boost::asio::system_executor();
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
    auto self = shared_from_this(); // Capture shared_from_this for coroutine safety
    auto plugin_load_executor = getWorkerExecutor(); // Use the new io_context executor
    std::vector<boost::asio::awaitable<Result<PluginDescriptor>>> load_tasks;
    size_t loaded_count = 0; // Initialize loaded_count here
    try {
        // FSM guard: avoid concurrent autoload scans
        try {
            auto ps = pluginHostFsm_.snapshot().state;
            spdlog::info("Plugin autoload(now): FSM state check - current state: {}",
                         static_cast<int>(ps));
            if (ps == PluginHostState::ScanningDirectories ||
                ps == PluginHostState::LoadingPlugins) {
                spdlog::warn("Plugin autoload skipped: scan already in progress (state={})",
                             static_cast<int>(ps));
                co_return Result<size_t>(0);
            }
            // VerifyingTrust and NotInitialized are OK - proceed with scan
        } catch (...) {
        }
        refreshPluginStatusSnapshot();
        if (config_.useMockModelProvider || env_truthy(std::getenv("YAMS_USE_MOCK_PROVIDER"))) {
            spdlog::info("Plugin autoload skipped (mock provider in use)");
            co_return Result<size_t>(0);
        }
        if (const char* d = std::getenv("YAMS_DISABLE_ABI_PLUGINS"); d && *d) {
            spdlog::info("Plugin autoload disabled by YAMS_DISABLE_ABI_PLUGINS");
            co_return Result<size_t>(0);
        }
        std::vector<std::filesystem::path> roots;
        if (abiHost_) {
            for (const auto& p : abiHost_->trustList())
                roots.push_back(p);
        }
        try {
            namespace fs = std::filesystem;
            if (const char* home = std::getenv("HOME")) {
                roots.push_back(fs::path(home) / ".local" / "lib" / "yams" / "plugins");
            }
            roots.push_back(std::filesystem::path("/usr/local/lib/yams/plugins"));
            roots.push_back(std::filesystem::path("/usr/lib/yams/plugins"));
#ifdef YAMS_INSTALL_PREFIX
            roots.push_back(std::filesystem::path(YAMS_INSTALL_PREFIX) / "lib" / "yams" /
                            "plugins");
#endif
        } catch (...) {
        }

        std::sort(roots.begin(), roots.end());
        roots.erase(std::unique(roots.begin(), roots.end()), roots.end());

        spdlog::info("Plugin autoload(now): {} roots to scan", roots.size());
        try {
            pluginHostFsm_.dispatch(PluginScanStartedEvent{roots.size()});
        } catch (...) {
        }
        for (const auto& r : roots) {
            spdlog::info("Plugin autoload(now): scanning root {}", r.string());
        }
        for (const auto& r : roots) {
            try {
                if (self->abiHost_) {                                 // Use self->abiHost_
                    if (auto sr = self->abiHost_->scanDirectory(r)) { // Use self->abiHost_
                        if (sr.value().empty()) {
                            spdlog::info("Plugin autoload(now): no candidates in {}", r.string());
                        } else {
                            spdlog::info("Plugin autoload(now): found {} candidate(s) in {}",
                                         sr.value().size(), r.string());
                        }
                        for (const auto& d : sr.value()) {
                            spdlog::info(
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
                            // Create an awaitable for each plugin load
                            load_tasks.push_back(boost::asio::co_spawn(
                                plugin_load_executor,
                                [self, d]() -> boost::asio::awaitable<Result<PluginDescriptor>> {
                                    co_return self->abiHost_->load(d.path, "");
                                },
                                boost::asio::use_awaitable));
                        }
                    } else {
                        // Scan failure (e.g., invalid directory): mark degraded
                        try {
                            self->pluginHostFsm_.dispatch(
                                PluginLoadFailedEvent{sr.error().message});
                        } catch (...) {
                        }
                        self->refreshPluginStatusSnapshot();
                    }
                }
            } catch (const std::exception& e) {
                spdlog::warn("Plugin autoload(now): scan/load error at {}: {}", r.string(),
                             e.what());
                try {
                    self->pluginHostFsm_.dispatch(PluginLoadFailedEvent{e.what()});
                } catch (...) {
                }
                self->refreshPluginStatusSnapshot();
            } catch (...) {
                spdlog::warn("Plugin autoload(now): unknown error at {}", r.string());
                try {
                    self->pluginHostFsm_.dispatch(PluginLoadFailedEvent{"unknown error"});
                } catch (...) {
                    // Ignore FSM dispatch errors
                }
                self->refreshPluginStatusSnapshot();
            }
        }
        // Await all plugin load tasks (sequentially for simplicity with Result<T>)
        if (!load_tasks.empty()) {
            for (auto& task : load_tasks) {
                try {
                    auto res = co_await std::move(task);
                    if (res) {
                        ++loaded_count;
                        spdlog::info("Plugin autoload(now): loaded '{}' (ifaces=[{}])",
                                     res.value().name, [&]() {
                                         std::string s;
                                         for (size_t i = 0; i < res.value().interfaces.size();
                                              ++i) {
                                             if (i)
                                                 s += ",";
                                             s += res.value().interfaces[i];
                                         }
                                         return s;
                                     }());
                        try {
                            self->pluginHostFsm_.dispatch(PluginLoadedEvent{res.value().name});
                        } catch (...) {
                        }
                        self->refreshPluginStatusSnapshot();
                    } else {
                        spdlog::warn("Plugin autoload(now): load failed: {}", res.error().message);
                        try {
                            self->pluginHostFsm_.dispatch(
                                PluginLoadFailedEvent{res.error().message});
                        } catch (...) {
                        }
                        self->refreshPluginStatusSnapshot();
                    }
                } catch (const std::exception& e) {
                    spdlog::warn("Plugin autoload(now): exception awaiting task: {}", e.what());
                }
            }
        }
        spdlog::info("Plugin autoload(now): loaded {} plugin(s)", loaded_count);
        try {
            self->pluginHostFsm_.dispatch(AllPluginsLoadedEvent{loaded_count});
        } catch (...) {
        }
        self->refreshPluginStatusSnapshot();
        auto adopted = self->adoptModelProviderFromHosts();
        if (adopted && adopted.value()) {
            spdlog::info("Plugin autoload(now): model provider adopted");
        } else {
            spdlog::info("Plugin autoload(now): no model provider adopted");
        }
        (void)self->adoptContentExtractorsFromHosts();
        spdlog::info("Model preload deferred until after initialization completes");
        writeBootstrapStatusFile(self->config_, self->state_);
        co_return Result<size_t>(loaded_count);
    } catch (const std::exception& e) {
        co_return Error{ErrorCode::InternalError, e.what()};
    }
}

void ServiceManager::preloadPreferredModelIfConfigured() {
    // FSM-based idempotence: skip if already loading or ready
    if (embeddingFsm_.isLoadingOrReady()) {
        spdlog::debug("preloadPreferredModelIfConfigured: already loading or ready (FSM)");
        return;
    }

    try {
        (void)init::step<void>("schedule_model_preload", [&]() -> yams::Result<void> {
            spdlog::info("Scheduling preferred model preload (async)");
            return yams::Result<void>();
        });
        // Skip if embedding already ready (FSM is authoritative)
        if (embeddingFsm_.isReady()) {
            spdlog::debug("preloadPreferredModelIfConfigured: already ready (FSM)");
            return;
        }

        if (!modelProvider_) {
            spdlog::debug("preloadPreferredModelIfConfigured: no model provider available");
            return;
        }

        std::string preferred = resolvePreferredModel();
        if (preferred.empty()) {
            spdlog::info("Model preload skipped: no preferred model configured (set "
                         "embeddings.preferred_model in config or install a model)");
            return;
        }
        spdlog::info("Preloading preferred model: {}", preferred);
        try {
            embeddingFsm_.dispatch(ModelLoadStartedEvent{preferred});
        } catch (...) {
        }

        // Use the worker executor from the new io_context architecture
        auto executor = getWorkerExecutor();
        spdlog::info("Using io_context executor for preload of '{}'", preferred);

        // Safely handle shared_from_this - it may not be available during early initialization
        try {
            spdlog::info("Attempting shared_from_this() for model load task '{}'", preferred);
            auto self = shared_from_this();
            spdlog::info(
                "shared_from_this() succeeded, about to post model load task to executor for '{}'",
                preferred);
            boost::asio::post(executor, [self, preferred]() {
                spdlog::info("***** INSIDE POSTED LAMBDA for '{}'", preferred);
                spdlog::info("Model preload task started for '{}'", preferred);
                try {
                    spdlog::info("Calling modelProvider_->loadModel('{}')...", preferred);
                    auto r = self->modelProvider_->loadModel(preferred);
                    spdlog::info("modelProvider_->loadModel('{}') returned: success={}", preferred,
                                 r.has_value());
                    if (r) {
                        self->state_.readiness.modelProviderReady.store(true,
                                                                        std::memory_order_relaxed);
                        self->state_.readiness.modelLoadProgress.store(100,
                                                                       std::memory_order_relaxed);
                        spdlog::info("Preferred model '{}' preloaded successfully", preferred);
                        self->clearModelProviderError();
                        try {
                            std::size_t dim = 0;
                            try {
                                dim = self->getEmbeddingDimension();
                            } catch (...) {
                            }
                            self->embeddingFsm_.dispatch(
                                ModelLoadedEvent{self->embeddingModelName_, dim});
                        } catch (...) {
                        }
                    } else {
                        self->state_.readiness.modelLoadProgress.store(0,
                                                                       std::memory_order_relaxed);
                        spdlog::warn("Preferred model '{}' preload failed: {}", preferred,
                                     r.error().message);
                        const std::string errorMsg =
                            std::string("preload failed: ") + r.error().message;
                        try {
                            self->embeddingFsm_.dispatch(ProviderDegradedEvent{errorMsg});
                            self->lifecycleFsm_.setSubsystemDegraded("embeddings", true, errorMsg);
                        } catch (...) {
                        }
                        try {
                            if (self->modelProvider_)
                                (void)self->modelProvider_->unloadModel(preferred);
                        } catch (...) {
                        }
                        // Retry once after a short delay to tolerate slow filesystems
                        std::this_thread::sleep_for(std::chrono::milliseconds(500));

                        auto r_retry = self->modelProvider_->loadModel(preferred);
                        if (r_retry) {
                            self->state_.readiness.modelProviderReady.store(
                                true, std::memory_order_relaxed);
                            self->state_.readiness.modelLoadProgress.store(
                                100, std::memory_order_relaxed);
                            spdlog::info("Preferred model '{}' preloaded on retry", preferred);
                            self->clearModelProviderError();
                            try {
                                std::size_t dim = 0;
                                try {
                                    dim = self->getEmbeddingDimension();
                                } catch (...) {
                                }
                                self->embeddingFsm_.dispatch(
                                    ModelLoadedEvent{self->embeddingModelName_, dim});
                                self->lifecycleFsm_.setSubsystemDegraded(
                                    "embeddings", false); // Clear on retry success
                            } catch (...) {
                            }
                        } else {
#ifndef YAMS_USE_ONNX_RUNTIME
                            spdlog::warn("ONNX runtime disabled in this build; model "
                                         "preloading not supported by daemon binary");
#endif
                            spdlog::warn("Preferred model '{}' failed twice: {}", preferred,
                                         r_retry.error().message);
                            try {
                                self->embeddingFsm_.dispatch(
                                    LoadFailureEvent{r_retry.error().message});
                                self->lifecycleFsm_.setSubsystemDegraded("embeddings", true,
                                                                         r_retry.error().message);
                            } catch (...) {
                            }
                        }
                    }
                } catch (const std::exception& e) {
                    spdlog::warn("preloadPreferredModelIfConfigured lambda error: {}", e.what());
                } catch (...) {
                    spdlog::warn("preloadPreferredModelIfConfigured lambda: unknown error");
                }
            });
        } catch (const std::bad_weak_ptr& e) {
            // Fall back to synchronous loading if shared_from_this() is not available
            spdlog::warn("shared_from_this() not available for '{}', falling back to raw pointer "
                         "model load: {}",
                         preferred, e.what());

            // Capture necessary members by value/pointer for the lambda
            auto* provider = modelProvider_.get();
            auto* readiness = &state_.readiness;

            boost::asio::post(executor, [this, provider, readiness, preferred]() {
                try {
                    auto r = provider->loadModel(preferred);
                    if (r) {
                        readiness->modelProviderReady.store(true, std::memory_order_relaxed);
                        readiness->modelLoadProgress.store(100, std::memory_order_relaxed);
                        spdlog::info("Preferred model '{}' preloaded (fallback)", preferred);
                        try {
                            std::size_t dim = 0;
                            try {
                                dim = this->getEmbeddingDimension();
                            } catch (...) {
                            }
                            this->embeddingFsm_.dispatch(
                                ModelLoadedEvent{this->embeddingModelName_, dim});
                        } catch (...) {
                        }
                    } else {
#ifndef YAMS_USE_ONNX_RUNTIME
                        spdlog::warn("ONNX runtime disabled in this build; model preloading not "
                                     "supported by daemon binary");
#endif
                        spdlog::warn("Preferred model '{}' failed (fallback): {}", preferred,
                                     r.error().message);
                    }
                } catch (const std::exception& e) {
                    spdlog::warn("Model preload task (fallback) threw: {}", e.what());
                } catch (...) {
                    spdlog::warn("Model preload task (fallback) threw unknown exception");
                }
            });
        }
    } catch (const std::exception& e) {
        spdlog::warn("preloadPreferredModelIfConfigured error: {}", e.what());
    } catch (...) {
        spdlog::warn("preloadPreferredModelIfConfigured: unknown error");
    }
}

Result<size_t> ServiceManager::adoptSymbolExtractorsFromHosts() {
    try {
        size_t adopted = adoptPluginInterface<yams_symbol_extractor_v1, AbiSymbolExtractorAdapter,
                                              AbiSymbolExtractorAdapter>(
            abiHost_.get(), YAMS_IFACE_SYMBOL_EXTRACTOR_V1, YAMS_IFACE_SYMBOL_EXTRACTOR_V1_VERSION,
            symbolExtractors_, [](const yams_symbol_extractor_v1* table) {
                return table->abi_version == YAMS_IFACE_SYMBOL_EXTRACTOR_V1_VERSION;
            });
        return Result<size_t>(adopted);
    } catch (const std::exception& e) {
        return Error{ErrorCode::Unknown, e.what()};
    }
}

boost::asio::awaitable<void> ServiceManager::co_enableEmbeddingsAndRebuild() {
    // FSM-based guard: if embedding already loading or ready, skip duplicate init
    if (embeddingFsm_.isLoadingOrReady()) {
        spdlog::debug("[Rebuild] skip: embedding already loading or ready (FSM)");
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
            auto graphService = graphComponent_ ? graphComponent_->getQueryService() : nullptr;
            auto rebuildResult = co_await searchEngineManager_.buildEngine(
                metadataRepo_, vectorIndexManager_, embGen, graphService, "rebuild", build_timeout,
                getWorkerExecutor());

            if (rebuildResult.has_value()) {
                const auto& rebuilt = rebuildResult.value();
                {
                    std::lock_guard<std::shared_mutex> lk(searchEngineMutex_); // Exclusive write
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

std::shared_ptr<search::HybridSearchEngine> ServiceManager::getSearchEngineSnapshot() const {
    std::shared_lock lock(searchEngineMutex_); // Concurrent reads - no blocking!
    return searchEngine_;
}

yams::app::services::AppContext ServiceManager::getAppContext() const {
    app::services::AppContext ctx;
    ctx.service_manager = const_cast<ServiceManager*>(this);
    ctx.store = contentStore_;
    ctx.searchExecutor = std::atomic_load(&searchExecutor_);
    ctx.metadataRepo = metadataRepo_;
    ctx.hybridEngine = getSearchEngineSnapshot();
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
    // With the new architecture using io_context, we don't have a direct way to get queue depth
    // Return 0 for now
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

ServiceManager::SearchLoadMetrics ServiceManager::getSearchLoadMetrics() const {
    SearchLoadMetrics metrics;
    auto exec = std::atomic_load(&searchExecutor_);
    if (!exec)
        return metrics;
    auto load = exec->getLoadMetrics();
    metrics.active = load.active;
    metrics.queued = load.queued;
    metrics.executed = load.executed;
    metrics.avgLatencyUs = load.avgLatencyUs;
    metrics.concurrencyLimit = load.concurrencyLimit;
    const auto cacheTotal = load.cacheHits + load.cacheMisses;
    if (cacheTotal > 0) {
        metrics.cacheHitRate =
            static_cast<double>(load.cacheHits) / static_cast<double>(cacheTotal);
    }
    return metrics;
}

bool ServiceManager::applySearchConcurrencyTarget(std::size_t target) {
    auto exec = std::atomic_load(&searchExecutor_);
    if (!exec)
        return false;
    try {
        exec->setConcurrencyLimit(static_cast<std::uint32_t>(target));
        return true;
    } catch (...) {
        return false;
    }
}

// (Namespace yams::daemon remains open for subsequent member definitions)

bool ServiceManager::detectEmbeddingPreloadFlag() const {
    bool flag = false;

    // Config file precedence
    std::filesystem::path cfgPath = config_.configFilePath;
    if (cfgPath.empty())
        cfgPath = resolveDefaultConfigPath();
    if (!cfgPath.empty()) {
        try {
            auto kv = parseSimpleTomlFlat(cfgPath);
            auto it = kv.find("embeddings.preload_on_startup");
            if (it != kv.end()) {
                std::string lower = it->second;
                std::transform(lower.begin(), lower.end(), lower.begin(),
                               [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
                flag = (lower == "1" || lower == "true" || lower == "yes" || lower == "on");
            }
        } catch (const std::exception& e) {
            spdlog::debug("[Warmup] failed to read config for preload flag: {}", e.what());
        }
    }

    // Environment override wins
    if (const char* env = std::getenv("YAMS_EMBED_PRELOAD_ON_STARTUP")) {
        flag = env_truthy(env);
    }

    return flag;
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
            // Fast path: flat TOML for explicit key
            try {
                auto kv = parseSimpleTomlFlat(cfgPath);
                auto it = kv.find("embeddings.preferred_model");
                if (it != kv.end() && !it->second.empty()) {
                    preferred = it->second;
                    spdlog::debug("Preferred model from config: {}", preferred);
                    return preferred;
                }
            } catch (...) {
            }
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
        if (!resolvedDataDir_.empty()) {
            namespace fs = std::filesystem;
            fs::path models = resolvedDataDir_ / "models";
            std::error_code ec;
            if (fs::exists(models, ec) && fs::is_directory(models, ec)) {
                [[maybe_unused]] size_t dbDim = 0;
                try {
                    // Prefer sentinel dim when available
                    if (auto s = read_vector_sentinel_dim(getResolvedDataDir()))
                        dbDim = *s;
                } catch (...) {
                }
                std::vector<std::string> preferences;

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
        contentStore_ = std::shared_ptr<yams::api::IContentStore>(std::move(uniqueStore));
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

boost::asio::awaitable<std::shared_ptr<yams::search::HybridSearchEngine>>
ServiceManager::co_buildEngine(int timeout_ms, const boost::asio::cancellation_state& /*token*/,
                               bool includeEmbeddingGenerator) {
    auto exec = getWorkerExecutor();
    std::shared_ptr<yams::vector::EmbeddingGenerator> gen;
    if (includeEmbeddingGenerator && modelProvider_ && modelProvider_->isAvailable() &&
        !embeddingModelName_.empty()) {
        try {
            gen = modelProvider_->getEmbeddingGenerator(embeddingModelName_);
        } catch (...) {
        }
    }
    auto graphService = graphComponent_ ? graphComponent_->getQueryService() : nullptr;
    auto res = co_await searchEngineManager_.buildEngine(
        metadataRepo_, vectorIndexManager_, gen, graphService, "co_buildEngine", timeout_ms, exec);
    if (res.has_value()) {
        co_return res.value();
    }
    co_return std::shared_ptr<yams::search::HybridSearchEngine>{};
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

        // Create search executor
        std::atomic_store(&searchExecutor_,
                          std::make_shared<yams::search::SearchExecutor>(database_, metadataRepo_));

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

        auto vectorDb = std::make_shared<yams::vector::VectorDatabase>(cfg);
        auto initRes = vectorDb->initialize();
        if (!initRes) {
            co_return yams::Result<void>(Error{"Failed to initialize vector database"});
        }

        vectorDatabase_ = vectorDb;
        state_.readiness.vectorDbReady.store(true);

        spdlog::info("[ServiceManager::co_initVectorSystem] Vector system initialized");
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
            state_.readiness.pluginsReady.store(true);
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

bool ServiceManager::resizePostIngestThreads(std::size_t target) {
    // PostIngestQueue now uses strand, no dynamic thread pool resizing
    (void)target;
    return false;
}

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
        if (degraded) {
            embeddingFsm_.dispatch(
                ProviderDegradedEvent{error.empty() ? std::string{"test"} : error});
        } else {
            embeddingFsm_.dispatch(ModelLoadedEvent{embeddingModelName_, 0});
        }
    } catch (...) {
    }
}

} // namespace yams::daemon
