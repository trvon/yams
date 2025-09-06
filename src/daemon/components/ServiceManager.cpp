#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <thread>
#include <yams/api/content_store_builder.h>
#include <yams/compat/thread_stop_compat.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/ipc/retrieval_session.h>
#include <yams/daemon/resource/model_provider.h>
#include <yams/daemon/resource/plugin_loader.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>
#include <yams/search/hybrid_search_engine.h>
#include <yams/search/search_engine_builder.h>
#include <yams/search/search_executor.h>
#include <yams/vector/embedding_generator.h>
#include <yams/vector/vector_index_manager.h>

namespace yams::daemon {

ServiceManager::ServiceManager(const DaemonConfig& config, StateComponent& state)
    : config_(config), state_(state) {}

ServiceManager::~ServiceManager() {
    shutdown();
}

Result<void> ServiceManager::initialize() {
    // Validate data directory synchronously to fail fast if unwritable
    namespace fs = std::filesystem;
    fs::path dataDir = config_.dataDir;
    if (dataDir.empty()) {
        if (const char* storageEnv = std::getenv("YAMS_STORAGE")) {
            dataDir = fs::path(storageEnv);
        } else if (const char* dataEnv = std::getenv("YAMS_DATA_DIR")) {
            dataDir = fs::path(dataEnv);
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

    // Start background resource initialization
    initThread_ = yams::compat::jthread([this](yams::compat::stop_token token) {
        spdlog::info("Starting async resource initialization...");
        auto result = initializeAsync(token);
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

    // Sanity check: if dependencies are ready but searchExecutor_ not initialized
    if (state_.readiness.databaseReady.load() && state_.readiness.metadataRepoReady.load() &&
        !searchExecutor_) {
        spdlog::warn("SearchExecutor not initialized despite database and metadata repo ready");
    }
    return Result<void>();
}

void ServiceManager::shutdown() {
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

    // Shutdown plugins
    if (pluginLoader_) {
        pluginLoader_->unloadAllPlugins();
        pluginLoader_.reset();
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
        j["overall"] = state.readiness.overallStatus();
        nlohmann::json rd;
        rd["ipc_server"] = state.readiness.ipcServerReady.load();
        rd["content_store"] = state.readiness.contentStoreReady.load();
        rd["database"] = state.readiness.databaseReady.load();
        rd["metadata_repo"] = state.readiness.metadataRepoReady.load();
        rd["search_engine"] = state.readiness.searchEngineReady.load();
        rd["model_provider"] = state.readiness.modelProviderReady.load();
        rd["vector_index"] = state.readiness.vectorIndexReady.load();
        rd["plugins"] = state.readiness.pluginsReady.load();
        j["readiness"] = rd;
        nlohmann::json pr;
        pr["search_engine"] = state.readiness.searchProgress.load();
        pr["vector_index"] = state.readiness.vectorIndexProgress.load();
        pr["model_provider"] = state.readiness.modelLoadProgress.load();
        j["progress"] = pr;
        // Include per-component init durations when available
        if (!state.initDurationsMs.empty()) {
            nlohmann::json dur;
            for (const auto& [k, v] : state.initDurationsMs) {
                dur[k] = v;
            }
            j["durations_ms"] = dur;
            // Also compute top 3 slowest that have finished
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

Result<void> ServiceManager::initializeAsync(yams::compat::stop_token token) {
    spdlog::debug("ServiceManager: Initializing daemon resources");
    writeBootstrapStatusFile(config_, state_);

    // Load plugins BEFORE creating model provider
    // Readiness semantics: the plugin subsystem is considered "ready" once the loader has
    // completed its scan/initialization, even if zero plugins are present. This avoids
    // blocking overall daemon readiness on optional components.
    if (config_.autoLoadPlugins) {
        try {
            pluginLoader_ = std::make_unique<PluginLoader>();

            size_t pluginsLoaded = 0;
            if (!config_.pluginDir.empty()) {
                spdlog::info("Loading plugins from configured directory: {}",
                             config_.pluginDir.string());
                auto result = pluginLoader_->loadPluginsFromDirectory(config_.pluginDir);
                if (result) {
                    pluginsLoaded = result.value();
                }
            } else {
                spdlog::info("Auto-loading plugins from default directories");
                auto result = pluginLoader_->autoLoadPlugins();
                if (result) {
                    pluginsLoaded = result.value();
                }
            }

            spdlog::info("Plugin subsystem initialized ({} plugin(s) loaded)", pluginsLoaded);
            state_.readiness.pluginsReady = true; // subsystem ready even if none loaded
            // Record init duration for plugins
            try {
                auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - state_.stats.startTime)
                              .count();
                state_.initDurationsMs.emplace("plugins", static_cast<uint64_t>(ms));
            } catch (...) {
            }
            writeBootstrapStatusFile(config_, state_);
        } catch (const std::exception& e) {
            spdlog::warn("Plugin loading failed: {}", e.what());
            // Mark plugin subsystem as ready (no plugins available) to avoid readiness deadlock
            state_.readiness.pluginsReady = true;
            writeBootstrapStatusFile(config_, state_);
        }
    } else {
        // Plugins disabled: consider subsystem ready immediately
        state_.readiness.pluginsReady = true;
        writeBootstrapStatusFile(config_, state_);
    }

    if (token.stop_requested())
        return Error{ErrorCode::OperationCancelled, "Shutdown requested"};

    ModelPoolConfig poolCfg = config_.modelPoolConfig;
    // Track embeddings flags from config.toml
    bool embeddingsPreloadOnStartup = true; // default: allow preload unless disabled
    bool embeddingsKeepHot = true;          // default: keep model hot (non-lazy)

    // Read embeddings settings (preferred model, models root) from config.toml and propagate into
    // model pool config
    {
        std::filesystem::path cfgPath;
        const char* xdgConfigHome = std::getenv("XDG_CONFIG_HOME");
        const char* homeEnv = std::getenv("HOME");
        if (xdgConfigHome) {
            cfgPath = std::filesystem::path(xdgConfigHome) / "yams" / "config.toml";
        } else if (homeEnv) {
            cfgPath = std::filesystem::path(homeEnv) / ".config" / "yams" / "config.toml";
        }

        if (!cfgPath.empty() && std::filesystem::exists(cfgPath)) {
            try {
                std::ifstream file(cfgPath);
                if (file) {
                    std::string line;
                    std::string currentSection;
                    std::string prefModel;
                    std::string modelsRoot;

                    auto trim = [](std::string& s) {
                        if (s.empty())
                            return;
                        s.erase(0, s.find_first_not_of(" \t"));
                        auto pos = s.find_last_not_of(" \t");
                        if (pos != std::string::npos)
                            s.erase(pos + 1);
                    };

                    while (std::getline(file, line)) {
                        if (line.empty() || line[0] == '#')
                            continue;

                        if (!line.empty() && line[0] == '[') {
                            size_t end = line.find(']');
                            if (end != std::string::npos) {
                                currentSection = line.substr(1, end - 1);
                            } else {
                                currentSection.clear();
                            }
                            continue;
                        }

                        size_t eq = line.find('=');
                        if (eq == std::string::npos)
                            continue;

                        std::string key = line.substr(0, eq);
                        std::string value = line.substr(eq + 1);
                        trim(key);
                        trim(value);

                        // Strip inline comments
                        size_t hashPos = value.find('#');
                        if (hashPos != std::string::npos) {
                            value = value.substr(0, hashPos);
                            trim(value);
                        }

                        // Remove quotes if present
                        if (value.size() >= 2 && value.front() == '"' && value.back() == '"') {
                            value = value.substr(1, value.size() - 2);
                        }

                        if (currentSection == "embeddings") {
                            if (key == "preferred_model" && prefModel.empty()) {
                                prefModel = value;
                            } else if (key == "model_path" && modelsRoot.empty()) {
                                modelsRoot = value;
                            } else if (key == "keep_model_hot") {
                                std::string v = value;
                                for (auto& c : v)
                                    c = static_cast<char>(std::tolower(c));
                                embeddingsKeepHot =
                                    !(v == "false" || v == "0" || v == "no" || v == "off");
                            } else if (key == "preload_on_startup") {
                                std::string v = value;
                                for (auto& c : v)
                                    c = static_cast<char>(std::tolower(c));
                                embeddingsPreloadOnStartup =
                                    !(v == "false" || v == "0" || v == "no" || v == "off");
                            }
                        }
                    }

                    if (!modelsRoot.empty()) {
                        poolCfg.modelsRoot = modelsRoot;
                        spdlog::info("Embeddings models root set from config: {}", modelsRoot);
                    }

                    if (!prefModel.empty()) {
                        auto& preloads = poolCfg.preloadModels;
                        bool found = false;
                        for (auto it = preloads.begin(); it != preloads.end(); ++it) {
                            if (*it == prefModel) {
                                if (it != preloads.begin()) {
                                    preloads.erase(it);
                                    preloads.insert(preloads.begin(), prefModel);
                                }
                                found = true;
                                break;
                            }
                        }
                        if (!found) {
                            preloads.insert(preloads.begin(), prefModel);
                        }
                        spdlog::info("Preferred embedding model set from config: {}", prefModel);
                    }
                }
            } catch (const std::exception& e) {
                spdlog::warn("Failed to read embeddings config from {}: {}", cfgPath.string(),
                             e.what());
            }
        }
    }

    // Map embeddings.keep_model_hot -> pool lazy loading
    // keep_model_hot=false => lazyLoading=true; keep_model_hot=true => lazyLoading=false
    poolCfg.lazyLoading = !embeddingsKeepHot;
    // Startup banner for embedding-related decisions
    try {
        spdlog::info("Embeddings startup: keep_model_hot={}, preload_on_startup={}, lazyLoading={}",
                     embeddingsKeepHot ? "true" : "false",
                     embeddingsPreloadOnStartup ? "true" : "false",
                     poolCfg.lazyLoading ? "true" : "false");
    } catch (...) {
    }

    // Initialize model provider based on configuration
    if (config_.enableModelProvider) {
        try {
            modelProvider_ = createModelProvider(poolCfg);
            if (modelProvider_) {
                spdlog::info("Initialized {} model provider", modelProvider_->getProviderName());
                state_.readiness.modelProviderReady = true;
                try {
                    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                  std::chrono::steady_clock::now() - state_.stats.startTime)
                                  .count();
                    state_.initDurationsMs.emplace("model_provider", static_cast<uint64_t>(ms));
                } catch (...) {
                }
                // Attempt preferred model preload to avoid "0 models loaded" UX
                try {
                    const char* disable = std::getenv("YAMS_DISABLE_MODEL_PRELOAD");
                    bool skipPreload = false;
                    if (disable && *disable) {
                        std::string v(disable);
                        for (auto& c : v)
                            c = static_cast<char>(std::tolower(c));
                        skipPreload = (v == "1" || v == "true" || v == "yes" || v == "on");
                    }
                    // Also respect pool configuration for lazy loading (from keep_model_hot)
                    if (poolCfg.lazyLoading) {
                        spdlog::info(
                            "Skipping model preload due to lazyLoading=true in daemon.models");
                        skipPreload = true;
                    }
                    // Respect embeddings.keep_model_hot=false (implies lazy)
                    if (!embeddingsKeepHot) {
                        spdlog::info(
                            "Skipping model preload due to embeddings.keep_model_hot=false");
                        skipPreload = true;
                    }
                    // Respect embeddings.preload_on_startup=false
                    if (!embeddingsPreloadOnStartup) {
                        spdlog::info(
                            "Skipping model preload due to embeddings.preload_on_startup=false");
                        skipPreload = true;
                    }
                    spdlog::info("Embeddings preload decision: {}",
                                 skipPreload ? "skipped" : "attempted");
                    if (!skipPreload) {
                        auto loaded = modelProvider_->getLoadedModels();
                        if (loaded.empty()) {
                            std::string preferred;
                            if (!poolCfg.preloadModels.empty()) {
                                preferred = poolCfg.preloadModels.front();
                            }
                            if (preferred.empty())
                                preferred = "all-MiniLM-L6-v2";
                            spdlog::info("Preloading preferred embedding model: {}", preferred);
                            auto lr = modelProvider_->loadModel(preferred);
                            if (!lr) {
                                spdlog::warn("Preferred model preload failed: {}",
                                             lr.error().message);
                            }
                        }
                    } else {
                        spdlog::info("Skipping model preload");
                    }
                } catch (const std::exception& e) {
                    spdlog::warn("Model preload attempt failed: {}", e.what());
                }
            }
        } catch (const std::exception& e) {
            spdlog::warn("Model provider initialization failed: {} - features disabled", e.what());
        }
    }

    if (token.stop_requested())
        return Error{ErrorCode::OperationCancelled, "Shutdown requested"};

    // Initialize database/storage and search primitives
    try {
        namespace fs = std::filesystem;
        fs::path dataDir = config_.dataDir;
        if (dataDir.empty()) {
            if (const char* storageEnv = std::getenv("YAMS_STORAGE")) {
                dataDir = fs::path(storageEnv);
            } else if (const char* dataEnv = std::getenv("YAMS_DATA_DIR")) {
                dataDir = fs::path(dataEnv);
            } else if (const char* homeEnv = std::getenv("HOME")) {
                dataDir = fs::path(homeEnv) / ".local" / "share" / "yams";
            } else {
                dataDir = fs::path(".") / "yams_data";
            }
        }
        std::error_code ec;
        fs::create_directories(dataDir, ec);
        spdlog::info("ServiceManager[async]: using data directory: {}", dataDir.string());

        // Persist again here in case initialize() was bypassed in some flows
        resolvedDataDir_ = dataDir;

        auto storeRoot = dataDir / "storage";
        spdlog::info("ContentStore root: {}", storeRoot.string());
        auto storeRes = yams::api::ContentStoreBuilder::createDefault(storeRoot);
        if (storeRes) {
            // Convert unique_ptr to shared_ptr
            auto& uniqueStore =
                const_cast<std::unique_ptr<yams::api::IContentStore>&>(storeRes.value());
            contentStore_ = std::shared_ptr<yams::api::IContentStore>(uniqueStore.release());
            state_.readiness.contentStoreReady = true;
            try {
                auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - state_.stats.startTime)
                              .count();
                state_.initDurationsMs.emplace("content_store", static_cast<uint64_t>(ms));
            } catch (...) {
            }
            spdlog::info("Content store initialized successfully");
            writeBootstrapStatusFile(config_, state_);
        }

        if (token.stop_requested())
            return Error{ErrorCode::OperationCancelled, "Shutdown requested"};

        auto dbPath = dataDir / "yams.db";
        spdlog::info("Opening metadata database: {}", dbPath.string());
        database_ = std::make_shared<metadata::Database>();
        if (auto dbRes = database_->open(dbPath.string(), metadata::ConnectionMode::Create);
            !dbRes) {
            return Error{dbRes.error()};
        }
        state_.readiness.databaseReady = true;
        try {
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::steady_clock::now() - state_.stats.startTime)
                          .count();
            state_.initDurationsMs.emplace("database", static_cast<uint64_t>(ms));
        } catch (...) {
        }
        spdlog::info("Database opened successfully");
        writeBootstrapStatusFile(config_, state_);

        metadata::MigrationManager mm(*database_);
        mm.initialize();
        mm.registerMigrations(metadata::YamsMetadataMigrations::getAllMigrations());
        mm.migrate();

        metadata::ConnectionPoolConfig dbPoolCfg;
        // Aggressive defaults for read-heavy workloads
        size_t hw = std::max<size_t>(1, std::thread::hardware_concurrency());
        dbPoolCfg.minConnections = std::min<size_t>(std::max<size_t>(4, hw / 2), 16);
        dbPoolCfg.maxConnections = 64;
        // Env overrides for tuning
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
        if (auto poolInit = connectionPool_->initialize(); !poolInit) {
            return poolInit.error();
        }
        metadataRepo_ = std::make_shared<metadata::MetadataRepository>(*connectionPool_);
        state_.readiness.metadataRepoReady = true;
        spdlog::info("Metadata repository initialized successfully");
        writeBootstrapStatusFile(config_, state_);

        // Signal daemon ready as soon as core metadata/search executor infra is available.
        // This avoids lifecycle being gated by optional model provider or vector index extras.
        try {
            if (initCompleteCallback_) {
                initCompleteCallback_(true, "");
                spdlog::info("Lifecycle Ready signal fired (source=metadata_repo)");
                // Clear to avoid duplicate signals later
                initCompleteCallback_ = nullptr;
            }
        } catch (...) {
            // best-effort
        }

        searchExecutor_ = std::make_shared<search::SearchExecutor>(database_, metadataRepo_);
        retrievalSessions_ = std::make_unique<RetrievalSessionManager>();

        // Initialize Vector Database and Index Manager
        // Note: VectorIndexManager is for in-memory indexes, but we need VectorDatabase for
        // persistent storage For now, just create a basic VectorIndexManager for compatibility
        try {
            vector::IndexConfig indexConfig;
            indexConfig.dimension = 768; // Standard dimension for most embedding models
            indexConfig.type = vector::IndexType::FLAT; // Simple flat index for now

            vectorIndexManager_ = std::make_shared<vector::VectorIndexManager>(indexConfig);
            if (auto initRes = vectorIndexManager_->initialize(); !initRes) {
                spdlog::warn("Failed to initialize VectorIndexManager: {}",
                             initRes.error().message);
                // Don't fail - vector search is optional
                vectorIndexManager_.reset();
            } else {
                // Vector index subsystem is ready once the manager is initialized,
                // regardless of whether a persisted index exists yet.
                state_.readiness.vectorIndexReady = true;
                try {
                    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                  std::chrono::steady_clock::now() - state_.stats.startTime)
                                  .count();
                    state_.initDurationsMs.emplace("vector_index", static_cast<uint64_t>(ms));
                } catch (...) {
                }
                writeBootstrapStatusFile(config_, state_);

                // Check if vectors.db exists and log informationally
                auto vectorDbPath = dataDir / "vectors.db";
                if (std::filesystem::exists(vectorDbPath)) {
                    // TODO: Get actual count from vectors.db
                    spdlog::info("VectorIndexManager initialized (vectors.db found at {})",
                                 vectorDbPath.string());
                } else {
                    spdlog::info("VectorIndexManager initialized (no vectors.db yet)");
                }
            }
        } catch (const std::exception& e) {
            spdlog::warn("Exception initializing VectorIndexManager: {}", e.what());
            // Continue without vector support
        }

        // Initialize Embedding Generator if model provider is available
        if (modelProvider_) {
            try {
                embeddingGenerator_ = std::make_shared<vector::EmbeddingGenerator>();
                spdlog::info("EmbeddingGenerator initialized with model provider");
                state_.readiness.modelProviderReady = true;
                try {
                    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                  std::chrono::steady_clock::now() - state_.stats.startTime)
                                  .count();
                    state_.initDurationsMs.emplace("model_provider", static_cast<uint64_t>(ms));
                } catch (...) {
                }
                writeBootstrapStatusFile(config_, state_);
            } catch (const std::exception& e) {
                spdlog::warn("Failed to initialize EmbeddingGenerator: {}", e.what());
            }
        }

        // Build and publish HybridSearchEngine when dependencies are available
        try {
            state_.readiness.searchProgress = 10; // starting
            if (metadataRepo_) {
                state_.readiness.searchProgress = 40; // repo ready
            }
            if (vectorIndexManager_) {
                state_.readiness.searchProgress = 70; // vector index ready (or optional)
            }

            // Compose builder (KG store optional; embedding generator optional)
            searchBuilder_ = std::make_shared<search::SearchEngineBuilder>();
            searchBuilder_->withMetadataRepo(metadataRepo_);
            if (vectorIndexManager_) {
                searchBuilder_->withVectorIndex(vectorIndexManager_);
            }
            if (embeddingGenerator_) {
                searchBuilder_->withEmbeddingGenerator(embeddingGenerator_);
            }

            auto opts = search::SearchEngineBuilder::BuildOptions::makeDefault();
            auto engRes = searchBuilder_->buildEmbedded(opts);
            if (engRes) {
                {
                    std::lock_guard<std::mutex> lk(searchEngineMutex_);
                    searchEngine_ = engRes.value();
                }
                state_.readiness.searchEngineReady = true;
                state_.readiness.searchProgress = 100;
                try {
                    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                  std::chrono::steady_clock::now() - state_.stats.startTime)
                                  .count();
                    state_.initDurationsMs.emplace("search_engine", static_cast<uint64_t>(ms));
                } catch (...) {
                }
                spdlog::info("HybridSearchEngine initialized and published to AppContext");
                writeBootstrapStatusFile(config_, state_);

                // Early-signal daemon readiness once core search is available.
                // Optional subsystems (models, vector index persistence) continue initializing.
                try {
                    if (initCompleteCallback_) {
                        initCompleteCallback_(true, "");
                        spdlog::info("Lifecycle Ready signal fired (source=search_engine)");
                        // Clear to avoid duplicate signals
                        initCompleteCallback_ = nullptr;
                    }
                } catch (...) {
                    // best-effort
                }
            } else {
                spdlog::warn("HybridSearchEngine build failed: {}", engRes.error().message);
                // Leave searchEngineReady=false; service layer will fallback to metadata search
            }
        } catch (const std::exception& e) {
            spdlog::warn("Exception wiring HybridSearchEngine: {}", e.what());
        }

    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError,
                     std::string("Fatal error during service initialization: ") + e.what()};
    }

    // Initialize HybridSearchEngine so daemon services and MCP can use hybrid search
    try {
        if (metadataRepo_ && vectorIndexManager_) {
            search::SearchEngineBuilder builder;
            builder.withVectorIndex(vectorIndexManager_).withMetadataRepo(metadataRepo_);
            if (embeddingGenerator_) {
                builder.withEmbeddingGenerator(embeddingGenerator_);
            }

            auto opts = search::SearchEngineBuilder::BuildOptions::makeDefault();
            auto engRes = builder.buildEmbedded(opts);
            if (engRes) {
                std::lock_guard<std::mutex> lk(searchEngineMutex_);
                searchEngine_ = engRes.value();
                spdlog::info("HybridSearchEngine initialized (embedded, KG {}abled)",
                             searchEngine_->getConfig().enable_kg ? "en" : "dis");
            } else {
                spdlog::warn("HybridSearchEngine build failed: {}", engRes.error().message);
            }
        } else {
            spdlog::warn(
                "HybridSearchEngine not initialized: missing vector index or metadata repo");
        }
    } catch (const std::exception& e) {
        spdlog::warn("HybridSearchEngine bring-up error (ignored): {}", e.what());
    }

    // Watchdog: if core infra is ready but FSM did not flip, promote once after a short delay
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

    return Result<void>();
}

std::shared_ptr<search::HybridSearchEngine> ServiceManager::getSearchEngineSnapshot() const {
    std::lock_guard<std::mutex> lock(searchEngineMutex_);
    return searchEngine_;
}

app::services::AppContext ServiceManager::getAppContext() const {
    app::services::AppContext ctx;
    ctx.store = contentStore_;
    ctx.searchExecutor = searchExecutor_;
    ctx.metadataRepo = metadataRepo_;
    ctx.hybridEngine = getSearchEngineSnapshot();
    return ctx;
}

} // namespace yams::daemon
