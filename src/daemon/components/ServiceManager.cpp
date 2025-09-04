#include <spdlog/spdlog.h>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <thread>
#include <yams/api/content_store_builder.h>
#include <yams/daemon/components/ServiceManager.h>
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
        } else if (const char* homeEnv = std::getenv("HOME")) {
            dataDir = fs::path(homeEnv) / ".local" / "share" / "yams";
        } else {
            dataDir = fs::path(".") / "yams_data";
        }
    }
    std::error_code ec;
    fs::create_directories(dataDir, ec);
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

    // Start background resource initialization
    initThread_ = std::jthread([this](std::stop_token token) {
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

    // Shutdown model provider
    if (modelProvider_) {
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

Result<void> ServiceManager::initializeAsync(std::stop_token token) {
    spdlog::debug("ServiceManager: Initializing daemon resources");

    // Load plugins BEFORE creating model provider
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

            if (pluginsLoaded > 0) {
                spdlog::info("Successfully loaded {} plugin(s)", pluginsLoaded);
                state_.readiness.pluginsReady = true;
            }
        } catch (const std::exception& e) {
            spdlog::warn("Plugin loading failed: {}", e.what());
        }
    }

    if (token.stop_requested())
        return Error{ErrorCode::OperationCancelled, "Shutdown requested"};

    // Initialize model provider based on configuration
    if (config_.enableModelProvider) {
        try {
            modelProvider_ = createModelProvider(config_.modelPoolConfig);
            if (modelProvider_) {
                spdlog::info("Initialized {} model provider", modelProvider_->getProviderName());
                state_.readiness.modelProviderReady = true;
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
            } else if (const char* homeEnv = std::getenv("HOME")) {
                dataDir = fs::path(homeEnv) / ".local" / "share" / "yams";
            } else {
                dataDir = fs::path(".") / "yams_data";
            }
        }
        std::error_code ec;
        fs::create_directories(dataDir, ec);

        auto storeRes = yams::api::ContentStoreBuilder::createDefault(dataDir / "storage");
        if (storeRes) {
            // Convert unique_ptr to shared_ptr
            auto& uniqueStore =
                const_cast<std::unique_ptr<yams::api::IContentStore>&>(storeRes.value());
            contentStore_ = std::shared_ptr<yams::api::IContentStore>(uniqueStore.release());
            state_.readiness.contentStoreReady = true;
            spdlog::info("Content store initialized successfully");
        }

        if (token.stop_requested())
            return Error{ErrorCode::OperationCancelled, "Shutdown requested"};

        auto dbPath = dataDir / "yams.db";
        database_ = std::make_shared<metadata::Database>();
        if (auto dbRes = database_->open(dbPath.string(), metadata::ConnectionMode::Create);
            !dbRes) {
            return Error{dbRes.error()};
        }
        state_.readiness.databaseReady = true;
        spdlog::info("Database opened successfully");

        metadata::MigrationManager mm(*database_);
        mm.initialize();
        mm.registerMigrations(metadata::YamsMetadataMigrations::getAllMigrations());
        mm.migrate();

        metadata::ConnectionPoolConfig poolCfg;
        // Aggressive defaults for read-heavy workloads
        size_t hw = std::max<size_t>(1, std::thread::hardware_concurrency());
        poolCfg.minConnections = std::min<size_t>(std::max<size_t>(4, hw / 2), 16);
        poolCfg.maxConnections = 64;
        // Env overrides for tuning
        if (const char* envMax = std::getenv("YAMS_DB_POOL_MAX"); envMax && *envMax) {
            try {
                auto v = static_cast<size_t>(std::stoul(envMax));
                if (v >= poolCfg.minConnections)
                    poolCfg.maxConnections = v;
            } catch (...) {
            }
        }
        if (const char* envMin = std::getenv("YAMS_DB_POOL_MIN"); envMin && *envMin) {
            try {
                auto v = static_cast<size_t>(std::stoul(envMin));
                if (v > 0)
                    poolCfg.minConnections = v;
            } catch (...) {
            }
        }
        connectionPool_ = std::make_shared<metadata::ConnectionPool>(dbPath.string(), poolCfg);
        if (auto poolInit = connectionPool_->initialize(); !poolInit) {
            return poolInit.error();
        }
        metadataRepo_ = std::make_shared<metadata::MetadataRepository>(*connectionPool_);
        state_.readiness.metadataRepoReady = true;
        spdlog::info("Metadata repository initialized successfully");

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
                // Check if vectors.db exists and get stats from it
                auto vectorDbPath = config_.dataDir / "vectors.db";
                if (std::filesystem::exists(vectorDbPath)) {
                    state_.readiness.vectorIndexReady = true;
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
            } catch (const std::exception& e) {
                spdlog::warn("Failed to initialize EmbeddingGenerator: {}", e.what());
            }
        }

    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError,
                     std::string("Fatal error during service initialization: ") + e.what()};
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
