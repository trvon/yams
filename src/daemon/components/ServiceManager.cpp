#include <sqlite3.h>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <future>
#include <thread>
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
#include <yams/daemon/components/PoolManager.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/WorkerPool.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/ipc/retrieval_session.h>
#include <yams/daemon/resource/abi_content_extractor_adapter.h>
#include <yams/daemon/resource/abi_model_provider_adapter.h>
#include <yams/daemon/resource/model_provider.h>
#include <yams/daemon/resource/plugin_host.h>
#include <yams/daemon/resource/plugin_loader.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>
#include <yams/plugins/content_extractor_v1.h>
#include <yams/plugins/model_provider_v1.h>
#include <yams/repair/embedding_repair_util.h>
#include <yams/search/hybrid_search_engine.h>
#include <yams/search/search_engine_builder.h>
#include <yams/search/search_executor.h>
#include <yams/vector/embedding_generator.h>
#include <yams/vector/vector_database.h>
#include <yams/vector/vector_index_manager.h>

namespace yams::daemon {

namespace {
using nlohmann::json;

static std::filesystem::path sentinel_path_for(const std::filesystem::path& dataDir) {
    return dataDir / "vectors_sentinel.json";
}

static void write_vector_sentinel(const std::filesystem::path& dataDir, std::size_t embedding_dim,
                                  const std::string& schema = "vec0",
                                  std::uint32_t schema_version = 1) {
    try {
        json j;
        j["embedding_dim"] = embedding_dim;
        j["schema"] = schema;
        j["schema_version"] = schema_version;
        j["updated"] = static_cast<std::int64_t>(std::time(nullptr));
        auto p = sentinel_path_for(dataDir);
        std::ofstream out(p);
        out << j.dump(2);
        out.close();
        spdlog::debug("VECTOR_DB_SENTINEL write dim={} schema={} path={}", embedding_dim, schema,
                      p.string());
    } catch (...) {
        // best-effort only
    }
}

static std::optional<std::size_t> read_vector_sentinel_dim(const std::filesystem::path& dataDir) {
    try {
        auto p = sentinel_path_for(dataDir);
        if (!std::filesystem::exists(p))
            return std::nullopt;
        std::ifstream in(p);
        json j;
        in >> j;
        if (j.contains("embedding_dim"))
            return j["embedding_dim"].get<std::size_t>();
    } catch (...) {
    }
    return std::nullopt;
}

// Inspect sqlite_master DDL to infer vec0 embedding dimension, if vectors.db exists
static std::optional<std::size_t> read_db_embedding_dim(const std::filesystem::path& dbPath) {
    sqlite3* db = nullptr;
    if (sqlite3_open(dbPath.string().c_str(), &db) != SQLITE_OK) {
        if (db)
            sqlite3_close(db);
        return std::nullopt;
    }
    const char* sql = "SELECT sql FROM sqlite_master WHERE name='doc_embeddings' LIMIT 1";
    sqlite3_stmt* stmt = nullptr;
    std::optional<std::size_t> out{};
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            const unsigned char* txt = sqlite3_column_text(stmt, 0);
            if (txt) {
                std::string ddl(reinterpret_cast<const char*>(txt));
                auto pos = ddl.find("float[");
                if (pos != std::string::npos) {
                    auto end = ddl.find(']', pos);
                    if (end != std::string::npos && end > pos + 6) {
                        std::string num = ddl.substr(pos + 6, end - (pos + 6));
                        try {
                            out = static_cast<std::size_t>(std::stoul(num));
                        } catch (...) {
                        }
                    }
                }
            }
        }
        sqlite3_finalize(stmt);
    }
    sqlite3_close(db);
    return out;
}
} // end anonymous namespace

ServiceManager::ServiceManager(const DaemonConfig& config, StateComponent& state)
    : config_(config), state_(state) {
    // Initialize WAL metrics provider (may remain unattached until WAL subsystem is wired)
    walMetricsProvider_ = std::make_shared<WalMetricsProvider>();
    // Initialize ABI plugin loader and trust policy file (~/.config/yams/plugins_trust.txt)
    abiPluginLoader_ = std::make_unique<AbiPluginLoader>();
    abiHost_ = std::make_unique<AbiPluginHost>(this);
    try {
        namespace fs = std::filesystem;
        fs::path cfgHome;
        if (const char* xdg = std::getenv("XDG_CONFIG_HOME"))
            cfgHome = fs::path(xdg);
        else if (const char* home = std::getenv("HOME"))
            cfgHome = fs::path(home) / ".config";
        else
            cfgHome = fs::path("~/.config");
        auto trustFile = cfgHome / "yams" / "plugins_trust.txt";
        abiPluginLoader_->setTrustFile(trustFile);
        abiHost_->setTrustFile(trustFile);
        wasmHost_ = std::make_unique<WasmPluginHost>(trustFile);
        // Default-trust system plugin directories so installed plugins auto-load
        try {
            for (const auto& dir : PluginLoader::getDefaultPluginDirectories()) {
                auto trustRes = abiHost_->trustAdd(dir);
                if (!trustRes) {
                    spdlog::warn("Failed to auto-trust plugin dir {}: {}", dir.string(),
                                 trustRes.error().message);
                } else {
                    spdlog::debug("Auto-trusted plugin directory: {}", dir.string());
                }

                auto trustRes2 = abiPluginLoader_->trustAdd(dir);
                if (!trustRes2) {
                    spdlog::warn("Failed to auto-trust plugin dir {} for loader: {}", dir.string(),
                                 trustRes2.error().message);
                } else {
                    spdlog::debug("Auto-trusted plugin directory for loader: {}", dir.string());
                }
            }

            // Also trust environment override if set
            if (const char* env = std::getenv("YAMS_PLUGIN_DIR")) {
                std::filesystem::path penv(env);
                if (!penv.empty()) {
                    auto tr1 = abiHost_->trustAdd(penv);
                    if (!tr1) {
                        spdlog::warn("Failed to auto-trust YAMS_PLUGIN_DIR {}: {}", penv.string(),
                                     tr1.error().message);
                    } else {
                        spdlog::debug("Auto-trusted YAMS_PLUGIN_DIR: {}", penv.string());
                    }
                    auto tr2 = abiPluginLoader_->trustAdd(penv);
                    if (!tr2) {
                        spdlog::warn("Failed to auto-trust YAMS_PLUGIN_DIR for loader {}: {}",
                                     penv.string(), tr2.error().message);
                    } else {
                        spdlog::debug("Auto-trusted YAMS_PLUGIN_DIR for loader: {}", penv.string());
                    }
                }
            }
        } catch (const std::exception& e) {
            spdlog::warn("Exception during auto-trust setup: {}", e.what());
        } catch (...) {
            spdlog::warn("Unknown exception during auto-trust setup");
        }
    } catch (const std::exception& e) {
        spdlog::warn("Exception during ServiceManager constructor setup: {}", e.what());
    } catch (...) {
        spdlog::warn("Unknown exception during ServiceManager constructor setup");
    }
}

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
        // Naive ETA estimation per stage using defaults and coarse progress
        // Defaults (seconds) used when no historical durations are available
        auto sec_since_start = std::chrono::duration_cast<std::chrono::seconds>(
                                   std::chrono::steady_clock::now() - state.stats.startTime)
                                   .count();
        // Default expected durations per stage (tunable via env later if needed)
        std::map<std::string, int> expected_s{
            {"plugins", 1},      {"content_store", 2}, {"database", 2},       {"metadata_repo", 2},
            {"vector_index", 3}, {"search_engine", 4}, {"model_provider", 20}};
        nlohmann::json eta;
        auto add_eta = [&](const std::string& key, bool ready, int progress) {
            if (ready)
                return;
            int exp = expected_s.count(key) ? expected_s[key] : 5;
            // Prefer observed durations from previous runs if present
            try {
                if (state.initDurationsMs.count(key)) {
                    int hist = static_cast<int>((state.initDurationsMs.at(key) + 999) / 1000);
                    if (hist > 0)
                        exp = hist;
                }
            } catch (...) {
            }
            // Rough remaining estimate: exp * (1 - p) or exp - elapsed, whichever is more
            // conservative
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

// Removed legacy initializeAsync() — all initialization uses coroutine path.
#if 0
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
                            } else if (key == "auto_on_add") {
                                std::string v = value;
                                for (auto& c : v)
                                    c = static_cast<char>(std::tolower(c));
                                embeddingsAutoOnAdd_ =
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
            // Prefer host-backed model_provider_v1 via AbiPluginHost when trusted plugin is present
            bool hostBacked = false;
            if (abiHost_) {
                try {
                    for (const auto& dir : PluginLoader::getDefaultPluginDirectories()) {
                        auto scan = abiHost_->scanDirectory(dir);
                        if (!scan)
                            continue;
                        for (const auto& d : scan.value()) {
                            bool hasIface = false;
                            for (const auto& id : d.interfaces) {
                                if (id == std::string("model_provider_v1")) {
                                    hasIface = true;
                                    break;
                                }
                            }
                            if (!hasIface)
                                continue;
                            // Attempt to load; trust policy enforced inside load()
                            auto lr = abiHost_->load(d.path, "");
                            if (!lr)
                                continue;
                            auto ifaceRes = abiHost_->getInterface(d.name, "model_provider_v1", 1);
                            if (!ifaceRes)
                                continue;
                            auto* table =
                                reinterpret_cast<yams_model_provider_v1*>(ifaceRes.value());
                            if (!table ||
                                table->abi_version != YAMS_IFACE_MODEL_PROVIDER_V1_VERSION)
                                continue;
                            // Wrap vtable in ABI adapter
                            modelProvider_ = std::make_shared<AbiModelProviderAdapter>(table);
                            hostBacked = (modelProvider_ != nullptr);
                            break;
                        }
                        if (hostBacked)
                            break;
                    }
                } catch (...) {
                    // ignore and fallback
                }
            }
            if (!hostBacked) {
                modelProvider_ = createModelProvider(poolCfg);
            }
            if (modelProvider_) {
                spdlog::info("Initialized {} model provider", modelProvider_->getProviderName());

                // Embedding generator will be initialized later after all dependencies are ready
                spdlog::debug("Model provider initialized, embedding generator initialization deferred");
                state_.readiness.modelProviderReady = true;
                try {
                    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                  std::chrono::steady_clock::now() - state_.stats.startTime)
                                  .count();
                    state_.initDurationsMs.emplace("model_provider", static_cast<uint64_t>(ms));
                } catch (...) {
                }
                // Attempt preferred model preload in parallel with the rest of init
                try {
                    const char* disable = std::getenv("YAMS_DISABLE_MODEL_PRELOAD");
                    bool skipPreload = false;
                    if (disable && *disable) {
                        std::string v(disable);
                        for (auto& c : v)
                            c = static_cast<char>(std::tolower(c));
                        skipPreload = (v == "1" || v == "true" || v == "yes" || v == "on");
                    }
                    if (poolCfg.lazyLoading || !embeddingsKeepHot || !embeddingsPreloadOnStartup) {
                        spdlog::info("Skipping model preload (lazy/loading config)");
                        skipPreload = true;
                    }
                    spdlog::info("Embeddings preload decision: {}",
                                 skipPreload ? "skipped" : "attempted");
                    if (!skipPreload) {
                        auto loaded = modelProvider_->getLoadedModels();
                        if (loaded.empty()) {
                            std::string preferred;
                            if (!poolCfg.preloadModels.empty())
                                preferred = poolCfg.preloadModels.front();
                            if (preferred.empty())
                                preferred = "all-MiniLM-L6-v2";
                            spdlog::info("Preloading preferred embedding model (async): {}",
                                         preferred);

                            // Non-blocking preload with provider-driven progress; continue
                            // initializing
                            state_.readiness.modelLoadProgress.store(5, std::memory_order_relaxed);
                            // Wire provider progress callback to readiness progress
                            try {
                                modelProvider_->setProgressCallback(
                                    [this](const ModelLoadEvent& ev) {
                                        int p = 0;
                                        // Map phases to coarse progress if bytes not available
                                        if (ev.bytesTotal > 0) {
                                            p = static_cast<int>((ev.bytesLoaded * 100) /
                                                                 ev.bytesTotal);
                                        } else {
                                            if (ev.phase == "started")
                                                p = 5;
                                            else if (ev.phase == "downloading")
                                                p = 25;
                                            else if (ev.phase == "initializing")
                                                p = 50;
                                            else if (ev.phase == "warming")
                                                p = 80;
                                            else if (ev.phase == "completed")
                                                p = 100;
                                            else if (ev.phase == "error")
                                                p = 0;
                                        }
                                        p = std::clamp(p, 0, 100);
                                        state_.readiness.modelLoadProgress.store(
                                            p, std::memory_order_relaxed);
                                        // Reflect into bootstrap file for early UI consumers
                                        writeBootstrapStatusFile(config_, state_);
                                    });
                            } catch (...) {
                            }

                            // Launch on a detached thread to avoid blocking init path
                            // Capture 'preferred' by value to avoid dangling reference
                            std::thread([this, preferred]() {
                                try {
                                    // Soft timeout for model preload to avoid indefinite blocking
                                    // Default 30s; override via YAMS_MODEL_LOAD_TIMEOUT_MS
                                    int timeout_ms = 30000;
                                    if (const char* env = std::getenv("YAMS_MODEL_LOAD_TIMEOUT_MS")) {
                                        try {
                                            timeout_ms = std::stoi(env);
                                            if (timeout_ms < 1000) timeout_ms = 1000;
                                        } catch (...) {
                                        }
                                    }
                                    auto fut = std::async(std::launch::async, [this, preferred]() {
                                        return modelProvider_->loadModel(preferred);
                                    });
                                    if (fut.wait_for(std::chrono::milliseconds(timeout_ms)) ==
                                        std::future_status::timeout) {
                                        spdlog::warn(
                                            "Preferred model preload timed out after {} ms (model='{}')",
                                            timeout_ms, preferred);
                                        state_.readiness.modelLoadProgress.store(
                                            0, std::memory_order_relaxed);
                                        // Mark provider degraded and try to unload any partial state
                                        modelProviderDegraded_.store(true, std::memory_order_relaxed);
                                        lastModelError_ = "preload timeout";
                                        try { if (modelProvider_) (void)modelProvider_->unloadModel(preferred); } catch (...) {}
                                        return; // Leave provider to complete in background if it can
                                    }
                                    auto lr = fut.get();
                                    if (!lr) {
                                        spdlog::warn("Preferred model preload failed: {}",
                                                     lr.error().message);
                                        state_.readiness.modelLoadProgress.store(
                                            0, std::memory_order_relaxed);
                                        modelProviderDegraded_.store(true, std::memory_order_relaxed);
                                        lastModelError_ = std::string("preload failed: ") + lr.error().message;
                                        try { if (modelProvider_) (void)modelProvider_->unloadModel(preferred); } catch (...) {}
                                    } else {
                                        state_.readiness.modelLoadProgress.store(
                                            100, std::memory_order_relaxed);
                                        spdlog::info("Preferred model '{}' preloaded", preferred);
                                        clearModelProviderError();
                                    }
                                } catch (const std::exception& ex) {
                                    spdlog::warn("Model preload exception: {}", ex.what());
                                    state_.readiness.modelLoadProgress.store(
                                        0, std::memory_order_relaxed);
                                    modelProviderDegraded_.store(true, std::memory_order_relaxed);
                                    lastModelError_ = std::string("preload exception: ") + ex.what();
                                    try { if (modelProvider_) (void)modelProvider_->unloadModel(preferred); } catch (...) {}
                                }
                            }).detach();
                        }
                    } else {
                        state_.readiness.modelLoadProgress.store(0, std::memory_order_relaxed);
                        spdlog::info("Skipping model preload");
                    }
                } catch (const std::exception& e) {
                    spdlog::warn("Model preload attempt failed: {}", e.what());
                    state_.readiness.modelLoadProgress.store(0, std::memory_order_relaxed);
                }
            }
        } catch (const std::exception& e) {
            spdlog::warn("Model provider initialization failed: {} - features disabled", e.what());
        }
    }

    if (token.stop_requested())
        return Error{ErrorCode::OperationCancelled, "Shutdown requested"};

    // Start worker pool for CPU-heavy tasks (embedding/model load)
    try {
        // Resolve worker thread count with precedence: env > config > conservative default
        std::size_t threads = 0;
        if (const char* wtenv = std::getenv("YAMS_WORKER_THREADS"); wtenv && *wtenv) {
            try {
                auto v = static_cast<std::size_t>(std::stoul(wtenv));
                if (v > 0) threads = v;
            } catch (...) {
            }
        }
        if (threads == 0) {
            if (config_.workerThreads > 0) {
                threads = static_cast<std::size_t>(config_.workerThreads);
            } else {
                auto hw = std::thread::hardware_concurrency();
                // Lower floor for idle footprint: quarter of hw, clamped to [1, 4]
                unsigned base = hw > 0 ? (hw / 4u) : 1u;
                base = std::clamp(base, 1u, 4u);
                threads = static_cast<std::size_t>(base);
            }
        }

        spdlog::info("WorkerPool threads resolved: chosen={} (env YAMS_WORKER_THREADS, config={}, hwc={})",
                     threads, config_.workerThreads,
                     std::max(1u, std::thread::hardware_concurrency()));
        workerPool_ = std::make_shared<WorkerPool>(threads);
        poolThreads_ = threads;

        // Phase 6: initialize PoolManager config for IPC (CPU) and IPC IO components and start recon thread
        try {
            auto& pm = PoolManager::instance();
            PoolManager::Config cfg;
            // Lower the floor: keep at least 1 worker; scale up under load
            cfg.min_size = 1;
            // Allow the CPU worker pool to scale well beyond initial threads under backpressure
            cfg.max_size = static_cast<std::uint32_t>(std::min<std::size_t>(std::max<std::size_t>(threads * 4, 8), 64));
            cfg.cooldown_ms = 500;
            cfg.low_watermark = 25;
            cfg.high_watermark = 85;
            pm.configure("ipc", cfg);
            spdlog::info("PoolManager configured for 'ipc': min={}, max={}, cooldown_ms={}",
                         cfg.min_size, cfg.max_size, cfg.cooldown_ms);

            // Configure a separate pool for IPC IO workers; keep conservative limits
            PoolManager::Config ioCfg;
            ioCfg.min_size = 1;
            // Max IO workers: up to 3/4 of hardware threads (fallback 4), at least 2
            {
                unsigned hw = std::max(1u, std::thread::hardware_concurrency());
                unsigned max_io = hw ? std::max(2u, (hw * 3u) / 4u) : 4u;
                // Do not exceed total hardware threads
                if (max_io > hw) max_io = hw;
                ioCfg.max_size = static_cast<std::uint32_t>(max_io);
            }
            ioCfg.cooldown_ms = 500;
            ioCfg.low_watermark = 25;
            ioCfg.high_watermark = 85;
            pm.configure("ipc_io", ioCfg);
            spdlog::info("PoolManager configured for 'ipc_io': min={}, max={}, cooldown_ms={}",
                         ioCfg.min_size, ioCfg.max_size, ioCfg.cooldown_ms);

            poolReconThread_ = yams::compat::jthread([this](yams::compat::stop_token st) {
                using namespace std::chrono_literals;
                while (!st.stop_requested()) {
                    auto stats = PoolManager::instance().stats("ipc");
                    spdlog::trace(
                        "Pool 'ipc': size={} resizes={} rejected_on_cap={} throttled_on_cooldown={}",
                        stats.current_size, stats.resize_events, stats.rejected_on_cap,
                        stats.throttled_on_cooldown);
                    try {
                        if (workerPool_) {
                            // Align WorkerPool threads to PoolManager's current size
                            workerPool_->resize(stats.current_size);
                        }
                    } catch (...) {
                        // best-effort; continue
                    }
                    std::this_thread::sleep_for(1000ms);
                }
            });
        } catch (...) {
            spdlog::debug("PoolManager configuration skipped (not available)");
        }
    } catch (const std::exception& e) {
        spdlog::warn("WorkerPool init failed: {}", e.what());
    }

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
        bool db_ok = false;
        {
            int open_timeout = read_timeout_ms("YAMS_DB_OPEN_TIMEOUT_MS", 5000, 250);
            auto [completed, dbRes] = run_with_deadline(
                [this, dbPath]() { return database_->open(dbPath.string(), metadata::ConnectionMode::Create); },
                std::chrono::milliseconds(open_timeout));
            if (!completed) {
                spdlog::warn("Database open timed out after {} ms; continuing in degraded mode",
                             open_timeout);
            } else if (!dbRes) {
                spdlog::warn("Database open failed: {} — continuing in degraded mode", dbRes.error().message);
            } else {
                db_ok = true;
            }
        }
        if (db_ok) {
            state_.readiness.databaseReady = true;
        }
        try {
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::steady_clock::now() - state_.stats.startTime)
                          .count();
            state_.initDurationsMs.emplace("database", static_cast<uint64_t>(ms));
        } catch (...) {
        }
        if (db_ok) spdlog::info("Database opened successfully");
        writeBootstrapStatusFile(config_, state_);

        if (db_ok) {
            metadata::MigrationManager mm(*database_);
            mm.initialize();
            mm.registerMigrations(metadata::YamsMetadataMigrations::getAllMigrations());
            bool migrated = false;
            {
                int mig_timeout = read_timeout_ms("YAMS_DB_MIGRATE_TIMEOUT_MS", 7000, 250);
                auto [completed, r] = run_with_deadline([&mm]() { return mm.migrate(); },
                                                        std::chrono::milliseconds(mig_timeout));
                if (!completed) {
                    spdlog::warn("Database migration timed out after {} ms; proceeding", mig_timeout);
                } else if (!r) {
                    spdlog::warn("Database migration failed: {} — proceeding", r.error().message);
                } else {
                    migrated = true;
                }
            }
            (void)migrated;
        }

        if (db_ok) {
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
                spdlog::warn("Connection pool init failed: {} — continuing degraded", poolInit.error().message);
            } else {
                metadataRepo_ = std::make_shared<metadata::MetadataRepository>(*connectionPool_);
                state_.readiness.metadataRepoReady = true;
            }
        }
        try {
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::steady_clock::now() - state_.stats.startTime)
                          .count();
            state_.initDurationsMs.emplace("metadata_repo", static_cast<uint64_t>(ms));
        } catch (...) {
        }
        if (state_.readiness.metadataRepoReady.load())
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
                // Schedule plugin autoload now that core is ready
                try {
                    bool enableAutoload = true;
                    if (const char* env = std::getenv("YAMS_AUTOLOAD_PLUGINS")) {
                        std::string v(env);
                        for (auto& c : v)
                            c = static_cast<char>(std::tolower(c));
                        if (v == "0" || v == "false" || v == "off")
                            enableAutoload = false;
                    }
                    if (enableAutoload && !pluginsAutoloadScheduled_.exchange(true)) {
                        std::thread([this]() {
                            std::this_thread::sleep_for(std::chrono::milliseconds(200));
                            try {
                                spdlog::info("Plugin autoload: scanning trusted roots and default directories");
                                std::vector<std::filesystem::path> roots;
                                if (abiHost_) {
                                    for (const auto& p : abiHost_->trustList())
                                        roots.push_back(p);
                                }
                                if (wasmHost_) {
                                    for (const auto& p : wasmHost_->trustList())
                                        roots.push_back(p);
                                }
                                try {
                                    for (const auto& d :
                                         PluginLoader::getDefaultPluginDirectories())
                                        roots.push_back(d);
                                } catch (...) {
                                }
                                try {
                                    if (const char* env = std::getenv("YAMS_PLUGIN_DIR")) {
                                        std::filesystem::path penv(env);
                                        if (!penv.empty())
                                            roots.push_back(penv);
                                    }
                                } catch (...) {
                                }
                                std::sort(roots.begin(), roots.end());
                                roots.erase(std::unique(roots.begin(), roots.end()), roots.end());
                                spdlog::info("Plugin autoload: {} roots to scan", roots.size());
                                size_t loaded_count = 0;
                                for (const auto& r : roots) {
                                    try {
                                        if (abiHost_) {
                                            if (auto sr = abiHost_->scanDirectory(r)) {
                                                for (const auto& d : sr.value())
                                                    if (abiHost_->load(d.path, "")) { ++loaded_count; }
                                            }
                                        }
                                        if (wasmHost_) {
                                            if (auto sr = wasmHost_->scanDirectory(r)) {
                                                for (const auto& d : sr.value())
                                                    if (wasmHost_->load(d.path, "")) { ++loaded_count; }
                                            }
                                        }
                                    } catch (...) {
                                    }
                                }
                                spdlog::info("Plugin autoload: loaded {} plugin(s)", loaded_count);
                                auto mp = adoptModelProviderFromHosts();
                                if (mp && mp.value()) {
                                    spdlog::info("Plugin autoload: model provider adopted");
                                } else {
                                    spdlog::info("Plugin autoload: no model provider adopted");
                                }
                                auto ce = adoptContentExtractorsFromHosts();
                                if (ce) {
                                    spdlog::info("Plugin autoload: adopted {} content extractor(s)", ce.value());
                                }
                            } catch (...) {
                            }
                            writeBootstrapStatusFile(config_, state_);
                        }).detach();
                    }
                } catch (...) {
                }
            }
        } catch (...) {
            // best-effort
        }

        if (database_ && metadataRepo_)
            searchExecutor_ = std::make_shared<search::SearchExecutor>(database_, metadataRepo_);
        retrievalSessions_ = std::make_unique<RetrievalSessionManager>();

        // Initialize Vector Database and Index Manager
        // Note: VectorIndexManager is for in-memory indexes, but we need VectorDatabase for
        // persistent storage For now, just create a basic VectorIndexManager for compatibility
        try {
            // Optional: allow disabling vector database via env for troubleshooting
            bool disableVectors = false;
            if (const char* env = std::getenv("YAMS_DISABLE_VECTORS")) {
                std::string v(env);
                for (auto& c : v) c = static_cast<char>(std::tolower(c));
                disableVectors = (v == "1" || v == "true" || v == "yes" || v == "on");
            }
            if (disableVectors) {
                spdlog::warn("Vector database initialization disabled by YAMS_DISABLE_VECTORS");
            } else {
                // Persistent VectorDatabase (SQLite) initialization
                if (vectorDatabase_) { spdlog::debug("Skipping late VectorDB init (already initialized)"); } else
            // Optional: allow disabling vector database initialization via env (e.g., for CI/CPUs
            // without required instruction sets). Search will operate in keyword/metadata mode.
            const bool disable_vecdb = [](){ if(const char* s=getenv("YAMS_DISABLE_VECTOR_DB")){ std::string v(s); std::transform(v.begin(),v.end(),v.begin(),::tolower); return v=="1"||v=="true"||v=="yes"||v=="on"; } return false; }();
            try {
                if (disable_vecdb) {
                    spdlog::warn("Vector database initialization disabled by YAMS_DISABLE_VECTOR_DB");
                }
                vector::VectorDatabaseConfig vdbCfg;
                vdbCfg.database_path = (dataDir / "vectors.db").string();

                    // Decide create/open based on file existence
                    namespace fs = std::filesystem;
                    fs::path __vdbPath = dataDir / "vectors.db";
                    bool __create = !fs::exists(__vdbPath);
                    vdbCfg.create_if_missing = __create;
                    std::string __vdim_src;
                    size_t vdim = 0;
                    if (__create) {
                        // Resolve vector DB embedding dimension: config > env > generator > model > heuristic
                        try {
                            // Config first
                            fs::path cfgHome;
                            if (const char* xdg = std::getenv("XDG_CONFIG_HOME")) cfgHome = fs::path(xdg);
                            else if (const char* home = std::getenv("HOME")) cfgHome = fs::path(home) / ".config";
                            fs::path cfgPath = cfgHome / "yams" / "config.toml";
                            if (!cfgPath.empty() && fs::exists(cfgPath)) {
                                std::ifstream in(cfgPath);
                                std::string line;
                                auto trim=[&](std::string& t){ if(t.empty()) return; t.erase(0,t.find_first_not_of(" 	")); auto p=t.find_last_not_of(" 	"); if(p!=std::string::npos) t.erase(p+1); };
                                while (std::getline(in, line)) {
                                    std::string l=line; trim(l); if(l.empty()||l[0]=='#') continue;
                                    if (l.find("embeddings.embedding_dim")!=std::string::npos) {
                                        auto eq=l.find('='); if(eq!=std::string::npos){ std::string v=l.substr(eq+1); trim(v); if(!v.empty()&&v.front()=='"'&&v.back()=='"') v=v.substr(1,v.size()-2); try{ vdim=static_cast<size_t>(std::stoul(v)); __vdim_src="config"; }catch(...){} }
                                        if (vdim>0) break;
                                    }
                                }
                            }
                        } catch (...) {}
                        if (vdim==0) { try { if (const char* envd = std::getenv("YAMS_EMBED_DIM")) { vdim = static_cast<size_t>(std::stoul(envd)); __vdim_src="env"; } } catch(...){} }
                        if (vdim==0) { try { if (embeddingGenerator_) { vdim = embeddingGenerator_->getEmbeddingDimension(); if (vdim>0) __vdim_src="generator"; } } catch(...){} }
                        if (vdim==0) { __vdim_src="heuristic"; vdim = 384; }
                        spdlog::info("VECTOR_DB_CREATE dim={} source={} path={}", vdim, __vdim_src, vdbCfg.database_path);
                    } else {
                        spdlog::info("VECTOR_DB_OPEN path={}", vdbCfg.database_path);
                    }

                    vdbCfg.create_if_missing = false; // do not auto-create; doctor handles creation/migration
                    // Resolve vector DB embedding dimension: config > env > generator > heuristic
                    size_t vdim = 0;
                    try {
                        // Config first (embeddings.embedding_dim only)
                        namespace fs = std::filesystem;
                        fs::path cfgHome;
                        if (const char* xdg = std::getenv("XDG_CONFIG_HOME")) cfgHome = fs::path(xdg);
                        else if (const char* home = std::getenv("HOME")) cfgHome = fs::path(home) / ".config";
                        fs::path cfgPath = cfgHome / "yams" / "config.toml";
                        if (!cfgPath.empty() && fs::exists(cfgPath)) {
                            std::ifstream in(cfgPath);
                            std::string line;
                            auto trim=[&](std::string& t){ if(t.empty()) return; t.erase(0,t.find_first_not_of(" \t")); auto p=t.find_last_not_of(" \t"); if(p!=std::string::npos) t.erase(p+1); };
                            while (std::getline(in, line)) {
                                std::string l=line; trim(l); if(l.empty()||l[0]=='#') continue;
                                if (l.find("embeddings.embedding_dim")!=std::string::npos) {
                                    auto eq=l.find('='); if(eq!=std::string::npos){ std::string v=l.substr(eq+1); trim(v); if(!v.empty()&&v.front()=='"'&&v.back()=='"') v=v.substr(1,v.size()-2); try{ vdim=static_cast<size_t>(std::stoul(v)); }catch(...){} }
                                    break;
                                }
                            }
                        }
                    } catch (...) {}
                    // Env next
                    if (vdim == 0) { try { if (const char* envd = std::getenv("YAMS_EMBED_DIM")) vdim = static_cast<size_t>(std::stoul(envd)); } catch (...) {} }
                    // Generator
                    try { if (vdim == 0 && embeddingGenerator_) vdim = embeddingGenerator_->getEmbeddingDimension(); } catch (...) {}
                    // Heuristic fallback
                    if (vdim == 0) vdim = 384;
                    vdbCfg.embedding_dim = vdim;
                    if (!disable_vecdb) {
                        auto vdb = std::make_shared<vector::VectorDatabase>(vdbCfg);
                        if (vdb->initialize()) {
                            vectorDatabase_ = std::move(vdb);
                            spdlog::info("Vector database initialized at {}", vdbCfg.database_path);
                        } else {
                            spdlog::warn("Vector database not ready ({}). Use 'yams doctor --dim <N>' to create or migrate.", vdb->getLastError());
                        }
                    }
                } catch (const std::exception& e) {
                    spdlog::warn("Vector database init exception: {}", e.what());
                }
            }

            vector::IndexConfig indexConfig;
            // Derive index dimension from the same resolved dimension used for Vector DB
            indexConfig.dimension = vdbCfg.embedding_dim;
            indexConfig.type = vector::IndexType::FLAT; // Simple flat index for now

            if (!disable_vecdb) {
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
            } else {
                spdlog::warn("Vector index initialization disabled by YAMS_DISABLE_VECTOR_DB");
            }
        } catch (const std::exception& e) {
            spdlog::warn("Exception initializing VectorIndexManager: {}", e.what());
            // Continue without vector support
        }

        // Initialize Embedding Generator.
        // Prefer model provider; if unavailable, fall back to direct ONNX model discovery
        // Embedding generator initialization is deferred until after plugins are loaded
        // This ensures we can adopt model providers from plugins if available
        spdlog::debug("Deferring embedding generator initialization until plugin loading phase");
        } else {
            // Fallback: try to initialize from ~/.yams/models
            try {
                namespace fs = std::filesystem;
                std::string selected;
                if (const char* home = std::getenv("HOME")) {
                    fs::path models = fs::path(home) / ".yams" / "models";
                    if (fs::exists(models) && fs::is_directory(models)) {
                        for (const auto& entry : fs::directory_iterator(models)) {
                            if (entry.is_directory() && fs::exists(entry.path() / "model.onnx")) {
                                selected = entry.path().filename().string();
                                break;
                            }
                        }
                        if (const char* pref = std::getenv("YAMS_PREFERRED_MODEL")) {
                            std::string p(pref);
                            if (fs::exists(models / p / "model.onnx")) selected = p;
                        }
                    }
                }
                if (!selected.empty()) {
                    vector::EmbeddingConfig ecfg;
                    if (const char* home = std::getenv("HOME")) {
                        ecfg.model_path = (std::filesystem::path(home) / ".yams" / "models" / selected / "model.onnx").string();
                    }
                    ecfg.model_name = selected;
                    if (selected == "all-MiniLM-L6-v2") ecfg.embedding_dim = 384; else ecfg.embedding_dim = 768;
                    auto eg = std::make_shared<vector::EmbeddingGenerator>(ecfg);
                    if (eg->initialize()) {
                        embeddingGenerator_ = eg;
                        spdlog::info("EmbeddingGenerator initialized from local model: {}", selected);

                        // Align VectorIndexManager dimension to generator dimension if needed
                        try {
                            if (vectorIndexManager_) {
                                size_t d = embeddingGenerator_->getEmbeddingDimension();
                                if (d == 0) d = 768;
                                if (vectorIndexManager_->getConfig().dimension != d) {
                                    auto cfg = vectorIndexManager_->getConfig();
                                    cfg.dimension = d;
                                    vectorIndexManager_->setConfig(cfg);
                                    auto rr = vectorIndexManager_->rebuildIndex();
                                    if (!rr) {
                                        spdlog::warn("VectorIndexManager rebuild with dim {} failed: {}", d, rr.error().message);
                                    } else {
                                        spdlog::info("VectorIndexManager dimension aligned to {}", d);
                                    }
                                }
                            }
                        } catch (...) { spdlog::debug("VectorIndexManager align skipped"); }
                        state_.readiness.modelProviderReady = true; // generator available
                        writeBootstrapStatusFile(config_, state_);
                    } else {
                        spdlog::warn("Local model present but EmbeddingGenerator init failed for {}", selected);
                    }
                } else {
                    spdlog::info("No model provider and no local models detected; embeddings disabled");
                }
            } catch (const std::exception& e) {
                spdlog::warn("EmbeddingGenerator local fallback failed: {}", e.what());
            }
        }

        // Build and publish HybridSearchEngine when dependencies are available
        try {
            state_.readiness.searchProgress = 10; // starting
            spdlog::debug("Search init progress: {}%", state_.readiness.searchProgress.load());
            writeBootstrapStatusFile(config_, state_);
            if (metadataRepo_) {
                state_.readiness.searchProgress = 40; // repo ready
                spdlog::debug("Search init progress: {}% (metadata ready)", state_.readiness.searchProgress.load());
                writeBootstrapStatusFile(config_, state_);
            }
            if (vectorIndexManager_) {
                state_.readiness.searchProgress = 70; // vector index ready (or optional)
                spdlog::debug("Search init progress: {}% (vector index ready)", state_.readiness.searchProgress.load());
                writeBootstrapStatusFile(config_, state_);
            }

            // Compose builder (KG store optional; embedding generator optional)
            searchBuilder_ = std::make_shared<search::SearchEngineBuilder>();
            spdlog::debug(
                "Search builder preconditions: metadataRepo={} vectorIndexManager={} embeddingGenerator={}",
                metadataRepo_ ? "yes" : "no", vectorIndexManager_ ? "yes" : "no",
                embeddingGenerator_ ? "yes" : "no");
            searchBuilder_->withMetadataRepo(metadataRepo_);
            if (vectorIndexManager_) {
                searchBuilder_->withVectorIndex(vectorIndexManager_);
            }
            if (embeddingGenerator_) {
                searchBuilder_->withEmbeddingGenerator(embeddingGenerator_);
            }

            auto opts = search::SearchEngineBuilder::BuildOptions::makeDefault();
            // Time-bound engine build to avoid blocking init
            std::shared_ptr<search::HybridSearchEngine> built;
            {
                int build_timeout = read_timeout_ms("YAMS_SEARCH_BUILD_TIMEOUT_MS", 5000, 250);
                auto [completed, engRes] = run_with_deadline(
                    [this, opts]() { return searchBuilder_->buildEmbedded(opts); },
                    std::chrono::milliseconds(build_timeout));
                if (!completed) {
                    spdlog::warn("HybridSearchEngine build timed out after {} ms; continuing degraded",
                                 build_timeout);
                } else if (engRes) {
                    built = engRes.value();
                } else {
                    spdlog::warn("HybridSearchEngine build failed: {}", engRes.error().message);
                }
            }
            if (built) {
                {
                    std::lock_guard<std::mutex> lk(searchEngineMutex_);
                    searchEngine_ = built;
                }
                state_.readiness.searchEngineReady = true;
                state_.readiness.searchProgress = 100;
                spdlog::debug("Search init progress: {}% (engine ready)", state_.readiness.searchProgress.load());
                writeBootstrapStatusFile(config_, state_);
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

                // Schedule plugin autoload after core is ready to avoid blocking startup
                try {
                    bool enableAutoload = true;
                    if (const char* env = std::getenv("YAMS_AUTOLOAD_PLUGINS")) {
                        std::string v(env);
                        for (auto& c : v)
                            c = static_cast<char>(std::tolower(c));
                        if (v == "0" || v == "false" || v == "off")
                            enableAutoload = false;
                    }
                    if (enableAutoload && !pluginsAutoloadScheduled_.exchange(true)) {
                        std::thread([this]() {
                            // Small delay to ensure listeners are fully up
                            std::this_thread::sleep_for(std::chrono::milliseconds(200));
                            try {
                                std::vector<std::filesystem::path> roots;
                                if (abiHost_) {
                                    for (const auto& p : abiHost_->trustList())
                                        roots.push_back(p);
                                }
                                if (wasmHost_) {
                                    for (const auto& p : wasmHost_->trustList())
                                        roots.push_back(p);
                                }
                                try {
                                    for (const auto& d :
                                         PluginLoader::getDefaultPluginDirectories())
                                        roots.push_back(d);
                                } catch (...) {
                                }
                                try {
                                    if (const char* env = std::getenv("YAMS_PLUGIN_DIR")) {
                                        std::filesystem::path penv(env);
                                        if (!penv.empty())
                                            roots.push_back(penv);
                                    }
                                } catch (...) {
                                }
                                std::sort(roots.begin(), roots.end());
                                roots.erase(std::unique(roots.begin(), roots.end()), roots.end());
                                for (const auto& r : roots) {
                                    try {
                                        if (abiHost_) {
                                            if (auto sr = abiHost_->scanDirectory(r)) {
                                                for (const auto& d : sr.value())
                                                    (void)abiHost_->load(d.path, "");
                                            }
                                        }
                                        if (wasmHost_) {
                                            if (auto sr = wasmHost_->scanDirectory(r)) {
                                                for (const auto& d : sr.value())
                                                    (void)wasmHost_->load(d.path, "");
                                            }
                                        }
                                    } catch (...) {
                                    }
                                }
                                (void)adoptModelProviderFromHosts();
                                (void)adoptContentExtractorsFromHosts();
                            } catch (...) {
                            }
                            writeBootstrapStatusFile(config_, state_);
                        }).detach();
                    }
                } catch (...) {
                }
            } else {
                // Leave searchEngineReady=false; service layer will fallback to metadata search
                writeBootstrapStatusFile(config_, state_);
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
        if (!searchEngine_ && metadataRepo_ && vectorIndexManager_) {
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

#endif

boost::asio::awaitable<Result<void>>
ServiceManager::initializeAsyncAwaitable(yams::compat::stop_token token) {
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

    // Plugins ready immediately (host-driven); schedule autoload later
    try {
        state_.readiness.pluginsReady = true;
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - state_.stats.startTime)
                      .count();
        state_.initDurationsMs.emplace("plugins", static_cast<uint64_t>(ms));
    } catch (...) {
        state_.readiness.pluginsReady = true;
    }
    writeBootstrapStatusFile(config_, state_);

    if (token.stop_requested())
        co_return Error{ErrorCode::OperationCancelled, "Shutdown requested"};

    // Resolve data dir (reuse logic from sync path)
    namespace fs = std::filesystem;
    fs::path dataDir = config_.dataDir;
    if (dataDir.empty()) {
        if (const char* storageEnv = std::getenv("YAMS_STORAGE"))
            dataDir = fs::path(storageEnv);
        else if (const char* dataEnv = std::getenv("YAMS_DATA_DIR"))
            dataDir = fs::path(dataEnv);
        else if (const char* homeEnv = std::getenv("HOME"))
            dataDir = fs::path(homeEnv) / ".local" / "share" / "yams";
        else
            dataDir = fs::path(".") / "yams_data";
    }
    {
        std::error_code ec;
        fs::create_directories(dataDir, ec);
        resolvedDataDir_ = dataDir;
    }
    spdlog::info("ServiceManager[co]: using data directory: {}", dataDir.string());

    // Content store (synchronous, quick)
    auto storeRoot = dataDir / "storage";
    spdlog::info("ContentStore root: {}", storeRoot.string());
    auto storeRes = yams::api::ContentStoreBuilder::createDefault(storeRoot);
    if (storeRes) {
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
        writeBootstrapStatusFile(config_, state_);
    }

    if (token.stop_requested())
        co_return Error{ErrorCode::OperationCancelled, "Shutdown requested"};

    // Phase: Open metadata DB with timeout
    auto dbPath = dataDir / "yams.db";
    database_ = std::make_shared<metadata::Database>();
    int open_timeout = read_timeout_ms("YAMS_DB_OPEN_TIMEOUT_MS", 5000, 250);
    bool db_ok = co_await co_openDatabase(dbPath, open_timeout, token);
    try {
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - state_.stats.startTime)
                      .count();
        state_.initDurationsMs.emplace("database", static_cast<uint64_t>(ms));
    } catch (...) {
    }
    writeBootstrapStatusFile(config_, state_);

    // Phase: Migrations (if DB ok)
    if (db_ok) {
        int mig_timeout = read_timeout_ms("YAMS_DB_MIGRATE_TIMEOUT_MS", 7000, 250);
        (void)co_await co_migrateDatabase(mig_timeout, token);
    }

    // Phase: Connection pool + repo
    if (db_ok) {
        metadata::ConnectionPoolConfig dbPoolCfg;
        size_t hw = std::max<size_t>(1, std::thread::hardware_concurrency());
        dbPoolCfg.minConnections = std::min<size_t>(std::max<size_t>(4, hw / 2), 16);
        dbPoolCfg.maxConnections = 64;
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
            spdlog::warn("Connection pool init failed: {} — continuing degraded",
                         poolInit.error().message);
        } else {
            metadataRepo_ = std::make_shared<metadata::MetadataRepository>(*connectionPool_);
            state_.readiness.metadataRepoReady = true;
            spdlog::info("Metadata repository initialized successfully");
        }
        try {
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::steady_clock::now() - state_.stats.startTime)
                          .count();
            state_.initDurationsMs.emplace("metadata_repo", static_cast<uint64_t>(ms));
        } catch (...) {
        }
        writeBootstrapStatusFile(config_, state_);
    }

    // Executors and sessions
    if (database_ && metadataRepo_)
        searchExecutor_ = std::make_shared<search::SearchExecutor>(database_, metadataRepo_);
    retrievalSessions_ = std::make_unique<RetrievalSessionManager>();

    // Vector DB init (uses internal timeout in backend)
    const bool __disable_vecdb = []() {
        if (const char* s = getenv("YAMS_DISABLE_VECTOR_DB")) {
            std::string v(s);
            std::transform(v.begin(), v.end(), v.begin(), ::tolower);
            return v == "1" || v == "true" || v == "yes" || v == "on";
        }
        return false;
    }();
    try {
        vector::VectorDatabaseConfig vdbCfg;
        vdbCfg.database_path = (dataDir / "vectors.db").string();
        vdbCfg.create_if_missing = false;
        // Resolve DB embedding dimension from config/env/generator
        size_t vdim = 0;
        std::optional<size_t> __storedDim{};
        // If DB already exists, prefer its stored dimension to avoid mismatches
        try {
            namespace fs = std::filesystem;
            auto vectorDbPath = dataDir / "vectors.db";
            if (fs::exists(vectorDbPath)) {
                auto d = read_db_embedding_dim(vectorDbPath);
                if (d && *d > 0) {
                    __storedDim = *d;
                    vdim = *d;
                    spdlog::info("VECTOR_DB_OPEN detected existing dim={} from DDL", vdim);
                }
            }
        } catch (...) {
        }
        // 1) Environment override
        try {
            if (const char* envd = std::getenv("YAMS_EMBED_DIM"))
                vdim = static_cast<size_t>(std::stoul(envd));
        } catch (...) {
        }
        // 2) Config file
        try {
            if (true) {
                namespace fs = std::filesystem;
                fs::path cfgHome;
                if (const char* xdg = std::getenv("XDG_CONFIG_HOME"))
                    cfgHome = fs::path(xdg);
                else if (const char* home = std::getenv("HOME"))
                    cfgHome = fs::path(home) / ".config";
                fs::path cfgPath = cfgHome / "yams" / "config.toml";
                if (!cfgPath.empty() && fs::exists(cfgPath)) {
                    std::ifstream in(cfgPath);
                    std::string line;
                    auto trim = [&](std::string& t) {
                        if (t.empty())
                            return;
                        t.erase(0, t.find_first_not_of(" 	"));
                        auto p = t.find_last_not_of(" 	");
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
                                    vdim = static_cast<size_t>(std::stoul(v));
                                } catch (...) {
                                }
                            }
                            if (vdim > 0)
                                break;
                        }
                    }
                }
            }
        } catch (...) {
        }
        // 3) Generator (if already present)
        try {
            if (vdim == 0 && embeddingGenerator_)
                vdim = embeddingGenerator_->getEmbeddingDimension();
        } catch (...) {
        }
        // 4) Model heuristic removed; fallback applied below
        if (vdim == 0)
            vdim = 384;
        vdbCfg.embedding_dim = vdim;
        if (__disable_vecdb) {
            spdlog::warn("Vector database initialization disabled by YAMS_DISABLE_VECTOR_DB");
        }
        if (!__disable_vecdb) {
            auto vdb = std::make_shared<vector::VectorDatabase>(vdbCfg);
            if (vdb->initialize()) {
                vectorDatabase_ = std::move(vdb);
                spdlog::info("Vector database initialized at {}", vdbCfg.database_path);
                // Persist sentinel and perform quick health probes
                try {
                    write_vector_sentinel(resolvedDataDir_, vdim, "vec0", 1);
                } catch (...) {
                }
                try {
                    std::size_t rows = vectorDatabase_->getVectorCount();
                    std::size_t docs = 0;
                    if (metadataRepo_) {
                        auto dc = metadataRepo_->getDocumentCount();
                        if (dc)
                            docs = static_cast<std::size_t>(dc.value());
                    }
                    if (docs > 0 && rows == 0) {
                        spdlog::info("Vector DB is empty but {} documents exist — repair will "
                                     "enqueue backlog after Ready.",
                                     docs);
                        // Auto-kick a bootstrap repair immediately (non-blocking).
                        // RepairCoordinator will also run.
                        try {
                            // Ensure generator ready
                            (void)ensureEmbeddingGeneratorReady();
                            auto embedGen = getEmbeddingGenerator();
                            auto store = getContentStore();
                            auto meta = getMetadataRepo();
                            auto extractors = getContentExtractors();
                            if (embedGen && store && meta) {
                                // Defer all embedding repairs to the RepairCoordinator once the
                                // daemon is Ready.
                                spdlog::info(
                                    "Bootstrap repair: deferred to RepairCoordinator after Ready");
                            }
                        } catch (...) {
                        }
                    }
                } catch (...) {
                }
                try {
                    auto sdim = read_vector_sentinel_dim(resolvedDataDir_);
                    if (sdim && *sdim != vdim) {
                        spdlog::warn("VECTOR_DB_SENTINEL dimension mismatch: sentinel={} "
                                     "expected={} — run 'yams doctor' if searches look off.",
                                     *sdim, vdim);
                    }
                } catch (...) {
                }
            } else {
                spdlog::warn("Vector database init failed ({}). Vector persistence disabled",
                             vdb->getLastError());
            }
        }
    } catch (const std::exception& e) {
        spdlog::warn("Vector database init exception: {}", e.what());
    }

    // Vector index manager
    try {
        const bool __disable_vecindex = __disable_vecdb;
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
        if (!__disable_vecindex) {
            vectorIndexManager_ = std::make_shared<vector::VectorIndexManager>(indexConfig);
            if (auto initRes = vectorIndexManager_->initialize(); !initRes) {
                spdlog::warn("Failed to initialize VectorIndexManager: {}",
                             initRes.error().message);
                vectorIndexManager_.reset();
            } else {
                state_.readiness.vectorIndexReady = true;
                try {
                    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                  std::chrono::steady_clock::now() - state_.stats.startTime)
                                  .count();
                    state_.initDurationsMs.emplace("vector_index", static_cast<uint64_t>(ms));
                } catch (...) {
                }
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
            if (auto loadResult = autoloadPluginsNow(); loadResult) {
                spdlog::info("ServiceManager: Autoloaded {} plugins.", loadResult.value());
            } else {
                spdlog::warn("ServiceManager: Plugin autoload failed: {}",
                             loadResult.error().message);
            }
            if (auto adoptResult = adoptModelProviderFromHosts();
                adoptResult && adoptResult.value()) {
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
            if (auto extractorResult = adoptContentExtractorsFromHosts(); extractorResult) {
                spdlog::info("ServiceManager: Adopted {} content extractors.",
                             extractorResult.value());
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
        auto built = co_await co_buildEngine(build_timeout, token, false);
        if (built) {
            std::lock_guard<std::mutex> lk(searchEngineMutex_);
            searchEngine_ = built;
            state_.readiness.searchEngineReady = true;
            state_.readiness.searchProgress = 100;
            writeBootstrapStatusFile(config_, state_);
            spdlog::info("HybridSearchEngine initialized and published to AppContext");
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
        if (r)
            co_return r.value();
        co_return nullptr;
    }

    boost::asio::steady_timer timer(ex);
    timer.expires_after(std::chrono::milliseconds(timeout_ms));
    auto which = co_await (ch.async_receive(boost::asio::as_tuple(boost::asio::use_awaitable)) ||
                           timer.async_wait(boost::asio::as_tuple(boost::asio::use_awaitable)));
    if (which.index() == 1 || token.stop_requested()) {
        spdlog::warn("HybridSearchEngine build timed out after {} ms; continuing degraded",
                     timeout_ms);
        co_return nullptr;
    }
    auto tup2 = std::move(std::get<0>(which));
    auto ec2 = std::get<0>(tup2);
    auto pres2 = std::get<1>(tup2);
    if (ec2 || !pres2 || !(*pres2)) {
        if (pres2 && (*pres2))
            spdlog::warn("HybridSearchEngine build failed: {}", pres2->error().message);
        co_return nullptr;
    }
    co_return pres2->value();
}

Result<bool> ServiceManager::adoptModelProviderFromHosts(const std::string& preferredName) {
    try {
        if (abiHost_) {
            auto loaded = abiHost_->listLoaded();
            auto try_adopt = [&](const std::string& pluginName) -> bool {
                auto ifaceRes = abiHost_->getInterface(pluginName, "model_provider_v1", 1);
                if (!ifaceRes) {
                    spdlog::debug("Model provider iface not found for plugin '{}' : {}", pluginName,
                                  ifaceRes.error().message);
                    return false;
                }
                auto* table = reinterpret_cast<yams_model_provider_v1*>(ifaceRes.value());
                if (!table || table->abi_version != YAMS_IFACE_MODEL_PROVIDER_V1_VERSION)
                    return false;
                modelProvider_ = std::make_shared<AbiModelProviderAdapter>(table);
                state_.readiness.modelProviderReady = (modelProvider_ != nullptr);
                spdlog::info("Adopted model provider from plugin: {}", pluginName);
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
                spdlog::debug("Trying model provider adoption from loaded plugin {}", d.name);
                if (try_adopt(d.name))
                    return Result<bool>(true);
                // Try stem of path as alternate plugin name
                try {
                    std::string alt = std::filesystem::path(d.path).stem().string();
                    if (!alt.empty() && alt != d.name) {
                        spdlog::debug("Trying model provider adoption from plugin path stem {}",
                                      alt);
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
                            spdlog::debug("getInterface(model_provider_v1,{}) failed for {}", vv,
                                          d.name);
                            continue;
                        }
                        auto* table = reinterpret_cast<yams_model_provider_v1*>(ifaceAlt.value());
                        if (!table) {
                            spdlog::debug("Null provider table for {}", d.name);
                            continue;
                        }
                        spdlog::info("Adopted model provider (alt ABI v{}) from plugin: {}", vv,
                                     d.name);
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

Result<size_t> ServiceManager::autoloadPluginsNow() {
    try {
        std::vector<std::filesystem::path> roots;
        if (abiHost_) {
            for (const auto& p : abiHost_->trustList())
                roots.push_back(p);
        }
        if (wasmHost_) {
            for (const auto& p : wasmHost_->trustList())
                roots.push_back(p);
        }
        try {
            for (const auto& d : PluginLoader::getDefaultPluginDirectories())
                roots.push_back(d);
        } catch (...) {
        }
        try {
            if (const char* env = std::getenv("YAMS_PLUGIN_DIR")) {
                std::filesystem::path penv(env);
                if (!penv.empty())
                    roots.push_back(penv);
            }
        } catch (...) {
        }

        std::sort(roots.begin(), roots.end());
        roots.erase(std::unique(roots.begin(), roots.end()), roots.end());

        spdlog::info("Plugin autoload(now): {} roots to scan", roots.size());
        size_t loaded_count = 0;
        for (const auto& r : roots) {
            try {
                if (abiHost_) {
                    if (auto sr = abiHost_->scanDirectory(r)) {
                        for (const auto& d : sr.value()) {
                            if (abiHost_->load(d.path, ""))
                                ++loaded_count;
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
        co_return; // Already started
    }

    try {
        auto res = ensureEmbeddingGeneratorReady();
        if (!res) {
            spdlog::warn("Embedding init failed (deferred): {}", res.error().message);
            co_return;
        }

        embedInitCompleted_ = true;
        // Model is already loaded by ensureEmbeddingGeneratorReady(), no need to preload again

        // Protect against concurrent rebuilds
        if (!rebuildInProgress_.exchange(true)) {
            spdlog::info(
                "Embedding generator ready, rebuilding search engine to enable vector search.");
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

                spdlog::info("HybridSearchEngine rebuilt with embedding generator. Vector search "
                             "is now active.");
            } else {
                spdlog::warn("Failed to rebuild HybridSearchEngine with embedding generator.");
            }

            rebuildInProgress_ = false;
        } else {
            spdlog::debug("Search engine rebuild already in progress, skipping duplicate request.");
        }
    } catch (const std::exception& e) {
        spdlog::warn("co_enableEmbeddingsAndRebuild error: {}", e.what());
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

app::services::AppContext ServiceManager::getAppContext() const {
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

} // namespace yams::daemon

namespace yams::daemon {

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

        // Try to get or create a model provider
        if (!modelProvider_ || !modelProvider_->isAvailable()) {
            spdlog::debug("Model provider not available, attempting to load plugins");
            (void)autoloadPluginsNow();
            auto adopted = adoptModelProviderFromHosts();
            if (!adopted || !adopted.value()) {
                spdlog::debug("No model provider adopted from plugins, trying local fallback");
                // Fallback to local model generator when provider not available
                try {
                    std::string preferred_local;
                    if (const char* envp = std::getenv("YAMS_PREFERRED_MODEL")) {
                        preferred_local = envp;
                    }
                    if (preferred_local.empty()) {
                        namespace fs = std::filesystem;
                        if (const char* home = std::getenv("HOME")) {
                            fs::path models = fs::path(home) / ".yams" / "models";
                            std::error_code ec;
                            if (fs::exists(models, ec) && fs::is_directory(models, ec)) {
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
                    if (!preferred_local.empty()) {
                        vector::EmbeddingConfig ecfg;
                        if (const char* home = std::getenv("HOME")) {
                            ecfg.model_path = (std::filesystem::path(home) / ".yams" / "models" /
                                               preferred_local / "model.onnx")
                                                  .string();
                        }
                        ecfg.model_name = preferred_local;
                        ecfg.backend = vector::EmbeddingConfig::Backend::Hybrid;
                        ecfg.daemon_auto_start = false;
                        auto eg = std::make_shared<vector::EmbeddingGenerator>(ecfg);
                        if (eg->initialize()) {
                            embeddingGenerator_ = eg;
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
            embeddingGenerator_ = std::move(eg);
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
            }
        }
    } catch (const std::exception& e) {
        spdlog::debug("Error reading config for preferred model: {}", e.what());
    }

    // 3. Auto-detect from available models
    try {
        if (const char* home = std::getenv("HOME")) {
            namespace fs = std::filesystem;
            fs::path models = fs::path(home) / ".yams" / "models";
            std::error_code ec;
            if (fs::exists(models, ec) && fs::is_directory(models, ec)) {
                // Prefer models in this order
                std::vector<std::string> preferences = {"nomic-embed-text-v1.5",
                                                        "nomic-embed-text-v1", "all-MiniLM-L6-v2",
                                                        "all-mpnet-base-v2"};

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
