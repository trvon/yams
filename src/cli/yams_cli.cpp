#include <spdlog/spdlog.h>
#include <fstream>
#include <future>
#include <iostream>
#include <map>
#include <sstream>
#include <yams/api/content_store_builder.h>
#include <yams/cli/command_registry.h>
#include <yams/cli/yams_cli.h>
#include <yams/config/config_helpers.h>
#include <yams/config/config_migration.h>
#include <yams/daemon/client/global_io_context.h>
#include <yams/metadata/database.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>
#include <yams/search/search_engine_builder.h>
#include <yams/vector/embedding_generator.h>
#include <yams/vector/embedding_service.h>
#include <yams/vector/sqlite_vec_backend.h>
#include <yams/vector/vector_database.h>
#include <yams/vector/vector_index_manager.h>
#include <yams/version.hpp>
// Error hints for actionable error messages
#include <yams/cli/error_hints.h>
// Generated version header (builddir/version_generated.h via generated_inc)
#if __has_include(<version_generated.h>)
#include <version_generated.h>
#endif

namespace fs = std::filesystem;

#ifdef __linux__
#include <unistd.h>
#include <linux/limits.h>
#elif defined(__APPLE__)
#include <mach-o/dyld.h>
#endif
#if defined(YAMS_EMBEDDED_VERBOSE_HELP)
#if defined(__has_include)
#if __has_include(<generated/cli_help.hpp>) && \
        __has_include(<generated/cli_help_init.hpp>) && \
        __has_include(<generated/cli_help_add.hpp>) && \
        __has_include(<generated/cli_help_get.hpp>) && \
        __has_include(<generated/cli_help_delete.hpp>) && \
        __has_include(<generated/cli_help_list.hpp>) && \
        __has_include(<generated/cli_help_search.hpp>) && \
        __has_include(<generated/cli_help_config.hpp>) && \
        __has_include(<generated/cli_help_auth.hpp>) && \
        __has_include(<generated/cli_help_stats.hpp>) && \
        __has_include(<generated/cli_help_uninstall.hpp>) && \
        __has_include(<generated/cli_help_migrate.hpp>) && \
        __has_include(<generated/cli_help_serve.hpp>)
#define YAMS_HAVE_GENERATED_CLI_HELP 1
#include <generated/cli_help.hpp>
#include <generated/cli_help_add.hpp>
#include <generated/cli_help_auth.hpp>
#include <generated/cli_help_config.hpp>
#include <generated/cli_help_delete.hpp>
#include <generated/cli_help_get.hpp>
#include <generated/cli_help_init.hpp>
#include <generated/cli_help_list.hpp>
#include <generated/cli_help_migrate.hpp>
#include <generated/cli_help_search.hpp>
#include <generated/cli_help_serve.hpp>
#include <generated/cli_help_stats.hpp>
#include <generated/cli_help_uninstall.hpp>
#else
// Generated CLI help was requested but headers are not available.
// Disable embedded help to fall back to runtime message paths.
#undef YAMS_EMBEDDED_VERBOSE_HELP
#endif
#else
// Compiler does not support __has_include; disable embedded help fallback.
#undef YAMS_EMBEDDED_VERBOSE_HELP
#endif
#endif
#include <algorithm>
#ifdef NDEBUG
#include <spdlog/sinks/null_sink.h>
#endif
#include <chrono>
#include <cstdlib>
#include <string>
#include <string_view>
#include <vector>
// Async runner for deferred command execution
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>

namespace yams::cli {
// NOTE: KG store is now managed as an instance member on YamsCLI (kgStore_)

void YamsCLI::setPendingCommand(ICommand* cmd) {
    pendingCommand_ = cmd;
}

YamsCLI::YamsCLI(boost::asio::any_io_executor executor) : executor_(std::move(executor)) {
    // Set a conservative default; finalized after parsing flags in run()
    spdlog::set_level(spdlog::level::warn);

    // Fall back to GlobalIOContext if no executor provided (for backwards compatibility)
    if (!executor_) {
        executor_ = yams::daemon::GlobalIOContext::instance().get_io_context().get_executor();
    }

    app_ = std::make_unique<CLI::App>("YAMS", "yams");
    app_->prefix_command(); // Allow global options before and after subcommands
    // Prefer generated effective version if present; otherwise fallback to existing macros.
#if __has_include(<version_generated.h>)
    {
        yams::VersionInfo ver{};
        std::string long_version = std::string(ver.effective_version);
        // Show commit hash if available (for dev builds)
        std::string commit = ver.git_commit ? std::string(ver.git_commit) : "";
        if (!commit.empty()) {
            long_version += " (commit: " + commit + ")";
        } else if (std::string(ver.git_describe).size()) {
            // Fallback to git describe if commit not available
            long_version += " (" + std::string(ver.git_describe) + ")";
        }
        long_version += " built:" + std::string(ver.build_timestamp);
        app_->set_version_flag("--version", long_version);
    }
#elif defined(YAMS_VERSION_LONG_STRING)
    app_->set_version_flag("--version", YAMS_VERSION_LONG_STRING);
#else
    app_->set_version_flag("--version", YAMS_VERSION_STRING);
#endif

    // Global options
    // Use platform-specific data directory (XDG_DATA_HOME on Unix, LOCALAPPDATA on Windows)
    std::filesystem::path defaultDataPath = yams::config::get_data_dir();

    // Try to load default data dir from config.toml (core.data_dir)
    try {
        auto cfgPath = getConfigPath();
        if (std::filesystem::exists(cfgPath)) {
            auto cfg = parseSimpleToml(cfgPath);
            auto it = cfg.find("core.data_dir");
            if (it != cfg.end() && !it->second.empty()) {
                std::string p = it->second;
                // Expand leading ~ to HOME
                if (!p.empty() && p.front() == '~') {
                    if (const char* home = std::getenv("HOME")) {
                        p = std::string(home) + p.substr(1);
                    }
                }
                defaultDataPath = std::filesystem::path(p);
                // Record that config provided a value to enforce precedence later
                configProvidesDataDir_ = true;
            }
        }
    } catch (...) {
        // Ignore config errors and keep env-based fallback
    }

    // We intentionally do not bind envname() here so we can enforce precedence (config > env > CLI)
    storageOpt_ = app_->add_option("--data-dir,--storage", dataPath_, "Data directory for storage")
                      ->default_val(defaultDataPath.string());

    app_->add_flag("-v,--verbose", verbose_, "Enable verbose output");
    app_->add_flag("--json", jsonOutput_, "Output in JSON format");
}

YamsCLI::~YamsCLI() {
    // Cleanup will be handled by smart pointers
}

bool YamsCLI::hasExplicitDataDir() const {
    return storageOpt_ && storageOpt_->count() > 0;
}

int YamsCLI::run(int argc, char* argv[]) {
    try {
        // Pre-scan for verbose help flags before CLI11 handles --help
        auto isFlag = [](const char* s) -> bool { return s && s[0] == '-'; };

        // Known subcommands (kept in sync with CommandRegistry)
        static const std::vector<std::string> kCommands = {
            "init",  "add",       "get",     "delete", "list",  "search",     "config", "auth",
            "stats", "uninstall", "migrate", "update", "serve", "completion", "model"};

        auto hasArg = [&](std::string_view needle) {
            for (int i = 1; i < argc; ++i) {
                if (argv[i] == nullptr)
                    continue;
                if (needle == argv[i])
                    return true;
            }
            return false;
        };

        // Determine potential subcommand (first non-flag token)
        std::string subcmd;
        if (argc > 1 && !isFlag(argv[1])) {
            subcmd = argv[1];
        }

        const bool helpAll = hasArg("--help-all");
        const bool helpFlag = hasArg("--help") || hasArg("-h");
        const bool verboseOpt = hasArg("--verbose"); // do not treat -v as help-verbose

#ifdef YAMS_HAVE_GENERATED_CLI_HELP
        auto printFullVerbose = [&]() { std::cout << yams::cli_help::VERBOSE << std::endl; };
        auto printCmdVerbose = [&](const std::string& cmd) {
            // Prefer per-command embedded constants when available; fallback to slicing from full
            // blob
            const char* sectionPtr = nullptr;
            if (false) {
            } else if (cmd == "init")
                sectionPtr = yams::cli_help::CMD_INIT;
            else if (cmd == "add")
                sectionPtr = yams::cli_help::CMD_ADD;
            else if (cmd == "get")
                sectionPtr = yams::cli_help::CMD_GET;
            else if (cmd == "delete")
                sectionPtr = yams::cli_help::CMD_DELETE;
            else if (cmd == "list")
                sectionPtr = yams::cli_help::CMD_LIST;
            else if (cmd == "search")
                sectionPtr = yams::cli_help::CMD_SEARCH;
            else if (cmd == "config")
                sectionPtr = yams::cli_help::CMD_CONFIG;
            else if (cmd == "auth")
                sectionPtr = yams::cli_help::CMD_AUTH;
            else if (cmd == "stats")
                sectionPtr = yams::cli_help::CMD_STATS;
            else if (cmd == "uninstall")
                sectionPtr = yams::cli_help::CMD_UNINSTALL;
            else if (cmd == "migrate")
                sectionPtr = yams::cli_help::CMD_MIGRATE;
            else if (cmd == "serve")
                sectionPtr = yams::cli_help::CMD_SERVE;
            if (sectionPtr && sectionPtr[0] != '\0') {
                std::cout << sectionPtr << std::endl;
                return;
            }
            // Fallback to slicing from the full verbose blob
            std::string_view all{yams::cli_help::VERBOSE};
            std::string marker = "## " + cmd;
            size_t pos = all.find(marker);
            if (pos == std::string_view::npos) {
                marker = std::string("\n## ") + cmd;
                pos = all.find(marker);
                if (pos != std::string_view::npos) {
                    pos += 1;
                }
            }
            if (pos == std::string_view::npos) {
                printFullVerbose();
                return;
            }
            size_t next = all.find("\n## ", pos + 3);
            std::string_view section =
                (next == std::string_view::npos) ? all.substr(pos) : all.substr(pos, next - pos);
            std::cout << section << std::endl;
        };
#endif

        // Handle top-level verbose help
        if (helpAll || (helpFlag && verboseOpt && subcmd.empty())) {
#ifdef YAMS_HAVE_GENERATED_CLI_HELP
            printFullVerbose();
#else
            std::cout
                << "Verbose help not embedded. See docs/user_guide/cli.md or rebuild with docs.\n";
#endif
            return 0;
        }

        // Handle per-command verbose help: yams <cmd> --help --verbose
        if (!subcmd.empty() && helpFlag && verboseOpt) {
            bool known = std::find(kCommands.begin(), kCommands.end(), subcmd) != kCommands.end();
            if (known) {
#ifdef YAMS_HAVE_GENERATED_CLI_HELP
                printCmdVerbose(subcmd);
#else
                std::cout << "Verbose help not embedded for command '" << subcmd
                          << "'. See docs/user_guide/cli.md.\n";
#endif
                return 0;
            }
        }

        // Register all commands
        registerBuiltinCommands();

        // If 'daemon' is invoked without a subcommand, inject 'status' by default
        // Detect: first non-flag token is 'daemon' and there is no subsequent non-flag token
        bool injectDaemonStatus = false;
        if (argc > 1 && argv[1] && std::string_view(argv[1]) == "daemon") {
            bool hasSub = false;
            for (int i = 2; i < argc; ++i) {
                if (argv[i] == nullptr)
                    continue;
                if (!isFlag(argv[i])) {
                    hasSub = true;
                    break;
                }
            }
            if (!hasSub)
                injectDaemonStatus = true;
        }

        // Handle deprecated command aliases
        std::vector<char*> argvVec;
        bool needsAliasRewrite = false;
        static std::string repairStr = std::string("repair");
        static std::string mimeFlag = std::string("--mime");

        if (argc > 1 && std::string(argv[1]) == "repair-mime") {
            // Rewrite "repair-mime" to "repair --mime"
            needsAliasRewrite = true;
            argvVec.reserve(argc + 1);
            argvVec.push_back(argv[0]); // program name
            argvVec.push_back(const_cast<char*>(repairStr.c_str()));
            argvVec.push_back(const_cast<char*>(mimeFlag.c_str()));
            for (int i = 2; i < argc; ++i)
                argvVec.push_back(argv[i]);
            argvVec.push_back(nullptr);
        } else if (injectDaemonStatus) {
            argvVec.reserve(argc + 1);
            for (int i = 0; i < argc; ++i)
                argvVec.push_back(argv[i]);
            static std::string statusStr = std::string("status");
            argvVec.push_back(const_cast<char*>(statusStr.c_str()));
            argvVec.push_back(nullptr);
        }

        if (needsAliasRewrite) {
            app_->parse(static_cast<int>(argvVec.size() - 1), argvVec.data());
        } else if (injectDaemonStatus) {
            app_->parse(argc + 1, argvVec.data());
        } else {
            // Parse command line
            app_->parse(argc, argv);
        }

        // Disallow multiple top-level subcommands (e.g., "yams search graph"),
        // which can accidentally treat query terms as commands.
        {
            std::vector<std::string> active;
            for (const auto* sub : app_->get_subcommands()) {
                if (sub && sub->count() > 0) {
                    active.push_back(sub->get_name());
                }
            }
            if (active.size() > 1) {
                std::cerr << "[FAIL] Multiple subcommands detected: ";
                for (size_t i = 0; i < active.size(); ++i) {
                    if (i)
                        std::cerr << ", ";
                    std::cerr << active[i];
                }
                std::cerr << "\n";
                auto isSearch = std::find(active.begin(), active.end(), "search") != active.end();
                if (isSearch) {
                    for (const auto& name : active) {
                        if (name != "search") {
                            std::cerr << "Hint: If '" << name
                                      << "' is your search term, use --query (e.g., "
                                         "`yams search --query "
                                      << name << "`).\n";
                            break;
                        }
                    }
                }
                return 1;
            }
        }

        // Apply log level after parsing flags (avoid CLI11 Option::callback dependency)
        // Precedence: env YAMS_LOG_LEVEL > --verbose > build-type default
        auto parseLevel = [](const std::string& s) -> std::optional<spdlog::level::level_enum> {
            std::string v;
            v.reserve(s.size());
            for (char c : s)
                v.push_back(static_cast<char>(std::tolower(c)));
            if (v == "trace")
                return spdlog::level::trace;
            if (v == "debug")
                return spdlog::level::debug;
            if (v == "info")
                return spdlog::level::info;
            if (v == "warn" || v == "warning")
                return spdlog::level::warn;
            if (v == "error" || v == "err")
                return spdlog::level::err;
            if (v == "critical" || v == "crit")
                return spdlog::level::critical;
            if (v == "off" || v == "none" || v == "silent")
                return spdlog::level::off;
            return std::nullopt;
        };

        if (const char* envLvl = std::getenv("YAMS_LOG_LEVEL"); envLvl && *envLvl) {
            if (auto lvl = parseLevel(envLvl)) {
                spdlog::set_level(*lvl);
            }
        } else if (verbose_) {
            spdlog::set_level(spdlog::level::debug);
        } else {
#ifdef NDEBUG
            // Suppress spdlog output in Release unless explicitly enabled
            auto null_sink = std::make_shared<spdlog::sinks::null_sink_mt>();
            auto null_logger = std::make_shared<spdlog::logger>("yams-cli-null", null_sink);
            spdlog::set_default_logger(null_logger);
            spdlog::set_level(spdlog::level::off);
#else
            spdlog::set_level(spdlog::level::warn);
#endif
        }

        // Enforce data directory precedence after parsing options
        // Order: config (if present) > env (YAMS_STORAGE/YAMS_DATA_DIR) > CLI-provided > default
        try {
            // If config provided data dir, force it
            if (configProvidesDataDir_) {
                // Ensure dataPath_ reflects the config-derived default
                // (already set via default_val in constructor)
            } else {
                // If no config value, allow environment to override
                const char* envStorage = std::getenv("YAMS_STORAGE");
                const char* envDataDir = std::getenv("YAMS_DATA_DIR");
                if (envStorage && *envStorage) {
                    dataPath_ = fs::path(envStorage);
                } else if (envDataDir && *envDataDir) {
                    dataPath_ = fs::path(envDataDir);
                } else if (storageOpt_ && storageOpt_->count() > 0) {
                    // CLI was explicitly provided; leave dataPath_ as set by CLI11
                } else {
                    // Keep constructor default (XDG/HOME-based)
                }
            }
        } catch (...) {
            // Non-fatal: keep whatever CLI11 assigned
        }

        // Check for config migration before storage initialization
        checkConfigMigration();

        // Storage initialization is performed lazily by commands via ensureStorageInitialized()

        // If a command scheduled deferred execution, run it now once via a single async spawn
        if (pendingCommand_) {
            std::promise<Result<void>> prom;
            auto fut = prom.get_future();
            boost::asio::co_spawn(
                executor_,
                [this, &prom]() -> boost::asio::awaitable<void> {
                    auto r = co_await pendingCommand_->executeAsync();
                    prom.set_value(std::move(r));
                    co_return;
                },
                boost::asio::detached);
            auto status = fut.wait_for(std::chrono::minutes(10));
            if (status != std::future_status::ready) {
                spdlog::error("Command timed out");
                std::cerr << formatErrorWithHint(ErrorCode::Timeout, "Command timed out") << "\n";
                return 1;
            }
            auto result = fut.get();
            if (!result) {
                // Use error hints for actionable feedback
                std::cerr << formatErrorWithHint(result.error().code, result.error().message)
                          << "\n";
                return 1;
            }
        }

        return 0;
    } catch (const CLI::ParseError& e) {
        return app_->exit(e);
    } catch (const std::exception& e) {
        // Always provide a user-facing error even if logging is off
        std::cerr << "[FAIL] Unexpected error: " << e.what() << "\n";
        spdlog::error("Unexpected error: {}", e.what());
        return 1;
    }
}

Result<void> YamsCLI::ensureStorageInitialized() {
    if (contentStore_) {
        return Result<void>();
    }
    auto initResult = initializeStorage();
    if (!initResult) {
        // Use centralized error hints for actionable guidance
        auto hint = getErrorHint(initResult.error().code, initResult.error().message);
        std::string enhancedMessage = initResult.error().message;
        if (!hint.hint.empty()) {
            enhancedMessage += " (hint: " + hint.hint;
            if (!hint.command.empty()) {
                enhancedMessage += "; try '" + hint.command + "'";
            }
            enhancedMessage += ")";
        }
        return Error{initResult.error().code, enhancedMessage};
    }
    return Result<void>();
}

std::shared_ptr<vector::EmbeddingGenerator> YamsCLI::getEmbeddingGenerator() {
    if (embeddingGenerator_ && !embeddingGenerator_->isInitialized()) {
        // Lazy initialization on first actual use
        if (embeddingGenerator_->initialize()) {
            spdlog::info("Successfully initialized EmbeddingGenerator with backend: {}",
                         embeddingGenerator_->getModelInfo());
        } else {
            spdlog::warn("Failed to initialize EmbeddingGenerator on first use");
            embeddingGenerator_.reset();
        }
    }
    return embeddingGenerator_;
}

std::shared_ptr<app::services::AppContext> YamsCLI::getAppContext() {
    std::lock_guard<std::mutex> lock(appContextMutex_);

    if (!appContext_) {
        // Ensure storage is initialized before creating context
        auto initResult = ensureStorageInitialized();
        if (!initResult) {
            if (verbose_) {
                spdlog::error("Failed to initialize storage for AppContext: {}",
                              initResult.error().message);
            }
            return nullptr;
        }

        // Create the app context with all available components
        appContext_ = std::make_shared<app::services::AppContext>();
        appContext_->store = getContentStore();
        appContext_->searchExecutor = getSearchExecutor();
        appContext_->metadataRepo = getMetadataRepository();
        appContext_->kgStore = getKnowledgeGraphStore(); // PBI-043: tree diff KG integration
        appContext_->workerExecutor = executor_;         // 066-59: Thread executor through services

        // Initialize SearchEngine so SearchService can use hybrid search by default
        try {
            auto vecMgr = getVectorIndexManager();
            auto vecDb = getVectorDatabase();
            auto repo = getMetadataRepository();

            if (vecMgr && repo) {
                yams::search::SearchEngineBuilder builder;
                builder.withVectorIndex(vecMgr)
                    .withVectorDatabase(vecDb)
                    .withMetadataRepo(repo)
                    .withKGStore(getKnowledgeGraphStore());

                // Reuse a shared embedding generator if available
                if (auto emb = getEmbeddingGenerator()) {
                    builder.withEmbeddingGenerator(emb);
                }

                auto opts = yams::search::SearchEngineBuilder::BuildOptions::makeDefault();
                auto engRes = builder.buildEmbedded(opts);
                if (engRes) {
                    appContext_->searchEngine = engRes.value();
                    appContext_->vectorDatabase = vecDb;
                    spdlog::info("SearchEngine initialized for AppContext");
                } else {
                    spdlog::warn("SearchEngine initialization failed: {}", engRes.error().message);
                    appContext_->searchEngine = nullptr;
                }
            } else {
                appContext_->searchEngine = nullptr;
            }
        } catch (const std::exception& e) {
            spdlog::warn("SearchEngine bring-up error (ignored): {}", e.what());
            appContext_->searchEngine = nullptr;
        }

        spdlog::debug("Created AppContext for services");
    }

    return appContext_;
}

Result<void> YamsCLI::initializeStorage() {
    try {
        // Validate dataPath_ is a reasonable filesystem path, not a CLI description
        // This guards against CLI11 option description leaks into the data path
        auto pathStr = dataPath_.string();
        bool looksLikeDescription =
            pathStr.find('(') != std::string::npos || // CLI descriptions often have parens
            pathStr.find('[') != std::string::npos || // [Deprecated] etc.
            pathStr.length() > 200 ||                 // Paths shouldn't be this long
            std::count(pathStr.begin(), pathStr.end(), ' ') > 3; // Too many spaces

        if (looksLikeDescription) {
            spdlog::error("Invalid data path (looks like CLI description): {}", pathStr);
            return Error{ErrorCode::InvalidArgument,
                         "Data path appears invalid: '" + pathStr.substr(0, 60) +
                             "...' - please set YAMS_DATA_DIR or use --data-dir"};
        }

        // Create data directory if it doesn't exist
        if (!std::filesystem::exists(dataPath_)) {
            std::filesystem::create_directories(dataPath_);
        }

        // Initialize database and connection pool
        auto dbPath = dataPath_ / "yams.db";

        // Create separate database instance for SearchExecutor
        database_ = std::make_shared<metadata::Database>();
        auto dbResult = database_->open(dbPath.string(), metadata::ConnectionMode::Create);
        if (!dbResult) {
            return dbResult;
        }

        // Run migrations to create database schema
        metadata::MigrationManager migrationManager(*database_);

        // Initialize migration system (creates migration_history table)
        auto initResult = migrationManager.initialize();
        if (!initResult) {
            if (verbose_) {
                spdlog::error("Failed to initialize migration system: {}",
                              initResult.error().message);
            }
            return initResult;
        }

        // Register all migrations
        auto migrations = metadata::YamsMetadataMigrations::getAllMigrations();
        for (const auto& migration : migrations) {
            migrationManager.registerMigration(migration);
        }

        // Apply all migrations
        auto migrateResult = migrationManager.migrate();
        if (!migrateResult) {
            const std::string em = migrateResult.error().message;
            // Be tolerant to idempotent/constraint cases: proceed if schema already satisfies
            // constraints
            if (em.find("constraint failed") != std::string::npos ||
                em.find("already exists") != std::string::npos) {
                spdlog::warn("Proceeding despite migration warning: {}", em);
            } else {
                if (verbose_) {
                    spdlog::error("Failed to run database migrations: {}", em);
                }
                return migrateResult;
            }
        }

        // Create connection pool for MetadataRepository
        metadata::ConnectionPoolConfig poolConfig;
        poolConfig.maxConnections = 10;
        poolConfig.minConnections = 1;
        poolConfig.connectTimeout = std::chrono::seconds(30);

        connectionPool_ = std::make_shared<metadata::ConnectionPool>(dbPath.string(), poolConfig);
        auto poolResult = connectionPool_->initialize();
        if (!poolResult) {
            return poolResult;
        }

        // Initialize metadata repository
        metadataRepo_ = std::make_shared<metadata::MetadataRepository>(*connectionPool_);

        // Build content store
        api::ContentStoreBuilder builder;
        builder.withStoragePath(dataPath_ / "storage").withChunkSize(DEFAULT_CHUNK_SIZE);

        // Load compression settings from config
        auto compressionConfig = loadCompressionConfig();
        builder.withCompression(compressionConfig.enable)
            .withCompressionType(compressionConfig.algorithm)
            .withCompressionLevel(compressionConfig.level)
            .withDeduplication(true)
            .withIntegrityChecks(true);

        auto storeResult = builder.build();
        if (!storeResult) {
            return Error{storeResult.error().code, storeResult.error().message};
        }

        // Move the unique_ptr and convert to shared_ptr
        auto store =
            std::move(const_cast<std::unique_ptr<api::IContentStore>&>(storeResult.value()));
        contentStore_ = std::shared_ptr<api::IContentStore>(store.release());

        // Initialize search executor
        searchExecutor_ = std::make_shared<search::SearchExecutor>(database_, metadataRepo_);

        // Initialize Knowledge Graph store with defaults (local-first)
        {
            metadata::KnowledgeGraphStoreConfig kgCfg;
            kgCfg.enable_alias_fts = true; // use FTS5 if available, fallback is automatic
            kgCfg.enable_wal = true;       // align with DB mode

            auto kgStoreRes = metadata::makeSqliteKnowledgeGraphStore(dbPath.string(), kgCfg);
            if (!kgStoreRes) {
                spdlog::warn("KG store initialization failed: {}", kgStoreRes.error().message);
            } else {
                // Promote to shared_ptr for downstream composition in CLI
                auto kgStoreUnique = std::move(kgStoreRes).value();
                kgStore_ = std::shared_ptr<metadata::KnowledgeGraphStore>(std::move(kgStoreUnique));

                // Best-effort health check; do not fail storage init on KG issues
                auto hc = kgStore_->healthCheck();
                if (!hc) {
                    spdlog::warn("KG store health check failed: {}", hc.error().message);
                }
            }
        }

        // Initialize VectorIndexManager if vector support is available
        try {
            // Try to detect proper dimension from existing vectors or available models
            size_t vectorDimension = 384; // Safe default

            // First, check if vectors.db exists and read stored dimension directly
            fs::path vectorDbPath = dataPath_ / "vectors.db";
            if (fs::exists(vectorDbPath)) {
                try {
                    vector::SqliteVecBackend be;
                    if (be.initialize(vectorDbPath.string())) {
                        (void)be.ensureVecLoaded();
                        if (auto sdim = be.getStoredEmbeddingDimension()) {
                            if (*sdim > 0)
                                vectorDimension = *sdim;
                        }
                        be.close();
                    }
                } catch (const std::exception& e) {
                    spdlog::debug("Could not read stored vector dimension: {}", e.what());
                }
            }

            // If no stored dim was found, prefer config > env > generator > heuristic
            if (vectorDimension == 384) {
                try {
                    auto cfgPath = getConfigPath();
                    if (fs::exists(cfgPath)) {
                        auto cfg = parseSimpleToml(cfgPath);
                        auto it = cfg.find("embeddings.embedding_dim");
                        if (it != cfg.end()) {
                            try {
                                vectorDimension = static_cast<size_t>(std::stoul(it->second));
                            } catch (...) {
                            }
                        }
                    }
                } catch (...) {
                }
                if (vectorDimension == 384) {
                    if (const char* envd = std::getenv("YAMS_EMBED_DIM")) {
                        try {
                            vectorDimension = static_cast<size_t>(std::stoul(envd));
                        } catch (...) {
                        }
                    }
                }
                if (vectorDimension == 384) {
                    try {
                        if (auto emb = getEmbeddingGenerator()) {
                            auto d = emb->getEmbeddingDimension();
                            if (d > 0)
                                vectorDimension = d;
                        }
                    } catch (...) {
                    }
                }
                // Heuristic remains 384
            }

            // Second, detect dimension from available models
            if (const char* home = std::getenv("HOME"); home) {
                fs::path modelsPath = dataPath_ / "models";
                if (fs::exists(modelsPath)) {
                    // Check for specific models in priority order
                    if (fs::exists(modelsPath / "all-MiniLM-L6-v2" / "model.onnx")) {
                        vectorDimension = 384;
                        if (verbose_) {
                            spdlog::info("Detected all-MiniLM-L6-v2 model, using 384 dimensions");
                        }
                    } else if (fs::exists(modelsPath / "all-mpnet-base-v2" / "model.onnx")) {
                        vectorDimension = 768;
                        if (verbose_) {
                            spdlog::info("Detected all-mpnet-base-v2 model, using 768 dimensions");
                        }
                    }
                }
            }

            // Configure the vector index with detected dimension
            vector::IndexConfig indexConfig;
            indexConfig.dimension = vectorDimension;
            indexConfig.type = vector::IndexType::FLAT; // Simple flat index for reliability
            indexConfig.distance_metric = vector::DistanceMetric::COSINE;
            indexConfig.index_path = (dataPath_ / "vector_index.bin").string();
            indexConfig.enable_persistence = true;
            indexConfig.max_elements = 1000000; // 1M vectors max

            vectorIndexManager_ = std::make_shared<vector::VectorIndexManager>(indexConfig);

            // Initialize the index
            auto vectorInitResult = vectorIndexManager_->initialize();
            if (!vectorInitResult) {
                spdlog::warn("Failed to initialize VectorIndexManager: {}",
                             vectorInitResult.error().message);
                vectorIndexManager_.reset(); // Clear it if initialization failed
            } else if (verbose_) {
                spdlog::info("VectorIndexManager initialized with {} dimension vectors",
                             indexConfig.dimension);
            }

            // Initialize EmbeddingClient (daemon-based) if VectorIndexManager is available
            if (vectorIndexManager_) {
                try {
                    // Check for available models
                    if (const char* home = std::getenv("HOME"); home) {
                        fs::path modelsPath = dataPath_ / "models";
                        if (fs::exists(modelsPath)) {
                            // Configure embedding settings
                            vector::EmbeddingConfig embConfig;

                            // Check for specific models in priority order
                            std::string selectedModel;
                            // 1) Preferred from config ([embeddings].preferred_model) or env
                            try {
                                std::string pref;
                                // Simple TOML parse via helper
                                auto cfgPath = getConfigPath();
                                if (fs::exists(cfgPath)) {
                                    auto cfg = parseSimpleToml(cfgPath);
                                    auto it = cfg.find("embeddings.preferred_model");
                                    if (it != cfg.end() && !it->second.empty())
                                        pref = it->second;
                                }
                                if (const char* p = std::getenv("YAMS_PREFERRED_MODEL")) {
                                    if (pref.empty())
                                        pref = p;
                                }
                                if (!pref.empty() && fs::exists(modelsPath / pref / "model.onnx")) {
                                    selectedModel = pref;
                                }
                            } catch (...) {
                            }

                            // 2) Known models (MiniLM/mpnet/nomic)
                            if (selectedModel.empty()) {
                                if (fs::exists(modelsPath / "nomic-embed-text-v1.5" /
                                               "model.onnx")) {
                                    selectedModel = "nomic-embed-text-v1.5";
                                } else if (fs::exists(modelsPath / "nomic-embed-text-v1" /
                                                      "model.onnx")) {
                                    selectedModel = "nomic-embed-text-v1";
                                } else if (fs::exists(modelsPath / "all-MiniLM-L6-v2" /
                                                      "model.onnx")) {
                                    selectedModel = "all-MiniLM-L6-v2";
                                } else if (fs::exists(modelsPath / "all-mpnet-base-v2" /
                                                      "model.onnx")) {
                                    selectedModel = "all-mpnet-base-v2";
                                }
                            }

                            // 3) Any model directory containing model.onnx
                            if (selectedModel.empty()) {
                                for (const auto& e : fs::directory_iterator(modelsPath)) {
                                    if (e.is_directory() && fs::exists(e.path() / "model.onnx")) {
                                        selectedModel = e.path().filename().string();
                                        break;
                                    }
                                }
                            }

                            if (!selectedModel.empty()) {
                                embConfig.model_path =
                                    (modelsPath / selectedModel / "model.onnx").string();
                                embConfig.model_name = selectedModel;
                                embConfig.batch_size = 32;
                                // Provisional defaults (overridden at init by backend-reported)
                                if (selectedModel.find("MiniLM") != std::string::npos)
                                    embConfig.embedding_dim = 384;
                                else if (selectedModel.find("mpnet") != std::string::npos)
                                    embConfig.embedding_dim = 768;
                                else if (selectedModel.find("nomic") != std::string::npos)
                                    embConfig.embedding_dim = 768;
                                else
                                    embConfig.embedding_dim = 384;
                                embConfig.max_sequence_length = 512;
                                embConfig.normalize_embeddings = true;

                                // Configure backend selection (Hybrid by default for best
                                // performance)
                                embConfig.backend =
                                    vector::EmbeddingConfig::Backend::Hybrid; // Try daemon first,
                                                                              // fallback to local

                                // Additional daemon settings
                                // daemon_socket left empty - will be auto-resolved by DaemonClient
                                embConfig.daemon_timeout = std::chrono::milliseconds(5000);
                                embConfig.daemon_max_retries = 3;
                                // Do not auto-start the daemon from generic CLI init paths;
                                // rely on explicit `yams daemon start` or on-demand components.
                                embConfig.daemon_auto_start = false;

                                spdlog::info(
                                    "Creating EmbeddingGenerator with model: {} (hybrid backend)",
                                    selectedModel);

                                embeddingGenerator_ =
                                    std::make_shared<vector::EmbeddingGenerator>(embConfig);
                                // Defer initialization until actually needed to avoid daemon
                                // connection during stats and other non-embedding commands
                                spdlog::debug(
                                    "Created EmbeddingGenerator (deferred init) with model: {}",
                                    selectedModel);
                            } else {
                                spdlog::debug("No embedding models found in {}",
                                              modelsPath.string());
                            }
                        } else {
                            spdlog::debug("Models directory does not exist: {}",
                                          modelsPath.string());
                        }
                    } else {
                        spdlog::debug("HOME environment variable not set");
                    }
                } catch (const std::exception& e) {
                    spdlog::warn("Exception while initializing EmbeddingGenerator: {}", e.what());
                    // Continue without embedding generator
                }
            } else {
                spdlog::debug(
                    "VectorIndexManager not available, skipping EmbeddingGenerator initialization");
            }

            // Initialize vector database proactively using resolved dimension to avoid warnings
            try {
                vector::VectorDatabaseConfig vdbConfig;
                vdbConfig.database_path = (dataPath_ / "vectors.db").string();
                vdbConfig.embedding_dim = vectorDimension;

                auto vectorDb = std::make_shared<vector::VectorDatabase>(vdbConfig);
                if (vectorDb->initialize()) {
                    vectorDatabase_ = vectorDb;
                    if (verbose_) {
                        spdlog::info("Vector database initialized at: {}", vdbConfig.database_path);
                    }
                } else {
                    spdlog::debug("Vector database initialization deferred");
                }
            } catch (const std::exception& e) {
                spdlog::debug("Vector database initialization failed: {}", e.what());
            }

            // Check and trigger embedding repair if needed
            // This ensures embeddings are generated for existing documents
            if (contentStore_ && metadataRepo_) {
                try {
                    auto embeddingService = std::make_unique<vector::EmbeddingService>(
                        contentStore_, metadataRepo_, dataPath_);

                    if (embeddingService->isAvailable()) {
                        // Trigger repair thread if there are missing embeddings
                        embeddingService->triggerRepairIfNeeded();
                        if (verbose_) {
                            spdlog::info("Checking for missing embeddings...");
                        }
                    } else if (verbose_) {
                        spdlog::info("Embedding generation not available (no models found)");
                    }
                } catch (const std::exception& e) {
                    spdlog::debug("Failed to initialize embedding service: {}", e.what());
                }
            }

            if (verbose_) {
                spdlog::info(
                    "Vector support initialized - embeddings will be generated automatically");
            }
        } catch (const std::exception& e) {
            spdlog::warn("Failed to initialize vector support: {}", e.what());
            // Don't fail storage initialization if vector support fails
        }

        if (verbose_) {
            spdlog::info("Storage initialized at: {}", dataPath_.string());
        }

        return Result<void>();
    } catch (const std::exception& e) {
        return Error{ErrorCode::DatabaseError,
                     std::string("Failed to initialize storage: ") + e.what()};
    }
}

void YamsCLI::registerBuiltinCommands() {
    CommandRegistry::registerAllCommands(this);
}

void YamsCLI::registerCommand(std::unique_ptr<ICommand> command) {
    // Set executor on the command before registration
    command->setExecutor(executor_);
    command->registerCommand(*app_, this);
    commands_.push_back(std::move(command));
}

std::filesystem::path YamsCLI::findMagicNumbersFile() {
    namespace fs = std::filesystem;

    std::vector<fs::path> searchPaths;

    // 1. Check environment variable
    if (const char* dataDir = std::getenv("YAMS_DATA_DIR")) {
        searchPaths.push_back(fs::path(dataDir) / "magic_numbers.json");
    }

    // 2. Check relative to executable location (for installed binaries)
    try {
// Get the path to the current executable
#ifdef __linux__
        char result[PATH_MAX];
        ssize_t count = readlink("/proc/self/exe", result, PATH_MAX);
        if (count != -1) {
            fs::path exePath(std::string(result, count));
            // Check ../share/yams/data relative to binary
            searchPaths.push_back(exePath.parent_path().parent_path() / "share" / "yams" / "data" /
                                  "magic_numbers.json");
        }
#elif defined(__APPLE__)
        char path[1024];
        uint32_t size = sizeof(path);
        if (_NSGetExecutablePath(path, &size) == 0) {
            fs::path exePath(path);
            // Check ../share/yams/data relative to binary
            searchPaths.push_back(exePath.parent_path().parent_path() / "share" / "yams" / "data" /
                                  "magic_numbers.json");
        }
#endif
    } catch (...) {
        // Ignore errors in getting executable path
    }

    // 3. Check relative to current working directory (development/testing)
    searchPaths.push_back("data/magic_numbers.json");
    searchPaths.push_back("../data/magic_numbers.json");
    searchPaths.push_back("../../data/magic_numbers.json");
    searchPaths.push_back("../../../data/magic_numbers.json");
    searchPaths.push_back("../../../../data/magic_numbers.json");

    // 4. Check common installation paths
    searchPaths.push_back("/usr/local/share/yams/data/magic_numbers.json");
    searchPaths.push_back("/usr/share/yams/data/magic_numbers.json");
    searchPaths.push_back("/opt/yams/share/data/magic_numbers.json");

    // 5. Check in home directory
    if (const char* home = std::getenv("HOME")) {
        searchPaths.push_back(fs::path(home) / ".local" / "share" / "yams" / "data" /
                              "magic_numbers.json");
    }

    // Find the first existing file
    for (const auto& path : searchPaths) {
        if (fs::exists(path) && fs::is_regular_file(path)) {
            auto resolved = path.is_absolute() ? path : fs::absolute(path);
            spdlog::debug("Found magic_numbers.json at: {}", resolved.string());
            return resolved;
        }
    }

    spdlog::debug(
        "magic_numbers.json not found (using compiled-in patterns from magic_numbers.hpp)");
    return fs::path(); // Return empty path if not found
}

YamsCLI::CompressionConfig YamsCLI::loadCompressionConfig() const {
    CompressionConfig config;
    config.enable = true;      // Default
    config.algorithm = "zstd"; // Default
    config.level = 3;          // Default

    // Try to read from config file
    fs::path configPath = getConfigPath();
    if (!fs::exists(configPath)) {
        return config; // Return defaults
    }

    auto configMap = parseSimpleToml(configPath);

    // Load compression enable flag
    if (configMap.find("compression.enable") != configMap.end()) {
        config.enable = (configMap["compression.enable"] == "true");
    }

    // Load algorithm
    if (configMap.find("compression.algorithm") != configMap.end()) {
        config.algorithm = configMap["compression.algorithm"];
    }

    // Load compression level based on algorithm
    if (config.algorithm == "zstd" && configMap.find("compression.zstd_level") != configMap.end()) {
        try {
            config.level = std::stoi(configMap["compression.zstd_level"]);
        } catch (...) {
            config.level = 3; // Default zstd level
        }
    } else if (config.algorithm == "lzma" &&
               configMap.find("compression.lzma_level") != configMap.end()) {
        try {
            config.level = std::stoi(configMap["compression.lzma_level"]);
        } catch (...) {
            config.level = 6; // Default lzma level
        }
    }

    spdlog::debug("Loaded compression config: enable={}, algorithm={}, level={}", config.enable,
                  config.algorithm, config.level);

    return config;
}

fs::path YamsCLI::getConfigPath() const {
    // Use platform-specific config path (XDG_CONFIG_HOME on Unix, APPDATA on Windows)
    return yams::config::get_config_path();
}

std::map<std::string, std::string> YamsCLI::parseSimpleToml(const fs::path& path) const {
    std::map<std::string, std::string> config;
    std::ifstream file(path);
    if (!file) {
        return config;
    }

    std::string line;
    std::string currentSection;

    while (std::getline(file, line)) {
        // Skip comments and empty lines
        if (line.empty() || line[0] == '#')
            continue;

        // Check for section headers
        if (line[0] == '[') {
            size_t end = line.find(']');
            if (end != std::string::npos) {
                currentSection = line.substr(1, end - 1);
                if (!currentSection.empty()) {
                    currentSection += ".";
                }
            }
            continue;
        }

        // Parse key-value pairs
        size_t eq = line.find('=');
        if (eq != std::string::npos) {
            std::string key = line.substr(0, eq);
            std::string value = line.substr(eq + 1);

            // Trim whitespace
            key.erase(0, key.find_first_not_of(" \t"));
            key.erase(key.find_last_not_of(" \t") + 1);
            value.erase(0, value.find_first_not_of(" \t"));
            value.erase(value.find_last_not_of(" \t") + 1);

            // Remove quotes if present
            if (value.size() >= 2 && value[0] == '"' && value.back() == '"') {
                value = value.substr(1, value.size() - 2);
            }

            // Remove comments from value
            size_t comment = value.find('#');
            if (comment != std::string::npos) {
                value = value.substr(0, comment);
                // Trim again after removing comment
                value.erase(value.find_last_not_of(" \t") + 1);
            }

            config[currentSection + key] = value;
        }
    }

    return config;
}

void YamsCLI::checkConfigMigration() {
    try {
        auto configPath = getConfigPath();
        config::ConfigMigrator migrator;

        auto needsResult = migrator.needsMigration(configPath);
        if (!needsResult) {
            if (verbose_) {
                spdlog::debug("Config migration check failed: {}", needsResult.error().message);
            }
            return;
        }

        if (needsResult.value()) {
            // In non-interactive mode (tests, CI), auto-accept migration
            bool autoMigrate = false;
            if (const char* env = std::getenv("YAMS_NON_INTERACTIVE"); env && *env) {
                autoMigrate = true;
            }

            if (!autoMigrate) {
                std::cout << "\nConfiguration Migration Required\n";
                std::cout << "YAMS needs to update your configuration to version 2.\n";
                std::cout << "This will add new features and improve performance.\n\n";
                std::cout << "Proceed with migration? [Y/n]: ";
                std::cout.flush();

                std::string response;
                std::getline(std::cin, response);

                // Default to 'yes' if empty or starts with 'y'/'Y'
                autoMigrate = response.empty() || response[0] == 'y' || response[0] == 'Y';
            }

            if (autoMigrate) {
                auto migrateResult = migrator.migrateToV2(configPath, true);
                if (migrateResult) {
                    std::cout << "✅ Configuration successfully migrated to v2\n\n";
                } else {
                    std::cout << "❌ Migration failed: " << migrateResult.error().message << "\n\n";
                }
            } else {
                std::cout << "Migration skipped. Some features may not work correctly.\n\n";
            }
        }
    } catch (const std::exception& e) {
        if (verbose_) {
            spdlog::debug("Config migration check error: {}", e.what());
        }
    }
}

} // namespace yams::cli
