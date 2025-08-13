#include <yams/cli/yams_cli.h>
#include <yams/cli/command_registry.h>
#include <yams/api/content_store_builder.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/search/hybrid_search_factory.h>
#include <spdlog/spdlog.h>
#include <iostream>
#include <yams/version.hpp>
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
        __has_include(<generated/cli_help_browse.hpp>) && \
        __has_include(<generated/cli_help_serve.hpp>)
      #define YAMS_HAVE_GENERATED_CLI_HELP 1
      #include <generated/cli_help.hpp>
      #include <generated/cli_help_init.hpp>
      #include <generated/cli_help_add.hpp>
      #include <generated/cli_help_get.hpp>
      #include <generated/cli_help_delete.hpp>
      #include <generated/cli_help_list.hpp>
      #include <generated/cli_help_search.hpp>
      #include <generated/cli_help_config.hpp>
      #include <generated/cli_help_auth.hpp>
      #include <generated/cli_help_stats.hpp>
      #include <generated/cli_help_uninstall.hpp>
      #include <generated/cli_help_migrate.hpp>
      #include <generated/cli_help_browse.hpp>
      #include <generated/cli_help_serve.hpp>
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
#include <filesystem>
#include <cstdlib>
#include <chrono>
#include <string>
#include <string_view>
#include <vector>
#include <optional>
#include <algorithm>

namespace yams::cli {
// NOTE: KG store is now managed as an instance member on YamsCLI (kgStore_)

YamsCLI::YamsCLI() {
    app_ = std::make_unique<CLI::App>("YAMS Document Management System", "yams");
    app_->set_version_flag("--version", YAMS_VERSION_STRING);
    
    // Global options
    // Use XDG_DATA_HOME if set, otherwise ~/.local/share
    const char* xdgDataHome = std::getenv("XDG_DATA_HOME");
    const char* homeEnv = std::getenv("HOME");
    std::filesystem::path defaultDataPath;
    if (xdgDataHome) {
        defaultDataPath = std::filesystem::path(xdgDataHome) / "yams";
    } else if (homeEnv) {
        defaultDataPath = std::filesystem::path(homeEnv) / ".local" / "share" / "yams";
    } else {
        defaultDataPath = std::filesystem::path(".") / "yams_data";
    }
    
    app_->add_option("--data-dir,--storage", dataPath_, "Data directory for storage")
        ->envname("YAMS_STORAGE")
        ->default_val(defaultDataPath);
    
    app_->add_flag("-v,--verbose", verbose_, "Enable verbose output");
    app_->add_flag("--json", jsonOutput_, "Output in JSON format");
}

YamsCLI::~YamsCLI() = default;

int YamsCLI::run(int argc, char* argv[]) {
    try {
        // Pre-scan for verbose help flags before CLI11 handles --help
        auto isFlag = [](const char* s) -> bool { return s && s[0] == '-'; };

        // Known subcommands (kept in sync with CommandRegistry)
        static const std::vector<std::string> kCommands = {
            "init","add","get","delete","list","search","config","auth","stats","uninstall","migrate","update","browse","serve"
        };

        auto hasArg = [&](std::string_view needle) {
            for (int i = 1; i < argc; ++i) {
                if (argv[i] == nullptr) continue;
                if (needle == argv[i]) return true;
            }
            return false;
        };

        // Determine potential subcommand (first non-flag token)
        std::string subcmd;
        if (argc > 1 && !isFlag(argv[1])) {
            subcmd = argv[1];
        }

        const bool helpAll    = hasArg("--help-all");
        const bool helpFlag   = hasArg("--help") || hasArg("-h");
        const bool verboseOpt = hasArg("--verbose"); // do not treat -v as help-verbose

#ifdef YAMS_HAVE_GENERATED_CLI_HELP
        auto printFullVerbose = [&]() {
            std::cout << yams::cli_help::VERBOSE << std::endl;
        };
        auto printCmdVerbose = [&](const std::string& cmd) {
            // Prefer per-command embedded constants when available; fallback to slicing from full blob
            const char* sectionPtr = nullptr;
            if (false) {}
            else if (cmd == "init") sectionPtr = yams::cli_help::CMD_INIT;
            else if (cmd == "add") sectionPtr = yams::cli_help::CMD_ADD;
            else if (cmd == "get") sectionPtr = yams::cli_help::CMD_GET;
            else if (cmd == "delete") sectionPtr = yams::cli_help::CMD_DELETE;
            else if (cmd == "list") sectionPtr = yams::cli_help::CMD_LIST;
            else if (cmd == "search") sectionPtr = yams::cli_help::CMD_SEARCH;
            else if (cmd == "config") sectionPtr = yams::cli_help::CMD_CONFIG;
            else if (cmd == "auth") sectionPtr = yams::cli_help::CMD_AUTH;
            else if (cmd == "stats") sectionPtr = yams::cli_help::CMD_STATS;
            else if (cmd == "uninstall") sectionPtr = yams::cli_help::CMD_UNINSTALL;
            else if (cmd == "migrate") sectionPtr = yams::cli_help::CMD_MIGRATE;
            else if (cmd == "browse") sectionPtr = yams::cli_help::CMD_BROWSE;
            else if (cmd == "serve") sectionPtr = yams::cli_help::CMD_SERVE;
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
            std::string_view section = (next == std::string_view::npos)
                ? all.substr(pos)
                : all.substr(pos, next - pos);
            std::cout << section << std::endl;
        };
#endif

        // Handle top-level verbose help
        if (helpAll || (helpFlag && verboseOpt && subcmd.empty())) {
#ifdef YAMS_HAVE_GENERATED_CLI_HELP
            printFullVerbose();
#else
            std::cout << "Verbose help not embedded. See docs/user_guide/cli.md or rebuild with docs.\n";
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
                std::cout << "Verbose help not embedded for command '" << subcmd << "'. See docs/user_guide/cli.md.\n";
#endif
                return 0;
            }
        }

        // Register all commands
        registerBuiltinCommands();
        
        // Parse command line
        app_->parse(argc, argv);
        
        // Storage initialization is performed lazily by commands via ensureStorageInitialized()
        
        return 0;
    } catch (const CLI::ParseError& e) {
        return app_->exit(e);
    } catch (const std::exception& e) {
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
        auto hint = std::string(" (tip: run 'yams init --storage \"") + dataPath_.string() + "' )";
        return Error{initResult.error().code, initResult.error().message + hint};
    }
    return Result<void>();
}

Result<void> YamsCLI::initializeStorage() {
    try {
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
            spdlog::error("Failed to initialize migration system: {}", initResult.error().message);
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
            spdlog::error("Failed to run database migrations: {}", migrateResult.error().message);
            return migrateResult;
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
        builder.withStoragePath(dataPath_ / "storage")
               .withChunkSize(DEFAULT_CHUNK_SIZE)
               .withCompression(true)
               .withCompressionType("zstd")
               .withCompressionLevel(3)
               .withDeduplication(true)
               .withIntegrityChecks(true);
        
        auto storeResult = builder.build();
        if (!storeResult) {
            return Error{storeResult.error().code, storeResult.error().message};
        }
        
        // Move the unique_ptr and convert to shared_ptr
        auto store = std::move(const_cast<std::unique_ptr<api::IContentStore>&>(storeResult.value()));
        contentStore_ = std::shared_ptr<api::IContentStore>(store.release());
        
        // Initialize search executor
        searchExecutor_ = std::make_shared<search::SearchExecutor>(database_, metadataRepo_);
        
        // Initialize Knowledge Graph store with defaults (local-first)
        {
            metadata::KnowledgeGraphStoreConfig kgCfg;
            kgCfg.enable_alias_fts = true;  // use FTS5 if available, fallback is automatic
            kgCfg.enable_wal = true;        // align with DB mode

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
    command->registerCommand(*app_, this);
    commands_.push_back(std::move(command));
}

} // namespace yams::cli