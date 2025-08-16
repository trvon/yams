#include <spdlog/spdlog.h>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <yams/api/content_store_builder.h>
#include <yams/cli/command_registry.h>
#include <yams/cli/yams_cli.h>
#include <yams/metadata/database.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>
#include <yams/search/hybrid_search_factory.h>
#include <yams/vector/embedding_service.h>
#include <yams/vector/vector_database.h>
#include <yams/vector/vector_index_manager.h>
#include <yams/version.hpp>

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
        __has_include(<generated/cli_help_browse.hpp>) && \
        __has_include(<generated/cli_help_serve.hpp>)
#define YAMS_HAVE_GENERATED_CLI_HELP 1
#include <generated/cli_help.hpp>
#include <generated/cli_help_add.hpp>
#include <generated/cli_help_auth.hpp>
#include <generated/cli_help_browse.hpp>
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
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace yams::cli {
// NOTE: KG store is now managed as an instance member on YamsCLI (kgStore_)

YamsCLI::YamsCLI() {
    // Default: suppress info/debug unless --verbose is used
    spdlog::set_level(spdlog::level::warn);

    app_ = std::make_unique<CLI::App>("YAMS", "yams");
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

YamsCLI::~YamsCLI() {
    // No cleanup needed for simplified embedding service
}

int YamsCLI::run(int argc, char* argv[]) {
    try {
        // Pre-scan for verbose help flags before CLI11 handles --help
        auto isFlag = [](const char* s) -> bool { return s && s[0] == '-'; };

        // Known subcommands (kept in sync with CommandRegistry)
        static const std::vector<std::string> kCommands = {
            "init",   "add",        "get",         "delete",    "list",    "search",
            "config", "auth",       "stats",       "uninstall", "migrate", "update",
#ifdef YAMS_ENABLE_TUI
            "browse",
#endif
            "serve",  "completion", "repair-mime", "model"};

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
#ifdef YAMS_ENABLE_TUI
            else if (cmd == "browse")
                sectionPtr = yams::cli_help::CMD_BROWSE;
#endif
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

        // Parse command line
        app_->parse(argc, argv);

        // Apply log level after parsing flags (avoid CLI11 Option::callback dependency)
        if (verbose_) {
            spdlog::set_level(spdlog::level::debug);
        } else {
            spdlog::set_level(spdlog::level::warn);
        }

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

            // First, check if vectors.db exists and has vectors to detect dimension
            fs::path vectorDbPath = dataPath_ / "vectors.db";
            if (fs::exists(vectorDbPath)) {
                try {
                    vector::VectorDatabaseConfig vdbConfig;
                    vdbConfig.database_path = vectorDbPath.string();
                    vdbConfig.embedding_dim = 384; // Temporary for initialization

                    auto vectorDb = std::make_unique<vector::VectorDatabase>(vdbConfig);
                    if (vectorDb->initialize() && vectorDb->tableExists()) {
                        // Try to get dimension from existing vectors
                        auto vectorCount = vectorDb->getVectorCount();
                        if (vectorCount > 0) {
                            // In production, we'd query the actual dimension from the DB
                            // For now, detect based on model availability
                            if (verbose_) {
                                spdlog::info("Found existing vector database with {} vectors",
                                             vectorCount);
                            }
                        }
                    }
                } catch (const std::exception& e) {
                    spdlog::debug("Could not probe vector database: {}", e.what());
                }
            }

            // Second, detect dimension from available models
            const char* home = std::getenv("HOME");
            if (home) {
                fs::path modelsPath = fs::path(home) / ".yams" / "models";
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

            // Initialize vector database proactively to avoid "Not found" messages
            try {
                vector::VectorDatabaseConfig vdbConfig;
                vdbConfig.database_path = (dataPath_ / "vectors.db").string();
                vdbConfig.embedding_dim = vectorDimension;

                auto vectorDb = std::make_unique<vector::VectorDatabase>(vdbConfig);
                if (vectorDb->initialize()) {
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

    // 3. Check common installation paths
    searchPaths.push_back("/usr/local/share/yams/data/magic_numbers.json");
    searchPaths.push_back("/usr/share/yams/data/magic_numbers.json");
    searchPaths.push_back("/opt/yams/share/data/magic_numbers.json");

    // 4. Check relative to current working directory (for development)
    searchPaths.push_back("data/magic_numbers.json");
    searchPaths.push_back("../data/magic_numbers.json");
    searchPaths.push_back("../../data/magic_numbers.json");
    searchPaths.push_back("../../../data/magic_numbers.json");
    searchPaths.push_back("../../../../data/magic_numbers.json");

    // 5. Check in home directory
    if (const char* home = std::getenv("HOME")) {
        searchPaths.push_back(fs::path(home) / ".local" / "share" / "yams" / "data" /
                              "magic_numbers.json");
    }

    // Find the first existing file
    for (const auto& path : searchPaths) {
        if (fs::exists(path) && fs::is_regular_file(path)) {
            spdlog::debug("Found magic_numbers.json at: {}", path.string());
            return path;
        }
    }

    spdlog::warn("magic_numbers.json not found in any standard location");
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
    const char* xdgConfigHome = std::getenv("XDG_CONFIG_HOME");
    const char* homeEnv = std::getenv("HOME");

    fs::path configHome;
    if (xdgConfigHome) {
        configHome = fs::path(xdgConfigHome);
    } else if (homeEnv) {
        configHome = fs::path(homeEnv) / ".config";
    } else {
        return fs::path("~/.config") / "yams" / "config.toml";
    }

    return configHome / "yams" / "config.toml";
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

} // namespace yams::cli
