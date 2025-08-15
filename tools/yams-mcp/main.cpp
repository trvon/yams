#include <yams/mcp/mcp_server.h>
#include <yams/api/content_store.h>
#include <yams/api/content_store_builder.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/search/search_executor.h>
#include <CLI/CLI.hpp>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <csignal>
#include <atomic>
#include <thread>
#include <filesystem>

namespace fs = std::filesystem;

std::atomic<bool> g_running{true};

void signalHandler(int signal) {
    spdlog::info("Received signal {}, shutting down...", signal);
    g_running = false;
}

int main(int argc, char* argv[]) {
    CLI::App app{"YAMS MCP Server - Model Context Protocol server for YAMS"};

    // Server options
    std::string storage_path = std::getenv("YAMS_STORAGE") ?
                               std::getenv("YAMS_STORAGE") :
                               fs::path(std::getenv("HOME")) / "yams";
    std::string log_level = "info";
    std::string log_file;
    bool daemon_mode = false;

    // CLI options
    app.add_option("-s,--storage", storage_path, "Storage directory path")
       ->envname("YAMS_STORAGE");
    app.add_option("-l,--log-level", log_level, "Log level (trace, debug, info, warn, error)")
       ->default_val("info");
    app.add_option("--log-file", log_file, "Log file path (optional)");
    app.add_flag("-d,--daemon", daemon_mode, "Run as daemon");

    // Parse command line
    CLI11_PARSE(app, argc, argv);

    // Setup logging
    try {
        std::vector<spdlog::sink_ptr> sinks;

        // Console sink - use stderr to avoid interfering with STDIO MCP transport
        if (!daemon_mode) {
            auto console_sink = std::make_shared<spdlog::sinks::stderr_color_sink_mt>();
            sinks.push_back(console_sink);
        }

        // File sink if specified
        if (!log_file.empty()) {
            auto file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
                log_file, 10 * 1024 * 1024, 3); // 10MB, 3 files
            sinks.push_back(file_sink);
        }

        // Create logger
        auto logger = std::make_shared<spdlog::logger>("yams-mcp", sinks.begin(), sinks.end());
        spdlog::set_default_logger(logger);

        // Set log level
        if (log_level == "trace") spdlog::set_level(spdlog::level::trace);
        else if (log_level == "debug") spdlog::set_level(spdlog::level::debug);
        else if (log_level == "info") spdlog::set_level(spdlog::level::info);
        else if (log_level == "warn") spdlog::set_level(spdlog::level::warn);
        else if (log_level == "error") spdlog::set_level(spdlog::level::err);

        spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%l] [%n] %v");
    } catch (const std::exception& e) {
        std::cerr << "Failed to setup logging: " << e.what() << std::endl;
        return 1;
    }

    // Log startup info
    spdlog::info("YAMS MCP Server v{}.{}.{}", 0, 0, 2);
    spdlog::info("Storage path: {}", storage_path);
    spdlog::info("Transport: STDIO");

    // Check storage directory
    if (!fs::exists(storage_path)) {
        spdlog::error("Storage path does not exist: {}", storage_path);
        spdlog::info("Run 'yams init --storage {}' to initialize storage", storage_path);
        return 1;
    }

    // Setup signal handlers
    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);

    try {
        // Initialize the core components with actual YAMS storage
        spdlog::info("Initializing YAMS components from storage: {}", storage_path);
        
        // Initialize ContentStore
        auto storeResult = yams::api::createContentStore(storage_path);
        if (!storeResult) {
            spdlog::error("Failed to initialize content store at {}: {}", 
                         storage_path, storeResult.error().message);
            return 1;
        }
        // Move the unique_ptr out of the Result and convert to shared_ptr
        auto storeUnique = std::move(storeResult).value();
        std::shared_ptr<yams::api::IContentStore> store = std::move(storeUnique);
        
        // Initialize metadata database and connection pool
        fs::path dbPath = fs::path(storage_path) / "metadata.db";
        yams::metadata::ConnectionPoolConfig poolConfig;
        poolConfig.maxConnections = 5;  // Smaller pool for MCP server
        poolConfig.minConnections = 1;
        poolConfig.connectTimeout = std::chrono::seconds(10);
        
        auto connectionPool = std::make_shared<yams::metadata::ConnectionPool>(dbPath.string(), poolConfig);
        auto poolResult = connectionPool->initialize();
        if (!poolResult) {
            spdlog::error("Failed to initialize database connection pool: {}", poolResult.error().message);
            return 1;
        }
        
        // Initialize metadata repository
        auto metadataRepo = std::make_shared<yams::metadata::MetadataRepository>(*connectionPool);
        
        // Initialize database for search executor
        auto database = std::make_shared<yams::metadata::Database>();
        auto dbOpenResult = database->open(dbPath.string());
        if (!dbOpenResult) {
            spdlog::error("Failed to open database: {}", dbOpenResult.error().message);
            return 1;
        }
        
        // Initialize search executor
        auto searchExecutor = std::make_shared<yams::search::SearchExecutor>(database, metadataRepo);

        // MCP servers for Claude Desktop must use STDIO transport
        // WebSocket transport is optional and not needed for basic functionality
        std::unique_ptr<yams::mcp::ITransport> transport = std::make_unique<yams::mcp::StdioTransport>();

        // Create MCP server
        auto server = std::make_unique<yams::mcp::MCPServer>(
            store,
            searchExecutor,
            metadataRepo,
            nullptr,                      // hybrid engine (builder can be added later)
            std::move(transport));        // transport (STDIO only)

        // Start server in background thread
        std::thread server_thread([&server]() {
            server->start();
        });

        spdlog::info("MCP server started successfully");
        spdlog::info("Press Ctrl+C to stop");

        // Main loop
        while (g_running) {
            std::this_thread::sleep_for(std::chrono::seconds(1));

            // Could add periodic tasks here
            // e.g., stats logging, health checks
        }

        // Shutdown
        spdlog::info("Shutting down MCP server...");
        server->stop();

        if (server_thread.joinable()) {
            server_thread.join();
        }

        spdlog::info("MCP server stopped");

    } catch (const std::exception& e) {
        spdlog::error("Fatal error: {}", e.what());
        return 1;
    }

    return 0;
}
