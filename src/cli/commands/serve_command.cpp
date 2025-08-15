#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>
#include <yams/mcp/mcp_server.h>
#include <yams/search/search_engine_builder.h>
#include <yams/vector/vector_index_manager.h>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <iostream>
#include <csignal>
#include <atomic>
#include <cstring>
#include <unistd.h>

namespace yams::cli {

static std::atomic<bool> g_shutdown{false};

class ServeCommand : public ICommand {
public:
    std::string getName() const override { return "serve"; }
    
    std::string getDescription() const override { 
        return "Start MCP server (stdio transport only)";
    }
    
    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;
        
        auto* cmd = app.add_subcommand("serve", getDescription());
        
        // MCP server now only supports stdio transport
        // WebSocket support has been removed to reduce dependencies
        
        cmd->callback([this]() { 
            auto result = execute();
            if (!result) {
                spdlog::error("Command failed: {}", result.error().message);
                throw CLI::RuntimeError(1);
            }
        });
    }
    
    Result<void> execute() override {
        try {
            // Redirect logging to stderr to avoid protocol conflicts with stdio transport
            // Create a stderr sink for spdlog
            auto stderr_sink = std::make_shared<spdlog::sinks::stderr_color_sink_mt>();
            auto logger = std::make_shared<spdlog::logger>("stderr", stderr_sink);
            spdlog::set_default_logger(logger);
            
            // Print startup message to stderr for user feedback
            std::cerr << "\n=== YAMS MCP Server ===" << std::endl;
            std::cerr << "Transport: stdio (JSON-RPC over stdin/stdout)" << std::endl;
            std::cerr << "Status: Waiting for client connection..." << std::endl;
            std::cerr << "Press Ctrl+C to stop the server" << std::endl;
            std::cerr << std::endl;
            
            // Check if we're in an interactive terminal
            if (isatty(STDIN_FILENO)) {
                std::cerr << "Note: MCP stdio transport expects JSON-RPC messages on stdin." << std::endl;
                std::cerr << std::endl;
            }
            
            // Set up signal handler for graceful shutdown using sigaction
            // This allows interrupting blocking I/O operations
            struct sigaction sa;
            std::memset(&sa, 0, sizeof(sa));
            
            // Use a lambda wrapper to set the shutdown flag
            sa.sa_handler = [](int sig) { 
                g_shutdown = true;
                // Print shutdown message to stderr
                std::cerr << "\n[Signal " << sig << " received, shutting down...]" << std::endl;
                // Clear any error state on stdin to allow getline to return
                if (std::cin.fail()) {
                    std::cin.clear();
                }
            };
            
            // Don't set SA_RESTART to allow interrupting blocking I/O
            sa.sa_flags = 0;
            
            // Install the handler for SIGINT and SIGTERM
            if (sigaction(SIGINT, &sa, nullptr) == -1) {
                spdlog::warn("Failed to install SIGINT handler");
            }
            if (sigaction(SIGTERM, &sa, nullptr) == -1) {
                spdlog::warn("Failed to install SIGTERM handler");
            }
            
            auto ensured = cli_->ensureStorageInitialized();
            if (!ensured) {
                return ensured;
            }
            
            auto store = cli_->getContentStore();
            auto searchExecutor = cli_->getSearchExecutor();
            auto metadataRepo = cli_->getMetadataRepository();

            // Prepare embedded HybridSearchEngine (not yet used; kept for future MCP integration)
            std::shared_ptr<yams::search::HybridSearchEngine> hybridEngine;
            try {
                auto vecMgr = std::make_shared<yams::vector::VectorIndexManager>();
                yams::search::SearchEngineBuilder builder;
                builder.withVectorIndex(vecMgr)
                       .withMetadataRepo(metadataRepo)
                       .withKGStore(cli_->getKnowledgeGraphStore());

                auto buildRes = builder.buildEmbedded(yams::search::SearchEngineBuilder::BuildOptions::makeDefault());
                if (buildRes) {
                    hybridEngine = buildRes.value();
                    spdlog::info("HybridSearchEngine initialized (embedded, KG {}abled)",
                                 hybridEngine->getConfig().enable_kg ? "en" : "dis");
                } else {
                    spdlog::warn("HybridSearchEngine initialization failed: {}", buildRes.error().message);
                }
            } catch (const std::exception& e) {
                spdlog::warn("HybridSearchEngine bring-up error (ignored): {}", e.what());
            }
            
            if (!store || !searchExecutor) {
                return Error{ErrorCode::NotInitialized, "Storage not initialized"};
            }
            
            // Create stdio transport (only supported transport)
            std::unique_ptr<mcp::ITransport> transport = std::make_unique<mcp::StdioTransport>();
            spdlog::info("MCP server initialized with stdio transport");
            
            // Create and start MCP server with shutdown flag
            auto server = std::make_unique<mcp::MCPServer>(
                store, searchExecutor, metadataRepo, hybridEngine, std::move(transport), &g_shutdown);
            
            // Run server until shutdown signal
            server->start();
            
            while (!g_shutdown && server->isRunning()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
            
            server->stop();
            spdlog::info("MCP server stopped");
            
            return Result<void>();
            
        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string("Server error: ") + e.what()};
        }
    }
    
private:
    YamsCLI* cli_ = nullptr;
};

// Factory function
std::unique_ptr<ICommand> createServeCommand() {
    return std::make_unique<ServeCommand>();
}

} // namespace yams::cli