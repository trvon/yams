#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>
#include <yams/mcp/mcp_server.h>
#include <spdlog/spdlog.h>
#include <iostream>
#include <csignal>
#include <atomic>

namespace yams::cli {

static std::atomic<bool> g_shutdown{false};

class ServeCommand : public ICommand {
public:
    std::string getName() const override { return "serve"; }
    
    std::string getDescription() const override { 
        return "Start MCP server (transports: stdio, websocket)";
    }
    
    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;
        
        auto* cmd = app.add_subcommand("serve", getDescription());
        
        cmd->add_option("-t,--transport", transport_, "Transport: stdio | websocket")
            ->default_val("stdio");
        
        cmd->add_option("-p,--port", port_, "WebSocket port (when --transport=websocket)")
            ->default_val(8080);
        cmd->add_option("--host", host_, "WebSocket host (when --transport=websocket)")
            ->default_val("127.0.0.1");
        cmd->add_option("--path", wsPath_, "WebSocket path (default /mcp)")
            ->default_val("/mcp");
        cmd->add_flag("--ssl", useSSL_, "Use TLS for WebSocket (wss) (when --transport=websocket)");
        
        cmd->callback([this]() { 
            auto result = execute();
            if (!result) {
                spdlog::error("Command failed: {}", result.error().message);
                exit(1);
            }
        });
    }
    
    Result<void> execute() override {
        try {
            // Set up signal handler for graceful shutdown
            std::signal(SIGINT, [](int) { g_shutdown = true; });
            std::signal(SIGTERM, [](int) { g_shutdown = true; });
            
            auto ensured = cli_->ensureStorageInitialized();
            if (!ensured) {
                return ensured;
            }
            
            auto store = cli_->getContentStore();
            auto searchExecutor = cli_->getSearchExecutor();
            
            if (!store || !searchExecutor) {
                return Error{ErrorCode::NotInitialized, "Storage not initialized"};
            }
            
            // Create transport based on type
            std::unique_ptr<mcp::ITransport> transport;
            if (transport_ == "stdio") {
                transport = std::make_unique<mcp::StdioTransport>();
                spdlog::info("Starting MCP server with stdio transport");
            } else if (transport_ == "websocket" || transport_ == "ws") {
                mcp::WebSocketTransport::Config cfg;
                cfg.host = host_;
                cfg.port = static_cast<uint16_t>(port_);
                cfg.path = wsPath_;
                cfg.useSSL = useSSL_;
                auto wsTransport = std::make_unique<mcp::WebSocketTransport>(cfg);
                auto* ws = dynamic_cast<mcp::WebSocketTransport*>(wsTransport.get());
                if (!ws || !ws->connect()) {
                    return Error{ErrorCode::InternalError, "Failed to open websocket transport"};
                }
                spdlog::info("Starting MCP server with websocket transport on {}://{}:{}{}",
                             (useSSL_ ? "wss" : "ws"), host_, port_, wsPath_);
                transport = std::move(wsTransport);
            } else {
                return Error{ErrorCode::NotImplemented, 
                             "Transport type not implemented: " + transport_};
            }
            
            // Create and start MCP server
            auto server = std::make_unique<mcp::MCPServer>(
                store, searchExecutor, std::move(transport));
            
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
    std::string transport_ = "stdio";
    int port_ = 8080;
    std::string host_ = "127.0.0.1";
    std::string wsPath_ = "/mcp";
    bool useSSL_ = false;
};

// Factory function
std::unique_ptr<ICommand> createServeCommand() {
    return std::make_unique<ServeCommand>();
}

} // namespace yams::cli