#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <future>
#include <iostream>
#include <thread>
#include <unistd.h>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>
#include <yams/mcp/http_server.h>
#include <yams/mcp/mcp_server.h>
#include <yams/search/search_engine_builder.h>
#include <yams/vector/vector_index_manager.h>
#ifndef _WIN32
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#endif

namespace yams::cli {

static std::atomic<bool> g_shutdown{false};

class ServeCommand : public ICommand {
public:
    std::string getName() const override { return "serve"; }

    std::string getDescription() const override { return "Start MCP server"; }

    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;

        auto* cmd = app.add_subcommand("serve", getDescription());

        // Backward compatible (now redundant) flag: default is already quiet.
        cmd->add_flag(
            "--quiet", quietFlag_,
            "Deprecated / no-op: server runs quiet by default (use --verbose to show banner)");

        // New flag enabling banner + info-level logging
        cmd->add_flag(
            "--verbose", verbose_,
            "Show startup banner and enable info-level logging (overrides default quiet mode)");

        cmd->add_flag("--http", http_, "Use HTTP+SSE transport instead of stdio");
        cmd->add_option("--host", http_host_, "HTTP host to bind to (default: 127.0.0.1)")
            ->envname("YAMS_MCP_HTTP_HOST");
        cmd->add_option("--port", http_port_, "HTTP port to bind to (default: 8757)")
            ->envname("YAMS_MCP_HTTP_PORT");

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
            // Dedicated stderr logger to avoid contaminating stdout protocol stream
            auto stderr_sink = std::make_shared<spdlog::sinks::stderr_color_sink_mt>();
            auto logger = std::make_shared<spdlog::logger>("stderr", stderr_sink);
            spdlog::set_default_logger(logger);

            // Prevent abrupt termination on broken pipe when client disconnects early
#ifndef _WIN32
            std::signal(SIGPIPE, SIG_IGN);
#endif
            // Optional: install terminate handler to log uncaught exceptions.
            // Disabled by default for MCP stdio to avoid noisy messages when the host closes stdin.
            if (const char* tlog = std::getenv("YAMS_MCP_LOG_TERMINATE");
                tlog && *tlog && tlog[0] != '0') {
                std::set_terminate([]() noexcept {
                    try {
                        if (auto ep = std::current_exception()) {
                            try {
                                std::rethrow_exception(ep);
                            } catch (const std::exception& e) {
                                spdlog::critical("std::terminate: uncaught exception: {}",
                                                 e.what());
                            } catch (...) {
                                spdlog::critical("std::terminate: unknown uncaught exception");
                            }
                        } else {
                            spdlog::critical("std::terminate called without active exception");
                        }
                    } catch (...) {
                    }
                    std::_Exit(1);
                });
            }

            // Allow multiple instances. For HTTP mode, binding conflicts will be reported by the
            // OS (EADDRINUSE). For stdio, instances are independent per client.

            const bool interactive = isatty(STDIN_FILENO);

            // Environment override: YAMS_MCP_QUIET=0 => force verbose, =1 => force quiet
            bool envOverride = false;
            bool envQuietValue = true; // default if override sets quiet
            if (const char* q = std::getenv("YAMS_MCP_QUIET")) {
                if (q[0] == '0') {
                    envOverride = true;
                    envQuietValue = false;
                } else if (q[0] == '1') {
                    envOverride = true;
                    envQuietValue = true;
                }
            }

            // Effective quiet logic:
            // Priority: explicit env override > --verbose flag > default quiet
            bool quiet = envOverride ? envQuietValue : (!verbose_);

            // Logging level: quiet -> warn, verbose -> info (unless SPLOG_LEVEL overrides)
            if (!interactive || quiet) {
                logger->set_level(spdlog::level::warn);
                spdlog::set_level(spdlog::level::warn);
                spdlog::flush_on(spdlog::level::warn);
            } else {
                logger->set_level(spdlog::level::info);
                spdlog::set_level(spdlog::level::info);
            }

            // Env manual override of spdlog level still honored
            if (const char* env_l = std::getenv("SPLOG_LEVEL")) {
                std::string level_str = env_l;
                std::transform(level_str.begin(), level_str.end(), level_str.begin(),
                               [](unsigned char c) { return std::tolower(c); });
                if (level_str == "trace")
                    spdlog::set_level(spdlog::level::trace);
                else if (level_str == "debug")
                    spdlog::set_level(spdlog::level::debug);
                else if (level_str == "info")
                    spdlog::set_level(spdlog::level::info);
                else if (level_str == "warn" || level_str == "warning")
                    spdlog::set_level(spdlog::level::warn);
                else if (level_str == "error")
                    spdlog::set_level(spdlog::level::err);
                else if (level_str == "critical")
                    spdlog::set_level(spdlog::level::critical);
            }

            // Signal handlers
            struct sigaction sa;
            std::memset(&sa, 0, sizeof(sa));
            sa.sa_handler = [](int sig) {
                g_shutdown = true;
                std::cerr << "\n[Signal " << sig << " received, shutting down...]\n";
                if (std::cin.fail())
                    std::cin.clear();
            };
            sa.sa_flags = 0;
            if (sigaction(SIGINT, &sa, nullptr) == -1)
                spdlog::warn("Failed to install SIGINT handler");
            if (sigaction(SIGTERM, &sa, nullptr) == -1)
                spdlog::warn("Failed to install SIGTERM handler");

            // Do not enforce storage initialization here. MCPServer resolves dataDir and connects
            // to the daemon on its own, and many tools don't require a pre-initialized store.

            effectiveQuiet_ = quiet; // store for server methods

            if (http_) {
                return runHttpServer();
            } else {
                return runStdioServer();
            }

        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string("Server error: ") + e.what()};
        }
    }

private:
    Result<void> runStdioServer() {
        if (!effectiveQuiet_) {
            std::cerr << "\n=== YAMS MCP Server (stdio) ===\n";
            std::cerr << "Transport: stdio (JSON-RPC over stdin/stdout)\n";
            std::cerr << "Status: Waiting for client connection...\n";
            std::cerr << "Press Ctrl+C to stop the server\n\n";
        }

        auto transport = std::make_unique<mcp::StdioTransport>();
        auto server = std::make_unique<mcp::MCPServer>(std::move(transport), &g_shutdown);
        server->start();
        spdlog::info("MCP stdio server stopped");
        return {};
    }

    Result<void> runHttpServer() {
        if (!effectiveQuiet_) {
            std::cerr << "\n=== YAMS MCP Server (HTTP) ===\n";
            std::cerr << "Transport: HTTP+SSE\n";
            std::cerr << "Listening on: " << http_host_ << ":" << http_port_ << "\n";
            std::cerr << "Press Ctrl+C to stop the server\n\n";
        }

        auto server = std::make_shared<mcp::MCPServer>(nullptr, &g_shutdown);

        mcp::HttpMcpServer::Config cfg;
        cfg.bindAddress = http_host_;
        cfg.bindPort = http_port_;

        boost::asio::io_context io;
        mcp::HttpMcpServer http(io, server, cfg);

        http.run();

        spdlog::info("MCP HTTP server stopped");
        return {};
    }

    YamsCLI* cli_ = nullptr;

    // Flags / options
    bool quietFlag_ = true; // kept for backward compatibility (default quiet)
    bool verbose_ = false;  // new flag: enables banner & info logging
    bool http_ = false;
    std::string http_host_ = "127.0.0.1";
    uint16_t http_port_ = 8757;

    // Derived effective quiet state after env + flags
    bool effectiveQuiet_ = true;
};

// Factory function
std::unique_ptr<ICommand> createServeCommand() {
    return std::make_unique<ServeCommand>();
}

} // namespace yams::cli
