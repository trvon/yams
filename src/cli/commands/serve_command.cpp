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
#ifndef _WIN32
#include <unistd.h>
#else
#include <io.h>
#define isatty _isatty
#define STDIN_FILENO 0
#endif
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <yams/cli/command.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/cli/yams_cli.h>
#include <yams/mcp/mcp_server.h>
#include <yams/search/search_engine_builder.h>
#include <yams/vector/vector_index_manager.h>
#ifndef _WIN32
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#endif

namespace yams::cli {

// MCPServer::start() interprets externalShutdown_ with "running" semantics:
// the main loop continues while *externalShutdown_ is true, and exits when false.
// So this flag must start true and be set to false on shutdown.
static std::atomic<bool> g_running{true};

class ServeCommand : public ICommand {
public:
    std::string getName() const override { return "serve"; }

    std::string getDescription() const override { return "Start MCP server"; }

    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;

        auto* cmd = app.add_subcommand("serve", getDescription());
        cmd->add_option("--daemon-socket", daemonSocket_, "Override daemon socket path")
            ->envname("YAMS_DAEMON_SOCKET");

        // Backward compatible (now redundant) flag: default is already quiet.
        cmd->add_flag(
            "--quiet", quietFlag_,
            "Deprecated / no-op: server runs quiet by default (use --verbose to show banner)");

        // New flag enabling banner + info-level logging
        cmd->add_flag(
            "--verbose", verbose_,
            "Show startup banner and enable info-level logging (overrides default quiet mode)");

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

            // When enabled, show handshake trace during integration debugging.
            // Note: logs go to stderr (safe for MCP stdio framing).
            if (const char* ht = std::getenv("YAMS_MCP_HANDSHAKE_TRACE")) {
                if (ht && *ht && ht[0] != '0') {
                    spdlog::set_level(spdlog::level::trace);
                    spdlog::flush_on(spdlog::level::trace);
                    spdlog::info("YAMS_MCP_HANDSHAKE_TRACE enabled");
                }
            }

            // Signal handlers
#ifdef _WIN32
            auto handler = [](int sig) {
                g_running = false;
                std::cerr << "\n[Signal " << sig << " received, shutting down...]\n";
                if (std::cin.fail())
                    std::cin.clear();
            };
            if (signal(SIGINT, handler) == SIG_ERR)
                spdlog::warn("Failed to install SIGINT handler");
            if (signal(SIGTERM, handler) == SIG_ERR)
                spdlog::warn("Failed to install SIGTERM handler");
#else
            struct sigaction sa = {};
            sa.sa_handler = [](int sig) {
                g_running = false;
                std::cerr << "\n[Signal " << sig << " received, shutting down...]\n";
                if (std::cin.fail())
                    std::cin.clear();
            };
            sa.sa_flags = 0;
            if (sigaction(SIGINT, &sa, nullptr) == -1)
                spdlog::warn("Failed to install SIGINT handler");
            if (sigaction(SIGTERM, &sa, nullptr) == -1)
                spdlog::warn("Failed to install SIGTERM handler");
#endif

            // Do not enforce storage initialization here. MCPServer resolves dataDir and connects
            // to the daemon on its own, and many tools don't require a pre-initialized store.

            effectiveQuiet_ = quiet; // store for server methods

            return runStdioServer();

        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string("Server error: ") + e.what()};
        }
    }

private:
    Result<void> runStdioServer() {
        // Pre-resolve daemon socket (non-fatal) by writing a transient override file if needed
        if (!daemonSocket_.empty()) {
            // Instead of env mutation, prefer a config-first override by creating a lightweight
            // ephemeral config snippet (not persisted) if future logic supports it. For now we
            // just log; DaemonClient will still auto-resolve if unset.
            spdlog::debug("serve: requested daemon socket override {} (config-based resolution)",
                          daemonSocket_);
        }
        if (!effectiveQuiet_) {
            std::cerr << "\n=== YAMS MCP Server (stdio) ===\n";
            std::cerr << "Transport: stdio (JSON-RPC over stdin/stdout)\n";
            std::cerr << "Status: Waiting for client connection...\n";
            std::cerr << "Press Ctrl+C to stop the server\n\n";
        }

        auto transport = std::make_unique<mcp::StdioTransport>();
        auto server =
            std::make_shared<mcp::MCPServer>(std::move(transport), &g_running, daemonSocket_);
        server->start();
        spdlog::info("MCP stdio server stopped");
        return {};
    }

    YamsCLI* cli_ = nullptr;

    // Flags / options
    bool quietFlag_ = true; // kept for backward compatibility (default quiet)
    bool verbose_ = false;  // new flag: enables banner & info logging

    // Derived effective quiet state after env + flags
    bool effectiveQuiet_ = true;
    std::string daemonSocket_;
};

// Factory function
std::unique_ptr<ICommand> createServeCommand() {
    return std::make_unique<ServeCommand>();
}

} // namespace yams::cli
