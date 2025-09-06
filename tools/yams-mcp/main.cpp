#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include <atomic>
#include <csignal>
#include <cstdlib>
#include <thread>

#include <CLI/CLI.hpp>

#include <yams/mcp/mcp_server.h>

std::atomic<bool> g_running{true};

void signalHandler(int signal) {
    spdlog::info("Received signal {}, shutting down...", signal);
    g_running = false;
}

int main(int argc, char* argv[]) {
    CLI::App app{"YAMS MCP Server - Model Context Protocol server for YAMS"};

    std::string log_level = "info";
    std::string log_file;
    bool daemon_mode = false;
    std::string list_mode = "auto";
    std::string grep_mode = "auto";
    std::string retrieval_mode = "auto";

    app.add_option("-l,--log-level", log_level, "Log level (trace, debug, info, warn, error)")
        ->default_val("info");
    app.add_option("--log-file", log_file, "Log file path (optional)");
    app.add_flag("-d,--daemon", daemon_mode, "Run as daemon");
    app.add_option("--list-mode", list_mode, "Hot/cold mode for list: hot_only|cold_only|auto")
        ->check(CLI::IsMember({"hot_only", "hot", "cold_only", "cold", "auto"}))
        ->default_val("auto");
    app.add_option("--grep-mode", grep_mode, "Hot/cold mode for grep: hot_only|cold_only|auto")
        ->check(CLI::IsMember({"hot_only", "hot", "cold_only", "cold", "auto"}))
        ->default_val("auto");
    app.add_option("--retrieval-mode", retrieval_mode,
                   "Hot/cold mode for retrieval: hot_only|cold_only|auto")
        ->check(CLI::IsMember({"hot_only", "hot", "cold_only", "cold", "auto"}))
        ->default_val("auto");
    CLI11_PARSE(app, argc, argv);

    try {
        std::vector<spdlog::sink_ptr> sinks;
        if (!daemon_mode) {
            auto console_sink = std::make_shared<spdlog::sinks::stderr_color_sink_mt>();
            sinks.push_back(console_sink);
        }
        if (!log_file.empty()) {
            auto file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
                log_file, 10 * 1024 * 1024, 3);
            sinks.push_back(file_sink);
        }

        auto logger = std::make_shared<spdlog::logger>("yams-mcp", sinks.begin(), sinks.end());
        spdlog::set_default_logger(logger);

        if (log_level == "trace")
            spdlog::set_level(spdlog::level::trace);
        else if (log_level == "debug")
            spdlog::set_level(spdlog::level::debug);
        else if (log_level == "info")
            spdlog::set_level(spdlog::level::info);
        else if (log_level == "warn")
            spdlog::set_level(spdlog::level::warn);
        else if (log_level == "error")
            spdlog::set_level(spdlog::level::err);

        spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%l] [%n] %v");
        // Apply hot/cold modes for list/grep/retrieval so MCP uses hot paths like CLI
        if (!list_mode.empty()) {
            setenv("YAMS_LIST_MODE", list_mode.c_str(), 1);
        }
        if (!grep_mode.empty()) {
            setenv("YAMS_GREP_MODE", grep_mode.c_str(), 1);
        }
        if (!retrieval_mode.empty()) {
            setenv("YAMS_RETRIEVAL_MODE", retrieval_mode.c_str(), 1);
        }
    } catch (const std::exception& e) {
        std::cerr << "Failed to setup logging: " << e.what() << std::endl;
        return 1;
    }

    spdlog::info("YAMS MCP Server v{}.{}.{}", 0, 0, 2);
    spdlog::info("Transport: STDIO");

    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);

    try {
        std::unique_ptr<yams::mcp::ITransport> transport =
            std::make_unique<yams::mcp::StdioTransport>();

        auto server = std::make_unique<yams::mcp::MCPServer>(std::move(transport), &g_running);

        std::thread server_thread([&server]() { server->start(); });

        spdlog::info("MCP server started successfully");
        spdlog::info("Press Ctrl+C to stop");

        while (g_running) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

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
