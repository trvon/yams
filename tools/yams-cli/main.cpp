#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include <spdlog/spdlog.h>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <yams/cli/yams_cli.h>
#include <yams/platform/windows_init.h>
#include <yams/version.hpp>

#if __has_include(<version_generated.h>)
#include <version_generated.h>
#endif

namespace {

bool cli_perf_trace_enabled() {
    const char* raw = std::getenv("YAMS_CLI_PERF_TRACE");
    if (raw == nullptr || *raw == '\0') {
        return false;
    }
    std::string value(raw);
    for (auto& ch : value) {
        ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    }
    return value == "1" || value == "true" || value == "on" || value == "yes";
}

bool env_truthy(const char* raw) {
    if (raw == nullptr || *raw == '\0') {
        return false;
    }
    std::string value(raw);
    for (auto& ch : value) {
        ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    }
    return value == "1" || value == "true" || value == "on" || value == "yes";
}

void cli_perf_trace(std::string_view stage, std::chrono::microseconds elapsed,
                    std::string_view note = {}) {
    if (!cli_perf_trace_enabled()) {
        return;
    }
    if (note.empty()) {
        std::fprintf(stderr, "[yams-cli-perf] stage=%.*s elapsed_us=%lld\n",
                     static_cast<int>(stage.size()), stage.data(),
                     static_cast<long long>(elapsed.count()));
    } else {
        std::fprintf(stderr, "[yams-cli-perf] stage=%.*s elapsed_us=%lld note=%.*s\n",
                     static_cast<int>(stage.size()), stage.data(),
                     static_cast<long long>(elapsed.count()), static_cast<int>(note.size()),
                     note.data());
    }
}

} // namespace

int main(int argc, char* argv[]) {
    const auto mainStart = std::chrono::steady_clock::now();
    try {
#ifndef _WIN32
        ::setenv("YAMS_CLI_ONE_SHOT", "1", 1);
#else
        _putenv_s("YAMS_CLI_ONE_SHOT", "1");
#endif

        if (argc == 2 && argv[1] != nullptr && std::string_view(argv[1]) == "--version") {
#if __has_include(<version_generated.h>)
            yams::VersionInfo ver{};
            std::string longVersion = std::string(ver.effective_version);
            std::string commit = ver.git_commit ? std::string(ver.git_commit) : "";
            if (!commit.empty()) {
                longVersion += " (commit: " + commit + ")";
            } else if (std::string(ver.git_describe).size()) {
                longVersion += " (" + std::string(ver.git_describe) + ")";
            }
            longVersion += " built:" + std::string(ver.build_timestamp);
            std::cout << longVersion << '\n';
#elif defined(YAMS_VERSION_LONG_STRING)
            std::cout << YAMS_VERSION_LONG_STRING << '\n';
#else
            std::cout << YAMS_VERSION_STRING << '\n';
#endif
            cli_perf_trace("main.total",
                           std::chrono::duration_cast<std::chrono::microseconds>(
                               std::chrono::steady_clock::now() - mainStart),
                           "version_fast_path");
            return 0;
        }

        // Set up logging with conservative default; YamsCLI::run() adjusts based on flags
        spdlog::set_level(spdlog::level::warn);
        spdlog::set_pattern("[%H:%M:%S] [%l] %v");
        cli_perf_trace("main.logging", std::chrono::duration_cast<std::chrono::microseconds>(
                                           std::chrono::steady_clock::now() - mainStart));

        // Create io_context for async operations
        const auto ioStart = std::chrono::steady_clock::now();
        boost::asio::io_context io_context;
        auto work_guard = boost::asio::make_work_guard(io_context);
        cli_perf_trace("main.io_context", std::chrono::duration_cast<std::chrono::microseconds>(
                                              std::chrono::steady_clock::now() - ioStart));

        // Start worker threads
        const auto threadStart = std::chrono::steady_clock::now();
        unsigned int thread_count = std::thread::hardware_concurrency();
        if (thread_count == 0)
            thread_count = 4;
        thread_count = std::min(thread_count, 16u);
        if (env_truthy(std::getenv("YAMS_CLI_ONE_SHOT"))) {
            thread_count = 1;
        }

        std::vector<std::thread> threads;
        for (unsigned int i = 0; i < thread_count; ++i) {
            threads.emplace_back([&io_context]() { io_context.run(); });
        }
        cli_perf_trace("main.thread_pool",
                       std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::steady_clock::now() - threadStart),
                       std::to_string(thread_count));

        // Create and run CLI with executor
        const auto cliStart = std::chrono::steady_clock::now();
        yams::cli::YamsCLI cli(io_context.get_executor());
        cli_perf_trace("main.cli_ctor", std::chrono::duration_cast<std::chrono::microseconds>(
                                            std::chrono::steady_clock::now() - cliStart));
        const auto runStart = std::chrono::steady_clock::now();
        int result = cli.run(argc, argv);
        cli_perf_trace("main.cli_run", std::chrono::duration_cast<std::chrono::microseconds>(
                                           std::chrono::steady_clock::now() - runStart));

        // Cleanup
        const auto cleanupStart = std::chrono::steady_clock::now();
        work_guard.reset();
        io_context.stop();
        for (auto& t : threads) {
            if (t.joinable())
                t.join();
        }
        cli_perf_trace("main.cleanup", std::chrono::duration_cast<std::chrono::microseconds>(
                                           std::chrono::steady_clock::now() - cleanupStart));
        cli_perf_trace("main.total", std::chrono::duration_cast<std::chrono::microseconds>(
                                         std::chrono::steady_clock::now() - mainStart));

        return result;

    } catch (const std::exception& e) {
        spdlog::error("Fatal error: {}", e.what());
        return 1;
    } catch (...) {
        spdlog::error("Unknown fatal error");
        return 1;
    }
}
