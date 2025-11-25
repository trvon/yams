#include <cstdio>
#include <thread>
#include <vector>

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <spdlog/spdlog.h>
#include <yams/cli/yams_cli.h>
#include <yams/platform/windows_init.h>

int main(int argc, char* argv[]) {
    try {
        // Set up logging with conservative default; YamsCLI::run() adjusts based on flags
        spdlog::set_level(spdlog::level::warn);
        spdlog::set_pattern("[%H:%M:%S] [%l] %v");

        // Create io_context for async operations
        boost::asio::io_context io_context;
        auto work_guard = boost::asio::make_work_guard(io_context);

        // Start worker threads
        unsigned int thread_count = std::thread::hardware_concurrency();
        if (thread_count == 0)
            thread_count = 4;
        thread_count = std::min(thread_count, 16u);

        std::vector<std::thread> threads;
        for (unsigned int i = 0; i < thread_count; ++i) {
            threads.emplace_back([&io_context]() { io_context.run(); });
        }

        // Create and run CLI with executor
        yams::cli::YamsCLI cli(io_context.get_executor());
        int result = cli.run(argc, argv);

        // Cleanup
        work_guard.reset();
        io_context.stop();
        for (auto& t : threads) {
            if (t.joinable())
                t.join();
        }

        return result;

    } catch (const std::exception& e) {
        spdlog::error("Fatal error: {}", e.what());
        return 1;
    } catch (...) {
        spdlog::error("Unknown fatal error");
        return 1;
    }
}
