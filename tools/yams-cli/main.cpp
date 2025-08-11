#include <yams/cli/yams_cli.h>
#include <spdlog/spdlog.h>

int main(int argc, char* argv[]) {
    try {
        // Set up logging
        spdlog::set_level(spdlog::level::info);
        spdlog::set_pattern("[%H:%M:%S] [%l] %v");
        
        // Create and run CLI
        yams::cli::YamsCLI cli;
        return cli.run(argc, argv);
        
    } catch (const std::exception& e) {
        spdlog::error("Fatal error: {}", e.what());
        return 1;
    }
}