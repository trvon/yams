#include <yams/tools/command.h>
#include <CLI/CLI.hpp>
#include <spdlog/spdlog.h>

#include <memory>
#include <map>
#include <iostream>
#include <csignal>

namespace kronos::tools {

// Forward declare command classes
class GCCommand;
class StatsCommand;
class VerifyCommand;

// Command factory functions (defined in each command file)
std::unique_ptr<Command> createGCCommand();
std::unique_ptr<Command> createStatsCommand();
std::unique_ptr<Command> createVerifyCommand();

class KronosTools {
public:
    KronosTools() : app_("kronos-tools", "Kronos Storage Maintenance Tools") {
        setupApp();
        registerCommands();
    }
    
    int run(int argc, char** argv) {
        try {
            app_.parse(argc, argv);
            
            // Execute the appropriate command
            for (auto& [name, cmd] : commands_) {
                if (app_.got_subcommand(name)) {
                    return cmd->execute();
                }
            }
            
            // No subcommand specified, show help
            std::cout << app_.help() << std::endl;
            return 0;
            
        } catch (const CLI::ParseError& e) {
            return app_.exit(e);
        } catch (const std::exception& e) {
            std::cerr << "Error: " << e.what() << std::endl;
            return 1;
        }
    }
    
private:
    void setupApp() {
        app_.set_version_flag("-V,--version", "1.0.0");
        app_.require_subcommand(0, 1);
        
        // Global options
        app_.add_flag("--debug", debug_, "Enable debug logging");
    }
    
    void registerCommands() {
        // Register each command
        registerCommand(createGCCommand());
        registerCommand(createStatsCommand());
        registerCommand(createVerifyCommand());
    }
    
    void registerCommand(std::unique_ptr<Command> cmd) {
        if (!cmd) return;
        
        const std::string& name = cmd->getName();
        cmd->setupOptions(app_);
        commands_[name] = std::move(cmd);
    }
    
    CLI::App app_;
    std::map<std::string, std::unique_ptr<Command>> commands_;
    bool debug_ = false;
};

// Command factory implementations
std::unique_ptr<Command> createGCCommand() {
    class GCCommandImpl : public GCCommand {
    public:
        using GCCommand::GCCommand;
    };
    return std::make_unique<GCCommandImpl>();
}

std::unique_ptr<Command> createStatsCommand() {
    class StatsCommandImpl : public StatsCommand {
    public:
        using StatsCommand::StatsCommand;
    };
    return std::make_unique<StatsCommandImpl>();
}

std::unique_ptr<Command> createVerifyCommand() {
    class VerifyCommandImpl : public VerifyCommand {
    public:
        using VerifyCommand::VerifyCommand;
    };
    return std::make_unique<VerifyCommandImpl>();
}

} // namespace kronos::tools

// Signal handling
std::atomic<bool> g_interrupted{false};

void signalHandler(int signal) {
    if (signal == SIGINT || signal == SIGTERM) {
        g_interrupted = true;
        std::cerr << "\nInterrupted. Cleaning up..." << std::endl;
    }
}

int main(int argc, char** argv) {
    // Set up signal handling
    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);
    
    // Initialize logging
    spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%l] %v");
    
    // Run the application
    kronos::tools::KronosTools app;
    return app.run(argc, argv);
}