#pragma once

#include <yams/core/types.h>
#include <CLI/CLI.hpp>

#include <filesystem>
#include <string>
#include <memory>

namespace kronos::tools {

/**
 * Base class for all CLI commands
 * 
 * Provides common functionality like progress reporting,
 * dry-run mode, and verbose output
 */
class Command {
public:
    Command(std::string name, std::string description)
        : name_(std::move(name))
        , description_(std::move(description))
    {}
    
    virtual ~Command() = default;
    
    // Setup command-specific options
    virtual void setupOptions(CLI::App& app) = 0;
    
    // Execute the command
    virtual int execute() = 0;
    
    // Get command name
    const std::string& getName() const { return name_; }
    
    // Get command description
    const std::string& getDescription() const { return description_; }
    
protected:
    // Common options available to all commands
    struct CommonOptions {
        bool dryRun = false;
        bool verbose = false;
        bool quiet = false;
        std::filesystem::path configFile;
        std::filesystem::path storagePath;
    };
    
    // Add common options to the command
    void addCommonOptions(CLI::App& app) {
        app.add_flag("-n,--dry-run", options_.dryRun, 
                     "Perform a trial run with no changes made");
        app.add_flag("-v,--verbose", options_.verbose,
                     "Enable verbose output");
        app.add_flag("-q,--quiet", options_.quiet,
                     "Suppress all output except errors");
        app.add_option("-c,--config", options_.configFile,
                     "Path to configuration file");
        app.add_option("-s,--storage", options_.storagePath,
                     "Path to storage directory")
           ->default_val(".");
    }
    
    // Check if we're in dry-run mode
    bool isDryRun() const { return options_.dryRun; }
    
    // Check if verbose output is enabled
    bool isVerbose() const { return options_.verbose && !options_.quiet; }
    
    // Check if quiet mode is enabled
    bool isQuiet() const { return options_.quiet; }
    
    // Get storage path
    const std::filesystem::path& getStoragePath() const { 
        return options_.storagePath; 
    }
    
    // Print message based on verbosity settings
    void log(const std::string& message) const {
        if (!isQuiet()) {
            std::cout << message << std::endl;
        }
    }
    
    void logVerbose(const std::string& message) const {
        if (isVerbose()) {
            std::cout << "[VERBOSE] " << message << std::endl;
        }
    }
    
    void logError(const std::string& message) const {
        std::cerr << "[ERROR] " << message << std::endl;
    }
    
private:
    std::string name_;
    std::string description_;
    CommonOptions options_;
};

} // namespace kronos::tools