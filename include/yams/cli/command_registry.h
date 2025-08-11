#pragma once

#include <yams/cli/command.h>
#include <memory>
#include <vector>

namespace yams::cli {

// Forward declaration
class YamsCLI;

/**
 * Registry of all available CLI commands
 */
class CommandRegistry {
public:
    /**
     * Register all built-in commands with the CLI
     */
    static void registerAllCommands(YamsCLI* cli);
    
    /**
     * Create init command
     */
    static std::unique_ptr<ICommand> createInitCommand();
    
    /**
     * Create add command
     */
    static std::unique_ptr<ICommand> createAddCommand();
    
    /**
     * Create get command
     */
    static std::unique_ptr<ICommand> createGetCommand();
    
    /**
     * Create delete command
     */
    static std::unique_ptr<ICommand> createDeleteCommand();
    
    /**
     * Create list command
     */
    static std::unique_ptr<ICommand> createListCommand();
    
    /**
     * Create search command
     */
    static std::unique_ptr<ICommand> createSearchCommand();
    
    /**
     * Create config command
     */
    static std::unique_ptr<ICommand> createConfigCommand();
    
    /**
     * Create auth command
     */
    static std::unique_ptr<ICommand> createAuthCommand();
    
    /**
     * Create stats command
     */
    static std::unique_ptr<ICommand> createStatsCommand();
    
    /**
     * Create uninstall command
     */
    static std::unique_ptr<ICommand> createUninstallCommand();
    
    /**
     * Create migrate command
     */
    static std::unique_ptr<ICommand> createMigrateCommand();
    
    /**
     * Create serve command (MCP server)
     */
    static std::unique_ptr<ICommand> createServeCommand();
    
    /**
     * Create browse command (TUI browser)
     */
    static std::unique_ptr<ICommand> createBrowseCommand();
};

} // namespace yams::cli