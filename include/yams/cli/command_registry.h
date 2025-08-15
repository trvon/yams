#pragma once

#include <memory>
#include <vector>
#include <yams/cli/command.h>

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
     * Create restore command
     */
    static std::unique_ptr<ICommand> createRestoreCommand();

    /**
     * Create cat command
     */
    static std::unique_ptr<ICommand> createCatCommand();

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
     * Create grep command
     */
    static std::unique_ptr<ICommand> createGrepCommand();

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
     * Create status command (quick system overview)
     */
    static std::unique_ptr<ICommand> createStatusCommand();

    /**
     * Create uninstall command
     */
    static std::unique_ptr<ICommand> createUninstallCommand();

    /**
     * Create migrate command
     */
    static std::unique_ptr<ICommand> createMigrateCommand();

    /**
     * Create update command
     */
    static std::unique_ptr<ICommand> createUpdateCommand();

    /**
     * Create serve command (MCP server)
     */
    static std::unique_ptr<ICommand> createServeCommand();

    /**
     * Create browse command (TUI browser)
     */
    static std::unique_ptr<ICommand> createBrowseCommand();

    /**
     * Create completion command (shell completions)
     */
    static std::unique_ptr<ICommand> createCompletionCommand();

    /**
     * Create repair-mime command
     */
    static std::unique_ptr<ICommand> createRepairMimeCommand();

    /**
     * Create repair command (maintenance and integrity)
     */
    static std::unique_ptr<ICommand> createRepairCommand();

    /**
     * Create model command (ONNX model management)
     */
    static std::unique_ptr<ICommand> createModelCommand();
};

} // namespace yams::cli