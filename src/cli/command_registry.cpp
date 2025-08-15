#include <yams/cli/command_registry.h>
#include <yams/cli/yams_cli.h>

namespace yams::cli {

// External factory functions from command implementations (in this namespace)
std::unique_ptr<ICommand> createInitCommand();
std::unique_ptr<ICommand> createAddCommand();
std::unique_ptr<ICommand> createGetCommand();
std::unique_ptr<ICommand> createRestoreCommand();
std::unique_ptr<ICommand> createCatCommand();
std::unique_ptr<ICommand> createDeleteCommand();
std::unique_ptr<ICommand> createListCommand();
std::unique_ptr<ICommand> createSearchCommand();
std::unique_ptr<ICommand> createGrepCommand();
std::unique_ptr<ICommand> createConfigCommand();
std::unique_ptr<ICommand> createAuthCommand();
std::unique_ptr<ICommand> createStatsCommand();
std::unique_ptr<ICommand> createUninstallCommand();
std::unique_ptr<ICommand> createMigrateCommand();
std::unique_ptr<ICommand> createUpdateCommand();
#ifdef YAMS_ENABLE_TUI
std::unique_ptr<ICommand> createBrowseCommand();
#endif
std::unique_ptr<ICommand> createCompletionCommand();
std::unique_ptr<ICommand> createRepairMimeCommand();
std::unique_ptr<ICommand> createRepairCommand();
std::unique_ptr<ICommand> createModelCommand();
#ifdef YAMS_BUILD_MCP_SERVER
std::unique_ptr<ICommand> createServeCommand();
#endif

void CommandRegistry::registerAllCommands(YamsCLI* cli) {
    cli->registerCommand(CommandRegistry::createInitCommand());
    cli->registerCommand(CommandRegistry::createAddCommand());
    cli->registerCommand(CommandRegistry::createGetCommand());
    cli->registerCommand(CommandRegistry::createRestoreCommand());
    cli->registerCommand(CommandRegistry::createCatCommand());
    cli->registerCommand(CommandRegistry::createDeleteCommand());
    cli->registerCommand(CommandRegistry::createListCommand());
    cli->registerCommand(CommandRegistry::createSearchCommand());
    cli->registerCommand(CommandRegistry::createGrepCommand());
    cli->registerCommand(CommandRegistry::createConfigCommand());
    cli->registerCommand(CommandRegistry::createAuthCommand());
    cli->registerCommand(CommandRegistry::createStatsCommand());
    cli->registerCommand(CommandRegistry::createUninstallCommand());
    cli->registerCommand(CommandRegistry::createMigrateCommand());
    cli->registerCommand(CommandRegistry::createUpdateCommand());
    #ifdef YAMS_ENABLE_TUI
    cli->registerCommand(CommandRegistry::createBrowseCommand());
    #endif
    cli->registerCommand(CommandRegistry::createCompletionCommand());
    cli->registerCommand(CommandRegistry::createRepairMimeCommand());
    cli->registerCommand(CommandRegistry::createRepairCommand());
    cli->registerCommand(CommandRegistry::createModelCommand());
#ifdef YAMS_BUILD_MCP_SERVER
    cli->registerCommand(CommandRegistry::createServeCommand());
#endif
}

std::unique_ptr<ICommand> CommandRegistry::createInitCommand() {
    return ::yams::cli::createInitCommand();
}

std::unique_ptr<ICommand> CommandRegistry::createAddCommand() {
    return ::yams::cli::createAddCommand();
}

std::unique_ptr<ICommand> CommandRegistry::createGetCommand() {
    return ::yams::cli::createGetCommand();
}

std::unique_ptr<ICommand> CommandRegistry::createRestoreCommand() {
    return ::yams::cli::createRestoreCommand();
}

std::unique_ptr<ICommand> CommandRegistry::createCatCommand() {
    return ::yams::cli::createCatCommand();
}

std::unique_ptr<ICommand> CommandRegistry::createDeleteCommand() {
    return ::yams::cli::createDeleteCommand();
}

std::unique_ptr<ICommand> CommandRegistry::createListCommand() {
    return ::yams::cli::createListCommand();
}

std::unique_ptr<ICommand> CommandRegistry::createSearchCommand() {
    return ::yams::cli::createSearchCommand();
}

std::unique_ptr<ICommand> CommandRegistry::createGrepCommand() {
    return ::yams::cli::createGrepCommand();
}

std::unique_ptr<ICommand> CommandRegistry::createConfigCommand() {
    return ::yams::cli::createConfigCommand();
}

std::unique_ptr<ICommand> CommandRegistry::createAuthCommand() {
    return ::yams::cli::createAuthCommand();
}

std::unique_ptr<ICommand> CommandRegistry::createStatsCommand() {
    return ::yams::cli::createStatsCommand();
}

std::unique_ptr<ICommand> CommandRegistry::createUninstallCommand() {
    return ::yams::cli::createUninstallCommand();
}

std::unique_ptr<ICommand> CommandRegistry::createMigrateCommand() {
    return ::yams::cli::createMigrateCommand();
}

std::unique_ptr<ICommand> CommandRegistry::createUpdateCommand() {
    return ::yams::cli::createUpdateCommand();
}

#ifdef YAMS_ENABLE_TUI
std::unique_ptr<ICommand> CommandRegistry::createBrowseCommand() {
    return ::yams::cli::createBrowseCommand();
}
#endif

std::unique_ptr<ICommand> CommandRegistry::createCompletionCommand() {
    return ::yams::cli::createCompletionCommand();
}

std::unique_ptr<ICommand> CommandRegistry::createRepairMimeCommand() {
    return ::yams::cli::createRepairMimeCommand();
}

std::unique_ptr<ICommand> CommandRegistry::createRepairCommand() {
    return ::yams::cli::createRepairCommand();
}

std::unique_ptr<ICommand> CommandRegistry::createModelCommand() {
    // The standalone createModelCommand function is in this namespace
    return ::yams::cli::createModelCommand();
}

#ifdef YAMS_BUILD_MCP_SERVER
std::unique_ptr<ICommand> CommandRegistry::createServeCommand() {
    return ::yams::cli::createServeCommand();
}
#endif

} // namespace yams::cli