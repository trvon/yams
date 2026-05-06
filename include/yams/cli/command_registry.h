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
     * Register a minimal built-in subset for fast one-shot commands.
     * Returns true when the command name matched a supported fast path.
     */
    static bool registerMinimalCommandSet(YamsCLI* cli, std::string_view commandName);
};

} // namespace yams::cli
