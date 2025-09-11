#pragma once

#include <memory>
#include <string>
#include <boost/asio/awaitable.hpp>
#include <CLI/CLI.hpp>
#include <yams/core/types.h>

namespace yams::cli {

// Forward declarations
class YamsCLI;

/**
 * Base interface for CLI commands
 */
class ICommand {
public:
    virtual ~ICommand() = default;

    /**
     * Get the command name (e.g., "add", "search")
     */
    virtual std::string getName() const = 0;

    /**
     * Get the command description for help text
     */
    virtual std::string getDescription() const = 0;

    /**
     * Register this command with the CLI11 app
     */
    virtual void registerCommand(CLI::App& app, YamsCLI* cli) = 0;

    /**
     * Execute the command
     */
    virtual Result<void> execute() = 0;

    /**
     * Asynchronous execution (optional during migration). Default bridges to execute().
     */
    virtual boost::asio::awaitable<Result<void>> executeAsync() { co_return execute(); }
};

} // namespace yams::cli
