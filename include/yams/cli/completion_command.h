#pragma once

#include <memory>
#include <string>
#include <vector>
#include <CLI/CLI.hpp>
#include <yams/cli/command.h>
#include <yams/core/types.h>

namespace yams::cli {

class YamsCLI;

/**
 * Command to generate shell completion scripts for bash, zsh, and fish
 * Since CLI11 v2.4.1 doesn't support built-in completions, we generate
 * them manually with hardcoded knowledge of YAMS command structure.
 */
class CompletionCommand : public ICommand {
public:
    std::string getName() const override;
    std::string getDescription() const override;
    void registerCommand(CLI::App& app, YamsCLI* cli) override;
    Result<void> execute() override;
    boost::asio::awaitable<Result<void>> executeAsync() override { co_return execute(); }

private:
    YamsCLI* cli_ = nullptr;
    std::string shell_; // bash, zsh, fish

    // Shell-specific completion script generators
    std::string generateBashCompletion() const;
    std::string generateZshCompletion() const;
    std::string generateFishCompletion() const;

    // Helper to get list of available commands dynamically
    std::vector<std::string> getAvailableCommands() const;

    // Helper to get common global flags
    std::vector<std::string> getGlobalFlags() const;
};

// Factory function
std::unique_ptr<ICommand> createCompletionCommand();

} // namespace yams::cli
