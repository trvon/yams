#pragma once

#include <memory>
#include <string>
#include <yams/cli/command.h>

namespace yams::cli {

/**
 * Terminal User Interface (TUI) browser for documents
 * Provides an interactive interface for browsing and managing documents
 */
class BrowseCommand : public ICommand {
public:
    BrowseCommand();
    ~BrowseCommand();

    std::string getName() const override;
    std::string getDescription() const override;

    void registerCommand(CLI::App& app, YamsCLI* cli) override;

    Result<void> execute() override;

private:
    // Forward declaration for implementation
    class Impl;
    std::unique_ptr<Impl> pImpl;
};

// Factory function
std::unique_ptr<ICommand> createBrowseCommand();

} // namespace yams::cli