#pragma once

#include <ostream>
#include <string>
#include <vector>

namespace yams::cli {
class YamsCLI;
} // namespace yams::cli

namespace yams::cli::doctor {

/// Runs the prune subcommand — sends a prune request to the daemon
/// and displays the results. Implements `yams doctor prune`.
class PruneCommand {
public:
    struct Config {
        std::vector<std::string> categories;
        std::vector<std::string> extensions;
        std::string olderThan;
        std::string largerThan;
        std::string smallerThan;
        bool apply{false};
        bool verbose{false};
    };

    explicit PruneCommand(YamsCLI* cli, Config config);
    void execute(std::ostream& os);

private:
    YamsCLI* cli_{nullptr};
    Config config_;
};

} // namespace yams::cli::doctor
