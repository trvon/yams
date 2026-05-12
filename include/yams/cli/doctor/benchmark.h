#pragma once

#include <ostream>
#include <string>

namespace yams::cli {
class YamsCLI;
} // namespace yams::cli

namespace yams::cli::doctor {

/// Runs search quality benchmarks and optional baseline comparisons.
/// Implements `yams doctor benchmark`.
class BenchmarkCommand {
public:
    struct Config {
        size_t queryCount{10};
        bool verbose{false};
        bool json{false};
        std::string saveBaseline;
        std::string compareBaseline;
        bool history{false};
        bool historyJson{false};
    };

    explicit BenchmarkCommand(YamsCLI* cli, Config config);
    void execute(std::ostream& os);

private:
    YamsCLI* cli_{nullptr};
    Config config_;
};

} // namespace yams::cli::doctor
