#pragma once
#include <ostream>
#include <string>

namespace yams::cli {
class YamsCLI;
}

namespace yams::cli::doctor {

class DedupeCommand {
public:
    struct Config {
        std::string mode{"path"};
        std::string strategy{"keep-newest"};
        double semanticThreshold{0.92};
        bool apply{false};
        bool verbose{false};
        bool force{false};
        bool listOnly{false};
        int listLimit{25};
        std::string groupKey;
    };
    void execute(std::ostream& os, YamsCLI* cli, const Config& cfg);
};

} // namespace yams::cli::doctor
