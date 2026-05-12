#pragma once
#include <ostream>

namespace yams::cli {
class YamsCLI;
}

namespace yams::cli::doctor {

class DbRepairCommand {
public:
    struct Config {
        bool repairEmbeddings{false};
        bool repairFts5{false};
        bool repairGraph{false};
        bool noDaemonRepair{false};
    };
    void execute(std::ostream& os, YamsCLI* cli, const Config& cfg);
};

} // namespace yams::cli::doctor
