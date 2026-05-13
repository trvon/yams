#pragma once

#include <cstdint>
#include <ostream>
#include <string>

namespace yams::cli {
class YamsCLI;
}

namespace yams::cli::doctor {

/// Performs local plugin ABI/interface probes and optional daemon dry-run load.
class PluginCheck {
public:
    struct Config {
        std::string arg;
        std::string ifaceId;
        uint32_t ifaceVersion{1};
        bool noDaemonProbe{false};
    };

    void execute(std::ostream& os, YamsCLI* cli, const Config& cfg);
};

} // namespace yams::cli::doctor
