#pragma once
#include <optional>
#include <ostream>
#include <yams/daemon/ipc/ipc_protocol.h>

namespace yams::cli {
class YamsCLI;
}

namespace yams::cli::doctor {

/// Checks daemon connectivity, status, migration health, resources,
/// plugin trust, and loaded plugins.
class DaemonCheck {
public:
    void execute(std::ostream& os, YamsCLI* cli,
                 std::optional<daemon::StatusResponse>& cachedStatus);
};

} // namespace yams::cli::doctor
