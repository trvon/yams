#pragma once

#include <cstdint>

#include <yams/daemon/ipc/ipc_protocol.h>

#include <yams/daemon/components/repair/repair_health_probe.h>

namespace yams::daemon::repair {

enum class AutoRepairTier { Fast, Warm, Cold };

class RepairPlanBuilder {
public:
    static RepairRequest buildFast(const RepairHealthSnapshot& health);
    static RepairRequest buildWarm(const RepairHealthSnapshot& health, int32_t maxRetries);
    static RepairRequest buildCold();
    static bool hasWork(const RepairRequest& request);
};

} // namespace yams::daemon::repair
