#pragma once

#include <cstdint>
#include <yams/daemon/components/TuningSnapshot.h>

namespace yams::daemon::repair_tuning {

inline uint32_t repairDegradeHoldMs() {
    if (auto snap = TuningSnapshotRegistry::instance().get()) {
        return snap->repairDegradeHoldMs;
    }
    return 750;
}

inline uint32_t repairReadyHoldMs() {
    if (auto snap = TuningSnapshotRegistry::instance().get()) {
        return snap->repairReadyHoldMs;
    }
    return 1500;
}

} // namespace yams::daemon::repair_tuning
