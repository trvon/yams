#pragma once

#include <cstddef>
#include <cstdint>

namespace yams::daemon {

// Persistable/configurable knobs for adaptive tuning. Defaults are conservative and
// can be adjusted at runtime via ServiceManager::setTuningConfig.
struct TuningConfig {
    // Target aggregate CPU budget for ingest/tuning logic (percentage of one core units).
    // Controller uses this as a soft target; not a hard cap.
    std::uint32_t targetCpuPercent{200};

    // Post-ingest queue capacity and thread bounds
    std::size_t postIngestCapacity{2000};
    std::size_t postIngestThreadsMin{2};
    std::size_t postIngestThreadsMax{8};

    // Admission thresholds
    std::size_t admitWarnThreshold{1500};
    std::size_t admitStopThreshold{1900};

    // Controller cadence and stability parameters
    std::uint32_t controlIntervalMs{1000};
    std::uint32_t holdMs{3000};
};

} // namespace yams::daemon
