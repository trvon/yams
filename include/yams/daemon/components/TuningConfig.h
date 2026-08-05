#pragma once

#include <cstddef>
#include <cstdint>
#include <map>
#include <string>

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
    std::uint32_t postIngestCoalesceMs{2};

    // Maximum storage tasks committed together before they enter post-ingest processing.
    // This is intentionally independent from the downstream post-ingest transaction batch.
    std::uint32_t ingestStoreBatchSize{64};

    // Admission thresholds
    std::size_t admitWarnThreshold{1500};
    std::size_t admitStopThreshold{1900};

    // Controller cadence and stability parameters
    std::uint32_t controlIntervalMs{1000};
    std::uint32_t holdMs{3000};

    // P3.1: Topology engine algorithm key, resolved via topology::makeEngine.
    // Default "connected" preserves pre-P3 behavior (connected-components engine).
    // Unknown values fall back to "connected" at resolve time.
    std::string topologyAlgorithm{"connected"};

    // True after ConfigResolver has established a fresh lifecycle boundary for the legacy
    // TuneAdvisor atomics. Direct embedded ServiceManager construction starts with false and must
    // therefore clear any resolver-owned overrides left by an earlier in-process daemon.
    bool tuneAdvisorOverridesResolved{false};

    // Valid product configuration keys that contributed to the effective startup policy.
    // Compatibility environment sources are appended by ServiceManager when it snapshots them.
    std::map<std::string, std::string> provenance;
};

} // namespace yams::daemon
