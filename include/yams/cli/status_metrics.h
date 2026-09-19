#pragma once

#include <chrono>
#include <cstdint>
#include <optional>
#include <string_view>

// pi-lens-ignore: fatal error
#include <yams/cli/daemon_helpers.h>
#include <yams/core/types.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/metric_keys.h>
#include <yams/storage/disk_pressure.h>

namespace yams::cli {

struct DaemonRepairState {
    bool inProgress = false;
    bool serviceRunning = false;
    std::optional<std::uint64_t> queueDepth;
};

inline DaemonRepairState extractRepairState(const daemon::StatusResponse& status) {
    DaemonRepairState state;
    auto findCount = [&](std::string_view key) -> const std::size_t* {
        auto entry = status.requestCounts.find(std::string(key));
        return entry != status.requestCounts.end() ? &entry->second : nullptr;
    };
    if (const auto* value = findCount(daemon::metrics::kRepairInProgress)) {
        state.inProgress = (*value != 0);
    }
    if (const auto* value = findCount(daemon::metrics::kRepairRunning)) {
        state.serviceRunning = (*value != 0);
    }
    if (const auto* value = findCount(daemon::metrics::kRepairQueueDepth)) {
        state.queueDepth = static_cast<std::uint64_t>(*value);
    }
    return state;
}

struct DaemonStoragePressure {
    // pi-lens-ignore: no-bit-fields
    std::optional<std::uint64_t> capacityBytes;
    std::optional<std::uint64_t> availableBytes;
    // pi-lens-ignore: no-bit-fields
    storage::DiskPressureLevel level{storage::DiskPressureLevel::Unknown};
    double warningFreePercent{0.0};
    std::uint64_t minimumWriteAdmissionBytes{0};
    std::uint64_t emergencyReserveBytes{0};
};

inline DaemonStoragePressure extractStoragePressure(const daemon::StatusResponse& status) {
    DaemonStoragePressure pressure;
    auto findCount = [&](std::string_view key) -> const std::size_t* {
        const auto entry = status.requestCounts.find(std::string(key));
        return entry != status.requestCounts.end() ? &entry->second : nullptr;
    };
    auto readUint64 = [&](std::string_view lowKey,
                          std::string_view highKey) -> std::optional<std::uint64_t> {
        const auto* low = findCount(lowKey);
        const auto* high = findCount(highKey);
        if (!low || !high) {
            return std::nullopt;
        }
        return (static_cast<std::uint64_t>(*high) << 32U) |
               (static_cast<std::uint64_t>(*low) & 0xffffffffULL);
    };
    pressure.capacityBytes = readUint64(daemon::metrics::kStorageCapacityBytesLow,
                                        daemon::metrics::kStorageCapacityBytesHigh);
    pressure.availableBytes = readUint64(daemon::metrics::kStorageAvailableBytesLow,
                                         daemon::metrics::kStorageAvailableBytesHigh);
    if (const auto* value = findCount(daemon::metrics::kStoragePressureLevel);
        value && *value <= static_cast<std::size_t>(storage::DiskPressureLevel::Emergency)) {
        pressure.level = static_cast<storage::DiskPressureLevel>(*value);
    }
    if (const auto* value = findCount(daemon::metrics::kStorageWarningFreePercentBp)) {
        pressure.warningFreePercent = static_cast<double>(*value) / 100.0;
    }
    pressure.minimumWriteAdmissionBytes =
        readUint64(daemon::metrics::kStorageWriteAdmissionBytesLow,
                   daemon::metrics::kStorageWriteAdmissionBytesHigh)
            .value_or(0);
    pressure.emergencyReserveBytes = readUint64(daemon::metrics::kStorageEmergencyReserveBytesLow,
                                                daemon::metrics::kStorageEmergencyReserveBytesHigh)
                                         .value_or(0);
    return pressure;
}

inline Result<DaemonRepairState>
probeDaemonRepairState(daemon::DaemonClient& client,
                       std::chrono::milliseconds timeout = std::chrono::seconds(3)) {
    daemon::StatusRequest sreq;
    sreq.detailed = false;
    auto sres = run_result<daemon::StatusResponse>(client.call(sreq), timeout);
    if (!sres) {
        return sres.error();
    }
    return extractRepairState(sres.value());
}

inline daemon::StatusResponse::SearchMetrics
effectiveSearchMetrics(const daemon::StatusResponse& status) {
    auto metrics = status.searchMetrics;

    auto findCount = [&](std::string_view key) -> const std::size_t* {
        auto it = status.requestCounts.find(std::string(key));
        return it != status.requestCounts.end() ? &it->second : nullptr;
    };

    if (metrics.active == 0) {
        if (const auto* value = findCount(daemon::metrics::kSearchActive)) {
            metrics.active = static_cast<std::uint32_t>(*value);
        }
    }
    if (metrics.queued == 0) {
        if (const auto* value = findCount(daemon::metrics::kSearchQueued)) {
            metrics.queued = static_cast<std::uint32_t>(*value);
        }
    }
    if (metrics.executed == 0) {
        if (const auto* value = findCount(daemon::metrics::kSearchExecuted)) {
            metrics.executed = static_cast<std::uint64_t>(*value);
        }
    }
    if (metrics.cacheHitRate == 0.0) {
        if (const auto* value = findCount(daemon::metrics::kSearchCacheHitRatePct)) {
            metrics.cacheHitRate = static_cast<double>(*value) / 100.0;
        }
    }
    if (metrics.avgLatencyUs == 0) {
        if (const auto* value = findCount(daemon::metrics::kSearchAvgLatencyUs)) {
            metrics.avgLatencyUs = static_cast<std::uint64_t>(*value);
        }
    }
    if (metrics.concurrencyLimit == 0) {
        if (const auto* value = findCount(daemon::metrics::kSearchConcurrencyLimit)) {
            metrics.concurrencyLimit = static_cast<std::uint32_t>(*value);
        }
    }

    return metrics;
}

} // namespace yams::cli
