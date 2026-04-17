#pragma once

#include <chrono>
#include <cstdint>
#include <optional>
#include <string_view>

#include <yams/cli/daemon_helpers.h>
#include <yams/core/types.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/metric_keys.h>

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
