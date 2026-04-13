#pragma once

#include <string_view>

#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/metric_keys.h>

namespace yams::cli {

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
