#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include <yams/core/types.h>
#include <yams/daemon/components/ResourceGovernor.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/mux_metrics_registry.h>

namespace yams::daemon::admission {

struct OverloadSnapshot {
    std::uint64_t workerQueued{0};
    std::uint64_t maxWorkerQueue{0};
    std::int64_t muxQueuedBytes{0};
    std::uint64_t maxMuxBytes{0};
    std::uint64_t activeConnections{0};
    std::uint64_t maxActiveConnections{0};
    std::uint64_t postIngestQueued{0};
    std::uint64_t postIngestCapacity{0};
    std::uint32_t controlIntervalMs{1000};
};

struct OverloadDecision {
    ErrorCode code{ErrorCode::ResourceExhausted};
    std::uint32_t retryAfterMs{0};
    std::string message;
};

inline OverloadSnapshot
captureSnapshot(const ServiceManager* services, const StateComponent* state,
                std::optional<std::uint64_t> activeConnections = std::nullopt,
                std::optional<std::uint64_t> maxActiveConnections = std::nullopt) {
    OverloadSnapshot snapshot;
    if (services != nullptr) {
        snapshot.workerQueued = services->getWorkerQueueDepth();
        snapshot.maxWorkerQueue = TuneAdvisor::maxWorkerQueue(services->getWorkerThreads());
        snapshot.controlIntervalMs = services->getTuningConfig().controlIntervalMs;
        if (auto pq = services->getPostIngestQueue()) {
            snapshot.postIngestQueued = pq->size();
            snapshot.postIngestCapacity = pq->capacity();
        }
    }

    try {
        auto mux = MuxMetricsRegistry::instance().snapshot();
        snapshot.muxQueuedBytes = mux.queuedBytes;
    } catch (...) {
    }
    snapshot.maxMuxBytes = TuneAdvisor::maxMuxBytes();

    if (activeConnections.has_value()) {
        snapshot.activeConnections = *activeConnections;
    } else if (state != nullptr) {
        snapshot.activeConnections = state->stats.activeConnections.load(std::memory_order_relaxed);
    }

    if (maxActiveConnections.has_value()) {
        snapshot.maxActiveConnections = *maxActiveConnections;
    } else if (state != nullptr) {
        snapshot.maxActiveConnections = state->stats.maxConnections.load(std::memory_order_relaxed);
    }

    return snapshot;
}

inline std::uint32_t computeRetryAfterMs(const OverloadSnapshot& snapshot) {
    return ResourceGovernor::instance().recommendRetryAfterMs(
        snapshot.workerQueued, snapshot.maxWorkerQueue, snapshot.muxQueuedBytes,
        snapshot.maxMuxBytes, snapshot.activeConnections, snapshot.maxActiveConnections,
        snapshot.postIngestQueued, snapshot.postIngestCapacity, snapshot.controlIntervalMs);
}

inline std::string formatRetryMessage(std::string_view subject, std::uint32_t retryAfterMs) {
    std::string message(subject);
    if (retryAfterMs > 0) {
        message += "; retry in " + std::to_string(retryAfterMs) + "ms";
    } else {
        message += "; retry shortly";
    }
    return message;
}

inline OverloadDecision makeBusyDecision(std::string_view subject,
                                         const OverloadSnapshot& snapshot) {
    OverloadDecision decision;
    decision.retryAfterMs = computeRetryAfterMs(snapshot);
    decision.message = formatRetryMessage(subject, decision.retryAfterMs);
    return decision;
}

inline ErrorResponse makeBusyError(std::string_view subject, const OverloadSnapshot& snapshot) {
    auto decision = makeBusyDecision(subject, snapshot);
    ErrorResponse error;
    error.code = decision.code;
    error.message = std::move(decision.message);
    error.retry = ErrorResponse::RetryInfo{decision.retryAfterMs, "overload"};
    return error;
}

} // namespace yams::daemon::admission
