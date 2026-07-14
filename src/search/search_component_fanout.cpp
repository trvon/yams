// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#include "search_component_fanout_internal.h"

#include <spdlog/spdlog.h>

namespace yams::search::detail {

ComponentStatus ComponentFanoutCollector::collect(Future& future, const char* name,
                                                  std::atomic<uint64_t>& queryCount,
                                                  std::atomic<uint64_t>& avgTime) {
    if (!future.valid())
        return ComponentStatus::Success;

    trace_.markStageAttempted(name);

    auto waitStart = std::chrono::steady_clock::now();

    std::future_status status;
    if (config_.componentTimeout.count() == 0) {
        future.wait();
        status = std::future_status::ready;
    } else {
        status = future.wait_for(config_.componentTimeout);
    }

    if (status == std::future_status::ready) {
        try {
            auto results = future.get();
            auto waitEnd = std::chrono::steady_clock::now();
            auto duration =
                std::chrono::duration_cast<std::chrono::microseconds>(waitEnd - waitStart).count();

            sinks_.componentTiming[name] = duration;

            if (results) {
                trace_.markStageResult(name, results.value(), duration, !results.value().empty());
                if (!results.value().empty()) {
                    sinks_.allComponentResults.insert(sinks_.allComponentResults.end(),
                                                      results.value().begin(),
                                                      results.value().end());
                    sinks_.contributing.push_back(name);
                }
                queryCount.fetch_add(1, std::memory_order_relaxed);
                avgTime.store(duration, std::memory_order_relaxed);
                return ComponentStatus::Success;
            } else {
                spdlog::debug("Parallel {} query returned error: {}", name,
                              results.error().message);
                trace_.markStageFailure(name, duration);
                return ComponentStatus::Failed;
            }
        } catch (const std::exception& e) {
            spdlog::warn("Parallel {} query failed: {}", name, e.what());
            auto waitEnd = std::chrono::steady_clock::now();
            trace_.markStageFailure(
                name,
                std::chrono::duration_cast<std::chrono::microseconds>(waitEnd - waitStart).count());
            return ComponentStatus::Failed;
        }
    } else {
        spdlog::warn("Parallel {} query timed out after {} ms", name,
                     config_.componentTimeout.count());
        sinks_.timedOutQueries.fetch_add(1, std::memory_order_relaxed);
        auto waitEnd = std::chrono::steady_clock::now();
        trace_.markStageTimeout(
            name, std::chrono::duration_cast<std::chrono::microseconds>(waitEnd - waitStart).count());
        return ComponentStatus::TimedOut;
    }
}

void ComponentFanoutCollector::collectAndRecord(Future& future, const char* name,
                                                std::atomic<uint64_t>& queryCount,
                                                std::atomic<uint64_t>& avgTime) {
    const auto status = collect(future, name, queryCount, avgTime);
    if (status == ComponentStatus::Failed) {
        sinks_.failed.push_back(name);
    } else if (status == ComponentStatus::TimedOut) {
        sinks_.timedOut.push_back(name);
    }
}

} // namespace yams::search::detail
