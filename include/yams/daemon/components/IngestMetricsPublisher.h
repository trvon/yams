// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (c) 2024-2026 YAMS Project Contributors

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>

namespace yams::daemon {

class IngestMetricsPublisher {
public:
    struct Snapshot {
        std::size_t queued;
        std::size_t active;
        std::size_t target;
    };

    void publishIngest(std::size_t queued, std::size_t active) {
        ingestQueued_.store(queued, std::memory_order_relaxed);
        ingestActive_.store(active, std::memory_order_relaxed);
    }

    void publishQueued(std::size_t queued) {
        ingestQueued_.store(queued, std::memory_order_relaxed);
    }

    void publishActive(std::size_t active) {
        ingestActive_.store(active, std::memory_order_relaxed);
    }

    void setWorkerTarget(std::size_t target) {
        if (target < 1)
            target = 1;
        ingestWorkerTarget_.store(target, std::memory_order_relaxed);
    }

    [[nodiscard]] std::size_t workerTarget() const {
        auto v = ingestWorkerTarget_.load(std::memory_order_relaxed);
        return v == 0 ? 1 : v;
    }

    [[nodiscard]] Snapshot snapshot() const {
        return {ingestQueued_.load(std::memory_order_relaxed),
                ingestActive_.load(std::memory_order_relaxed),
                ingestWorkerTarget_.load(std::memory_order_relaxed)};
    }

    void onSnapshotPersisted() { snapshotsPersisted_.fetch_add(1, std::memory_order_relaxed); }

    [[nodiscard]] std::uint64_t snapshotsPersistedCount() const {
        return snapshotsPersisted_.load(std::memory_order_relaxed);
    }

    void onWorkerJobStart() {
        poolActive_.fetch_add(1, std::memory_order_relaxed);
        poolPosted_.fetch_add(1, std::memory_order_relaxed);
    }

    void onWorkerJobEnd() {
        poolActive_.fetch_sub(1, std::memory_order_relaxed);
        poolCompleted_.fetch_add(1, std::memory_order_relaxed);
    }

    [[nodiscard]] std::size_t workerActive() const {
        return poolActive_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] std::size_t workerPosted() const {
        return poolPosted_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] std::size_t workerCompleted() const {
        return poolCompleted_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] std::function<void(bool)> workerJobSignal() {
        return [this](bool start) {
            if (start)
                onWorkerJobStart();
            else
                onWorkerJobEnd();
        };
    }

private:
    std::atomic<std::size_t> ingestQueued_{0};
    std::atomic<std::size_t> ingestActive_{0};
    std::atomic<std::size_t> ingestWorkerTarget_{1};
    std::atomic<std::size_t> poolActive_{0};
    std::atomic<std::size_t> poolPosted_{0};
    std::atomic<std::size_t> poolCompleted_{0};
    std::atomic<std::uint64_t> snapshotsPersisted_{0};
};

} // namespace yams::daemon
