// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (c) 2024-2026 YAMS Project Contributors

#pragma once

#include <atomic>
#include <cstdint>

namespace yams::daemon {

class SearchAdmissionController {
public:
    void onQueued() { queued_.fetch_add(1, std::memory_order_relaxed); }

    bool tryStart(std::uint32_t concurrencyCap) {
        if (concurrencyCap == 0) {
            decrementQueued();
            active_.fetch_add(1, std::memory_order_relaxed);
            return true;
        }
        std::uint32_t active = active_.load(std::memory_order_relaxed);
        while (true) {
            if (active >= concurrencyCap)
                return false;
            if (active_.compare_exchange_weak(active, active + 1, std::memory_order_relaxed)) {
                decrementQueued();
                return true;
            }
        }
    }

    void onFinished() {
        std::uint32_t active = active_.load(std::memory_order_relaxed);
        while (active > 0 &&
               !active_.compare_exchange_weak(active, active - 1, std::memory_order_relaxed)) {
        }
    }

    void onRejected() { decrementQueued(); }

    [[nodiscard]] std::uint32_t active() const { return active_.load(std::memory_order_relaxed); }

    [[nodiscard]] std::uint32_t queued() const { return queued_.load(std::memory_order_relaxed); }

private:
    void decrementQueued() {
        std::uint32_t q = queued_.load(std::memory_order_relaxed);
        while (q > 0 && !queued_.compare_exchange_weak(q, q - 1, std::memory_order_relaxed)) {
        }
    }

    std::atomic<std::uint32_t> active_{0};
    std::atomic<std::uint32_t> queued_{0};
};

} // namespace yams::daemon
