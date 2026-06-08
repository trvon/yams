// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#if __has_include(<yams/compat/thread_stop_compat.h>)
#include <yams/compat/thread_stop_compat.h>
#elif __has_include("yams/compat/thread_stop_compat.h")
#include "yams/compat/thread_stop_compat.h"
#endif

#include <atomic>
#include <chrono>
#include <future>

namespace yams::daemon {
namespace detail {
#if __has_include(<yams/compat/thread_stop_compat.h>) || __has_include("yams/compat/thread_stop_compat.h")
using AsyncInitStopToken = yams::compat::stop_token;
using AsyncInitStopSource = yams::compat::stop_source;
#else
struct AsyncInitStopToken {
    constexpr bool stop_requested() const noexcept { return false; }
};

struct AsyncInitStopSource {
    AsyncInitStopToken get_token() const noexcept { return AsyncInitStopToken{}; }
    bool request_stop() noexcept { return true; }
    constexpr bool stop_possible() const noexcept { return true; }
};
#endif
} // namespace detail

/**
 * @brief Owns the state machine for ServiceManager's async initialization.
 *
 * Extracted from ServiceManager (PBI-088 Phase 6f). Centralizes three atomic
 * flags plus the stop-source + future that previously lived as loose members,
 * giving the async-init lifecycle a single home.
 *
 * Responsibilities:
 *  - One-shot start tripwire (`tryStart`)
 *  - Stop-source for cooperative cancellation (`getStopToken`, `requestStop`)
 *  - Ownership of the spawned coroutine's future (`setFuture` / `waitForCompletion`)
 *  - Separate one-shot tripwire for deferred metadata warmup
 *  - Reset hook for daemon restart
 */
class AsyncInitOrchestrator {
public:
    AsyncInitOrchestrator() = default;

    AsyncInitOrchestrator(const AsyncInitOrchestrator&) = delete;
    AsyncInitOrchestrator& operator=(const AsyncInitOrchestrator&) = delete;

    /// Attempt to transition to "started". Returns false if another thread
    /// already started the async init.
    bool tryStart() {
        bool expected = false;
        return started_.compare_exchange_strong(expected, true, std::memory_order_acq_rel);
    }

    /// Attempt to transition to "metadata warmup started". Returns true if
    /// this caller claimed the one-shot.
    bool tryBeginMetadataWarmup() {
        return !metadataWarmupStarted_.exchange(true, std::memory_order_acq_rel);
    }

    /// Stop-token passed to the async init coroutine.
    detail::AsyncInitStopToken getStopToken() { return stopSource_.get_token(); }

    /// Store the future returned by co_spawn.
    void setFuture(std::future<void> fut) { future_ = std::move(fut); }

    /// Reset lifecycle state so the daemon can be re-initialized after a
    /// restart. Does not affect an in-flight future — callers must drain it
    /// first via requestStopAndWait().
    void resetForRestart() { stopSource_ = detail::AsyncInitStopSource{}; }

    /// Request cancellation and wait up to `timeout` for the coroutine to
    /// complete. Safe to call when no future has been stored.
    /// Returns true if the coroutine finished (or was never started),
    /// false if the wait timed out.
    bool requestStopAndWait(std::chrono::milliseconds timeout) {
        if (stopSource_.stop_possible()) {
            stopSource_.request_stop();
        }
        if (!future_.valid()) {
            return true;
        }
        auto status = future_.wait_for(timeout);
        if (status == std::future_status::timeout) {
            return false;
        }
        try {
            future_.get();
        } catch (const std::exception&) {
            future_ = std::future<void>();
            return true;
        } catch (...) {
            future_ = std::future<void>();
            return true;
        }
        future_ = std::future<void>();
        return true;
    }

private:
    std::atomic<bool> started_{false};
    std::atomic<bool> metadataWarmupStarted_{false};
    detail::AsyncInitStopSource stopSource_;
    std::future<void> future_;
};

} // namespace yams::daemon
