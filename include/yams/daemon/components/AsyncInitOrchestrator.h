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
#include <condition_variable>
#include <future>
#include <mutex>

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
        std::lock_guard lock(futureMutex_);
        bool expected = false;
        if (!started_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
            return false;
        }
        futureInstallationComplete_ = false;
        return true;
    }

    /// Attempt to transition to "metadata warmup started". Returns true if
    /// this caller claimed the one-shot.
    bool tryBeginMetadataWarmup() {
        return !metadataWarmupStarted_.exchange(true, std::memory_order_acq_rel);
    }

    /// Stop-token passed to the async init coroutine.
    detail::AsyncInitStopToken getStopToken() {
        std::lock_guard lock(futureMutex_);
        return stopSource_.get_token();
    }

    /// Store the future returned by co_spawn.
    void setFuture(std::future<void> fut) {
        {
            std::lock_guard lock(futureMutex_);
            future_ = std::move(fut);
            futureInstallationComplete_ = true;
        }
        futureInstalled_.notify_all();
    }

    /// Complete a start attempt that exited before spawning a coroutine.
    void markFutureNotExpected() {
        {
            std::lock_guard lock(futureMutex_);
            futureInstallationComplete_ = true;
        }
        futureInstalled_.notify_all();
    }

    /// Reset lifecycle state so the daemon can be re-initialized after a
    /// restart. Does not affect an in-flight future — callers must drain it
    /// first via requestStopAndWait().
    void resetForRestart() {
        std::lock_guard waitLock(waitMutex_);
        std::lock_guard lock(futureMutex_);
        stopSource_ = detail::AsyncInitStopSource{};
        started_.store(false, std::memory_order_release);
        metadataWarmupStarted_.store(false, std::memory_order_release);
        futureInstallationComplete_ = true;
    }

    /// Request cancellation and wait up to `timeout` for the coroutine to
    /// complete. Safe to call when no future has been stored.
    /// Returns true if the coroutine finished (or was never started),
    /// false if the wait timed out.
    bool requestStopAndWait(std::chrono::milliseconds timeout) {
        std::lock_guard waitLock(waitMutex_);
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        std::future<void> future;
        {
            std::unique_lock lock(futureMutex_);
            if (stopSource_.stop_possible()) {
                stopSource_.request_stop();
            }
            if (!futureInstalled_.wait_until(lock, deadline,
                                             [this] { return futureInstallationComplete_; })) {
                return false;
            }
            if (!future_.valid()) {
                return true;
            }
            future = std::move(future_);
        }
        if (future.wait_until(deadline) == std::future_status::timeout) {
            std::lock_guard lock(futureMutex_);
            future_ = std::move(future);
            return false;
        }
        try {
            future.get();
        } catch (...) {
            // Async initialization failures are surfaced through service state.
        }
        return true;
    }

private:
    std::atomic<bool> started_{false};
    std::atomic<bool> metadataWarmupStarted_{false};
    detail::AsyncInitStopSource stopSource_;
    std::mutex waitMutex_;
    std::mutex futureMutex_;
    std::condition_variable futureInstalled_;
    std::future<void> future_;
    bool futureInstallationComplete_{true};
};

} // namespace yams::daemon
