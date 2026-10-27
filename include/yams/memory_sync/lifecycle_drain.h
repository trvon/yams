// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors
#pragma once

#include <mutex>
#include <utility>

#include <yams/core/assert.hpp>

namespace yams::memory_sync::detail {

struct CallbackDrainObserver {
    void operator()() const noexcept {}
};

/// Shared callback-serialization boundary for restart and stop. The synchronous
/// observer exposes the boundary without adding a mutable service hook or changing
/// its layout. On return, both lifecycle and callback locks are owned by the caller.
template <typename BeforeWait = CallbackDrainObserver>
std::unique_lock<std::recursive_mutex> acquireCallbackDrain(std::unique_lock<std::mutex>& lifecycle,
                                                            std::recursive_mutex& callbacks,
                                                            BeforeWait&& beforeWait = {}) {
    YAMS_PRECONDITION(lifecycle.owns_lock(), "callback drain requires the lifecycle gate");
    // Callbacks may enter another service's lifecycle. Never wait for their
    // process-wide serializer while retaining this service's lifecycle gate.
    lifecycle.unlock();
    std::forward<BeforeWait>(beforeWait)();
    std::unique_lock<std::recursive_mutex> callbackDrain(callbacks);
    lifecycle.lock();
    YAMS_POSTCONDITION(lifecycle.owns_lock() && callbackDrain.owns_lock(),
                       "callback drain must return both gates");
    return callbackDrain;
}

} // namespace yams::memory_sync::detail
