#pragma once

#include <atomic>
#include <condition_variable>
#include <exception>
#include <mutex>
#include <new>
#include <optional>
#include <system_error>
#include <thread>
#include <utility>

#include <yams/core/assert.hpp>
#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/InitWaiterOwnership.h>

namespace yams::daemon::detail {

struct DaemonStopStateView {
    std::atomic<bool>& running;
    std::atomic<bool>& stopRequested;
    std::thread& waiter;
    std::mutex& completionMutex;
    std::condition_variable& completionCv;
    bool& completed;
    InitWaiterOwnership& ownership;
};

enum class InitWaiterAdmission { Created, AlreadyRegistered, Cancelled };

inline bool hasPendingStop(DaemonStopStateView state) {
    std::lock_guard lock(state.ownership.mutex);
    return state.waiter.joinable() || state.ownership.phase == InitWaiterClosePhase::Closing ||
           state.ownership.phase == InitWaiterClosePhase::FailedRetained;
}

inline bool isCurrentInitWaiter(DaemonStopStateView state) {
    std::lock_guard lock(state.ownership.mutex);
    const auto owner =
        state.waiter.joinable() ? state.waiter.get_id() : state.ownership.drainingOwner;
    return owner == std::this_thread::get_id();
}

inline Result<void> beginStart(DaemonStopStateView state) {
    std::lock_guard lock(state.ownership.mutex);
    if (state.running.load()) {
        return Error{ErrorCode::InvalidState, "Daemon already running"};
    }
    if (state.waiter.joinable() || state.ownership.phase == InitWaiterClosePhase::Closing ||
        state.ownership.phase == InitWaiterClosePhase::FailedRetained) {
        return Error{ErrorCode::ResourceBusy, "Previous initialization waiter close is incomplete"};
    }
    state.ownership.phase = InitWaiterClosePhase::Open;
    state.ownership.drainingOwner = {};
    state.running.store(true);
    return Result<void>();
}

template <typename Permission, typename Job>
Result<InitWaiterAdmission> registerInitWaiter(DaemonStopStateView state, Permission permission,
                                               Job job) {
    const auto preflight = [&]() -> std::optional<InitWaiterAdmission> {
        if (!state.running.load(std::memory_order_acquire) ||
            state.stopRequested.load(std::memory_order_acquire) ||
            state.ownership.phase != InitWaiterClosePhase::Open) {
            return InitWaiterAdmission::Cancelled;
        }
        if (state.waiter.joinable()) {
            return InitWaiterAdmission::AlreadyRegistered;
        }
        return std::nullopt;
    };
    {
        std::lock_guard lock(state.ownership.mutex);
        if (auto existing = preflight()) {
            return *existing;
        }
    }

    // Governor and test observations may lock/log/block: never run them under the slot gate.
    bool permitted = false;
    std::optional<Error> decisionError;
    try {
        permitted = permission();
    } catch (const std::exception& error) {
        decisionError.emplace(ErrorCode::InternalError, error.what());
    } catch (...) {
        decisionError.emplace(ErrorCode::InternalError, "Initialization admission query failed");
    }

    std::lock_guard lock(state.ownership.mutex);
    if (auto existing = preflight()) {
        return *existing;
    }
    if (decisionError) {
        return *decisionError;
    }
    if (!permitted) {
        return Error{ErrorCode::ResourceExhausted, "Initialization waiter admission denied; retry"};
    }
    try {
        // Only construction/publication occurs under the gate. The job runs on its own thread.
        state.waiter = std::thread(std::move(job));
    } catch (const std::system_error& error) {
        const auto exhausted = error.code() == std::errc::resource_unavailable_try_again ||
                               error.code() == std::errc::not_enough_memory;
        return Error{exhausted ? ErrorCode::ResourceExhausted : ErrorCode::InternalError,
                     error.what()};
    } catch (const std::bad_alloc& error) {
        return Error{ErrorCode::ResourceExhausted, error.what()};
    } catch (const std::exception& error) {
        return Error{ErrorCode::InternalError, error.what()};
    }
    return InitWaiterAdmission::Created;
}

struct StopCompletionGuard {
    DaemonStopStateView state;
    // Explicit success only; unwinding or returning a checked error cannot complete a close.
    void commit() {
        std::lock_guard lock(state.completionMutex);
        state.completed = true;
    }
};

template <typename Snapshot, typename AlreadyStopping, typename Body>
Result<void> runStopFlow(DaemonStopStateView state, Snapshot snapshot,
                         AlreadyStopping alreadyStopping, Body body) {
    bool claimed = false;
    {
        std::lock_guard lock(state.ownership.mutex);
        const auto owner =
            state.waiter.joinable() ? state.waiter.get_id() : state.ownership.drainingOwner;
        if (owner == std::this_thread::get_id()) {
            return Error{ErrorCode::ResourceBusy,
                         "Initialization waiter cannot synchronously stop itself"};
        }
        if (state.ownership.phase == InitWaiterClosePhase::Closing) {
            return Error{ErrorCode::ResourceBusy, "Daemon close is already being drained"};
        }
        const bool retry = state.ownership.phase == InitWaiterClosePhase::FailedRetained;
        claimed = state.running.exchange(false) || retry;
        if (claimed) {
            state.ownership.phase = InitWaiterClosePhase::Closing;
            state.stopRequested.store(true, std::memory_order_release);
        }
    }
    if (!claimed) {
        const auto lifecycle = snapshot();
        if (state.stopRequested.load(std::memory_order_acquire) ||
            lifecycle.state == LifecycleState::Stopping ||
            lifecycle.state == LifecycleState::Stopped) {
            alreadyStopping();
            return Result<void>();
        }
        return Error{ErrorCode::InvalidState, "Daemon not running"};
    }
    state.completionCv.notify_all();
    {
        std::lock_guard lock(state.completionMutex);
        state.completed = false;
    }
    StopCompletionGuard completion{state};
    auto outcome = [&]() -> Result<void> {
        try {
            auto result = body();
            if (!result) {
                return result.error();
            }
            if (snapshot().state != LifecycleState::Stopped) {
                return Error{ErrorCode::InvalidState, "Daemon close did not reach Stopped"};
            }
            return Result<void>();
        } catch (const std::exception& error) {
            return Error{ErrorCode::InternalError, error.what()};
        } catch (...) {
            return Error{ErrorCode::InternalError, "Unexpected daemon close failure"};
        }
    }();
    // Keep Closing (and therefore start/other close excluded) until completion is published.
    if (outcome) {
        completion.commit();
    }
    {
        std::lock_guard lock(state.ownership.mutex);
        state.ownership.phase =
            outcome ? InitWaiterClosePhase::Closed : InitWaiterClosePhase::FailedRetained;
        if (outcome) {
            state.ownership.drainingOwner = {};
        }
    }
    // Publish the closed phase before notifying; the predicate reads it atomically under the
    // completion mutex. Taking that mutex here prevents a missed wake without lock nesting.
    {
        std::lock_guard lock(state.completionMutex);
        state.completionCv.notify_all();
    }
    return outcome;
}

inline void signalStopRequest(DaemonStopStateView state) {
    state.stopRequested.store(true, std::memory_order_release);
    state.completionCv.notify_all();
}

inline void requestLifecycleClose(DaemonLifecycleFsm& lifecycle) {
    const auto beforeShutdown = lifecycle.snapshot().state;
    if (beforeShutdown != LifecycleState::Stopping && beforeShutdown != LifecycleState::Stopped &&
        beforeShutdown != LifecycleState::Failed) {
        lifecycle.dispatch(ShutdownRequestedEvent{});
    }
}

template <typename BeforeJoin, typename AfterJoin>
Result<void> joinInitWaiter(DaemonStopStateView state, BeforeJoin beforeJoin, AfterJoin afterJoin) {
    std::thread pending;
    {
        std::lock_guard lock(state.ownership.mutex);
        if (state.waiter.joinable()) {
            state.ownership.drainingOwner = state.waiter.get_id();
            pending = std::move(state.waiter);
        }
    }
    if (!pending.joinable()) {
        return Result<void>();
    }
    // Restore BEFORE formatting an error or destroying a joinable local, including hook failures.
    const auto restore = [&] {
        std::lock_guard lock(state.ownership.mutex);
        if (pending.joinable()) {
            YAMS_ASSERT(!state.waiter.joinable(), "Closed waiter slot must remain unoccupied");
            state.waiter = std::move(pending);
        } else {
            state.ownership.drainingOwner = {};
        }
    };
    try {
        beforeJoin();
        pending.join();
        afterJoin();
    } catch (const std::exception& error) {
        restore();
        return Error{ErrorCode::InternalError, error.what()};
    } catch (...) {
        restore();
        return Error{ErrorCode::InternalError, "Unexpected initialization waiter join failure"};
    }
    {
        std::lock_guard lock(state.ownership.mutex);
        state.ownership.drainingOwner = {};
    }
    return Result<void>();
}

} // namespace yams::daemon::detail
