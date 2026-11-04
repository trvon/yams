#pragma once

#include <atomic>
#include <mutex>
#include <thread>

namespace yams::daemon {

// This is the thread slot's close protocol, not a second daemon lifecycle FSM.
enum class InitWaiterClosePhase { Open, Closing, FailedRetained, Closed };

struct InitWaiterOwnership {
    std::mutex mutex;
    // Atomic read by the completion predicate avoids nesting completion and slot mutexes.
    std::atomic<InitWaiterClosePhase> phase{InitWaiterClosePhase::Open};
    // Remains valid while the slot has been moved into an external join operation.
    std::thread::id drainingOwner{};
};

} // namespace yams::daemon
