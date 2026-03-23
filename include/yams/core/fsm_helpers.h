#pragma once

#include <chrono>
#include <memory>
#include <stdexcept>
#include <thread>
#include <yams/daemon/ipc/connection_fsm.h>
#include <yams/daemon/ipc/fsm_helpers.h>

namespace yams {
namespace core {

// Lightweight guard helpers to keep client code simple and consistent
inline void require_can_read(const std::shared_ptr<daemon::ConnectionFsm>& fsm) {
    if (fsm) {
        daemon::fsm_helpers::require_can_read(*fsm);
    }
}

inline void require_can_write(const std::shared_ptr<daemon::ConnectionFsm>& fsm) {
    if (fsm) {
        daemon::fsm_helpers::require_can_write(*fsm);
    }
}

inline void validate_op(const std::shared_ptr<daemon::ConnectionFsm>& fsm,
                        daemon::ConnectionFsm::Operation op) {
    if (!fsm)
        return;
    daemon::fsm_helpers::validate_op(*fsm, op);
}

namespace fsm {

// Wait for FSM-like object to reach a state. Assumes fsm.snapshot().state is comparable.
template <typename FsmT, typename StateT>
bool wait_for_state(const FsmT& fsm, StateT targetState, std::chrono::milliseconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (fsm.snapshot().state != targetState) {
        if (std::chrono::steady_clock::now() >= deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return true;
}

// Require a specific state; throws if not satisfied.
template <typename FsmT, typename StateT>
void require_state(const FsmT& fsm, StateT requiredState, const char* context = nullptr) {
    if (fsm.snapshot().state != requiredState) {
        std::string msg = "FSM not in required state";
        if (context)
            msg += std::string(" (") + context + ")";
        throw std::runtime_error(msg);
    }
}

// Check if current state is any of the provided ones.
template <typename FsmT, typename... StatesT>
bool is_in_any_state(const FsmT& fsm, StatesT... states) {
    auto current = fsm.snapshot().state;
    return ((current == states) || ...);
}

} // namespace fsm

} // namespace core
} // namespace yams
