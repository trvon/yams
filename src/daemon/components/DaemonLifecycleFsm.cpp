#include <yams/daemon/components/DaemonLifecycleFsm.h>

#include <spdlog/spdlog.h>
#include <yams/core/assert.hpp>

namespace yams::daemon {

// Transitions match the former tinyfsm machine. Events a state does not handle are ignored.
// FailureEvent moves every state except Failed, Stopping and Stopped to Failed; ShutdownRequested
// moves every state except Stopping and Stopped to Stopping. Entering Unknown, Starting,
// Initializing, Ready, Stopping or Stopped clears lastError; Degraded and Failed keep it.
void DaemonLifecycleFsm::enterLocked(LifecycleState next) {
    switch (next) {
        case LifecycleState::Degraded:
        case LifecycleState::Failed:
            break;
        default:
            snap_.lastError.clear();
            break;
    }
    const auto prev = snap_.state;
    snap_.state = next;
    snap_.lastTransition = std::chrono::steady_clock::now();
    if (prev != next || !snap_.lastError.empty()) {
        spdlog::info("Lifecycle transition: {} -> {}{}", static_cast<int>(prev),
                     static_cast<int>(next),
                     snap_.lastError.empty() ? "" : (std::string{" error="} + snap_.lastError));
    }
}

void DaemonLifecycleFsm::applyLocked(const BootstrappedEvent&) {
    if (snap_.state == LifecycleState::Unknown || snap_.state == LifecycleState::Starting) {
        spdlog::info("BootstrappedEvent received, current state={}", static_cast<int>(snap_.state));
        enterLocked(LifecycleState::Initializing);
    }
}

void DaemonLifecycleFsm::applyLocked(const HealthyEvent&) {
    if (snap_.state == LifecycleState::Initializing || snap_.state == LifecycleState::Degraded) {
        spdlog::info("HealthyEvent received, current state={}", static_cast<int>(snap_.state));
        enterLocked(LifecycleState::Ready);
    }
}

void DaemonLifecycleFsm::applyLocked(const DegradedEvent&) {
    if (snap_.state == LifecycleState::Initializing || snap_.state == LifecycleState::Ready) {
        enterLocked(LifecycleState::Degraded);
    }
}

void DaemonLifecycleFsm::applyLocked(const FailureEvent& ev) {
    switch (snap_.state) {
        case LifecycleState::Failed:
        case LifecycleState::Stopping:
        case LifecycleState::Stopped:
            return;
        default:
            snap_.lastError = ev.error;
            enterLocked(LifecycleState::Failed);
    }
}

void DaemonLifecycleFsm::applyLocked(const ShutdownRequestedEvent&) {
    if (snap_.state != LifecycleState::Stopping && snap_.state != LifecycleState::Stopped) {
        enterLocked(LifecycleState::Stopping);
    }
}

void DaemonLifecycleFsm::applyLocked(const StoppedEvent&) {
    if (snap_.state == LifecycleState::Failed || snap_.state == LifecycleState::Stopping) {
        enterLocked(LifecycleState::Stopped);
    }
}

namespace {

bool canDispatch(LifecycleState state, const BootstrappedEvent&) {
    return state == LifecycleState::Unknown || state == LifecycleState::Starting;
}

bool canDispatch(LifecycleState state, const HealthyEvent&) {
    return state == LifecycleState::Initializing || state == LifecycleState::Degraded;
}

bool canDispatch(LifecycleState state, const DegradedEvent&) {
    return state == LifecycleState::Initializing || state == LifecycleState::Ready;
}

bool canDispatch(LifecycleState state, const FailureEvent&) {
    return state != LifecycleState::Stopped;
}

bool canDispatch(LifecycleState state, const ShutdownRequestedEvent&) {
    return state != LifecycleState::Stopped;
}

bool canDispatch(LifecycleState state, const StoppedEvent&) {
    return state == LifecycleState::Failed || state == LifecycleState::Stopping ||
           state == LifecycleState::Stopped;
}

template <typename Event>
void validateDispatch(LifecycleState state, const Event& ev, const char* message) {
    YAMS_DCHECK(canDispatch(state, ev), message);
}

} // namespace

DaemonLifecycleFsm::DaemonLifecycleFsm() {
    snap_.lastTransition = std::chrono::steady_clock::now();
}

LifecycleSnapshot DaemonLifecycleFsm::snapshot() const {
    MutexLock lock(mutex_);
    return snap_;
}

void DaemonLifecycleFsm::tick() {}

void DaemonLifecycleFsm::dispatch(const BootstrappedEvent& ev) {
    validateDispatch(snapshot().state, ev,
                     "DaemonLifecycleFsm BootstrappedEvent requires Unknown or Starting state");
    MutexLock lock(mutex_);
    applyLocked(ev);
}

void DaemonLifecycleFsm::dispatch(const HealthyEvent& ev) {
    validateDispatch(snapshot().state, ev,
                     "DaemonLifecycleFsm HealthyEvent requires Initializing or Degraded state");
    MutexLock lock(mutex_);
    applyLocked(ev);
}

void DaemonLifecycleFsm::dispatch(const DegradedEvent& ev) {
    validateDispatch(snapshot().state, ev,
                     "DaemonLifecycleFsm DegradedEvent requires Initializing or Ready state");
    MutexLock lock(mutex_);
    applyLocked(ev);
}

void DaemonLifecycleFsm::dispatch(const FailureEvent& ev) {
    validateDispatch(snapshot().state, ev,
                     "DaemonLifecycleFsm FailureEvent must not arrive after Stopped state");
    MutexLock lock(mutex_);
    applyLocked(ev);
}

void DaemonLifecycleFsm::dispatch(const ShutdownRequestedEvent& ev) {
    validateDispatch(
        snapshot().state, ev,
        "DaemonLifecycleFsm ShutdownRequestedEvent must not arrive after Stopped state");
    MutexLock lock(mutex_);
    applyLocked(ev);
}

void DaemonLifecycleFsm::dispatch(const StoppedEvent& ev) {
    validateDispatch(snapshot().state, ev,
                     "DaemonLifecycleFsm StoppedEvent requires Failed, Stopping, or Stopped state");
    MutexLock lock(mutex_);
    applyLocked(ev);
}

Result<bool>
DaemonLifecycleFsm::tryCommitInitializationReadiness(const std::atomic<bool>& stopRequested,
                                                     bool providerUnavailable) {
    MutexLock lock(mutex_);
    const auto state = snap_.state;
    if (stopRequested.load(std::memory_order_acquire) || state == LifecycleState::Stopping ||
        state == LifecycleState::Stopped || state == LifecycleState::Failed) {
        return false;
    }

    const bool allowed = providerUnavailable ? canDispatch(state, DegradedEvent{})
                                             : canDispatch(state, HealthyEvent{});
    if (!allowed) {
        return Error{ErrorCode::InvalidState,
                     "Initialization readiness cannot commit in the current lifecycle state"};
    }
    if (providerUnavailable) {
        validateDispatch(state, DegradedEvent{},
                         "Initialization degradation requires Initializing or Ready state");
    } else {
        validateDispatch(state, HealthyEvent{},
                         "Initialization readiness requires Initializing or Degraded state");
    }

    setSubsystemDegradedLocked(
        "model_provider", providerUnavailable,
        providerUnavailable
            ? "Model provider unavailable; embeddings disabled until provider recovery"
            : std::string{});
    if (providerUnavailable) {
        applyLocked(DegradedEvent{});
    } else {
        applyLocked(HealthyEvent{});
    }
    return true;
}

void DaemonLifecycleFsm::reset() {
    MutexLock lock(mutex_);
    snap_ = {};
    snap_.lastTransition = std::chrono::steady_clock::now();
}

void DaemonLifecycleFsm::setSubsystemDegraded(const std::string& name, bool degraded,
                                              const std::string& reason) {
    YAMS_PRECONDITION(!name.empty(),
                      "DaemonLifecycleFsm subsystem degradation tracking requires a name");
    MutexLock lock(mutex_);
    setSubsystemDegradedLocked(name, degraded, reason);
}

void DaemonLifecycleFsm::setSubsystemDegradedLocked(const std::string& name, bool degraded,
                                                    const std::string& reason) {
    auto it = degraded_.find(name);
    if (it == degraded_.end() || it->second != degraded) {
        degraded_[name] = degraded;
        if (degraded && !reason.empty()) {
            degradeReasons_[name] = reason;
        } else if (!degraded) {
            degradeReasons_.erase(name);
        }
        const auto suffix = reason.empty() ? std::string{} : (std::string{" reason="} + reason);
        if (degraded) {
            spdlog::warn("Subsystem '{}' degraded: true{}", name, suffix);
        } else {
            spdlog::info("Subsystem '{}' degraded: false{}", name, suffix);
        }
    } else if (degraded && !reason.empty()) {
        degradeReasons_[name] = reason;
    }
}

bool DaemonLifecycleFsm::isSubsystemDegraded(const std::string& name) const {
    MutexLock lock(mutex_);
    auto it = degraded_.find(name);
    return it != degraded_.end() && it->second;
}

std::string DaemonLifecycleFsm::degradationReason(const std::string& name) const {
    MutexLock lock(mutex_);
    auto it = degradeReasons_.find(name);
    return it == degradeReasons_.end() ? std::string() : it->second;
}

} // namespace yams::daemon
