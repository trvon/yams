#include <spdlog/spdlog.h>
#include <yams/daemon/components/DaemonLifecycleFsm.h>

namespace yams::daemon {

void DaemonLifecycleFsm::transitionTo(LifecycleState next, std::optional<std::string> err) {
    MutexLock lock(mutex_);
    if (snapshot_.state == next && (!err || snapshot_.lastError == *err)) {
        spdlog::debug("Lifecycle transition no-op: already in state {}", static_cast<int>(next));
        return; // no-op
    }
    auto prev = snapshot_.state;
    snapshot_.state = next;
    snapshot_.lastError = err.value_or("");
    snapshot_.lastTransition = std::chrono::steady_clock::now();
    spdlog::info("Lifecycle transition: {} -> {}{}", static_cast<int>(prev), static_cast<int>(next),
                 snapshot_.lastError.empty() ? "" : (std::string{" error="} + snapshot_.lastError));
}

void DaemonLifecycleFsm::tick() {
    // Currently no time-based auto transitions; placeholder for future degraded->ready recovery,
    // etc.
}

void DaemonLifecycleFsm::dispatch(const BootstrappedEvent&) {
    const auto current = snapshot();
    switch (current.state) {
        case LifecycleState::Unknown:
        case LifecycleState::Starting:
            transitionTo(LifecycleState::Initializing);
            break;
        default:
            // ignore
            break;
    }
}

void DaemonLifecycleFsm::dispatch(const HealthyEvent&) {
    const auto current = snapshot();
    switch (current.state) {
        case LifecycleState::Initializing:
        case LifecycleState::Degraded:
            transitionTo(LifecycleState::Ready);
            break;
        default:
            break;
    }
}

void DaemonLifecycleFsm::dispatch(const DegradedEvent&) {
    const auto current = snapshot();
    switch (current.state) {
        case LifecycleState::Ready:
        case LifecycleState::Initializing:
            transitionTo(LifecycleState::Degraded);
            break;
        default:
            break;
    }
}

void DaemonLifecycleFsm::dispatch(const FailureEvent& ev) {
    const auto current = snapshot();
    switch (current.state) {
        case LifecycleState::Unknown:
        case LifecycleState::Starting:
        case LifecycleState::Initializing:
        case LifecycleState::Ready:
        case LifecycleState::Degraded:
            transitionTo(LifecycleState::Failed, ev.error);
            break;
        default:
            break;
    }
}

void DaemonLifecycleFsm::dispatch(const ShutdownRequestedEvent&) {
    const auto current = snapshot();
    switch (current.state) {
        case LifecycleState::Unknown:
        case LifecycleState::Starting:
        case LifecycleState::Initializing:
        case LifecycleState::Ready:
        case LifecycleState::Degraded:
        case LifecycleState::Failed:
            transitionTo(LifecycleState::Stopping);
            break;
        default:
            break;
    }
}

void DaemonLifecycleFsm::dispatch(const StoppedEvent&) {
    const auto current = snapshot();
    switch (current.state) {
        case LifecycleState::Stopping:
        case LifecycleState::Failed:
            transitionTo(LifecycleState::Stopped);
            break;
        default:
            break;
    }
}

void DaemonLifecycleFsm::reset() {
    transitionTo(LifecycleState::Unknown);
}

void DaemonLifecycleFsm::setSubsystemDegraded(const std::string& name, bool degraded,
                                              const std::string& reason) {
    MutexLock lock(mutex_);
    auto it = degraded_.find(name);
    if (it == degraded_.end() || it->second != degraded) {
        degraded_[name] = degraded;
        if (!reason.empty())
            degradeReasons_[name] = reason;
        spdlog::warn("Subsystem '{}' degraded: {}{}", name, degraded ? "true" : "false",
                     reason.empty() ? "" : (std::string{" reason="} + reason));
    } else if (degraded && !reason.empty()) {
        // Update reason when degraded remains true
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
