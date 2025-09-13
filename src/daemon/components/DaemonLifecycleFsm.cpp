#include <spdlog/spdlog.h>
#include <yams/daemon/components/DaemonLifecycleFsm.h>

namespace yams::daemon {

void DaemonLifecycleFsm::transitionTo(LifecycleState next, std::optional<std::string> err) {
    if (snapshot_.state == next && (!err || snapshot_.lastError == *err)) {
        return; // no-op
    }
    snapshot_.state = next;
    snapshot_.lastError = err.value_or("");
    snapshot_.lastTransition = std::chrono::steady_clock::now();
    spdlog::debug("Lifecycle transition -> {}{}", static_cast<int>(next),
                  snapshot_.lastError.empty() ? ""
                                              : (std::string{" error="} + snapshot_.lastError));
}

void DaemonLifecycleFsm::tick() {
    // Currently no time-based auto transitions; placeholder for future degraded->ready recovery,
    // etc.
}

void DaemonLifecycleFsm::dispatch(const BootstrappedEvent&) {
    switch (snapshot_.state) {
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
    switch (snapshot_.state) {
        case LifecycleState::Initializing:
        case LifecycleState::Degraded:
            transitionTo(LifecycleState::Ready);
            break;
        default:
            break;
    }
}

void DaemonLifecycleFsm::dispatch(const DegradedEvent&) {
    switch (snapshot_.state) {
        case LifecycleState::Ready:
        case LifecycleState::Initializing:
            transitionTo(LifecycleState::Degraded);
            break;
        default:
            break;
    }
}

void DaemonLifecycleFsm::dispatch(const FailureEvent& ev) {
    switch (snapshot_.state) {
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
    switch (snapshot_.state) {
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
    switch (snapshot_.state) {
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
    auto it = degraded_.find(name);
    return it != degraded_.end() && it->second;
}

std::string DaemonLifecycleFsm::degradationReason(const std::string& name) const {
    auto it = degradeReasons_.find(name);
    return it == degradeReasons_.end() ? std::string() : it->second;
}

} // namespace yams::daemon
