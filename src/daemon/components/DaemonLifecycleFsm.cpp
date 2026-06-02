#include <yams/daemon/components/DaemonLifecycleFsm.h>

#include <spdlog/spdlog.h>
#include <tinyfsm.hpp>
#include <yams/core/assert.hpp>

namespace yams::daemon {
namespace detail {

struct LCUnknown;
struct LCStarting;
struct LCInitializing;
struct LCReady;
struct LCDegraded;
struct LCFailed;
struct LCStopping;
struct LCStopped;

struct LifecycleMachine : tinyfsm::MooreMachine<LifecycleMachine> {
    static LifecycleSnapshot snap;

    virtual void react(const BootstrappedEvent&) {}
    virtual void react(const HealthyEvent&) {}
    virtual void react(const DegradedEvent&) {}
    virtual void react(const StoppedEvent&) {}

    virtual void react(const FailureEvent& ev);
    virtual void react(const ShutdownRequestedEvent&);

protected:
    void applyEntry(LifecycleState next) {
        auto prev = snap.state;
        snap.state = next;
        snap.lastTransition = std::chrono::steady_clock::now();
        if (prev != next || !snap.lastError.empty()) {
            spdlog::info("Lifecycle transition: {} -> {}{}", static_cast<int>(prev),
                         static_cast<int>(next),
                         snap.lastError.empty() ? "" : (std::string{" error="} + snap.lastError));
        }
    }
};

LifecycleSnapshot LifecycleMachine::snap{};

struct LCUnknown : LifecycleMachine {
    void entry() override {
        snap.lastError.clear();
        applyEntry(LifecycleState::Unknown);
    }
    void react(const BootstrappedEvent&) override {
        spdlog::info("BootstrappedEvent received, current state={}", static_cast<int>(snap.state));
        transit<LCInitializing>();
    }
};

struct LCStarting : LifecycleMachine {
    void entry() override {
        snap.lastError.clear();
        applyEntry(LifecycleState::Starting);
    }
    void react(const BootstrappedEvent&) override {
        spdlog::info("BootstrappedEvent received, current state={}", static_cast<int>(snap.state));
        transit<LCInitializing>();
    }
};

struct LCInitializing : LifecycleMachine {
    void entry() override {
        snap.lastError.clear();
        applyEntry(LifecycleState::Initializing);
    }
    void react(const HealthyEvent&) override {
        spdlog::info("HealthyEvent received, current state={}", static_cast<int>(snap.state));
        transit<LCReady>();
    }
    void react(const DegradedEvent&) override { transit<LCDegraded>(); }
};

struct LCReady : LifecycleMachine {
    void entry() override {
        snap.lastError.clear();
        applyEntry(LifecycleState::Ready);
    }
    void react(const DegradedEvent&) override { transit<LCDegraded>(); }
};

struct LCDegraded : LifecycleMachine {
    void entry() override { applyEntry(LifecycleState::Degraded); }
    void react(const HealthyEvent&) override {
        spdlog::info("HealthyEvent received, current state={}", static_cast<int>(snap.state));
        transit<LCReady>();
    }
};

struct LCFailed : LifecycleMachine {
    void entry() override { applyEntry(LifecycleState::Failed); }
    void react(const FailureEvent&) override {}
    void react(const StoppedEvent&) override { transit<LCStopped>(); }
};

struct LCStopping : LifecycleMachine {
    void entry() override {
        snap.lastError.clear();
        applyEntry(LifecycleState::Stopping);
    }
    void react(const FailureEvent&) override {}
    void react(const ShutdownRequestedEvent&) override {}
    void react(const StoppedEvent&) override { transit<LCStopped>(); }
};

struct LCStopped : LifecycleMachine {
    void entry() override {
        snap.lastError.clear();
        applyEntry(LifecycleState::Stopped);
    }
    void react(const FailureEvent&) override {}
    void react(const ShutdownRequestedEvent&) override {}
};

void LifecycleMachine::react(const FailureEvent& ev) {
    snap.lastError = ev.error;
    transit<LCFailed>();
}

void LifecycleMachine::react(const ShutdownRequestedEvent&) {
    transit<LCStopping>();
}

} // namespace detail
} // namespace yams::daemon

FSM_INITIAL_STATE(yams::daemon::detail::LifecycleMachine, yams::daemon::detail::LCUnknown)

namespace yams::daemon {

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
    return state == LifecycleState::Failed || state == LifecycleState::Stopping;
}

template <typename Event>
void validateDispatch(LifecycleState state, const Event& ev, const char* message) {
    YAMS_DCHECK(canDispatch(state, ev), message);
}

} // namespace

DaemonLifecycleFsm::DaemonLifecycleFsm() {
    MutexLock lock(sharedMutex());
    detail::LifecycleMachine::snap = {};
    detail::LifecycleMachine::start();
}

LifecycleSnapshot DaemonLifecycleFsm::snapshot() const {
    MutexLock lock(sharedMutex());
    return detail::LifecycleMachine::snap;
}

void DaemonLifecycleFsm::tick() {}

void DaemonLifecycleFsm::dispatch(const BootstrappedEvent& ev) {
    validateDispatch(snapshot().state, ev,
                     "DaemonLifecycleFsm BootstrappedEvent requires Unknown or Starting state");
    MutexLock lock(sharedMutex());
    detail::LifecycleMachine::dispatch(ev);
}

void DaemonLifecycleFsm::dispatch(const HealthyEvent& ev) {
    validateDispatch(snapshot().state, ev,
                     "DaemonLifecycleFsm HealthyEvent requires Initializing or Degraded state");
    MutexLock lock(sharedMutex());
    detail::LifecycleMachine::dispatch(ev);
}

void DaemonLifecycleFsm::dispatch(const DegradedEvent& ev) {
    validateDispatch(snapshot().state, ev,
                     "DaemonLifecycleFsm DegradedEvent requires Initializing or Ready state");
    MutexLock lock(sharedMutex());
    detail::LifecycleMachine::dispatch(ev);
}

void DaemonLifecycleFsm::dispatch(const FailureEvent& ev) {
    validateDispatch(snapshot().state, ev,
                     "DaemonLifecycleFsm FailureEvent must not arrive after Stopped state");
    MutexLock lock(sharedMutex());
    detail::LifecycleMachine::dispatch(ev);
}

void DaemonLifecycleFsm::dispatch(const ShutdownRequestedEvent& ev) {
    validateDispatch(
        snapshot().state, ev,
        "DaemonLifecycleFsm ShutdownRequestedEvent must not arrive after Stopped state");
    MutexLock lock(sharedMutex());
    detail::LifecycleMachine::dispatch(ev);
}

void DaemonLifecycleFsm::dispatch(const StoppedEvent& ev) {
    validateDispatch(snapshot().state, ev,
                     "DaemonLifecycleFsm StoppedEvent requires Failed or Stopping state");
    MutexLock lock(sharedMutex());
    detail::LifecycleMachine::dispatch(ev);
}

void DaemonLifecycleFsm::reset() {
    MutexLock lock(sharedMutex());
    detail::LifecycleMachine::snap = {};
    detail::LifecycleMachine::start();
}

void DaemonLifecycleFsm::setSubsystemDegraded(const std::string& name, bool degraded,
                                              const std::string& reason) {
    YAMS_PRECONDITION(!name.empty(),
                      "DaemonLifecycleFsm subsystem degradation tracking requires a name");
    MutexLock lock(sharedMutex());
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
    MutexLock lock(sharedMutex());
    auto it = degraded_.find(name);
    return it != degraded_.end() && it->second;
}

std::string DaemonLifecycleFsm::degradationReason(const std::string& name) const {
    MutexLock lock(sharedMutex());
    auto it = degradeReasons_.find(name);
    return it == degradeReasons_.end() ? std::string() : it->second;
}

} // namespace yams::daemon
