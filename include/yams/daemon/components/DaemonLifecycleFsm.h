#pragma once

#include <chrono>
#include <optional>
#include <string>

namespace yams::daemon {

// Unified daemon lifecycle state model.
// This is the source of truth for daemon readiness and phase.

enum class LifecycleState {
    Unknown = 0,
    Starting,
    Initializing,
    Ready,
    Degraded,
    Failed,
    Stopping,
    Stopped,
};

struct LifecycleSnapshot {
    LifecycleState state{LifecycleState::Unknown};
    std::string lastError; // empty when no error
    std::chrono::steady_clock::time_point lastTransition{};
};

// Events that can be dispatched to the FSM
struct BootstrappedEvent {};
struct HealthyEvent {};
struct DegradedEvent {};
struct FailureEvent {
    std::string error;
};
struct ShutdownRequestedEvent {};
struct StoppedEvent {};

class DaemonLifecycleFsm {
public:
    DaemonLifecycleFsm() = default;
    ~DaemonLifecycleFsm() = default;

    // Returns the latest known lifecycle snapshot.
    LifecycleSnapshot snapshot() const { return snapshot_; }

    // Advance time-based guards; lightweight and safe to call frequently.
    void tick();

    // Dispatch strongly-typed events
    void dispatch(const BootstrappedEvent&);
    void dispatch(const HealthyEvent&);
    void dispatch(const DegradedEvent&);
    void dispatch(const FailureEvent&);
    void dispatch(const ShutdownRequestedEvent&);
    void dispatch(const StoppedEvent&);

    // Convenience: reset to Unknown
    void reset();

private:
    void transitionTo(LifecycleState next, std::optional<std::string> err = std::nullopt);

    LifecycleSnapshot snapshot_{};
};

} // namespace yams::daemon
