#pragma once

#include <atomic>
#include <chrono>
#include <map>
#include <mutex>
#include <optional>
#include <string>

#include <yams/core/types.h>

namespace yams::daemon {

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
    std::string lastError;
    std::chrono::steady_clock::time_point lastTransition{};
};

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
    DaemonLifecycleFsm();
    ~DaemonLifecycleFsm() = default;

    LifecycleSnapshot snapshot() const;

    void tick();

    void dispatch(const BootstrappedEvent&);
    void dispatch(const HealthyEvent&);
    void dispatch(const DegradedEvent&);
    void dispatch(const FailureEvent&);
    void dispatch(const ShutdownRequestedEvent&);
    void dispatch(const StoppedEvent&);

    // True: admitted; false: cancellation/terminal close; error: unexpected state.
    // Check, provider bookkeeping and transition share the existing FSM mutex.
    Result<bool> tryCommitInitializationReadiness(const std::atomic<bool>& stopRequested,
                                                  bool providerUnavailable);

    void reset();

    void setSubsystemDegraded(const std::string& name, bool degraded,
                              const std::string& reason = std::string());
    bool isSubsystemDegraded(const std::string& name) const;
    std::map<std::string, bool> degradedSubsystems() const {
        MutexLock lock(sharedMutex());
        return degraded_;
    }
    std::string degradationReason(const std::string& name) const;

private:
    void setSubsystemDegradedLocked(const std::string& name, bool degraded,
                                    const std::string& reason);

#if defined(__cpp_lib_scoped_lock) && __cpp_lib_scoped_lock >= 201703L
    using MutexLock = std::scoped_lock<std::mutex>;
#else
    using MutexLock = std::lock_guard<std::mutex>;
#endif

    static std::mutex& sharedMutex() {
        static std::mutex mutex;
        return mutex;
    }

    std::map<std::string, bool> degraded_;
    std::map<std::string, std::string> degradeReasons_;
};

} // namespace yams::daemon
