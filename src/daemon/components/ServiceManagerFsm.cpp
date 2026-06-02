#include <yams/daemon/components/ServiceManagerFsm.h>

#include <spdlog/spdlog.h>
#include <tinyfsm.hpp>
#include <yams/core/assert.hpp>

namespace yams::daemon {
namespace detail {

struct SMUninitialized;
struct SMOpeningDatabase;
struct SMDatabaseReady;
struct SMMigratingSchema;
struct SMSchemaReady;
struct SMVectorsReady;
struct SMBuildingSearchEngine;
struct SMReady;
struct SMShuttingDown;
struct SMStopped;
struct SMFailed;

struct ServiceManagerMachine : tinyfsm::MooreMachine<ServiceManagerMachine> {
    static ServiceManagerSnapshot snap;

    virtual void react(const OpeningDatabaseEvent&) {}
    virtual void react(const DatabaseOpenedEvent&) {}
    virtual void react(const MigrationStartedEvent&) {}
    virtual void react(const MigrationCompletedEvent&) {}
    virtual void react(const VectorsInitializedEvent&) {}
    virtual void react(const SearchEngineBuildStartedEvent&) {}
    virtual void react(const SearchEngineBuiltEvent&) {}
    virtual void react(const InitializationFailedEvent& ev);
    virtual void react(const ShutdownEvent&);
    virtual void react(const ServiceManagerStoppedEvent&) {}

protected:
    void applyEntry(ServiceManagerState next) {
        auto prev = snap.state;
        snap.state = next;
        snap.lastTransition = std::chrono::steady_clock::now();
        spdlog::info("[ServiceManagerFSM] {} -> {}", static_cast<int>(prev),
                     static_cast<int>(next));
    }
};

ServiceManagerSnapshot ServiceManagerMachine::snap{};

struct SMUninitialized : ServiceManagerMachine {
    void entry() override { applyEntry(ServiceManagerState::Uninitialized); }
    void react(const OpeningDatabaseEvent&) override { transit<SMOpeningDatabase>(); }
};

struct SMOpeningDatabase : ServiceManagerMachine {
    void entry() override { applyEntry(ServiceManagerState::OpeningDatabase); }
    void react(const DatabaseOpenedEvent&) override { transit<SMDatabaseReady>(); }
};

struct SMDatabaseReady : ServiceManagerMachine {
    void entry() override { applyEntry(ServiceManagerState::DatabaseReady); }
    void react(const MigrationStartedEvent&) override { transit<SMMigratingSchema>(); }
};

struct SMMigratingSchema : ServiceManagerMachine {
    void entry() override { applyEntry(ServiceManagerState::MigratingSchema); }
    void react(const MigrationCompletedEvent&) override { transit<SMSchemaReady>(); }
};

struct SMSchemaReady : ServiceManagerMachine {
    void entry() override { applyEntry(ServiceManagerState::SchemaReady); }
    void react(const VectorsInitializedEvent&) override { transit<SMVectorsReady>(); }
    void react(const SearchEngineBuildStartedEvent&) override { transit<SMBuildingSearchEngine>(); }
};

struct SMVectorsReady : ServiceManagerMachine {
    void entry() override { applyEntry(ServiceManagerState::VectorsReady); }
    void react(const SearchEngineBuildStartedEvent&) override { transit<SMBuildingSearchEngine>(); }
};

struct SMBuildingSearchEngine : ServiceManagerMachine {
    void entry() override { applyEntry(ServiceManagerState::BuildingSearchEngine); }
    void react(const SearchEngineBuiltEvent&) override { transit<SMReady>(); }
};

struct SMReady : ServiceManagerMachine {
    void entry() override { applyEntry(ServiceManagerState::Ready); }
    void react(const InitializationFailedEvent&) override {}
};

struct SMShuttingDown : ServiceManagerMachine {
    void entry() override { applyEntry(ServiceManagerState::ShuttingDown); }
    void react(const InitializationFailedEvent&) override {}
    void react(const ShutdownEvent&) override {}
    void react(const ServiceManagerStoppedEvent&) override { transit<SMStopped>(); }
};

struct SMStopped : ServiceManagerMachine {
    void entry() override { applyEntry(ServiceManagerState::Stopped); }
    void react(const InitializationFailedEvent&) override {}
    void react(const ShutdownEvent&) override {}
};

struct SMFailed : ServiceManagerMachine {
    void entry() override { applyEntry(ServiceManagerState::Failed); }
    void react(const InitializationFailedEvent&) override {}
};

void ServiceManagerMachine::react(const InitializationFailedEvent& ev) {
    snap.lastError = ev.error;
    transit<SMFailed>();
}

void ServiceManagerMachine::react(const ShutdownEvent&) {
    transit<SMShuttingDown>();
}

} // namespace detail
} // namespace yams::daemon

FSM_INITIAL_STATE(yams::daemon::detail::ServiceManagerMachine,
                  yams::daemon::detail::SMUninitialized)

namespace yams::daemon {

namespace {

bool canDispatch(ServiceManagerState state, const OpeningDatabaseEvent&) {
    return state == ServiceManagerState::Uninitialized;
}

bool canDispatch(ServiceManagerState state, const DatabaseOpenedEvent&) {
    return state == ServiceManagerState::OpeningDatabase;
}

bool canDispatch(ServiceManagerState state, const MigrationStartedEvent&) {
    return state == ServiceManagerState::DatabaseReady;
}

bool canDispatch(ServiceManagerState state, const MigrationCompletedEvent&) {
    return state == ServiceManagerState::MigratingSchema;
}

bool canDispatch(ServiceManagerState state, const VectorsInitializedEvent&) {
    return state == ServiceManagerState::SchemaReady ||
           state == ServiceManagerState::VectorsReady ||
           state == ServiceManagerState::BuildingSearchEngine ||
           state == ServiceManagerState::Ready;
}

bool canDispatch(ServiceManagerState state, const SearchEngineBuildStartedEvent&) {
    return state == ServiceManagerState::SchemaReady || state == ServiceManagerState::VectorsReady;
}

bool canDispatch(ServiceManagerState state, const SearchEngineBuiltEvent&) {
    return state == ServiceManagerState::BuildingSearchEngine;
}

bool canDispatch(ServiceManagerState state, const InitializationFailedEvent&) {
    return state != ServiceManagerState::Ready && state != ServiceManagerState::ShuttingDown &&
           state != ServiceManagerState::Stopped && state != ServiceManagerState::Failed;
}

bool canDispatch(ServiceManagerState state, const ServiceManagerStoppedEvent&) {
    return state == ServiceManagerState::ShuttingDown;
}

template <typename Event>
void validateDispatch(ServiceManagerState state, const Event& ev, const char* message) {
    YAMS_DCHECK(canDispatch(state, ev), message);
}

} // namespace

static bool isTerminal(ServiceManagerState s) {
    return s == ServiceManagerState::Ready || s == ServiceManagerState::Failed ||
           s == ServiceManagerState::Stopped || s == ServiceManagerState::ShuttingDown;
}

template <typename Event>
void dispatchNoThrow(std::mutex& mutex, std::condition_variable& cv, const Event& ev) noexcept {
    try {
        std::lock_guard<std::mutex> lock(mutex);
        detail::ServiceManagerMachine::dispatch(ev);
        if (isTerminal(detail::ServiceManagerMachine::snap.state))
            cv.notify_all();
    } catch (const std::exception& e) {
        spdlog::debug("[ServiceManagerFSM] dispatchNoThrow swallowed exception: {}", e.what());
    } catch (...) {
        spdlog::debug("[ServiceManagerFSM] dispatchNoThrow swallowed unknown exception");
    }
}

ServiceManagerFsm::ServiceManagerFsm() {
    std::lock_guard<std::mutex> lock(sharedMutex());
    detail::ServiceManagerMachine::snap = {};
    detail::ServiceManagerMachine::start();
}

ServiceManagerSnapshot ServiceManagerFsm::snapshot() const noexcept {
    try {
        std::lock_guard<std::mutex> lock(sharedMutex());
        return detail::ServiceManagerMachine::snap;
    } catch (...) {
        return detail::ServiceManagerMachine::snap;
    }
}

void ServiceManagerFsm::dispatch(const OpeningDatabaseEvent& ev) noexcept {
    validateDispatch(snapshot().state, ev,
                     "ServiceManagerFsm OpeningDatabaseEvent requires Uninitialized state");
    dispatchNoThrow(sharedMutex(), cv_, ev);
}

void ServiceManagerFsm::dispatch(const DatabaseOpenedEvent& ev) noexcept {
    validateDispatch(snapshot().state, ev,
                     "ServiceManagerFsm DatabaseOpenedEvent requires OpeningDatabase state");
    dispatchNoThrow(sharedMutex(), cv_, ev);
}

void ServiceManagerFsm::dispatch(const MigrationStartedEvent& ev) noexcept {
    validateDispatch(snapshot().state, ev,
                     "ServiceManagerFsm MigrationStartedEvent requires DatabaseReady state");
    dispatchNoThrow(sharedMutex(), cv_, ev);
}

void ServiceManagerFsm::dispatch(const MigrationCompletedEvent& ev) noexcept {
    validateDispatch(snapshot().state, ev,
                     "ServiceManagerFsm MigrationCompletedEvent requires MigratingSchema state");
    dispatchNoThrow(sharedMutex(), cv_, ev);
}

void ServiceManagerFsm::dispatch(const VectorsInitializedEvent& ev) noexcept {
    validateDispatch(snapshot().state, ev,
                     "ServiceManagerFsm VectorsInitializedEvent requires SchemaReady or later "
                     "vector-activation startup states");
    dispatchNoThrow(sharedMutex(), cv_, ev);
}

void ServiceManagerFsm::dispatch(const SearchEngineBuildStartedEvent& ev) noexcept {
    validateDispatch(snapshot().state, ev,
                     "ServiceManagerFsm SearchEngineBuildStartedEvent requires SchemaReady or "
                     "VectorsReady state");
    dispatchNoThrow(sharedMutex(), cv_, ev);
}

void ServiceManagerFsm::dispatch(const SearchEngineBuiltEvent& ev) noexcept {
    validateDispatch(
        snapshot().state, ev,
        "ServiceManagerFsm SearchEngineBuiltEvent requires BuildingSearchEngine state");
    dispatchNoThrow(sharedMutex(), cv_, ev);
}

void ServiceManagerFsm::dispatch(const InitializationFailedEvent& ev) noexcept {
    validateDispatch(snapshot().state, ev,
                     "ServiceManagerFsm InitializationFailedEvent must not arrive after Ready or "
                     "shutdown terminal states");
    dispatchNoThrow(sharedMutex(), cv_, ev);
}

void ServiceManagerFsm::dispatch(const ShutdownEvent& ev) noexcept {
    dispatchNoThrow(sharedMutex(), cv_, ev);
}

void ServiceManagerFsm::dispatch(const ServiceManagerStoppedEvent& ev) noexcept {
    validateDispatch(snapshot().state, ev,
                     "ServiceManagerFsm ServiceManagerStoppedEvent requires ShuttingDown state");
    dispatchNoThrow(sharedMutex(), cv_, ev);
}

bool ServiceManagerFsm::canInitializeVectors() const noexcept {
    try {
        std::lock_guard<std::mutex> lock(sharedMutex());
        return detail::ServiceManagerMachine::snap.state == ServiceManagerState::SchemaReady;
    } catch (...) {
        return false;
    }
}

bool ServiceManagerFsm::canBuildSearchEngine() const noexcept {
    try {
        std::lock_guard<std::mutex> lock(sharedMutex());
        auto s = detail::ServiceManagerMachine::snap.state;
        return s == ServiceManagerState::VectorsReady || s == ServiceManagerState::SchemaReady;
    } catch (...) {
        return false;
    }
}

bool ServiceManagerFsm::isReady() const noexcept {
    try {
        std::lock_guard<std::mutex> lock(sharedMutex());
        return detail::ServiceManagerMachine::snap.state == ServiceManagerState::Ready;
    } catch (...) {
        return false;
    }
}

bool ServiceManagerFsm::hasShutdown() const noexcept {
    try {
        std::lock_guard<std::mutex> lock(sharedMutex());
        auto s = detail::ServiceManagerMachine::snap.state;
        return s == ServiceManagerState::Stopped || s == ServiceManagerState::ShuttingDown;
    } catch (...) {
        return true;
    }
}

bool ServiceManagerFsm::isTerminalState() const noexcept {
    try {
        std::lock_guard<std::mutex> lock(sharedMutex());
        return isTerminal(detail::ServiceManagerMachine::snap.state);
    } catch (...) {
        return true;
    }
}

ServiceManagerSnapshot ServiceManagerFsm::waitForTerminalState(int timeoutSeconds) noexcept {
    try {
        std::unique_lock<std::mutex> lock(sharedMutex());
        auto pred = []() { return isTerminal(detail::ServiceManagerMachine::snap.state); };
        bool completed = cv_.wait_for(lock, std::chrono::seconds(timeoutSeconds), pred);
        if (!completed) {
            try {
                spdlog::warn("[ServiceManagerFSM] waitForTerminalState timed out after {}s, "
                             "current state={}",
                             timeoutSeconds,
                             static_cast<int>(detail::ServiceManagerMachine::snap.state));
            } catch (const std::exception& e) {
                spdlog::debug("[ServiceManagerFSM] timeout warning logging failed: {}", e.what());
            } catch (...) {
                spdlog::debug("[ServiceManagerFSM] timeout warning logging failed: unknown error");
            }
        }
        return detail::ServiceManagerMachine::snap;
    } catch (...) {
        return detail::ServiceManagerMachine::snap;
    }
}

void ServiceManagerFsm::cancelWait() noexcept {
    try {
        std::lock_guard<std::mutex> lock(sharedMutex());
        cv_.notify_all();
    } catch (const std::exception& e) {
        spdlog::debug("[ServiceManagerFSM] cancelWait swallowed exception: {}", e.what());
    } catch (...) {
        spdlog::debug("[ServiceManagerFSM] cancelWait swallowed unknown exception");
    }
}

void ServiceManagerFsm::reset() noexcept {
    try {
        std::lock_guard<std::mutex> lock(sharedMutex());
        detail::ServiceManagerMachine::snap = {};
        detail::ServiceManagerMachine::start();
    } catch (const std::exception& e) {
        spdlog::debug("[ServiceManagerFSM] reset swallowed exception: {}", e.what());
    } catch (...) {
        spdlog::debug("[ServiceManagerFSM] reset swallowed unknown exception");
    }
}

} // namespace yams::daemon
