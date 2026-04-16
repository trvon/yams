#include <yams/daemon/components/ServiceManagerFsm.h>

#include <spdlog/spdlog.h>
#include <tinyfsm.hpp>

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

static bool isTerminal(ServiceManagerState s) {
    return s == ServiceManagerState::Ready || s == ServiceManagerState::Failed ||
           s == ServiceManagerState::Stopped;
}

ServiceManagerFsm::ServiceManagerFsm() {
    std::lock_guard<std::mutex> lock(mutex_);
    detail::ServiceManagerMachine::snap = {};
    detail::ServiceManagerMachine::start();
}

ServiceManagerSnapshot ServiceManagerFsm::snapshot() const noexcept {
    try {
        std::lock_guard<std::mutex> lock(mutex_);
        return detail::ServiceManagerMachine::snap;
    } catch (...) {
        return detail::ServiceManagerMachine::snap;
    }
}

void ServiceManagerFsm::dispatch(const OpeningDatabaseEvent& ev) noexcept {
    try {
        std::lock_guard<std::mutex> lock(mutex_);
        detail::ServiceManagerMachine::dispatch(ev);
        if (isTerminal(detail::ServiceManagerMachine::snap.state))
            cv_.notify_all();
    } catch (...) {
    }
}

void ServiceManagerFsm::dispatch(const DatabaseOpenedEvent& ev) noexcept {
    try {
        std::lock_guard<std::mutex> lock(mutex_);
        detail::ServiceManagerMachine::dispatch(ev);
        if (isTerminal(detail::ServiceManagerMachine::snap.state))
            cv_.notify_all();
    } catch (...) {
    }
}

void ServiceManagerFsm::dispatch(const MigrationStartedEvent& ev) noexcept {
    try {
        std::lock_guard<std::mutex> lock(mutex_);
        detail::ServiceManagerMachine::dispatch(ev);
        if (isTerminal(detail::ServiceManagerMachine::snap.state))
            cv_.notify_all();
    } catch (...) {
    }
}

void ServiceManagerFsm::dispatch(const MigrationCompletedEvent& ev) noexcept {
    try {
        std::lock_guard<std::mutex> lock(mutex_);
        detail::ServiceManagerMachine::dispatch(ev);
        if (isTerminal(detail::ServiceManagerMachine::snap.state))
            cv_.notify_all();
    } catch (...) {
    }
}

void ServiceManagerFsm::dispatch(const VectorsInitializedEvent& ev) noexcept {
    try {
        std::lock_guard<std::mutex> lock(mutex_);
        detail::ServiceManagerMachine::dispatch(ev);
        if (isTerminal(detail::ServiceManagerMachine::snap.state))
            cv_.notify_all();
    } catch (...) {
    }
}

void ServiceManagerFsm::dispatch(const SearchEngineBuildStartedEvent& ev) noexcept {
    try {
        std::lock_guard<std::mutex> lock(mutex_);
        detail::ServiceManagerMachine::dispatch(ev);
        if (isTerminal(detail::ServiceManagerMachine::snap.state))
            cv_.notify_all();
    } catch (...) {
    }
}

void ServiceManagerFsm::dispatch(const SearchEngineBuiltEvent& ev) noexcept {
    try {
        std::lock_guard<std::mutex> lock(mutex_);
        detail::ServiceManagerMachine::dispatch(ev);
        if (isTerminal(detail::ServiceManagerMachine::snap.state))
            cv_.notify_all();
    } catch (...) {
    }
}

void ServiceManagerFsm::dispatch(const InitializationFailedEvent& ev) noexcept {
    try {
        std::lock_guard<std::mutex> lock(mutex_);
        detail::ServiceManagerMachine::dispatch(ev);
        if (isTerminal(detail::ServiceManagerMachine::snap.state))
            cv_.notify_all();
    } catch (...) {
    }
}

void ServiceManagerFsm::dispatch(const ShutdownEvent& ev) noexcept {
    try {
        std::lock_guard<std::mutex> lock(mutex_);
        detail::ServiceManagerMachine::dispatch(ev);
        if (isTerminal(detail::ServiceManagerMachine::snap.state))
            cv_.notify_all();
    } catch (...) {
    }
}

void ServiceManagerFsm::dispatch(const ServiceManagerStoppedEvent& ev) noexcept {
    try {
        std::lock_guard<std::mutex> lock(mutex_);
        detail::ServiceManagerMachine::dispatch(ev);
        if (isTerminal(detail::ServiceManagerMachine::snap.state))
            cv_.notify_all();
    } catch (...) {
    }
}

bool ServiceManagerFsm::canInitializeVectors() const noexcept {
    try {
        std::lock_guard<std::mutex> lock(mutex_);
        return detail::ServiceManagerMachine::snap.state == ServiceManagerState::SchemaReady;
    } catch (...) {
        return false;
    }
}

bool ServiceManagerFsm::canBuildSearchEngine() const noexcept {
    try {
        std::lock_guard<std::mutex> lock(mutex_);
        auto s = detail::ServiceManagerMachine::snap.state;
        return s == ServiceManagerState::VectorsReady || s == ServiceManagerState::SchemaReady;
    } catch (...) {
        return false;
    }
}

bool ServiceManagerFsm::isReady() const noexcept {
    try {
        std::lock_guard<std::mutex> lock(mutex_);
        return detail::ServiceManagerMachine::snap.state == ServiceManagerState::Ready;
    } catch (...) {
        return false;
    }
}

bool ServiceManagerFsm::hasShutdown() const noexcept {
    try {
        std::lock_guard<std::mutex> lock(mutex_);
        auto s = detail::ServiceManagerMachine::snap.state;
        return s == ServiceManagerState::Stopped || s == ServiceManagerState::ShuttingDown;
    } catch (...) {
        return true;
    }
}

bool ServiceManagerFsm::isTerminalState() const noexcept {
    try {
        std::lock_guard<std::mutex> lock(mutex_);
        return isTerminal(detail::ServiceManagerMachine::snap.state);
    } catch (...) {
        return true;
    }
}

ServiceManagerSnapshot ServiceManagerFsm::waitForTerminalState(int timeoutSeconds) noexcept {
    try {
        std::unique_lock<std::mutex> lock(mutex_);
        auto pred = [this]() { return isTerminal(detail::ServiceManagerMachine::snap.state); };
        bool completed = cv_.wait_for(lock, std::chrono::seconds(timeoutSeconds), pred);
        if (!completed) {
            try {
                spdlog::warn("[ServiceManagerFSM] waitForTerminalState timed out after {}s, "
                             "current state={}",
                             timeoutSeconds,
                             static_cast<int>(detail::ServiceManagerMachine::snap.state));
            } catch (...) {
            }
        }
        return detail::ServiceManagerMachine::snap;
    } catch (...) {
        return detail::ServiceManagerMachine::snap;
    }
}

void ServiceManagerFsm::cancelWait() noexcept {
    try {
        std::lock_guard<std::mutex> lock(mutex_);
        cv_.notify_all();
    } catch (...) {
    }
}

void ServiceManagerFsm::reset() noexcept {
    try {
        std::lock_guard<std::mutex> lock(mutex_);
        detail::ServiceManagerMachine::snap = {};
        detail::ServiceManagerMachine::start();
    } catch (...) {
    }
}

} // namespace yams::daemon
