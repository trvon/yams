#pragma once

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <string>

namespace yams::daemon {

enum class ServiceManagerState {
    Uninitialized,
    OpeningDatabase,
    DatabaseReady,
    MigratingSchema,
    SchemaReady,
    VectorsReady,
    BuildingSearchEngine,
    Ready,
    ShuttingDown,
    Stopped,
    Failed
};

struct ServiceManagerSnapshot {
    ServiceManagerState state{ServiceManagerState::Uninitialized};
    std::string lastError;
    std::chrono::steady_clock::time_point lastTransition{};
};

struct OpeningDatabaseEvent {};
struct DatabaseOpenedEvent {};
struct MigrationStartedEvent {};
struct MigrationCompletedEvent {};
struct VectorsInitializedEvent {
    std::size_t dimension{0};
};
struct SearchEngineBuildStartedEvent {};
struct SearchEngineBuiltEvent {};
struct InitializationFailedEvent {
    std::string error;
};
struct ShutdownEvent {};
struct ServiceManagerStoppedEvent {};

// Startup/shutdown lifecycle of one ServiceManager. Each instance owns its state; managers in the
// same process (a restart, a second daemon that loses the instance lock, tests) do not share it.
class ServiceManagerFsm {
public:
    ServiceManagerFsm();
    ~ServiceManagerFsm() = default;

    ServiceManagerSnapshot snapshot() const noexcept;

    void dispatch(const OpeningDatabaseEvent&) noexcept;
    // Race-safe startup helper for async initialization. Returns false if shutdown/stop
    // already won before DB-open begins; DCHECKs on other invalid lifecycle states.
    bool tryStartOpeningDatabase() noexcept;
    void dispatch(const DatabaseOpenedEvent&) noexcept;
    void dispatch(const MigrationStartedEvent&) noexcept;
    void dispatch(const MigrationCompletedEvent&) noexcept;
    void dispatch(const VectorsInitializedEvent&) noexcept;
    void dispatch(const SearchEngineBuildStartedEvent&) noexcept;
    void dispatch(const SearchEngineBuiltEvent&) noexcept;
    void dispatch(const InitializationFailedEvent&) noexcept;
    void dispatch(const ShutdownEvent&) noexcept;
    void dispatch(const ServiceManagerStoppedEvent&) noexcept;
    // Shutdown finalizer for daemon/service-manager teardown: drives this instance to Stopped from
    // any state and returns whether it got there.
    [[nodiscard]] bool completeShutdown() noexcept;

    bool canInitializeVectors() const noexcept;
    bool canBuildSearchEngine() const noexcept;
    bool isReady() const noexcept;
    bool hasShutdown() const noexcept;
    bool isTerminalState() const noexcept;

    ServiceManagerSnapshot waitForTerminalState(int timeoutSeconds = 60) noexcept;
    void cancelWait() noexcept;
    void reset() noexcept;

private:
    void enterLocked(ServiceManagerState next);
    void applyLocked(const OpeningDatabaseEvent&);
    void applyLocked(const DatabaseOpenedEvent&);
    void applyLocked(const MigrationStartedEvent&);
    void applyLocked(const MigrationCompletedEvent&);
    void applyLocked(const VectorsInitializedEvent&);
    void applyLocked(const SearchEngineBuildStartedEvent&);
    void applyLocked(const SearchEngineBuiltEvent&);
    void applyLocked(const InitializationFailedEvent&);
    void applyLocked(const ShutdownEvent&);
    void applyLocked(const ServiceManagerStoppedEvent&);
    template <typename Event> void dispatchNoThrow(const Event& ev) noexcept;

    mutable std::mutex mutex_;
    std::condition_variable cv_;
    ServiceManagerSnapshot snap_{};
};

} // namespace yams::daemon
