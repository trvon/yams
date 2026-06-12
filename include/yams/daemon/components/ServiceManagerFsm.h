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
    // Race-safe shutdown finalizer for daemon/service-manager teardown. The tinyfsm backend stores
    // process-wide state, so concurrent ServiceManagerFsm construction in tests or same-process
    // retries can reset the shared state between shutdown start and shutdown completion.
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
    static std::mutex& sharedMutex() {
        static std::mutex mutex;
        return mutex;
    }

    std::condition_variable cv_;
};

} // namespace yams::daemon
