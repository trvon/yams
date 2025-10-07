#pragma once

#include <spdlog/spdlog.h>
#include <chrono>
#include <optional>
#include <string>

namespace yams::daemon {

enum class ServiceManagerState {
    Uninitialized,
    OpeningDatabase,
    DatabaseReady,
    MigratingSchema,
    SchemaReady,
    InitializingVectors,
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
    ServiceManagerSnapshot snapshot() const { return snap_; }

    // Event dispatchers
    void dispatch(const OpeningDatabaseEvent&) {
        transitionTo(ServiceManagerState::OpeningDatabase);
    }
    void dispatch(const DatabaseOpenedEvent&) { transitionTo(ServiceManagerState::DatabaseReady); }
    void dispatch(const MigrationStartedEvent&) {
        transitionTo(ServiceManagerState::MigratingSchema);
    }
    void dispatch(const MigrationCompletedEvent&) {
        transitionTo(ServiceManagerState::SchemaReady);
    }
    void dispatch(const VectorsInitializedEvent&) {
        transitionTo(ServiceManagerState::VectorsReady);
    }
    void dispatch(const SearchEngineBuildStartedEvent&) {
        transitionTo(ServiceManagerState::BuildingSearchEngine);
    }
    void dispatch(const SearchEngineBuiltEvent&) { transitionTo(ServiceManagerState::Ready); }
    void dispatch(const InitializationFailedEvent& ev) {
        snap_.lastError = ev.error;
        transitionTo(ServiceManagerState::Failed);
    }
    void dispatch(const ShutdownEvent&) { transitionTo(ServiceManagerState::ShuttingDown); }
    void dispatch(const ServiceManagerStoppedEvent&) { transitionTo(ServiceManagerState::Stopped); }

    // Queries
    bool canInitializeVectors() const { return snap_.state == ServiceManagerState::SchemaReady; }
    bool canBuildSearchEngine() const {
        return snap_.state == ServiceManagerState::VectorsReady ||
               snap_.state == ServiceManagerState::SchemaReady;
    }
    bool isReady() const { return snap_.state == ServiceManagerState::Ready; }
    bool hasShutdown() const {
        return snap_.state == ServiceManagerState::Stopped ||
               snap_.state == ServiceManagerState::ShuttingDown;
    }

private:
    void transitionTo(ServiceManagerState next) {
        auto prev = snap_.state;
        snap_.state = next;
        snap_.lastTransition = std::chrono::steady_clock::now();
        try {
            spdlog::info("[ServiceManagerFSM] {} -> {}", static_cast<int>(prev),
                         static_cast<int>(next));
        } catch (...) {
        }
    }

    ServiceManagerSnapshot snap_{};
};

} // namespace yams::daemon
