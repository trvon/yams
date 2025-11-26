#pragma once

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <string>

#include <spdlog/spdlog.h>

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
    ServiceManagerSnapshot snapshot() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return snap_;
    }

    // Event dispatchers
    void dispatch(const OpeningDatabaseEvent&) {
        std::lock_guard<std::mutex> lock(mutex_);
        transitionTo(ServiceManagerState::OpeningDatabase);
    }
    void dispatch(const DatabaseOpenedEvent&) {
        std::lock_guard<std::mutex> lock(mutex_);
        transitionTo(ServiceManagerState::DatabaseReady);
    }
    void dispatch(const MigrationStartedEvent&) {
        std::lock_guard<std::mutex> lock(mutex_);
        transitionTo(ServiceManagerState::MigratingSchema);
    }
    void dispatch(const MigrationCompletedEvent&) {
        std::lock_guard<std::mutex> lock(mutex_);
        transitionTo(ServiceManagerState::SchemaReady);
    }
    void dispatch(const VectorsInitializedEvent&) {
        std::lock_guard<std::mutex> lock(mutex_);
        transitionTo(ServiceManagerState::VectorsReady);
    }
    void dispatch(const SearchEngineBuildStartedEvent&) {
        std::lock_guard<std::mutex> lock(mutex_);
        transitionTo(ServiceManagerState::BuildingSearchEngine);
    }
    void dispatch(const SearchEngineBuiltEvent&) {
        std::lock_guard<std::mutex> lock(mutex_);
        transitionTo(ServiceManagerState::Ready);
    }
    void dispatch(const InitializationFailedEvent& ev) {
        std::lock_guard<std::mutex> lock(mutex_);
        snap_.lastError = ev.error;
        transitionTo(ServiceManagerState::Failed);
    }
    void dispatch(const ShutdownEvent&) {
        std::lock_guard<std::mutex> lock(mutex_);
        transitionTo(ServiceManagerState::ShuttingDown);
    }
    void dispatch(const ServiceManagerStoppedEvent&) {
        std::lock_guard<std::mutex> lock(mutex_);
        transitionTo(ServiceManagerState::Stopped);
    }

    // Queries
    bool canInitializeVectors() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return snap_.state == ServiceManagerState::SchemaReady;
    }
    bool canBuildSearchEngine() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return snap_.state == ServiceManagerState::VectorsReady ||
               snap_.state == ServiceManagerState::SchemaReady;
    }
    bool isReady() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return snap_.state == ServiceManagerState::Ready;
    }
    bool hasShutdown() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return snap_.state == ServiceManagerState::Stopped ||
               snap_.state == ServiceManagerState::ShuttingDown;
    }

    bool isTerminalState() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return snap_.state == ServiceManagerState::Ready ||
               snap_.state == ServiceManagerState::Failed ||
               snap_.state == ServiceManagerState::Stopped;
    }

    ServiceManagerSnapshot waitForTerminalState(int timeoutSeconds = 60) {
        std::unique_lock<std::mutex> lock(mutex_);
        auto pred = [this]() {
            return snap_.state == ServiceManagerState::Ready ||
                   snap_.state == ServiceManagerState::Failed ||
                   snap_.state == ServiceManagerState::Stopped;
        };
        bool completed = cv_.wait_for(lock, std::chrono::seconds(timeoutSeconds), pred);
        if (!completed) {
            spdlog::warn("[ServiceManagerFSM] waitForTerminalState timed out after {}s, current state={}",
                         timeoutSeconds, static_cast<int>(snap_.state));
        }
        return snap_;
    }

    void cancelWait() {
        std::lock_guard<std::mutex> lock(mutex_);
        cv_.notify_all();
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
        if (next == ServiceManagerState::Ready ||
            next == ServiceManagerState::Failed ||
            next == ServiceManagerState::Stopped) {
            cv_.notify_all();
        }
    }

    ServiceManagerSnapshot snap_{};
    mutable std::mutex mutex_;
    std::condition_variable cv_;
};

} // namespace yams::daemon
