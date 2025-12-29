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
    ServiceManagerSnapshot snapshot() const noexcept {
        try {
            std::lock_guard<std::mutex> lock(mutex_);
            return snap_;
        } catch (...) {
            return snap_;
        }
    }

    // Event dispatchers - all noexcept to prevent crashes during destruction
    void dispatch(const OpeningDatabaseEvent&) noexcept {
        try {
            std::lock_guard<std::mutex> lock(mutex_);
            transitionTo(ServiceManagerState::OpeningDatabase);
        } catch (...) {
        }
    }
    void dispatch(const DatabaseOpenedEvent&) noexcept {
        try {
            std::lock_guard<std::mutex> lock(mutex_);
            transitionTo(ServiceManagerState::DatabaseReady);
        } catch (...) {
        }
    }
    void dispatch(const MigrationStartedEvent&) noexcept {
        try {
            std::lock_guard<std::mutex> lock(mutex_);
            transitionTo(ServiceManagerState::MigratingSchema);
        } catch (...) {
        }
    }
    void dispatch(const MigrationCompletedEvent&) noexcept {
        try {
            std::lock_guard<std::mutex> lock(mutex_);
            transitionTo(ServiceManagerState::SchemaReady);
        } catch (...) {
        }
    }
    void dispatch(const VectorsInitializedEvent&) noexcept {
        try {
            std::lock_guard<std::mutex> lock(mutex_);
            transitionTo(ServiceManagerState::VectorsReady);
        } catch (...) {
        }
    }
    void dispatch(const SearchEngineBuildStartedEvent&) noexcept {
        try {
            std::lock_guard<std::mutex> lock(mutex_);
            transitionTo(ServiceManagerState::BuildingSearchEngine);
        } catch (...) {
        }
    }
    void dispatch(const SearchEngineBuiltEvent&) noexcept {
        try {
            std::lock_guard<std::mutex> lock(mutex_);
            transitionTo(ServiceManagerState::Ready);
        } catch (...) {
        }
    }
    void dispatch(const InitializationFailedEvent& ev) noexcept {
        try {
            std::lock_guard<std::mutex> lock(mutex_);
            snap_.lastError = ev.error;
            transitionTo(ServiceManagerState::Failed);
        } catch (...) {
        }
    }
    void dispatch(const ShutdownEvent&) noexcept {
        try {
            std::lock_guard<std::mutex> lock(mutex_);
            transitionTo(ServiceManagerState::ShuttingDown);
        } catch (...) {
        }
    }
    void dispatch(const ServiceManagerStoppedEvent&) noexcept {
        try {
            std::lock_guard<std::mutex> lock(mutex_);
            transitionTo(ServiceManagerState::Stopped);
        } catch (...) {
        }
    }

    // Queries - noexcept to prevent crashes during destruction
    bool canInitializeVectors() const noexcept {
        try {
            std::lock_guard<std::mutex> lock(mutex_);
            return snap_.state == ServiceManagerState::SchemaReady;
        } catch (...) {
            return false;
        }
    }
    bool canBuildSearchEngine() const noexcept {
        try {
            std::lock_guard<std::mutex> lock(mutex_);
            return snap_.state == ServiceManagerState::VectorsReady ||
                   snap_.state == ServiceManagerState::SchemaReady;
        } catch (...) {
            return false;
        }
    }
    bool isReady() const noexcept {
        try {
            std::lock_guard<std::mutex> lock(mutex_);
            return snap_.state == ServiceManagerState::Ready;
        } catch (...) {
            return false;
        }
    }
    bool hasShutdown() const noexcept {
        try {
            std::lock_guard<std::mutex> lock(mutex_);
            return snap_.state == ServiceManagerState::Stopped ||
                   snap_.state == ServiceManagerState::ShuttingDown;
        } catch (...) {
            return true;
        }
    }

    bool isTerminalState() const noexcept {
        try {
            std::lock_guard<std::mutex> lock(mutex_);
            return snap_.state == ServiceManagerState::Ready ||
                   snap_.state == ServiceManagerState::Failed ||
                   snap_.state == ServiceManagerState::Stopped;
        } catch (...) {
            return true;
        }
    }

    ServiceManagerSnapshot waitForTerminalState(int timeoutSeconds = 60) noexcept {
        try {
            std::unique_lock<std::mutex> lock(mutex_);
            auto pred = [this]() {
                return snap_.state == ServiceManagerState::Ready ||
                       snap_.state == ServiceManagerState::Failed ||
                       snap_.state == ServiceManagerState::Stopped;
            };
            bool completed = cv_.wait_for(lock, std::chrono::seconds(timeoutSeconds), pred);
            if (!completed) {
                try {
                    spdlog::warn("[ServiceManagerFSM] waitForTerminalState timed out after {}s, "
                                 "current state={}",
                                 timeoutSeconds, static_cast<int>(snap_.state));
                } catch (...) {
                }
            }
            return snap_;
        } catch (...) {
            return snap_;
        }
    }

    void cancelWait() noexcept {
        try {
            std::lock_guard<std::mutex> lock(mutex_);
            cv_.notify_all();
        } catch (...) {
        }
    }

    void reset() noexcept {
        try {
            std::lock_guard<std::mutex> lock(mutex_);
            snap_.state = ServiceManagerState::Uninitialized;
            snap_.lastError.clear();
            snap_.lastTransition = std::chrono::steady_clock::now();
        } catch (...) {
        }
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
        if (next == ServiceManagerState::Ready || next == ServiceManagerState::Failed ||
            next == ServiceManagerState::Stopped) {
            cv_.notify_all();
        }
    }

    ServiceManagerSnapshot snap_{};
    mutable std::mutex mutex_;
    std::condition_variable cv_;
};

} // namespace yams::daemon
