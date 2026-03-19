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
        } catch (const std::exception&) {
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
        } catch (const std::exception&) {
            ignoreNoexceptFsmFailure();
        } catch (...) {
            ignoreNoexceptFsmFailure();
        }
    }
    void dispatch(const DatabaseOpenedEvent&) noexcept {
        try {
            std::lock_guard<std::mutex> lock(mutex_);
            transitionTo(ServiceManagerState::DatabaseReady);
        } catch (const std::exception&) {
            ignoreNoexceptFsmFailure();
        } catch (...) {
            ignoreNoexceptFsmFailure();
        }
    }
    void dispatch(const MigrationStartedEvent&) noexcept {
        try {
            std::lock_guard<std::mutex> lock(mutex_);
            transitionTo(ServiceManagerState::MigratingSchema);
        } catch (const std::exception&) {
            ignoreNoexceptFsmFailure();
        } catch (...) {
            ignoreNoexceptFsmFailure();
        }
    }
    void dispatch(const MigrationCompletedEvent&) noexcept {
        try {
            std::lock_guard<std::mutex> lock(mutex_);
            transitionTo(ServiceManagerState::SchemaReady);
        } catch (const std::exception&) {
            ignoreNoexceptFsmFailure();
        } catch (...) {
            ignoreNoexceptFsmFailure();
        }
    }
    void dispatch(const VectorsInitializedEvent&) noexcept {
        try {
            std::lock_guard<std::mutex> lock(mutex_);
            transitionTo(ServiceManagerState::VectorsReady);
        } catch (const std::exception&) {
            ignoreNoexceptFsmFailure();
        } catch (...) {
            ignoreNoexceptFsmFailure();
        }
    }
    void dispatch(const SearchEngineBuildStartedEvent&) noexcept {
        try {
            std::lock_guard<std::mutex> lock(mutex_);
            transitionTo(ServiceManagerState::BuildingSearchEngine);
        } catch (const std::exception&) {
            ignoreNoexceptFsmFailure();
        } catch (...) {
            ignoreNoexceptFsmFailure();
        }
    }
    void dispatch(const SearchEngineBuiltEvent&) noexcept {
        try {
            std::lock_guard<std::mutex> lock(mutex_);
            transitionTo(ServiceManagerState::Ready);
        } catch (const std::exception&) {
            ignoreNoexceptFsmFailure();
        } catch (...) {
            ignoreNoexceptFsmFailure();
        }
    }
    void dispatch(const InitializationFailedEvent& ev) noexcept {
        try {
            std::lock_guard<std::mutex> lock(mutex_);
            snap_.lastError = ev.error;
            transitionTo(ServiceManagerState::Failed);
        } catch (const std::exception&) {
            ignoreNoexceptFsmFailure();
        } catch (...) {
            ignoreNoexceptFsmFailure();
        }
    }
    void dispatch(const ShutdownEvent&) noexcept {
        try {
            std::lock_guard<std::mutex> lock(mutex_);
            transitionTo(ServiceManagerState::ShuttingDown);
        } catch (const std::exception&) {
            ignoreNoexceptFsmFailure();
        } catch (...) {
            ignoreNoexceptFsmFailure();
        }
    }
    void dispatch(const ServiceManagerStoppedEvent&) noexcept {
        try {
            std::lock_guard<std::mutex> lock(mutex_);
            transitionTo(ServiceManagerState::Stopped);
        } catch (const std::exception&) {
            ignoreNoexceptFsmFailure();
        } catch (...) {
            ignoreNoexceptFsmFailure();
        }
    }

    // Queries - noexcept to prevent crashes during destruction
    bool canInitializeVectors() const noexcept {
        try {
            std::lock_guard<std::mutex> lock(mutex_);
            return snap_.state == ServiceManagerState::SchemaReady;
        } catch (const std::exception&) {
            return false;
        } catch (...) {
            return false;
        }
    }
    bool canBuildSearchEngine() const noexcept {
        try {
            std::lock_guard<std::mutex> lock(mutex_);
            return snap_.state == ServiceManagerState::VectorsReady ||
                   snap_.state == ServiceManagerState::SchemaReady;
        } catch (const std::exception&) {
            return false;
        } catch (...) {
            return false;
        }
    }
    bool isReady() const noexcept {
        try {
            std::lock_guard<std::mutex> lock(mutex_);
            return snap_.state == ServiceManagerState::Ready;
        } catch (const std::exception&) {
            return false;
        } catch (...) {
            return false;
        }
    }
    bool hasShutdown() const noexcept {
        try {
            std::lock_guard<std::mutex> lock(mutex_);
            return snap_.state == ServiceManagerState::Stopped ||
                   snap_.state == ServiceManagerState::ShuttingDown;
        } catch (const std::exception&) {
            return true;
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
        } catch (const std::exception&) {
            return true;
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
                } catch (const std::exception&) {
                    ignoreNoexceptFsmFailure();
                } catch (...) {
                    ignoreNoexceptFsmFailure();
                }
            }
            return snap_;
        } catch (const std::exception&) {
            return snap_;
        } catch (...) {
            return snap_;
        }
    }

    void cancelWait() noexcept {
        try {
            std::lock_guard<std::mutex> lock(mutex_);
            cv_.notify_all();
        } catch (const std::exception&) {
            ignoreNoexceptFsmFailure();
        } catch (...) {
            ignoreNoexceptFsmFailure();
        }
    }

    void reset() noexcept {
        try {
            std::lock_guard<std::mutex> lock(mutex_);
            snap_.state = ServiceManagerState::Uninitialized;
            snap_.lastError.clear();
            snap_.lastTransition = std::chrono::steady_clock::now();
        } catch (const std::exception&) {
            ignoreNoexceptFsmFailure();
        } catch (...) {
            ignoreNoexceptFsmFailure();
        }
    }

private:
    static void ignoreNoexceptFsmFailure() noexcept {}

    void transitionTo(ServiceManagerState next) {
        auto prev = snap_.state;
        snap_.state = next;
        snap_.lastTransition = std::chrono::steady_clock::now();
        try {
            spdlog::info("[ServiceManagerFSM] {} -> {}", static_cast<int>(prev),
                         static_cast<int>(next));
        } catch (const std::exception&) {
            ignoreNoexceptFsmFailure();
        } catch (...) {
            ignoreNoexceptFsmFailure();
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
