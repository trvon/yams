#include <yams/daemon/components/ServiceManagerFsm.h>

#include <spdlog/spdlog.h>
#include <yams/core/assert.hpp>

namespace yams::daemon {

// Transitions. Each state's accepted events match the former tinyfsm machine: events a state does
// not handle are ignored, InitializationFailedEvent moves any non-terminal state to Failed, and
// ShutdownEvent moves any state except ShuttingDown/Stopped to ShuttingDown.
void ServiceManagerFsm::enterLocked(ServiceManagerState next) {
    const auto prev = snap_.state;
    snap_.state = next;
    snap_.lastTransition = std::chrono::steady_clock::now();
    spdlog::info("[ServiceManagerFSM] {} -> {}", static_cast<int>(prev), static_cast<int>(next));
}

void ServiceManagerFsm::applyLocked(const OpeningDatabaseEvent&) {
    if (snap_.state == ServiceManagerState::Uninitialized) {
        enterLocked(ServiceManagerState::OpeningDatabase);
    }
}

void ServiceManagerFsm::applyLocked(const DatabaseOpenedEvent&) {
    if (snap_.state == ServiceManagerState::OpeningDatabase) {
        enterLocked(ServiceManagerState::DatabaseReady);
    }
}

void ServiceManagerFsm::applyLocked(const MigrationStartedEvent&) {
    if (snap_.state == ServiceManagerState::DatabaseReady) {
        enterLocked(ServiceManagerState::MigratingSchema);
    }
}

void ServiceManagerFsm::applyLocked(const MigrationCompletedEvent&) {
    if (snap_.state == ServiceManagerState::MigratingSchema) {
        enterLocked(ServiceManagerState::SchemaReady);
    }
}

void ServiceManagerFsm::applyLocked(const VectorsInitializedEvent&) {
    if (snap_.state == ServiceManagerState::SchemaReady) {
        enterLocked(ServiceManagerState::VectorsReady);
    }
}

void ServiceManagerFsm::applyLocked(const SearchEngineBuildStartedEvent&) {
    if (snap_.state == ServiceManagerState::SchemaReady ||
        snap_.state == ServiceManagerState::VectorsReady) {
        enterLocked(ServiceManagerState::BuildingSearchEngine);
    }
}

void ServiceManagerFsm::applyLocked(const SearchEngineBuiltEvent&) {
    if (snap_.state == ServiceManagerState::BuildingSearchEngine) {
        enterLocked(ServiceManagerState::Ready);
    }
}

void ServiceManagerFsm::applyLocked(const InitializationFailedEvent& ev) {
    switch (snap_.state) {
        case ServiceManagerState::Ready:
        case ServiceManagerState::ShuttingDown:
        case ServiceManagerState::Stopped:
        case ServiceManagerState::Failed:
            return;
        default:
            snap_.lastError = ev.error;
            enterLocked(ServiceManagerState::Failed);
    }
}

void ServiceManagerFsm::applyLocked(const ShutdownEvent&) {
    if (snap_.state != ServiceManagerState::ShuttingDown &&
        snap_.state != ServiceManagerState::Stopped) {
        enterLocked(ServiceManagerState::ShuttingDown);
    }
}

void ServiceManagerFsm::applyLocked(const ServiceManagerStoppedEvent&) {
    if (snap_.state == ServiceManagerState::ShuttingDown) {
        enterLocked(ServiceManagerState::Stopped);
    }
}

} // namespace yams::daemon

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
    // Accept from any state except the two terminal states that have no handler.
    // Ready and ShuttingDown silently ignore init failures; init-path states
    // (Uninitialized, OpeningDatabase, etc.) transit to Failed.
    return state != ServiceManagerState::Stopped && state != ServiceManagerState::Failed;
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

template <typename Event> void ServiceManagerFsm::dispatchNoThrow(const Event& ev) noexcept {
    try {
        std::lock_guard<std::mutex> lock(mutex_);
        applyLocked(ev);
        if (isTerminal(snap_.state))
            cv_.notify_all();
    } catch (const std::exception& e) {
        spdlog::debug("[ServiceManagerFSM] dispatchNoThrow swallowed exception: {}", e.what());
    } catch (...) {
        spdlog::debug("[ServiceManagerFSM] dispatchNoThrow swallowed unknown exception");
    }
}

ServiceManagerFsm::ServiceManagerFsm() {
    std::lock_guard<std::mutex> lock(mutex_);
    enterLocked(ServiceManagerState::Uninitialized);
}

ServiceManagerSnapshot ServiceManagerFsm::snapshot() const noexcept {
    try {
        std::lock_guard<std::mutex> lock(mutex_);
        return snap_;
    } catch (...) {
        return snap_;
    }
}

void ServiceManagerFsm::dispatch(const OpeningDatabaseEvent& ev) noexcept {
    validateDispatch(snapshot().state, ev,
                     "ServiceManagerFsm OpeningDatabaseEvent requires Uninitialized state");
    dispatchNoThrow(ev);
}

bool ServiceManagerFsm::tryStartOpeningDatabase() noexcept {
    try {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto state = snap_.state;
        if (state == ServiceManagerState::Uninitialized) {
            applyLocked(OpeningDatabaseEvent{});
            return true;
        }
        if (state == ServiceManagerState::ShuttingDown || state == ServiceManagerState::Stopped) {
            return false;
        }
        YAMS_DCHECK(false,
                    "ServiceManagerFsm OpeningDatabaseEvent requires Uninitialized state unless "
                    "shutdown already started");
    } catch (const std::exception& e) {
        spdlog::debug("[ServiceManagerFSM] tryStartOpeningDatabase swallowed exception: {}",
                      e.what());
    } catch (...) {
        spdlog::debug("[ServiceManagerFSM] tryStartOpeningDatabase swallowed unknown exception");
    }
    return false;
}

void ServiceManagerFsm::dispatch(const DatabaseOpenedEvent& ev) noexcept {
    validateDispatch(snapshot().state, ev,
                     "ServiceManagerFsm DatabaseOpenedEvent requires OpeningDatabase state");
    dispatchNoThrow(ev);
}

void ServiceManagerFsm::dispatch(const MigrationStartedEvent& ev) noexcept {
    validateDispatch(snapshot().state, ev,
                     "ServiceManagerFsm MigrationStartedEvent requires DatabaseReady state");
    dispatchNoThrow(ev);
}

void ServiceManagerFsm::dispatch(const MigrationCompletedEvent& ev) noexcept {
    validateDispatch(snapshot().state, ev,
                     "ServiceManagerFsm MigrationCompletedEvent requires MigratingSchema state");
    dispatchNoThrow(ev);
}

void ServiceManagerFsm::dispatch(const VectorsInitializedEvent& ev) noexcept {
    validateDispatch(snapshot().state, ev,
                     "ServiceManagerFsm VectorsInitializedEvent requires SchemaReady or later "
                     "vector-activation startup states");
    dispatchNoThrow(ev);
}

void ServiceManagerFsm::dispatch(const SearchEngineBuildStartedEvent& ev) noexcept {
    validateDispatch(snapshot().state, ev,
                     "ServiceManagerFsm SearchEngineBuildStartedEvent requires SchemaReady or "
                     "VectorsReady state");
    dispatchNoThrow(ev);
}

void ServiceManagerFsm::dispatch(const SearchEngineBuiltEvent& ev) noexcept {
    validateDispatch(
        snapshot().state, ev,
        "ServiceManagerFsm SearchEngineBuiltEvent requires BuildingSearchEngine state");
    dispatchNoThrow(ev);
}

void ServiceManagerFsm::dispatch(const InitializationFailedEvent& ev) noexcept {
    validateDispatch(snapshot().state, ev,
                     "ServiceManagerFsm InitializationFailedEvent invalid in terminal states");
    dispatchNoThrow(ev);
}

void ServiceManagerFsm::dispatch(const ShutdownEvent& ev) noexcept {
    dispatchNoThrow(ev);
}

void ServiceManagerFsm::dispatch(const ServiceManagerStoppedEvent& ev) noexcept {
    validateDispatch(snapshot().state, ev,
                     "ServiceManagerFsm ServiceManagerStoppedEvent requires ShuttingDown state");
    dispatchNoThrow(ev);
}

bool ServiceManagerFsm::completeShutdown() noexcept {
    try {
        std::lock_guard<std::mutex> lock(mutex_);
        auto& state = snap_.state;
        if (state != ServiceManagerState::ShuttingDown && state != ServiceManagerState::Stopped) {
            applyLocked(ShutdownEvent{});
        }
        if (state == ServiceManagerState::ShuttingDown) {
            applyLocked(ServiceManagerStoppedEvent{});
        }
        if (isTerminal(state)) {
            cv_.notify_all();
        }
        return state == ServiceManagerState::Stopped;
    } catch (const std::exception& e) {
        spdlog::debug("[ServiceManagerFSM] completeShutdown swallowed exception: {}", e.what());
    } catch (...) {
        spdlog::debug("[ServiceManagerFSM] completeShutdown swallowed unknown exception");
    }
    return false;
}

bool ServiceManagerFsm::canInitializeVectors() const noexcept {
    try {
        std::lock_guard<std::mutex> lock(mutex_);
        return snap_.state == ServiceManagerState::SchemaReady;
    } catch (...) {
        return false;
    }
}

bool ServiceManagerFsm::canBuildSearchEngine() const noexcept {
    try {
        std::lock_guard<std::mutex> lock(mutex_);
        auto s = snap_.state;
        return s == ServiceManagerState::VectorsReady || s == ServiceManagerState::SchemaReady;
    } catch (...) {
        return false;
    }
}

bool ServiceManagerFsm::isReady() const noexcept {
    try {
        std::lock_guard<std::mutex> lock(mutex_);
        return snap_.state == ServiceManagerState::Ready;
    } catch (...) {
        return false;
    }
}

bool ServiceManagerFsm::hasShutdown() const noexcept {
    try {
        std::lock_guard<std::mutex> lock(mutex_);
        auto s = snap_.state;
        return s == ServiceManagerState::Stopped || s == ServiceManagerState::ShuttingDown;
    } catch (...) {
        return true;
    }
}

bool ServiceManagerFsm::isTerminalState() const noexcept {
    try {
        std::lock_guard<std::mutex> lock(mutex_);
        return isTerminal(snap_.state);
    } catch (...) {
        return true;
    }
}

ServiceManagerSnapshot ServiceManagerFsm::waitForTerminalState(int timeoutSeconds) noexcept {
    try {
        std::unique_lock<std::mutex> lock(mutex_);
        auto pred = [this]() { return isTerminal(snap_.state); };
        bool completed = cv_.wait_for(lock, std::chrono::seconds(timeoutSeconds), pred);
        if (!completed) {
            try {
                spdlog::warn("[ServiceManagerFSM] waitForTerminalState timed out after {}s, "
                             "current state={}",
                             timeoutSeconds, static_cast<int>(snap_.state));
            } catch (const std::exception& e) {
                spdlog::debug("[ServiceManagerFSM] timeout warning logging failed: {}", e.what());
            } catch (...) {
                spdlog::debug("[ServiceManagerFSM] timeout warning logging failed: unknown error");
            }
        }
        return snap_;
    } catch (...) {
        return snap_;
    }
}

void ServiceManagerFsm::cancelWait() noexcept {
    try {
        std::lock_guard<std::mutex> lock(mutex_);
        cv_.notify_all();
    } catch (const std::exception& e) {
        spdlog::debug("[ServiceManagerFSM] cancelWait swallowed exception: {}", e.what());
    } catch (...) {
        spdlog::debug("[ServiceManagerFSM] cancelWait swallowed unknown exception");
    }
}

void ServiceManagerFsm::reset() noexcept {
    try {
        std::lock_guard<std::mutex> lock(mutex_);
        snap_ = {};
        enterLocked(ServiceManagerState::Uninitialized);
    } catch (const std::exception& e) {
        spdlog::debug("[ServiceManagerFSM] reset swallowed exception: {}", e.what());
    } catch (...) {
        spdlog::debug("[ServiceManagerFSM] reset swallowed unknown exception");
    }
}

} // namespace yams::daemon
