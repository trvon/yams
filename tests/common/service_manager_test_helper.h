#pragma once

#include <chrono>
#include <memory>
#include <thread>

#include <spdlog/spdlog.h>

#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/ServiceManagerFsm.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/daemon.h>

namespace yams::test {

inline bool
initializeServiceManagerFully(std::shared_ptr<yams::daemon::ServiceManager> serviceManager,
                              std::chrono::milliseconds timeout = std::chrono::seconds(30),
                              bool allowNonReadyStates = false) {
    if (!serviceManager) {
        spdlog::error("ServiceManager is null");
        return false;
    }

    auto initResult = serviceManager->initialize();
    if (!initResult) {
        spdlog::error("ServiceManager initialize() failed: {}", initResult.error().message);
        return false;
    }

    spdlog::info("ServiceManager initialize() called, starting async initialization...");

    serviceManager->startAsyncInit();

    int timeoutSeconds =
        static_cast<int>(std::chrono::duration_cast<std::chrono::seconds>(timeout).count());
    auto snapshot = serviceManager->waitForServiceManagerTerminalState(timeoutSeconds);

    if (snapshot.state == yams::daemon::ServiceManagerState::Ready) {
        spdlog::info("ServiceManager fully initialized");
        return true;
    }

    if (allowNonReadyStates) {
        // Some integration tests only need the schema/vector DB to exist; they may not need the
        // fully built search engine.
        if (snapshot.state == yams::daemon::ServiceManagerState::SchemaReady ||
            snapshot.state == yams::daemon::ServiceManagerState::VectorsReady ||
            snapshot.state == yams::daemon::ServiceManagerState::BuildingSearchEngine) {
            spdlog::warn("ServiceManager not Ready within timeout (state={}); continuing "
                         "(allowNonReadyStates)",
                         static_cast<int>(snapshot.state));
            return true;
        }
    }

    if (snapshot.state == yams::daemon::ServiceManagerState::Failed) {
        spdlog::error("ServiceManager async initialization failed: {}", snapshot.lastError);
        return false;
    }

    spdlog::error("ServiceManager async initialization timed out or unexpected state: {}",
                  static_cast<int>(snapshot.state));
    return false;
}

inline yams::daemon::PostIngestQueue*
waitForPostIngestQueue(std::shared_ptr<yams::daemon::ServiceManager> serviceManager,
                       std::chrono::milliseconds timeout = std::chrono::seconds(5)) {
    auto start = std::chrono::steady_clock::now();

    while (std::chrono::steady_clock::now() - start < timeout) {
        auto* queue = serviceManager->getPostIngestQueue();
        if (queue) {
            return queue;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    return nullptr;
}

} // namespace yams::test
