/**
 * @file service_manager_test_helper.h
 * @brief Helper for properly initializing ServiceManager in integration tests
 *
 * ServiceManager has a two-phase initialization:
 * 1. initialize() - spawns async thread that runs initializeAsyncAwaitable()
 * 2. Wait for async initialization to complete (PostIngestQueue created)
 *
 * This helper ensures both phases complete properly for testing.
 */

#pragma once

#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/daemon.h>

#include <chrono>
#include <memory>
#include <thread>

namespace yams::test {

/**
 * @brief Initialize ServiceManager and wait for async initialization to complete
 *
 * ServiceManager::initialize() spawns a jthread that runs the async initialization
 * coroutine (see ServiceManager.cpp line 629-641). This helper calls initialize()
 * and waits for PostIngestQueue to be created, which indicates async init completed.
 *
 * @param serviceManager The ServiceManager to initialize
 * @param timeout Maximum time to wait for async initialization
 * @return true if initialization succeeded, false otherwise
 */
inline bool
initializeServiceManagerFully(std::shared_ptr<yams::daemon::ServiceManager> serviceManager,
                              std::chrono::milliseconds timeout = std::chrono::seconds(30)) {
    if (!serviceManager) {
        spdlog::error("ServiceManager is null");
        return false;
    }

    auto start = std::chrono::steady_clock::now();

    // Call initialize() - this spawns the async initialization jthread internally
    // See ServiceManager.cpp line 638: initThread_ = jthread([this]...)
    auto initResult = serviceManager->initialize();
    if (!initResult) {
        spdlog::error("ServiceManager initialize() failed: {}", initResult.error().message);
        return false;
    }

    spdlog::info("ServiceManager initialize() called, waiting for async initialization...");

    // Wait for PostIngestQueue to be created (indicates async init completed)
    // PostIngestQueue is created in initializeAsyncAwaitable() at line 1901
    auto deadline = start + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        auto* queue = serviceManager->getPostIngestQueue();
        if (queue != nullptr) {
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start);
            spdlog::info("ServiceManager fully initialized in {}ms", elapsed.count());

            // Give a brief moment for any final setup to complete
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            return true;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    spdlog::error("ServiceManager async initialization timed out after {}ms", timeout.count());
    return false;
}

/**
 * @brief Wait for PostIngestQueue to exist and be ready
 *
 * @param serviceManager The ServiceManager to check
 * @param timeout Maximum time to wait
 * @return Pointer to PostIngestQueue if available, nullptr otherwise
 */
inline yams::daemon::PostIngestQueue*
waitForPostIngestQueue(std::shared_ptr<yams::daemon::ServiceManager> serviceManager,
                       std::chrono::milliseconds timeout = std::chrono::seconds(5)) {
    auto start = std::chrono::steady_clock::now();

    while (std::chrono::steady_clock::now() - start < timeout) {
        auto* queue = serviceManager->getPostIngestQueue();
        if (queue && queue->threads() > 0) {
            return queue;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    return nullptr;
}

} // namespace yams::test
