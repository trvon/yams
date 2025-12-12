// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <yams/daemon/components/DaemonLifecycleFsm.h>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/steady_timer.hpp>

#include <atomic>
#include <memory>

namespace yams::daemon {

/**
 * @brief Manages background task consumers (embedding jobs, FTS5 indexing, orphan scanning).
 *
 * Extracts background task orchestration from ServiceManager into a focused component.
 * Owns coroutines that process InternalEventBus job queues and runs periodic maintenance.
 *
 * ## Architecture
 * - **EmbedJob Consumer**: Processes embedding generation requests from the event bus.
 * - **Fts5Job Consumer**: Handles FTS5 full-text indexing and orphan cleanup operations.
 * - **OrphanScan Task**: Periodic scan for orphaned FTS5 index entries.
 *
 * ## FSM Integration
 * - Reports subsystem health to `DaemonLifecycleFsm` via `setSubsystemDegraded()`.
 * - Tracks "background_tasks" subsystem status.
 *
 * ## Lifecycle
 * 1. Construct with dependencies via DI
 * 2. Call `start()` to launch all background coroutines
 * 3. Call `stop()` to gracefully shut down all tasks
 *
 * @note Must be constructed after ServiceManager's shared_ptr is available
 *       (required for shared_from_this() in coroutines).
 */
class BackgroundTaskManager {
public:
    /**
     * @brief Dependency injection structure for BackgroundTaskManager.
     *
     * All dependencies are passed at construction to enable testability.
     */
    struct Dependencies {
        /// Shared pointer to ServiceManager for accessing services
        /// (required for shared_from_this() in coroutines)
        std::weak_ptr<class ServiceManager> serviceManager;

        /// Lifecycle FSM for degradation reporting
        DaemonLifecycleFsm& lifecycleFsm;

        /// Executor for spawning coroutines
        boost::asio::any_io_executor executor;
    };

    /**
     * @brief Constructs BackgroundTaskManager with required dependencies.
     *
     * @param deps Dependency injection structure
     * @throws std::invalid_argument if executor is invalid
     */
    explicit BackgroundTaskManager(Dependencies deps);

    /**
     * @brief Destructor - ensures all tasks are stopped.
     */
    ~BackgroundTaskManager();

    // Disable copy/move (manages active coroutines)
    BackgroundTaskManager(const BackgroundTaskManager&) = delete;
    BackgroundTaskManager& operator=(const BackgroundTaskManager&) = delete;
    BackgroundTaskManager(BackgroundTaskManager&&) = delete;
    BackgroundTaskManager& operator=(BackgroundTaskManager&&) = delete;

    /**
     * @brief Starts all background task consumers.
     *
     * Launches three coroutines:
     * - EmbedJob consumer (processes embedding generation requests)
     * - Fts5Job consumer (handles full-text indexing)
     * - OrphanScan task (periodic maintenance)
     *
     * @note Idempotent - safe to call multiple times (no-op if already running).
     * @throws std::runtime_error if ServiceManager weak_ptr has expired
     */
    void start();

    /**
     * @brief Stops all background tasks gracefully.
     *
     * Requests cancellation of all coroutines and waits for graceful shutdown.
     * @note Idempotent - safe to call multiple times.
     */
    void stop();

    /**
     * @brief Checks if background tasks are currently running.
     *
     * @return true if tasks are active, false otherwise
     */
    bool isRunning() const noexcept { return running_.load(std::memory_order_acquire); }

private:
    /**
     * @brief Launches the EmbedJob consumer coroutine.
     *
     * Subscribes to "embed_jobs" channel on InternalEventBus and processes
     * embedding generation requests by calling repairMissingEmbeddings().
     *
     * Polls queue at 100ms intervals; triggers embedding generator initialization
     * if not yet ready.
     */
    void launchEmbedJobConsumer();

    /**
     * @brief Launches the Fts5Job consumer coroutine.
     *
     * Subscribes to "fts5_jobs" channel on InternalEventBus and handles:
     * - ExtractAndIndex: Extract text from documents and index in FTS5
     * - RemoveOrphans: Clean up orphaned FTS5 index entries
     *
     * Polls queue at 200ms intervals; tracks detailed failure metrics.
     */
    void launchFts5JobConsumer();

    /**
     * @brief Launches the OrphanScan periodic task.
     *
     * Runs periodic scans (interval configured via TuneAdvisor) to detect
     * orphaned FTS5 index entries (entries for deleted documents).
     *
     * Detected orphans are batched and pushed to the fts5_jobs queue
     * as RemoveOrphans operations.
     *
     * Initial delay: 5 minutes; subsequent runs per TuneAdvisor config.
     */
    void launchOrphanScanTask();

    /**
     * @brief Launches the PathTreeRepair periodic task.
     *
     * Runs periodic scans to detect documents missing from the path tree index
     * and creates the missing entries. This repairs documents that were added
     * before the path tree feature was implemented.
     *
     * Initial delay: 2 minutes; runs once per daemon lifetime (not periodic).
     */
    void launchPathTreeRepairTask();

    void launchCheckpointTask();

    /**
     * @brief Launches the Storage GC periodic task.
     *
     * Runs periodic garbage collection to remove unreferenced storage chunks
     * (blocks with ref_count=0 that are not referenced by any manifest).
     *
     * This is critical for reclaiming disk space after document deletions.
     *
     * Initial delay: 10 minutes; runs hourly thereafter.
     */
    void launchStorageGcTask();

    Dependencies deps_;                ///< Dependency injection container
    std::atomic<bool> running_{false}; ///< Tracks whether tasks are active

    // Shared stop flag that coroutines can safely capture
    // (must outlive coroutines, so use shared_ptr)
    std::shared_ptr<std::atomic<bool>> stopRequested_;

    // Note: Coroutines are detached (boost::asio::detached) and run until
    // the executor stops. No manual timer management - coroutines create
    // their own local timers and exit naturally when stopRequested_ is set
    // and the executor shuts down.
};

} // namespace yams::daemon
