// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 YAMS Project Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>

namespace yams::daemon {

/**
 * @brief Centralized async work coordination component for daemon services.
 *
 * WorkCoordinator encapsulates all threading complexity for the ServiceManager,
 * providing a unified io_context with work-stealing thread pool and strand factory.
 *
 * ## Design Rationale
 *
 * Previously, ServiceManager (3700+ lines) directly owned io_context, work guard,
 * and worker threads. This WorkCoordinator extraction provides:
 *
 * 1. **Separation of Concerns**: ServiceManager delegates threading to WorkCoordinator
 * 2. **Reusability**: Other components can use WorkCoordinator for async operations
 * 3. **Testability**: WorkCoordinator can be unit tested independently
 * 4. **Clean Shutdown**: Centralized lifecycle management with graceful work draining
 *
 * ## Thread Pool Architecture
 *
 * - **Shared io_context**: All async work runs on unified thread pool
 * - **Work Stealing**: Threads can execute work from any service's strand
 * - **Strands for Isolation**: Each service gets dedicated strand for ordering guarantees
 *
 * ## Usage Pattern
 *
 * ```cpp
 * // In ServiceManager constructor
 * workCoordinator_ = std::make_unique<WorkCoordinator>();
 * workCoordinator_->start();
 *
 * // In service constructors
 * strand_ = workCoordinator_->makeStrand();
 *
 * // Post async work
 * boost::asio::post(strand_, []() { doWork(); });
 * ```
 *
 * ## Shutdown Behavior
 *
 * - `stop()`: Resets work guard, allowing io_context to drain naturally
 * - `join()`: Blocks until all workers complete (safe for destruction)
 *
 * @see docs/delivery/003/prd.md for full design documentation
 */
class WorkCoordinator {
public:
    /**
     * @brief Construct WorkCoordinator (does not start threads).
     *
     * Call start() explicitly to spawn worker threads.
     */
    WorkCoordinator();

    /**
     * @brief Destructor ensures clean shutdown.
     *
     * Automatically calls stop() and join() if not already done.
     */
    ~WorkCoordinator();

    // Non-copyable, non-movable (owns thread resources)
    WorkCoordinator(const WorkCoordinator&) = delete;
    WorkCoordinator& operator=(const WorkCoordinator&) = delete;
    WorkCoordinator(WorkCoordinator&&) = delete;
    WorkCoordinator& operator=(WorkCoordinator&&) = delete;

    /**
     * @brief Start the worker thread pool.
     *
     * Spawns std::thread::hardware_concurrency() threads (minimum 1).
     * Each thread runs io_context.run() in a loop.
     *
     * @param numThreads Optional thread count override (default: hardware_concurrency)
     *
     * @throws std::runtime_error if already started or thread creation fails
     */
    void start(std::optional<std::size_t> numThreads = std::nullopt);

    /**
     * @brief Stop accepting new work and drain io_context.
     *
     * Resets work guard, allowing io_context to exit when all posted work completes.
     * Does NOT block - call join() to wait for threads.
     *
     * Safe to call multiple times (idempotent).
     */
    void stop();

    /**
     * @brief Wait for all worker threads to finish.
     *
     * Blocks until io_context.run() completes in all workers.
     * Must call stop() first, or this will hang indefinitely.
     *
     * Safe to call multiple times (idempotent).
     */
    void join();

    /**
     * @brief Wait for workers with timeout, detaching any that don't finish.
     *
     * Like join(), but returns false if timeout expires before all workers exit.
     * Workers that don't exit within the timeout are detached and will be cleaned
     * up when the process terminates.
     *
     * @param timeout Maximum time to wait for workers to exit
     * @return true if all workers joined within timeout, false if timeout expired
     */
    bool joinWithTimeout(std::chrono::milliseconds timeout);

    /**
     * @brief Get shared pointer to io_context for direct access.
     *
     * Used by components that need to spawn coroutines with co_spawn.
     *
     * @return Shared io_context (never null after construction)
     */
    [[nodiscard]] std::shared_ptr<boost::asio::io_context> getIOContext() const noexcept;

    /**
     * @brief Get io_context executor for posting work.
     *
     * @return Executor type for boost::asio::post operations
     */
    [[nodiscard]] boost::asio::io_context::executor_type getExecutor() const noexcept;

    /**
     * @brief Create a new strand for logical work isolation.
     *
     * Strands guarantee that posted work executes serially (FIFO order),
     * while still allowing work stealing across thread pool.
     *
     * @return New strand bound to io_context executor
     */
    [[nodiscard]] boost::asio::strand<boost::asio::io_context::executor_type> makeStrand() const;

    /**
     * @brief Check if worker threads are running.
     *
     * @return true if start() called and threads active, false otherwise
     */
    [[nodiscard]] bool isRunning() const noexcept;

    /**
     * @brief Get number of worker threads.
     *
     * @return Worker count (0 if not started)
     */
    [[nodiscard]] std::size_t getWorkerCount() const noexcept;

private:
    /// Shared io_context for all async operations
    std::shared_ptr<boost::asio::io_context> ioContext_;

    /// Work guard to keep io_context alive until stop() called
    std::optional<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>>
        workGuard_;

    /// Worker thread pool running io_context.run()
    std::vector<std::thread> workers_;

    /// Flag to track if start() has been called
    bool started_ = false;

    /// Count of workers currently executing io_context::run()
    std::atomic<std::size_t> activeWorkers_{0};

    /// Mutex for joinWithTimeout() condition variable
    std::mutex joinMutex_;

    /// Condition variable for joinWithTimeout() to wait on worker exit
    std::condition_variable joinCV_;
};

} // namespace yams::daemon
