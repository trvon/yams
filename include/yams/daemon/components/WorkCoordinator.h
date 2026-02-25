// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (c) 2024-2025 YAMS Project Contributors
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

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
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
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
    enum class Priority {
        High,
        Normal,
        Background,
    };

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
     * @brief Get an executor for a specific priority class.
     *
     * High-priority work is isolated from background work through dedicated strands.
     */
    [[nodiscard]] boost::asio::any_io_executor getPriorityExecutor(Priority priority) const;

    template <typename Fn> void post(Priority priority, Fn&& fn) const {
        boost::asio::post(getPriorityExecutor(priority), std::forward<Fn>(fn));
    }

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

    /**
     * @brief Get number of active workers currently executing work.
     *
     * @return Active worker count
     */
    [[nodiscard]] std::size_t getActiveWorkerCount() const noexcept {
        return activeWorkers_.load(std::memory_order_relaxed);
    }

    /**
     * @brief Get work coordinator statistics.
     */
    struct Stats {
        std::size_t workerCount{0};
        std::size_t activeWorkers{0};
        bool isRunning{false};
    };

    [[nodiscard]] Stats getStats() const noexcept {
        return Stats{.workerCount = getWorkerCount(),
                     .activeWorkers = getActiveWorkerCount(),
                     .isRunning = isRunning()};
    }

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

    /// Protects worker lifecycle snapshots used by shutdown diagnostics
    mutable std::mutex workerStateMutex_;

    /// Per-worker thread id (aligned to workers_ index)
    std::vector<std::thread::id> workerThreadIds_;

    /// Per-worker run-loop enter timestamp (steady clock)
    std::vector<std::chrono::steady_clock::time_point> workerRunStart_;

    /// Per-worker run-loop exit timestamp (steady clock)
    std::vector<std::chrono::steady_clock::time_point> workerRunEnd_;

    /// Per-worker run-loop exited flag (true after io_context::run loop exits)
    std::vector<bool> workerExited_;

    /// Stop-request timestamp used to attribute shutdown delay to specific workers
    std::chrono::steady_clock::time_point stopRequestedAt_{};

    /// Whether stopRequestedAt_ is valid for current shutdown cycle
    bool stopRequestedSet_ = false;

    /// Priority-isolated executors bound to the shared io_context
    boost::asio::strand<boost::asio::io_context::executor_type> highPriorityStrand_;
    boost::asio::strand<boost::asio::io_context::executor_type> normalPriorityStrand_;
    boost::asio::strand<boost::asio::io_context::executor_type> backgroundPriorityStrand_;
};

} // namespace yams::daemon
