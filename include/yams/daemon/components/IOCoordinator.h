// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <optional>
#include <thread>
#include <vector>

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>

namespace yams::daemon {

/**
 * @brief Dedicated I/O thread pool for socket operations.
 *
 * IOCoordinator provides a separate io_context and thread pool specifically
 * for socket I/O operations (accept, read, write). This prevents CPU-bound
 * work from starving socket operations under load.
 *
 * ## Problem Addressed
 *
 * When the daemon is overwhelmed:
 * 1. All WorkCoordinator threads become busy with slow operations (DB, indexing)
 * 2. Socket read timeouts trigger (idle detection)
 * 3. Server closes "idle" connections aggressively
 * 4. Clients see "Connection closed by server (possibly stale connection)"
 *
 * ## Solution
 *
 * Separate I/O context with dedicated threads ensures socket operations
 * (accept loop, connection reads/writes) are never starved by CPU-bound work.
 *
 * ## Architecture
 *
 * ```
 * ┌─────────────────────┐     ┌─────────────────────┐
 * │   IOCoordinator     │     │   WorkCoordinator   │
 * │   (Socket I/O)      │     │   (CPU Work)        │
 * │                     │     │                     │
 * │  ┌───────────────┐  │     │  ┌───────────────┐  │
 * │  │ io_context    │  │     │  │ io_context    │  │
 * │  │ (2 threads)   │  │     │  │ (N threads)   │  │
 * │  └───────────────┘  │     │  └───────────────┘  │
 * │                     │     │                     │
 * │  - Socket accept    │     │  - DB queries       │
 * │  - Connection read  │     │  - Indexing         │
 * │  - Response write   │     │  - Search           │
 * └─────────────────────┘     └─────────────────────┘
 * ```
 *
 * ## Usage
 *
 * ```cpp
 * // In daemon initialization
 * IOCoordinator::Config io_cfg;
 * io_cfg.num_threads = 2;
 * auto io_coordinator = std::make_unique<IOCoordinator>(io_cfg);
 * io_coordinator->start();
 *
 * // SocketServer uses IO executor
 * SocketServer::Config sock_cfg;
 * sock_cfg.executor = io_coordinator->getExecutor();
 *
 * // RequestDispatcher uses WorkCoordinator (CPU executor)
 * dispatcher->setExecutor(work_coordinator->getExecutor());
 * ```
 *
 * ## Configuration
 *
 * - `YAMS_IO_THREADS`: Number of dedicated I/O threads (default: 2)
 *
 * @see WorkCoordinator for CPU-bound work execution
 * @see PBI-089 for design documentation
 */
class IOCoordinator {
public:
    /**
     * @brief Configuration for IOCoordinator.
     */
    struct Config {
        /// Number of dedicated I/O threads (default: 2)
        size_t num_threads = 2;

        /// Whether to pin I/O threads to CPUs (experimental, default: false)
        bool pin_threads = false;
    };

    /**
     * @brief Construct IOCoordinator with configuration.
     *
     * @param config Configuration options
     */
    explicit IOCoordinator(Config config);

    /**
     * @brief Construct IOCoordinator with default configuration.
     */
    IOCoordinator();

    /**
     * @brief Destructor ensures clean shutdown.
     */
    ~IOCoordinator();

    // Non-copyable, non-movable
    IOCoordinator(const IOCoordinator&) = delete;
    IOCoordinator& operator=(const IOCoordinator&) = delete;
    IOCoordinator(IOCoordinator&&) = delete;
    IOCoordinator& operator=(IOCoordinator&&) = delete;

    /**
     * @brief Start the I/O thread pool.
     *
     * @throws std::runtime_error if already started
     */
    void start();

    /**
     * @brief Stop accepting new work and drain io_context.
     */
    void stop();

    /**
     * @brief Wait for all I/O threads to finish.
     */
    void join();

    /**
     * @brief Get shared pointer to io_context.
     */
    [[nodiscard]] std::shared_ptr<boost::asio::io_context> getIOContext() const noexcept;

    /**
     * @brief Get io_context executor for socket operations.
     */
    [[nodiscard]] boost::asio::io_context::executor_type getExecutor() const noexcept;

    /**
     * @brief Create a strand for serialized I/O operations.
     */
    [[nodiscard]] boost::asio::strand<boost::asio::io_context::executor_type> makeStrand() const;

    /**
     * @brief Check if I/O threads are running.
     */
    [[nodiscard]] bool isRunning() const noexcept;

    /**
     * @brief Get number of I/O threads.
     */
    [[nodiscard]] size_t getThreadCount() const noexcept;

    /**
     * @brief Get configuration (read-only).
     */
    [[nodiscard]] const Config& config() const noexcept;

private:
    Config config_;
    std::shared_ptr<boost::asio::io_context> io_context_;
    std::optional<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>>
        work_guard_;
    std::vector<std::thread> threads_;
    std::atomic<bool> running_{false};
};

} // namespace yams::daemon
