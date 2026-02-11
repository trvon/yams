#pragma once

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/local/stream_protocol.hpp>
#include <yams/core/types.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <semaphore>
#include <thread>
#include <vector>
#include <yams/compat/thread_stop_compat.h>

namespace yams::daemon {

// Forward declarations
class RequestDispatcher;
class IOCoordinator;
class WorkCoordinator;
struct StateComponent;

/**
 * Modernized socket server using native Boost.ASIO
 *
 * This implementation directly uses Boost.ASIO's local::stream_protocol
 * for Unix domain sockets, eliminating unnecessary abstraction layers.
 */
class SocketServer {
public:
    struct Config {
        std::filesystem::path socketPath;
        size_t maxConnections = 1024;
        std::chrono::milliseconds connectionTimeout{2000};
        std::chrono::milliseconds acceptBackoffMs{100};
        std::chrono::seconds maxConnectionLifetime{300}; // 5 minutes absolute limit

        // Proxy socket for multiplexed CLI connections (empty = disabled)
        std::filesystem::path proxySocketPath;
        size_t proxyMaxConnections = 512;
    };

    SocketServer(const Config& config, IOCoordinator* ioCoordinator, WorkCoordinator* coordinator,
                 RequestDispatcher* dispatcher, StateComponent* state);
    ~SocketServer();

    // Lifecycle
    Result<void> start();
    Result<void> stop();
    bool isRunning() const { return running_.load(); }

    void setWriterBudget(std::size_t bytes);

    // Optional: allow safe rebinding of the dispatcher after startup.
    void setDispatcher(RequestDispatcher* dispatcher) {
        std::lock_guard<std::mutex> lk(dispatcherMutex_);
        dispatcher_ = dispatcher;
    }

    // Metrics
    size_t activeConnections() const { return activeConnections_.load(); }
    uint64_t totalConnections() const { return totalConnections_.load(); }
    uint64_t connectionToken() const { return connectionToken_.load(); }

    // Proxy metrics
    size_t proxyActiveConnections() const { return proxyActiveConnections_.load(); }
    std::filesystem::path proxySocketPath() const {
        std::lock_guard<std::mutex> lk(socketPathsMutex_);
        return proxySocketPath_;
    }

    // Compute age of oldest active connection (in seconds); 0 if no connections
    uint64_t oldestConnectionAgeSeconds() const;

    // -------- Dynamic connection slot sizing (PBI-085) --------
    // Resize the connection slot semaphore. Grows immediately, shrinks gracefully via debt
    // tracking. Returns true if resize succeeded (or was a no-op), false on error.
    bool resizeConnectionSlots(size_t newSize);

    // Get current slot limit (approximate, may be slightly stale under high contention)
    size_t getConnectionSlotLimit() const { return slotLimit_.load(std::memory_order_relaxed); }

    // Check slot utilization: activeConnections / slotLimit (0.0 to 1.0+ if overcommitted)
    double getSlotUtilization() const;

private:
    // Async operations
    boost::asio::awaitable<void> accept_loop(bool isProxy = false);
    struct TrackedSocket {
        std::shared_ptr<boost::asio::local::stream_protocol::socket> socket;
        boost::asio::any_io_executor executor;
        // Use atomic to prevent data races when accessing creation time from multiple threads
        // Store as nanoseconds since steady_clock epoch for thread-safety
        std::atomic<int64_t> created_at_ns{0}; // Connection creation time in nanoseconds

        // Helper to get creation time as time_point (thread-safe read)
        std::chrono::steady_clock::time_point created_at() const {
            return std::chrono::steady_clock::time_point(
                std::chrono::nanoseconds(created_at_ns.load(std::memory_order_acquire)));
        }

        // Helper to set creation time (thread-safe write)
        void set_created_at(std::chrono::steady_clock::time_point tp) {
            created_at_ns.store(
                std::chrono::duration_cast<std::chrono::nanoseconds>(tp.time_since_epoch()).count(),
                std::memory_order_release);
        }
    };
    boost::asio::awaitable<void> handle_connection(std::shared_ptr<TrackedSocket> tracked_socket,
                                                   uint64_t conn_token, bool isProxy = false);

    // Register active socket for deterministic shutdown (RAII pattern)
    void register_socket(std::shared_ptr<TrackedSocket> tracked_socket);

    // Configuration
    Config config_;
    IOCoordinator* ioCoordinator_{nullptr};
    WorkCoordinator* coordinator_;
    RequestDispatcher* dispatcher_;
    StateComponent* state_;
    mutable std::mutex dispatcherMutex_;

    // Boost.ASIO components (use IOCoordinator's io_context)
    std::unique_ptr<boost::asio::local::stream_protocol::acceptor> acceptor_;
    std::future<void> acceptLoopFuture_;

    // Proxy acceptor (second listen socket for multiplexed CLI connections)
    std::unique_ptr<boost::asio::local::stream_protocol::acceptor> proxyAcceptor_;
    std::future<void> proxyAcceptLoopFuture_;
    std::atomic<size_t> proxyActiveConnections_{0};
    std::filesystem::path proxySocketPath_;

    // Socket tracking
    std::filesystem::path actualSocketPath_;

    // actualSocketPath_ and proxySocketPath_ are read from other threads (e.g. DaemonMetrics)
    // while being mutated during start/stop/rebuild.
    mutable std::mutex socketPathsMutex_;

    // Connection metrics
    std::atomic<size_t> activeConnections_{0};
    std::atomic<size_t> mainActiveConnections_{0};
    std::atomic<uint64_t> totalConnections_{0};
    std::atomic<uint64_t> connectionToken_{0};
    std::atomic<uint64_t> forcedCloseCount_{0}; // Connections closed due to lifetime exceeded

    std::shared_ptr<std::atomic<std::size_t>> writerBudget_;

    // Lifecycle state
    std::atomic<bool> running_{false};
    std::atomic<bool> stopping_{false};

    // Stop token source for canceling active connections during shutdown
    yams::compat::stop_source stop_source_;

    // Track active connections for deterministic shutdown
    mutable std::mutex activeSocketsMutex_;
    std::vector<std::weak_ptr<TrackedSocket>> activeSockets_;

    // Track connection futures for graceful shutdown (PBI-066-41)
    std::mutex connectionFuturesMutex_;
    std::vector<std::future<void>> connectionFutures_;

    std::unique_ptr<std::counting_semaphore<>> connectionSlots_;
    std::unique_ptr<std::counting_semaphore<>> proxyConnectionSlots_;

    // Dynamic sizing state (PBI-085)
    std::atomic<size_t> slotLimit_{0};   // Current slot limit (tracked for resize decisions)
    std::atomic<int32_t> shrinkDebt_{0}; // Deficit when shrinking below active connections

    void prune_completed_futures();
    void execute_on_io_context(std::function<void()> fn);
    void close_acceptor_on_executor();
    std::size_t
    close_sockets_on_executor(const std::vector<std::shared_ptr<TrackedSocket>>& tracked_sockets);
};

} // namespace yams::daemon
