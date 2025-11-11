#pragma once

#include <boost/asio/awaitable.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/local/stream_protocol.hpp>
#include <yams/core/types.h>

#include <atomic>
#include <chrono>
#include <filesystem>
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
        size_t workerThreads = 1; // Default 1, tuneable via TuneAdvisor
        std::chrono::milliseconds connectionTimeout{2000};
        std::chrono::milliseconds acceptBackoffMs{100};
    };

    SocketServer(const Config& config, RequestDispatcher* dispatcher, StateComponent* state);
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

private:
    // Async operations
    boost::asio::awaitable<void> accept_loop();
    boost::asio::awaitable<void>
    handle_connection(boost::asio::local::stream_protocol::socket socket);

    // Register active socket for deterministic shutdown (RAII pattern)
    void register_socket(std::weak_ptr<boost::asio::local::stream_protocol::socket> socket);

    // Configuration
    Config config_;
    RequestDispatcher* dispatcher_;
    StateComponent* state_;
    mutable std::mutex dispatcherMutex_;

    // Boost.ASIO components
    boost::asio::io_context io_context_;
    std::unique_ptr<boost::asio::local::stream_protocol::acceptor> acceptor_;

    // Worker pool - using std::thread for explicit control over join/detach
    std::vector<std::thread> workers_;

    // Keep io_context_ alive while running (RAII with optional)
    std::optional<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>>
        work_guard_;

    // Socket tracking
    std::filesystem::path actualSocketPath_;

    // Connection metrics
    std::atomic<size_t> activeConnections_{0};
    std::atomic<uint64_t> totalConnections_{0};

    std::shared_ptr<std::atomic<std::size_t>> writerBudget_;

    // Lifecycle state
    std::atomic<bool> running_{false};
    std::atomic<bool> stopping_{false};

    // Stop token source for canceling active connections during shutdown
    yams::compat::stop_source stop_source_;

    // Track active connections for deterministic shutdown
    std::mutex activeSocketsMutex_;
    std::vector<std::weak_ptr<boost::asio::local::stream_protocol::socket>> activeSockets_;

    // Track connection futures for graceful shutdown (PBI-066-41)
    std::mutex connectionFuturesMutex_;
    std::vector<std::future<void>> connectionFutures_;

    std::unique_ptr<std::counting_semaphore<>> connectionSlots_;

    void prune_completed_futures();
};

} // namespace yams::daemon
