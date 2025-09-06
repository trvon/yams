#pragma once

#include <boost/asio/awaitable.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/local/stream_protocol.hpp>
#include <yams/core/types.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

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
        size_t workerThreads = 4;
        std::chrono::milliseconds connectionTimeout{30000};
        std::chrono::milliseconds acceptBackoffMs{100};
    };

    SocketServer(const Config& config, RequestDispatcher* dispatcher, StateComponent* state);
    ~SocketServer();

    // Lifecycle
    Result<void> start();
    Result<void> stop();
    bool isRunning() const { return running_.load(); }

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

    // Configuration
    Config config_;
    RequestDispatcher* dispatcher_;
    StateComponent* state_;
    mutable std::mutex dispatcherMutex_;

    // Boost.ASIO components
    boost::asio::io_context io_context_;
    std::unique_ptr<boost::asio::local::stream_protocol::acceptor> acceptor_;
    std::vector<std::thread> workers_;
    // Keep io_context_ alive while running to avoid race where threads exit
    std::optional<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>>
        work_guard_;

    // Socket tracking
    std::filesystem::path actualSocketPath_;

    // Connection metrics
    std::atomic<size_t> activeConnections_{0};
    std::atomic<uint64_t> totalConnections_{0};

    // Lifecycle state
    std::atomic<bool> running_{false};
    std::atomic<bool> stopping_{false};
};

} // namespace yams::daemon
