#pragma once

#include <yams/core/types.h>
#include <yams/daemon/ipc/async_socket.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/request_handler.h>
#include <yams/daemon/ipc/thread_pool.h>

#include <filesystem>
#include <functional>
#include <memory>
#include <stop_token>
#include <thread>
#include <vector>

namespace yams::daemon {

// Modern async IPC server using coroutines
class AsyncIpcServer {
public:
    using RequestHandlerFunc = std::function<Response(const Request&)>;

    struct Config {
        std::filesystem::path socket_path;
        size_t worker_threads;
        size_t max_connections;
        size_t max_message_size;
        std::chrono::seconds connection_timeout;
        bool reuse_address;

        Config()
            : worker_threads(4), max_connections(100), max_message_size(10 * 1024 * 1024),
              connection_timeout{30}, reuse_address(true) {}
    };

    explicit AsyncIpcServer(Config config = {});
    ~AsyncIpcServer();

    // Non-copyable but movable
    AsyncIpcServer(const AsyncIpcServer&) = delete;
    AsyncIpcServer& operator=(const AsyncIpcServer&) = delete;
    AsyncIpcServer(AsyncIpcServer&&) noexcept;
    AsyncIpcServer& operator=(AsyncIpcServer&&) noexcept;

    // Set the request handler
    void set_handler(RequestHandlerFunc handler);
    void set_processor(std::shared_ptr<RequestProcessor> processor);

    // Server lifecycle
    [[nodiscard]] Result<void> start();
    void stop();
    bool is_running() const { return running_.load(); }

    // Statistics
    struct Stats {
        size_t total_connections{0};
        size_t active_connections{0};
        size_t total_requests{0};
        size_t failed_requests{0};
        size_t bytes_received{0};
        size_t bytes_sent{0};
    };

    [[nodiscard]] Stats get_stats() const {
        Stats copy;
        copy.total_connections = stats_.total_connections.load();
        copy.active_connections = stats_.active_connections.load();
        copy.total_requests = stats_.total_requests.load();
        copy.failed_requests = stats_.failed_requests.load();
        copy.bytes_received = stats_.bytes_received.load();
        copy.bytes_sent = stats_.bytes_sent.load();
        return copy;
    }
    void reset_stats() {
        stats_.total_connections = 0;
        stats_.active_connections = 0;
        stats_.total_requests = 0;
        stats_.failed_requests = 0;
        stats_.bytes_received = 0;
        stats_.bytes_sent = 0;
    }

private:
    [[nodiscard]] Result<void> create_socket();
    [[nodiscard]] Result<void> bind_socket();
    [[nodiscard]] Task<void> accept_loop(std::stop_token token);
    [[nodiscard]] Task<void> handle_client(AsyncSocket socket, std::stop_token token);
    void cleanup_socket();

    Config config_;
    int listen_fd_ = -1;
    std::atomic<bool> running_{false};
    std::stop_source stop_source_;

    // Own the AsyncIOContext for this server instance
    std::unique_ptr<AsyncIOContext> io_context_;

    // Thread pool for handling requests
    std::unique_ptr<ThreadPool> thread_pool_;

    std::unique_ptr<RequestHandler> request_handler_;
    std::shared_ptr<RequestProcessor> processor_;
    RequestHandlerFunc handler_func_;

    // Worker threads for handling connections
    std::vector<std::thread> worker_threads_;
    std::thread accept_thread_;
    std::thread io_thread_; // Single thread for IO event loop

    // Retain client tasks to ensure coroutine lifetime across threads
    std::mutex tasks_mutex_;
    std::vector<Task<void>> client_tasks_;

    // Internal stats with atomics
    struct InternalStats {
        std::atomic<size_t> total_connections{0};
        std::atomic<size_t> active_connections{0};
        std::atomic<size_t> total_requests{0};
        std::atomic<size_t> failed_requests{0};
        std::atomic<size_t> bytes_received{0};
        std::atomic<size_t> bytes_sent{0};
    };
    mutable InternalStats stats_;
};

// Factory for creating the appropriate server based on config
class IpcServerFactory {
public:
    enum class ServerType {
        Blocking, // Original IpcServer
        Async     // New AsyncIpcServer
    };

    static std::unique_ptr<AsyncIpcServer>
    create_async_server(const std::filesystem::path& socket_path, size_t worker_threads = 4);

    static std::unique_ptr<AsyncIpcServer> create_async_server(AsyncIpcServer::Config config);
};

} // namespace yams::daemon
