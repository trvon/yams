#pragma once

#include <yams/core/types.h>
#include <yams/daemon/ipc/async_socket.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <semaphore>
#include <stop_token>
#include <thread>
#include <vector>

namespace yams::daemon {

// Connection pool for efficient socket management
class ConnectionPool {
public:
    struct Config {
        size_t min_connections;
        size_t max_connections;
        std::chrono::seconds idle_timeout;
        std::chrono::seconds connection_timeout;
        bool keepalive;
        bool nodelay;

        Config()
            : min_connections(1), max_connections(10), idle_timeout{60}, connection_timeout{5},
              keepalive(true), nodelay(true) {}
    };

    explicit ConnectionPool(Config config = {});
    ~ConnectionPool();

    // RAII handle for pooled connections
    class ConnectionHandle {
    public:
        ConnectionHandle() = default;
        ConnectionHandle(ConnectionPool* pool, std::unique_ptr<AsyncSocket> socket)
            : pool_(pool), socket_(std::move(socket)) {}

        ~ConnectionHandle() {
            if (pool_ && socket_) {
                pool_->release(std::move(socket_));
            }
        }

        // Move-only
        ConnectionHandle(ConnectionHandle&& other) noexcept
            : pool_(std::exchange(other.pool_, nullptr)), socket_(std::move(other.socket_)) {}

        ConnectionHandle& operator=(ConnectionHandle&& other) noexcept {
            if (this != &other) {
                if (pool_ && socket_) {
                    pool_->release(std::move(socket_));
                }
                pool_ = std::exchange(other.pool_, nullptr);
                socket_ = std::move(other.socket_);
            }
            return *this;
        }

        // Delete copy operations
        ConnectionHandle(const ConnectionHandle&) = delete;
        ConnectionHandle& operator=(const ConnectionHandle&) = delete;

        // Access socket
        AsyncSocket* operator->() { return socket_.get(); }
        const AsyncSocket* operator->() const { return socket_.get(); }
        AsyncSocket& operator*() { return *socket_; }
        const AsyncSocket& operator*() const { return *socket_; }

        bool is_valid() const { return socket_ && socket_->is_valid(); }
        explicit operator bool() const { return is_valid(); }

    private:
        ConnectionPool* pool_ = nullptr;
        std::unique_ptr<AsyncSocket> socket_;
    };

    // Acquire a connection from the pool
    [[nodiscard]] Task<Result<ConnectionHandle>> acquire();
    [[nodiscard]] Result<ConnectionHandle> try_acquire();

    // Pool management
    Result<void> warm_up(size_t count);
    void shrink_to_fit();
    void clear();

    // Statistics
    struct Stats {
        size_t total_connections;
        size_t active_connections;
        size_t idle_connections;
        size_t total_acquisitions;
        size_t total_releases;
        size_t failed_acquisitions;
        std::chrono::steady_clock::duration total_wait_time;
    };

    [[nodiscard]] Stats get_stats() const;

private:
    struct PooledConnection {
        std::unique_ptr<AsyncSocket> socket;
        std::chrono::steady_clock::time_point last_used;
        bool in_use = false;
        size_t use_count = 0;
    };

    void release(std::unique_ptr<AsyncSocket> socket);
    [[nodiscard]] Result<std::unique_ptr<AsyncSocket>> create_connection();
    void prune_idle_connections();

    Config config_;
    mutable std::mutex mutex_;
    std::vector<PooledConnection> connections_;
    std::counting_semaphore<> available_semaphore_;
    std::atomic<size_t> active_count_{0};

    // Statistics
    std::atomic<size_t> total_acquisitions_{0};
    std::atomic<size_t> total_releases_{0};
    std::atomic<size_t> failed_acquisitions_{0};
    std::chrono::steady_clock::duration total_wait_time_{};

    std::thread pruning_thread_;
    std::atomic<bool> stop_requested_{false};
};

// Connection factory for creating new connections
class ConnectionFactory {
public:
    virtual ~ConnectionFactory() = default;

    [[nodiscard]] virtual Result<std::unique_ptr<AsyncSocket>> create() = 0;
    [[nodiscard]] virtual Result<void> validate(const AsyncSocket& socket) = 0;
};

// Unix domain socket connection factory
class UnixSocketFactory : public ConnectionFactory {
public:
    explicit UnixSocketFactory(std::string socket_path, AsyncIOContext& context)
        : socket_path_(std::move(socket_path)), context_(context) {}

    [[nodiscard]] Result<std::unique_ptr<AsyncSocket>> create() override;
    [[nodiscard]] Result<void> validate(const AsyncSocket& socket) override;

private:
    std::string socket_path_;
    AsyncIOContext& context_;
};

// TCP socket connection factory
class TcpSocketFactory : public ConnectionFactory {
public:
    TcpSocketFactory(std::string host, uint16_t port, AsyncIOContext& context)
        : host_(std::move(host)), port_(port), context_(context) {}

    [[nodiscard]] Result<std::unique_ptr<AsyncSocket>> create() override;
    [[nodiscard]] Result<void> validate(const AsyncSocket& socket) override;

private:
    std::string host_;
    uint16_t port_;
    AsyncIOContext& context_;
};

} // namespace yams::daemon