#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include <boost/asio/awaitable.hpp>

#include <yams/daemon/client/asio_connection.h>
#include <yams/daemon/client/transport_options.h>

namespace yams::daemon {

// Connection pool that maintains multiple reusable connections per socket path.
// Supports concurrent access with proper lifecycle management and stale connection removal.
class AsioConnectionPool : public std::enable_shared_from_this<AsioConnectionPool> {
public:
    static std::shared_ptr<AsioConnectionPool> get_or_create(const TransportOptions& opts);
    static void shutdown_all(std::chrono::milliseconds timeout = std::chrono::milliseconds{2000});

    boost::asio::awaitable<std::shared_ptr<AsioConnection>> acquire();
    void release(const std::shared_ptr<AsioConnection>& conn);
    void shutdown(std::chrono::milliseconds timeout = std::chrono::milliseconds{2000});

    AsioConnectionPool(const TransportOptions& opts, bool shared);

private:
    boost::asio::awaitable<std::shared_ptr<AsioConnection>> create_connection();
    void cleanup_stale_connections();

    TransportOptions opts_;
    bool shared_{true};
    std::mutex mutex_;

    // Pool of available connections (not just one!)
    // Use weak_ptr to allow connections to be destroyed when not in use
    std::vector<std::weak_ptr<AsioConnection>> connection_pool_;
    static constexpr size_t kMaxPoolSize = 32; // Cap concurrent connections per socket path

    // RAII wrapper for automatic checkin on destruction
    class ConnectionGuard {
    public:
        explicit ConnectionGuard(std::shared_ptr<AsioConnection> conn) : conn_(std::move(conn)) {
            if (conn_) {
                conn_->in_use.store(true, std::memory_order_relaxed);
            }
        }
        ~ConnectionGuard() {
            if (conn_) {
                conn_->in_use.store(false, std::memory_order_relaxed);
            }
        }
        ConnectionGuard(const ConnectionGuard&) = delete;
        ConnectionGuard& operator=(const ConnectionGuard&) = delete;
        ConnectionGuard(ConnectionGuard&&) = default;
        ConnectionGuard& operator=(ConnectionGuard&&) = default;

        std::shared_ptr<AsioConnection> get() const { return conn_; }

    private:
        std::shared_ptr<AsioConnection> conn_;
    };
};

} // namespace yams::daemon
