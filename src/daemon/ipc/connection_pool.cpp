#include <yams/daemon/ipc/connection_pool.h>

#include <spdlog/spdlog.h>
#include <cstring>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/un.h>

namespace yams::daemon {

// ============================================================================
// ConnectionPool Implementation
// ============================================================================

ConnectionPool::ConnectionPool(Config config)
    : config_(std::move(config)),
      available_semaphore_(static_cast<std::ptrdiff_t>(config_.max_connections)) {
    connections_.reserve(config_.max_connections);

    // Start pruning thread for idle connection cleanup
    pruning_thread_ = std::thread([this]() {
        while (!stop_requested_.load()) {
            std::this_thread::sleep_for(config_.idle_timeout / 2);
            prune_idle_connections();
        }
    });
}

ConnectionPool::~ConnectionPool() {
    stop_requested_ = true;
    if (pruning_thread_.joinable()) {
        pruning_thread_.join();
    }
    clear();
}

Task<Result<ConnectionPool::ConnectionHandle>> ConnectionPool::acquire() {
    auto start_time = std::chrono::steady_clock::now();

    // Try to acquire from semaphore with timeout
    if (!available_semaphore_.try_acquire_for(config_.connection_timeout)) {
        failed_acquisitions_++;
        co_return Error{ErrorCode::Timeout, "Failed to acquire connection from pool"};
    }

    std::unique_lock lock(mutex_);

    // Find an available connection
    for (auto& conn : connections_) {
        if (!conn.in_use) {
            conn.in_use = true;
            conn.last_used = std::chrono::steady_clock::now();
            conn.use_count++;
            active_count_++;

            // Validate connection is still good
            if (conn.socket && conn.socket->is_valid()) {
                auto wait_time = std::chrono::steady_clock::now() - start_time;
                total_wait_time_ += wait_time;
                total_acquisitions_++;

                auto socket = std::move(conn.socket);
                co_return ConnectionHandle(this, std::move(socket));
            } else {
                // Connection is bad, create a new one
                conn.in_use = false;
                active_count_--;
            }
        }
    }

    lock.unlock();

    // No available connections, create a new one
    auto socket_result = create_connection();
    if (!socket_result) {
        available_semaphore_.release();
        failed_acquisitions_++;
        co_return socket_result.error();
    }

    lock.lock();

    // Add to pool if there's space
    if (connections_.size() < config_.max_connections) {
        PooledConnection& conn = connections_.emplace_back();
        conn.socket = std::move(socket_result).value();
        conn.last_used = std::chrono::steady_clock::now();
        conn.in_use = true;
        conn.use_count = 1;
        active_count_++;

        auto wait_time = std::chrono::steady_clock::now() - start_time;
        total_wait_time_ += wait_time;
        total_acquisitions_++;

        co_return ConnectionHandle(this, std::move(conn.socket));
    }

    // Pool is full, return the connection directly
    auto wait_time = std::chrono::steady_clock::now() - start_time;
    total_wait_time_ += wait_time;
    total_acquisitions_++;
    active_count_++;

    co_return ConnectionHandle(this, std::move(socket_result).value());
}

Result<ConnectionPool::ConnectionHandle> ConnectionPool::try_acquire() {
    if (!available_semaphore_.try_acquire()) {
        size_t active = active_count_.load();
        auto msg = std::string("No connections available (active=") + std::to_string(active) +
                   ", max=" + std::to_string(config_.max_connections) + ")";
        return Error{ErrorCode::ResourceBusy, std::move(msg)};
    }

    std::lock_guard lock(mutex_);

    // Find an available connection
    for (auto& conn : connections_) {
        if (!conn.in_use) {
            conn.in_use = true;
            conn.last_used = std::chrono::steady_clock::now();
            conn.use_count++;
            active_count_++;

            if (conn.socket && conn.socket->is_valid()) {
                total_acquisitions_++;
                auto socket = std::move(conn.socket);
                return ConnectionHandle(this, std::move(socket));
            } else {
                conn.in_use = false;
                active_count_--;
            }
        }
    }

    // No available connections, create new one if possible
    auto socket_result = create_connection();
    if (!socket_result) {
        available_semaphore_.release();
        failed_acquisitions_++;
        return socket_result.error();
    }

    if (connections_.size() < config_.max_connections) {
        PooledConnection& conn = connections_.emplace_back();
        conn.socket = std::move(socket_result).value();
        conn.last_used = std::chrono::steady_clock::now();
        conn.in_use = true;
        conn.use_count = 1;
        active_count_++;
        total_acquisitions_++;

        return ConnectionHandle(this, std::move(conn.socket));
    }

    total_acquisitions_++;
    active_count_++;
    return ConnectionHandle(this, std::move(socket_result).value());
}

void ConnectionPool::release(std::unique_ptr<AsyncSocket> socket) {
    std::lock_guard lock(mutex_);

    // Find a slot to return the connection to
    for (auto& conn : connections_) {
        if (conn.in_use && !conn.socket) {
            conn.socket = std::move(socket);
            conn.last_used = std::chrono::steady_clock::now();
            conn.in_use = false;
            available_semaphore_.release();
            active_count_--;
            total_releases_++;
            return;
        }
    }

    // No slot found (e.g. for a connection created when pool was full),
    // so the connection will be destroyed.
    available_semaphore_.release();
    active_count_--;
    total_releases_++;
}

Result<void> ConnectionPool::warm_up(size_t count) {
    count = std::min(count, config_.max_connections);

    std::lock_guard lock(mutex_);

    for (size_t i = connections_.size(); i < count; ++i) {
        auto socket_result = create_connection();
        if (!socket_result) {
            spdlog::warn("Failed to create connection during warm-up: {}",
                         socket_result.error().message);
            continue;
        }

        PooledConnection& conn = connections_.emplace_back();
        conn.socket = std::move(socket_result).value();
        conn.last_used = std::chrono::steady_clock::now();
        conn.in_use = false;
        conn.use_count = 0;
    }

    return Result<void>();
}

void ConnectionPool::shrink_to_fit() {
    std::lock_guard lock(mutex_);

    auto now = std::chrono::steady_clock::now();
    connections_.erase(std::remove_if(connections_.begin(), connections_.end(),
                                      [now, this](const PooledConnection& conn) {
                                          return !conn.in_use &&
                                                 (now - conn.last_used) > config_.idle_timeout;
                                      }),
                       connections_.end());

    connections_.shrink_to_fit();
}

void ConnectionPool::clear() {
    std::lock_guard lock(mutex_);
    connections_.clear();
    active_count_ = 0;
}

ConnectionPool::Stats ConnectionPool::get_stats() const {
    std::lock_guard lock(mutex_);

    size_t idle_count = 0;
    for (const auto& conn : connections_) {
        if (!conn.in_use) {
            idle_count++;
        }
    }

    return Stats{.total_connections = connections_.size(),
                 .active_connections = active_count_.load(),
                 .idle_connections = idle_count,
                 .total_acquisitions = total_acquisitions_.load(),
                 .total_releases = total_releases_.load(),
                 .failed_acquisitions = failed_acquisitions_.load(),
                 .total_wait_time = total_wait_time_};
}

Result<std::unique_ptr<AsyncSocket>> ConnectionPool::create_connection() {
    std::shared_ptr<ConnectionFactory> factoryCopy;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        factoryCopy = factory_;
    }

    if (!factoryCopy) {
        return Error{ErrorCode::NotImplemented,
                     "Connection factory not set - use UnixSocketFactory or TcpSocketFactory"};
    }

    // Create socket via factory
    auto sockRes = factoryCopy->create();
    if (!sockRes) {
        return sockRes.error();
    }
    auto sock = std::move(sockRes).value();

    // Validate the new connection
    if (auto v = factoryCopy->validate(*sock); !v) {
        return v.error();
    }

    // Apply pool-level socket options (keepalive/nodelay)
    if (auto opt = apply_socket_options(*sock); !opt) {
        return opt.error();
    }

    return sock;
}

void ConnectionPool::prune_idle_connections() {
    std::lock_guard lock(mutex_);

    auto now = std::chrono::steady_clock::now();
    size_t pruned = 0;

    for (auto& conn : connections_) {
        if (!conn.in_use && conn.socket && (now - conn.last_used) > config_.idle_timeout) {
            conn.socket.reset();
            pruned++;
        }
    }

    if (pruned > 0) {
        spdlog::debug("Pruned {} idle connections", pruned);
    }
}

// ============================================================================
// UnixSocketFactory Implementation
// ============================================================================

Result<std::unique_ptr<AsyncSocket>> UnixSocketFactory::create() {
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        return Error{ErrorCode::NetworkError,
                     "Failed to create socket: " + std::string(strerror(errno))};
    }

    struct sockaddr_un addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    std::strncpy(addr.sun_path, socket_path_.c_str(), sizeof(addr.sun_path) - 1);

    if (connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        close(fd);
        return Error{ErrorCode::NetworkError, "Failed to connect: " + std::string(strerror(errno))};
    }

    return std::make_unique<AsyncSocket>(fd, context_);
}

Result<void> UnixSocketFactory::validate(const AsyncSocket& socket) {
    if (!socket.is_valid()) {
        return Error{ErrorCode::InvalidState, "Socket is not valid"};
    }

    // Could add more validation here (e.g., send ping)
    return Result<void>();
}

// ============================================================================
// TcpSocketFactory Implementation
// ============================================================================

Result<std::unique_ptr<AsyncSocket>> TcpSocketFactory::create() {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return Error{ErrorCode::NetworkError,
                     "Failed to create socket: " + std::string(strerror(errno))};
    }

    struct sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port_);

    if (inet_pton(AF_INET, host_.c_str(), &addr.sin_addr) <= 0) {
        close(fd);
        return Error{ErrorCode::InvalidArgument, "Invalid address: " + host_};
    }

    if (connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        close(fd);
        return Error{ErrorCode::NetworkError, "Failed to connect: " + std::string(strerror(errno))};
    }

    return std::make_unique<AsyncSocket>(fd, context_);
}

Result<void> TcpSocketFactory::validate(const AsyncSocket& socket) {
    if (!socket.is_valid()) {
        return Error{ErrorCode::InvalidState, "Socket is not valid"};
    }

    // Could add more validation here
    return Result<void>();
}

} // namespace yams::daemon