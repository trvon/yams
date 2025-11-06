#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <thread>
#if defined(__cpp_lib_jthread) && __cpp_lib_jthread >= 201911L
#include <semaphore>
#include <stop_token>
#endif
#include <memory>
#include <mutex>
#include <queue>
#include <type_traits>
#include <yams/metadata/database.h>

namespace yams::metadata {

/**
 * @brief Configuration for connection pool
 */
struct ConnectionPoolConfig {
    size_t minConnections = 2;                   ///< Minimum connections to maintain
    size_t maxConnections = 10;                  ///< Maximum connections allowed
    std::chrono::seconds idleTimeout{300};       ///< Idle connection timeout
    std::chrono::seconds maxConnectionAge{3600}; ///< Maximum connection age before refresh
    std::chrono::seconds connectTimeout{
        2}; ///< Connection establishment timeout (mirrors socket server)
    std::chrono::milliseconds busyTimeout{
        2000};                     ///< SQLite busy timeout (mirrors socket server default)
    bool enableWAL = true;         ///< Enable WAL mode
    bool enableForeignKeys = true; ///< Enable foreign key constraints
};

/**
 * @brief Database connection wrapper with metadata
 */
class PooledConnection {
public:
    explicit PooledConnection(std::unique_ptr<Database> db,
                              std::function<void(PooledConnection*)> returnFunc,
                              std::uint64_t generation = 0);
    ~PooledConnection();

    // Move-only
    PooledConnection(PooledConnection&& other) noexcept;
    PooledConnection& operator=(PooledConnection&& other) noexcept;
    PooledConnection(const PooledConnection&) = delete;
    PooledConnection& operator=(const PooledConnection&) = delete;

    /**
     * @brief Access the underlying database
     */
    Database* operator->() { return db_.get(); }
    const Database* operator->() const { return db_.get(); }
    Database& operator*() { return *db_; }
    const Database& operator*() const { return *db_; }

    /**
     * @brief Check if connection is valid
     */
    [[nodiscard]] bool isValid() const { return db_ != nullptr; }

    /**
     * @brief Get last access time
     */
    [[nodiscard]] std::chrono::steady_clock::time_point lastAccessed() const {
        return lastAccessed_;
    }

    /**
     * @brief Get creation time
     */
    [[nodiscard]] std::chrono::steady_clock::time_point createdAt() const { return createdAt_; }

    /**
     * @brief Mark connection as accessed
     */
    void touch() { lastAccessed_ = std::chrono::steady_clock::now(); }

private:
    friend class ConnectionPool; // Allow ConnectionPool to access private members

    std::unique_ptr<Database> db_;
    std::function<void(PooledConnection*)> returnFunc_;
    std::chrono::steady_clock::time_point lastAccessed_;
    std::chrono::steady_clock::time_point createdAt_{std::chrono::steady_clock::now()};
    bool returned_ = false;
    uint64_t generation_ = 0; // PBI-079: Track which refresh generation this connection is from
};

/**
 * @brief Thread-safe database connection pool
 */
class ConnectionPool {
public:
    explicit ConnectionPool(const std::string& dbPath, const ConnectionPoolConfig& config = {});
    ~ConnectionPool();

    // Non-copyable, non-movable
    ConnectionPool(const ConnectionPool&) = delete;
    ConnectionPool& operator=(const ConnectionPool&) = delete;
    ConnectionPool(ConnectionPool&&) = delete;
    ConnectionPool& operator=(ConnectionPool&&) = delete;

    /**
     * @brief Initialize the connection pool
     */
    Result<void> initialize();

    /**
     * @brief Shutdown the connection pool
     */
    void shutdown();

    /**
     * @brief Acquire a connection from the pool
     */
    Result<std::unique_ptr<PooledConnection>> acquire(
        std::chrono::milliseconds timeout = std::chrono::milliseconds(30000)); // 30 seconds default

    /**
     * @brief Execute a function with a connection
     */
    template <typename Func>
    auto withConnection(Func&& func) -> std::invoke_result_t<Func, Database&> {
        auto connResult = acquire();
        if (!connResult) {
            return Error{ErrorCode::ResourceExhausted, "Failed to acquire database connection"};
        }

        auto conn = std::move(connResult).value();
        conn->touch();

        try {
            return func(**conn);
        } catch (const std::exception& e) {
            return Error{ErrorCode::DatabaseError, e.what()};
        }
    }

    /**
     * @brief Get current pool statistics
     */
    struct Stats {
        size_t totalConnections;
        size_t availableConnections;
        size_t activeConnections;
        size_t waitingRequests;
        size_t maxObservedWaiting;
        std::uint64_t totalWaitMicros;
        size_t timeoutCount;
        size_t totalAcquired;
        size_t totalReleased;
        size_t failedAcquisitions;
    };

    [[nodiscard]] Stats getStats() const;

    /**
     * @brief Health check for all connections
     */
    Result<void> healthCheck();

    /**
     * @brief Prune idle connections
     */
    void pruneIdleConnections();

    /**
     * @brief Refresh next connection (discard stale connection from pool)
     */
    void refreshNext();

    /**
     * @brief Refresh all available connections (discard all idle connections)
     */
    void refreshAll();

private:
    std::string dbPath_;
    ConnectionPoolConfig config_;

    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::queue<std::unique_ptr<PooledConnection>> available_;
    std::atomic<size_t> totalConnections_{0};
    std::atomic<size_t> activeConnections_{0};
    std::atomic<size_t> waitingRequests_{0};
    std::atomic<size_t> maxWaitingRequests_{0};
    std::atomic<std::uint64_t> totalWaitMicros_{0};
    std::atomic<size_t> timeoutCount_{0};
    std::atomic<size_t> totalAcquired_{0};
    std::atomic<size_t> totalReleased_{0};
    std::atomic<size_t> failedAcquisitions_{0};
    std::atomic<bool> shutdown_{false};
    std::atomic<uint64_t> currentGeneration_{0}; // PBI-079: Incremented on refreshAll()

#if defined(__cpp_lib_jthread) && __cpp_lib_jthread >= 201911L
    std::jthread maintenanceThread_;
#endif

    /**
     * @brief Create a new connection
     */
    Result<std::unique_ptr<Database>> createConnection();

    /**
     * @brief Configure a new connection
     */
    Result<void> configureConnection(Database& db);

    /**
     * @brief Return a connection to the pool
     */
    void returnConnection(PooledConnection* conn);

    /**
     * @brief Check if a connection is still valid
     */
    bool isConnectionValid(const Database& db) const;

#if defined(__cpp_lib_jthread) && __cpp_lib_jthread >= 201911L
    void startMaintenanceThread();
#endif
};

/**
 * @brief RAII helper for automatic connection management
 */
class ScopedConnection {
public:
    explicit ScopedConnection(ConnectionPool& pool) : pool_(pool) {
        auto result = pool_.acquire();
        if (result) {
            conn_ = std::move(result).value();
        }
    }

    ~ScopedConnection() = default;

    // Non-copyable, movable
    ScopedConnection(const ScopedConnection&) = delete;
    ScopedConnection& operator=(const ScopedConnection&) = delete;
    ScopedConnection(ScopedConnection&&) = default;
    ScopedConnection& operator=(ScopedConnection&&) = delete;

    /**
     * @brief Check if connection was acquired successfully
     */
    [[nodiscard]] bool isValid() const { return conn_ && conn_->isValid(); }

    /**
     * @brief Access the database connection
     */
    Database* operator->() { return conn_ ? &(**conn_) : nullptr; }

    const Database* operator->() const { return conn_ ? &(**conn_) : nullptr; }

    Database& operator*() { return **conn_; }

    const Database& operator*() const { return **conn_; }

private:
    ConnectionPool& pool_;
    std::unique_ptr<PooledConnection> conn_;
};

} // namespace yams::metadata
