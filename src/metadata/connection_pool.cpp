#include <spdlog/spdlog.h>
#include <thread>
#include <yams/metadata/connection_pool.h>

namespace yams::metadata {

// PooledConnection implementation
PooledConnection::PooledConnection(std::unique_ptr<Database> db,
                                   std::function<void(PooledConnection*)> returnFunc)
    : db_(std::move(db)), returnFunc_(returnFunc), lastAccessed_(std::chrono::steady_clock::now()) {
}

PooledConnection::~PooledConnection() {
    if (db_ && returnFunc_ && !returned_) {
        returnFunc_(this);
    }
}

PooledConnection::PooledConnection(PooledConnection&& other) noexcept
    : db_(std::move(other.db_)), returnFunc_(std::move(other.returnFunc_)),
      lastAccessed_(other.lastAccessed_), returned_(other.returned_) {
    other.returned_ = true; // Prevent double return
}

PooledConnection& PooledConnection::operator=(PooledConnection&& other) noexcept {
    if (this != &other) {
        if (db_ && returnFunc_ && !returned_) {
            returnFunc_(this);
        }
        db_ = std::move(other.db_);
        returnFunc_ = std::move(other.returnFunc_);
        lastAccessed_ = other.lastAccessed_;
        returned_ = other.returned_;
        other.returned_ = true;
    }
    return *this;
}

// ConnectionPool implementation
ConnectionPool::ConnectionPool(const std::string& dbPath, const ConnectionPoolConfig& config)
    : dbPath_(dbPath), config_(config) {}

ConnectionPool::~ConnectionPool() {
    shutdown();
}

Result<void> ConnectionPool::initialize() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (shutdown_) {
        return Error{ErrorCode::InvalidState, "Pool is shut down"};
    }

    // Create minimum connections
    for (size_t i = 0; i < config_.minConnections; ++i) {
        auto connResult = createConnection();
        if (!connResult) {
            // Clean up any created connections
            while (!available_.empty()) {
                available_.pop();
            }
            totalConnections_ = 0;
            return connResult.error();
        }

        auto pooledConn = std::make_unique<PooledConnection>(
            std::move(connResult).value(),
            [this](PooledConnection* conn) { returnConnection(conn); });

        available_.push(std::move(pooledConn));
        totalConnections_++;
    }

    spdlog::debug("Connection pool initialized with {} connections", config_.minConnections);
    return {};
}

void ConnectionPool::shutdown() {
    std::unique_lock<std::mutex> lock(mutex_);

    if (shutdown_) {
        return; // Already shut down
    }

    shutdown_ = true;
    cv_.notify_all();

    // Clear all available connections
    while (!available_.empty()) {
        auto conn = std::move(available_.front());
        available_.pop();
        // Mark connection as returned to prevent callback
        conn->returned_ = true;
    }

    // Reset counters immediately - don't wait for active connections
    // They will handle shutdown state in their destructors
    totalConnections_ = 0;
    activeConnections_ = 0;

    spdlog::debug("Connection pool shut down");
}

Result<std::unique_ptr<PooledConnection>>
ConnectionPool::acquire(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mutex_);

    if (shutdown_) {
        return Error{ErrorCode::InvalidState, "Pool is shut down"};
    }

    waitingRequests_++;

    // Wait for available connection
    auto deadline = std::chrono::steady_clock::now() + timeout;

    while (available_.empty()) {
        // Can we create a new connection?
        if (totalConnections_ < config_.maxConnections) {
            lock.unlock();
            auto connResult = createConnection();
            lock.lock();

            if (connResult) {
                auto pooledConn = std::make_unique<PooledConnection>(
                    std::move(connResult).value(),
                    [this](PooledConnection* conn) { returnConnection(conn); });

                totalConnections_++;
                activeConnections_++;
                waitingRequests_--;
                totalAcquired_++;

                return pooledConn;
            } else {
                waitingRequests_--;
                failedAcquisitions_++;
                return connResult.error();
            }
        }

        // Wait for connection to become available
        if (timeout == std::chrono::milliseconds::max()) {
            cv_.wait(lock, [this] { return !available_.empty() || shutdown_; });
        } else {
            if (!cv_.wait_until(lock, deadline,
                                [this] { return !available_.empty() || shutdown_; })) {
                waitingRequests_--;
                failedAcquisitions_++;
                return Error{ErrorCode::Timeout, "Timeout acquiring connection"};
            }
        }

        if (shutdown_) {
            waitingRequests_--;
            failedAcquisitions_++;
            return Error{ErrorCode::InvalidState, "Pool is shut down"};
        }
    }

    // Get connection from pool
    auto conn = std::move(available_.front());
    available_.pop();

    // Validate connection
    if (!isConnectionValid(**conn)) {
        // Try to create a new one
        lock.unlock();
        auto connResult = createConnection();
        lock.lock();

        if (!connResult) {
            waitingRequests_--;
            failedAcquisitions_++;
            totalConnections_--;
            return connResult.error();
        }

        conn = std::make_unique<PooledConnection>(
            std::move(connResult).value(), [this](PooledConnection* c) { returnConnection(c); });
    }

    conn->touch();
    activeConnections_++;
    waitingRequests_--;
    totalAcquired_++;

    return conn;
}

ConnectionPool::Stats ConnectionPool::getStats() const {
    std::lock_guard<std::mutex> lock(mutex_);

    return {totalConnections_, available_.size(), activeConnections_, waitingRequests_,
            totalAcquired_,    totalReleased_,    failedAcquisitions_};
}

Result<void> ConnectionPool::healthCheck() {
    std::lock_guard<std::mutex> lock(mutex_);

    std::queue<std::unique_ptr<PooledConnection>> valid;

    while (!available_.empty()) {
        auto conn = std::move(available_.front());
        available_.pop();

        if (isConnectionValid(**conn)) {
            valid.push(std::move(conn));
        } else {
            totalConnections_--;
            spdlog::warn("Removed invalid connection from pool");
        }
    }

    available_ = std::move(valid);

    // Ensure minimum connections
    while (available_.size() + activeConnections_ < config_.minConnections &&
           totalConnections_ < config_.maxConnections) {
        auto connResult = createConnection();
        if (!connResult) {
            return connResult.error();
        }

        auto pooledConn = std::make_unique<PooledConnection>(
            std::move(connResult).value(),
            [this](PooledConnection* conn) { returnConnection(conn); });

        available_.push(std::move(pooledConn));
        totalConnections_++;
    }

    return {};
}

void ConnectionPool::pruneIdleConnections() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (available_.size() <= config_.minConnections) {
        return;
    }

    std::queue<std::unique_ptr<PooledConnection>> keep;
    auto now = std::chrono::steady_clock::now();

    while (!available_.empty() && keep.size() + activeConnections_ < config_.minConnections) {
        auto conn = std::move(available_.front());
        available_.pop();

        auto idleTime = now - conn->lastAccessed();
        if (idleTime < config_.idleTimeout) {
            keep.push(std::move(conn));
        } else {
            totalConnections_--;
            spdlog::debug("Pruned idle connection");
        }
    }

    // Move remaining connections to keep queue
    while (!available_.empty()) {
        keep.push(std::move(available_.front()));
        available_.pop();
    }

    available_ = std::move(keep);
}

Result<std::unique_ptr<Database>> ConnectionPool::createConnection() {
    auto db = std::make_unique<Database>();

    auto openResult = db->open(dbPath_, ConnectionMode::Create);
    if (!openResult) {
        return openResult.error();
    }

    auto configResult = configureConnection(*db);
    if (!configResult) {
        return configResult.error();
    }

    return db;
}

Result<void> ConnectionPool::configureConnection(Database& db) {
    // Set busy timeout
    auto timeoutResult = db.setBusyTimeout(config_.busyTimeout);
    if (!timeoutResult) {
        return timeoutResult.error();
    }

    // Enable WAL mode if requested
    if (config_.enableWAL) {
        auto walResult = db.enableWAL();
        if (!walResult) {
            return walResult.error();
        }
    }

    // Enable foreign keys if requested
    if (config_.enableForeignKeys) {
        auto fkResult = db.execute("PRAGMA foreign_keys = ON");
        if (!fkResult) {
            return fkResult.error();
        }
    }

    // Additional pragmas for performance
    db.execute("PRAGMA synchronous = NORMAL");
    db.execute("PRAGMA temp_store = MEMORY");
    db.execute("PRAGMA mmap_size = 268435456"); // 256MB

    return {};
}

void ConnectionPool::returnConnection(PooledConnection* conn) {
    if (!conn || !conn->db_)
        return;

    std::lock_guard<std::mutex> lock(mutex_);

    // If we're shut down, just discard the connection
    if (shutdown_) {
        // Don't modify counters during shutdown
        spdlog::debug("Discarding connection during shutdown");
        return;
    }

    // Check if connection is still valid
    if (!isConnectionValid(**conn)) {
        activeConnections_--;
        totalConnections_--;
        spdlog::warn("Returned connection is invalid, discarding");
        return;
    }

    // Reset any pending transaction
    conn->operator*().rollback(); // Ignore error

    // Move the database out of the PooledConnection
    auto db = std::move(conn->db_);

    // Create new PooledConnection with the database
    auto newConn = std::make_unique<PooledConnection>(
        std::move(db), [this](PooledConnection* c) { returnConnection(c); });

    newConn->touch();
    available_.push(std::move(newConn));
    activeConnections_--;
    totalReleased_++;

    cv_.notify_one();
}

bool ConnectionPool::isConnectionValid(const Database& db) const {
    if (!db.isOpen()) {
        return false;
    }

    // Try a simple query
    try {
        auto stmtResult = const_cast<Database&>(db).prepare("SELECT 1");
        if (!stmtResult)
            return false;

        Statement stmt = std::move(stmtResult).value();
        auto result = stmt.step();
        return result.has_value();
    } catch (...) {
        return false;
    }
}

} // namespace yams::metadata