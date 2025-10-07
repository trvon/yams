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
#if defined(__cpp_lib_jthread) && __cpp_lib_jthread >= 201911L
    startMaintenanceThread();
#endif
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
#if defined(__cpp_lib_jthread) && __cpp_lib_jthread >= 201911L
    if (maintenanceThread_.joinable()) {
        maintenanceThread_.request_stop();
    }
#endif
}

Result<std::unique_ptr<PooledConnection>>
ConnectionPool::acquire(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mutex_);

    if (shutdown_) {
        return Error{ErrorCode::InvalidState, "Pool is shut down"};
    }

    class WaitingRequestGuard {
    public:
        explicit WaitingRequestGuard(ConnectionPool& pool) : pool_(pool) {}
        ~WaitingRequestGuard() { finish(); }

        void activate() {
            if (active_)
                return;
            active_ = true;
            startedAt_ = std::chrono::steady_clock::now();
            const auto current = pool_.waitingRequests_.fetch_add(1, std::memory_order_relaxed) + 1;
            auto previous = pool_.maxWaitingRequests_.load(std::memory_order_relaxed);
            while (current > previous &&
                   !pool_.maxWaitingRequests_.compare_exchange_weak(
                       previous, current, std::memory_order_relaxed, std::memory_order_relaxed)) {
                // CAS loop updates 'previous' with the latest observed value.
            }
        }

        void markTimeout() { timeout_ = true; }

        void finish() {
            if (!active_)
                return;
            const auto ended = std::chrono::steady_clock::now();
            pool_.waitingRequests_.fetch_sub(1, std::memory_order_relaxed);
            const auto micros =
                std::chrono::duration_cast<std::chrono::microseconds>(ended - startedAt_).count();
            pool_.totalWaitMicros_.fetch_add(static_cast<std::uint64_t>(micros),
                                             std::memory_order_relaxed);
            if (timeout_) {
                pool_.timeoutCount_.fetch_add(1, std::memory_order_relaxed);
            }
            active_ = false;
            timeout_ = false;
        }

    private:
        ConnectionPool& pool_;
        bool active_{false};
        bool timeout_{false};
        std::chrono::steady_clock::time_point startedAt_{};
    } waitingGuard(*this);

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
                totalAcquired_++;

                return pooledConn;
            } else {
                failedAcquisitions_++;
                return connResult.error();
            }
        }

        // Wait for connection to become available
        waitingGuard.activate();
        if (timeout.count() >= 30000) { // >= 30 seconds, treat as indefinite wait
            cv_.wait(lock, [this] { return !available_.empty() || shutdown_; });
            waitingGuard.finish();
        } else {
            if (!cv_.wait_until(lock, deadline,
                                [this] { return !available_.empty() || shutdown_; })) {
                waitingGuard.markTimeout();
                waitingGuard.finish();
                failedAcquisitions_++;
                return Error{ErrorCode::Timeout, "Timeout acquiring connection"};
            }
            waitingGuard.finish();
        }

        if (shutdown_) {
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
            failedAcquisitions_++;
            totalConnections_--;
            return connResult.error();
        }

        conn = std::make_unique<PooledConnection>(
            std::move(connResult).value(), [this](PooledConnection* c) { returnConnection(c); });
    }

    conn->touch();
    activeConnections_++;
    totalAcquired_++;

    waitingGuard.finish();

    return conn;
}

ConnectionPool::Stats ConnectionPool::getStats() const {
    std::lock_guard<std::mutex> lock(mutex_);

    return {totalConnections_,
            available_.size(),
            activeConnections_,
            waitingRequests_,
            maxWaitingRequests_.load(std::memory_order_relaxed),
            totalWaitMicros_.load(std::memory_order_relaxed),
            timeoutCount_.load(std::memory_order_relaxed),
            totalAcquired_,
            totalReleased_,
            failedAcquisitions_};
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

    // Enable WAL mode if requested (disabled in tests to avoid tempfs WAL I/O issues)
    // If enabling WAL fails (e.g., on restricted filesystems), log and fall back to a
    // memory journal to improve concurrency in test environments.
#ifndef YAMS_TESTING
    if (config_.enableWAL) {
        auto walResult = db.enableWAL();
        if (!walResult) {
            spdlog::warn("WAL enable failed: {} — continuing without WAL",
                         walResult.error().message);
            // Best-effort fallbacks for CI/restricted environments
            db.execute("PRAGMA journal_mode=MEMORY");
            db.execute("PRAGMA locking_mode=NORMAL");
        }
    }
#endif

    // Enable foreign keys if requested
    if (config_.enableForeignKeys) {
        auto fkResult = db.execute("PRAGMA foreign_keys = ON");
        if (!fkResult) {
            return fkResult.error();
        }
    }

    // Additional pragmas for performance (more relaxed when running tests)
    if (std::getenv("YAMS_TEST_TMPDIR")) {
        db.execute("PRAGMA synchronous = OFF");
        db.execute("PRAGMA journal_mode=MEMORY");
    } else {
        db.execute("PRAGMA synchronous = NORMAL");
    }
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

#if defined(__cpp_lib_jthread) && __cpp_lib_jthread >= 201911L
void ConnectionPool::startMaintenanceThread() {
    using namespace std::chrono_literals;
    maintenanceThread_ = std::jthread([this](std::stop_token st) {
        while (!st.stop_requested()) {
            (void)this->healthCheck();
            this->pruneIdleConnections();
            for (int i = 0; i < 60 && !st.stop_requested(); ++i) {
                std::this_thread::sleep_for(1s);
            }
        }
    });
}
#endif

} // namespace yams::metadata
