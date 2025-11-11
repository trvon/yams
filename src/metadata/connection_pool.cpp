#include <spdlog/spdlog.h>
#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <string>
#include <thread>
#include <yams/metadata/connection_pool.h>

namespace yams::metadata {

// PooledConnection implementation
PooledConnection::PooledConnection(std::unique_ptr<Database> db,
                                   std::function<void(PooledConnection*)> returnFunc,
                                   std::uint64_t generation)
    : db_(std::move(db)), returnFunc_(returnFunc), lastAccessed_(std::chrono::steady_clock::now()),
      generation_(generation) {}

PooledConnection::~PooledConnection() {
    if (db_ && returnFunc_ && !returned_) {
        returnFunc_(this);
    }
}

PooledConnection::PooledConnection(PooledConnection&& other) noexcept
    : db_(std::move(other.db_)), returnFunc_(std::move(other.returnFunc_)),
      lastAccessed_(other.lastAccessed_), returned_(other.returned_),
      generation_(other.generation_) {
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
            [this](PooledConnection* conn) { returnConnection(conn); }, currentGeneration_.load());

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
                    [this](PooledConnection* conn) { returnConnection(conn); },
                    currentGeneration_.load());

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

    // Get connection from pool and validate it
    std::unique_ptr<PooledConnection> conn;
    bool foundValid = false;

    // PBI-079: Get current generation for staleness check
    const uint64_t currentGen = currentGeneration_.load();

    // Try up to 3 connections from the pool before creating a new one
    for (int attempt = 0; attempt < 3 && !available_.empty() && !foundValid; ++attempt) {
        conn = std::move(available_.front());
        available_.pop();

        // PBI-079: Check if connection is from an old generation (stale)
        if (conn->generation_ < currentGen) {
            totalConnections_--;
            spdlog::debug("[PBI-079] Discarded stale connection (gen {}, current {})",
                          conn->generation_, currentGen);
            conn.reset();
            continue;
        }

        if (isConnectionValid(**conn)) {
            foundValid = true;
            break;
        } else {
            // Connection is stale, discard it
            totalConnections_--;
            spdlog::warn("Discarded stale connection on acquire (attempt {})", attempt + 1);
            conn.reset();
        }
    }

    // If no valid connection found, create a new one
    if (!foundValid) {
        lock.unlock();
        auto connResult = createConnection();
        lock.lock();

        if (!connResult) {
            failedAcquisitions_++;
            return connResult.error();
        }

        conn = std::make_unique<PooledConnection>(
            std::move(connResult).value(), [this](PooledConnection* c) { returnConnection(c); },
            currentGeneration_.load());
        totalConnections_++;
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
            [this](PooledConnection* conn) { returnConnection(conn); }, currentGeneration_.load());

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
    size_t prunedIdle = 0;
    size_t prunedInvalid = 0;
    size_t prunedAge = 0;

    while (!available_.empty()) {
        auto conn = std::move(available_.front());
        available_.pop();

        auto age = now - conn->createdAt();
        auto idleTime = now - conn->lastAccessed();

        // Always keep minimum connections, but validate them and check age
        if (keep.size() + activeConnections_ < config_.minConnections) {
            // Even for minimum connections, prune if too old
            if (age >= config_.maxConnectionAge) {
                totalConnections_--;
                prunedAge++;
                spdlog::debug("Pruned aged connection (age: {}s)",
                              std::chrono::duration_cast<std::chrono::seconds>(age).count());
            } else if (isConnectionValid(**conn)) {
                keep.push(std::move(conn));
            } else {
                totalConnections_--;
                prunedInvalid++;
                spdlog::debug("Pruned invalid connection during maintenance");
            }
            continue;
        }

        // For connections above minimum, check idle time and age
        if (age >= config_.maxConnectionAge) {
            totalConnections_--;
            prunedAge++;
            spdlog::debug("Pruned aged connection (age: {}s)",
                          std::chrono::duration_cast<std::chrono::seconds>(age).count());
        } else if (idleTime >= config_.idleTimeout) {
            totalConnections_--;
            prunedIdle++;
            spdlog::debug("Pruned idle connection (idle: {}s)",
                          std::chrono::duration_cast<std::chrono::seconds>(idleTime).count());
        } else {
            // Validate before keeping
            if (isConnectionValid(**conn)) {
                keep.push(std::move(conn));
            } else {
                totalConnections_--;
                prunedInvalid++;
                spdlog::debug("Pruned invalid connection during maintenance");
            }
        }
    }

    available_ = std::move(keep);

    if (prunedIdle > 0 || prunedInvalid > 0 || prunedAge > 0) {
        spdlog::info("Connection pool pruned {} idle, {} invalid, {} aged connections", prunedIdle,
                     prunedInvalid, prunedAge);
    }
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

    // Enable WAL mode if requested (disabled in select environments)
    // If enabling WAL fails (e.g., on restricted filesystems), log and fall back to a
    // memory journal to improve concurrency in test environments.
    const auto envTruthy = [](const char* value) {
        if (!value)
            return false;
        std::string v(value);
        std::transform(v.begin(), v.end(), v.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        return !(v.empty() || v == "0" || v == "false" || v == "off" || v == "no");
    };

    const bool walDisabledViaEnv = [&]() {
        const char* keys[] = {"YAMS_DISABLE_WAL",     "YAMS_TEST_DISABLE_WAL",
                              "YAMS_TESTING",         "YAMS_TEST_SAFE_SINGLE_INSTANCE",
                              "YAMS_DISABLE_VECTORS", "YAMS_TEST_FAST_START"};
        for (const char* key : keys) {
            if (envTruthy(std::getenv(key))) {
                spdlog::debug("WAL override active via {}", key);
                return true;
            }
        }
        return false;
    }();

    if (config_.enableWAL && !walDisabledViaEnv) {
        auto walResult = db.enableWAL();
        if (!walResult) {
            spdlog::warn("WAL enable failed: {} — continuing without WAL",
                         walResult.error().message);
            // Best-effort fallbacks for CI/restricted environments
            db.execute("PRAGMA journal_mode=MEMORY");
            db.execute("PRAGMA locking_mode=NORMAL");
        }
    } else if (config_.enableWAL) {
        spdlog::debug("Skipping WAL enablement due to environment override");
        db.execute("PRAGMA journal_mode=MEMORY");
        db.execute("PRAGMA locking_mode=NORMAL");
    }

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

    // Reset any pending transaction before validation
    try {
        conn->operator*().rollback(); // Ignore error
    } catch (...) {
        // If rollback fails, connection is likely stale
        activeConnections_--;
        totalConnections_--;
        spdlog::warn("Failed to rollback transaction, discarding connection");
        return;
    }

    // Check if connection is still valid
    if (!isConnectionValid(**conn)) {
        activeConnections_--;
        totalConnections_--;
        spdlog::warn("Returned connection is invalid, discarding");
        return;
    }

    // Move the database out of the PooledConnection
    auto db = std::move(conn->db_);

    // Create new PooledConnection with the database (preserve generation)
    auto newConn = std::make_unique<PooledConnection>(
        std::move(db), [this](PooledConnection* c) { returnConnection(c); }, conn->generation_);

    newConn->touch();
    available_.push(std::move(newConn));
    activeConnections_--;
    totalReleased_++;

    cv_.notify_one();
}

void ConnectionPool::refreshNext() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!available_.empty()) {
        available_.pop();
        totalConnections_--;
    }
}

void ConnectionPool::refreshAll() {
    std::lock_guard<std::mutex> lock(mutex_);

    // PBI-079: Increment generation to mark all existing connections as stale
    uint64_t oldGen = currentGeneration_.load();
    currentGeneration_.fetch_add(1);
    uint64_t newGen = currentGeneration_.load();

    spdlog::debug("[PBI-079] refreshAll: generation {} -> {}, discarding {} idle connections",
                  oldGen, newGen, available_.size());

    size_t discarded = 0;
    while (!available_.empty()) {
        auto conn = std::move(available_.front());
        available_.pop();

        // Mark as returned to prevent destructor from calling returnConnection()
        // which would deadlock (we already hold the mutex)
        conn->returned_ = true;

        totalConnections_--;
        discarded++;
    }
}

bool ConnectionPool::isConnectionValid(const Database& db) const {
    if (!db.isOpen()) {
        return false;
    }

    // Try a simple query with write operation to detect stale connections
    try {
        // First verify we can read
        auto stmtResult = const_cast<Database&>(db).prepare("SELECT 1");
        if (!stmtResult)
            return false;

        Statement stmt = std::move(stmtResult).value();
        auto result = stmt.step();
        if (!result.has_value())
            return false;

        // Also verify we can write (detects locked/stale connections)
        // Use a temp table to avoid affecting the schema
        auto writeTest = const_cast<Database&>(db).execute(
            "CREATE TEMP TABLE IF NOT EXISTS __pool_health_check (id INTEGER)");
        if (!writeTest)
            return false;

        return true;
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
