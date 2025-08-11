#include <yams/storage/reference_counter.h>
#include <yams/storage/storage_engine.h>

#include "reference_db.cpp"  // Include DB wrappers

#include <spdlog/spdlog.h>
#include <filesystem>
#include <fstream>
#include <thread>
#include <chrono>

namespace yams::storage {

// Implementation details
struct ReferenceCounter::Impl {
    Config config;
    std::unique_ptr<Database> db;
    std::unique_ptr<StatementCache> stmtCache;
    mutable std::shared_mutex dbMutex;
    std::atomic<int64_t> nextTransactionId{1};
    
    // Prepared statement keys
    static constexpr auto INCREMENT_STMT = "increment";
    static constexpr auto DECREMENT_STMT = "decrement";
    static constexpr auto GET_REF_COUNT_STMT = "get_ref_count";
    static constexpr auto GET_UNREFERENCED_STMT = "get_unreferenced";
    static constexpr auto UPDATE_STATS_STMT = "update_stats";
    static constexpr auto INSERT_TRANSACTION_STMT = "insert_transaction";
    static constexpr auto UPDATE_TRANSACTION_STMT = "update_transaction";
    static constexpr auto INSERT_OP_STMT = "insert_op";
    
    explicit Impl(Config cfg) : config(std::move(cfg)) {}
};

// Constructor
ReferenceCounter::ReferenceCounter(Config config) 
    : pImpl(std::make_unique<Impl>(std::move(config))) {
    auto result = initializeDatabase();
    if (!result) {
        throw std::runtime_error("Failed to initialize reference counter database");
    }
}

// Destructor
ReferenceCounter::~ReferenceCounter() = default;

// Move constructor
ReferenceCounter::ReferenceCounter(ReferenceCounter&&) noexcept = default;

// Move assignment
ReferenceCounter& ReferenceCounter::operator=(ReferenceCounter&&) noexcept = default;

// Initialize database
Result<void> ReferenceCounter::initializeDatabase() {
    try {
        // Create database directory if needed
        std::filesystem::create_directories(pImpl->config.databasePath.parent_path());
        
        // Open database
        pImpl->db = std::make_unique<Database>(pImpl->config.databasePath);
        
        // Configure database
        if (pImpl->config.enableWAL) {
            pImpl->db->execute("PRAGMA journal_mode = WAL");
        }
        
        pImpl->db->execute(std::format("PRAGMA cache_size = {}", pImpl->config.cacheSize));
        pImpl->db->execute(std::format("PRAGMA busy_timeout = {}", pImpl->config.busyTimeout));
        pImpl->db->execute("PRAGMA synchronous = NORMAL");
        pImpl->db->execute("PRAGMA temp_store = MEMORY");
        
        // Execute schema
        auto schemaPath = std::filesystem::path(__FILE__).parent_path().parent_path().parent_path() 
                          / "sql" / "reference_schema.sql";
        if (std::filesystem::exists(schemaPath)) {
            pImpl->db->executeFile(schemaPath);
        } else {
            // Fallback: execute inline schema
            pImpl->db->execute(R"(
                CREATE TABLE IF NOT EXISTS block_references (
                    block_hash TEXT PRIMARY KEY,
                    ref_count INTEGER NOT NULL DEFAULT 0,
                    block_size INTEGER NOT NULL,
                    created_at INTEGER NOT NULL,
                    last_accessed INTEGER NOT NULL,
                    metadata TEXT,
                    CHECK (ref_count >= 0),
                    CHECK (block_size > 0)
                );
                
                CREATE INDEX IF NOT EXISTS idx_ref_count ON block_references(ref_count);
                CREATE INDEX IF NOT EXISTS idx_last_accessed ON block_references(last_accessed);
            )");
        }
        
        // Create statement cache
        pImpl->stmtCache = std::make_unique<StatementCache>(*pImpl->db);
        
        // Prepare common statements
        pImpl->stmtCache->get(Impl::INCREMENT_STMT, R"(
            INSERT INTO block_references (block_hash, ref_count, block_size, created_at, last_accessed)
            VALUES (?, 1, ?, strftime('%s', 'now'), strftime('%s', 'now'))
            ON CONFLICT(block_hash) DO UPDATE SET
                ref_count = ref_count + 1,
                last_accessed = strftime('%s', 'now')
        )");
        
        pImpl->stmtCache->get(Impl::DECREMENT_STMT, R"(
            UPDATE block_references 
            SET ref_count = ref_count - 1,
                last_accessed = strftime('%s', 'now')
            WHERE block_hash = ? AND ref_count > 0
        )");
        
        pImpl->stmtCache->get(Impl::GET_REF_COUNT_STMT, 
            "SELECT ref_count FROM block_references WHERE block_hash = ?");
        
        pImpl->stmtCache->get(Impl::GET_UNREFERENCED_STMT, R"(
            SELECT block_hash, block_size FROM block_references 
            WHERE ref_count = 0 
            AND (strftime('%s', 'now') - last_accessed) >= ?
            ORDER BY last_accessed ASC
            LIMIT ?
        )");
        
        // Initialize nextTransactionId to be higher than any existing transaction
        auto maxIdStmt = pImpl->db->prepare("SELECT COALESCE(MAX(transaction_id), 0) FROM ref_transactions");
        if (maxIdStmt.step()) {
            int64_t maxId = maxIdStmt.getInt64(0);
            pImpl->nextTransactionId = maxId + 1;
            spdlog::debug("Initialized nextTransactionId to {}", pImpl->nextTransactionId.load());
        }
        
        spdlog::debug("Reference counter database initialized at {}", 
                      pImpl->config.databasePath.string());
        
        return {};
    } catch (const std::exception& e) {
        spdlog::error("Failed to initialize reference counter database: {}", e.what());
        return Result<void>(ErrorCode::DatabaseError);
    }
}

// Single increment operation
Result<void> ReferenceCounter::increment(std::string_view blockHash, size_t blockSize) {
    try {
        std::unique_lock lock(pImpl->dbMutex);
        
        auto& stmt = pImpl->stmtCache->get(Impl::INCREMENT_STMT, "");
        stmt.bind(1, blockHash);
        stmt.bind(2, static_cast<int64_t>(blockSize));
        stmt.execute();
        
        return {};
    } catch (const std::exception& e) {
        spdlog::error("Failed to increment reference count for {}: {}", blockHash, e.what());
        return Result<void>(ErrorCode::DatabaseError);
    }
}

// Single decrement operation
Result<void> ReferenceCounter::decrement(std::string_view blockHash) {
    try {
        std::unique_lock lock(pImpl->dbMutex);
        
        auto& stmt = pImpl->stmtCache->get(Impl::DECREMENT_STMT, "");
        stmt.bind(1, blockHash);
        stmt.execute();
        
        if (pImpl->db->changes() == 0) {
            spdlog::warn("Attempted to decrement non-existent or zero reference count for {}", 
                         blockHash);
        }
        
        return {};
    } catch (const std::exception& e) {
        spdlog::error("Failed to decrement reference count for {}: {}", blockHash, e.what());
        return Result<void>(ErrorCode::DatabaseError);
    }
}

// Get reference count
Result<uint64_t> ReferenceCounter::getRefCount(std::string_view blockHash) const {
    try {
        std::shared_lock lock(pImpl->dbMutex);
        
        auto& stmt = pImpl->stmtCache->get(Impl::GET_REF_COUNT_STMT, "");
        stmt.bind(1, blockHash);
        
        if (stmt.step()) {
            return static_cast<uint64_t>(stmt.getInt64(0));
        }
        
        return 0;  // Block not found
    } catch (const std::exception& e) {
        spdlog::error("Failed to get reference count for {}: {}", blockHash, e.what());
        return Result<uint64_t>(ErrorCode::DatabaseError);
    }
}

// Check if block has references
Result<bool> ReferenceCounter::hasReferences(std::string_view blockHash) const {
    auto result = getRefCount(blockHash);
    if (!result) {
        return Result<bool>(result.error());
    }
    return result.value() > 0;
}

// Get statistics
Result<RefCountStats> ReferenceCounter::getStats() const {
    try {
        std::shared_lock lock(pImpl->dbMutex);
        
        RefCountStats stats{};
        
        // Use the block_statistics view
        auto stmt = pImpl->db->prepare(R"(
            SELECT 
                total_blocks,
                total_references,
                total_bytes,
                unreferenced_blocks,
                unreferenced_bytes
            FROM block_statistics
        )");
        
        if (stmt.step()) {
            stats.totalBlocks = static_cast<uint64_t>(stmt.getInt64(0));
            stats.totalReferences = static_cast<uint64_t>(stmt.getInt64(1));
            stats.totalBytes = static_cast<uint64_t>(stmt.getInt64(2));
            stats.unreferencedBlocks = static_cast<uint64_t>(stmt.getInt64(3));
            stats.unreferencedBytes = static_cast<uint64_t>(stmt.getInt64(4));
        }
        
        // Get transaction stats
        auto txnStmt = pImpl->db->prepare(R"(
            SELECT 
                (SELECT stat_value FROM ref_statistics WHERE stat_name = 'transactions_completed'),
                (SELECT stat_value FROM ref_statistics WHERE stat_name = 'transactions_rolled_back')
        )");
        
        if (txnStmt.step()) {
            stats.transactions = static_cast<uint64_t>(txnStmt.getInt64(0));
            stats.rollbacks = static_cast<uint64_t>(txnStmt.getInt64(1));
        }
        
        return stats;
    } catch (const std::exception& e) {
        spdlog::error("Failed to get statistics: {}", e.what());
        return Result<RefCountStats>(ErrorCode::DatabaseError);
    }
}

// Get unreferenced blocks
Result<std::vector<std::string>> ReferenceCounter::getUnreferencedBlocks(
    size_t limit, std::chrono::seconds minAge) const {
    try {
        std::shared_lock lock(pImpl->dbMutex);
        
        std::vector<std::string> blocks;
        blocks.reserve(limit);
        
        auto& stmt = pImpl->stmtCache->get(Impl::GET_UNREFERENCED_STMT, "");
        stmt.bind(1, static_cast<int64_t>(minAge.count()));
        stmt.bind(2, static_cast<int64_t>(limit));
        
        while (stmt.step()) {
            blocks.emplace_back(stmt.getString(0));
        }
        
        return blocks;
    } catch (const std::exception& e) {
        spdlog::error("Failed to get unreferenced blocks: {}", e.what());
        return Result<std::vector<std::string>>(ErrorCode::DatabaseError);
    }
}

// Begin transaction
std::unique_ptr<IReferenceCounter::ITransaction> ReferenceCounter::beginTransaction() {
    int64_t txnId = pImpl->nextTransactionId.fetch_add(1);
    return std::unique_ptr<ITransaction>(new Transaction(this, txnId));
}

// Transaction implementation
ReferenceCounter::Transaction::Transaction(ReferenceCounter* counter, int64_t id)
    : counter_(counter), transactionId_(id), active_(true), committed_(false) {
    
    try {
        // Start database transaction
        counter_->pImpl->db->beginTransaction();
        
        // Record transaction start
        auto stmt = counter_->pImpl->db->prepare(R"(
            INSERT INTO ref_transactions (transaction_id, start_timestamp, state)
            VALUES (?, strftime('%s', 'now'), 'PENDING')
        )");
        stmt.bind(1, transactionId_);
        stmt.execute();
        
        spdlog::debug("Started reference counting transaction {}", transactionId_);
    } catch (const std::exception& e) {
        active_ = false;
        throw std::runtime_error(std::format("Failed to start transaction: {}", e.what()));
    }
}

ReferenceCounter::Transaction::~Transaction() {
    if (active_ && !committed_) {
        try {
            rollback();
        } catch (...) {
            // Suppress exceptions in destructor
        }
    }
}

// Move constructor
ReferenceCounter::Transaction::Transaction(Transaction&& other) noexcept
    : counter_(other.counter_), 
      transactionId_(other.transactionId_),
      active_(other.active_),
      committed_(other.committed_),
      operations_(std::move(other.operations_)) {
    other.active_ = false;
    other.counter_ = nullptr;
}

// Move assignment
ReferenceCounter::Transaction& ReferenceCounter::Transaction::operator=(Transaction&& other) noexcept {
    if (this != &other) {
        if (active_ && !committed_) {
            try {
                rollback();
            } catch (...) {}
        }
        
        counter_ = other.counter_;
        transactionId_ = other.transactionId_;
        active_ = other.active_;
        committed_ = other.committed_;
        operations_ = std::move(other.operations_);
        
        other.active_ = false;
        other.counter_ = nullptr;
    }
    return *this;
}

// Increment in transaction
void ReferenceCounter::Transaction::increment(std::string_view blockHash, size_t blockSize) {
    if (!active_) {
        throw std::runtime_error("Transaction is not active");
    }
    
    operations_.push_back({
        .type = Operation::Type::Increment,
        .blockHash = std::string(blockHash),
        .blockSize = blockSize,
        .delta = 1
    });
    
    // Record operation
    try {
        auto stmt = counter_->pImpl->db->prepare(R"(
            INSERT INTO ref_transaction_ops 
            (transaction_id, block_hash, operation, delta, block_size, timestamp)
            VALUES (?, ?, 'INCREMENT', 1, ?, strftime('%s', 'now'))
        )");
        stmt.bind(1, transactionId_);
        stmt.bind(2, blockHash);
        stmt.bind(3, static_cast<int64_t>(blockSize));
        stmt.execute();
    } catch (const std::exception& e) {
        throw std::runtime_error(std::format("Failed to record increment operation: {}", e.what()));
    }
}

// Decrement in transaction
void ReferenceCounter::Transaction::decrement(std::string_view blockHash) {
    if (!active_) {
        throw std::runtime_error("Transaction is not active");
    }
    
    operations_.push_back({
        .type = Operation::Type::Decrement,
        .blockHash = std::string(blockHash),
        .blockSize = 0,
        .delta = -1
    });
    
    // Record operation
    try {
        auto stmt = counter_->pImpl->db->prepare(R"(
            INSERT INTO ref_transaction_ops 
            (transaction_id, block_hash, operation, delta, timestamp)
            VALUES (?, ?, 'DECREMENT', 1, strftime('%s', 'now'))
        )");
        stmt.bind(1, transactionId_);
        stmt.bind(2, blockHash);
        stmt.execute();
    } catch (const std::exception& e) {
        throw std::runtime_error(std::format("Failed to record decrement operation: {}", e.what()));
    }
}

// Commit transaction
Result<void> ReferenceCounter::Transaction::commit() {
    if (!active_) {
        return Result<void>(ErrorCode::TransactionFailed);
    }
    
    try {
        std::unique_lock lock(counter_->pImpl->dbMutex);
        
        // Apply all operations
        for (const auto& op : operations_) {
            if (op.type == Operation::Type::Increment) {
                auto& stmt = counter_->pImpl->stmtCache->get(ReferenceCounter::Impl::INCREMENT_STMT, "");
                stmt.bind(1, op.blockHash);
                stmt.bind(2, static_cast<int64_t>(op.blockSize));
                stmt.execute();
            } else {
                auto& stmt = counter_->pImpl->stmtCache->get(ReferenceCounter::Impl::DECREMENT_STMT, "");
                stmt.bind(1, op.blockHash);
                stmt.execute();
            }
        }
        
        // Update transaction state
        auto stmt = counter_->pImpl->db->prepare(R"(
            UPDATE ref_transactions 
            SET commit_timestamp = strftime('%s', 'now'), state = 'COMMITTED'
            WHERE transaction_id = ?
        )");
        stmt.bind(1, transactionId_);
        stmt.execute();
        
        // Update statistics
        counter_->updateStatistics("transactions_completed", 1);
        
        // Commit database transaction
        counter_->pImpl->db->commit();
        
        active_ = false;
        committed_ = true;
        
        spdlog::debug("Committed transaction {} with {} operations", 
                      transactionId_, operations_.size());
        
        return {};
    } catch (const std::exception& e) {
        spdlog::error("Failed to commit transaction {}: {}", transactionId_, e.what());
        rollback();
        return Result<void>(ErrorCode::TransactionFailed);
    }
}

// Rollback transaction
void ReferenceCounter::Transaction::rollback() {
    if (!active_) {
        return;
    }
    
    try {
        std::unique_lock lock(counter_->pImpl->dbMutex);
        
        // Update transaction state
        auto stmt = counter_->pImpl->db->prepare(R"(
            UPDATE ref_transactions 
            SET state = 'ROLLED_BACK'
            WHERE transaction_id = ?
        )");
        stmt.bind(1, transactionId_);
        stmt.execute();
        
        // Update statistics
        counter_->updateStatistics("transactions_rolled_back", 1);
        
        // Rollback database transaction
        counter_->pImpl->db->rollback();
        
        active_ = false;
        
        spdlog::debug("Rolled back transaction {}", transactionId_);
    } catch (const std::exception& e) {
        spdlog::error("Error during rollback of transaction {}: {}", transactionId_, e.what());
    }
}

// Update statistics
Result<void> ReferenceCounter::updateStatistics(const std::string& statName, int64_t delta) {
    try {
        auto stmt = pImpl->db->prepare(R"(
            UPDATE ref_statistics 
            SET stat_value = stat_value + ?,
                updated_at = strftime('%s', 'now')
            WHERE stat_name = ?
        )");
        stmt.bind(1, delta);
        stmt.bind(2, statName);
        stmt.execute();
        
        return {};
    } catch (const std::exception& e) {
        spdlog::error("Failed to update statistic {}: {}", statName, e.what());
        return Result<void>(ErrorCode::DatabaseError);
    }
}

// Maintenance operations
Result<void> ReferenceCounter::vacuum() {
    try {
        std::unique_lock lock(pImpl->dbMutex);
        pImpl->db->vacuum();
        spdlog::debug("Database vacuum completed");
        return {};
    } catch (const std::exception& e) {
        spdlog::error("Failed to vacuum database: {}", e.what());
        return Result<void>(ErrorCode::DatabaseError);
    }
}

Result<void> ReferenceCounter::checkpoint() {
    try {
        std::unique_lock lock(pImpl->dbMutex);
        pImpl->db->checkpoint();
        spdlog::debug("WAL checkpoint completed");
        return {};
    } catch (const std::exception& e) {
        spdlog::error("Failed to checkpoint database: {}", e.what());
        return Result<void>(ErrorCode::DatabaseError);
    }
}

Result<void> ReferenceCounter::analyze() {
    try {
        std::unique_lock lock(pImpl->dbMutex);
        pImpl->db->analyze();
        spdlog::debug("Database analysis completed");
        return {};
    } catch (const std::exception& e) {
        spdlog::error("Failed to analyze database: {}", e.what());
        return Result<void>(ErrorCode::DatabaseError);
    }
}

// Backup and restore
Result<void> ReferenceCounter::backup(const std::filesystem::path& destPath) {
    try {
        std::shared_lock lock(pImpl->dbMutex);
        pImpl->db->backup(destPath);
        spdlog::debug("Database backed up to {}", destPath.string());
        return {};
    } catch (const std::exception& e) {
        spdlog::error("Failed to backup database: {}", e.what());
        return Result<void>(ErrorCode::DatabaseError);
    }
}

Result<void> ReferenceCounter::restore(const std::filesystem::path& srcPath) {
    try {
        std::unique_lock lock(pImpl->dbMutex);
        
        // Close current database
        pImpl->stmtCache.reset();
        pImpl->db.reset();
        
        // Copy backup to database path
        std::filesystem::copy_file(srcPath, pImpl->config.databasePath,
                                  std::filesystem::copy_options::overwrite_existing);
        
        // Re-initialize
        return initializeDatabase();
    } catch (const std::exception& e) {
        spdlog::error("Failed to restore database: {}", e.what());
        return Result<void>(ErrorCode::DatabaseError);
    }
}

// Verify integrity
Result<bool> ReferenceCounter::verifyIntegrity() const {
    try {
        std::shared_lock lock(pImpl->dbMutex);
        return pImpl->db->checkIntegrity();
    } catch (const std::exception& e) {
        spdlog::error("Failed to verify database integrity: {}", e.what());
        return Result<bool>(ErrorCode::DatabaseError);
    }
}

// Factory function
std::unique_ptr<IReferenceCounter> createReferenceCounter(ReferenceCounter::Config config) {
    return std::make_unique<ReferenceCounter>(std::move(config));
}

} // namespace yams::storage