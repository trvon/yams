#include <yams/storage/reference_counter.h>
#include <yams/storage/storage_engine.h>

#include "reference_db.cpp" // Include DB wrappers

#include <spdlog/spdlog.h>
#if defined(YAMS_HAS_STD_FORMAT) && YAMS_HAS_STD_FORMAT
#include <format>
namespace yamsfmt = std;
#else
#include <spdlog/fmt/fmt.h>
namespace yamsfmt = fmt;
#endif
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <thread>

#ifdef __linux__
#include <unistd.h>
#include <linux/limits.h>
#elif defined(__APPLE__)
#include <mach-o/dyld.h>
#elif defined(_WIN32)
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#endif

namespace yams::storage {

// Implementation details
struct ReferenceCounter::Impl {
    Config config;
    std::unique_ptr<Database> db;
    std::unique_ptr<StatementCache> stmtCache;
    mutable std::shared_mutex dbMutex;
    // Transaction IDs now managed by database AUTOINCREMENT

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

// Helper function to find reference_schema.sql
static std::filesystem::path findReferenceSchemaSql() {
    namespace fs = std::filesystem;

    std::vector<fs::path> searchPaths;

    // 1. Check environment variable
    if (const char* dataDir = std::getenv("YAMS_DATA_DIR")) {
        searchPaths.push_back(fs::path(dataDir) / "reference_schema.sql");
        searchPaths.push_back(fs::path(dataDir) / "sql" / "reference_schema.sql");
    }

    // 2. Check relative to executable location (for installed binaries)
    try {
#ifdef __linux__
        char result[PATH_MAX];
        ssize_t count = readlink("/proc/self/exe", result, PATH_MAX);
        if (count != -1) {
            fs::path exePath(std::string(result, count));
            // Check ../share/yams/sql relative to binary
            searchPaths.push_back(exePath.parent_path().parent_path() / "share" / "yams" / "sql" /
                                  "reference_schema.sql");
        }
#elif defined(__APPLE__)
        char path[1024];
        uint32_t size = sizeof(path);
        if (_NSGetExecutablePath(path, &size) == 0) {
            fs::path exePath(path);
            // Check ../share/yams/sql relative to binary
            searchPaths.push_back(exePath.parent_path().parent_path() / "share" / "yams" / "sql" /
                                  "reference_schema.sql");
        }
#elif defined(_WIN32)
        wchar_t path[MAX_PATH];
        DWORD len = GetModuleFileNameW(nullptr, path, MAX_PATH);
        if (len > 0 && len < MAX_PATH) {
            fs::path exePath(path);
            // Check relative to binary directory (for development/build)
            searchPaths.push_back(exePath.parent_path() / "sql" / "reference_schema.sql");
            searchPaths.push_back(exePath.parent_path().parent_path() / "sql" /
                                  "reference_schema.sql");
            // Check ../share/yams/sql relative to binary (for installed binaries)
            searchPaths.push_back(exePath.parent_path().parent_path() / "share" / "yams" / "sql" /
                                  "reference_schema.sql");
        }
#endif
    } catch (...) {
        // Ignore errors in getting executable path
    }

    // 3. Check common installation paths
    searchPaths.push_back("/usr/local/share/yams/sql/reference_schema.sql");
    searchPaths.push_back("/usr/share/yams/sql/reference_schema.sql");
    searchPaths.push_back("/opt/yams/share/sql/reference_schema.sql");

    // 4. Check relative to current working directory (for development)
    searchPaths.push_back("sql/reference_schema.sql");
    searchPaths.push_back("../sql/reference_schema.sql");
    searchPaths.push_back("../../sql/reference_schema.sql");
    searchPaths.push_back("../../../sql/reference_schema.sql");

    // 5. Check in home directory
    if (const char* home = std::getenv("HOME")) {
        searchPaths.push_back(fs::path(home) / ".local" / "share" / "yams" / "sql" /
                              "reference_schema.sql");
    }

    // Search for the file
    for (const auto& path : searchPaths) {
        if (fs::exists(path) && fs::is_regular_file(path)) {
            spdlog::debug("Found reference_schema.sql at: {}", path.string());
            return path;
        }
    }

    spdlog::debug("reference_schema.sql not found in any search path");
    return {};
}

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

        pImpl->db->execute(yamsfmt::format("PRAGMA cache_size = {}", pImpl->config.cacheSize));
        pImpl->db->execute(yamsfmt::format("PRAGMA busy_timeout = {}", pImpl->config.busyTimeout));
        pImpl->db->execute("PRAGMA synchronous = NORMAL");
        pImpl->db->execute("PRAGMA temp_store = MEMORY");

        // Execute schema - use proper path discovery
        auto schemaPath = findReferenceSchemaSql();
        if (!schemaPath.empty() && std::filesystem::exists(schemaPath)) {
            spdlog::debug("Loading reference schema from: {}", schemaPath.string());
            pImpl->db->executeFile(schemaPath);
        } else {
            // Fallback: execute complete inline schema
            spdlog::warn("reference_schema.sql not found at {}, using inline schema",
                         schemaPath.string());
            pImpl->db->execute(R"(
                -- Enable foreign key constraints
                PRAGMA foreign_keys = ON;
                
                -- Main table for tracking block references
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
                
                -- Transaction log for crash recovery and atomicity
                CREATE TABLE IF NOT EXISTS ref_transactions (
                    transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    start_timestamp INTEGER NOT NULL,
                    commit_timestamp INTEGER,
                    state TEXT NOT NULL DEFAULT 'PENDING',
                    description TEXT,
                    CHECK (state IN ('PENDING', 'COMMITTED', 'ROLLED_BACK'))
                );
                
                -- Individual operations within a transaction
                CREATE TABLE IF NOT EXISTS ref_transaction_ops (
                    op_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    transaction_id INTEGER NOT NULL,
                    block_hash TEXT NOT NULL,
                    operation TEXT NOT NULL,
                    delta INTEGER NOT NULL DEFAULT 1,
                    block_size INTEGER,
                    timestamp INTEGER NOT NULL,
                    FOREIGN KEY (transaction_id) REFERENCES ref_transactions(transaction_id),
                    CHECK (operation IN ('INCREMENT', 'DECREMENT')),
                    CHECK (delta > 0)
                );
                
                -- Statistics table for monitoring
                CREATE TABLE IF NOT EXISTS ref_statistics (
                    stat_name TEXT PRIMARY KEY,
                    stat_value INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                );
                
                -- Initialize statistics
                INSERT OR IGNORE INTO ref_statistics (stat_name, stat_value, updated_at) VALUES
                    ('total_blocks', 0, strftime('%s', 'now')),
                    ('total_references', 0, strftime('%s', 'now')),
                    ('total_bytes', 0, strftime('%s', 'now')),
                    ('transactions_completed', 0, strftime('%s', 'now')),
                    ('transactions_rolled_back', 0, strftime('%s', 'now')),
                    ('gc_runs', 0, strftime('%s', 'now')),
                    ('gc_blocks_collected', 0, strftime('%s', 'now')),
                    ('gc_bytes_reclaimed', 0, strftime('%s', 'now'));
                
                -- Audit log for important operations
                CREATE TABLE IF NOT EXISTS ref_audit_log (
                    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp INTEGER NOT NULL,
                    operation TEXT NOT NULL,
                    block_hash TEXT,
                    old_value INTEGER,
                    new_value INTEGER,
                    transaction_id INTEGER,
                    details TEXT
                );
                
                -- Indexes for performance
                CREATE INDEX IF NOT EXISTS idx_ref_count ON block_references(ref_count);
                CREATE INDEX IF NOT EXISTS idx_last_accessed ON block_references(last_accessed);
                CREATE INDEX IF NOT EXISTS idx_block_size ON block_references(block_size);
                CREATE INDEX IF NOT EXISTS idx_transaction_ops ON ref_transaction_ops(transaction_id);
                CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON ref_audit_log(timestamp);
                CREATE INDEX IF NOT EXISTS idx_audit_block ON ref_audit_log(block_hash);
                
                -- View for unreferenced blocks (garbage collection candidates)
                CREATE VIEW IF NOT EXISTS unreferenced_blocks AS
                SELECT 
                    block_hash,
                    block_size,
                    created_at,
                    last_accessed,
                    (strftime('%s', 'now') - last_accessed) AS age_seconds
                FROM block_references
                WHERE ref_count = 0
                ORDER BY last_accessed ASC;
                
                -- View for block statistics
                CREATE VIEW IF NOT EXISTS block_statistics AS
                SELECT 
                    COUNT(*) AS total_blocks,
                    SUM(ref_count) AS total_references,
                    SUM(block_size) AS total_bytes,
                    COUNT(CASE WHEN ref_count = 0 THEN 1 END) AS unreferenced_blocks,
                    SUM(CASE WHEN ref_count = 0 THEN block_size ELSE 0 END) AS unreferenced_bytes,
                    AVG(ref_count) AS avg_ref_count,
                    MAX(ref_count) AS max_ref_count
                FROM block_references;
                
                -- View for transaction history
                CREATE VIEW IF NOT EXISTS transaction_history AS
                SELECT 
                    t.transaction_id,
                    t.start_timestamp,
                    t.commit_timestamp,
                    t.state,
                    t.description,
                    COUNT(o.op_id) AS operation_count,
                    SUM(CASE WHEN o.operation = 'INCREMENT' THEN o.delta ELSE 0 END) AS increments,
                    SUM(CASE WHEN o.operation = 'DECREMENT' THEN o.delta ELSE 0 END) AS decrements
                FROM ref_transactions t
                LEFT JOIN ref_transaction_ops o ON t.transaction_id = o.transaction_id
                GROUP BY t.transaction_id
                ORDER BY t.start_timestamp DESC;

                -- Composite index to accelerate GC/list operations
                CREATE INDEX IF NOT EXISTS idx_unref_order ON block_references(ref_count, last_accessed);

                -- Triggers to maintain materialized counters in ref_statistics
                CREATE TRIGGER IF NOT EXISTS trg_blocks_after_insert
                AFTER INSERT ON block_references
                BEGIN
                    UPDATE ref_statistics SET stat_value = stat_value + 1, updated_at=strftime('%s','now')
                      WHERE stat_name='total_blocks';
                    UPDATE ref_statistics SET stat_value = stat_value + NEW.block_size, updated_at=strftime('%s','now')
                      WHERE stat_name='total_bytes';
                    UPDATE ref_statistics SET stat_value = stat_value + NEW.ref_count, updated_at=strftime('%s','now')
                      WHERE stat_name='total_references';
                    UPDATE ref_statistics SET stat_value = stat_value + (CASE WHEN NEW.ref_count=0 THEN 1 ELSE 0 END), updated_at=strftime('%s','now')
                      WHERE stat_name='unreferenced_blocks';
                    UPDATE ref_statistics SET stat_value = stat_value + (CASE WHEN NEW.ref_count=0 THEN NEW.block_size ELSE 0 END), updated_at=strftime('%s','now')
                      WHERE stat_name='unreferenced_bytes';
                END;

                CREATE TRIGGER IF NOT EXISTS trg_blocks_after_update_ref
                AFTER UPDATE OF ref_count ON block_references
                BEGIN
                    -- Adjust total references by the delta
                    UPDATE ref_statistics SET stat_value = stat_value + (NEW.ref_count - OLD.ref_count), updated_at=strftime('%s','now')
                      WHERE stat_name='total_references';
                    -- Transition 0 -> >0 (leaving unreferenced)
                    UPDATE ref_statistics SET stat_value = stat_value - (CASE WHEN OLD.ref_count=0 AND NEW.ref_count>0 THEN 1 ELSE 0 END), updated_at=strftime('%s','now')
                      WHERE stat_name='unreferenced_blocks';
                    UPDATE ref_statistics SET stat_value = stat_value - (CASE WHEN OLD.ref_count=0 AND NEW.ref_count>0 THEN OLD.block_size ELSE 0 END), updated_at=strftime('%s','now')
                      WHERE stat_name='unreferenced_bytes';
                    -- Transition >0 -> 0 (becoming unreferenced)
                    UPDATE ref_statistics SET stat_value = stat_value + (CASE WHEN OLD.ref_count>0 AND NEW.ref_count=0 THEN 1 ELSE 0 END), updated_at=strftime('%s','now')
                      WHERE stat_name='unreferenced_blocks';
                    UPDATE ref_statistics SET stat_value = stat_value + (CASE WHEN OLD.ref_count>0 AND NEW.ref_count=0 THEN NEW.block_size ELSE 0 END), updated_at=strftime('%s','now')
                      WHERE stat_name='unreferenced_bytes';
                END;

                CREATE TRIGGER IF NOT EXISTS trg_blocks_after_delete
                AFTER DELETE ON block_references
                BEGIN
                    UPDATE ref_statistics SET stat_value = stat_value - 1, updated_at=strftime('%s','now')
                      WHERE stat_name='total_blocks';
                    UPDATE ref_statistics SET stat_value = stat_value - OLD.block_size, updated_at=strftime('%s','now')
                      WHERE stat_name='total_bytes';
                    UPDATE ref_statistics SET stat_value = stat_value - OLD.ref_count, updated_at=strftime('%s','now')
                      WHERE stat_name='total_references';
                    UPDATE ref_statistics SET stat_value = stat_value - (CASE WHEN OLD.ref_count=0 THEN 1 ELSE 0 END), updated_at=strftime('%s','now')
                      WHERE stat_name='unreferenced_blocks';
                    UPDATE ref_statistics SET stat_value = stat_value - (CASE WHEN OLD.ref_count=0 THEN OLD.block_size ELSE 0 END), updated_at=strftime('%s','now')
                      WHERE stat_name='unreferenced_bytes';
                END;

                -- One-time backfill to initialize counters from existing data
                INSERT OR REPLACE INTO ref_statistics(stat_name, stat_value, updated_at)
                VALUES
                  ('total_blocks', (SELECT COUNT(*) FROM block_references), strftime('%s','now')),
                  ('total_references', (SELECT IFNULL(SUM(ref_count),0) FROM block_references), strftime('%s','now')),
                  ('total_bytes', (SELECT IFNULL(SUM(block_size),0) FROM block_references), strftime('%s','now')),
                  ('unreferenced_blocks', (SELECT COUNT(*) FROM block_references WHERE ref_count=0), strftime('%s','now')),
                  ('unreferenced_bytes', (SELECT IFNULL(SUM(block_size),0) FROM block_references WHERE ref_count=0), strftime('%s','now'));
            )");
        }

        // Ensure stats materialization objects exist even when schema was loaded from file
        pImpl->db->execute(R"(
            CREATE INDEX IF NOT EXISTS idx_unref_order ON block_references(ref_count, last_accessed);
            CREATE TRIGGER IF NOT EXISTS trg_blocks_after_insert
            AFTER INSERT ON block_references
            BEGIN
                UPDATE ref_statistics SET stat_value = stat_value + 1, updated_at=strftime('%s','now')
                  WHERE stat_name='total_blocks';
                UPDATE ref_statistics SET stat_value = stat_value + NEW.block_size, updated_at=strftime('%s','now')
                  WHERE stat_name='total_bytes';
                UPDATE ref_statistics SET stat_value = stat_value + NEW.ref_count, updated_at=strftime('%s','now')
                  WHERE stat_name='total_references';
                UPDATE ref_statistics SET stat_value = stat_value + (CASE WHEN NEW.ref_count=0 THEN 1 ELSE 0 END), updated_at=strftime('%s','now')
                  WHERE stat_name='unreferenced_blocks';
                UPDATE ref_statistics SET stat_value = stat_value + (CASE WHEN NEW.ref_count=0 THEN NEW.block_size ELSE 0 END), updated_at=strftime('%s','now')
                  WHERE stat_name='unreferenced_bytes';
            END;

            CREATE TRIGGER IF NOT EXISTS trg_blocks_after_update_ref
            AFTER UPDATE OF ref_count ON block_references
            BEGIN
                UPDATE ref_statistics SET stat_value = stat_value + (NEW.ref_count - OLD.ref_count), updated_at=strftime('%s','now')
                  WHERE stat_name='total_references';
                UPDATE ref_statistics SET stat_value = stat_value - (CASE WHEN OLD.ref_count=0 AND NEW.ref_count>0 THEN 1 ELSE 0 END), updated_at=strftime('%s','now')
                  WHERE stat_name='unreferenced_blocks';
                UPDATE ref_statistics SET stat_value = stat_value - (CASE WHEN OLD.ref_count=0 AND NEW.ref_count>0 THEN OLD.block_size ELSE 0 END), updated_at=strftime('%s','now')
                  WHERE stat_name='unreferenced_bytes';
                UPDATE ref_statistics SET stat_value = stat_value + (CASE WHEN OLD.ref_count>0 AND NEW.ref_count=0 THEN 1 ELSE 0 END), updated_at=strftime('%s','now')
                  WHERE stat_name='unreferenced_blocks';
                UPDATE ref_statistics SET stat_value = stat_value + (CASE WHEN OLD.ref_count>0 AND NEW.ref_count=0 THEN NEW.block_size ELSE 0 END), updated_at=strftime('%s','now')
                  WHERE stat_name='unreferenced_bytes';
            END;

            CREATE TRIGGER IF NOT EXISTS trg_blocks_after_delete
            AFTER DELETE ON block_references
            BEGIN
                UPDATE ref_statistics SET stat_value = stat_value - 1, updated_at=strftime('%s','now')
                  WHERE stat_name='total_blocks';
                UPDATE ref_statistics SET stat_value = stat_value - OLD.block_size, updated_at=strftime('%s','now')
                  WHERE stat_name='total_bytes';
                UPDATE ref_statistics SET stat_value = stat_value - OLD.ref_count, updated_at=strftime('%s','now')
                  WHERE stat_name='total_references';
                UPDATE ref_statistics SET stat_value = stat_value - (CASE WHEN OLD.ref_count=0 THEN 1 ELSE 0 END), updated_at=strftime('%s','now')
                  WHERE stat_name='unreferenced_blocks';
                UPDATE ref_statistics SET stat_value = stat_value - (CASE WHEN OLD.ref_count=0 THEN OLD.block_size ELSE 0 END), updated_at=strftime('%s','now')
                  WHERE stat_name='unreferenced_bytes';
            END;

            INSERT OR REPLACE INTO ref_statistics(stat_name, stat_value, updated_at)
            VALUES
              ('total_blocks', (SELECT COUNT(*) FROM block_references), strftime('%s','now')),
              ('total_references', (SELECT IFNULL(SUM(ref_count),0) FROM block_references), strftime('%s','now')),
              ('total_bytes', (SELECT IFNULL(SUM(block_size),0) FROM block_references), strftime('%s','now')),
              ('unreferenced_blocks', (SELECT COUNT(*) FROM block_references WHERE ref_count=0), strftime('%s','now')),
              ('unreferenced_bytes', (SELECT IFNULL(SUM(block_size),0) FROM block_references WHERE ref_count=0), strftime('%s','now'));
        )");

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

        // Transaction IDs are now managed by SQLite AUTOINCREMENT
        // No need to manually track nextTransactionId

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

        auto stmt = pImpl->stmtCache->get(Impl::INCREMENT_STMT, "");
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

        auto stmt = pImpl->stmtCache->get(Impl::DECREMENT_STMT, "");
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
        // Serialize access to the SQLite handle; shared access can race on Windows builds
        std::unique_lock lock(pImpl->dbMutex);

        auto stmt = pImpl->stmtCache->get(Impl::GET_REF_COUNT_STMT, "");
        stmt.bind(1, blockHash);

        if (stmt.step()) {
            return static_cast<uint64_t>(stmt.getInt64(0));
        }

        return 0; // Block not found
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
        // Serialize reads with writes to avoid SQLite handle reuse races on Windows
        std::unique_lock lock(pImpl->dbMutex);

        RefCountStats stats{};

        // Read materialized counters from ref_statistics (single-row result via scalar subselects)
        // Create statement in local scope - destructor will properly finalize
        {
            auto stmt = pImpl->db->prepare(R"(
                SELECT
                  (SELECT stat_value FROM ref_statistics WHERE stat_name='total_blocks'),
                  (SELECT stat_value FROM ref_statistics WHERE stat_name='total_references'),
                  (SELECT stat_value FROM ref_statistics WHERE stat_name='total_bytes'),
                  (SELECT stat_value FROM ref_statistics WHERE stat_name='unreferenced_blocks'),
                  (SELECT stat_value FROM ref_statistics WHERE stat_name='unreferenced_bytes'),
                  (SELECT stat_value FROM ref_statistics WHERE stat_name='transactions_completed'),
                  (SELECT stat_value FROM ref_statistics WHERE stat_name='transactions_rolled_back')
            )");

            if (stmt.step()) {
                stats.totalBlocks = static_cast<uint64_t>(stmt.getInt64(0));
                stats.totalReferences = static_cast<uint64_t>(stmt.getInt64(1));
                stats.totalBytes = static_cast<uint64_t>(stmt.getInt64(2));
                stats.unreferencedBlocks = static_cast<uint64_t>(stmt.getInt64(3));
                stats.unreferencedBytes = static_cast<uint64_t>(stmt.getInt64(4));
                stats.transactions = static_cast<uint64_t>(stmt.getInt64(5));
                stats.rollbacks = static_cast<uint64_t>(stmt.getInt64(6));
            }
            // stmt destructor called here, finalizes sqlite3_stmt
        }

        // Defensive recomputation: if triggers drift or stats table falls behind, reconcile
        // with authoritative values from block_references.
        {
            auto recalcStmt = pImpl->db->prepare(R"(
                SELECT
                  COUNT(*) AS total_blocks,
                  IFNULL(SUM(ref_count), 0) AS total_references,
                  IFNULL(SUM(block_size), 0) AS total_bytes,
                  COUNT(CASE WHEN ref_count = 0 THEN 1 END) AS unreferenced_blocks,
                  IFNULL(SUM(CASE WHEN ref_count = 0 THEN block_size ELSE 0 END), 0) AS unreferenced_bytes
                FROM block_references
            )");

            if (recalcStmt.step()) {
                const auto tableBlocks = static_cast<uint64_t>(recalcStmt.getInt64(0));
                const auto tableRefs = static_cast<uint64_t>(recalcStmt.getInt64(1));
                const auto tableBytes = static_cast<uint64_t>(recalcStmt.getInt64(2));
                const auto tableUnref = static_cast<uint64_t>(recalcStmt.getInt64(3));
                const auto tableUnrefBytes = static_cast<uint64_t>(recalcStmt.getInt64(4));

                if (stats.totalBlocks != tableBlocks || stats.totalReferences != tableRefs ||
                    stats.totalBytes != tableBytes || stats.unreferencedBlocks != tableUnref ||
                    stats.unreferencedBytes != tableUnrefBytes) {
                    stats.totalBlocks = tableBlocks;
                    stats.totalReferences = tableRefs;
                    stats.totalBytes = tableBytes;
                    stats.unreferencedBlocks = tableUnref;
                    stats.unreferencedBytes = tableUnrefBytes;
                }
            }
            // recalcStmt destructor called here, finalizes sqlite3_stmt
        }

        return stats;
    } catch (const std::exception& e) {
        spdlog::error("Failed to get statistics: {}", e.what());
        return Result<RefCountStats>(ErrorCode::DatabaseError);
    }
}

// Get unreferenced blocks
Result<std::vector<std::string>>
ReferenceCounter::getUnreferencedBlocks(size_t limit, std::chrono::seconds minAge) const {
    try {
        // Serialize access to the single SQLite handle
        std::unique_lock lock(pImpl->dbMutex);

        std::vector<std::string> blocks;
        blocks.reserve(limit);

        // Get a fresh statement (cache now returns by value)
        auto stmt = pImpl->stmtCache->get(Impl::GET_UNREFERENCED_STMT, "");
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
    // Transaction ID will be assigned by database AUTOINCREMENT
    return std::unique_ptr<ITransaction>(
        new Transaction(this, -1)); // -1 indicates ID not yet assigned
}

// Transaction implementation
ReferenceCounter::Transaction::Transaction(ReferenceCounter* counter, int64_t id)
    : counter_(counter), transactionId_(id), active_(true), committed_(false) {
    try {
        // Record transaction start - let database assign ID via AUTOINCREMENT
        // Don't start SQLite transaction yet - operations will be batched and applied during commit
        {
            std::unique_lock lock(counter_->pImpl->dbMutex);
            auto stmt = counter_->pImpl->db->prepare(R"(
                INSERT INTO ref_transactions (start_timestamp, state)
                VALUES (strftime('%s', 'now'), 'PENDING')
            )");
            stmt.execute();

            // Get the assigned transaction ID
            transactionId_ = counter_->pImpl->db->lastInsertRowId();
        }

        spdlog::debug("Started reference counting transaction {}", transactionId_);
    } catch (const std::exception& e) {
        active_ = false;
        throw std::runtime_error(yamsfmt::format("Failed to start transaction: {}", e.what()));
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
    : counter_(other.counter_), transactionId_(other.transactionId_), active_(other.active_),
      committed_(other.committed_), operations_(std::move(other.operations_)) {
    other.active_ = false;
    other.counter_ = nullptr;
}

// Move assignment
ReferenceCounter::Transaction&
ReferenceCounter::Transaction::operator=(Transaction&& other) noexcept {
    if (this != &other) {
        if (active_ && !committed_) {
            try {
                rollback();
            } catch (...) {
            }
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

    operations_.push_back({.type = Operation::Type::Increment,
                           .blockHash = std::string(blockHash),
                           .blockSize = blockSize,
                           .delta = 1});

    // Operations are recorded only in-memory during queue phase
    // Database writes will occur only during commit() for true atomic behavior
}

// Decrement in transaction
void ReferenceCounter::Transaction::decrement(std::string_view blockHash) {
    if (!active_) {
        throw std::runtime_error("Transaction is not active");
    }

    operations_.push_back({.type = Operation::Type::Decrement,
                           .blockHash = std::string(blockHash),
                           .blockSize = 0,
                           .delta = -1});

    // Operations are recorded only in-memory during queue phase
    // Database writes will occur only during commit() for true atomic behavior
}

// Commit transaction
Result<void> ReferenceCounter::Transaction::commit() {
    if (!active_) {
        return Result<void>(ErrorCode::TransactionFailed);
    }

    try {
        std::unique_lock lock(counter_->pImpl->dbMutex);

        // Start SQLite transaction for atomic operation application
        counter_->pImpl->db->beginTransaction();

        try {
            // Apply all queued operations atomically
            for (const auto& op : operations_) {
                if (op.type == Operation::Type::Increment) {
                    auto stmt =
                        counter_->pImpl->stmtCache->get(ReferenceCounter::Impl::INCREMENT_STMT, "");
                    stmt.bind(1, op.blockHash);
                    stmt.bind(2, static_cast<int64_t>(op.blockSize));
                    stmt.execute();
                } else {
                    auto stmt =
                        counter_->pImpl->stmtCache->get(ReferenceCounter::Impl::DECREMENT_STMT, "");
                    stmt.bind(1, op.blockHash);
                    stmt.execute();
                }
            }

            // Record all operations in audit log during commit
            for (const auto& op : operations_) {
                auto stmt = counter_->pImpl->db->prepare(R"(
                    INSERT INTO ref_transaction_ops 
                    (transaction_id, block_hash, operation, delta, block_size, timestamp)
                    VALUES (?, ?, ?, ?, ?, strftime('%s', 'now'))
                )");
                stmt.bind(1, transactionId_);
                stmt.bind(2, op.blockHash);
                stmt.bind(3, std::string(op.type == Operation::Type::Increment ? "INCREMENT"
                                                                               : "DECREMENT"));
                stmt.bind(4,
                          1); // Schema requires positive delta - operation type indicates direction
                stmt.bind(5, static_cast<int64_t>(op.blockSize));
                stmt.execute();
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

            // Commit SQLite transaction
            counter_->pImpl->db->commit();

            active_ = false;
            committed_ = true;

            spdlog::debug("Committed transaction {} with {} operations", transactionId_,
                          operations_.size());

            return {};

        } catch (const std::exception& e) {
            // Rollback SQLite transaction on any error
            counter_->pImpl->db->rollback();
            throw;
        }

    } catch (const std::exception& e) {
        spdlog::error("Failed to commit transaction {}: {}", transactionId_, e.what());
        // Mark logical transaction as failed
        active_ = false;
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

        // Update transaction state (no SQLite transaction to rollback since operations were queued)
        auto stmt = counter_->pImpl->db->prepare(R"(
            UPDATE ref_transactions 
            SET state = 'ROLLED_BACK'
            WHERE transaction_id = ?
        )");
        stmt.bind(1, transactionId_);
        stmt.execute();

        // Update statistics
        counter_->updateStatistics("transactions_rolled_back", 1);

        // Log and clear queued operations, then mark as inactive
        size_t operationCount = operations_.size();
        operations_.clear();
        active_ = false;

        spdlog::debug("Rolled back transaction {} (cleared {} queued operations)", transactionId_,
                      operationCount);
    } catch (const std::exception& e) {
        spdlog::error("Error during rollback of transaction {}: {}", transactionId_, e.what());
        // Force inactive state even if update failed
        active_ = false;
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
        // Integrity check must be exclusive on this connection
        std::unique_lock lock(pImpl->dbMutex);
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
        // Exclusive lock: underlying SQLite handle is NOMUTEX
        std::unique_lock lock(pImpl->dbMutex);
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
