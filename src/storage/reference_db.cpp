#include <sqlite3.h>
#include <spdlog/spdlog.h>
#if defined(YAMS_HAS_STD_FORMAT) && YAMS_HAS_STD_FORMAT
#include <format>
namespace yamsfmt = std;
#else
#include <spdlog/fmt/fmt.h>
namespace yamsfmt = fmt;
#endif
#include <yams/core/types.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace yams::storage {

// Retry parameters vary by backend:
// - libsql MVCC: fewer retries needed since concurrent writers don't block
// - SQLite: more retries with longer backoff for single-writer model
#if YAMS_LIBSQL_BACKEND
constexpr int kMaxRetries = 3;
constexpr int kInitialBackoffMs = 5;
#else
constexpr int kMaxRetries = 5;
constexpr int kInitialBackoffMs = 10;
#endif

// SQLite RAII wrapper for statements
class Statement {
public:
    Statement(sqlite3* db, const std::string& sql) : db_(db) {
        int rc = sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt_, nullptr);
        if (rc != SQLITE_OK) {
            throw std::runtime_error(
                yamsfmt::format("Failed to prepare statement: {}", sqlite3_errmsg(db)));
        }
    }

    ~Statement() {
        if (stmt_) {
            sqlite3_finalize(stmt_);
        }
    }

    // Delete copy, enable move
    Statement(const Statement&) = delete;
    Statement& operator=(const Statement&) = delete;

    Statement(Statement&& other) noexcept : stmt_(other.stmt_), db_(other.db_) {
        other.stmt_ = nullptr;
    }

    Statement& operator=(Statement&& other) noexcept {
        if (this != &other) {
            if (stmt_) {
                sqlite3_finalize(stmt_);
            }
            stmt_ = other.stmt_;
            db_ = other.db_;
            other.stmt_ = nullptr;
        }
        return *this;
    }

    // Bind parameters
    void bind(int index, int value) {
        if (sqlite3_bind_int(stmt_, index, value) != SQLITE_OK) {
            throw std::runtime_error("Failed to bind int parameter");
        }
    }

    void bind(int index, int64_t value) {
        if (sqlite3_bind_int64(stmt_, index, value) != SQLITE_OK) {
            throw std::runtime_error("Failed to bind int64 parameter");
        }
    }

    void bind(int index, const std::string& value) {
        if (sqlite3_bind_text(stmt_, index, value.c_str(), -1, SQLITE_TRANSIENT) != SQLITE_OK) {
            throw std::runtime_error("Failed to bind text parameter");
        }
    }

    void bind(int index, std::string_view value) {
        if (sqlite3_bind_text(stmt_, index, value.data(), static_cast<int>(value.size()),
                              SQLITE_TRANSIENT) != SQLITE_OK) {
            throw std::runtime_error("Failed to bind text parameter");
        }
    }

    void bindNull(int index) {
        if (sqlite3_bind_null(stmt_, index) != SQLITE_OK) {
            throw std::runtime_error("Failed to bind null parameter");
        }
    }

    // Execute and fetch results with retry for transient lock errors
    bool step() {
        auto backoff = std::chrono::milliseconds(kInitialBackoffMs);

        for (int attempt = 0; attempt < kMaxRetries; ++attempt) {
            int rc = sqlite3_step(stmt_);
            if (rc == SQLITE_ROW) {
                return true;
            } else if (rc == SQLITE_DONE) {
                return false;
            }

            // Check for transient lock errors
            if ((rc == SQLITE_BUSY || rc == SQLITE_LOCKED) && attempt + 1 < kMaxRetries) {
                sqlite3_reset(stmt_);
                std::this_thread::sleep_for(backoff);
                backoff *= 2;
                continue;
            }

            throw std::runtime_error(
                yamsfmt::format("Statement execution failed: {}", sqlite3_errmsg(db_)));
        }

        throw std::runtime_error("Statement execution failed: max retries exceeded");
    }

    void execute() {
        if (!step()) {
            return;
        }
        throw std::runtime_error("Execute called on query that returns data");
    }

    void reset() {
        sqlite3_reset(stmt_);
        sqlite3_clear_bindings(stmt_);
    }

    // Get column values
    int getInt(int col) const { return sqlite3_column_int(stmt_, col); }

    int64_t getInt64(int col) const { return sqlite3_column_int64(stmt_, col); }

    std::string getString(int col) const {
        const char* text = reinterpret_cast<const char*>(sqlite3_column_text(stmt_, col));
        return text ? std::string(text) : std::string();
    }

    bool isNull(int col) const { return sqlite3_column_type(stmt_, col) == SQLITE_NULL; }

    int columnCount() const { return sqlite3_column_count(stmt_); }

private:
    sqlite3_stmt* stmt_ = nullptr;
    sqlite3* db_ = nullptr;
};

// Simple database wrapper with proper thread safety
class Database {
public:
    explicit Database(const std::filesystem::path& path) {
        // Use FULLMUTEX for proper thread safety - SQLite will handle internal locking
        int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX;

        int rc = sqlite3_open_v2(path.string().c_str(), &db_, flags, nullptr);
        if (rc != SQLITE_OK) {
            std::string errMsg = db_ ? sqlite3_errmsg(db_) : "Failed to open database";
            throw std::runtime_error(yamsfmt::format("Failed to open database: {}", errMsg));
        }

        // Enable foreign keys
        execute("PRAGMA foreign_keys = ON");

        // Set busy timeout for concurrent transactions (15 seconds)
        sqlite3_busy_timeout(db_, 15000);

        // WAL mode for better concurrency (unless testing)
        if (const char* test_env = std::getenv("YAMS_TESTING")) {
            std::string v(test_env);
            std::transform(v.begin(), v.end(), v.begin(),
                           [](unsigned char c) { return (char)std::tolower(c); });
            if (v == "1" || v == "true" || v == "yes" || v == "on") {
                execute("PRAGMA journal_mode = MEMORY");
                spdlog::debug("Reference DB: using MEMORY journal mode (YAMS_TESTING=1)");
            } else {
                execute("PRAGMA journal_mode = WAL");
            }
        } else {
            execute("PRAGMA journal_mode = WAL");
        }
    }

    ~Database() {
        if (db_) {
            // Finalize all cached statements before closing
            sqlite3_close_v2(db_); // v2 allows cleanup of lingering statements
        }
    }

    // Delete copy
    Database(const Database&) = delete;
    Database& operator=(const Database&) = delete;

    // Execute SQL without results, with retry for transient lock errors
    void execute(const std::string& sql) {
        auto backoff = std::chrono::milliseconds(kInitialBackoffMs);

        for (int attempt = 0; attempt < kMaxRetries; ++attempt) {
            char* errMsg = nullptr;
            int rc = sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, &errMsg);

            if (rc == SQLITE_OK) {
                return;
            }

            std::string error = errMsg ? errMsg : "Unknown error";
            sqlite3_free(errMsg);

            // Check for transient lock errors
            if ((rc == SQLITE_BUSY || rc == SQLITE_LOCKED) && attempt + 1 < kMaxRetries) {
                spdlog::debug("[ReferenceDB] transient lock, retry {}/{}", attempt + 1,
                              kMaxRetries);
                std::this_thread::sleep_for(backoff);
                backoff *= 2;
                continue;
            }

            throw std::runtime_error(yamsfmt::format("SQL execution failed: {}", error));
        }

        throw std::runtime_error("SQL execution failed: max retries exceeded");
    }

    // Execute SQL from file
    void executeFile(const std::filesystem::path& path) {
        std::ifstream file(path);
        if (!file) {
            throw std::runtime_error("Failed to open SQL file: " + path.string());
        }

        std::stringstream buffer;
        buffer << file.rdbuf();
        execute(buffer.str());
    }

    // Prepare statement - now returns by value (move semantics handle efficiency)
    Statement prepare(const std::string& sql) {
        if (!db_) {
            throw std::runtime_error("Database not initialized");
        }
        return Statement(db_, sql);
    }

    // Transaction control with backend-appropriate semantics
    void beginTransaction() {
#if YAMS_LIBSQL_BACKEND
        // libsql MVCC: use regular BEGIN (deferred) for better concurrency
        execute("BEGIN TRANSACTION");
#else
        // SQLite: use BEGIN IMMEDIATE to acquire write lock immediately
        execute("BEGIN IMMEDIATE TRANSACTION");
#endif
    }

    void commit() { execute("COMMIT"); }

    void rollback() { execute("ROLLBACK"); }

    // Get last insert rowid
    int64_t lastInsertRowId() const { return sqlite3_last_insert_rowid(db_); }

    // Get changes from last statement
    int changes() const { return sqlite3_changes(db_); }

    // WAL checkpoint
    void checkpoint() {
        int log, ckpt;
        sqlite3_wal_checkpoint_v2(db_, nullptr, SQLITE_CHECKPOINT_PASSIVE, &log, &ckpt);
    }

    // Vacuum database
    void vacuum() { execute("VACUUM"); }

    // Analyze database
    void analyze() { execute("ANALYZE"); }

    // Backup database
    void backup(const std::filesystem::path& destPath) {
        sqlite3* destDb = nullptr;
        int rc = sqlite3_open(destPath.string().c_str(), &destDb);
        if (rc != SQLITE_OK) {
            throw std::runtime_error("Failed to open backup destination");
        }

        sqlite3_backup* backup = sqlite3_backup_init(destDb, "main", db_, "main");
        if (!backup) {
            sqlite3_close(destDb);
            throw std::runtime_error("Failed to initialize backup");
        }

        // Copy entire database
        rc = sqlite3_backup_step(backup, -1);
        sqlite3_backup_finish(backup);
        sqlite3_close(destDb);

        if (rc != SQLITE_DONE) {
            throw std::runtime_error("Backup failed");
        }
    }

    // Check integrity
    bool checkIntegrity() {
        auto stmt = prepare("PRAGMA integrity_check");
        bool ok = true;

        while (stmt.step()) {
            std::string result = stmt.getString(0);
            if (result != "ok") {
                spdlog::error("Integrity check failed: {}", result);
                ok = false;
            }
        }

        return ok;
    }

    // Get SQLite version
    std::string getVersion() const { return sqlite3_libversion(); }

private:
    sqlite3* db_ = nullptr;
};

// Statement cache for prepared statements
// Note: Returns by value to avoid lifetime issues. Move semantics make this efficient.
class StatementCache {
public:
    explicit StatementCache(Database& db) : db_(db) {}

    // Returns a copy/move of the cached statement with reset bindings
    Statement get(const std::string& key, const std::string& sql) {
        std::lock_guard lock(mutex_);

        auto it = sqlCache_.find(key);
        if (it != sqlCache_.end()) {
            // Use cached SQL to create a fresh statement
            return db_.prepare(it->second);
        }

        // Cache the SQL text, not the statement itself
        sqlCache_[key] = sql;
        return db_.prepare(sql);
    }

    void clear() {
        std::lock_guard lock(mutex_);
        sqlCache_.clear();
    }

private:
    Database& db_;
    // Cache SQL text, not prepared statements (avoids lifetime issues)
    std::unordered_map<std::string, std::string> sqlCache_;
    mutable std::mutex mutex_;
};

} // namespace yams::storage