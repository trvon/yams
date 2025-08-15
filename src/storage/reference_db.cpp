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

    // Execute and fetch results
    bool step() {
        int rc = sqlite3_step(stmt_);
        if (rc == SQLITE_ROW) {
            return true;
        } else if (rc == SQLITE_DONE) {
            return false;
        } else {
            throw std::runtime_error(
                yamsfmt::format("Statement execution failed: {}", sqlite3_errmsg(db_)));
        }
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

// Simple database wrapper
class Database {
public:
    explicit Database(const std::filesystem::path& path) {
        int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE |
                    SQLITE_OPEN_NOMUTEX; // We handle thread safety ourselves

        int rc = sqlite3_open_v2(path.string().c_str(), &db_, flags, nullptr);
        if (rc != SQLITE_OK) {
            throw std::runtime_error(
                yamsfmt::format("Failed to open database: {}", sqlite3_errmsg(db_)));
        }

        // Enable foreign keys
        execute("PRAGMA foreign_keys = ON");

        // Set busy timeout for concurrent transactions (15 seconds)
        sqlite3_busy_timeout(db_, 15000);
    }

    ~Database() {
        if (db_) {
            sqlite3_close(db_);
        }
    }

    // Delete copy
    Database(const Database&) = delete;
    Database& operator=(const Database&) = delete;

    // Execute SQL without results
    void execute(const std::string& sql) {
        char* errMsg = nullptr;
        int rc = sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, &errMsg);
        if (rc != SQLITE_OK) {
            std::string error = errMsg ? errMsg : "Unknown error";
            sqlite3_free(errMsg);
            throw std::runtime_error(yamsfmt::format("SQL execution failed: {}", error));
        }
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

    // Prepare statement
    Statement prepare(const std::string& sql) { return Statement(db_, sql); }

    // Transaction control with proper timeout handling
    void beginTransaction() {
        // IMMEDIATE transaction with high busy timeout should handle concurrency
        execute("BEGIN IMMEDIATE TRANSACTION");
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
class StatementCache {
public:
    explicit StatementCache(Database& db) : db_(db) {}

    Statement& get(const std::string& key, const std::string& sql) {
        std::lock_guard lock(mutex_);

        auto it = cache_.find(key);
        if (it != cache_.end()) {
            it->second.reset();
            return it->second;
        }

        auto [inserted, success] = cache_.emplace(key, db_.prepare(sql));
        return inserted->second;
    }

    void clear() {
        std::lock_guard lock(mutex_);
        cache_.clear();
    }

private:
    Database& db_;
    std::unordered_map<std::string, Statement> cache_;
    mutable std::mutex mutex_;
};

} // namespace yams::storage