#pragma once

#include <yams/core/types.h>
#include <sqlite3.h>
#include <memory>
#include <string>
#include <vector>
#include <functional>
#include <span>
#include <chrono>
#include <optional>

namespace yams::metadata {

/**
 * @brief Database error codes
 */
enum class DatabaseError {
    ConnectionFailed,
    QueryFailed,
    TransactionFailed,
    MigrationFailed,
    Busy,
    Locked,
    Corrupt,
    NotFound,
    ConstraintViolation,
    TypeMismatch,
    Unknown
};

/**
 * @brief Database connection mode
 */
enum class ConnectionMode {
    ReadWrite,      ///< Read-write mode (default)
    ReadOnly,       ///< Read-only mode
    Memory,         ///< In-memory database
    Create          ///< Create if not exists
};

/**
 * @brief SQLite statement wrapper with RAII
 */
class Statement {
public:
    Statement() = default;
    Statement(sqlite3* db, const std::string& sql);
    ~Statement();
    
    // Move-only
    Statement(Statement&& other) noexcept;
    Statement& operator=(Statement&& other) noexcept;
    Statement(const Statement&) = delete;
    Statement& operator=(const Statement&) = delete;
    
    /**
     * @brief Bind parameters to statement
     */
    Result<void> bind(int index, std::nullptr_t);
    Result<void> bind(int index, int value);
    Result<void> bind(int index, int64_t value);
    Result<void> bind(int index, double value);
    Result<void> bind(int index, const std::string& value);
    Result<void> bind(int index, std::string_view value);
    Result<void> bind(int index, std::span<const std::byte> blob);
    Result<void> bind(int index, const char* value) {
        return bind(index, std::string_view(value));
    }
    
    /**
     * @brief Bind multiple parameters using variadic templates
     */
    template<typename... Args>
    Result<void> bindAll(Args&&... args) {
        return bindHelper(1, std::forward<Args>(args)...);
    }
    
    /**
     * @brief Execute statement (for non-SELECT queries)
     */
    Result<void> execute();
    
    /**
     * @brief Step through results (for SELECT queries)
     * @return true if row available, false if done
     */
    Result<bool> step();
    
    /**
     * @brief Get column values
     */
    int getInt(int column) const;
    int64_t getInt64(int column) const;
    double getDouble(int column) const;
    std::string getString(int column) const;
    std::vector<std::byte> getBlob(int column) const;
    bool isNull(int column) const;
    
    /**
     * @brief Get column count
     */
    int columnCount() const;
    
    /**
     * @brief Get column name
     */
    std::string columnName(int column) const;
    
    /**
     * @brief Reset statement for reuse
     */
    Result<void> reset();
    
    /**
     * @brief Clear all bindings
     */
    Result<void> clearBindings();
    
private:
    sqlite3_stmt* stmt_ = nullptr;
    
    template<typename T, typename... Rest>
    Result<void> bindHelper(int index, T&& value, Rest&&... rest) {
        auto result = bind(index, std::forward<T>(value));
        if (!result) return result;
        if constexpr (sizeof...(rest) > 0) {
            return bindHelper(index + 1, std::forward<Rest>(rest)...);
        }
        return {};
    }
    
    Result<void> bindHelper(int) { return {}; }
};

/**
 * @brief Database connection wrapper
 */
class Database {
public:
    Database() = default;
    ~Database();
    
    // Move-only
    Database(Database&& other) noexcept;
    Database& operator=(Database&& other) noexcept;
    Database(const Database&) = delete;
    Database& operator=(const Database&) = delete;
    
    /**
     * @brief Open database connection
     */
    Result<void> open(const std::string& path, ConnectionMode mode = ConnectionMode::ReadWrite);
    
    /**
     * @brief Close database connection
     */
    void close();
    
    /**
     * @brief Check if database is open
     */
    [[nodiscard]] bool isOpen() const { return db_ != nullptr; }
    
    /**
     * @brief Prepare SQL statement
     */
    Result<Statement> prepare(const std::string& sql);
    
    /**
     * @brief Execute SQL directly (for non-SELECT queries)
     */
    Result<void> execute(const std::string& sql);
    
    /**
     * @brief Begin transaction
     */
    Result<void> beginTransaction();
    
    /**
     * @brief Commit transaction
     */
    Result<void> commit();
    
    /**
     * @brief Rollback transaction
     */
    Result<void> rollback();
    
    /**
     * @brief Execute within transaction
     */
    template<typename Func>
    Result<void> transaction(Func&& func) {
        auto beginResult = beginTransaction();
        if (!beginResult) return beginResult;
        
        try {
            auto result = func();
            if (!result) {
                rollback();
                return result;
            }
            return commit();
        } catch (...) {
            rollback();
            throw;
        }
    }
    
    /**
     * @brief Get last insert row ID
     */
    int64_t lastInsertRowId() const;
    
    /**
     * @brief Get number of rows affected by last query
     */
    int changes() const;
    
    /**
     * @brief Check if a table exists
     */
    Result<bool> tableExists(const std::string& table);
    
    /**
     * @brief Check if FTS5 is available
     */
    Result<bool> hasFTS5();
    
    /**
     * @brief Set busy timeout
     */
    Result<void> setBusyTimeout(std::chrono::milliseconds timeout);
    
    /**
     * @brief Enable WAL mode
     */
    Result<void> enableWAL();
    
    /**
     * @brief Optimize database
     */
    Result<void> optimize();
    
    /**
     * @brief Get SQLite version
     */
    static std::string version();
    
    /**
     * @brief Get database path
     */
    [[nodiscard]] const std::string& path() const { return path_; }
    
private:
    sqlite3* db_ = nullptr;
    std::string path_;
    bool inTransaction_ = false;
    
    /**
     * @brief Convert SQLite error code to our error type
     */
    static DatabaseError translateError(int sqliteError);
    
    /**
     * @brief Get error message from SQLite
     */
    std::string getErrorMessage() const;
};

/**
 * @brief Query builder for constructing SQL queries
 */
class QueryBuilder {
public:
    QueryBuilder() = default;
    
    // SELECT
    QueryBuilder& select(const std::vector<std::string>& columns = {});
    QueryBuilder& from(const std::string& table);
    QueryBuilder& where(const std::string& condition);
    QueryBuilder& andWhere(const std::string& condition);
    QueryBuilder& orWhere(const std::string& condition);
    QueryBuilder& join(const std::string& table, const std::string& on);
    QueryBuilder& leftJoin(const std::string& table, const std::string& on);
    QueryBuilder& orderBy(const std::string& column, bool ascending = true);
    QueryBuilder& limit(int limit);
    QueryBuilder& offset(int offset);
    QueryBuilder& groupBy(const std::string& column);
    QueryBuilder& having(const std::string& condition);
    
    // INSERT
    QueryBuilder& insertInto(const std::string& table);
    QueryBuilder& values(const std::vector<std::string>& columns);
    
    // UPDATE
    QueryBuilder& update(const std::string& table);
    QueryBuilder& set(const std::string& column, const std::string& placeholder = "?");
    
    // DELETE
    QueryBuilder& deleteFrom(const std::string& table);
    
    // Build final SQL
    [[nodiscard]] std::string build() const;
    
    // Reset builder
    void reset();
    
private:
    enum class QueryType {
        None,
        Select,
        Insert,
        Update,
        Delete
    };
    
    QueryType type_ = QueryType::None;
    std::string table_;
    std::vector<std::string> selectColumns_;
    std::vector<std::string> insertColumns_;
    std::vector<std::pair<std::string, std::string>> setClauses_;
    std::vector<std::string> whereClauses_;
    std::vector<std::string> joinClauses_;
    std::string groupByClause_;
    std::string havingClause_;
    std::string orderByClause_;
    int limit_ = -1;
    int offset_ = -1;
};

} // namespace yams::metadata