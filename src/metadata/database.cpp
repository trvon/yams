#include <cstring>
#include <sstream>
#include <yams/metadata/database.h>

namespace yams::metadata {

// Statement implementation
Statement::Statement(sqlite3* db, const std::string& sql) {
    const char* tail;
    int rc = sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt_, &tail);
    if (rc != SQLITE_OK) {
        throw std::runtime_error("Failed to prepare statement: " + std::string(sqlite3_errmsg(db)));
    }
}

Statement::~Statement() {
    if (stmt_) {
        sqlite3_finalize(stmt_);
    }
}

Statement::Statement(Statement&& other) noexcept : stmt_(other.stmt_) {
    other.stmt_ = nullptr;
}

Statement& Statement::operator=(Statement&& other) noexcept {
    if (this != &other) {
        if (stmt_) {
            sqlite3_finalize(stmt_);
        }
        stmt_ = other.stmt_;
        other.stmt_ = nullptr;
    }
    return *this;
}

Result<void> Statement::bind(int index, std::nullptr_t) {
    int rc = sqlite3_bind_null(stmt_, index);
    if (rc != SQLITE_OK) {
        return Error{ErrorCode::DatabaseError, "Failed to bind null"};
    }
    return {};
}

Result<void> Statement::bind(int index, int value) {
    int rc = sqlite3_bind_int(stmt_, index, value);
    if (rc != SQLITE_OK) {
        return Error{ErrorCode::DatabaseError, "Failed to bind int"};
    }
    return {};
}

Result<void> Statement::bind(int index, int64_t value) {
    int rc = sqlite3_bind_int64(stmt_, index, value);
    if (rc != SQLITE_OK) {
        return Error{ErrorCode::DatabaseError, "Failed to bind int64"};
    }
    return {};
}

Result<void> Statement::bind(int index, float value) {
    // SQLite doesn't have a specific float bind, so convert to double
    int rc = sqlite3_bind_double(stmt_, index, static_cast<double>(value));
    if (rc != SQLITE_OK) {
        return Error{ErrorCode::DatabaseError, "Failed to bind float"};
    }
    return {};
}

Result<void> Statement::bind(int index, double value) {
    int rc = sqlite3_bind_double(stmt_, index, value);
    if (rc != SQLITE_OK) {
        return Error{ErrorCode::DatabaseError, "Failed to bind double"};
    }
    return {};
}

Result<void> Statement::bind(int index, const std::string& value) {
    int rc = sqlite3_bind_text(stmt_, index, value.c_str(), static_cast<int>(value.size()),
                               SQLITE_TRANSIENT);
    if (rc != SQLITE_OK) {
        return Error{ErrorCode::DatabaseError, "Failed to bind string"};
    }
    return {};
}

Result<void> Statement::bind(int index, std::string_view value) {
    int rc = sqlite3_bind_text(stmt_, index, value.data(), static_cast<int>(value.size()),
                               SQLITE_TRANSIENT);
    if (rc != SQLITE_OK) {
        return Error{ErrorCode::DatabaseError, "Failed to bind string_view"};
    }
    return {};
}

Result<void> Statement::bind(int index, std::span<const std::byte> blob) {
    int rc = sqlite3_bind_blob(stmt_, index, blob.data(), static_cast<int>(blob.size()),
                               SQLITE_TRANSIENT);
    if (rc != SQLITE_OK) {
        return Error{ErrorCode::DatabaseError, "Failed to bind blob"};
    }
    return {};
}

Result<void> Statement::execute() {
    int rc = sqlite3_step(stmt_);
    if (rc != SQLITE_DONE) {
        return Error{ErrorCode::DatabaseError,
                     "Failed to execute statement: " + std::string(sqlite3_errstr(rc))};
    }
    return {};
}

Result<bool> Statement::step() {
    int rc = sqlite3_step(stmt_);
    if (rc == SQLITE_ROW) {
        return true;
    } else if (rc == SQLITE_DONE) {
        return false;
    } else {
        return Error{ErrorCode::DatabaseError,
                     "Failed to step statement: " + std::string(sqlite3_errstr(rc))};
    }
}

int Statement::getInt(int column) const {
    return sqlite3_column_int(stmt_, column);
}

int64_t Statement::getInt64(int column) const {
    return sqlite3_column_int64(stmt_, column);
}

double Statement::getDouble(int column) const {
    return sqlite3_column_double(stmt_, column);
}

std::string Statement::getString(int column) const {
    const char* text = reinterpret_cast<const char*>(sqlite3_column_text(stmt_, column));
    if (!text)
        return "";
    return std::string(text);
}

std::vector<std::byte> Statement::getBlob(int column) const {
    const void* blob = sqlite3_column_blob(stmt_, column);
    int size = sqlite3_column_bytes(stmt_, column);
    if (!blob || size <= 0)
        return {};

    std::vector<std::byte> result(size);
    std::memcpy(result.data(), blob, size);
    return result;
}

bool Statement::isNull(int column) const {
    return sqlite3_column_type(stmt_, column) == SQLITE_NULL;
}

int Statement::columnCount() const {
    return sqlite3_column_count(stmt_);
}

std::string Statement::columnName(int column) const {
    const char* name = sqlite3_column_name(stmt_, column);
    return name ? name : "";
}

Result<void> Statement::reset() {
    int rc = sqlite3_reset(stmt_);
    if (rc != SQLITE_OK) {
        return Error{ErrorCode::DatabaseError, "Failed to reset statement"};
    }
    return {};
}

Result<void> Statement::clearBindings() {
    int rc = sqlite3_clear_bindings(stmt_);
    if (rc != SQLITE_OK) {
        return Error{ErrorCode::DatabaseError, "Failed to clear bindings"};
    }
    return {};
}

// Database implementation
Database::~Database() {
    close();
}

Database::Database(Database&& other) noexcept
    : db_(other.db_), path_(std::move(other.path_)), inTransaction_(other.inTransaction_) {
    other.db_ = nullptr;
    other.inTransaction_ = false;
}

Database& Database::operator=(Database&& other) noexcept {
    if (this != &other) {
        close();
        db_ = other.db_;
        path_ = std::move(other.path_);
        inTransaction_ = other.inTransaction_;
        other.db_ = nullptr;
        other.inTransaction_ = false;
    }
    return *this;
}

Result<void> Database::open(const std::string& path, ConnectionMode mode) {
    int flags = 0;
    switch (mode) {
        case ConnectionMode::ReadOnly:
            flags = SQLITE_OPEN_READONLY;
            break;
        case ConnectionMode::ReadWrite:
            flags = SQLITE_OPEN_READWRITE;
            break;
        case ConnectionMode::Create:
            flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
            break;
        case ConnectionMode::Memory:
            flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MEMORY;
            break;
    }

    int rc = sqlite3_open_v2(path.c_str(), &db_, flags, nullptr);
    if (rc != SQLITE_OK) {
        std::string error = db_ ? sqlite3_errmsg(db_) : "Unknown error";
        if (db_) {
            sqlite3_close(db_);
            db_ = nullptr;
        }
        return Error{ErrorCode::DatabaseError, "Failed to open database: " + error};
    }

    // Set busy timeout to avoid indefinite blocking
    sqlite3_busy_timeout(db_, 5000); // 5 second timeout

    path_ = path;
    return {};
}

void Database::close() {
    if (db_) {
        sqlite3_close(db_);
        db_ = nullptr;
    }
    path_.clear();
    inTransaction_ = false;
}

Result<Statement> Database::prepare(const std::string& sql) {
    if (!db_) {
        return Error{ErrorCode::InvalidState, "Database not open"};
    }

    try {
        return Statement(db_, sql);
    } catch (const std::exception& e) {
        return Error{ErrorCode::DatabaseError, e.what()};
    }
}

Result<void> Database::execute(const std::string& sql) {
    if (!db_) {
        return Error{ErrorCode::InvalidState, "Database not open"};
    }

    char* errMsg = nullptr;
    int rc = sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, &errMsg);
    if (rc != SQLITE_OK) {
        std::string error = errMsg ? errMsg : "Unknown error";
        sqlite3_free(errMsg);
        return Error{ErrorCode::DatabaseError, "Failed to execute SQL: " + error};
    }
    return {};
}

Result<void> Database::beginTransaction() {
    if (inTransaction_) {
        return Error{ErrorCode::InvalidState, "Already in transaction"};
    }

    auto result = execute("BEGIN");
    if (result) {
        inTransaction_ = true;
    }
    return result;
}

Result<void> Database::commit() {
    if (!inTransaction_) {
        return Error{ErrorCode::InvalidState, "Not in transaction"};
    }

    auto result = execute("COMMIT");
    if (result) {
        inTransaction_ = false;
    }
    return result;
}

Result<void> Database::rollback() {
    if (!inTransaction_) {
        return Error{ErrorCode::InvalidState, "Not in transaction"};
    }

    auto result = execute("ROLLBACK");
    inTransaction_ = false; // Always clear flag, even on error
    return result;
}

int64_t Database::lastInsertRowId() const {
    return db_ ? sqlite3_last_insert_rowid(db_) : 0;
}

int Database::changes() const {
    return db_ ? sqlite3_changes(db_) : 0;
}

Result<bool> Database::tableExists(const std::string& table) {
    auto stmtResult = prepare("SELECT COUNT(*) FROM sqlite_master "
                              "WHERE type='table' AND name=?");
    if (!stmtResult)
        return stmtResult.error();

    Statement stmt = std::move(stmtResult).value();
    auto bindResult = stmt.bind(1, table);
    if (!bindResult)
        return bindResult.error();

    auto stepResult = stmt.step();
    if (!stepResult)
        return stepResult.error();

    return stmt.getInt(0) > 0;
}

Result<bool> Database::hasFTS5() {
    auto stmtResult = prepare("SELECT sqlite_compileoption_used('ENABLE_FTS5')");
    if (!stmtResult)
        return stmtResult.error();

    Statement stmt = std::move(stmtResult).value();
    auto stepResult = stmt.step();
    if (!stepResult)
        return stepResult.error();

    return stepResult.value() && stmt.getInt(0) == 1;
}

Result<void> Database::setBusyTimeout(std::chrono::milliseconds timeout) {
    if (!db_) {
        return Error{ErrorCode::InvalidState, "Database not open"};
    }

    int rc = sqlite3_busy_timeout(db_, static_cast<int>(timeout.count()));
    if (rc != SQLITE_OK) {
        return Error{ErrorCode::DatabaseError, "Failed to set busy timeout"};
    }
    return {};
}

Result<void> Database::enableWAL() {
    return execute("PRAGMA journal_mode=WAL");
}

Result<void> Database::optimize() {
    return execute("PRAGMA optimize");
}

std::string Database::version() {
    return sqlite3_libversion();
}

DatabaseError Database::translateError(int sqliteError) {
    switch (sqliteError) {
        case SQLITE_BUSY:
            return DatabaseError::Busy;
        case SQLITE_LOCKED:
            return DatabaseError::Locked;
        case SQLITE_CORRUPT:
            return DatabaseError::Corrupt;
        case SQLITE_NOTFOUND:
            return DatabaseError::NotFound;
        case SQLITE_CONSTRAINT:
            return DatabaseError::ConstraintViolation;
        case SQLITE_MISMATCH:
            return DatabaseError::TypeMismatch;
        default:
            return DatabaseError::Unknown;
    }
}

std::string Database::getErrorMessage() const {
    return db_ ? sqlite3_errmsg(db_) : "No database connection";
}

// QueryBuilder implementation
QueryBuilder& QueryBuilder::select(const std::vector<std::string>& columns) {
    type_ = QueryType::Select;
    selectColumns_ = columns;
    return *this;
}

QueryBuilder& QueryBuilder::from(const std::string& table) {
    table_ = table;
    return *this;
}

QueryBuilder& QueryBuilder::where(const std::string& condition) {
    whereClauses_.clear();
    whereClauses_.push_back(condition);
    return *this;
}

QueryBuilder& QueryBuilder::andWhere(const std::string& condition) {
    whereClauses_.push_back("AND " + condition);
    return *this;
}

QueryBuilder& QueryBuilder::orWhere(const std::string& condition) {
    whereClauses_.push_back("OR " + condition);
    return *this;
}

QueryBuilder& QueryBuilder::join(const std::string& table, const std::string& on) {
    joinClauses_.push_back("JOIN " + table + " ON " + on);
    return *this;
}

QueryBuilder& QueryBuilder::leftJoin(const std::string& table, const std::string& on) {
    joinClauses_.push_back("LEFT JOIN " + table + " ON " + on);
    return *this;
}

QueryBuilder& QueryBuilder::orderBy(const std::string& column, bool ascending) {
    orderByClause_ = column + (ascending ? " ASC" : " DESC");
    return *this;
}

QueryBuilder& QueryBuilder::limit(int limit) {
    limit_ = limit;
    return *this;
}

QueryBuilder& QueryBuilder::offset(int offset) {
    offset_ = offset;
    return *this;
}

QueryBuilder& QueryBuilder::groupBy(const std::string& column) {
    groupByClause_ = column;
    return *this;
}

QueryBuilder& QueryBuilder::having(const std::string& condition) {
    havingClause_ = condition;
    return *this;
}

QueryBuilder& QueryBuilder::insertInto(const std::string& table) {
    type_ = QueryType::Insert;
    table_ = table;
    return *this;
}

QueryBuilder& QueryBuilder::values(const std::vector<std::string>& columns) {
    insertColumns_ = columns;
    return *this;
}

QueryBuilder& QueryBuilder::update(const std::string& table) {
    type_ = QueryType::Update;
    table_ = table;
    return *this;
}

QueryBuilder& QueryBuilder::set(const std::string& column, const std::string& placeholder) {
    setClauses_.push_back({column, placeholder});
    return *this;
}

QueryBuilder& QueryBuilder::deleteFrom(const std::string& table) {
    type_ = QueryType::Delete;
    table_ = table;
    return *this;
}

std::string QueryBuilder::build() const {
    std::stringstream sql;

    switch (type_) {
        case QueryType::Select: {
            sql << "SELECT ";
            if (selectColumns_.empty()) {
                sql << "*";
            } else {
                for (size_t i = 0; i < selectColumns_.size(); ++i) {
                    if (i > 0)
                        sql << ", ";
                    sql << selectColumns_[i];
                }
            }
            sql << " FROM " << table_;

            for (const auto& join : joinClauses_) {
                sql << " " << join;
            }

            if (!whereClauses_.empty()) {
                sql << " WHERE ";
                for (size_t i = 0; i < whereClauses_.size(); ++i) {
                    if (i > 0)
                        sql << " ";
                    sql << whereClauses_[i];
                }
            }

            if (!groupByClause_.empty()) {
                sql << " GROUP BY " << groupByClause_;
            }

            if (!havingClause_.empty()) {
                sql << " HAVING " << havingClause_;
            }

            if (!orderByClause_.empty()) {
                sql << " ORDER BY " << orderByClause_;
            }

            if (limit_ > 0) {
                sql << " LIMIT " << limit_;
            }

            if (offset_ > 0) {
                sql << " OFFSET " << offset_;
            }
            break;
        }

        case QueryType::Insert: {
            sql << "INSERT INTO " << table_;
            if (!insertColumns_.empty()) {
                sql << " (";
                for (size_t i = 0; i < insertColumns_.size(); ++i) {
                    if (i > 0)
                        sql << ", ";
                    sql << insertColumns_[i];
                }
                sql << ") VALUES (";
                for (size_t i = 0; i < insertColumns_.size(); ++i) {
                    if (i > 0)
                        sql << ", ";
                    sql << "?";
                }
                sql << ")";
            }
            break;
        }

        case QueryType::Update: {
            sql << "UPDATE " << table_ << " SET ";
            for (size_t i = 0; i < setClauses_.size(); ++i) {
                if (i > 0)
                    sql << ", ";
                sql << setClauses_[i].first << " = " << setClauses_[i].second;
            }

            if (!whereClauses_.empty()) {
                sql << " WHERE ";
                for (size_t i = 0; i < whereClauses_.size(); ++i) {
                    if (i > 0)
                        sql << " ";
                    sql << whereClauses_[i];
                }
            }
            break;
        }

        case QueryType::Delete: {
            sql << "DELETE FROM " << table_;

            if (!whereClauses_.empty()) {
                sql << " WHERE ";
                for (size_t i = 0; i < whereClauses_.size(); ++i) {
                    if (i > 0)
                        sql << " ";
                    sql << whereClauses_[i];
                }
            }
            break;
        }

        default:
            break;
    }

    return sql.str();
}

void QueryBuilder::reset() {
    type_ = QueryType::None;
    table_.clear();
    selectColumns_.clear();
    insertColumns_.clear();
    setClauses_.clear();
    whereClauses_.clear();
    joinClauses_.clear();
    groupByClause_.clear();
    havingClause_.clear();
    orderByClause_.clear();
    limit_ = -1;
    offset_ = -1;
}

} // namespace yams::metadata