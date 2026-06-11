#pragma once

#include <cstdint>
#include <filesystem>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>

struct sqlite3;
struct sqlite3_stmt;

namespace yams::storage {

class Statement {
public:
    Statement(sqlite3* db, const std::string& sql);
    ~Statement();

    Statement(const Statement&) = delete;
    Statement& operator=(const Statement&) = delete;
    Statement(Statement&& other) noexcept;
    Statement& operator=(Statement&& other) noexcept;

    void bind(int index, int value);
    void bind(int index, int64_t value);
    void bind(int index, const std::string& value);
    void bind(int index, std::string_view value);
    void bindNull(int index);

    bool step();
    void execute();
    void reset();

    int getInt(int col) const;
    int64_t getInt64(int col) const;
    std::string getString(int col) const;
    bool isNull(int col) const;
    int columnCount() const;

private:
    sqlite3_stmt* stmt_ = nullptr;
    sqlite3* db_ = nullptr;
};

class Database {
public:
    explicit Database(const std::filesystem::path& path);
    ~Database();

    Database(const Database&) = delete;
    Database& operator=(const Database&) = delete;

    void execute(const std::string& sql);
    void executeFile(const std::filesystem::path& path);
    Statement prepare(const std::string& sql);

    void beginTransaction();
    void commit();
    void rollback();

    int64_t lastInsertRowId() const;
    int changes() const;
    void checkpoint();
    void vacuum();
    void analyze();
    void backup(const std::filesystem::path& destPath);
    bool checkIntegrity();
    std::string getVersion() const;

private:
    sqlite3* db_ = nullptr;
    std::string path_;
};

class StatementCache {
public:
    explicit StatementCache(Database& db);

    Statement get(const std::string& key, const std::string& sql);
    void clear();

private:
    Database& db_;
    std::unordered_map<std::string, std::string> sqlCache_;
    mutable std::mutex mutex_;
};

} // namespace yams::storage
