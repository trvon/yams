// SPDX-License-Identifier: GPL-3.0-or-later
#include "sweep_store.h"

#include <sqlite3.h>

#include <array>
#include <limits>
#include <new>
#include <string>
#include <utility>

namespace yams::memory_sync::detail {
namespace {

constexpr std::uint64_t kSchemaFloorBytes = std::uint64_t{2} * 1024 * 1024;
constexpr std::uint64_t kHardDiskCapBytes = std::uint64_t{1} * 1024 * 1024 * 1024;
constexpr int kProgressOpcodes = 1000;

ErrorCode errorCodeForSqlite(int rc) noexcept {
    switch (rc & 0xff) {
        case SQLITE_FULL:
        case SQLITE_NOMEM:
        case SQLITE_TOOBIG:
            return ErrorCode::ResourceExhausted;
        case SQLITE_IOERR:
        case SQLITE_CANTOPEN:
            return ErrorCode::IOError;
        case SQLITE_INTERRUPT:
            return ErrorCode::OperationCancelled;
        default:
            return ErrorCode::DatabaseError;
    }
}

Error openError(sqlite3* db, int rc, std::string_view context) {
    std::string message(context);
    message.append(": ");
    message.append(db != nullptr ? sqlite3_errmsg(db) : sqlite3_errstr(rc));
    message.append(" (sqlite rc=");
    message.append(std::to_string(rc));
    message.push_back(')');
    return Error{errorCodeForSqlite(rc), std::move(message)};
}

Result<void> initialCheck(const SweepStore::Check& check) {
    if (!check) {
        return {};
    }
    try {
        return check();
    } catch (const std::bad_alloc&) {
        return Error{ErrorCode::ResourceExhausted,
                     "memory sync sweep admission check ran out of memory"};
    } catch (const std::exception& error) {
        return Error{ErrorCode::InternalError,
                     std::string("memory sync sweep admission check threw: ") + error.what()};
    } catch (...) {
        return Error{ErrorCode::InternalError,
                     "memory sync sweep admission check threw an unknown exception"};
    }
}

class Statement final {
public:
    Statement() = default;
    ~Statement() {
        if (stmt_ != nullptr) {
            sqlite3_finalize(stmt_);
        }
    }

    Statement(const Statement&) = delete;
    Statement& operator=(const Statement&) = delete;
    Statement(Statement&&) = delete;
    Statement& operator=(Statement&&) = delete;

    sqlite3_stmt** out() noexcept { return &stmt_; }
    sqlite3_stmt* get() const noexcept { return stmt_; }

    int finalize() noexcept {
        sqlite3_stmt* stmt = std::exchange(stmt_, nullptr);
        return stmt != nullptr ? sqlite3_finalize(stmt) : SQLITE_OK;
    }

private:
    sqlite3_stmt* stmt_{nullptr};
};

int bindBlob(sqlite3_stmt* stmt, int index, std::string_view value) noexcept {
    static constexpr unsigned char kEmpty = 0;
    const void* data =
        value.empty() ? static_cast<const void*>(&kEmpty) : static_cast<const void*>(value.data());
    return sqlite3_bind_blob64(stmt, index, data, static_cast<sqlite3_uint64>(value.size()),
                               SQLITE_TRANSIENT);
}

int bindBlob(sqlite3_stmt* stmt, int index, std::span<const std::byte> value) noexcept {
    static constexpr std::byte kEmpty{};
    const void* data =
        value.empty() ? static_cast<const void*>(&kEmpty) : static_cast<const void*>(value.data());
    return sqlite3_bind_blob64(stmt, index, data, static_cast<sqlite3_uint64>(value.size()),
                               SQLITE_TRANSIENT);
}

std::array<std::byte, 8> encodeCounter(std::uint64_t value) noexcept {
    std::array<std::byte, 8> encoded{};
    for (std::size_t i = 0; i < encoded.size(); ++i) {
        const auto shift = static_cast<unsigned>((encoded.size() - i - 1) * 8);
        encoded[i] = static_cast<std::byte>((value >> shift) & 0xffU);
    }
    return encoded;
}

std::string_view columnStringView(sqlite3_stmt* stmt, int column) noexcept {
    const int size = sqlite3_column_bytes(stmt, column);
    if (size == 0) {
        return {};
    }
    const auto* data = static_cast<const char*>(sqlite3_column_blob(stmt, column));
    return {data, static_cast<std::size_t>(size)};
}

std::span<const std::byte> columnBytes(sqlite3_stmt* stmt, int column) noexcept {
    const int size = sqlite3_column_bytes(stmt, column);
    if (size == 0) {
        return {};
    }
    const auto* data = static_cast<const std::byte*>(sqlite3_column_blob(stmt, column));
    return {data, static_cast<std::size_t>(size)};
}

} // namespace

struct SweepStore::Impl final {
    enum class ProgressFailure : unsigned char {
        None,
        CheckError,
        OutOfMemory,
        Exception,
        Unknown
    };

    explicit Impl(sqlite3* connection, Check callback)
        : db(connection), check(std::move(callback)) {
        // One stable handler avoids nested lookup calls uninstalling an outer scan's handler.
        sqlite3_progress_handler(db, kProgressOpcodes, &Impl::progress, this);
    }

    ~Impl() noexcept {
        if (db == nullptr) {
            return;
        }
        // Per-call statements should already be gone. Finalize defensively before close so
        // SQLite can delete the unnamed temporary main database immediately.
        while (sqlite3_stmt* stmt = sqlite3_next_stmt(db, nullptr)) {
            sqlite3_finalize(stmt);
        }
        sqlite3_progress_handler(db, 0, nullptr, nullptr);
        sqlite3_close_v2(db);
        db = nullptr;
    }

    Impl(const Impl&) = delete;
    Impl& operator=(const Impl&) = delete;

    static int progress(void* raw) noexcept {
        auto& self = *static_cast<Impl*>(raw);
        if (self.progressFailure != ProgressFailure::None) {
            return 1;
        }
        if (!self.check) {
            return 0;
        }
        try {
            auto checked = self.check();
            if (checked) {
                return 0;
            }
            try {
                self.progressError = checked.error();
                self.progressFailure = ProgressFailure::CheckError;
            } catch (...) {
                self.progressFailure = ProgressFailure::OutOfMemory;
            }
        } catch (const std::bad_alloc&) {
            self.progressFailure = ProgressFailure::OutOfMemory;
        } catch (const std::exception& error) {
            try {
                self.progressError =
                    Error{ErrorCode::InternalError,
                          std::string("memory sync sweep admission check threw: ") + error.what()};
                self.progressFailure = ProgressFailure::CheckError;
            } catch (...) {
                self.progressFailure = ProgressFailure::Exception;
            }
        } catch (...) {
            self.progressFailure = ProgressFailure::Unknown;
        }
        return 1;
    }

    Error takeProgressError() {
        Error error;
        switch (progressFailure) {
            case ProgressFailure::CheckError:
                if (progressError) {
                    error = std::move(*progressError);
                } else {
                    error = Error{ErrorCode::OperationCancelled,
                                  "memory sync sweep interrupted by its admission check"};
                }
                break;
            case ProgressFailure::OutOfMemory:
                error = Error{ErrorCode::ResourceExhausted,
                              "memory sync sweep admission check ran out of memory"};
                break;
            case ProgressFailure::Exception:
                error = Error{ErrorCode::InternalError,
                              "memory sync sweep admission check threw an exception"};
                break;
            case ProgressFailure::Unknown:
                error = Error{ErrorCode::InternalError,
                              "memory sync sweep admission check threw an unknown exception"};
                break;
            case ProgressFailure::None:
                error = Error{ErrorCode::OperationCancelled,
                              "memory sync sweep SQLite work was interrupted"};
                break;
        }
        progressError.reset();
        progressFailure = ProgressFailure::None;
        return error;
    }

    Error sqliteError(int rc, std::string_view context) {
        if ((rc & 0xff) == SQLITE_INTERRUPT && progressFailure != ProgressFailure::None) {
            return takeProgressError();
        }
        std::string message(context);
        message.append(": ");
        if ((rc & 0xff) == SQLITE_FULL) {
            message.append("disposable sweep staging reached its configured disk bound: ");
        } else if ((rc & 0xff) == SQLITE_NOMEM) {
            message.append("SQLite allocation failed: ");
        }
        message.append(sqlite3_errmsg(db));
        message.append(" (sqlite rc=");
        message.append(std::to_string(rc));
        message.append(", extended=");
        message.append(std::to_string(sqlite3_extended_errcode(db)));
        message.push_back(')');
        return Error{errorCodeForSqlite(rc), std::move(message)};
    }

    Error poison(Error error) {
        if (!failure) {
            failure = error;
        }
        return error;
    }

    Result<void> ready() {
        if (failure) {
            return *failure;
        }
        if (progressFailure != ProgressFailure::None) {
            return poison(takeProgressError());
        }
        if (!check) {
            return {};
        }
        try {
            auto checked = check();
            if (!checked) {
                return poison(checked.error());
            }
            return {};
        } catch (const std::bad_alloc&) {
            return poison(Error{ErrorCode::ResourceExhausted,
                                "memory sync sweep admission check ran out of memory"});
        } catch (const std::exception& error) {
            return poison(
                Error{ErrorCode::InternalError,
                      std::string("memory sync sweep admission check threw: ") + error.what()});
        } catch (...) {
            return poison(Error{ErrorCode::InternalError,
                                "memory sync sweep admission check threw an unknown exception"});
        }
    }

    Error failAfterFinalize(Statement& stmt, Error error, std::string_view context) {
        const int finalizeRc = stmt.finalize();
        if (finalizeRc != SQLITE_OK) {
            error.message.append("; ");
            error.message.append(context);
            error.message.append(" finalize failed: ");
            error.message.append(sqlite3_errmsg(db));
            error.message.append(" (sqlite rc=");
            error.message.append(std::to_string(finalizeRc));
            error.message.push_back(')');
        }
        return poison(std::move(error));
    }

    Result<void> finish(Statement& stmt, std::string_view context) {
        const int rc = stmt.finalize();
        if (rc != SQLITE_OK) {
            return poison(sqliteError(rc, context));
        }
        return {};
    }

    Result<void> prepare(const char* sql, Statement& stmt, std::string_view context) {
        if (auto checked = ready(); !checked) {
            return checked.error();
        }
        const int rc = sqlite3_prepare_v2(db, sql, -1, stmt.out(), nullptr);
        if (rc != SQLITE_OK) {
            return failAfterFinalize(stmt, sqliteError(rc, context), context);
        }
        return {};
    }

    Result<void> exec(const char* sql, std::string_view context) {
        Statement stmt;
        if (auto prepared = prepare(sql, stmt, context); !prepared) {
            return prepared.error();
        }
        for (;;) {
            if (auto checked = ready(); !checked) {
                return failAfterFinalize(stmt, checked.error(), context);
            }
            const int rc = sqlite3_step(stmt.get());
            if (rc == SQLITE_ROW) {
                continue;
            }
            if (rc == SQLITE_DONE) {
                return finish(stmt, context);
            }
            return failAfterFinalize(stmt, sqliteError(rc, context), context);
        }
    }

    Result<sqlite3_int64> scalarInt64(const char* sql, std::string_view context) {
        Statement stmt;
        if (auto prepared = prepare(sql, stmt, context); !prepared) {
            return prepared.error();
        }
        if (auto checked = ready(); !checked) {
            return failAfterFinalize(stmt, checked.error(), context);
        }
        int rc = sqlite3_step(stmt.get());
        if (rc != SQLITE_ROW) {
            Error error = rc == SQLITE_DONE
                              ? Error{ErrorCode::DatabaseError,
                                      std::string(context) + ": query returned no row"}
                              : sqliteError(rc, context);
            return failAfterFinalize(stmt, std::move(error), context);
        }
        if (sqlite3_column_type(stmt.get(), 0) != SQLITE_INTEGER) {
            return failAfterFinalize(
                stmt,
                Error{ErrorCode::DatabaseError,
                      std::string(context) + ": query returned a non-integer value"},
                context);
        }
        const sqlite3_int64 value = sqlite3_column_int64(stmt.get(), 0);
        if (auto checked = ready(); !checked) {
            return failAfterFinalize(stmt, checked.error(), context);
        }
        rc = sqlite3_step(stmt.get());
        if (rc != SQLITE_DONE) {
            Error error = rc == SQLITE_ROW
                              ? Error{ErrorCode::DatabaseError,
                                      std::string(context) + ": query returned multiple rows"}
                              : sqliteError(rc, context);
            return failAfterFinalize(stmt, std::move(error), context);
        }
        if (auto finalized = finish(stmt, context); !finalized) {
            return finalized.error();
        }
        return value;
    }

    sqlite3* db{nullptr};
    Check check;
    std::optional<Error> failure;
    std::optional<Error> progressError;
    ProgressFailure progressFailure{ProgressFailure::None};
};

SweepStore::SweepStore(std::unique_ptr<Impl> impl) noexcept : impl_(std::move(impl)) {}

SweepStore::~SweepStore() = default;

Result<std::shared_ptr<SweepStore>> SweepStore::create(std::size_t inputBudget, Check check) {
    if (auto checked = initialCheck(check); !checked) {
        return checked.error();
    }

    sqlite3* db = nullptr;
    constexpr int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_PRIVATECACHE |
                          SQLITE_OPEN_FULLMUTEX;
    int rc = sqlite3_open_v2("", &db, flags, nullptr);
    if (rc != SQLITE_OK) {
        Error error = openError(db, rc, "open disposable memory sync sweep staging");
        if (db != nullptr) {
            sqlite3_close_v2(db);
        }
        return error;
    }

    rc = sqlite3_extended_result_codes(db, 1);
    if (rc == SQLITE_OK) {
        rc = sqlite3_db_config(db, SQLITE_DBCONFIG_LOOKASIDE, nullptr, 0, 0);
    }
    if (rc != SQLITE_OK) {
        Error error = openError(db, rc, "configure disposable memory sync sweep staging");
        sqlite3_close_v2(db);
        return error;
    }

    std::unique_ptr<Impl> impl;
    try {
        impl = std::make_unique<Impl>(db, std::move(check));
        db = nullptr;

        // The empty filename asks SQLite's VFS for a private DELETEONCLOSE temporary main DB;
        // it has no persistent staging/migration name. FILE temp storage and the main DB may
        // still reside on tmpfs, so this is a bounded private-disk policy, not a physical-RAM
        // or process-RSS guarantee.
        constexpr std::array<std::pair<const char*, const char*>, 6> pragmas{{
            {"PRAGMA temp_store=FILE", "set sweep temporary storage mode"},
            {"PRAGMA journal_mode=OFF", "disable disposable sweep journal"},
            {"PRAGMA synchronous=OFF", "disable disposable sweep synchronization"},
            {"PRAGMA mmap_size=0", "disable sweep memory mapping"},
            {"PRAGMA cache_size=-512", "bound sweep SQLite page cache"},
            {"PRAGMA page_size=4096", "set sweep database page size"},
        }};
        for (const auto& [sql, context] : pragmas) {
            if (auto configured = impl->exec(sql, context); !configured) {
                return configured.error();
            }
        }

        auto pageSizeResult = impl->scalarInt64("PRAGMA page_size", "read sweep page size");
        if (!pageSizeResult) {
            return pageSizeResult.error();
        }
        const sqlite3_int64 signedPageSize = pageSizeResult.value();
        if (signedPageSize <= 0) {
            return impl->poison(
                Error{ErrorCode::DatabaseError, "SQLite reported an invalid sweep page size"});
        }
        const auto pageSize = static_cast<std::uint64_t>(signedPageSize);

        const std::uint64_t input = static_cast<std::uint64_t>(inputBudget);
        constexpr std::uint64_t maxScaledInput = (kHardDiskCapBytes - kSchemaFloorBytes) / 16;
        const std::uint64_t diskCap =
            input > maxScaledInput ? kHardDiskCapBytes : kSchemaFloorBytes + input * 16;
        const std::uint64_t maxPages = diskCap / pageSize;
        if (maxPages == 0 ||
            maxPages > static_cast<std::uint64_t>(std::numeric_limits<sqlite3_int64>::max())) {
            return impl->poison(
                Error{ErrorCode::DatabaseError, "cannot derive a valid sweep database page bound"});
        }
        const std::string setMaxPages = "PRAGMA max_page_count=" + std::to_string(maxPages);
        auto actualPagesResult =
            impl->scalarInt64(setMaxPages.c_str(), "set sweep database page bound");
        if (!actualPagesResult) {
            return actualPagesResult.error();
        }
        if (actualPagesResult.value() <= 0 ||
            static_cast<std::uint64_t>(actualPagesResult.value()) > maxPages) {
            return impl->poison(Error{ErrorCode::DatabaseError,
                                      "SQLite did not honor the sweep database page bound"});
        }

        // Primary keys and the history-order index are all created while the tables are empty.
        // The two forEach plans can therefore stream directly from an index without temp sorts.
        constexpr std::array<std::pair<const char*, const char*>, 8> schema{{
            {"CREATE TABLE operations("
             "operation_id BLOB PRIMARY KEY,"
             "record_hash BLOB NOT NULL,"
             "logical_key BLOB NOT NULL,"
             "fork INTEGER NOT NULL DEFAULT 0 CHECK(fork IN (0,1))) WITHOUT ROWID",
             "create sweep operations table"},
            {"CREATE TABLE vectors("
             "identity BLOB PRIMARY KEY,"
             "first_hash BLOB NOT NULL,"
             "first_operation_id BLOB NOT NULL) WITHOUT ROWID",
             "create sweep vectors table"},
            {"CREATE TABLE forks("
             "operation_id BLOB PRIMARY KEY) WITHOUT ROWID",
             "create sweep forks table"},
            {"CREATE TABLE candidates("
             "sequence INTEGER PRIMARY KEY,"
             "index_key BLOB NOT NULL,"
             "logical_key BLOB NOT NULL,"
             "record_hash BLOB NOT NULL,"
             "writer BLOB NOT NULL,"
             "counter BLOB NOT NULL CHECK(length(counter)=8),"
             "encoded BLOB NOT NULL)",
             "create sweep candidates table"},
            {"CREATE INDEX candidates_history_order "
             "ON candidates(writer,counter,record_hash,sequence)",
             "create sweep history order index"},
            {"CREATE TRIGGER operations_insert_fork AFTER INSERT ON operations "
             "WHEN NEW.fork=1 BEGIN "
             "INSERT OR IGNORE INTO forks(operation_id) VALUES(NEW.operation_id); END",
             "create sweep operation insert fork trigger"},
            {"CREATE TRIGGER operations_update_fork AFTER UPDATE OF fork ON operations "
             "WHEN NEW.fork=1 BEGIN "
             "INSERT OR IGNORE INTO forks(operation_id) VALUES(NEW.operation_id); END",
             "create sweep operation update fork trigger"},
            {"PRAGMA query_only=OFF", "confirm writable sweep staging"},
        }};
        for (const auto& [sql, context] : schema) {
            if (auto created = impl->exec(sql, context); !created) {
                return created.error();
            }
        }

        return std::shared_ptr<SweepStore>(new SweepStore(std::move(impl)));
    } catch (const std::bad_alloc&) {
        if (db != nullptr) {
            sqlite3_close_v2(db);
        }
        return Error{ErrorCode::ResourceExhausted,
                     "allocating disposable memory sync sweep staging failed"};
    } catch (const std::exception& error) {
        if (db != nullptr) {
            sqlite3_close_v2(db);
        }
        return Error{ErrorCode::InternalError,
                     std::string("initializing disposable memory sync sweep staging failed: ") +
                         error.what()};
    } catch (...) {
        if (db != nullptr) {
            sqlite3_close_v2(db);
        }
        return Error{ErrorCode::InternalError,
                     "initializing disposable memory sync sweep staging failed"};
    }
}

Result<void> SweepStore::observeOperation(std::string_view operationId, std::string_view recordHash,
                                          std::string_view logicalKey) {
    static constexpr const char* sql =
        "INSERT INTO operations(operation_id,record_hash,logical_key,fork) "
        "VALUES(?1,?2,?3,EXISTS(SELECT 1 FROM forks WHERE operation_id=?1)) "
        "ON CONFLICT(operation_id) DO UPDATE SET fork=CASE "
        "WHEN operations.fork<>0 "
        "OR operations.record_hash IS NOT excluded.record_hash "
        "OR operations.logical_key IS NOT excluded.logical_key "
        "THEN 1 ELSE 0 END";
    Statement stmt;
    constexpr std::string_view context = "observe sweep operation fingerprint";
    if (auto prepared = impl_->prepare(sql, stmt, context); !prepared) {
        return prepared.error();
    }
    int rc = bindBlob(stmt.get(), 1, operationId);
    if (rc == SQLITE_OK) {
        rc = bindBlob(stmt.get(), 2, recordHash);
    }
    if (rc == SQLITE_OK) {
        rc = bindBlob(stmt.get(), 3, logicalKey);
    }
    if (rc != SQLITE_OK) {
        return impl_->failAfterFinalize(stmt, impl_->sqliteError(rc, context), context);
    }
    if (auto checked = impl_->ready(); !checked) {
        return impl_->failAfterFinalize(stmt, checked.error(), context);
    }
    rc = sqlite3_step(stmt.get());
    if (rc != SQLITE_DONE) {
        return impl_->failAfterFinalize(stmt, impl_->sqliteError(rc, context), context);
    }
    return impl_->finish(stmt, context);
}

Result<void> SweepStore::observeVector(std::string_view identity, std::string_view recordHash,
                                       std::string_view operationId) {
    constexpr std::string_view insertContext = "observe exact sweep vector identity";
    Statement insert;
    if (auto prepared =
            impl_->prepare("INSERT OR IGNORE INTO vectors(identity,first_hash,first_operation_id) "
                           "VALUES(?1,?2,?3)",
                           insert, insertContext);
        !prepared) {
        return prepared.error();
    }
    int rc = bindBlob(insert.get(), 1, identity);
    if (rc == SQLITE_OK) {
        rc = bindBlob(insert.get(), 2, recordHash);
    }
    if (rc == SQLITE_OK) {
        rc = bindBlob(insert.get(), 3, operationId);
    }
    if (rc != SQLITE_OK) {
        return impl_->failAfterFinalize(insert, impl_->sqliteError(rc, insertContext),
                                        insertContext);
    }
    if (auto checked = impl_->ready(); !checked) {
        return impl_->failAfterFinalize(insert, checked.error(), insertContext);
    }
    rc = sqlite3_step(insert.get());
    if (rc != SQLITE_DONE) {
        return impl_->failAfterFinalize(insert, impl_->sqliteError(rc, insertContext),
                                        insertContext);
    }
    if (auto finalized = impl_->finish(insert, insertContext); !finalized) {
        return finalized.error();
    }

    constexpr std::string_view selectContext = "read first sweep vector observation";
    Statement select;
    if (auto prepared =
            impl_->prepare("SELECT first_hash,first_operation_id FROM vectors WHERE identity=?1",
                           select, selectContext);
        !prepared) {
        return prepared.error();
    }
    rc = bindBlob(select.get(), 1, identity);
    if (rc != SQLITE_OK) {
        return impl_->failAfterFinalize(select, impl_->sqliteError(rc, selectContext),
                                        selectContext);
    }
    if (auto checked = impl_->ready(); !checked) {
        return impl_->failAfterFinalize(select, checked.error(), selectContext);
    }
    rc = sqlite3_step(select.get());
    if (rc != SQLITE_ROW) {
        Error error = rc == SQLITE_DONE
                          ? Error{ErrorCode::DatabaseError,
                                  "vector observation disappeared from disposable sweep staging"}
                          : impl_->sqliteError(rc, selectContext);
        return impl_->failAfterFinalize(select, std::move(error), selectContext);
    }
    if (sqlite3_column_type(select.get(), 0) != SQLITE_BLOB ||
        sqlite3_column_type(select.get(), 1) != SQLITE_BLOB) {
        return impl_->failAfterFinalize(
            select,
            Error{ErrorCode::DatabaseError,
                  "vector observation has invalid types in disposable sweep staging"},
            selectContext);
    }

    std::string firstHash;
    std::string firstOperation;
    try {
        firstHash.assign(columnStringView(select.get(), 0));
        firstOperation.assign(columnStringView(select.get(), 1));
    } catch (const std::bad_alloc&) {
        return impl_->failAfterFinalize(
            select,
            Error{ErrorCode::ResourceExhausted, "allocating a sweep vector observation failed"},
            selectContext);
    }
    if (auto checked = impl_->ready(); !checked) {
        return impl_->failAfterFinalize(select, checked.error(), selectContext);
    }
    rc = sqlite3_step(select.get());
    if (rc != SQLITE_DONE) {
        Error error = rc == SQLITE_ROW ? Error{ErrorCode::DatabaseError,
                                               "duplicate exact vector identities in sweep staging"}
                                       : impl_->sqliteError(rc, selectContext);
        return impl_->failAfterFinalize(select, std::move(error), selectContext);
    }
    if (auto finalized = impl_->finish(select, selectContext); !finalized) {
        return finalized.error();
    }

    if (firstHash != recordHash) {
        if (auto marked = markFork(firstOperation); !marked) {
            return marked.error();
        }
        if (auto marked = markFork(operationId); !marked) {
            return marked.error();
        }
    }
    return {};
}

Result<void> SweepStore::markFork(std::string_view operationId) {
    constexpr std::string_view insertContext = "mark sweep fork identity";
    Statement insert;
    if (auto prepared = impl_->prepare("INSERT OR IGNORE INTO forks(operation_id) VALUES(?1)",
                                       insert, insertContext);
        !prepared) {
        return prepared.error();
    }
    int rc = bindBlob(insert.get(), 1, operationId);
    if (rc != SQLITE_OK) {
        return impl_->failAfterFinalize(insert, impl_->sqliteError(rc, insertContext),
                                        insertContext);
    }
    if (auto checked = impl_->ready(); !checked) {
        return impl_->failAfterFinalize(insert, checked.error(), insertContext);
    }
    rc = sqlite3_step(insert.get());
    if (rc != SQLITE_DONE) {
        return impl_->failAfterFinalize(insert, impl_->sqliteError(rc, insertContext),
                                        insertContext);
    }
    if (auto finalized = impl_->finish(insert, insertContext); !finalized) {
        return finalized.error();
    }

    constexpr std::string_view updateContext = "attach sweep fork to operation";
    Statement update;
    if (auto prepared = impl_->prepare("UPDATE operations SET fork=1 WHERE operation_id=?1", update,
                                       updateContext);
        !prepared) {
        return prepared.error();
    }
    rc = bindBlob(update.get(), 1, operationId);
    if (rc != SQLITE_OK) {
        return impl_->failAfterFinalize(update, impl_->sqliteError(rc, updateContext),
                                        updateContext);
    }
    if (auto checked = impl_->ready(); !checked) {
        return impl_->failAfterFinalize(update, checked.error(), updateContext);
    }
    rc = sqlite3_step(update.get());
    if (rc != SQLITE_DONE) {
        return impl_->failAfterFinalize(update, impl_->sqliteError(rc, updateContext),
                                        updateContext);
    }
    return impl_->finish(update, updateContext);
}

Result<bool> SweepStore::forked(std::string_view operationId) {
    constexpr std::string_view context = "query sweep fork identity";
    Statement stmt;
    if (auto prepared =
            impl_->prepare("SELECT 1 FROM forks WHERE operation_id=?1 LIMIT 1", stmt, context);
        !prepared) {
        return prepared.error();
    }
    int rc = bindBlob(stmt.get(), 1, operationId);
    if (rc != SQLITE_OK) {
        return impl_->failAfterFinalize(stmt, impl_->sqliteError(rc, context), context);
    }
    if (auto checked = impl_->ready(); !checked) {
        return impl_->failAfterFinalize(stmt, checked.error(), context);
    }
    rc = sqlite3_step(stmt.get());
    const bool found = rc == SQLITE_ROW;
    if (!found && rc != SQLITE_DONE) {
        return impl_->failAfterFinalize(stmt, impl_->sqliteError(rc, context), context);
    }
    if (found) {
        if (auto checked = impl_->ready(); !checked) {
            return impl_->failAfterFinalize(stmt, checked.error(), context);
        }
        rc = sqlite3_step(stmt.get());
        if (rc != SQLITE_DONE) {
            Error error = rc == SQLITE_ROW ? Error{ErrorCode::DatabaseError,
                                                   "duplicate fork identities in sweep staging"}
                                           : impl_->sqliteError(rc, context);
            return impl_->failAfterFinalize(stmt, std::move(error), context);
        }
    }
    if (auto finalized = impl_->finish(stmt, context); !finalized) {
        return finalized.error();
    }
    return found;
}

Result<std::optional<SweepStore::Fingerprint>> SweepStore::operation(std::string_view operationId) {
    constexpr std::string_view context = "query sweep operation fingerprint";
    Statement stmt;
    if (auto prepared = impl_->prepare(
            "SELECT record_hash,logical_key FROM operations WHERE operation_id=?1", stmt, context);
        !prepared) {
        return prepared.error();
    }
    int rc = bindBlob(stmt.get(), 1, operationId);
    if (rc != SQLITE_OK) {
        return impl_->failAfterFinalize(stmt, impl_->sqliteError(rc, context), context);
    }
    if (auto checked = impl_->ready(); !checked) {
        return impl_->failAfterFinalize(stmt, checked.error(), context);
    }
    rc = sqlite3_step(stmt.get());
    if (rc == SQLITE_DONE) {
        if (auto finalized = impl_->finish(stmt, context); !finalized) {
            return finalized.error();
        }
        return std::optional<Fingerprint>{};
    }
    if (rc != SQLITE_ROW) {
        return impl_->failAfterFinalize(stmt, impl_->sqliteError(rc, context), context);
    }
    if (sqlite3_column_type(stmt.get(), 0) != SQLITE_BLOB ||
        sqlite3_column_type(stmt.get(), 1) != SQLITE_BLOB) {
        return impl_->failAfterFinalize(
            stmt,
            Error{ErrorCode::DatabaseError,
                  "operation fingerprint has invalid types in disposable sweep staging"},
            context);
    }

    Fingerprint fingerprint;
    try {
        fingerprint.first.assign(columnStringView(stmt.get(), 0));
        fingerprint.second.assign(columnStringView(stmt.get(), 1));
    } catch (const std::bad_alloc&) {
        return impl_->failAfterFinalize(
            stmt,
            Error{ErrorCode::ResourceExhausted, "allocating a sweep operation fingerprint failed"},
            context);
    }
    if (auto checked = impl_->ready(); !checked) {
        return impl_->failAfterFinalize(stmt, checked.error(), context);
    }
    rc = sqlite3_step(stmt.get());
    if (rc != SQLITE_DONE) {
        Error error = rc == SQLITE_ROW ? Error{ErrorCode::DatabaseError,
                                               "duplicate operation identities in sweep staging"}
                                       : impl_->sqliteError(rc, context);
        return impl_->failAfterFinalize(stmt, std::move(error), context);
    }
    if (auto finalized = impl_->finish(stmt, context); !finalized) {
        return finalized.error();
    }
    return std::optional<Fingerprint>{std::move(fingerprint)};
}

Result<void> SweepStore::addCandidate(std::string_view indexKey, std::string_view logicalKey,
                                      std::string_view recordHash, std::string_view writer,
                                      std::uint64_t counter, std::span<const std::byte> encoded) {
    constexpr std::string_view context = "stage memory sync sweep candidate";
    Statement stmt;
    if (auto prepared = impl_->prepare(
            "INSERT INTO candidates(index_key,logical_key,record_hash,writer,counter,encoded) "
            "VALUES(?1,?2,?3,?4,?5,?6)",
            stmt, context);
        !prepared) {
        return prepared.error();
    }
    const auto encodedCounter = encodeCounter(counter);
    int rc = bindBlob(stmt.get(), 1, indexKey);
    if (rc == SQLITE_OK) {
        rc = bindBlob(stmt.get(), 2, logicalKey);
    }
    if (rc == SQLITE_OK) {
        rc = bindBlob(stmt.get(), 3, recordHash);
    }
    if (rc == SQLITE_OK) {
        rc = bindBlob(stmt.get(), 4, writer);
    }
    if (rc == SQLITE_OK) {
        rc = bindBlob(stmt.get(), 5,
                      std::span<const std::byte>{encodedCounter.data(), encodedCounter.size()});
    }
    if (rc == SQLITE_OK) {
        rc = bindBlob(stmt.get(), 6, encoded);
    }
    if (rc != SQLITE_OK) {
        return impl_->failAfterFinalize(stmt, impl_->sqliteError(rc, context), context);
    }
    if (auto checked = impl_->ready(); !checked) {
        return impl_->failAfterFinalize(stmt, checked.error(), context);
    }
    rc = sqlite3_step(stmt.get());
    if (rc != SQLITE_DONE) {
        return impl_->failAfterFinalize(stmt, impl_->sqliteError(rc, context), context);
    }
    return impl_->finish(stmt, context);
}

Result<void> SweepStore::forEach(bool historyOrder, const Visitor& visitor) {
    constexpr std::string_view context = "stream memory sync sweep candidates";
    if (!visitor) {
        return impl_->poison(Error{ErrorCode::InvalidArgument, "sweep candidate visitor is empty"});
    }
    static constexpr const char* historySql = "SELECT index_key,logical_key,record_hash,encoded "
                                              "FROM candidates INDEXED BY candidates_history_order "
                                              "ORDER BY writer,counter,record_hash,sequence";
    static constexpr const char* sequenceSql = "SELECT index_key,logical_key,record_hash,encoded "
                                               "FROM candidates NOT INDEXED ORDER BY sequence";

    Statement stmt;
    if (auto prepared = impl_->prepare(historyOrder ? historySql : sequenceSql, stmt, context);
        !prepared) {
        return prepared.error();
    }
    for (;;) {
        if (auto checked = impl_->ready(); !checked) {
            return impl_->failAfterFinalize(stmt, checked.error(), context);
        }
        const int rc = sqlite3_step(stmt.get());
        if (rc == SQLITE_DONE) {
            return impl_->finish(stmt, context);
        }
        if (rc != SQLITE_ROW) {
            return impl_->failAfterFinalize(stmt, impl_->sqliteError(rc, context), context);
        }
        if (auto checked = impl_->ready(); !checked) {
            return impl_->failAfterFinalize(stmt, checked.error(), context);
        }
        for (int column = 0; column < 4; ++column) {
            if (sqlite3_column_type(stmt.get(), column) != SQLITE_BLOB) {
                return impl_->failAfterFinalize(
                    stmt,
                    Error{ErrorCode::DatabaseError,
                          "candidate has invalid types in disposable sweep staging"},
                    context);
            }
        }

        Result<void> visited;
        try {
            visited = visitor(columnStringView(stmt.get(), 0), columnStringView(stmt.get(), 1),
                              columnStringView(stmt.get(), 2), columnBytes(stmt.get(), 3));
        } catch (const std::bad_alloc&) {
            visited =
                Error{ErrorCode::ResourceExhausted, "sweep candidate visitor ran out of memory"};
        } catch (const std::exception& error) {
            visited = Error{ErrorCode::InternalError,
                            std::string("sweep candidate visitor threw: ") + error.what()};
        } catch (...) {
            visited = Error{ErrorCode::InternalError,
                            "sweep candidate visitor threw an unknown exception"};
        }
        if (!visited) {
            return impl_->failAfterFinalize(stmt, visited.error(), context);
        }
    }
}

} // namespace yams::memory_sync::detail
