#include <yams/daemon/components/sqlite_vacuum.h>

#include <sqlite3.h>

#include <limits>
#include <optional>
#include <system_error>

namespace yams::daemon {

namespace {

class ConnectionGuard {
public:
    explicit ConnectionGuard(sqlite3* db) noexcept : db_(db) {}
    ~ConnectionGuard() {
        if (db_ != nullptr) {
            sqlite3_progress_handler(db_, 0, nullptr, nullptr);
            sqlite3_close_v2(db_);
        }
    }
    ConnectionGuard(const ConnectionGuard&) = delete;
    ConnectionGuard& operator=(const ConnectionGuard&) = delete;

private:
    sqlite3* db_;
};

std::optional<std::uint64_t> readPragma(sqlite3* db, const char* sql) {
    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        return std::nullopt;
    }
    std::optional<std::uint64_t> value;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        const auto raw = sqlite3_column_int64(stmt, 0);
        if (raw >= 0) {
            value = static_cast<std::uint64_t>(raw);
        }
    }
    sqlite3_finalize(stmt);
    return value;
}

std::uint64_t availableBytesAt(const std::filesystem::path& dir) {
    std::error_code ec;
    const auto info = std::filesystem::space(dir, ec);
    return ec ? 0 : static_cast<std::uint64_t>(info.available);
}

} // namespace

bool shouldVacuumSqlite(std::uint64_t databaseBytes, std::uint64_t pageCount,
                        std::uint64_t freePageCount, std::uint64_t pageSize,
                        const SqliteVacuumPolicy& policy) {
    if (databaseBytes <= policy.minDatabaseBytes || pageCount == 0 || pageSize == 0 ||
        freePageCount > pageCount) {
        return false;
    }
    if (freePageCount > std::numeric_limits<std::uint64_t>::max() / pageSize ||
        pageCount > std::numeric_limits<std::uint64_t>::max() / pageSize) {
        return false;
    }
    const auto reclaimablePageBytes = freePageCount * pageSize;
    const auto logicalBytes = pageCount * pageSize;
    const auto reclaimableTailBytes =
        databaseBytes > logicalBytes ? databaseBytes - logicalBytes : 0;
    const auto reclaimablePageRatio = static_cast<double>(freePageCount) / pageCount;
    const auto reclaimableTailRatio =
        static_cast<double>(reclaimableTailBytes) / static_cast<double>(databaseBytes);
    return (reclaimablePageBytes >= policy.minReclaimableBytes &&
            reclaimablePageRatio >= policy.minReclaimableRatio) ||
           (reclaimableTailBytes >= policy.minReclaimableBytes &&
            reclaimableTailRatio >= policy.minReclaimableRatio);
}

std::string_view sqliteVacuumStatusName(SqliteVacuumStatus status) noexcept {
    switch (status) {
        case SqliteVacuumStatus::NotNeeded:
            return "not_needed";
        case SqliteVacuumStatus::InsufficientSpace:
            return "insufficient_space";
        case SqliteVacuumStatus::OpenFailed:
            return "open_failed";
        case SqliteVacuumStatus::Busy:
            return "busy";
        case SqliteVacuumStatus::Interrupted:
            return "interrupted";
        case SqliteVacuumStatus::Vacuumed:
            return "vacuumed";
    }
    return "unknown";
}

SqliteVacuumOutcome vacuumSqliteFileIfUseful(const std::filesystem::path& dbPath,
                                             const SqliteVacuumPolicy& policy,
                                             const std::function<bool()>& shouldAbort) {
    SqliteVacuumOutcome outcome;
    std::error_code ec;
    const auto fileBytes = std::filesystem::file_size(dbPath, ec);
    if (ec) {
        outcome.status = SqliteVacuumStatus::OpenFailed;
        outcome.detail = "cannot stat database file: " + ec.message();
        return outcome;
    }
    outcome.bytesBefore = static_cast<std::uint64_t>(fileBytes);
    outcome.bytesAfter = outcome.bytesBefore;

    sqlite3* db = nullptr;
    const int openRc =
        sqlite3_open_v2(dbPath.string().c_str(), &db, SQLITE_OPEN_READWRITE, nullptr);
    ConnectionGuard guard(db);
    if (openRc != SQLITE_OK) {
        outcome.status = SqliteVacuumStatus::OpenFailed;
        outcome.detail = db != nullptr ? sqlite3_errmsg(db) : "sqlite3_open_v2 failed";
        return outcome;
    }
    // Fail fast rather than queueing behind a live writer; maintenance retries later.
    sqlite3_busy_timeout(db, 1000);
    // VACUUM builds a full copy of the live pages; keep it on disk, never in RAM.
    sqlite3_exec(db, "PRAGMA temp_store=FILE", nullptr, nullptr, nullptr);

    const auto pageCount = readPragma(db, "PRAGMA page_count");
    const auto freePageCount = readPragma(db, "PRAGMA freelist_count");
    const auto pageSize = readPragma(db, "PRAGMA page_size");
    if (!pageCount || !freePageCount || !pageSize) {
        outcome.status = SqliteVacuumStatus::Busy;
        outcome.detail = std::string("cannot read page statistics: ") + sqlite3_errmsg(db);
        return outcome;
    }
    outcome.reclaimableBytes = *freePageCount * *pageSize;
    if (!shouldVacuumSqlite(outcome.bytesBefore, *pageCount, *freePageCount, *pageSize, policy)) {
        outcome.status = SqliteVacuumStatus::NotNeeded;
        return outcome;
    }

    outcome.availableBytes = availableBytesAt(dbPath.parent_path());
    const auto liveBytes = (*pageCount - *freePageCount) * *pageSize;
    std::error_code tempEc;
    const auto tempDir = std::filesystem::temp_directory_path(tempEc);
    const auto tempAvailable = tempEc ? outcome.availableBytes : availableBytesAt(tempDir);
    if (!hasSpaceForSqliteVacuum(outcome.bytesBefore, outcome.availableBytes) ||
        tempAvailable <= liveBytes) {
        outcome.status = SqliteVacuumStatus::InsufficientSpace;
        outcome.detail = "data dir free=" + std::to_string(outcome.availableBytes) +
                         " temp free=" + std::to_string(tempAvailable) +
                         " db=" + std::to_string(outcome.bytesBefore) +
                         " live=" + std::to_string(liveBytes);
        return outcome;
    }

    bool aborted = false;
    struct AbortContext {
        const std::function<bool()>* shouldAbort;
        bool* aborted;
    } abortContext{&shouldAbort, &aborted};
    if (shouldAbort) {
        sqlite3_progress_handler(
            db, 1000,
            [](void* raw) -> int {
                auto* ctx = static_cast<AbortContext*>(raw);
                if ((*ctx->shouldAbort)()) {
                    *ctx->aborted = true;
                    return 1;
                }
                return 0;
            },
            &abortContext);
    }
    char* errMsg = nullptr;
    const int rc = sqlite3_exec(db, "VACUUM", nullptr, nullptr, &errMsg);
    sqlite3_progress_handler(db, 0, nullptr, nullptr);
    if (rc != SQLITE_OK) {
        outcome.detail = errMsg != nullptr ? errMsg : sqlite3_errmsg(db);
        sqlite3_free(errMsg);
        outcome.status = (aborted || rc == SQLITE_INTERRUPT) ? SqliteVacuumStatus::Interrupted
                                                             : SqliteVacuumStatus::Busy;
        return outcome;
    }
    // VACUUM in WAL mode writes the rewritten pages to the WAL; fold them back so the
    // reclaimed space shows up on disk now. A reader elsewhere can keep this from truncating,
    // in which case the owner's own checkpoints finish the job later.
    sqlite3_exec(db, "PRAGMA wal_checkpoint(TRUNCATE)", nullptr, nullptr, nullptr);

    outcome.status = SqliteVacuumStatus::Vacuumed;
    const auto after = std::filesystem::file_size(dbPath, ec);
    if (!ec) {
        outcome.bytesAfter = static_cast<std::uint64_t>(after);
    }
    return outcome;
}

} // namespace yams::daemon
