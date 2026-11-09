#include "storage_key.h"

#include <yams/compression/compression_header.h>
#include <yams/storage/block_reference_repair.h>

#include <sqlite3.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <fstream>
#include <memory>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

namespace yams::storage {

namespace {

namespace fs = std::filesystem;

struct DbCloser {
    void operator()(sqlite3* db) const noexcept { sqlite3_close(db); }
};
struct StmtFinalizer {
    void operator()(sqlite3_stmt* stmt) const noexcept { sqlite3_finalize(stmt); }
};
using DbHandle = std::unique_ptr<sqlite3, DbCloser>;
using StmtHandle = std::unique_ptr<sqlite3_stmt, StmtFinalizer>;

#if YAMS_LIBSQL_BACKEND
constexpr const char* kBeginSql = "BEGIN;";
#else
constexpr const char* kBeginSql = "BEGIN IMMEDIATE;";
#endif

// The same definitions as the refs.db schema backfill and the block_references triggers in
// reference_counter.cpp: totals cover every row, the unreferenced counters cover ref_count = 0.
constexpr const char* kRecomputeStatisticsSql = R"(
    INSERT OR REPLACE INTO ref_statistics(stat_name, stat_value, updated_at)
    VALUES
      ('total_blocks', (SELECT COUNT(*) FROM block_references), strftime('%s','now')),
      ('total_references', (SELECT IFNULL(SUM(ref_count),0) FROM block_references), strftime('%s','now')),
      ('total_bytes', (SELECT IFNULL(SUM(block_size),0) FROM block_references), strftime('%s','now')),
      ('total_uncompressed_bytes', (SELECT IFNULL(SUM(COALESCE(uncompressed_size, block_size)),0) FROM block_references), strftime('%s','now')),
      ('unreferenced_blocks', (SELECT COUNT(*) FROM block_references WHERE ref_count=0), strftime('%s','now')),
      ('unreferenced_bytes', (SELECT IFNULL(SUM(block_size),0) FROM block_references WHERE ref_count=0), strftime('%s','now'));
)";

constexpr const char* kAddStatisticSql =
    "UPDATE ref_statistics SET stat_value = stat_value + ?, updated_at = strftime('%s','now') "
    "WHERE stat_name = ?";

Error dbError(sqlite3* db, const std::string& what) {
    return Error{ErrorCode::DatabaseError, what + ": " + sqlite3_errmsg(db)};
}

Result<StmtHandle> prepare(sqlite3* db, const char* sql) {
    sqlite3_stmt* raw = nullptr;
    if (sqlite3_prepare_v2(db, sql, -1, &raw, nullptr) != SQLITE_OK) {
        sqlite3_finalize(raw);
        return dbError(db, std::string("Failed to prepare statement '") + sql + "'");
    }
    return StmtHandle(raw);
}

Result<void> exec(sqlite3* db, const char* sql, const char* what) {
    char* errMsg = nullptr;
    if (sqlite3_exec(db, sql, nullptr, nullptr, &errMsg) != SQLITE_OK) {
        std::string message = std::string(what) + ": " + (errMsg ? errMsg : sqlite3_errmsg(db));
        sqlite3_free(errMsg);
        return Error{ErrorCode::DatabaseError, message};
    }
    return {};
}

// Rolls back an open write transaction unless it was committed.
class WriteTransaction {
public:
    explicit WriteTransaction(sqlite3* db) : db_(db) {}
    ~WriteTransaction() { rollback(); }
    WriteTransaction(const WriteTransaction&) = delete;
    WriteTransaction& operator=(const WriteTransaction&) = delete;

    Result<void> begin() {
        if (auto r = exec(db_, kBeginSql, "Failed to begin refs.db transaction"); !r) {
            return r;
        }
        open_ = true;
        return {};
    }
    Result<void> commit() {
        if (auto r = exec(db_, "COMMIT;", "Failed to commit refs.db transaction"); !r) {
            rollback();
            return r;
        }
        open_ = false;
        return {};
    }
    void rollback() noexcept {
        if (open_) {
            sqlite3_exec(db_, "ROLLBACK;", nullptr, nullptr, nullptr);
            open_ = false;
        }
    }

private:
    sqlite3* db_;
    bool open_{false};
};

// ref_statistics changes caused by this batch's size updates. Size updates do not fire the
// schema triggers (they track inserts, deletes and ref_count changes), so the repair applies the
// trigger arithmetic itself inside the same transaction.
struct StatisticsDelta {
    int64_t totalBytes{0};
    int64_t totalUncompressedBytes{0};
    int64_t unreferencedBytes{0};
};

Result<void> applyDelta(sqlite3* db, sqlite3_stmt* addStmt, const StatisticsDelta& delta) {
    const std::pair<const char*, int64_t> changes[] = {
        {"total_bytes", delta.totalBytes},
        {"total_uncompressed_bytes", delta.totalUncompressedBytes},
        {"unreferenced_bytes", delta.unreferencedBytes},
    };
    for (const auto& [name, value] : changes) {
        if (value == 0) {
            continue;
        }
        sqlite3_reset(addStmt);
        sqlite3_bind_int64(addStmt, 1, value);
        sqlite3_bind_text(addStmt, 2, name, -1, SQLITE_STATIC);
        if (sqlite3_step(addStmt) != SQLITE_DONE) {
            return dbError(db, std::string("Failed to update ref_statistics.") + name);
        }
    }
    sqlite3_reset(addStmt);
    return {};
}

enum class RowOutcome { Updated, Unchanged, NoRow, Rejected };

// Brings one block_references row in line with its object file. `update` is null for a dry run.
// A row the schema rejects (CHECK) fails only that statement, so it is reported as Rejected and
// the batch continues; any other write failure is returned and abandons the batch.
Result<RowOutcome> reconcileRow(sqlite3* db, sqlite3_stmt* select, sqlite3_stmt* update,
                                const std::string& blockHash, int64_t diskSize,
                                int64_t uncompressedSize, StatisticsDelta& delta) {
    sqlite3_reset(select);
    sqlite3_bind_text(select, 1, blockHash.c_str(), -1, SQLITE_TRANSIENT);
    if (sqlite3_step(select) != SQLITE_ROW) {
        sqlite3_reset(select);
        // Object without a refs.db row: unexpected, but not this repair's job.
        spdlog::debug("Block refs repair: {} not in refs.db", blockHash.substr(0, 8));
        return RowOutcome::NoRow;
    }
    const int64_t currentBlockSize = sqlite3_column_int64(select, 0);
    const bool hasUncompressed = sqlite3_column_type(select, 1) != SQLITE_NULL;
    const int64_t currentUncompressed =
        hasUncompressed ? sqlite3_column_int64(select, 1) : currentBlockSize;
    const int64_t refCount = sqlite3_column_int64(select, 2);
    sqlite3_reset(select);

    if (currentBlockSize == diskSize && hasUncompressed &&
        currentUncompressed == uncompressedSize) {
        return RowOutcome::Unchanged;
    }
    if (!update) {
        return RowOutcome::Updated;
    }

    sqlite3_reset(update);
    sqlite3_bind_int64(update, 1, diskSize);
    sqlite3_bind_int64(update, 2, uncompressedSize);
    sqlite3_bind_text(update, 3, blockHash.c_str(), -1, SQLITE_TRANSIENT);
    const int rc = sqlite3_step(update);
    if (rc != SQLITE_DONE) {
        auto error = dbError(db, "Failed to update block " + blockHash.substr(0, 8));
        sqlite3_reset(update);
        if ((rc & 0xff) != SQLITE_CONSTRAINT) {
            return error;
        }
        spdlog::warn("Block refs repair: {}", error.message);
        return RowOutcome::Rejected;
    }
    sqlite3_reset(update);

    // Trigger arithmetic (reference_counter.cpp): totals cover every row and count
    // COALESCE(uncompressed_size, block_size); unreferenced bytes cover ref_count = 0.
    delta.totalBytes += diskSize - currentBlockSize;
    delta.totalUncompressedBytes += uncompressedSize - currentUncompressed;
    if (refCount == 0) {
        delta.unreferencedBytes += diskSize - currentBlockSize;
    }
    return RowOutcome::Updated;
}

uint64_t approximateBlockCount(sqlite3* db) {
    sqlite3_stmt* raw = nullptr;
    if (sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM block_references", -1, &raw, nullptr) !=
        SQLITE_OK) {
        sqlite3_finalize(raw);
        return 0;
    }
    StmtHandle stmt(raw);
    if (sqlite3_step(stmt.get()) != SQLITE_ROW) {
        return 0;
    }
    return static_cast<uint64_t>(std::max<int64_t>(0, sqlite3_column_int64(stmt.get(), 0)));
}

// On-disk size is the stored size; a valid compression header names the uncompressed size.
int64_t uncompressedSizeOf(const fs::path& path, int64_t diskSize, std::vector<std::byte>& buffer,
                           BlockRefsRepairResult& result) {
    if (diskSize >= static_cast<int64_t>(compression::CompressionHeader::SIZE)) {
        std::ifstream file(path, std::ios::binary);
        if (file) {
            file.read(reinterpret_cast<char*>(buffer.data()),
                      static_cast<std::streamsize>(compression::CompressionHeader::SIZE));
            if (file.gcount() ==
                static_cast<std::streamsize>(compression::CompressionHeader::SIZE)) {
                auto header = compression::CompressionHeader::parse(buffer);
                if (header && header.value().validate()) {
                    result.compressedBlocks++;
                    return static_cast<int64_t>(header.value().uncompressedSize);
                }
            }
        }
    }
    result.uncompressedBlocks++;
    return diskSize;
}

} // namespace

Result<BlockRefsRepairResult> repairBlockReferences(const fs::path& objectsPath,
                                                    const fs::path& refsDbPath,
                                                    const BlockRefsRepairOptions& options) {
    const bool dryRun = options.dryRun;
    const uint64_t batchSize = std::max<uint64_t>(1, options.batchSize);
    BlockRefsRepairResult result;

    std::error_code ec;
    if (!fs::exists(objectsPath, ec)) {
        return Error{ErrorCode::NotFound, "Objects directory not found: " + objectsPath.string()};
    }
    if (!fs::exists(refsDbPath, ec)) {
        return Error{ErrorCode::NotFound, "refs.db not found: " + refsDbPath.string()};
    }

    sqlite3* rawDb = nullptr;
    const int openRc =
        sqlite3_open_v2(refsDbPath.string().c_str(), &rawDb, SQLITE_OPEN_READWRITE, nullptr);
    DbHandle db(rawDb);
    if (openRc != SQLITE_OK) {
        return Error{ErrorCode::DatabaseError,
                     std::string("Failed to open refs.db: ") +
                         (rawDb ? sqlite3_errmsg(rawDb) : sqlite3_errstr(openRc))};
    }
    sqlite3_busy_timeout(db.get(), static_cast<int>(options.busyTimeout.count()));
    // WAL for concurrency with the daemon; best effort, as for the rest of the storage layer.
    sqlite3_exec(db.get(), "PRAGMA journal_mode=WAL;", nullptr, nullptr, nullptr);

    auto selectStmt = prepare(db.get(), "SELECT block_size, uncompressed_size, ref_count FROM "
                                        "block_references WHERE block_hash = ?");
    if (!selectStmt) {
        return selectStmt.error();
    }
    StmtHandle updateStmt;
    StmtHandle addStatStmt;
    if (!dryRun) {
        auto update = prepare(db.get(), "UPDATE block_references SET block_size = ?, "
                                        "uncompressed_size = ? WHERE block_hash = ?");
        if (!update) {
            return update.error();
        }
        updateStmt = std::move(update).value();
        auto addStat = prepare(db.get(), kAddStatisticSql);
        if (!addStat) {
            return addStat.error();
        }
        addStatStmt = std::move(addStat).value();
    }
    sqlite3_stmt* select = selectStmt.value().get();

    // Progress total comes from refs.db instead of a second walk; it is approximate because
    // objects and rows need not match one to one.
    const uint64_t approxTotal = approximateBlockCount(db.get());
    spdlog::info("Block refs repair: scanning CAS objects (~{} blocks, dry_run={})", approxTotal,
                 dryRun);

    WriteTransaction txn(db.get());
    StatisticsDelta delta;
    uint64_t batchUpdated = 0;
    uint64_t committedUpdated = 0;
    uint64_t inBatch = 0;

    auto beginBatch = [&]() -> Result<void> {
        if (dryRun) {
            return {};
        }
        return txn.begin();
    };
    // Commits the batch's updates together with their statistics. The last commit also
    // recomputes every counter from block_references so earlier drift is corrected too.
    auto commitBatch = [&](bool last) -> Result<void> {
        if (dryRun) {
            committedUpdated = result.blocksUpdated;
            return {};
        }
        if (auto r = applyDelta(db.get(), addStatStmt.get(), delta); !r) {
            return r;
        }
        if (last && result.blocksUpdated > 0) {
            if (auto r =
                    exec(db.get(), kRecomputeStatisticsSql, "Failed to recompute ref_statistics");
                !r) {
                return r;
            }
        }
        if (auto r = txn.commit(); !r) {
            return r;
        }
        committedUpdated += batchUpdated;
        batchUpdated = 0;
        delta = {};
        return {};
    };
    // Any write failure abandons the open batch: report only what earlier batches committed.
    auto failBatch = [&](const Error& error) -> Error {
        txn.rollback();
        return Error{error.code, error.message + " (" + std::to_string(committedUpdated) +
                                     " block updates committed before the failure)"};
    };

    if (auto r = beginBatch(); !r) {
        return failBatch(r.error());
    }

    std::vector<std::byte> headerBuffer(compression::CompressionHeader::SIZE);
    const fs::directory_iterator end;

    // StorageEngine writes each object to <storage>/temp and fsync+renames it into
    // objects/<prefix>/<suffix>, so every file under objects/ is complete. Only names that are
    // not storage keys (strays, editor files, interrupted foreign copies) need to be filtered.
    auto shards = fs::directory_iterator(objectsPath, ec);
    if (ec) {
        return failBatch(Error{ErrorCode::IOError, "Failed to list objects directory " +
                                                       objectsPath.string() + ": " + ec.message()});
    }
    for (; shards != end; shards.increment(ec)) {
        if (ec) {
            break;
        }
        const fs::path shardPath = shards->path();
        std::error_code shardEc;
        if (!shards->is_directory(shardEc) || shardEc) {
            result.entriesIgnored++;
            continue;
        }

        auto files = fs::directory_iterator(shardPath, shardEc);
        if (shardEc) {
            // The shard vanished (or became unreadable) after it was listed.
            result.entriesIgnored++;
            continue;
        }
        for (; files != end; files.increment(shardEc)) {
            if (shardEc) {
                break;
            }
            const fs::path objectPath = files->path();
            std::error_code fileEc;
            if (!files->is_regular_file(fileEc) || fileEc) {
                result.entriesIgnored++;
                continue;
            }
            const std::string blockHash = storageHashFromObjectPath(objectsPath, objectPath);
            if (!isValidStorageKey(blockHash) || isManifestKey(blockHash)) {
                result.entriesIgnored++;
                continue;
            }

            const auto rawSize = fs::file_size(objectPath, fileEc);
            if (fileEc) {
                if (fileEc == std::errc::no_such_file_or_directory) {
                    result.entriesIgnored++; // removed (e.g. by GC) after it was listed
                    continue;
                }
                result.blocksScanned++;
                result.errors++;
                spdlog::warn("Block refs repair: cannot get size for {}: {}",
                             blockHash.substr(0, 8), fileEc.message());
            } else {
                result.blocksScanned++;
                const auto diskSize = static_cast<int64_t>(rawSize);
                result.totalDiskBytes += static_cast<uint64_t>(diskSize);
                const int64_t uncompressedSize =
                    uncompressedSizeOf(objectPath, diskSize, headerBuffer, result);
                result.totalUncompressedBytes += static_cast<uint64_t>(uncompressedSize);

                auto outcome = reconcileRow(db.get(), select, updateStmt.get(), blockHash, diskSize,
                                            uncompressedSize, delta);
                if (!outcome) {
                    return failBatch(outcome.error());
                }
                switch (outcome.value()) {
                    case RowOutcome::Updated:
                        result.blocksUpdated++;
                        if (!dryRun) {
                            batchUpdated++;
                        }
                        break;
                    case RowOutcome::Unchanged:
                    case RowOutcome::NoRow:
                        result.blocksSkipped++;
                        break;
                    case RowOutcome::Rejected:
                        result.errors++;
                        break;
                }
            }

            if (options.progress && result.blocksScanned % 1000 == 0) {
                options.progress(result.blocksScanned, std::max(approxTotal, result.blocksScanned));
            }

            if (++inBatch < batchSize) {
                continue;
            }
            inBatch = 0;
            const bool cancel = options.shouldContinue && !options.shouldContinue();
            if (auto r = commitBatch(/*last=*/cancel); !r) {
                return failBatch(r.error());
            }
            if (cancel) {
                return Error{ErrorCode::OperationCancelled,
                             "Block refs repair canceled after scanning " +
                                 std::to_string(result.blocksScanned) + " blocks; " +
                                 std::to_string(committedUpdated) + " updates committed"};
            }
            if (auto r = beginBatch(); !r) {
                return failBatch(r.error());
            }
        }
        if (shardEc) {
            // The shard vanished part way through its listing.
            result.entriesIgnored++;
        }
    }
    if (ec) {
        return failBatch(Error{ErrorCode::IOError, "Failed to list objects directory " +
                                                       objectsPath.string() + ": " + ec.message()});
    }

    if (auto r = commitBatch(/*last=*/true); !r) {
        return failBatch(r.error());
    }

    if (options.progress) {
        options.progress(result.blocksScanned, result.blocksScanned);
    }

    spdlog::info("Block refs repair complete: scanned={} updated={} skipped={} errors={} "
                 "ignored={} compressed={} uncompressed={}",
                 result.blocksScanned, result.blocksUpdated, result.blocksSkipped, result.errors,
                 result.entriesIgnored, result.compressedBlocks, result.uncompressedBlocks);

    return result;
}

Result<BlockRefsRepairResult>
repairBlockReferences(const fs::path& objectsPath, const fs::path& refsDbPath, bool dryRun,
                      std::function<void(uint64_t current, uint64_t total)> progress,
                      std::function<bool()> shouldContinue) {
    BlockRefsRepairOptions options;
    options.dryRun = dryRun;
    options.progress = std::move(progress);
    options.shouldContinue = std::move(shouldContinue);
    return repairBlockReferences(objectsPath, refsDbPath, options);
}

} // namespace yams::storage
