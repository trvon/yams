#include <yams/daemon/components/db_recovery.h>
#include <yams/daemon/components/db_salvage.h>

#include <sqlite3.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <charconv>
#include <cstring>
#include <filesystem>
#include <optional>

namespace yams::daemon {

namespace fs = std::filesystem;

namespace {

std::string columnText(sqlite3_stmt* stmt, int col) {
    const char* text = reinterpret_cast<const char*>(sqlite3_column_text(stmt, col));
    return text ? std::string(text) : std::string{};
}

Result<void> execRaw(sqlite3* db, const char* sql) {
    char* errMsg = nullptr;
    int rc = sqlite3_exec(db, sql, nullptr, nullptr, &errMsg);
    if (rc != SQLITE_OK) {
        std::string err = errMsg ? errMsg : "unknown error";
        sqlite3_free(errMsg);
        return Error{ErrorCode::DatabaseError, err};
    }
    return {};
}

Result<void> attachCorruptDb(sqlite3* freshDb, const fs::path& corruptPath) {
    std::string path = corruptPath.string();
    // Escape single-quote characters for SQLite string literal.
    for (size_t pos = path.find('\''); pos != std::string::npos; pos = path.find('\'', pos + 2)) {
        path.insert(pos, "'");
    }
    std::string attachSql = "ATTACH DATABASE '" + path + "' AS corrupt";
    return execRaw(freshDb, attachSql.c_str());
}

void detachCorruptDb(sqlite3* freshDb) {
    (void)execRaw(freshDb, "DETACH DATABASE corrupt");
}

Result<void> copyDocumentsViaAttach(sqlite3* freshDb, const fs::path& corruptPath,
                                    DbSalvageResult& result) {
    auto attachResult = attachCorruptDb(freshDb, corruptPath);
    if (!attachResult) {
        return attachResult.error();
    }

    const char* copySql = "INSERT OR IGNORE INTO main.documents "
                          "SELECT * FROM corrupt.documents";

    char* errMsg = nullptr;
    int rc = sqlite3_exec(freshDb, copySql, nullptr, nullptr, &errMsg);
    if (rc != SQLITE_OK) {
        std::string err = errMsg ? errMsg : "unknown error";
        spdlog::warn("[db_salvage] ATTACH-based copy failed: {}", err);
        sqlite3_free(errMsg);
        detachCorruptDb(freshDb);

        std::vector<std::string> diag;
        diag.push_back("ATTACH-based copy failed: " + err);
        diag.push_back("Falling back to row-by-row salvage");
        result.diagnostics.insert(result.diagnostics.end(), diag.begin(), diag.end());
        return Error{ErrorCode::DatabaseError, err};
    }

    int changes = sqlite3_changes(freshDb);
    result.documentsSalvaged = static_cast<size_t>(changes);

    detachCorruptDb(freshDb);
    return {};
}

Result<void> copyDocumentsRowByRow(const fs::path& corruptPath, sqlite3* freshDb,
                                   DbSalvageResult& result) {
    sqlite3* corruptDb = nullptr;

    int rc =
        sqlite3_open_v2(corruptPath.string().c_str(), &corruptDb, SQLITE_OPEN_READONLY, nullptr);
    if (rc != SQLITE_OK) {
        std::string err = corruptDb ? sqlite3_errmsg(corruptDb) : "unknown error";
        spdlog::warn("[db_salvage] Cannot open corrupt DB for row-by-row copy: {}", err);
        if (corruptDb)
            sqlite3_close(corruptDb);
        return Error{ErrorCode::DatabaseError, "Cannot open corrupt DB: " + err};
    }

    sqlite3_stmt* stmt = nullptr;
    const char* selectSql =
        "SELECT file_path, file_name, file_extension, file_size, sha256_hash, "
        "mime_type, created_time, modified_time, indexed_time, content_extracted, "
        "extraction_status, extraction_error, path_prefix, reverse_path, path_hash, "
        "parent_hash, path_depth, repair_status, repair_attempted_at, repair_attempts "
        "FROM documents";

    rc = sqlite3_prepare_v2(corruptDb, selectSql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        std::string err = sqlite3_errmsg(corruptDb);
        spdlog::warn("[db_salvage] Cannot prepare SELECT from corrupt DB: {}", err);
        sqlite3_close(corruptDb);
        return Error{ErrorCode::DatabaseError, "Cannot read corrupt DB: " + err};
    }

    const char* insertSql =
        "INSERT OR IGNORE INTO documents "
        "(file_path, file_name, file_extension, file_size, sha256_hash, "
        "mime_type, created_time, modified_time, indexed_time, content_extracted, "
        "extraction_status, extraction_error, path_prefix, reverse_path, path_hash, "
        "parent_hash, path_depth, repair_status, repair_attempted_at, repair_attempts) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    sqlite3_stmt* insertStmt = nullptr;
    rc = sqlite3_prepare_v2(freshDb, insertSql, -1, &insertStmt, nullptr);
    if (rc != SQLITE_OK) {
        std::string err = sqlite3_errmsg(freshDb);
        spdlog::warn("[db_salvage] Cannot prepare INSERT into fresh DB: {}", err);
        sqlite3_finalize(stmt);
        sqlite3_close(corruptDb);
        return Error{ErrorCode::DatabaseError, "Cannot prepare INSERT: " + err};
    }

    auto bindNullableText = [](sqlite3_stmt* s, int idx, const std::string& val) {
        if (val.empty()) {
            sqlite3_bind_null(s, idx);
        } else {
            sqlite3_bind_text(s, idx, val.c_str(), static_cast<int>(val.size()), SQLITE_TRANSIENT);
        }
    };

    auto bindNullableInt = [](sqlite3_stmt* s, int idx, int64_t val, bool isNull) {
        if (isNull) {
            sqlite3_bind_null(s, idx);
        } else {
            sqlite3_bind_int64(s, idx, val);
        }
    };

    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        try {
            sqlite3_reset(insertStmt);
            sqlite3_clear_bindings(insertStmt);

            std::string filePath = columnText(stmt, 0);
            std::string fileName = columnText(stmt, 1);
            std::string fileExtension = columnText(stmt, 2);
            int64_t fileSize = sqlite3_column_int64(stmt, 3);
            std::string sha256Hash = columnText(stmt, 4);
            std::string mimeType = columnText(stmt, 5);
            int64_t createdTime = sqlite3_column_int64(stmt, 6);
            int64_t modifiedTime = sqlite3_column_int64(stmt, 7);
            int64_t indexedTime = sqlite3_column_int64(stmt, 8);
            int contentExtracted = sqlite3_column_int(stmt, 9);
            std::string extractionStatus = columnText(stmt, 10);
            std::string extractionError = columnText(stmt, 11);
            std::string pathPrefix = columnText(stmt, 12);
            std::string reversePath = columnText(stmt, 13);
            std::string pathHash = columnText(stmt, 14);
            std::string parentHash = columnText(stmt, 15);
            int64_t pathDepth = sqlite3_column_int64(stmt, 16);
            std::string repairStatus = columnText(stmt, 17);
            bool repairAttemptedAtNull = sqlite3_column_type(stmt, 18) == SQLITE_NULL;
            int64_t repairAttemptedAt = sqlite3_column_int64(stmt, 18);
            int64_t repairAttempts = sqlite3_column_int64(stmt, 19);

            sqlite3_bind_text(insertStmt, 1, filePath.c_str(), static_cast<int>(filePath.size()),
                              SQLITE_TRANSIENT);
            sqlite3_bind_text(insertStmt, 2, fileName.c_str(), static_cast<int>(fileName.size()),
                              SQLITE_TRANSIENT);
            bindNullableText(insertStmt, 3, fileExtension);
            sqlite3_bind_int64(insertStmt, 4, fileSize);
            sqlite3_bind_text(insertStmt, 5, sha256Hash.c_str(),
                              static_cast<int>(sha256Hash.size()), SQLITE_TRANSIENT);
            bindNullableText(insertStmt, 6, mimeType);
            bool createdNull = sqlite3_column_type(stmt, 6) == SQLITE_NULL;
            bindNullableInt(insertStmt, 7, createdTime, createdNull);
            bool modifiedNull = sqlite3_column_type(stmt, 7) == SQLITE_NULL;
            bindNullableInt(insertStmt, 8, modifiedTime, modifiedNull);
            bool indexedNull = sqlite3_column_type(stmt, 8) == SQLITE_NULL;
            bindNullableInt(insertStmt, 9, indexedTime, indexedNull);
            sqlite3_bind_int(insertStmt, 10, contentExtracted);
            bindNullableText(insertStmt, 11, extractionStatus);
            bindNullableText(insertStmt, 12, extractionError);
            bindNullableText(insertStmt, 13, pathPrefix);
            bindNullableText(insertStmt, 14, reversePath);
            bindNullableText(insertStmt, 15, pathHash);
            bindNullableText(insertStmt, 16, parentHash);
            sqlite3_bind_int64(insertStmt, 17, pathDepth);
            bindNullableText(insertStmt, 18, repairStatus);
            bindNullableInt(insertStmt, 19, repairAttemptedAt, repairAttemptedAtNull);
            sqlite3_bind_int64(insertStmt, 20, repairAttempts);

            int insertRc = sqlite3_step(insertStmt);
            if (insertRc != SQLITE_DONE) {
                spdlog::debug("[db_salvage] Row-by-row insert failed for hash={}: {}", sha256Hash,
                              sqlite3_errmsg(freshDb));
                result.documentsFailed++;
            } else {
                result.documentsSalvaged++;
            }
        } catch (const std::exception& e) {
            spdlog::debug("[db_salvage] Row-by-row exception: {}", e.what());
            result.documentsFailed++;
            sqlite3_reset(insertStmt);
        }
    }

    if (rc != SQLITE_DONE) {
        spdlog::warn("[db_salvage] SELECT from corrupt DB ended with code {}: {}", rc,
                     sqlite3_errmsg(corruptDb));
    }

    sqlite3_finalize(insertStmt);
    sqlite3_finalize(stmt);
    sqlite3_close(corruptDb);

    return {};
}

Result<void> runIntegrityCheck(sqlite3* db, std::vector<std::string>& diagnostics) {
    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(db, "PRAGMA integrity_check", -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        diagnostics.push_back(std::string("integrity_check prepare failed: ") + sqlite3_errmsg(db));
        return {};
    }

    int rowCount = 0;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        const unsigned char* text = sqlite3_column_text(stmt, 0);
        std::string line = text ? reinterpret_cast<const char*>(text) : "";
        if (!line.empty()) {
            diagnostics.push_back("integrity_check: " + line);
        }
        ++rowCount;
        if (rowCount >= 32)
            break;
    }
    sqlite3_finalize(stmt);
    return {};
}

} // namespace

Result<DbSalvageResult> salvageFromCorruptDb(const fs::path& corruptPath, const fs::path& freshPath,
                                             SalvageProgressFn progress) {
    DbSalvageResult result;

    if (corruptPath.empty() || freshPath.empty()) {
        return Error{ErrorCode::InvalidArgument, "corrupt or fresh DB path is empty"};
    }

    spdlog::info("[db_salvage] Opening corrupt DB: {}", corruptPath.string());
    spdlog::info("[db_salvage] Target fresh DB: {}", freshPath.string());

    if (!fs::exists(corruptPath)) {
        result.diagnostics.push_back("corrupt DB not found: " + corruptPath.string());
        spdlog::warn("[db_salvage] Corrupt DB not found: {}", corruptPath.string());
        return result;
    }

    if (!fs::exists(freshPath)) {
        return Error{ErrorCode::FileNotFound, "fresh DB does not exist: " + freshPath.string()};
    }

    // Diagnose the corrupt DB
    {
        sqlite3* corruptDb = nullptr;
        int rc = sqlite3_open_v2(corruptPath.string().c_str(), &corruptDb, SQLITE_OPEN_READONLY,
                                 nullptr);
        if (rc == SQLITE_OK) {
            spdlog::info("[db_salvage] Corrupt DB opened successfully");
            runIntegrityCheck(corruptDb, result.diagnostics);
            // Count documents in the corrupt DB
            sqlite3_stmt* countStmt = nullptr;
            if (sqlite3_prepare_v2(corruptDb, "SELECT COUNT(*) FROM documents", -1, &countStmt,
                                   nullptr) == SQLITE_OK) {
                if (sqlite3_step(countStmt) == SQLITE_ROW) {
                    int docCount = sqlite3_column_int(countStmt, 0);
                    spdlog::info("[db_salvage] Corrupt DB contains {} document(s)", docCount);
                }
                sqlite3_finalize(countStmt);
            }
            sqlite3_close(corruptDb);
        } else {
            result.diagnostics.push_back("Cannot open corrupt DB for diagnostics: " +
                                         std::string(sqlite3_errmsg(corruptDb)));
            if (corruptDb)
                sqlite3_close(corruptDb);
        }
    }

    // Open the fresh DB (read-write)
    sqlite3* freshDb = nullptr;
    int rc = sqlite3_open_v2(freshPath.string().c_str(), &freshDb, SQLITE_OPEN_READWRITE, nullptr);
    if (rc != SQLITE_OK) {
        std::string err = freshDb ? sqlite3_errmsg(freshDb) : "unknown error";
        if (freshDb)
            sqlite3_close(freshDb);
        return Error{ErrorCode::DatabaseError, "Cannot open fresh DB: " + err};
    }

    sqlite3_busy_timeout(freshDb, 10000);

    spdlog::info("[db_salvage] Attempting ATTACH-based copy");
    if (progress)
        progress("repairing", "Copying documents via ATTACH...", 0, 0);
    auto attachResult = copyDocumentsViaAttach(freshDb, corruptPath, result);

    if (!attachResult) {
        spdlog::warn("[db_salvage] ATTACH copy failed: {}, falling back to row-by-row",
                     attachResult.error().message);
        auto rowResult = copyDocumentsRowByRow(corruptPath, freshDb, result);
        if (!rowResult) {
            spdlog::error("[db_salvage] Row-by-row fallback also failed: {}",
                          rowResult.error().message);
            sqlite3_close(freshDb);
            return rowResult.error();
        }
    }

    sqlite3_close(freshDb);

    spdlog::info("[db_salvage] Salvage complete: salvaged={} failed={}", result.documentsSalvaged,
                 result.documentsFailed);
    if (progress)
        progress("completed",
                 std::to_string(result.documentsSalvaged) + " saved, " +
                     std::to_string(result.documentsFailed) + " failed",
                 result.documentsSalvaged, result.documentsSalvaged + result.documentsFailed);

    return result;
}

int64_t countDocumentsInDb(const fs::path& dbPath) {
    sqlite3* db = nullptr;

    // Try ReadOnly first. If the WAL is locked from an unclean shutdown,
    // this will fail. Fall back to ReadWrite which lets SQLite replay/recover
    // the WAL before reading.
    int rc = sqlite3_open_v2(dbPath.string().c_str(), &db, SQLITE_OPEN_READONLY, nullptr);
    if (rc != SQLITE_OK) {
        if (db) {
            sqlite3_close(db);
            db = nullptr;
        }
        spdlog::debug("[db_salvage] ReadOnly open failed for '{}', trying ReadWrite recovery",
                      dbPath.filename().string());
        rc = sqlite3_open_v2(dbPath.string().c_str(), &db, SQLITE_OPEN_READWRITE, nullptr);
        if (rc != SQLITE_OK) {
            if (db)
                sqlite3_close(db);
            spdlog::warn("[db_salvage] Cannot open corrupt DB '{}': {}", dbPath.filename().string(),
                         rc);
            return -1;
        }
        // WAL recovery on ReadWrite open: checkpoint and close, then
        // re-open ReadOnly for the actual count query.
        sqlite3_busy_timeout(db, 10000);
        sqlite3_exec(db, "PRAGMA wal_checkpoint(TRUNCATE)", nullptr, nullptr, nullptr);
        sqlite3_close(db);
        db = nullptr;
        rc = sqlite3_open_v2(dbPath.string().c_str(), &db, SQLITE_OPEN_READONLY, nullptr);
        if (rc != SQLITE_OK) {
            if (db)
                sqlite3_close(db);
            return -1;
        }
    }
    sqlite3_busy_timeout(db, 5000);
    sqlite3_stmt* stmt = nullptr;
    int64_t count = -1;
    rc = sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM documents", -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        spdlog::warn("[db_salvage] Cannot query documents in '{}': {}", dbPath.filename().string(),
                     sqlite3_errmsg(db));
    } else if (sqlite3_step(stmt) == SQLITE_ROW) {
        count = sqlite3_column_int64(stmt, 0);
    }
    if (stmt)
        sqlite3_finalize(stmt);
    sqlite3_close(db);
    return count;
}

AggregateSalvageResult salvageFromAllCorruptDbs(const fs::path& dataDir, const fs::path& freshPath,
                                                SalvageProgressFn progress) {
    AggregateSalvageResult result;

    auto corruptDbs = listCorruptDbs(dataDir, kMetadataDbFileName);

    // Sort by modification time, newest first
    std::sort(corruptDbs.begin(), corruptDbs.end(), [](const fs::path& a, const fs::path& b) {
        std::error_code ea, eb;
        auto ta = fs::last_write_time(a, ea);
        auto tb = fs::last_write_time(b, eb);
        return ta > tb;
    });

    spdlog::info("[db_salvage] Found {} corrupt DB(s) in {}", corruptDbs.size(), dataDir.string());

    size_t idx = 0;
    for (const auto& corruptPath : corruptDbs) {
        ++idx;
        if (progress) {
            progress("scanning",
                     "Checking " + corruptPath.filename().string() + " (" + std::to_string(idx) +
                         "/" + std::to_string(corruptDbs.size()) + ")",
                     idx, corruptDbs.size());
        }
        int64_t docCount = countDocumentsInDb(corruptPath);
        spdlog::info("[db_salvage] Corrupt DB '{}' contains {} document(s)",
                     corruptPath.filename().string(), docCount);

        if (docCount <= 0) {
            spdlog::info("[db_salvage] Skipping empty corrupt DB: {}",
                         corruptPath.filename().string());
            continue;
        }

        if (progress) {
            progress("repairing",
                     "Salvaging " + std::to_string(docCount) + " documents from " +
                         corruptPath.filename().string(),
                     idx, corruptDbs.size());
        }
        auto salvageResult = salvageFromCorruptDb(corruptPath, freshPath, progress);
        if (salvageResult) {
            auto& sr = salvageResult.value();
            result.combined.documentsSalvaged += sr.documentsSalvaged;
            result.combined.documentsFailed += sr.documentsFailed;
            result.salvagedPaths.push_back(corruptPath);
            result.combined.diagnostics.insert(result.combined.diagnostics.end(),
                                               sr.diagnostics.begin(), sr.diagnostics.end());
        } else {
            spdlog::warn("[db_salvage] Salvage from '{}' failed: {}",
                         corruptPath.filename().string(), salvageResult.error().message);
            result.combined.diagnostics.push_back("salvage failed for " +
                                                  corruptPath.filename().string() + ": " +
                                                  salvageResult.error().message);
        }
    }

    return result;
}

SalvageQuickCheck quickCheckSalvageNeeded(const fs::path& dataDir, const fs::path& dbPath) {
    SalvageQuickCheck qc;
    qc.currentDocCount = countDocumentsInDb(dbPath);

    for (const auto& corruptPath : listCorruptDbs(dataDir, dbPath.filename().string())) {
        ++qc.corruptDbCount;
        int64_t count = countDocumentsInDb(corruptPath);
        if (count < 0) {
            ++qc.unreadableCorruptDbCount;
            spdlog::warn("[db_salvage] Corrupt DB '{}' could not be counted; leaving it for "
                         "manual repair",
                         corruptPath.filename().string());
            continue;
        }
        spdlog::info("[db_salvage] Corrupt DB '{}' has {} docs (current DB has {})",
                     corruptPath.filename().string(), count, qc.currentDocCount);
        if (count > qc.currentDocCount) {
            qc.needsSalvage = true;
            qc.maxCorruptCount = std::max(qc.maxCorruptCount, count);
        }
    }
    return qc;
}

RecoverySentinelCleanup removeRecoverySentinels(const fs::path& dbPath) {
    RecoverySentinelCleanup cleanup;
    const fs::path dataDir = dbPath.has_parent_path() ? dbPath.parent_path() : fs::path(".");
    const std::string sentinelPrefix = dbPath.filename().string() + ".recovered-";

    std::error_code ec;
    for (const auto& entry : fs::directory_iterator(dataDir, ec)) {
        if (ec) {
            cleanup.errors.push_back("cannot scan " + dataDir.string() + ": " + ec.message());
            ec.clear();
            break;
        }
        const auto name = entry.path().filename().string();
        if (name.rfind(sentinelPrefix, 0) != 0) {
            continue;
        }

        std::error_code removeEc;
        if (fs::remove(entry.path(), removeEc)) {
            cleanup.removed.push_back(entry.path());
        } else if (removeEc) {
            cleanup.errors.push_back("cannot remove " + entry.path().string() + ": " +
                                     removeEc.message());
        }
    }

    return cleanup;
}

namespace {

// `YYYYmmddTHHMMSSZ[.N]` -> UTC time the artifact was quarantined.
std::optional<std::chrono::system_clock::time_point> parseQuarantineTimestamp(std::string_view s) {
    if (s.size() < 16 || s[8] != 'T' || s[15] != 'Z') {
        return std::nullopt;
    }
    const auto field = [&](std::size_t pos, std::size_t len, int& out) {
        const auto* first = s.data() + pos;
        const auto* last = first + len;
        const auto [ptr, ec] = std::from_chars(first, last, out);
        return ec == std::errc{} && ptr == last;
    };
    int y = 0;
    int mo = 0;
    int d = 0;
    int h = 0;
    int mi = 0;
    int sec = 0;
    if (!field(0, 4, y) || !field(4, 2, mo) || !field(6, 2, d) || !field(9, 2, h) ||
        !field(11, 2, mi) || !field(13, 2, sec) || y < 1970 || y > 2200 || h > 23 || mi > 59 ||
        sec > 60) {
        return std::nullopt;
    }
    const std::chrono::year_month_day ymd{std::chrono::year{y},
                                          std::chrono::month{static_cast<unsigned>(mo)},
                                          std::chrono::day{static_cast<unsigned>(d)}};
    if (!ymd.ok()) {
        return std::nullopt;
    }
    return std::chrono::sys_days{ymd} + std::chrono::hours{h} + std::chrono::minutes{mi} +
           std::chrono::seconds{sec};
}

// When the artifact was quarantined: the timestamp in its name, else its modification time.
std::optional<std::chrono::system_clock::time_point> quarantineTime(const fs::path& corruptPath,
                                                                    std::string_view dbFileName) {
    const auto name = corruptPath.filename().string();
    const auto prefix = corruptDbPrefix(dbFileName);
    if (name.size() > prefix.size()) {
        if (auto parsed = parseQuarantineTimestamp(std::string_view(name).substr(prefix.size()))) {
            return parsed;
        }
    }
    std::error_code ec;
    const auto written = fs::last_write_time(corruptPath, ec);
    if (ec) {
        return std::nullopt;
    }
    return std::chrono::system_clock::now() +
           std::chrono::duration_cast<std::chrono::system_clock::duration>(
               written - fs::file_time_type::clock::now());
}

} // namespace

Result<bool> corruptDbSalvageConfirmed(const fs::path& corruptPath, const fs::path& liveDbPath) {
    std::error_code existsEc;
    if (!fs::exists(liveDbPath, existsEc)) {
        return Error{ErrorCode::FileNotFound, "live DB missing: " + liveDbPath.string()};
    }

    sqlite3* db = nullptr;
    int rc = sqlite3_open_v2(corruptPath.string().c_str(), &db, SQLITE_OPEN_READONLY, nullptr);
    if (rc != SQLITE_OK) {
        std::string err = db ? sqlite3_errmsg(db) : sqlite3_errstr(rc);
        if (db)
            sqlite3_close(db);
        return Error{ErrorCode::DatabaseError, "cannot open corrupt DB: " + err};
    }
    sqlite3_busy_timeout(db, 5000);

    const auto fail = [&](const std::string& what) -> Result<bool> {
        std::string err = what + ": " + sqlite3_errmsg(db);
        sqlite3_close(db);
        return Error{ErrorCode::DatabaseError, err};
    };

    // The connection is read-only, so the attached live DB is opened read-only too.
    sqlite3_stmt* attach = nullptr;
    if (sqlite3_prepare_v2(db, "ATTACH DATABASE ?1 AS live", -1, &attach, nullptr) != SQLITE_OK) {
        return fail("cannot prepare ATTACH");
    }
    const auto live = liveDbPath.string();
    sqlite3_bind_text(attach, 1, live.c_str(), static_cast<int>(live.size()), SQLITE_TRANSIENT);
    rc = sqlite3_step(attach);
    sqlite3_finalize(attach);
    if (rc != SQLITE_DONE) {
        return fail("cannot attach live DB");
    }

    // Salvage copies documents keyed by content hash; it is complete when none is missing.
    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db,
                           "SELECT COUNT(*) FROM main.documents AS c WHERE NOT EXISTS "
                           "(SELECT 1 FROM live.documents AS l WHERE l.sha256_hash = "
                           "c.sha256_hash)",
                           -1, &stmt, nullptr) != SQLITE_OK) {
        return fail("cannot prepare salvage confirmation query");
    }
    rc = sqlite3_step(stmt);
    const int64_t missing = rc == SQLITE_ROW ? sqlite3_column_int64(stmt, 0) : -1;
    sqlite3_finalize(stmt);
    if (rc != SQLITE_ROW) {
        return fail("salvage confirmation query failed");
    }
    sqlite3_close(db);
    return missing == 0;
}

CorruptDbCleanup removeSalvagedCorruptDbs(const fs::path& liveDbPath, std::chrono::seconds minAge,
                                          std::chrono::system_clock::time_point now) {
    CorruptDbCleanup cleanup;
    const fs::path dataDir = liveDbPath.has_parent_path() ? liveDbPath.parent_path() : ".";
    const auto dbFileName = liveDbPath.filename().string();

    for (const auto& corruptPath : listCorruptDbs(dataDir, dbFileName)) {
        const auto name = corruptPath.filename().string();
        const auto quarantinedAt = quarantineTime(corruptPath, dbFileName);
        if (!quarantinedAt) {
            cleanup.retained.push_back({corruptPath, "quarantine time unknown"});
            continue;
        }
        if (now - *quarantinedAt < minAge) {
            cleanup.retained.push_back({corruptPath, "within retention window"});
            continue;
        }

        auto confirmed = corruptDbSalvageConfirmed(corruptPath, liveDbPath);
        if (!confirmed) {
            spdlog::warn("[db_salvage] Keeping corrupt DB '{}': salvage cannot be confirmed ({})",
                         name, confirmed.error().message);
            cleanup.retained.push_back({corruptPath, "unreadable: " + confirmed.error().message});
            continue;
        }
        if (!confirmed.value()) {
            spdlog::warn("[db_salvage] Keeping corrupt DB '{}': it holds documents missing from "
                         "the live DB",
                         name);
            cleanup.retained.push_back({corruptPath, "documents missing from live DB"});
            continue;
        }

        std::error_code removeEc;
        if (!fs::remove(corruptPath, removeEc)) {
            cleanup.errors.push_back("cannot remove " + corruptPath.string() + ": " +
                                     (removeEc ? removeEc.message() : std::string("not found")));
            continue;
        }
        cleanup.removed.push_back(corruptPath);
        for (const auto* suffix : {"-wal", "-shm"}) {
            std::error_code sidecarEc;
            fs::remove(fs::path(corruptPath.string() + suffix), sidecarEc);
            if (sidecarEc) {
                cleanup.errors.push_back("cannot remove " + corruptPath.string() + suffix + ": " +
                                         sidecarEc.message());
            }
        }
        spdlog::info("[db_salvage] Removed corrupt DB '{}': every document is in the live DB",
                     name);
    }

    return cleanup;
}

} // namespace yams::daemon
