#include <yams/daemon/components/db_salvage.h>

#include <sqlite3.h>

#include <spdlog/spdlog.h>

#include <cstring>
#include <filesystem>

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
    execRaw(freshDb, "PRAGMA foreign_keys = OFF");
    execRaw(freshDb, "PRAGMA journal_mode = OFF");

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
            execRaw(freshDb, "PRAGMA foreign_keys = ON");
            sqlite3_close(freshDb);
            return rowResult.error();
        }
    }

    execRaw(freshDb, "PRAGMA foreign_keys = ON");
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

    const std::string dbStem = "yams.db";
    const std::string corruptPrefix = dbStem + ".corrupt-";

    std::vector<fs::path> corruptDbs;
    std::error_code ec;
    for (const auto& entry : fs::directory_iterator(dataDir, ec)) {
        if (ec) {
            ec.clear();
            continue;
        }
        const auto& name = entry.path().filename().string();
        if (name.rfind(corruptPrefix, 0) == 0) {
            // Skip WAL and SHM sibling files
            if (name.size() >= 4 && (name.substr(name.size() - 4) == "-wal" ||
                                     name.substr(name.size() - 4) == "-shm")) {
                continue;
            }
            corruptDbs.push_back(entry.path());
        }
    }

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

} // namespace yams::daemon
