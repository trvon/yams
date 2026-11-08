#include <yams/compression/compression_header.h>
#include <yams/storage/block_reference_repair.h>

#include <sqlite3.h>
#include <spdlog/spdlog.h>

#include <fstream>
#include <string>
#include <vector>

namespace yams::storage {

Result<BlockRefsRepairResult>
repairBlockReferences(const std::filesystem::path& objectsPath,
                      const std::filesystem::path& refsDbPath, bool dryRun,
                      std::function<void(uint64_t, uint64_t)> progress) {
    namespace fs = std::filesystem;

    BlockRefsRepairResult result;

    // Validate paths
    if (!fs::exists(objectsPath)) {
        return Error{ErrorCode::NotFound, "Objects directory not found: " + objectsPath.string()};
    }
    if (!fs::exists(refsDbPath)) {
        return Error{ErrorCode::NotFound, "refs.db not found: " + refsDbPath.string()};
    }

    // Open refs database
    sqlite3* db = nullptr;
    if (sqlite3_open(refsDbPath.string().c_str(), &db) != SQLITE_OK) {
        std::string err = sqlite3_errmsg(db);
        sqlite3_close(db);
        return Error{ErrorCode::DatabaseError, "Failed to open refs.db: " + err};
    }

    // Count total files first for progress reporting
    uint64_t totalFiles = 0;
    for (const auto& shardDir : fs::directory_iterator(objectsPath)) {
        if (!fs::is_directory(shardDir))
            continue;
        for (const auto& entry : fs::directory_iterator(shardDir.path())) {
            if (fs::is_regular_file(entry)) {
                std::string filename = entry.path().filename().string();
                // Skip manifest files
                if (filename.size() > 9 && filename.substr(filename.size() - 9) == ".manifest")
                    continue;
                totalFiles++;
            }
        }
    }

    spdlog::info("Block refs repair: scanning {} CAS files (dry_run={})", totalFiles, dryRun);

    // Enable WAL mode for better concurrency with daemon
    sqlite3_exec(db, "PRAGMA journal_mode=WAL;", nullptr, nullptr, nullptr);
    sqlite3_exec(db, "PRAGMA busy_timeout=5000;", nullptr, nullptr, nullptr);

    // Prepare update statement
    sqlite3_stmt* updateStmt = nullptr;
    if (!dryRun) {
        const char* sql = "UPDATE block_references SET block_size = ?, uncompressed_size = ? WHERE "
                          "block_hash = ?";
        if (sqlite3_prepare_v2(db, sql, -1, &updateStmt, nullptr) != SQLITE_OK) {
            std::string err = sqlite3_errmsg(db);
            sqlite3_close(db);
            return Error{ErrorCode::DatabaseError, "Failed to prepare update statement: " + err};
        }

        // Start a transaction for batch updates
#if YAMS_LIBSQL_BACKEND
        sqlite3_exec(db, "BEGIN;", nullptr, nullptr, nullptr);
#else
        sqlite3_exec(db, "BEGIN IMMEDIATE;", nullptr, nullptr, nullptr);
#endif
    }

    // Prepare select statement to check current values
    sqlite3_stmt* selectStmt = nullptr;
    {
        const char* sql = "SELECT block_size, uncompressed_size FROM block_references WHERE "
                          "block_hash = ?";
        if (sqlite3_prepare_v2(db, sql, -1, &selectStmt, nullptr) != SQLITE_OK) {
            std::string err = sqlite3_errmsg(db);
            if (updateStmt)
                sqlite3_finalize(updateStmt);
            sqlite3_close(db);
            return Error{ErrorCode::DatabaseError, "Failed to prepare select statement: " + err};
        }
    }

    // Buffer for reading compression headers
    std::vector<std::byte> headerBuffer(compression::CompressionHeader::SIZE);

    // Scan all CAS files
    uint64_t processed = 0;
    for (const auto& shardDir : fs::directory_iterator(objectsPath)) {
        if (!fs::is_directory(shardDir))
            continue;

        std::string shardName = shardDir.path().filename().string();

        for (const auto& entry : fs::directory_iterator(shardDir.path())) {
            if (!fs::is_regular_file(entry))
                continue;

            std::string filename = entry.path().filename().string();

            // Skip manifest files
            if (filename.size() > 9 && filename.substr(filename.size() - 9) == ".manifest")
                continue;

            result.blocksScanned++;
            processed++;

            // Report progress
            if (progress && processed % 1000 == 0) {
                progress(processed, totalFiles);
            }

            // Reconstruct full hash: shard (2 chars) + filename
            std::string blockHash = shardName + filename;

            // Get on-disk file size
            std::error_code ec;
            auto diskSize = static_cast<int64_t>(fs::file_size(entry.path(), ec));
            if (ec) {
                spdlog::warn("Block refs repair: cannot get size for {}: {}",
                             blockHash.substr(0, 8), ec.message());
                result.errors++;
                continue;
            }

            result.totalDiskBytes += static_cast<uint64_t>(diskSize);

            // Read first 64 bytes to check for compression header
            int64_t uncompressedSize = diskSize; // Default: same as disk size (uncompressed)

            std::ifstream file(entry.path(), std::ios::binary);
            if (file && diskSize >= static_cast<int64_t>(compression::CompressionHeader::SIZE)) {
                file.read(reinterpret_cast<char*>(headerBuffer.data()),
                          static_cast<std::streamsize>(compression::CompressionHeader::SIZE));
                if (file.gcount() ==
                    static_cast<std::streamsize>(compression::CompressionHeader::SIZE)) {
                    auto headerResult = compression::CompressionHeader::parse(headerBuffer);
                    if (headerResult && headerResult.value().validate()) {
                        // This is a compressed block
                        uncompressedSize =
                            static_cast<int64_t>(headerResult.value().uncompressedSize);
                        result.compressedBlocks++;
                    } else {
                        result.uncompressedBlocks++;
                    }
                } else {
                    result.uncompressedBlocks++;
                }
            } else {
                result.uncompressedBlocks++;
            }

            result.totalUncompressedBytes += static_cast<uint64_t>(uncompressedSize);

            // Check current values in database
            sqlite3_reset(selectStmt);
            sqlite3_bind_text(selectStmt, 1, blockHash.c_str(), -1, SQLITE_TRANSIENT);

            if (sqlite3_step(selectStmt) == SQLITE_ROW) {
                int64_t currentBlockSize = sqlite3_column_int64(selectStmt, 0);
                // Check if uncompressed_size is NULL
                bool hasUncompressedSize = sqlite3_column_type(selectStmt, 1) != SQLITE_NULL;
                int64_t currentUncompressed =
                    hasUncompressedSize ? sqlite3_column_int64(selectStmt, 1) : 0;

                // Only update if values differ
                bool needsUpdate =
                    (currentBlockSize != diskSize) ||
                    (!hasUncompressedSize || currentUncompressed != uncompressedSize);

                if (needsUpdate) {
                    if (!dryRun) {
                        sqlite3_reset(updateStmt);
                        sqlite3_bind_int64(updateStmt, 1, diskSize);
                        sqlite3_bind_int64(updateStmt, 2, uncompressedSize);
                        sqlite3_bind_text(updateStmt, 3, blockHash.c_str(), -1, SQLITE_TRANSIENT);

                        int rc = sqlite3_step(updateStmt);
                        if (rc != SQLITE_DONE) {
                            spdlog::warn("Block refs repair: failed to update {}: {} (rc={})",
                                         blockHash.substr(0, 8), sqlite3_errmsg(db), rc);
                            result.errors++;
                            continue;
                        }
                    }
                    result.blocksUpdated++;

                    // Commit every 10000 updates to avoid holding lock too long
                    if (!dryRun && result.blocksUpdated % 10000 == 0) {
#if YAMS_LIBSQL_BACKEND
                        sqlite3_exec(db, "COMMIT; BEGIN;", nullptr, nullptr, nullptr);
#else
                        sqlite3_exec(db, "COMMIT; BEGIN IMMEDIATE;", nullptr, nullptr, nullptr);
#endif
                    }
                } else {
                    result.blocksSkipped++;
                }
            } else {
                // Block not found in refs.db - this is unexpected but not an error
                spdlog::debug("Block refs repair: {} not in refs.db", blockHash.substr(0, 8));
                result.blocksSkipped++;
            }
        }
    }

    // Final progress
    if (progress) {
        progress(totalFiles, totalFiles);
    }

    // Commit final transaction
    if (!dryRun && updateStmt) {
        sqlite3_exec(db, "COMMIT;", nullptr, nullptr, nullptr);
    }

    // Cleanup prepared statements
    sqlite3_finalize(selectStmt);
    if (updateStmt)
        sqlite3_finalize(updateStmt);

    // Refresh the ref_statistics totals from block_references
    if (!dryRun && result.blocksUpdated > 0) {
        spdlog::info("Block refs repair: refreshing ref_statistics totals...");
        const char* refreshSql = R"(
            UPDATE ref_statistics SET stat_value = (
                SELECT COALESCE(SUM(block_size), 0) FROM block_references WHERE ref_count > 0
            ), updated_at = strftime('%s', 'now')
            WHERE stat_name = 'total_bytes';

            UPDATE ref_statistics SET stat_value = (
                SELECT COALESCE(SUM(COALESCE(uncompressed_size, block_size)), 0)
                FROM block_references WHERE ref_count > 0
            ), updated_at = strftime('%s', 'now')
            WHERE stat_name = 'total_uncompressed_bytes';
        )";
        char* errMsg = nullptr;
        if (sqlite3_exec(db, refreshSql, nullptr, nullptr, &errMsg) != SQLITE_OK) {
            spdlog::warn("Block refs repair: failed to refresh ref_statistics: {}",
                         errMsg ? errMsg : "unknown");
            sqlite3_free(errMsg);
        } else {
            spdlog::info("Block refs repair: ref_statistics totals refreshed");
        }
    }

    sqlite3_close(db);

    spdlog::info("Block refs repair complete: scanned={} updated={} skipped={} errors={} "
                 "compressed={} uncompressed={}",
                 result.blocksScanned, result.blocksUpdated, result.blocksSkipped, result.errors,
                 result.compressedBlocks, result.uncompressedBlocks);

    return result;
}

} // namespace yams::storage
