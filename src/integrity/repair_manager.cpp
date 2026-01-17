#include <yams/compression/compression_header.h>
#include <yams/core/magic_numbers.hpp>
#include <yams/crypto/hasher.h>
#include <yams/integrity/repair_manager.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/metadata_repository.h>

#include <sqlite3.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <ranges>
#include <span>

namespace yams::integrity {

RepairManager::RepairManager(storage::IStorageEngine& storage, RepairManagerConfig config)
    : storage_(&storage), config_(std::move(config)), repo_(nullptr) {}

RepairManager::RepairManager(storage::IStorageEngine& storage, metadata::MetadataRepository& repo,
                             RepairManagerConfig config)
    : storage_(&storage), config_(std::move(config)), repo_(&repo) {}

RepairManager::RepairManager(metadata::MetadataRepository& repo)
    : storage_(nullptr), config_(), repo_(&repo) {}

bool RepairManager::attemptRepair(const std::string& blockHash,
                                  const std::vector<RepairStrategy>& order) {
    const auto& strategies = order.empty() ? config_.defaultOrder : order;
    for (auto strategy : strategies) {
        switch (strategy) {
            case RepairStrategy::FromBackup: {
                if (!config_.backupFetcher)
                    continue;
                auto data = config_.backupFetcher(blockHash);
                if (storeIfValid(blockHash, data)) {
                    spdlog::info("Repair succeeded for {} using backup", blockHash.substr(0, 8));
                    return true;
                }
                break;
            }
            case RepairStrategy::FromP2P: {
                if (!config_.p2pFetcher)
                    continue;
                auto data = config_.p2pFetcher(blockHash);
                if (storeIfValid(blockHash, data)) {
                    spdlog::info("Repair succeeded for {} via P2P", blockHash.substr(0, 8));
                    return true;
                }
                break;
            }
            case RepairStrategy::FromParity: {
                if (!config_.parityReconstructor)
                    continue;
                auto data = config_.parityReconstructor(blockHash);
                if (storeIfValid(blockHash, data)) {
                    spdlog::info("Repair succeeded for {} via parity reconstruction",
                                 blockHash.substr(0, 8));
                    return true;
                }
                break;
            }
            case RepairStrategy::FromManifest: {
                if (!config_.manifestReconstructor)
                    continue;
                auto data = config_.manifestReconstructor(blockHash);
                if (storeIfValid(blockHash, data)) {
                    spdlog::info("Repair succeeded for {} via manifest reconstruction",
                                 blockHash.substr(0, 8));
                    return true;
                }
                break;
            }
        }
    }

    spdlog::warn("Repair attempts failed for block {}", blockHash.substr(0, 8));
    return false;
}

bool RepairManager::canRepair(const std::string& blockHash) const {
    (void)blockHash;
    return config_.backupFetcher || config_.p2pFetcher || config_.parityReconstructor ||
           config_.manifestReconstructor;
}

bool RepairManager::storeIfValid(const std::string& blockHash,
                                 const Result<std::vector<std::byte>>& fetchResult) const {
    if (!storage_) {
        spdlog::error("Storage engine not available for repair");
        return false;
    }

    if (!fetchResult.has_value()) {
        spdlog::warn("Repair fetch failed for {}: {}", blockHash.substr(0, 8),
                     fetchResult.error().message);
        return false;
    }

    auto hasher = yams::crypto::createSHA256Hasher();
    hasher->init();
    hasher->update(
        std::span<const std::byte>(fetchResult.value().data(), fetchResult.value().size()));
    auto computed = hasher->finalize();
    if (computed != blockHash) {
        spdlog::warn("Repaired data hash mismatch for {} (computed {})", blockHash.substr(0, 8),
                     computed.substr(0, 8));
        return false;
    }

    auto storeRes =
        storage_->store(blockHash, std::span<const std::byte>(fetchResult.value().data(),
                                                              fetchResult.value().size()));
    if (!storeRes.has_value()) {
        spdlog::error("Failed to store repaired block {}: {}", blockHash.substr(0, 8),
                      storeRes.error().message);
        return false;
    }
    return true;
}

std::shared_ptr<RepairManager> makeRepairManager(storage::IStorageEngine& storage,
                                                 RepairManagerConfig config) {
    return std::make_shared<RepairManager>(storage, std::move(config));
}

// ============================================================================
// Prune Operations
// ============================================================================

std::vector<PruneCandidate>
RepairManager::queryCandidatesForPrune(metadata::MetadataRepository& repo,
                                       const PruneConfig& config) {
    std::vector<PruneCandidate> candidates;

    // Query all documents
    auto docsResult = repo.queryDocuments(metadata::DocumentQueryOptions{});
    if (!docsResult) {
        spdlog::error("Failed to query documents: {}", docsResult.error().message);
        return candidates;
    }

    auto& allDocs = docsResult.value();
    spdlog::debug("Prune query: scanning {} documents", allDocs.size());

    // Filter documents (static helper functions)
    auto matchesCategory = [&](const metadata::DocumentInfo& doc) -> bool {
        if (config.categories.empty())
            return true;
        auto category = magic::getPruneCategory(doc.filePath, doc.fileExtension);
        return std::ranges::any_of(config.categories, [category](const auto& cat) {
            return magic::matchesPruneGroup(category, cat);
        });
    };

    auto meetsAgeCriteria = [&](const metadata::DocumentInfo& doc) -> bool {
        if (config.minAge.count() == 0)
            return true;
        auto now = std::chrono::system_clock::now();
        auto fileAge = std::chrono::duration_cast<std::chrono::seconds>(now - doc.modifiedTime);
        return fileAge >= config.minAge;
    };

    auto meetsSizeCriteria = [&](const metadata::DocumentInfo& doc) -> bool {
        return doc.fileSize >= config.minSize && doc.fileSize <= config.maxSize;
    };

    auto meetsExtensionCriteria = [&](const metadata::DocumentInfo& doc) -> bool {
        if (config.extensions.empty())
            return true;
        return std::ranges::any_of(config.extensions, [&](const auto& ext) {
            return doc.fileExtension == ext || doc.fileExtension == ("." + ext);
        });
    };

    // Filter and build candidates
    for (const auto& doc : allDocs) {
        if (!matchesCategory(doc))
            continue;
        if (!meetsAgeCriteria(doc))
            continue;
        if (!meetsSizeCriteria(doc))
            continue;
        if (!meetsExtensionCriteria(doc))
            continue;

        PruneCandidate candidate;
        candidate.hash = doc.sha256Hash;
        candidate.path = doc.filePath;
        candidate.fileSize = doc.fileSize;
        candidate.modifiedTime = doc.modifiedTime;
        candidate.category =
            magic::getPruneCategoryName(magic::getPruneCategory(doc.filePath, doc.fileExtension));

        candidates.push_back(std::move(candidate));
    }

    spdlog::info("Prune query: found {} candidates", candidates.size());
    return candidates;
}

bool RepairManager::matchesCategory(const metadata::DocumentInfo& doc,
                                    std::span<const std::string> categories) const noexcept {
    if (categories.empty())
        return true;

    auto category = magic::getPruneCategory(doc.filePath, doc.fileExtension);
    return std::ranges::any_of(categories, [category](const auto& cat) {
        return magic::matchesPruneGroup(category, cat);
    });
}

bool RepairManager::meetsAgeCriteria(const metadata::DocumentInfo& doc,
                                     std::chrono::seconds minAge) const noexcept {
    if (minAge.count() == 0)
        return true;

    auto now = std::chrono::system_clock::now();
    auto fileAge = std::chrono::duration_cast<std::chrono::seconds>(now - doc.modifiedTime);
    return fileAge >= minAge;
}

bool RepairManager::meetsSizeCriteria(const metadata::DocumentInfo& doc, int64_t minSize,
                                      int64_t maxSize) const noexcept {
    return doc.fileSize >= minSize && doc.fileSize <= maxSize;
}

Result<PruneResult> RepairManager::pruneFiles(const PruneConfig& config,
                                              std::function<void(uint64_t, uint64_t)> progress) {
    if (!repo_) {
        return Error{ErrorCode::InvalidArgument, "MetadataRepository not initialized"};
    }

    // Query all documents
    auto docsResult = repo_->queryDocuments(metadata::DocumentQueryOptions{});
    if (!docsResult) {
        spdlog::error("Failed to query documents: {}", docsResult.error().message);
        return docsResult.error();
    }

    auto& allDocs = docsResult.value();
    spdlog::info("Prune: scanning {} documents", allDocs.size());

    // Filter using std::ranges (lazy evaluation)
    auto candidates =
        allDocs |
        std::views::filter([&](const auto& d) { return matchesCategory(d, config.categories); }) |
        std::views::filter([&](const auto& d) { return meetsAgeCriteria(d, config.minAge); }) |
        std::views::filter(
            [&](const auto& d) { return meetsSizeCriteria(d, config.minSize, config.maxSize); });

    // Materialize candidates for processing
    std::vector<metadata::DocumentInfo> toProcess;
    std::ranges::copy(candidates, std::back_inserter(toProcess));

    spdlog::info("Prune: found {} candidates (dry_run={})", toProcess.size(), config.dryRun);

    PruneResult result;
    uint64_t processed = 0;
    const uint64_t total = toProcess.size();

    for (const auto& doc : toProcess) {
        // Report progress
        if (progress && processed % 100 == 0) {
            progress(processed, total);
        }

        // Track category stats
        auto categoryName = std::string(
            magic::getPruneCategoryName(magic::getPruneCategory(doc.filePath, doc.fileExtension)));
        result.categoryCounts[categoryName]++;
        result.categorySizes[categoryName] += doc.fileSize;

        if (!config.dryRun) {
            // Delete from metadata only
            auto delResult = repo_->deleteDocument(doc.id);
            if (!delResult) {
                spdlog::warn("Failed to delete metadata for {}: {}", doc.filePath,
                             delResult.error().message);
                result.failedPaths.push_back(doc.filePath);
                result.filesFailed++;
                processed++;
                continue;
            }

            result.deletedPaths.push_back(doc.filePath);
            result.filesDeleted++;
            result.totalBytesFreed += doc.fileSize;
        } else {
            // Dry run: just count
            result.filesDeleted++;
            result.totalBytesFreed += doc.fileSize;
        }

        processed++;
    }

    // Final progress report
    if (progress) {
        progress(total, total);
    }

    spdlog::info("Prune complete: deleted={} failed={} bytes_freed={}", result.filesDeleted,
                 result.filesFailed, result.totalBytesFreed);

    return result;
}

// ============================================================================
// Path Tree Repair Operations
// ============================================================================

Result<PathTreeRepairResult>
RepairManager::repairPathTree(std::function<void(uint64_t, uint64_t)> progress) {
    if (!repo_) {
        return Error{ErrorCode::InvalidArgument, "MetadataRepository not initialized"};
    }

    PathTreeRepairResult result;

    // Query all documents
    auto docsResult = repo_->queryDocuments(metadata::DocumentQueryOptions{});
    if (!docsResult) {
        spdlog::error("Failed to query documents for path tree repair: {}",
                      docsResult.error().message);
        return docsResult.error();
    }

    auto& allDocs = docsResult.value();
    const uint64_t total = allDocs.size();
    spdlog::info("Path tree repair: scanning {} documents", total);

    uint64_t processed = 0;
    for (const auto& doc : allDocs) {
        result.documentsScanned++;

        // Report progress
        if (progress && processed % 100 == 0) {
            progress(processed, total);
        }

        // Skip documents without a valid path
        if (doc.filePath.empty()) {
            processed++;
            continue;
        }

        // Check if this document already has a path tree entry
        auto existingNode = repo_->findPathTreeNodeByFullPath(doc.filePath);
        if (existingNode && existingNode.value().has_value()) {
            // Node exists, skip (or could update doc_count if needed)
            processed++;
            continue;
        }

        // Create path tree entry for this document
        try {
            auto treeRes = repo_->upsertPathTreeForDocument(doc, doc.id, true /* isNewDocument */,
                                                            std::span<const float>());
            if (treeRes) {
                result.nodesCreated++;
                spdlog::debug("Path tree repair: created entry for '{}'", doc.filePath);
            } else {
                result.errors++;
                spdlog::warn("Path tree repair: failed for '{}': {}", doc.filePath,
                             treeRes.error().message);
            }
        } catch (const std::exception& e) {
            result.errors++;
            spdlog::warn("Path tree repair: exception for '{}': {}", doc.filePath, e.what());
        }

        processed++;
    }

    // Final progress report
    if (progress) {
        progress(total, total);
    }

    spdlog::info("Path tree repair complete: scanned={} created={} errors={}",
                 result.documentsScanned, result.nodesCreated, result.errors);

    return result;
}

// ============================================================================
// Block References Repair Operations
// ============================================================================

Result<BlockRefsRepairResult>
RepairManager::repairBlockReferences(const std::filesystem::path& objectsPath,
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

} // namespace yams::integrity
