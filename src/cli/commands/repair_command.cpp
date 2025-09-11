#include <sqlite3.h>
#include <spdlog/spdlog.h>
#include <atomic>
#include <fcntl.h>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <thread>
#include <unistd.h>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/system_executor.hpp>
#include <yams/cli/command.h>
#include <yams/cli/progress_indicator.h>
#include <yams/cli/yams_cli.h>
#include <yams/detection/file_type_detector.h>
#include <yams/metadata/metadata_repository.h>

#include <yams/app/services/services.hpp>
#include <yams/daemon/client/asio_transport.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/extraction/extraction_util.h>
#include <yams/vector/embedding_generator.h>
#include <yams/vector/sqlite_vec_backend.h>
#include <yams/vector/vector_database.h>
#include <yams/vector/vector_index_manager.h>

namespace yams::cli {

class RepairCommand : public ICommand {
public:
    std::string getName() const override { return "repair"; }

    std::string getDescription() const override { return "Repair and maintain storage integrity"; }

    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;

        auto* cmd = app.add_subcommand("repair", getDescription());

        cmd->add_flag("--orphans", repairOrphans_, "Clean orphaned metadata entries");
        cmd->add_flag("--mime", repairMime_, "Repair missing MIME types");
        cmd->add_flag("--chunks", repairChunks_, "Clean orphaned chunk files");
        cmd->add_flag("--embeddings", repairEmbeddings_, "Generate missing vector embeddings");
        cmd->add_flag("--fts5", repairFts5_, "Rebuild FTS5 index for documents (best-effort)");
        cmd->add_flag("--optimize", optimizeDb_, "Optimize and vacuum database");
        cmd->add_flag("--checksums", verifyChecksums_, "Verify and repair checksums");
        cmd->add_flag("--duplicates", mergeDuplicates_, "Find and optionally merge duplicates");
        cmd->add_flag("--downloads", repairDownloads_,
                      "Repair download documents: add tags/metadata and normalize names");
        // Embeddings-specific options
        cmd->add_option("--include-mime", includeMime_,
                        "Additional MIME types to embed (e.g., application/pdf). Repeatable.")
            ->type_size(-1);
        cmd->add_option("--model", embeddingModel_, "Embedding model to use (overrides preferred)");
        cmd->add_flag("--all", repairAll_, "Run all repair operations");
        cmd->add_flag("--stop-daemon", stopDaemon_,
                      "Attempt to stop daemon before vector DB operations");
        cmd->add_option("--embed-timeout-ms", embedTimeoutMs_, "Timeout for daemon embed RPC (ms)");
        cmd->add_option("--embed-retries", embedRetries_,
                        "Retries for daemon embed RPC (default 2)");
        cmd->add_flag("--dry-run", dryRun_, "Preview changes without applying");
        cmd->add_flag("--foreground", foreground_,
                      "Run embeddings repair in the foreground (stream progress)");
        cmd->add_flag("-v,--verbose", verbose_, "Show detailed progress");
        cmd->add_flag("--force", force_, "Skip confirmation prompts");

        cmd->callback([this]() {
            auto result = execute();
            if (!result) {
                spdlog::error("Repair failed: {}", result.error().message);
                std::exit(1);
            }
        });
    }

    Result<void> execute() override {
        try {
            if (stopDaemon_) {
                // Best-effort stop daemon like doctor command
                try {
                    yams::daemon::ClientConfig ccfg;
                    ccfg.singleUseConnections = true;
                    ccfg.requestTimeout = std::chrono::seconds(5);
                    yams::daemon::DaemonClient shut(ccfg);
                    std::promise<Result<void>> prom;
                    auto fut = prom.get_future();
                    boost::asio::co_spawn(
                        boost::asio::system_executor{},
                        [&]() -> boost::asio::awaitable<void> {
                            auto r = co_await shut.shutdown(true);
                            prom.set_value(std::move(r));
                            co_return;
                        },
                        boost::asio::detached);
                    (void)fut.wait_for(std::chrono::seconds(6));
                } catch (...) {
                }
            }

            auto ensured = cli_->ensureStorageInitialized();
            if (!ensured) {
                return ensured;
            }

            auto store = cli_->getContentStore();
            if (!store) {
                return Error{ErrorCode::NotInitialized, "Content store not initialized"};
            }

            auto metadataRepo = cli_->getMetadataRepository();
            if (!metadataRepo) {
                return Error{ErrorCode::NotInitialized, "Metadata repository not initialized"};
            }

            // If no specific repair requested, default to orphans
            if (!repairOrphans_ && !repairMime_ && !repairChunks_ && !repairEmbeddings_ &&
                !repairFts5_ && !optimizeDb_ && !verifyChecksums_ && !mergeDuplicates_ &&
                !repairAll_) {
                repairOrphans_ = true;
            }

            // If --all is specified, enable all repairs
            if (repairAll_) {
                repairOrphans_ = true;
                repairMime_ = true;
                repairChunks_ = true;
                repairEmbeddings_ = true;
                repairFts5_ = true;
                optimizeDb_ = true;
                repairDownloads_ = true;
                // verifyChecksums_ = true;  // Not implemented yet
                // mergeDuplicates_ = true;  // Not implemented yet
            }

            std::cout << "═══════════════════════════════════════════════════════════\n";
            std::cout << "                    YAMS Storage Repair                    \n";
            std::cout << "═══════════════════════════════════════════════════════════\n\n";

            if (dryRun_) {
                std::cout << "[DRY RUN MODE] No changes will be made\n\n";
            }

            bool anyRepairs = false;

            // Clean orphaned metadata
            if (repairOrphans_) {
                auto result = cleanOrphanedMetadata(store, metadataRepo);
                if (!result) {
                    return result;
                }
                anyRepairs = true;
            }

            // Repair MIME types
            if (repairMime_) {
                auto result = repairMimeTypes(store, metadataRepo);
                if (!result) {
                    return result;
                }
                anyRepairs = true;
            }

            // Clean orphaned chunks
            if (repairChunks_) {
                auto result = cleanOrphanedChunks(store);
                if (!result) {
                    return result;
                }
                anyRepairs = true;
            }

            // Generate missing embeddings
            if (repairEmbeddings_) {
                auto result = generateMissingEmbeddings(store, metadataRepo);
                if (!result) {
                    return result;
                }
                anyRepairs = true;
            }

            // Rebuild FTS5 index (best-effort)
            if (repairFts5_) {
                auto appCtx = cli_->getAppContext();
                if (!appCtx) {
                    return Error{ErrorCode::NotInitialized,
                                 "AppContext not available for FTS5 rebuild"};
                }
                auto result = rebuildFts5Index(*appCtx);
                if (!result) {
                    return result;
                }
                anyRepairs = true;
            }

            // Optimize database
            if (optimizeDb_) {
                auto result = optimizeDatabase();
                if (!result) {
                    return result;
                }
                anyRepairs = true;
            }

            if (repairDownloads_) {
                auto result = repairDownloads(metadataRepo);
                if (!result)
                    return result;
                anyRepairs = true;
            }

            // Verify checksums (not implemented yet)
            if (verifyChecksums_) {
                std::cout << "Checksum verification: Not implemented yet\n";
            }

            // Merge duplicates (not implemented yet)
            if (mergeDuplicates_) {
                std::cout << "Duplicate merging: Not implemented yet\n";
            }

            if (!anyRepairs) {
                std::cout << "No repair operations selected.\n";
            } else if (!dryRun_) {
                std::cout << "\n✓ Repair operations completed successfully\n";
            }

            std::cout << "═══════════════════════════════════════════════════════════\n";

            return Result<void>();

        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string(e.what())};
        }
    }

private:
    YamsCLI* cli_ = nullptr;
    bool repairOrphans_ = false;
    bool repairMime_ = false;
    bool repairChunks_ = false;
    bool repairEmbeddings_ = false;
    bool repairFts5_ = false;
    bool optimizeDb_ = false;
    bool verifyChecksums_ = false;
    bool mergeDuplicates_ = false;
    bool repairDownloads_ = false;
    bool foreground_ = false; // default background
    std::vector<std::string> includeMime_;
    std::string embeddingModel_;
    bool repairAll_ = false;
    bool dryRun_ = false;
    bool verbose_ = false;
    bool force_ = false;
    bool stopDaemon_ = false;
    int embedTimeoutMs_ = 600000;
    int embedRetries_ = 2;

    // Helper to rebuild FTS5 index for all documents (best-effort)
    Result<void> rebuildFts5Index(const app::services::AppContext& ctx);

    Result<void>
    cleanOrphanedMetadata(std::shared_ptr<api::IContentStore> store,
                          std::shared_ptr<metadata::IMetadataRepository> metadataRepo) {
        std::cout << "Cleaning orphaned metadata entries...\n";

        // Get all documents from metadata
        auto documentsResult = metadataRepo->findDocumentsByPath("%");
        if (!documentsResult) {
            return Error{ErrorCode::DatabaseError,
                         "Failed to query documents: " + documentsResult.error().message};
        }
        int orphanedCount = 0;
        int cleanedCount = 0;
        std::vector<int64_t> orphanedIds;

        ProgressIndicator progress(ProgressIndicator::Style::Percentage);
        if (!verbose_ && documentsResult.value().size() > 100) {
            progress.start("Scanning for orphans");
        }

        size_t current = 0;
        for (const auto& doc : documentsResult.value()) {
            current++;

            // Check if document exists in storage
            auto existsResult = store->exists(doc.sha256Hash);
            if (!existsResult || !existsResult.value()) {
                orphanedCount++;
                orphanedIds.push_back(doc.id);

                if (verbose_) {
                    std::cout << "  Orphaned: " << doc.filePath << " ("
                              << doc.sha256Hash.substr(0, 12) << "...)\n";
                }
            }

            // Update progress
            if (!verbose_ && documentsResult.value().size() > 100 && current % 10 == 0) {
                progress.update(current, documentsResult.value().size());
            }
        }

        progress.stop();

        if (orphanedCount == 0) {
            std::cout << "  ✓ No orphaned metadata entries found\n";
            return Result<void>();
        }

        std::cout << "  Found " << orphanedCount << " orphaned metadata entries\n";

        if (!dryRun_) {
            // Confirm deletion unless forced
            if (!force_) {
                std::cout << "  Delete " << orphanedCount << " orphaned entries? (y/N): ";
                std::string response;
                std::getline(std::cin, response);
                if (response != "y" && response != "Y") {
                    std::cout << "  Cleanup cancelled.\n";
                    return Result<void>();
                }
            }

            // Delete orphaned entries
            ProgressIndicator deleteProgress(ProgressIndicator::Style::Percentage);
            if (orphanedIds.size() > 100) {
                deleteProgress.start("Cleaning orphans");
            }

            current = 0;
            for (int64_t id : orphanedIds) {
                current++;
                auto deleteResult = metadataRepo->deleteDocument(id);
                if (deleteResult) {
                    cleanedCount++;
                } else if (verbose_) {
                    std::cout << "  Failed to delete metadata id " << id << ": "
                              << deleteResult.error().message << "\n";
                }

                if (orphanedIds.size() > 100 && current % 10 == 0) {
                    deleteProgress.update(current, orphanedIds.size());
                }
            }

            deleteProgress.stop();

            std::cout << "  ✓ Cleaned " << cleanedCount << " orphaned metadata entries\n";
        } else {
            std::cout << "  [DRY RUN] Would clean " << orphanedCount << " orphaned entries\n";
        }

        return Result<void>();
    }

    Result<void> repairDownloads(std::shared_ptr<metadata::IMetadataRepository> metadataRepo) {
        std::cout << "Repairing downloaded documents (tags/metadata/filenames)...\n";

        // Fetch all docs
        auto docsResult = metadataRepo->findDocumentsByPath("%");
        if (!docsResult) {
            return Error{ErrorCode::DatabaseError,
                         "Failed to query documents: " + docsResult.error().message};
        }

        auto is_url = [](const std::string& s) { return s.find("://") != std::string::npos; };
        auto extract_host = [](const std::string& url) -> std::string {
            auto p = url.find("://");
            if (p == std::string::npos)
                return {};
            auto rest = url.substr(p + 3);
            auto slash = rest.find('/');
            return (slash == std::string::npos) ? rest : rest.substr(0, slash);
        };
        auto extract_scheme = [](const std::string& url) -> std::string {
            auto p = url.find("://");
            return (p == std::string::npos) ? std::string{} : url.substr(0, p);
        };
        auto filename_from_url = [](std::string url) -> std::string {
            auto lastSlash = url.find_last_of('/');
            if (lastSlash != std::string::npos) {
                url = url.substr(lastSlash + 1);
            }
            auto q = url.find('?');
            if (q != std::string::npos)
                url = url.substr(0, q);
            if (url.empty())
                url = "downloaded_file";
            return url;
        };

        size_t updated = 0;
        for (auto doc : docsResult.value()) {
            const std::string originalPath = doc.filePath;

            // Determine a candidate source URL
            std::string sourceUrl;
            if (is_url(doc.filePath)) {
                sourceUrl = doc.filePath;
            } else {
                // Try reading existing metadata (if available); ignoring if no accessor exists
                // (Optionally, if repo has getMetadataValue(docId, "source_url"), use it)
            }

            if (sourceUrl.empty()) {
                // Not obviously a download; skip
                continue;
            }

            // Normalize name for retrieval by name
            std::string filename = filename_from_url(sourceUrl);
            std::string ext;
            auto dotPos = filename.rfind('.');
            if (dotPos != std::string::npos) {
                ext = filename.substr(dotPos);
            }

            // Update document path/name
            doc.filePath = filename;
            doc.fileName = filename;
            doc.fileExtension = ext;

            // Persist update
            if (auto up = metadataRepo->updateDocument(doc); !up) {
                spdlog::warn("Repair(downloads): updateDocument failed for id={} path='{}': {}",
                             doc.id, originalPath, up.error().message);
            } else {
                updated++;
            }

            // Set download tags/metadata
            try {
                metadataRepo->setMetadata(doc.id, "source_url", metadata::MetadataValue(sourceUrl));
                const auto host = extract_host(sourceUrl);
                const auto scheme = extract_scheme(sourceUrl);

                metadataRepo->setMetadata(doc.id, "tag", metadata::MetadataValue("downloaded"));
                if (!host.empty())
                    metadataRepo->setMetadata(doc.id, "tag",
                                              metadata::MetadataValue("host:" + host));
                if (!scheme.empty())
                    metadataRepo->setMetadata(doc.id, "tag",
                                              metadata::MetadataValue("scheme:" + scheme));
            } catch (const std::exception& e) {
                spdlog::warn("Repair(downloads): tagging failed for id={} path='{}': {}", doc.id,
                             originalPath, e.what());
            }
        }

        std::cout << "✓ Downloads repair updated " << updated << " document(s)\n";
        return Result<void>();
    }

    Result<void> repairMimeTypes(std::shared_ptr<api::IContentStore> /*store*/,
                                 std::shared_ptr<metadata::IMetadataRepository> metadataRepo) {
        std::cout << "\nRepairing missing MIME types...\n";

        // Initialize file type detector
        detection::FileTypeDetectorConfig config;
        config.patternsFile = YamsCLI::findMagicNumbersFile();
        config.useCustomPatterns = !config.patternsFile.empty();
        detection::FileTypeDetector::instance().initialize(config);

        // Get all documents from metadata
        auto documentsResult = metadataRepo->findDocumentsByPath("%");
        if (!documentsResult) {
            return Error{ErrorCode::DatabaseError,
                         "Failed to query documents: " + documentsResult.error().message};
        }

        int missingMimeCount = 0;
        int repairedCount = 0;
        std::vector<std::pair<int64_t, std::string>> toRepair;

        ProgressIndicator progress(ProgressIndicator::Style::Percentage);
        if (!verbose_ && documentsResult.value().size() > 100) {
            progress.start("Scanning MIME types");
        }

        size_t current = 0;
        for (const auto& doc : documentsResult.value()) {
            current++;

            // Check if MIME type is missing or unknown
            if (doc.mimeType.empty() || doc.mimeType == "application/octet-stream") {
                // Try to detect MIME type from extension
                std::string detectedMime;

                if (!doc.fileExtension.empty()) {
                    detectedMime =
                        detection::FileTypeDetector::getMimeTypeFromExtension(doc.fileExtension);
                }

                // If still unknown, try from filename
                if (detectedMime.empty() || detectedMime == "application/octet-stream") {
                    auto pos = doc.fileName.rfind('.');
                    if (pos != std::string::npos) {
                        std::string ext = doc.fileName.substr(pos);
                        detectedMime = detection::FileTypeDetector::getMimeTypeFromExtension(ext);
                    }
                }

                if (!detectedMime.empty() && detectedMime != "application/octet-stream") {
                    missingMimeCount++;
                    toRepair.push_back({doc.id, detectedMime});

                    if (verbose_) {
                        std::cout << "  Missing MIME: " << doc.fileName << " -> " << detectedMime
                                  << "\n";
                    }
                }
            }

            // Update progress
            if (!verbose_ && documentsResult.value().size() > 100 && current % 10 == 0) {
                progress.update(current, documentsResult.value().size());
            }
        }

        progress.stop();

        if (missingMimeCount == 0) {
            std::cout << "  ✓ All documents have valid MIME types\n";
            return Result<void>();
        }

        std::cout << "  Found " << missingMimeCount << " documents with missing MIME types\n";

        if (!dryRun_) {
            // Repair MIME types
            ProgressIndicator repairProgress(ProgressIndicator::Style::Percentage);
            if (toRepair.size() > 100) {
                repairProgress.start("Repairing MIME types");
            }

            current = 0;
            for (const auto& [id, mimeType] : toRepair) {
                current++;

                // Get the document
                auto docResult = metadataRepo->getDocument(id);
                if (docResult && docResult.value()) {
                    auto doc = *docResult.value();
                    doc.mimeType = mimeType;

                    // Update the document
                    auto updateResult = metadataRepo->updateDocument(doc);
                    if (updateResult) {
                        repairedCount++;
                    } else if (verbose_) {
                        std::cout << "  Failed to update document id " << id << ": "
                                  << updateResult.error().message << "\n";
                    }
                }

                if (toRepair.size() > 100 && current % 10 == 0) {
                    repairProgress.update(current, toRepair.size());
                }
            }

            repairProgress.stop();

            std::cout << "  ✓ Repaired MIME types for " << repairedCount << " documents\n";
        } else {
            std::cout << "  [DRY RUN] Would repair " << missingMimeCount << " MIME types\n";
        }

        return Result<void>();
    }

    Result<void> cleanOrphanedChunks(std::shared_ptr<api::IContentStore> /*store*/) {
        std::cout << "\nCleaning orphaned chunks...\n";

        namespace fs = std::filesystem;

        // Open refs database
        fs::path refsDbPath = cli_->getDataPath() / "storage" / "refs.db";
        if (!fs::exists(refsDbPath)) {
            std::cout << "  No reference database found\n";
            return Result<void>();
        }

        sqlite3* db;
        if (sqlite3_open(refsDbPath.string().c_str(), &db) != SQLITE_OK) {
            return Error{ErrorCode::DatabaseError,
                         "Failed to open refs.db: " + std::string(sqlite3_errmsg(db))};
        }

        // First, get all REFERENCED blocks (ref_count > 0)
        std::set<std::string> referencedHashes;
        sqlite3_stmt* stmt;
        const char* sql = "SELECT block_hash FROM block_references WHERE ref_count > 0";

        if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
            sqlite3_close(db);
            return Error{ErrorCode::DatabaseError,
                         "Failed to prepare query: " + std::string(sqlite3_errmsg(db))};
        }

        while (sqlite3_step(stmt) == SQLITE_ROW) {
            const char* hash = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
            if (hash) {
                referencedHashes.insert(hash);
            }
        }
        sqlite3_finalize(stmt);

        std::cout << "  Found " << referencedHashes.size() << " referenced chunks in database\n";

        // Now, get all files that exist in storage
        std::vector<std::pair<std::string, fs::path>> existingFiles;
        fs::path objectsPath = cli_->getDataPath() / "storage" / "objects";

        if (fs::exists(objectsPath)) {
            for (const auto& dirEntry : fs::directory_iterator(objectsPath)) {
                if (fs::is_directory(dirEntry)) {
                    std::string dirName = dirEntry.path().filename().string();
                    for (const auto& fileEntry : fs::directory_iterator(dirEntry.path())) {
                        if (fs::is_regular_file(fileEntry)) {
                            // Reconstruct full hash: directory name + filename
                            std::string fullHash = dirName + fileEntry.path().filename().string();
                            existingFiles.push_back({fullHash, fileEntry.path()});
                        }
                    }
                }
            }
        }

        std::cout << "  Found " << existingFiles.size() << " chunk files in storage\n";

        // Find orphaned chunks (exist in storage but not referenced)
        std::vector<std::pair<std::string, fs::path>> orphanedChunks;
        for (const auto& [hash, path] : existingFiles) {
            if (referencedHashes.find(hash) == referencedHashes.end()) {
                orphanedChunks.push_back({hash, path});
            }
        }

        if (orphanedChunks.empty()) {
            std::cout << "  ✓ No orphaned chunks found\n";
            sqlite3_close(db);
            return Result<void>();
        }

        std::cout << "  Identified " << orphanedChunks.size() << " orphaned chunk files\n";

        // Calculate space to reclaim
        uint64_t bytesToReclaim = 0;
        for (const auto& [hash, path] : orphanedChunks) {
            try {
                bytesToReclaim += fs::file_size(path);
            } catch (...) {
                // Ignore errors getting file size
            }
        }

        std::cout << "  Space to reclaim: " << formatSize(bytesToReclaim) << " from "
                  << orphanedChunks.size() << " files\n";

        if (!dryRun_) {
            // Confirm deletion unless forced
            if (!force_ && !orphanedChunks.empty()) {
                std::cout << "  Delete " << orphanedChunks.size()
                          << " orphaned chunk files? (y/N): ";
                std::string response;
                std::getline(std::cin, response);
                if (response != "y" && response != "Y") {
                    std::cout << "  Cleanup cancelled.\n";
                    sqlite3_close(db);
                    return Result<void>();
                }
            }

            // Delete chunk files with progress
            ProgressIndicator progress(ProgressIndicator::Style::Percentage);
            if (orphanedChunks.size() > 100) {
                progress.start("Deleting orphaned chunks");
            }

            int deleted = 0;
            uint64_t bytesReclaimed = 0;

            for (size_t i = 0; i < orphanedChunks.size(); ++i) {
                const auto& [hash, path] = orphanedChunks[i];

                try {
                    uint64_t fileSize = fs::file_size(path);
                    fs::remove(path);
                    bytesReclaimed += fileSize;
                    deleted++;

                    if (verbose_) {
                        std::cout << "  Deleted: " << hash.substr(0, 12) << "...\n";
                    }
                } catch (const std::exception& e) {
                    if (verbose_) {
                        std::cout << "  Failed to delete " << hash.substr(0, 12)
                                  << "...: " << e.what() << "\n";
                    }
                }

                if (orphanedChunks.size() > 100 && (i + 1) % 100 == 0) {
                    progress.update(i + 1, orphanedChunks.size());
                }
            }

            progress.stop();

            // Clean database - delete blocks with ref_count = 0
            char* errMsg = nullptr;
            if (sqlite3_exec(db, "DELETE FROM block_references WHERE ref_count = 0", nullptr,
                             nullptr, &errMsg) != SQLITE_OK) {
                std::string error = errMsg ? errMsg : "Unknown error";
                sqlite3_free(errMsg);
                // Don't fail if we can't clean DB, we already deleted files
                std::cout << "  Warning: Could not clean database: " << error << "\n";
            } else {
                std::cout << "  ✓ Cleaned database entries\n";
            }

            if (deleted > 0) {
                std::cout << "  ✓ Deleted " << deleted << " chunk files\n";
                std::cout << "  ✓ Reclaimed " << formatSize(bytesReclaimed) << "\n";
            }

        } else {
            std::cout << "  [DRY RUN] Would delete " << orphanedChunks.size() << " chunk files\n";
            std::cout << "  [DRY RUN] Would reclaim " << formatSize(bytesToReclaim) << "\n";
        }

        sqlite3_close(db);
        return Result<void>();
    }

    Result<void> optimizeDatabase() {
        std::cout << "\nOptimizing database...\n";

        namespace fs = std::filesystem;
        fs::path dbPath = cli_->getDataPath() / "yams.db";

        if (!fs::exists(dbPath)) {
            std::cout << "  Database not found\n";
            return Result<void>();
        }

        uint64_t oldSize = fs::file_size(dbPath);
        std::cout << "  Current database size: " << formatSize(oldSize) << "\n";

        if (!dryRun_) {
            sqlite3* db;
            if (sqlite3_open(dbPath.string().c_str(), &db) != SQLITE_OK) {
                return Error{ErrorCode::DatabaseError,
                             "Failed to open database: " + std::string(sqlite3_errmsg(db))};
            }

            std::cout << "  Running VACUUM (this may take a moment)...\n";

            char* errMsg = nullptr;
            // VACUUM reclaims space from deleted rows
            if (sqlite3_exec(db, "VACUUM", nullptr, nullptr, &errMsg) == SQLITE_OK) {
                // Also run ANALYZE to update statistics
                sqlite3_exec(db, "ANALYZE", nullptr, nullptr, nullptr);

                sqlite3_close(db);

                // Check new size after vacuum
                uint64_t newSize = fs::file_size(dbPath);
                uint64_t saved = oldSize > newSize ? oldSize - newSize : 0;

                std::cout << "  ✓ Database optimized\n";
                std::cout << "  New size: " << formatSize(newSize) << "\n";
                if (saved > 0) {
                    std::cout << "  Space reclaimed: " << formatSize(saved) << "\n";
                }
            } else {
                std::string error = errMsg ? errMsg : "Unknown error";
                sqlite3_free(errMsg);
                sqlite3_close(db);
                std::cout << "  Warning: Could not optimize: " << error << "\n";
            }
        } else {
            std::cout << "  [DRY RUN] Would optimize database\n";
            std::cout << "  [DRY RUN] Current size: " << formatSize(oldSize) << "\n";
        }

        return Result<void>();
    }

    Result<void>
    generateMissingEmbeddings(std::shared_ptr<api::IContentStore> store,
                              std::shared_ptr<metadata::IMetadataRepository> metadataRepo) {
        std::cout << "Generating Missing Embeddings\n";
        std::cout << "─────────────────────────────\n";

        // Ensure vector DB schema dimension matches target before generating
        // Determine target dimension: config > env > generator > heuristic
        size_t targetDim = 0;
        // Config (best-effort parse of ~/.config/yams/config.toml or XDG)
        try {
            namespace fs = std::filesystem;
            fs::path cfgHome;
            if (const char* xdg = std::getenv("XDG_CONFIG_HOME"))
                cfgHome = fs::path(xdg);
            else if (const char* h = std::getenv("HOME"))
                cfgHome = fs::path(h) / ".config";
            fs::path cfgPath = cfgHome / "yams" / "config.toml";
            if (!cfgPath.empty() && fs::exists(cfgPath)) {
                std::ifstream in(cfgPath);
                std::string line;
                auto trim = [&](std::string& t) {
                    if (t.empty())
                        return;
                    t.erase(0, t.find_first_not_of(" \t"));
                    auto p = t.find_last_not_of(" \t");
                    if (p != std::string::npos)
                        t.erase(p + 1);
                };
                while (std::getline(in, line)) {
                    std::string l = line;
                    trim(l);
                    if (l.empty() || l[0] == '#')
                        continue;
                    if (l.find("embeddings.embedding_dim") != std::string::npos) {
                        auto eq = l.find('=');
                        if (eq != std::string::npos) {
                            std::string v = l.substr(eq + 1);
                            trim(v);
                            if (!v.empty() && v.front() == '"' && v.back() == '"')
                                v = v.substr(1, v.size() - 2);
                            try {
                                targetDim = static_cast<size_t>(std::stoul(v));
                            } catch (...) {
                            }
                        }
                        break;
                    }
                }
            }
        } catch (...) {
        }
        // Env
        if (targetDim == 0) {
            if (const char* envd = std::getenv("YAMS_EMBED_DIM")) {
                try {
                    targetDim = static_cast<size_t>(std::stoul(envd));
                } catch (...) {
                }
            }
        }
        // Generator
        if (targetDim == 0) {
            try {
                if (auto emb = cli_->getEmbeddingGenerator()) {
                    auto d = emb->getEmbeddingDimension();
                    if (d > 0)
                        targetDim = d;
                }
            } catch (...) {
            }
        }
        if (targetDim == 0)
            targetDim = 384; // final fallback

        // Align vectors.db schema dimension if needed (no daemon required)
        try {
            namespace fs = std::filesystem;
            fs::path dbPath = cli_->getDataPath() / "vectors.db";
            yams::vector::SqliteVecBackend be;
            bool dbExists = fs::exists(dbPath);
            // For safety, ensure parent exists
            try {
                fs::create_directories(dbPath.parent_path());
            } catch (...) {
            }
            if (!be.initialize(dbPath.string())) {
                std::cout << "  ⚠ Vector DB open failed; proceeding without schema alignment\n";
            } else {
                (void)be.ensureVecLoaded();
                auto stored = be.getStoredEmbeddingDimension();
                bool needCreate = !stored.has_value() && !dbExists;
                bool needRecreate = stored.has_value() && (*stored != targetDim);
                auto promptYesNo = [&](const std::string& msg, bool defNo = true) {
                    if (force_)
                        return true; // honor --force to skip prompts
                    std::cout << msg;
                    std::string line;
                    std::getline(std::cin, line);
                    if (line.empty())
                        return !defNo;
                    char c = static_cast<char>(std::tolower(line[0]));
                    return c == 'y';
                };

                if (needCreate) {
                    if (!dryRun_) {
                        if (promptYesNo(std::string("Create vector tables now (dim=") +
                                        std::to_string(targetDim) + ")? [y/N]: ")) {
                            auto cr = be.createTables(targetDim);
                            if (cr) {
                                std::cout << "  ✓ Created vector tables (dim=" << targetDim
                                          << ")\n";
                            } else {
                                std::cout
                                    << "  ⚠ Create vector tables failed: " << cr.error().message
                                    << "\n";
                            }
                        } else {
                            std::cout << "  Skipped creating vector tables\n";
                        }
                    } else {
                        std::cout << "  [DRY RUN] Would create vector tables (dim=" << targetDim
                                  << ")\n";
                    }
                } else if (needRecreate) {
                    std::cout << "  ⚠ Vector schema dimension mismatch (stored="
                              << (stored ? std::to_string(*stored) : std::string("unknown"))
                              << ", target=" << targetDim << ")\n";
                    if (!dryRun_) {
                        if (promptYesNo(std::string("Drop and recreate vector tables to dim ") +
                                        std::to_string(targetDim) + "? [y/N]: ")) {
                            auto dr = be.dropTables();
                            if (!dr) {
                                std::cout << "  ✗ Drop tables failed: " << dr.error().message
                                          << "\n";
                            } else {
                                auto cr = be.createTables(targetDim);
                                if (cr) {
                                    std::cout << "  ✓ Recreated vector tables (dim=" << targetDim
                                              << ")\n";
                                } else {
                                    std::cout << "  ✗ Create tables failed: " << cr.error().message
                                              << "\n";
                                }
                            }
                        } else {
                            std::cout << "  Skipped schema recreation\n";
                        }
                    } else {
                        std::cout << "  [DRY RUN] Would drop and recreate vector tables (dim="
                                  << targetDim << ")\n";
                    }
                } else {
                    std::cout << "  ✓ Vector schema aligned (dim="
                              << (stored ? std::to_string(*stored) : std::to_string(targetDim))
                              << ")\n";
                }
                be.close();
            }
        } catch (const std::exception& e) {
            std::cout << "  ⚠ Vector DB schema alignment skipped: " << e.what() << "\n";
        }

        // Check for available ONNX models
        namespace fs = std::filesystem;
        const char* home = std::getenv("HOME");
        fs::path modelsPath = fs::path(home ? home : "") / ".yams" / "models";
        std::vector<std::string> availableModels;

        if (fs::exists(modelsPath)) {
            for (const auto& entry : fs::directory_iterator(modelsPath)) {
                if (entry.is_directory()) {
                    fs::path modelFile = entry.path() / "model.onnx";
                    if (fs::exists(modelFile)) {
                        availableModels.push_back(entry.path().filename().string());
                    }
                }
            }
        }

        if (availableModels.empty()) {
            std::cout << "  ⚠ No embedding models found\n";
            std::cout << "  Please run 'yams model --download all-MiniLM-L6-v2' first\n\n";
            return Error{ErrorCode::NotFound, "No embedding models available"};
        }

        std::cout << "  Available models:\n";
        for (const auto& model : availableModels) {
            std::cout << "    - " << model << "\n";
        }

        if (!dryRun_) {
            std::cout << "  ℹ Generating embeddings for all documents\n";
            // Resolve model selection: explicit --model > preferred (config/env/CLI) > known order
            // > first available
            auto pickPreferred = [&]() -> std::string {
                if (!embeddingModel_.empty())
                    return embeddingModel_;
                // 1) Prefer model from active EmbeddingGenerator
                try {
                    if (auto emb = cli_->getEmbeddingGenerator()) {
                        auto n = emb->getConfig().model_name;
                        if (!n.empty())
                            return n;
                    }
                } catch (...) {
                }
                // 2) Read from config/env
                try {
                    namespace fs = std::filesystem;
                    fs::path cfgp;
                    if (const char* xdg = std::getenv("XDG_CONFIG_HOME"))
                        cfgp = fs::path(xdg) / "yams" / "config.toml";
                    else if (const char* home = std::getenv("HOME"))
                        cfgp = fs::path(home) / ".config" / "yams" / "config.toml";
                    std::ifstream in(cfgp);
                    if (in) {
                        std::string line, section;
                        auto trim = [](std::string& str) {
                            if (str.empty())
                                return;
                            str.erase(0, str.find_first_not_of(" \t"));
                            auto p = str.find_last_not_of(" \t");
                            if (p != std::string::npos)
                                str.erase(p + 1);
                        };
                        while (std::getline(in, line)) {
                            std::string l = line;
                            trim(l);
                            if (l.empty() || l[0] == '#')
                                continue;
                            if (l.front() == '[') {
                                auto e = l.find(']');
                                section =
                                    e != std::string::npos ? l.substr(1, e - 1) : std::string();
                                continue;
                            }
                            auto eq = l.find('=');
                            if (eq == std::string::npos)
                                continue;
                            std::string key = l.substr(0, eq);
                            std::string val = l.substr(eq + 1);
                            trim(key);
                            trim(val);
                            if (!val.empty() && val.front() == '"' && val.back() == '"')
                                val = val.substr(1, val.size() - 2);
                            if (section == "embeddings" && key == "preferred_model" && !val.empty())
                                return val;
                        }
                    }
                } catch (...) {
                }
                if (const char* p = std::getenv("YAMS_PREFERRED_MODEL"))
                    return std::string(p);
                // 3) Known preference order
                auto has = [&](const std::string& n) {
                    return std::find(availableModels.begin(), availableModels.end(), n) !=
                           availableModels.end();
                };
                if (has("nomic-embed-text-v1.5"))
                    return "nomic-embed-text-v1.5";
                if (has("nomic-embed-text-v1"))
                    return "nomic-embed-text-v1";
                if (has("all-MiniLM-L6-v2"))
                    return "all-MiniLM-L6-v2";
                if (has("all-mpnet-base-v2"))
                    return "all-mpnet-base-v2";
                // 4) First available
                return availableModels[0];
            };
            const std::string chosenModel = pickPreferred();
            std::cout << "    Using model: " << chosenModel << "\n";

            // Stream embeddings via daemon and show progress (preferred path)
            try {
                // Query all documents from metadata
                auto allDocs = metadataRepo->findDocumentsByPath("%");
                if (!allDocs) {
                    std::cout << "  ✗ Failed to query documents: " << allDocs.error().message
                              << "\n";
                    return Error{allDocs.error()};
                }

                const auto& documents = allDocs.value();
                std::cout << "  Found " << documents.size()
                          << " documents (will filter for embedding)\n";

                // Build list of document hashes to embed, respecting MIME filters
                auto is_embeddable_mime = [this](const std::string& m) -> bool {
                    if (m.rfind("text/", 0) == 0)
                        return true;
                    if (m == "application/json" || m == "application/xml" ||
                        m == "application/x-yaml" || m == "application/yaml")
                        return true;
                    for (const auto& inc : includeMime_) {
                        if (!inc.empty() && (m == inc || m.rfind(inc, 0) == 0))
                            return true;
                    }
                    return false;
                };

                std::vector<std::string> hashes;
                hashes.reserve(documents.size());
                for (const auto& d : documents) {
                    if (is_embeddable_mime(d.mimeType))
                        hashes.push_back(d.sha256Hash);
                }
                if (hashes.empty()) {
                    std::cout << "  ✓ No eligible documents for embeddings after MIME filtering\n";
                    return Result<void>();
                }

                if (!foreground_) {
                    std::cout
                        << "  ✓ Background repair requested. The daemon's RepairCoordinator will\n"
                           "    process missing embeddings opportunistically based on live load.\n"
                           "    Monitor with 'yams stats --verbose'.\n";
                    return Result<void>();
                }

                // Configure batch size
                size_t batchSize = 32; // safe default
                if (const char* envBatch = std::getenv("YAMS_EMBED_BATCH")) {
                    try {
                        unsigned long v = std::stoul(std::string(envBatch));
                        if (v < 4UL)
                            v = 4UL;
                        if (v > 128UL)
                            v = 128UL;
                        batchSize = static_cast<size_t>(v);
                    } catch (...) {
                    }
                }

                // Streaming RPC to daemon; server emits EmbeddingEvent progress
                yams::daemon::ClientConfig cfg;
                cfg.requestTimeout = std::chrono::milliseconds(embedTimeoutMs_);
                cfg.headerTimeout = std::chrono::milliseconds(embedTimeoutMs_);
                cfg.bodyTimeout = std::chrono::milliseconds(embedTimeoutMs_);
                yams::daemon::DaemonClient client(cfg);
                yams::daemon::EmbedDocumentsRequest ereq{};
                ereq.modelName = chosenModel;
                ereq.normalize = true;
                ereq.batchSize = static_cast<std::size_t>(0); // let server dynamic batcher decide
                ereq.skipExisting = true;
                ereq.documentHashes = hashes; // stream a single request with all docs
                Result<std::vector<yams::daemon::EmbeddingEvent>> evres = Error{ErrorCode::Unknown};
                for (int attempt = 1; attempt <= std::max(1, embedRetries_); ++attempt) {
                    std::promise<Result<std::vector<yams::daemon::EmbeddingEvent>>> prom;
                    auto fut = prom.get_future();
                    boost::asio::co_spawn(
                        boost::asio::system_executor{},
                        [&]() -> boost::asio::awaitable<void> {
                            auto r = co_await client.callEvents(ereq);
                            prom.set_value(std::move(r));
                            co_return;
                        },
                        boost::asio::detached);
                    auto waitRes = fut.wait_for(std::chrono::milliseconds(embedTimeoutMs_ + 10000));
                    if (waitRes == std::future_status::ready) {
                        evres = fut.get();
                        if (evres)
                            break; // success
                    } else {
                        evres = Error{ErrorCode::Timeout, "embedding timeout"};
                    }
                    if (attempt < std::max(1, embedRetries_)) {
                        std::cout << "  ⚠ Embed RPC retry " << attempt << "/" << embedRetries_
                                  << " after backoff\n";
                        std::this_thread::sleep_for(std::chrono::milliseconds(500 * attempt));
                    }
                }
                if (!evres) {
                    auto err = evres.error();
                    std::cout << "  [FAIL] Embedding failed: " << err.message << "\n";
                    return err;
                }
                const auto& events = evres.value();
                // Print compact progress based on collected events
                for (const auto& e : events) {
                    std::cout << "[embed] model=" << e.modelName << " " << e.processed << "/"
                              << e.total << " success=" << e.success << " failure=" << e.failure
                              << " inserted=" << e.inserted << " phase=" << e.phase
                              << (e.message.empty() ? "" : (" " + e.message)) << "\n";
                }
                // Derive final stats from last event when available
                if (!events.empty()) {
                    const auto& last = events.back();
                    std::cout << "  ✓ Embedding generation complete\n";
                    std::cout << "    Requested:  " << last.total << "\n";
                    std::cout << "    Embedded:   " << last.success << "\n";
                    std::cout << "    Skipped:    " << 0 << "\n";
                    std::cout << "    Failed:     " << last.failure << "\n\n";
                }
                return Result<void>();
            } catch (...) {
                // Swallow exceptions during embeddings generation in repair flow
            }

        } else {
            std::cout << "  [DRY RUN] Would generate embeddings for documents\n";
            std::cout << "  [DRY RUN] Using model: " << availableModels[0] << "\n";
        }

        std::cout << "\n";
        return Result<void>();
    }

    std::string formatSize(uint64_t bytes) const {
        const char* units[] = {"B", "KB", "MB", "GB", "TB"};
        int unitIndex = 0;
        double size = static_cast<double>(bytes);

        while (size >= 1024 && unitIndex < 4) {
            size /= 1024;
            unitIndex++;
        }

        std::ostringstream oss;
        if (unitIndex == 0) {
            oss << bytes << " B";
        } else {
            oss << std::fixed << std::setprecision(2) << size << " " << units[unitIndex];
        }
        return oss.str();
    }
};

// Out-of-class helper definition (below class end)
Result<void> RepairCommand::rebuildFts5Index(const app::services::AppContext& ctx) {
    std::cout << "Rebuilding FTS5 index (best-effort)...\n";
    if (!ctx.store || !ctx.metadataRepo) {
        return Error{ErrorCode::NotInitialized, "Store/Metadata not initialized"};
    }
    auto docs = ctx.metadataRepo->findDocumentsByPath("%");
    if (!docs) {
        return Error{ErrorCode::DatabaseError,
                     "Failed to enumerate documents: " + docs.error().message};
    }

    size_t ok = 0, fail = 0;
    ProgressIndicator progress(ProgressIndicator::Style::Percentage);
    if (!verbose_ && docs.value().size() > 100) {
        progress.start("FTS5 reindex");
    }
    size_t current = 0, total = docs.value().size();
    for (const auto& d : docs.value()) {
        ++current;
        std::string ext = d.fileExtension;
        if (!ext.empty() && ext[0] == '.')
            ext.erase(0, 1);
        try {
            auto extractedOpt = yams::extraction::util::extractDocumentText(
                ctx.store, d.sha256Hash, d.mimeType, ext, ctx.contentExtractors);
            if (extractedOpt && !extractedOpt->empty()) {
                auto ir = ctx.metadataRepo->indexDocumentContent(d.id, d.fileName, *extractedOpt,
                                                                 d.mimeType);
                if (ir) {
                    (void)ctx.metadataRepo->updateFuzzyIndex(d.id);
                    ++ok;
                } else {
                    ++fail;
                }
            } else {
                ++fail;
            }
        } catch (...) {
            ++fail;
        }

        if (!verbose_ && total > 100) {
            progress.update(current, total);
        }
    }
    if (!verbose_ && total > 100)
        progress.stop();
    std::cout << "FTS5 reindex complete: ok=" << ok << ", fail=" << fail << "\n";
    return {};
}

// Factory function
std::unique_ptr<ICommand> createRepairCommand() {
    return std::make_unique<RepairCommand>();
}

} // namespace yams::cli
