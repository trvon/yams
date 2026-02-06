#include <sqlite3.h>
#include <spdlog/spdlog.h>
#include <atomic>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <thread>
#ifndef _WIN32
#include <unistd.h>
#endif
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <yams/cli/command.h>
#include <yams/cli/daemon_helpers.h>
#include <yams/cli/progress_indicator.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/cli/vector_db_util.h>
#include <yams/cli/yams_cli.h>
#include <yams/config/config_helpers.h>
#include <yams/detection/file_type_detector.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/query_helpers.h>

#include <yams/app/services/services.hpp>
#include <yams/daemon/client/asio_transport.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/extraction/extraction_util.h>
#include <yams/integrity/repair_manager.h>
#include <yams/vector/embedding_generator.h>
#include <yams/vector/sqlite_vec_backend.h>
#include <yams/vector/vector_database.h>
#include <yams/vector/vector_index_manager.h>
#include <yams/vector/vector_schema_migration.h>

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
        cmd->add_flag("--path-tree", repairPathTree_,
                      "Rebuild path tree index for documents missing from the tree");
        cmd->add_flag("--refs,--ref", repairBlockRefs_,
                      "Repair block_references with correct sizes from CAS files");
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
        cmd->add_flag("-v,--verbose", verbose_, "Show detailed progress and transient errors");
        cmd->add_flag("--force", force_, "Skip confirmation prompts");

        cmd->callback([this]() {
            auto result = execute();
            if (!result) {
                // Add targeted hints for common DB/FTS issues, avoid duplication
                std::string msg = result.error().message;
                auto add_hint = [&](std::string_view h) {
                    if (msg.find("hint:") == std::string::npos &&
                        msg.find(h) == std::string::npos) {
                        msg += std::string(" (") + std::string(h) + ")";
                    }
                };
                if (msg.find("FTS5") != std::string::npos ||
                    msg.find("tokenize") != std::string::npos) {
                    add_hint("hint: run 'yams repair --fts5'");
                } else if (msg.find("constraint failed") != std::string::npos) {
                    add_hint("hint: try 'yams repair --orphans' or 'yams doctor --fix'");
                }
                spdlog::error("Repair failed: {}", msg);
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
                    ccfg.requestTimeout = std::chrono::seconds(5);
                    auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(ccfg);
                    if (leaseRes) {
                        auto leaseHandle = std::move(leaseRes.value());
                        auto& shut = **leaseHandle;
                        (void)yams::cli::run_result(shut.shutdown(true), std::chrono::seconds(6));
                    }
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
                !repairPathTree_ && !repairBlockRefs_ && !repairAll_) {
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
                repairPathTree_ = true;
                repairBlockRefs_ = true;
                // verifyChecksums_ = true;  // Not implemented yet
                // mergeDuplicates_ = true;  // Not implemented yet
            }

            std::cout << ui::title_banner("YAMS Storage Repair") << "\n\n";

            // Surface the effective data directory so operators can verify the storage path
            try {
                auto dd = cli_->getDataPath();
                std::cout << "  " << ui::key_value("Data directory", dd.string()) << "\n\n";
            } catch (...) {
                // non-fatal
            }

            // Warn about daemon for operations that modify refs.db
            if ((repairBlockRefs_ || repairChunks_) && !dryRun_) {
                std::string flags = repairBlockRefs_ && repairChunks_
                                        ? "--refs/--chunks"
                                        : (repairBlockRefs_ ? "--refs" : "--chunks");
                std::cout << ui::colorize("⚠ Tip: For best results with " + flags +
                                              ", stop the daemon first:\n"
                                              "  yams daemon stop && yams repair " +
                                              flags + " && yams daemon start\n",
                                          ui::Ansi::YELLOW)
                          << "\n";
            }

            if (dryRun_) {
                std::cout << ui::status_info("[DRY RUN MODE] No changes will be made") << "\n\n";
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

            // Rebuild path tree index
            if (repairPathTree_) {
                auto result = rebuildPathTree(metadataRepo);
                if (!result)
                    return result;
                anyRepairs = true;
            }

            // Repair block references
            if (repairBlockRefs_) {
                auto result = repairBlockReferences();
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
                std::cout << "\n"
                          << ui::status_ok("Repair operations completed successfully") << "\n";
            }

            std::cout << ui::horizontal_rule(60, '=') << "\n";

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
    bool repairPathTree_ = false;
    bool repairBlockRefs_ = false;
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

    // Helper to rebuild path tree index for documents missing from the tree
    Result<void> rebuildPathTree(std::shared_ptr<metadata::IMetadataRepository> metadataRepo);

    // Helper to repair block references with correct sizes from CAS files
    Result<void> repairBlockReferences();

    Result<void>
    cleanOrphanedMetadata(std::shared_ptr<api::IContentStore> store,
                          std::shared_ptr<metadata::IMetadataRepository> metadataRepo) {
        std::cout << ui::status_pending("Cleaning orphaned metadata entries...") << "\n";

        // Get all documents from metadata
        auto documentsResult = metadata::queryDocumentsByPattern(*metadataRepo, "%");
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
            std::cout << "  " << ui::status_ok("No orphaned metadata entries found") << "\n";
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

            std::cout << "  "
                      << ui::status_ok("Cleaned " + std::to_string(cleanedCount) +
                                       " orphaned metadata entries")
                      << "\n";
        } else {
            std::cout << "  [DRY RUN] Would clean " << orphanedCount << " orphaned entries\n";
        }

        return Result<void>();
    }

    Result<void> repairDownloads(std::shared_ptr<metadata::IMetadataRepository> metadataRepo) {
        std::cout << ui::status_pending(
                         "Repairing downloaded documents (tags/metadata/filenames)...")
                  << "\n";

        // Fetch all docs
        auto docsResult = metadata::queryDocumentsByPattern(*metadataRepo, "%");
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
        ProgressIndicator dlProgress(ProgressIndicator::Style::Percentage);
        if (docsResult.value().size() > 50) {
            dlProgress.start("Scanning downloads");
        }

        size_t dlCurrent = 0;
        const size_t dlTotal = docsResult.value().size();
        for (auto doc : docsResult.value()) {
            ++dlCurrent;
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

            if (dlTotal > 50 && dlCurrent % 10 == 0) {
                dlProgress.update(dlCurrent, dlTotal);
            }
        }

        dlProgress.stop();

        std::cout << ui::status_ok("Downloads repair updated " + std::to_string(updated) +
                                   " document(s)")
                  << "\n";
        return Result<void>();
    }

    Result<void> repairMimeTypes(std::shared_ptr<api::IContentStore> /*store*/,
                                 std::shared_ptr<metadata::IMetadataRepository> metadataRepo) {
        std::cout << "\n" << ui::status_pending("Repairing missing MIME types...") << "\n";

        // Initialize file type detector
        detection::FileTypeDetectorConfig config;
        config.patternsFile = YamsCLI::findMagicNumbersFile();
        config.useCustomPatterns = !config.patternsFile.empty();
        detection::FileTypeDetector::instance().initialize(config);

        // Get all documents from metadata
        auto documentsResult = metadata::queryDocumentsByPattern(*metadataRepo, "%");
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

                // For extensionless content, default to text/plain (same as document_service)
                if (detectedMime.empty() || detectedMime == "application/octet-stream")
                    detectedMime = "text/plain";

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
            std::cout << "  " << ui::status_ok("All documents have valid MIME types") << "\n";
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

            std::cout << "  "
                      << ui::status_ok("Repaired MIME types for " + std::to_string(repairedCount) +
                                       " documents")
                      << "\n";
        } else {
            std::cout << "  [DRY RUN] Would repair " << missingMimeCount << " MIME types\n";
        }

        return Result<void>();
    }

    Result<void> cleanOrphanedChunks(std::shared_ptr<api::IContentStore> /*store*/) {
        std::cout << "\n" << ui::status_pending("Cleaning orphaned chunks...") << "\n";

        namespace fs = std::filesystem;

        // Open refs database
        fs::path refsDbPath = cli_->getDataPath() / "storage" / "refs.db";
        if (!fs::exists(refsDbPath)) {
            std::cout << "  " << ui::status_warning("No reference database found") << "\n";
            return Result<void>();
        }

        sqlite3* db;
        if (sqlite3_open(refsDbPath.string().c_str(), &db) != SQLITE_OK) {
            return Error{ErrorCode::DatabaseError,
                         "Failed to open refs.db: " + std::string(sqlite3_errmsg(db))};
        }

        // First, get all REFERENCED blocks (ref_count > 0) from refs.db
        std::set<std::string> referencedHashes;
        sqlite3_stmt* stmt;
        const char* sql = "SELECT block_hash FROM block_references WHERE ref_count > 0";

        if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
            sqlite3_close(db);
            return Error{ErrorCode::DatabaseError,
                         "Failed to prepare query: " + std::string(sqlite3_errmsg(db))};
        }

        ui::SpinnerRunner refsSpinner;
        refsSpinner.start("Querying refs.db for referenced blocks...");

        while (sqlite3_step(stmt) == SQLITE_ROW) {
            const char* hash = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
            if (hash) {
                referencedHashes.insert(hash);
            }
        }
        sqlite3_finalize(stmt);
        refsSpinner.stop();

        std::cout << "  Found " << referencedHashes.size() << " referenced chunks in refs.db\n";
        fs::path objectsPath = cli_->getDataPath() / "storage" / "objects";
        size_t manifestsScanned = 0;
        size_t chunksFromManifests = 0;

        ui::SpinnerRunner manifestSpinner;
        manifestSpinner.start("Scanning manifest files...");

        if (fs::exists(objectsPath)) {
            for (const auto& dirEntry : fs::directory_iterator(objectsPath)) {
                if (fs::is_directory(dirEntry)) {
                    for (const auto& fileEntry : fs::directory_iterator(dirEntry.path())) {
                        if (fs::is_regular_file(fileEntry)) {
                            std::string filename = fileEntry.path().filename().string();
                            // Check if this is a manifest file
                            if (filename.size() > 9 &&
                                filename.substr(filename.size() - 9) == ".manifest") {
                                manifestsScanned++;
                                // Read and parse manifest to extract chunk hashes
                                try {
                                    std::ifstream manifestFile(fileEntry.path(), std::ios::binary);
                                    if (manifestFile) {
                                        std::vector<char> data(
                                            (std::istreambuf_iterator<char>(manifestFile)),
                                            std::istreambuf_iterator<char>());
                                        manifestFile.close();

                                        // Parse manifest (supports both protobuf and binary
                                        // formats) Binary format: 4-byte magic, 4-byte version,
                                        // 8-byte fileSize,
                                        //               8-byte chunkSize, 4-byte numChunks, then
                                        //               chunks
                                        // Each chunk: 4-byte hashLen, hash string, 8-byte offset,
                                        // 8-byte size
                                        if (data.size() >= 28) {
                                            // Check for binary format magic: YMNF
                                            if (data[0] == 'Y' && data[1] == 'M' &&
                                                data[2] == 'N' && data[3] == 'F') {
                                                // Binary format
                                                size_t pos = 24; // Skip header (4+4+8+8)
                                                uint32_t numChunks = 0;
                                                if (pos + 4 <= data.size()) {
                                                    std::memcpy(&numChunks, data.data() + pos, 4);
                                                    pos += 4;
                                                }
                                                for (uint32_t i = 0;
                                                     i < numChunks && pos + 4 <= data.size(); ++i) {
                                                    uint32_t hashLen = 0;
                                                    std::memcpy(&hashLen, data.data() + pos, 4);
                                                    pos += 4;
                                                    if (hashLen > 0 && hashLen < 256 &&
                                                        pos + hashLen <= data.size()) {
                                                        std::string chunkHash(data.data() + pos,
                                                                              hashLen);
                                                        referencedHashes.insert(chunkHash);
                                                        chunksFromManifests++;
                                                        pos += hashLen;
                                                        pos += 16; // Skip offset (8) + size (8)
                                                    } else {
                                                        break; // Invalid format
                                                    }
                                                }
                                            } else {
                                                // Try protobuf format - look for hash strings in
                                                // raw data SHA-256 hashes are 64 hex characters
                                                std::string dataStr(data.begin(), data.end());
                                                for (size_t i = 0; i + 64 <= dataStr.size(); ++i) {
                                                    bool isHex = true;
                                                    for (size_t j = 0; j < 64 && isHex; ++j) {
                                                        char c = dataStr[i + j];
                                                        isHex = (c >= '0' && c <= '9') ||
                                                                (c >= 'a' && c <= 'f') ||
                                                                (c >= 'A' && c <= 'F');
                                                    }
                                                    if (isHex) {
                                                        std::string potentialHash =
                                                            dataStr.substr(i, 64);
                                                        // Verify it looks like a hash (not random
                                                        // hex) Check if next char is not hex (end
                                                        // of hash)
                                                        if (i + 64 >= dataStr.size() ||
                                                            !((dataStr[i + 64] >= '0' &&
                                                               dataStr[i + 64] <= '9') ||
                                                              (dataStr[i + 64] >= 'a' &&
                                                               dataStr[i + 64] <= 'f') ||
                                                              (dataStr[i + 64] >= 'A' &&
                                                               dataStr[i + 64] <= 'F'))) {
                                                            referencedHashes.insert(potentialHash);
                                                            chunksFromManifests++;
                                                            i += 63; // Skip past this hash
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                } catch (const std::exception& e) {
                                    if (verbose_) {
                                        std::cout << "  Warning: Could not parse manifest "
                                                  << filename << ": " << e.what() << "\n";
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        manifestSpinner.stop();

        std::cout << "  Scanned " << manifestsScanned << " manifest files, found "
                  << chunksFromManifests << " chunk references\n";
        std::cout << "  Total referenced chunks (refs.db + manifests): " << referencedHashes.size()
                  << "\n";

        // Now, get all files that exist in storage (excluding manifests)
        std::vector<std::pair<std::string, fs::path>> existingFiles;

        ui::SpinnerRunner storageSpinner;
        storageSpinner.start("Scanning storage for chunk files...");

        if (fs::exists(objectsPath)) {
            for (const auto& dirEntry : fs::directory_iterator(objectsPath)) {
                if (fs::is_directory(dirEntry)) {
                    std::string dirName = dirEntry.path().filename().string();
                    for (const auto& fileEntry : fs::directory_iterator(dirEntry.path())) {
                        if (fs::is_regular_file(fileEntry)) {
                            std::string filename = fileEntry.path().filename().string();
                            // Skip manifest files - they're not chunks
                            if (filename.size() > 9 &&
                                filename.substr(filename.size() - 9) == ".manifest") {
                                continue;
                            }
                            // Reconstruct full hash: directory name + filename
                            std::string fullHash = dirName + filename;
                            existingFiles.push_back({fullHash, fileEntry.path()});
                        }
                    }
                }
            }
        }
        storageSpinner.stop();

        std::cout << "  Found " << existingFiles.size() << " chunk files in storage\n";

        // Find orphaned chunks (exist in storage but not referenced by refs.db OR manifests)
        std::vector<std::pair<std::string, fs::path>> orphanedChunks;
        for (const auto& [hash, path] : existingFiles) {
            if (referencedHashes.find(hash) == referencedHashes.end()) {
                orphanedChunks.push_back({hash, path});
            }
        }

        if (orphanedChunks.empty()) {
            std::cout << "  " << ui::status_ok("No orphaned chunks found") << "\n";
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

        std::cout << "  Space to reclaim: " << ui::format_bytes(bytesToReclaim) << " from "
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
                std::cout << "  " << ui::status_ok("Cleaned database entries") << "\n";
            }

            if (deleted > 0) {
                std::cout << "  "
                          << ui::status_ok("Deleted " + std::to_string(deleted) + " chunk files")
                          << "\n";
                std::cout << "  " << ui::status_ok("Reclaimed " + ui::format_bytes(bytesReclaimed))
                          << "\n";
            }

        } else {
            std::cout << "  [DRY RUN] Would delete " << orphanedChunks.size() << " chunk files\n";
            std::cout << "  [DRY RUN] Would reclaim " << ui::format_bytes(bytesToReclaim) << "\n";
        }

        sqlite3_close(db);
        return Result<void>();
    }

    Result<void> optimizeDatabase() {
        std::cout << "\n" << ui::status_pending("Optimizing database...") << "\n";

        namespace fs = std::filesystem;
        fs::path dbPath = cli_->getDataPath() / "yams.db";

        if (!fs::exists(dbPath)) {
            std::cout << "  " << ui::status_warning("Database not found") << "\n";
            return Result<void>();
        }

        uint64_t oldSize = fs::file_size(dbPath);
        std::cout << "  Current database size: " << ui::format_bytes(oldSize) << "\n";

        if (!dryRun_) {
            sqlite3* db;
            if (sqlite3_open(dbPath.string().c_str(), &db) != SQLITE_OK) {
                return Error{ErrorCode::DatabaseError,
                             "Failed to open database: " + std::string(sqlite3_errmsg(db))};
            }

            ui::SpinnerRunner vacuumSpinner;
            vacuumSpinner.start("Running VACUUM...");

            char* errMsg = nullptr;
            // VACUUM reclaims space from deleted rows
            if (sqlite3_exec(db, "VACUUM", nullptr, nullptr, &errMsg) == SQLITE_OK) {
                // Also run ANALYZE to update statistics
                sqlite3_exec(db, "ANALYZE", nullptr, nullptr, nullptr);

                vacuumSpinner.stop();
                sqlite3_close(db);

                // Check new size after vacuum
                uint64_t newSize = fs::file_size(dbPath);
                uint64_t saved = oldSize > newSize ? oldSize - newSize : 0;

                std::cout << "  " << ui::status_ok("Database optimized") << "\n";
                std::cout << "  New size: " << ui::format_bytes(newSize) << "\n";
                if (saved > 0) {
                    std::cout << "  Space reclaimed: " << ui::format_bytes(saved) << "\n";
                }
            } else {
                vacuumSpinner.stop();
                std::string error = errMsg ? errMsg : "Unknown error";
                sqlite3_free(errMsg);
                sqlite3_close(db);
                std::cout << "  Warning: Could not optimize: " << error << "\n";
            }
        } else {
            std::cout << "  [DRY RUN] Would optimize database\n";
            std::cout << "  [DRY RUN] Current size: " << ui::format_bytes(oldSize) << "\n";
        }

        return Result<void>();
    }

    Result<void>
    generateMissingEmbeddings([[maybe_unused]] std::shared_ptr<api::IContentStore> store,
                              std::shared_ptr<metadata::IMetadataRepository> metadataRepo) {
        std::cout << ui::section_header("Generating Missing Embeddings") << "\n";

        // Ensure vector DB schema dimension matches target before generating
        // Use extracted utility for dimension resolution (config > env > generator > heuristic)
        auto resolved = vecutil::resolveEmbeddingDimension(cli_, cli_->getDataPath());
        size_t targetDim = resolved.dimension;

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
                std::cout << "  "
                          << ui::status_warning(
                                 "Vector DB open failed; proceeding without schema alignment")
                          << "\n";
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
                                std::cout << "  "
                                          << ui::status_ok("Created vector tables (dim=" +
                                                           std::to_string(targetDim) + ")")
                                          << "\n";
                            } else {
                                std::cout << "  "
                                          << ui::status_warning("Create vector tables failed: " +
                                                                cr.error().message)
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
                    std::cout << "  "
                              << ui::status_warning(
                                     "Vector schema dimension mismatch (stored=" +
                                     (stored ? std::to_string(*stored) : std::string("unknown")) +
                                     ", target=" + std::to_string(targetDim) + ")")
                              << "\n";
                    if (!dryRun_) {
                        if (promptYesNo(std::string("Drop and recreate vector tables to dim ") +
                                        std::to_string(targetDim) + "? [y/N]: ")) {
                            auto dr = be.dropTables();
                            if (!dr) {
                                std::cout
                                    << "  "
                                    << ui::status_error("Drop tables failed: " + dr.error().message)
                                    << "\n";
                            } else {
                                auto cr = be.createTables(targetDim);
                                if (cr) {
                                    std::cout << "  "
                                              << ui::status_ok("Recreated vector tables (dim=" +
                                                               std::to_string(targetDim) + ")")
                                              << "\n";
                                } else {
                                    std::cout << "  "
                                              << ui::status_error("Create tables failed: " +
                                                                  cr.error().message)
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
                    std::cout << "  "
                              << ui::status_ok("Vector schema aligned (dim=" +
                                               (stored ? std::to_string(*stored)
                                                       : std::to_string(targetDim)) +
                                               ")")
                              << "\n";
                }
                be.close();
            }
        } catch (const std::exception& e) {
            std::cout << "  "
                      << ui::status_warning("Vector DB schema alignment skipped: " +
                                            std::string(e.what()))
                      << "\n";
        }

        // Check for available ONNX models
        namespace fs = std::filesystem;
        fs::path modelsPath = cli_->getDataPath() / "models";
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
            std::cout << "  "
                      << ui::status_warning(
                             "No local embedding models found. Will attempt to use daemon default.")
                      << "\n";
            // Do not fail here - let the daemon decide if it can handle the request
        }

        std::cout << "  Available models:\n";
        for (const auto& model : availableModels) {
            std::cout << "    " << ui::bullet(model) << "\n";
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
                    auto cfgp = yams::config::get_config_path();
                    auto config = yams::config::parse_simple_toml(cfgp);
                    if (auto it = config.find("embeddings.preferred_model"); it != config.end()) {
                        if (!it->second.empty())
                            return it->second;
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
                // 4) First available or fallback
                if (!availableModels.empty())
                    return availableModels[0];
                return std::string("all-MiniLM-L6-v2"); // Default fallback
            };
            const std::string chosenModel = pickPreferred();
            std::cout << "    " << ui::key_value("Using model", chosenModel) << "\n";

            // Stream embeddings via daemon and show progress (preferred path)
            try {
                // Query all documents from metadata
                auto allDocs = metadata::queryDocumentsByPattern(*metadataRepo, "%");
                if (!allDocs) {
                    std::cout << "  "
                              << ui::status_error("Failed to query documents: " +
                                                  allDocs.error().message)
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

                // Configure batch size (GPU-aware with env override)
                size_t batchSize = 32; // safe default
                if (const char* envBatch = std::getenv("YAMS_EMBED_BATCH")) {
                    try {
                        unsigned long v = std::stoul(std::string(envBatch));
                        if (v < 4UL)
                            v = 4UL;
                        if (v > 256UL)
                            v = 256UL;
                        batchSize = static_cast<size_t>(v);
                    } catch (...) {
                    }
                }

                // Streaming RPC to daemon; server emits EmbeddingEvent progress
                yams::daemon::ClientConfig cfg;
                cfg.requestTimeout = std::chrono::milliseconds(embedTimeoutMs_);
                cfg.headerTimeout = std::chrono::milliseconds(embedTimeoutMs_);
                cfg.bodyTimeout = std::chrono::milliseconds(embedTimeoutMs_);
                auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(cfg);
                if (!leaseRes) {
                    return Result<void>(leaseRes.error());
                }
                auto leaseHandle = std::move(leaseRes.value());
                auto& client = **leaseHandle;
                yams::daemon::EmbedDocumentsRequest ereq{};
                ereq.modelName = chosenModel;
                ereq.normalize = true;
                ereq.batchSize = batchSize;
                ereq.skipExisting = true;
                ereq.documentHashes = hashes; // stream a single request with all docs
                Result<std::vector<yams::daemon::EmbeddingEvent>> evres = Error{ErrorCode::Unknown};
                for (int attempt = 1; attempt <= std::max(1, embedRetries_); ++attempt) {
                    evres = yams::cli::run_result<std::vector<yams::daemon::EmbeddingEvent>>(
                        client.callEvents(ereq),
                        std::chrono::milliseconds(embedTimeoutMs_ + 10000));
                    if (evres)
                        break; // success
                    if (!evres)
                        evres = Error{ErrorCode::Timeout, "embedding timeout"};
                    if (attempt < std::max(1, embedRetries_)) {
                        std::cout << "  "
                                  << ui::status_warning(
                                         "Embed RPC retry " + std::to_string(attempt) + "/" +
                                         std::to_string(embedRetries_) + " after backoff")
                                  << "\n";
                        std::this_thread::sleep_for(std::chrono::milliseconds(500 * attempt));
                    }
                }
                if (!evres) {
                    auto err = evres.error();
                    std::cout << "  " << ui::status_error("Embedding failed: " + err.message)
                              << "\n";
                    return err;
                }
                const auto& events = evres.value();
                // Print compact progress based on collected events
                for (const auto& e : events) {
                    std::cout << "  "
                              << ui::key_value("embed",
                                               e.modelName + " " + std::to_string(e.processed) +
                                                   "/" + std::to_string(e.total) +
                                                   " success=" + std::to_string(e.success) +
                                                   " fail=" + std::to_string(e.failure) +
                                                   " inserted=" + std::to_string(e.inserted) +
                                                   " phase=" + e.phase +
                                                   (e.message.empty() ? "" : (" " + e.message)))
                              << "\n";
                }
                // Derive final stats from last event when available
                if (!events.empty()) {
                    const auto& last = events.back();
                    std::cout << "  " << ui::status_ok("Embedding generation complete") << "\n";
                    std::vector<ui::Row> rows = {
                        {"Requested", std::to_string(last.total), ""},
                        {"Embedded", std::to_string(last.success), ""},
                        {"Skipped", "0", ""},
                        {"Failed",
                         last.failure > 0
                             ? ui::colorize(std::to_string(last.failure), ui::Ansi::RED)
                             : "0",
                         ""},
                    };
                    ui::render_rows(std::cout, rows);
                    std::cout << "\n";
                }
                return Result<void>();
            } catch (const std::exception& ex) {
                std::cout << "  "
                          << ui::status_error("Embedding exception: " + std::string(ex.what()))
                          << "\n";
                return Error{ErrorCode::InternalError,
                             std::string("embedding exception: ") + ex.what()};
            } catch (...) {
                std::cout << "  " << ui::status_error("Unknown embedding exception") << "\n";
                return Error{ErrorCode::InternalError, "unknown embedding exception"};
            }

        } else {
            std::cout << "  [DRY RUN] Would generate embeddings for documents\n";
            std::cout << "  [DRY RUN] Using model: " << availableModels[0] << "\n";
        }

        std::cout << "\n";
        return Result<void>();
    }
};

// Out-of-class helper definition (below class end)
Result<void> RepairCommand::rebuildFts5Index(const app::services::AppContext& ctx) {
    std::cout << ui::section_header("FTS5 Index Rebuild") << "\n";
    if (!ctx.store || !ctx.metadataRepo) {
        return Error{ErrorCode::NotInitialized, "Store/Metadata not initialized"};
    }
    auto docs = metadata::queryDocumentsByPattern(*ctx.metadataRepo, "%");
    if (!docs) {
        return Error{ErrorCode::DatabaseError,
                     "Failed to enumerate documents: " + docs.error().message};
    }

    // Heal known inconsistency before the bulk rebuild.
    // If a document is marked Success but has no row in document_content, list/snippet
    // hydration will show "Content not available". Reset these to Pending so --fts5 can
    // restore a consistent state.
    int64_t resetCount = 0;
    for (const auto& d : docs.value()) {
        if (d.extractionStatus != metadata::ExtractionStatus::Success)
            continue;

        auto contentRes = ctx.metadataRepo->getContent(d.id);
        if (!contentRes) {
            if (verbose_) {
                std::cout << "  "
                          << ui::status_warning("Could not inspect content for " + d.fileName +
                                                ": " + contentRes.error().message)
                          << "\n";
            }
            continue;
        }

        if (!contentRes.value().has_value()) {
            (void)ctx.metadataRepo->updateDocumentExtractionStatus(
                d.id, false, metadata::ExtractionStatus::Pending,
                "Missing content row; reset by repair --fts5");
            ++resetCount;
        }
    }

    if (resetCount > 0) {
        std::cout << "  "
                  << ui::colorize("Reset " + std::to_string(resetCount) +
                                      " documents from Success->Pending (missing content)",
                                  ui::Ansi::DIM)
                  << "\n";
    }

    // Use batch stats to aggregate errors instead of logging each one
    ui::BatchOperationStats stats;
    stats.total = docs.value().size();

    // Suppress transient errors unless verbose mode is enabled
    std::optional<ui::ScopedLogLevel> quietMode;
    if (!verbose_) {
        // Suppress error-level logs during batch operation; show summary at end
        quietMode.emplace(spdlog::level::critical);
    }

    ProgressIndicator progress(ProgressIndicator::Style::Percentage);
    if (!verbose_ && docs.value().size() > 100) {
        progress.start("FTS5 reindex");
    }

    size_t current = 0;
    const size_t total = docs.value().size();

    for (const auto& d : docs.value()) {
        ++current;
        std::string ext = d.fileExtension;
        if (!ext.empty() && ext[0] == '.')
            ext.erase(0, 1);

        // Re-detect MIME for documents with unhelpful MIME types
        std::string effectiveMime = d.mimeType;
        if (effectiveMime.empty() || effectiveMime == "application/octet-stream") {
            if (!ext.empty()) {
                auto detected = yams::detection::FileTypeDetector::getMimeTypeFromExtension(ext);
                if (!detected.empty() && detected != "application/octet-stream")
                    effectiveMime = detected;
            }
            // For extensionless content, default to text/plain (same as document_service)
            if (effectiveMime.empty() || effectiveMime == "application/octet-stream")
                effectiveMime = "text/plain";

            // Persist corrected MIME so future list/query uses it
            auto updated = d;
            updated.mimeType = effectiveMime;
            (void)ctx.metadataRepo->updateDocument(updated);
        }

        try {
            auto extractedOpt = yams::extraction::util::extractDocumentText(
                ctx.store, d.sha256Hash, effectiveMime, ext, ctx.contentExtractors);
            if (extractedOpt && !extractedOpt->empty()) {
                // Mark as pending while we attempt to write content + FTS5.
                // This avoids leaving a misleading Success status if persistence/indexing fails.
                (void)ctx.metadataRepo->updateDocumentExtractionStatus(
                    d.id, false, metadata::ExtractionStatus::Pending, "repair --fts5 processing");

                // Store the extracted content so it's available for snippets
                metadata::DocumentContent content;
                content.documentId = d.id;
                content.contentText = *extractedOpt;
                content.extractionMethod = "repair";
                content.language = ""; // Could detect language if needed
                auto contentResult = ctx.metadataRepo->insertContent(content);

                // Index in FTS5
                auto ir = ctx.metadataRepo->indexDocumentContent(d.id, d.fileName, *extractedOpt,
                                                                 effectiveMime);
                if (ir && contentResult) {
                    (void)ctx.metadataRepo->updateDocumentExtractionStatus(
                        d.id, true, metadata::ExtractionStatus::Success);
                    ++stats.succeeded;
                } else {
                    // Check if this was a transient error (database locked)
                    std::string errMsg = ir ? "" : ir.error().message;
                    if (errMsg.find("database is locked") != std::string::npos ||
                        errMsg.find("SQLITE_BUSY") != std::string::npos) {
                        ++stats.transient_retries;
                    }

                    std::string failMsg;
                    if (!contentResult) {
                        failMsg = "insertContent failed: " + contentResult.error().message;
                    } else if (!ir) {
                        failMsg = "indexDocumentContent failed: " + ir.error().message;
                    } else {
                        failMsg = "persist/index failed";
                    }
                    (void)ctx.metadataRepo->updateDocumentExtractionStatus(
                        d.id, false, metadata::ExtractionStatus::Failed, failMsg);
                    ++stats.failed;
                }
            } else {
                (void)ctx.metadataRepo->updateDocumentExtractionStatus(
                    d.id, false, metadata::ExtractionStatus::Skipped, "No extractable text");
                ++stats.skipped; // No content to index
            }
        } catch (const std::exception& e) {
            // Check for transient errors in exception message
            std::string errMsg = e.what();
            if (errMsg.find("database is locked") != std::string::npos ||
                errMsg.find("SQLITE_BUSY") != std::string::npos) {
                ++stats.transient_retries;
            }
            (void)ctx.metadataRepo->updateDocumentExtractionStatus(
                d.id, false, metadata::ExtractionStatus::Failed, errMsg);
            ++stats.failed;
            if (verbose_) {
                std::cout << "  "
                          << ui::status_warning("Exception for " + d.fileName + ": " + errMsg)
                          << "\n";
            }
        } catch (...) {
            (void)ctx.metadataRepo->updateDocumentExtractionStatus(
                d.id, false, metadata::ExtractionStatus::Failed, "Unknown exception");
            ++stats.failed;
        }

        if (!verbose_ && total > 100) {
            progress.update(current, total);
        }
    }

    // Release scoped log level before printing summary
    quietMode.reset();

    if (!verbose_ && total > 100)
        progress.stop();

    // Print summary with color-coded stats
    std::cout << "  " << ui::status_ok("FTS5 reindex complete: " + stats.summary_colored()) << "\n";

    // Show hint if there were transient errors
    if (stats.transient_retries > 0 && !verbose_) {
        std::cout
            << "  "
            << ui::colorize(
                   "Hint: " + std::to_string(stats.transient_retries.load()) +
                       " transient errors (database locked) occurred. Use --verbose for details.",
                   ui::Ansi::DIM)
            << "\n";
    }

    return {};
}

Result<void>
RepairCommand::rebuildPathTree(std::shared_ptr<metadata::IMetadataRepository> metadataRepo) {
    std::cout << ui::status_pending("Rebuilding path tree index...") << "\n";

    if (!metadataRepo) {
        return Error{ErrorCode::NotInitialized, "Metadata repository not initialized"};
    }

    // Get the concrete MetadataRepository for RepairManager
    auto concreteRepo = std::dynamic_pointer_cast<metadata::MetadataRepository>(metadataRepo);
    if (!concreteRepo) {
        return Error{ErrorCode::NotInitialized,
                     "Could not get concrete MetadataRepository for path tree repair"};
    }

    integrity::RepairManager repairMgr(*concreteRepo);

    if (dryRun_) {
        // For dry run, just count how many documents are missing from path tree
        auto docsResult = concreteRepo->queryDocuments(metadata::DocumentQueryOptions{});
        if (!docsResult) {
            return Error{ErrorCode::DatabaseError,
                         "Failed to query documents: " + docsResult.error().message};
        }

        uint64_t missing = 0;
        for (const auto& doc : docsResult.value()) {
            if (doc.filePath.empty())
                continue;
            auto existingNode = concreteRepo->findPathTreeNodeByFullPath(doc.filePath);
            if (!existingNode || !existingNode.value().has_value()) {
                missing++;
            }
        }

        std::cout << "  [DRY RUN] Would rebuild path tree for " << missing
                  << " documents missing from tree\n";
        return Result<void>();
    }

    // Run the actual repair
    ui::SpinnerRunner pathSpinner;
    if (!verbose_) {
        pathSpinner.start("Rebuilding path tree...");
    }

    auto progressFn = [this](uint64_t current, uint64_t total) {
        if (verbose_ && current % 100 == 0) {
            std::cout << "  Progress: " << current << "/" << total << "\n";
        }
    };

    auto result = repairMgr.repairPathTree(progressFn);
    if (!verbose_) {
        pathSpinner.stop();
    }
    if (!result) {
        return result.error();
    }

    const auto& stats = result.value();
    std::cout << "  " << ui::status_ok("Path tree rebuild complete") << "\n";
    std::vector<ui::Row> ptRows = {
        {"Documents scanned", std::to_string(stats.documentsScanned), ""},
        {"Nodes created", std::to_string(stats.nodesCreated), ""},
        {"Errors",
         stats.errors > 0 ? ui::colorize(std::to_string(stats.errors), ui::Ansi::RED) : "0", ""},
    };
    ui::render_rows(std::cout, ptRows);

    return Result<void>();
}

Result<void> RepairCommand::repairBlockReferences() {
    std::cout << ui::status_pending("Repairing block references (sizes from CAS files)...") << "\n";

    namespace fs = std::filesystem;

    // Get storage paths
    fs::path objectsPath = cli_->getDataPath() / "storage" / "objects";
    fs::path refsDbPath = cli_->getDataPath() / "storage" / "refs.db";

    if (!fs::exists(objectsPath)) {
        std::cout << "  No objects directory found\n";
        return Result<void>();
    }

    if (!fs::exists(refsDbPath)) {
        std::cout << "  No refs.db found\n";
        return Result<void>();
    }

    std::cout << "  " << ui::key_value("Objects", objectsPath.string(), ui::Ansi::CYAN, 10) << "\n";
    std::cout << "  " << ui::key_value("RefsDB", refsDbPath.string(), ui::Ansi::CYAN, 10) << "\n";

    // Progress indicator for scanning
    ui::SpinnerRunner scanSpinner;
    scanSpinner.start("Scanning CAS files...");

    // Progress callback (spinner doesn't need updates, just runs)
    auto progressFn = [](uint64_t /*current*/, uint64_t /*total*/) {
        // Spinner runs independently, no update needed
    };

    auto result = integrity::RepairManager::repairBlockReferences(objectsPath, refsDbPath, dryRun_,
                                                                  progressFn);
    scanSpinner.stop();

    if (!result) {
        return result.error();
    }

    const auto& stats = result.value();

    if (dryRun_) {
        std::cout << "  [DRY RUN] Would update " << stats.blocksUpdated << " block references\n";
    } else {
        std::cout << "  " << ui::status_ok("Block references repair complete") << "\n";
    }
    std::vector<ui::Row> brRows = {
        {"Blocks scanned", std::to_string(stats.blocksScanned), ""},
        {"Blocks updated", std::to_string(stats.blocksUpdated), ""},
        {"Blocks skipped", std::to_string(stats.blocksSkipped), ""},
        {"Compressed blocks", std::to_string(stats.compressedBlocks), ""},
        {"Uncompressed blocks", std::to_string(stats.uncompressedBlocks), ""},
        {"Total disk bytes", ui::format_bytes(stats.totalDiskBytes), ""},
        {"Total uncompressed", ui::format_bytes(stats.totalUncompressedBytes), ""},
    };
    if (stats.totalUncompressedBytes > stats.totalDiskBytes) {
        brRows.push_back({"Compression savings",
                          ui::format_bytes(stats.totalUncompressedBytes - stats.totalDiskBytes),
                          ""});
    }
    if (stats.errors > 0) {
        brRows.push_back({"Errors", ui::colorize(std::to_string(stats.errors), ui::Ansi::RED), ""});
    }
    ui::render_rows(std::cout, brRows);

    return Result<void>();
}

// Factory function
std::unique_ptr<ICommand> createRepairCommand() {
    return std::make_unique<RepairCommand>();
}

} // namespace yams::cli
