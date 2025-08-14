#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>
#include <yams/cli/progress_indicator.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/detection/file_type_detector.h>
#include <spdlog/spdlog.h>
#include <iostream>
#include <iomanip>
#include <sqlite3.h>
#include <filesystem>

namespace yams::cli {

class RepairCommand : public ICommand {
public:
    std::string getName() const override { return "repair"; }
    
    std::string getDescription() const override { 
        return "Repair and maintain storage integrity";
    }
    
    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;
        
        auto* cmd = app.add_subcommand("repair", getDescription());
        
        cmd->add_flag("--orphans", repairOrphans_, "Clean orphaned metadata entries");
        cmd->add_flag("--mime", repairMime_, "Repair missing MIME types");
        cmd->add_flag("--chunks", repairChunks_, "Clean orphaned chunk files");
        cmd->add_flag("--optimize", optimizeDb_, "Optimize and vacuum database");
        cmd->add_flag("--checksums", verifyChecksums_, "Verify and repair checksums");
        cmd->add_flag("--duplicates", mergeDuplicates_, "Find and optionally merge duplicates");
        cmd->add_flag("--all", repairAll_, "Run all repair operations");
        cmd->add_flag("--dry-run", dryRun_, "Preview changes without applying");
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
            if (!repairOrphans_ && !repairMime_ && !repairChunks_ && !optimizeDb_ &&
                !verifyChecksums_ && !mergeDuplicates_ && !repairAll_) {
                repairOrphans_ = true;
            }
            
            // If --all is specified, enable all repairs
            if (repairAll_) {
                repairOrphans_ = true;
                repairMime_ = true;
                repairChunks_ = true;
                optimizeDb_ = true;
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
            
            // Optimize database
            if (optimizeDb_) {
                auto result = optimizeDatabase();
                if (!result) {
                    return result;
                }
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
    bool optimizeDb_ = false;
    bool verifyChecksums_ = false;
    bool mergeDuplicates_ = false;
    bool repairAll_ = false;
    bool dryRun_ = false;
    bool verbose_ = false;
    bool force_ = false;
    
    Result<void> cleanOrphanedMetadata(
        std::shared_ptr<api::IContentStore> store,
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
    
    Result<void> repairMimeTypes(
        std::shared_ptr<api::IContentStore> store,
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
                    detectedMime = detection::FileTypeDetector::getMimeTypeFromExtension(doc.fileExtension);
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
                        std::cout << "  Missing MIME: " << doc.fileName 
                                 << " -> " << detectedMime << "\n";
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
    
    Result<void> cleanOrphanedChunks(
        std::shared_ptr<api::IContentStore> store) {
        
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
            return Error{ErrorCode::DatabaseError, "Failed to open refs.db: " + std::string(sqlite3_errmsg(db))};
        }
        
        // First, get all REFERENCED blocks (ref_count > 0)
        std::set<std::string> referencedHashes;
        sqlite3_stmt* stmt;
        const char* sql = "SELECT block_hash FROM block_references WHERE ref_count > 0";
        
        if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
            sqlite3_close(db);
            return Error{ErrorCode::DatabaseError, "Failed to prepare query: " + std::string(sqlite3_errmsg(db))};
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
        
        std::cout << "  Space to reclaim: " << formatSize(bytesToReclaim) 
                 << " from " << orphanedChunks.size() << " files\n";
        
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
            if (sqlite3_exec(db, "DELETE FROM block_references WHERE ref_count = 0", 
                            nullptr, nullptr, &errMsg) != SQLITE_OK) {
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
            std::cout << "  [DRY RUN] Would delete " << orphanedChunks.size() 
                     << " chunk files\n";
            std::cout << "  [DRY RUN] Would reclaim " << formatSize(bytesToReclaim) 
                     << "\n";
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
                return Error{ErrorCode::DatabaseError, "Failed to open database: " + std::string(sqlite3_errmsg(db))};
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

// Factory function
std::unique_ptr<ICommand> createRepairCommand() {
    return std::make_unique<RepairCommand>();
}

} // namespace yams::cli