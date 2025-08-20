#include <spdlog/spdlog.h>
#include <algorithm>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <vector>
#include <yams/cli/command.h>
#include <yams/cli/daemon_helpers.h>
#include <yams/cli/progress_indicator.h>
#include <yams/cli/yams_cli.h>
#include <yams/daemon/ipc/ipc_protocol.h>

namespace yams::cli {

class DeleteCommand : public ICommand {
public:
    std::string getName() const override { return "delete"; }

    std::string getDescription() const override {
        return "Delete documents by hash, name, or pattern";
    }

    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;

        auto* cmd = app.add_subcommand("delete", getDescription());
        cmd->alias("rm"); // Add rm as alias for delete

        // Create option group for deletion methods (only one can be used at a time)
        auto* group = cmd->add_option_group("deletion_method");
        group->add_option("hash", hash_, "Document hash to delete");
        group->add_option("--name", name_, "Delete document by name");
        group->add_option("--names", names_,
                          "Delete multiple documents by names (comma-separated)");
        group->add_option("--pattern", pattern_, "Delete documents matching pattern (e.g., *.log)");
        group->add_option("--directory", directory_, "Directory to delete (requires --recursive)");
        group->require_option(1);

        // Flags (can be combined with any deletion method)
        cmd->add_flag("--force,--no-confirm", force_, "Skip confirmation prompt");
        cmd->add_flag("--dry-run", dryRun_,
                      "Preview what would be deleted without actually deleting");
        cmd->add_flag("--keep-refs", keepRefs_, "Keep reference counts (don't decrement)");
        cmd->add_flag("-v,--verbose", verbose_, "Enable verbose output");
        cmd->add_flag("--recursive,-r", recursive_, "Delete directory contents recursively");

        cmd->callback([this]() {
            auto result = execute();
            if (!result) {
                spdlog::error("Delete failed: {}", result.error().message);
                throw CLI::RuntimeError(1);
            }
        });
    }

    Result<void> execute() override {
        try {
            // Simple daemon-first path for direct hash deletion
            if (!hash_.empty() && name_.empty() && names_.empty() && pattern_.empty() &&
                directory_.empty() && !dryRun_) {
                yams::daemon::DeleteRequest dreq;
                dreq.hash = hash_;
                dreq.purge = !keepRefs_;
                auto render = [&](const yams::daemon::SuccessResponse&) -> Result<void> {
                    std::cout << "Deleted " << hash_.substr(0, 12) << "... via daemon\n";
                    return Result<void>();
                };
                auto fallback = [&]() -> Result<void> {
                    return Error{ErrorCode::NotImplemented, "local"};
                };
                if (auto d = daemon_first(dreq, fallback, render); d) {
                    return Result<void>();
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

            // Collect all hashes to delete based on input method
            std::vector<std::pair<std::string, std::string>> toDelete; // pairs of (hash, name)

            if (!hash_.empty()) {
                // Direct hash deletion (existing functionality)
                if (hash_.length() != 64 ||
                    hash_.find_first_not_of("0123456789abcdef") != std::string::npos) {
                    return Error{ErrorCode::InvalidArgument,
                                 "Invalid hash format. Expected 64 character hex string."};
                }
                toDelete.push_back({hash_, "hash:" + hash_.substr(0, 8) + "..."});
            } else if (!name_.empty()) {
                // Delete by single name
                auto result = resolveNameToHashes(name_);
                if (!result) {
                    return result.error();
                }
                toDelete = result.value();
            } else if (!names_.empty()) {
                // Delete by multiple names
                auto result = resolveNamesToHashes(names_);
                if (!result) {
                    return result.error();
                }
                toDelete = result.value();
            } else if (!pattern_.empty()) {
                // Delete by pattern
                auto result = resolvePatternToHashes(pattern_);
                if (!result) {
                    return result.error();
                }
                toDelete = result.value();
            } else if (!directory_.empty()) {
                // Delete by directory
                if (!recursive_) {
                    return Error{ErrorCode::InvalidArgument,
                                 "Directory deletion requires --recursive flag for safety"};
                }
                auto result = resolveDirectoryToHashes(directory_);
                if (!result) {
                    return result.error();
                }
                toDelete = result.value();
            } else {
                return Error{ErrorCode::InvalidArgument, "No deletion criteria specified"};
            }

            if (toDelete.empty()) {
                std::cout << "No documents found matching the criteria.\n";
                return Result<void>();
            }

            // Show what will be deleted in dry-run or verbose mode
            if (dryRun_ || verbose_) {
                std::cout << "Documents to be deleted:\n";
                for (const auto& [hash, name] : toDelete) {
                    std::cout << "  " << name << " (" << hash.substr(0, 12) << "...)\n";
                }
                if (dryRun_) {
                    std::cout << "\n[DRY RUN] Would delete " << toDelete.size() << " document(s)\n";
                    return Result<void>();
                }
            }

            // Calculate total size if needed for confirmation
            uint64_t totalSize = 0;
            if (!force_ || verbose_) {
                auto metadataRepo = cli_->getMetadataRepository();
                if (metadataRepo) {
                    for (const auto& [hash, name] : toDelete) {
                        auto docResult = metadataRepo->getDocumentByHash(hash);
                        if (docResult && docResult.value()) {
                            totalSize += docResult.value()->fileSize;
                        }
                    }
                }
            }

            // Confirm deletion unless --force is used
            if (!force_) {
                std::string prompt =
                    toDelete.size() == 1
                        ? "Delete 1 document"
                        : "Delete " + std::to_string(toDelete.size()) + " documents";

                if (totalSize > 0) {
                    prompt += " (total size: " + formatSize(totalSize) + ")";
                }
                prompt += "? (y/N): ";

                std::cout << prompt;
                std::string response;
                std::getline(std::cin, response);
                if (response != "y" && response != "Y") {
                    std::cout << "Deletion cancelled.\n";
                    return Result<void>();
                }
            }

            // Delete all documents
            size_t successCount = 0;
            size_t failCount = 0;
            std::vector<std::string> failures;

            // Use progress indicator for large deletions
            ProgressIndicator progress(ProgressIndicator::Style::Percentage);
            if (toDelete.size() > 100 && !dryRun_) {
                progress.start("Deleting files");
            }

            for (size_t i = 0; i < toDelete.size(); ++i) {
                const auto& [hash, name] = toDelete[i];

                if (verbose_) {
                    std::cout << "Deleting " << name << "...\n";
                }

                // Check if document exists
                auto existsResult = store->exists(hash);
                if (!existsResult || !existsResult.value()) {
                    // File doesn't exist in storage, check if we need to clean up metadata
                    if (auto metadataRepo = cli_->getMetadataRepository()) {
                        auto docResult = metadataRepo->getDocumentByHash(hash);
                        if (docResult && docResult.value()) {
                            // Clean up orphaned metadata entry
                            auto deleteMetaResult =
                                metadataRepo->deleteDocument(docResult.value()->id);
                            if (deleteMetaResult) {
                                successCount++;
                                if (verbose_) {
                                    std::cout << "  Cleaned orphaned metadata for " << name << "\n";
                                }
                                continue;
                            }
                        }
                    }
                    failures.push_back(name + ": not found in storage");
                    failCount++;
                    continue;
                }

                // Delete the document from storage
                auto deleteResult = store->remove(hash);
                if (!deleteResult) {
                    failures.push_back(name + ": " + deleteResult.error().message);
                    failCount++;
                } else {
                    successCount++;

                    // Also remove from metadata repository
                    if (auto metadataRepo = cli_->getMetadataRepository()) {
                        auto docResult = metadataRepo->getDocumentByHash(hash);
                        if (docResult && docResult.value()) {
                            auto deleteMetaResult =
                                metadataRepo->deleteDocument(docResult.value()->id);
                            if (!deleteMetaResult) {
                                spdlog::warn("Failed to delete metadata for {}: {}", hash,
                                             deleteMetaResult.error().message);
                            }
                        }
                    }

                    if (verbose_) {
                        std::cout << "  Deleted " << name << "\n";
                    }
                }

                // Update progress indicator
                if (toDelete.size() > 100 && !dryRun_) {
                    progress.update(i + 1, toDelete.size());
                }
            }

            // Stop progress indicator
            progress.stop();

            // Report results
            if (successCount > 0) {
                std::cout << "Successfully deleted " << successCount << " document(s)\n";
            }
            if (failCount > 0) {
                std::cout << "Failed to delete " << failCount << " document(s):\n";
                for (const auto& failure : failures) {
                    std::cout << "  " << failure << "\n";
                }
            }

            // Show storage stats if verbose
            if (verbose_ && successCount > 0) {
                auto stats = store->getStats();
                std::cout << "\nStorage statistics:\n";
                std::cout << "  Total objects: " << stats.totalObjects << "\n";
                std::cout << "  Unique blocks: " << stats.uniqueBlocks << "\n";
                std::cout << "  Storage size: " << formatSize(stats.totalBytes) << "\n";
            }

            return failCount > 0 && successCount == 0
                       ? Error{ErrorCode::InvalidOperation, "All deletions failed"}
                       : Result<void>();

        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string(e.what())};
        }
    }

private:
    YamsCLI* cli_ = nullptr;
    std::string hash_;
    std::string name_;
    std::string names_;
    std::string pattern_;
    bool force_ = false;
    bool dryRun_ = false;
    bool keepRefs_ = false;
    bool verbose_ = false;
    bool recursive_ = false;
    std::string directory_;

    // Helper methods for name resolution
    Result<std::vector<std::pair<std::string, std::string>>>
    resolveNameToHashes(const std::string& name) {
        auto metadataRepo = cli_->getMetadataRepository();
        if (!metadataRepo) {
            return Error{ErrorCode::NotInitialized, "Metadata repository not available"};
        }

        // Search for documents with matching fileName
        auto documentsResult = metadataRepo->findDocumentsByPath("%/" + name);
        if (!documentsResult) {
            // Try exact match
            documentsResult = metadataRepo->findDocumentsByPath(name);
            if (!documentsResult) {
                return Error{ErrorCode::NotFound,
                             "Failed to query documents: " + documentsResult.error().message};
            }
        }

        std::vector<std::pair<std::string, std::string>> results;
        for (const auto& doc : documentsResult.value()) {
            if (doc.fileName == name || doc.filePath == name) {
                results.push_back({doc.sha256Hash, doc.fileName});
            }
        }

        if (results.empty()) {
            return Error{ErrorCode::NotFound, "No documents found with name: " + name};
        }

        // Handle ambiguous names
        if (results.size() > 1 && !force_) {
            std::cout << "Multiple documents found with name '" << name << "':\n";
            for (size_t i = 0; i < results.size(); ++i) {
                const auto& [hash, fname] = results[i];
                std::cout << "  " << (i + 1) << ". " << fname << " (" << hash.substr(0, 12)
                          << "...)\n";
            }
            std::cout << "Use --force to delete all matches, or specify the exact hash.\n";
            return Error{ErrorCode::InvalidOperation, "Multiple documents with same name"};
        }

        return results;
    }

    Result<std::vector<std::pair<std::string, std::string>>>
    resolveNamesToHashes(const std::string& namesList) {
        std::vector<std::pair<std::string, std::string>> allResults;

        // Split comma-separated names
        std::vector<std::string> names;
        std::stringstream ss(namesList);
        std::string name;
        while (std::getline(ss, name, ',')) {
            // Trim whitespace
            name.erase(0, name.find_first_not_of(" \t"));
            name.erase(name.find_last_not_of(" \t") + 1);
            if (!name.empty()) {
                names.push_back(name);
            }
        }

        if (names.empty()) {
            return Error{ErrorCode::InvalidArgument, "No valid names provided"};
        }

        // Resolve each name
        for (const auto& n : names) {
            auto result = resolveNameToHashes(n);
            if (result) {
                for (const auto& pair : result.value()) {
                    allResults.push_back(pair);
                }
            } else if (result.error().code != ErrorCode::NotFound) {
                // Only fail on errors other than NotFound
                if (!force_) {
                    return result.error();
                }
            }
        }

        return allResults;
    }

    Result<std::vector<std::pair<std::string, std::string>>>
    resolvePatternToHashes(const std::string& pattern) {
        auto metadataRepo = cli_->getMetadataRepository();
        if (!metadataRepo) {
            return Error{ErrorCode::NotInitialized, "Metadata repository not available"};
        }

        // Convert glob pattern to SQL LIKE pattern
        std::string sqlPattern = pattern;
        // Replace * with %
        size_t pos = 0;
        while ((pos = sqlPattern.find('*', pos)) != std::string::npos) {
            sqlPattern.replace(pos, 1, "%");
            pos++;
        }
        // Replace ? with _
        pos = 0;
        while ((pos = sqlPattern.find('?', pos)) != std::string::npos) {
            sqlPattern.replace(pos, 1, "_");
            pos++;
        }

        // Add % for path matching if pattern doesn't start with /
        if (sqlPattern[0] != '/') {
            sqlPattern = "%/" + sqlPattern;
        }

        auto documentsResult = metadataRepo->findDocumentsByPath(sqlPattern);
        if (!documentsResult) {
            return Error{ErrorCode::NotFound,
                         "Failed to query documents: " + documentsResult.error().message};
        }

        std::vector<std::pair<std::string, std::string>> results;
        for (const auto& doc : documentsResult.value()) {
            // Additional filtering for exact pattern match on fileName
            if (matchesPattern(doc.fileName, pattern) || matchesPattern(doc.filePath, pattern)) {
                results.push_back({doc.sha256Hash, doc.fileName});
            }
        }

        if (results.empty()) {
            return Error{ErrorCode::NotFound, "No documents found matching pattern: " + pattern};
        }

        return results;
    }

    bool matchesPattern(const std::string& str, const std::string& pattern) {
        size_t strIdx = 0, patIdx = 0;
        size_t strLen = str.length(), patLen = pattern.length();
        size_t lastWildcard = std::string::npos, lastMatch = 0;

        while (strIdx < strLen) {
            if (patIdx < patLen && (pattern[patIdx] == '?' || pattern[patIdx] == str[strIdx])) {
                strIdx++;
                patIdx++;
            } else if (patIdx < patLen && pattern[patIdx] == '*') {
                lastWildcard = patIdx;
                lastMatch = strIdx;
                patIdx++;
            } else if (lastWildcard != std::string::npos) {
                patIdx = lastWildcard + 1;
                lastMatch++;
                strIdx = lastMatch;
            } else {
                return false;
            }
        }

        while (patIdx < patLen && pattern[patIdx] == '*') {
            patIdx++;
        }

        return patIdx == patLen;
    }

    Result<std::vector<std::pair<std::string, std::string>>>
    resolveDirectoryToHashes(const std::string& directory) {
        auto metadataRepo = cli_->getMetadataRepository();
        if (!metadataRepo) {
            return Error{ErrorCode::NotInitialized, "Metadata repository not available"};
        }

        // Build pattern for directory contents
        std::string pattern = directory;
        if (!pattern.empty() && pattern.back() != '/') {
            pattern += '/';
        }
        pattern += "%";

        auto documentsResult = metadataRepo->findDocumentsByPath(pattern);
        if (!documentsResult) {
            return Error{ErrorCode::DatabaseError,
                         "Failed to query directory: " + documentsResult.error().message};
        }

        std::vector<std::pair<std::string, std::string>> results;
        std::string dirPrefix = directory;
        if (!dirPrefix.empty() && dirPrefix.back() != '/') {
            dirPrefix += '/';
        }

        for (const auto& doc : documentsResult.value()) {
            // Verify path starts with directory
            if (doc.filePath.find(dirPrefix) == 0 || doc.filePath == directory) {
                results.push_back({doc.sha256Hash, doc.filePath});
            }
        }

        if (results.empty()) {
            return Error{ErrorCode::NotFound, "No documents found in directory: " + directory};
        }

        return results;
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
std::unique_ptr<ICommand> createDeleteCommand() {
    return std::make_unique<DeleteCommand>();
}

} // namespace yams::cli
