#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>
#include <spdlog/spdlog.h>
#include <iostream>
#include <sstream>
#include <vector>
#include <algorithm>

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
        cmd->alias("rm");  // Add rm as alias for delete
        
        // Create option group for deletion methods (only one can be used at a time)
        auto* group = cmd->add_option_group("deletion_method");
        group->add_option("hash", hash_, "Document hash to delete");
        group->add_option("--name", name_, "Delete document by name");
        group->add_option("--names", names_, "Delete multiple documents by names (comma-separated)");
        group->add_option("--pattern", pattern_, "Delete documents matching pattern (e.g., *.log)");
        group->require_option(1);
        
        // Flags (can be combined with any deletion method)
        cmd->add_flag("--force,--no-confirm", force_, "Skip confirmation prompt");
        cmd->add_flag("--dry-run", dryRun_, "Preview what would be deleted without actually deleting");
        cmd->add_flag("--keep-refs", keepRefs_, "Keep reference counts (don't decrement)");
        cmd->add_flag("-v,--verbose", verbose_, "Enable verbose output");
        
        cmd->callback([this]() { 
            auto result = execute();
            if (!result) {
                spdlog::error("Delete failed: {}", result.error().message);
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
            
            // Collect all hashes to delete based on input method
            std::vector<std::pair<std::string, std::string>> toDelete; // pairs of (hash, name)
            
            if (!hash_.empty()) {
                // Direct hash deletion (existing functionality)
                if (hash_.length() != 64 || hash_.find_first_not_of("0123456789abcdef") != std::string::npos) {
                    return Error{ErrorCode::InvalidArgument, "Invalid hash format. Expected 64 character hex string."};
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
            
            // Confirm deletion unless --force is used
            if (!force_) {
                std::string prompt = toDelete.size() == 1 
                    ? "Are you sure you want to delete 1 document? (y/N): "
                    : "Are you sure you want to delete " + std::to_string(toDelete.size()) + " documents? (y/N): ";
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
            
            for (const auto& [hash, name] : toDelete) {
                if (verbose_) {
                    std::cout << "Deleting " << name << "...\n";
                }
                
                // Check if document exists
                auto existsResult = store->exists(hash);
                if (!existsResult || !existsResult.value()) {
                    failures.push_back(name + ": not found in storage");
                    failCount++;
                    continue;
                }
                
                // Delete the document
                auto deleteResult = store->remove(hash);
                if (!deleteResult) {
                    failures.push_back(name + ": " + deleteResult.error().message);
                    failCount++;
                } else {
                    successCount++;
                    if (verbose_) {
                        std::cout << "  Deleted " << name << "\n";
                    }
                }
            }
            
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
                std::cout << "  Storage size: " << (stats.totalBytes / (1024.0 * 1024.0)) 
                         << " MB\n";
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
    
    // Helper methods for name resolution
    Result<std::vector<std::pair<std::string, std::string>>> resolveNameToHashes(const std::string& name) {
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
                return Error{ErrorCode::NotFound, "Failed to query documents: " + documentsResult.error().message};
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
                std::cout << "  " << (i + 1) << ". " << fname << " (" << hash.substr(0, 12) << "...)\n";
            }
            std::cout << "Use --force to delete all matches, or specify the exact hash.\n";
            return Error{ErrorCode::InvalidOperation, "Multiple documents with same name"};
        }
        
        return results;
    }
    
    Result<std::vector<std::pair<std::string, std::string>>> resolveNamesToHashes(const std::string& namesList) {
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
    
    Result<std::vector<std::pair<std::string, std::string>>> resolvePatternToHashes(const std::string& pattern) {
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
            return Error{ErrorCode::NotFound, "Failed to query documents: " + documentsResult.error().message};
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
};

// Factory function
std::unique_ptr<ICommand> createDeleteCommand() {
    return std::make_unique<DeleteCommand>();
}

} // namespace yams::cli