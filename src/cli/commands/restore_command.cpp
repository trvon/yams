#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <regex>
#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>

namespace yams::cli {

using json = nlohmann::json;

class RestoreCommand : public ICommand {
public:
    std::string getName() const override { return "restore"; }

    std::string getDescription() const override {
        return "Restore documents from collections or snapshots to filesystem";
    }

    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;

        auto* cmd = app.add_subcommand("restore", getDescription());

        // Scoped retrieval options (mutually exclusive)
        auto* scopeGroup = cmd->add_option_group("scope");
        scopeGroup->add_option("-c,--collection", collection_,
                               "Restore all documents from collection");
        scopeGroup->add_option("--snapshot-id", snapshotId_,
                               "Restore all documents from snapshot ID");
        scopeGroup->add_option("--snapshot-label", snapshotLabel_,
                               "Restore all documents from snapshot label");
        scopeGroup->require_option(1);

        // Output options
        cmd->add_option("-o,--output", outputDir_, "Output directory (default: current directory)")
            ->default_val(".");
        cmd->add_option("--layout", layoutTemplate_, "Layout template for file placement")
            ->default_val("{path}");

        // Filter options
        cmd->add_option("--include", includePatterns_,
                        "File patterns to include (e.g., '*.txt,*.md')");
        cmd->add_option("--exclude", excludePatterns_,
                        "File patterns to exclude (e.g., '*.tmp,*.log')");

        // Control options
        cmd->add_flag("--dry-run", dryRun_,
                      "Show what would be restored without actually restoring");
        cmd->add_flag("--overwrite", overwrite_, "Overwrite existing files");
        cmd->add_flag("--create-dirs", createDirs_,
                      "Create output directories if they don't exist");

        cmd->callback([this]() {
            auto result = execute();
            if (!result) {
                spdlog::error("Command failed: {}", result.error().message);
                throw CLI::RuntimeError(1);
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

            // Get documents to restore based on scope
            auto documentsResult = getDocumentsToRestore(metadataRepo);
            if (!documentsResult) {
                return documentsResult.error();
            }

            const auto& documents = documentsResult.value();

            if (documents.empty()) {
                if (cli_->getJsonOutput() || cli_->getVerbose()) {
                    json output;
                    output["documents_found"] = 0;
                    output["documents_restored"] = 0;
                    output["scope"] = getScopeDescription();
                    std::cout << output.dump(2) << std::endl;
                } else {
                    std::cout << "No documents found matching the specified scope." << std::endl;
                }
                return Result<void>();
            }

            // Filter documents by include/exclude patterns
            auto filteredDocs = filterDocuments(documents);

            if (dryRun_) {
                return showDryRun(filteredDocs);
            }

            // Create output directory if needed
            if (createDirs_ && !std::filesystem::exists(outputDir_)) {
                std::filesystem::create_directories(outputDir_);
            }

            // Restore documents
            return restoreDocuments(store, metadataRepo, filteredDocs);

        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string("Unexpected error: ") + e.what()};
        }
    }

private:
    Result<std::vector<metadata::DocumentInfo>>
    getDocumentsToRestore(std::shared_ptr<metadata::IMetadataRepository> metadataRepo) {
        if (!collection_.empty()) {
            return metadataRepo->findDocumentsByCollection(collection_);
        } else if (!snapshotId_.empty()) {
            return metadataRepo->findDocumentsBySnapshot(snapshotId_);
        } else if (!snapshotLabel_.empty()) {
            return metadataRepo->findDocumentsBySnapshotLabel(snapshotLabel_);
        }

        return Error{ErrorCode::InvalidArgument, "No scope specified"};
    }

    std::vector<metadata::DocumentInfo>
    filterDocuments(const std::vector<metadata::DocumentInfo>& documents) {
        std::vector<metadata::DocumentInfo> filtered;

        for (const auto& doc : documents) {
            if (shouldIncludeDocument(doc)) {
                filtered.push_back(doc);
            }
        }

        return filtered;
    }

    bool shouldIncludeDocument(const metadata::DocumentInfo& doc) {
        std::string fileName = doc.fileName;

        // Check exclude patterns first
        for (const auto& pattern : excludePatterns_) {
            if (matchesPattern(fileName, pattern)) {
                return false;
            }
        }

        // If no include patterns specified, include all files (that weren't excluded)
        if (includePatterns_.empty()) {
            return true;
        }

        // Check include patterns
        for (const auto& pattern : includePatterns_) {
            if (matchesPattern(fileName, pattern)) {
                return true;
            }
        }

        return false;
    }

    bool matchesPattern(const std::string& text, const std::string& pattern) {
        // Simple wildcard matching (* and ?)
        std::regex regexPattern;
        std::string regexString = pattern;

        // Convert wildcards to regex
        size_t pos = 0;
        while ((pos = regexString.find('*', pos)) != std::string::npos) {
            regexString.replace(pos, 1, ".*");
            pos += 2;
        }
        pos = 0;
        while ((pos = regexString.find('?', pos)) != std::string::npos) {
            regexString.replace(pos, 1, ".");
            pos += 1;
        }

        try {
            regexPattern = std::regex(regexString, std::regex_constants::icase);
            return std::regex_match(text, regexPattern);
        } catch (const std::regex_error&) {
            // If regex fails, fall back to simple string comparison
            return text == pattern;
        }
    }

    Result<std::string>
    expandLayoutTemplate(const metadata::DocumentInfo& doc,
                         const std::unordered_map<std::string, metadata::MetadataValue>& metadata) {
        std::string result = layoutTemplate_;

        // Replace placeholders
        size_t pos = 0;
        while ((pos = result.find("{", pos)) != std::string::npos) {
            size_t endPos = result.find("}", pos);
            if (endPos == std::string::npos)
                break;

            std::string placeholder = result.substr(pos + 1, endPos - pos - 1);
            std::string replacement;

            if (placeholder == "collection") {
                auto it = metadata.find("collection");
                replacement = (it != metadata.end()) ? it->second.asString() : "unknown";
            } else if (placeholder == "snapshot_id") {
                auto it = metadata.find("snapshot_id");
                replacement = (it != metadata.end()) ? it->second.asString() : "unknown";
            } else if (placeholder == "snapshot_label") {
                auto it = metadata.find("snapshot_label");
                replacement = (it != metadata.end()) ? it->second.asString() : "unknown";
            } else if (placeholder == "path") {
                auto it = metadata.find("path");
                replacement = (it != metadata.end()) ? it->second.asString() : doc.fileName;
            } else if (placeholder == "name") {
                replacement = doc.fileName;
            } else if (placeholder == "hash") {
                replacement = doc.sha256Hash.substr(0, 12);
            } else {
                // Unknown placeholder, leave as is
                pos = endPos + 1;
                continue;
            }

            result.replace(pos, endPos - pos + 1, replacement);
            pos += replacement.length();
        }

        return result;
    }

    Result<void> showDryRun(const std::vector<metadata::DocumentInfo>& documents) {
        if (cli_->getJsonOutput() || cli_->getVerbose()) {
            json output;
            output["dry_run"] = true;
            output["documents_found"] = documents.size();
            output["scope"] = getScopeDescription();
            output["output_directory"] = outputDir_.string();
            output["layout_template"] = layoutTemplate_;

            json fileList = json::array();
            for (const auto& doc : documents) {
                json fileInfo;
                fileInfo["name"] = doc.fileName;
                fileInfo["hash"] = doc.sha256Hash;
                fileInfo["size"] = doc.fileSize;

                // Get metadata for layout expansion
                auto metadataRepo = cli_->getMetadataRepository();
                auto metadataResult = metadataRepo->getAllMetadata(doc.id);
                if (metadataResult) {
                    auto layoutResult = expandLayoutTemplate(doc, metadataResult.value());
                    if (layoutResult) {
                        auto outputPath = outputDir_ / layoutResult.value();
                        fileInfo["output_path"] = outputPath.string();
                    }
                }

                fileList.push_back(fileInfo);
            }
            output["files"] = fileList;

            std::cout << output.dump(2) << std::endl;
        } else {
            std::cout << "DRY RUN - Documents that would be restored:" << std::endl;
            std::cout << "Scope: " << getScopeDescription() << std::endl;
            std::cout << "Output directory: " << outputDir_ << std::endl;
            std::cout << "Layout template: " << layoutTemplate_ << std::endl;
            std::cout << std::endl;

            auto metadataRepo = cli_->getMetadataRepository();
            for (const auto& doc : documents) {
                auto metadataResult = metadataRepo->getAllMetadata(doc.id);
                std::string outputPath = doc.fileName;
                if (metadataResult) {
                    auto layoutResult = expandLayoutTemplate(doc, metadataResult.value());
                    if (layoutResult) {
                        outputPath = layoutResult.value();
                    }
                }

                std::cout << "  " << doc.fileName << " -> " << (outputDir_ / outputPath).string()
                          << " (" << doc.fileSize << " bytes)" << std::endl;
            }

            std::cout << std::endl;
            std::cout << "Total: " << documents.size() << " documents" << std::endl;
        }

        return Result<void>();
    }

    Result<void> restoreDocuments(std::shared_ptr<api::IContentStore> store,
                                  std::shared_ptr<metadata::IMetadataRepository> metadataRepo,
                                  const std::vector<metadata::DocumentInfo>& documents) {
        size_t successCount = 0;
        size_t failureCount = 0;
        size_t skippedCount = 0;

        for (const auto& doc : documents) {
            // Get metadata for layout expansion
            auto metadataResult = metadataRepo->getAllMetadata(doc.id);
            if (!metadataResult) {
                spdlog::warn("Failed to get metadata for {}: {}", doc.fileName,
                             metadataResult.error().message);
                failureCount++;
                continue;
            }

            // Expand layout template
            auto layoutResult = expandLayoutTemplate(doc, metadataResult.value());
            if (!layoutResult) {
                spdlog::error("Failed to expand layout template for {}: {}", doc.fileName,
                              layoutResult.error().message);
                failureCount++;
                continue;
            }

            auto outputPath = outputDir_ / layoutResult.value();

            // Check if file already exists
            if (std::filesystem::exists(outputPath) && !overwrite_) {
                spdlog::info("Skipping existing file: {}", outputPath.string());
                skippedCount++;
                continue;
            }

            // Create parent directories if needed
            if (createDirs_) {
                std::filesystem::create_directories(outputPath.parent_path());
            }

            // Retrieve document
            auto result = store->retrieve(doc.sha256Hash, outputPath);
            if (!result) {
                spdlog::error("Failed to restore {}: {}", doc.fileName, result.error().message);
                failureCount++;
                continue;
            }

            spdlog::info("Restored: {} -> {}", doc.fileName, outputPath.string());
            successCount++;
        }

        // Output summary
        if (cli_->getJsonOutput() || cli_->getVerbose()) {
            json output;
            output["documents_found"] = documents.size();
            output["documents_restored"] = successCount;
            output["documents_failed"] = failureCount;
            output["documents_skipped"] = skippedCount;
            output["scope"] = getScopeDescription();
            output["output_directory"] = outputDir_.string();
            std::cout << output.dump(2) << std::endl;
        } else {
            std::cout << "Restore complete!" << std::endl;
            std::cout << "Documents restored: " << successCount << std::endl;
            if (failureCount > 0) {
                std::cout << "Documents failed: " << failureCount << std::endl;
            }
            if (skippedCount > 0) {
                std::cout << "Documents skipped: " << skippedCount << std::endl;
            }
            std::cout << "Output directory: " << outputDir_ << std::endl;
        }

        return Result<void>();
    }

    std::string getScopeDescription() {
        if (!collection_.empty()) {
            return "collection: " + collection_;
        } else if (!snapshotId_.empty()) {
            return "snapshot ID: " + snapshotId_;
        } else if (!snapshotLabel_.empty()) {
            return "snapshot label: " + snapshotLabel_;
        }
        return "unknown";
    }

    YamsCLI* cli_ = nullptr;

    // Scope options
    std::string collection_;
    std::string snapshotId_;
    std::string snapshotLabel_;

    // Output options
    std::filesystem::path outputDir_;
    std::string layoutTemplate_;

    // Filter options
    std::vector<std::string> includePatterns_;
    std::vector<std::string> excludePatterns_;

    // Control options
    bool dryRun_ = false;
    bool overwrite_ = false;
    bool createDirs_ = false;
};

// Factory function
std::unique_ptr<ICommand> createRestoreCommand() {
    return std::make_unique<RestoreCommand>();
}

} // namespace yams::cli