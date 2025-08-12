#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>
#include <yams/api/content_metadata.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/document_metadata.h>
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>
#include <iostream>
#include <fstream>
#include <sstream>
#include <chrono>
#include <random>
#include <iomanip>
#include <regex>

namespace yams::cli {

using json = nlohmann::json;

class AddCommand : public ICommand {
public:
    std::string getName() const override { return "add"; }
    
    std::string getDescription() const override { 
        return "Add document(s) or directory to the content store";
    }
    
    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;
        
        auto* cmd = app.add_subcommand("add", getDescription());
        cmd->add_option("path", targetPath_, "Path to file/directory to add (use '-' for stdin)")
            ->default_val("-");
        
        cmd->add_option("-n,--name", documentName_, "Name for the document (especially useful for stdin)");
        cmd->add_option("-t,--tags", tags_, "Tags for the document (comma-separated)");
        cmd->add_option("-m,--metadata", metadata_, "Metadata key=value pairs");
        cmd->add_option("--mime-type", mimeType_, "MIME type of the document");
        
        // Collection and snapshot options
        cmd->add_option("-c,--collection", collection_, "Collection name for organizing documents");
        cmd->add_option("--snapshot-id", snapshotId_, "Unique snapshot identifier");
        cmd->add_option("--snapshot-label", snapshotLabel_, "User-friendly snapshot label");
        
        // Directory options
        cmd->add_flag("-r,--recursive", recursive_, "Recursively add files from directories");
        cmd->add_option("--include", includePatterns_, "File patterns to include (e.g., '*.txt,*.md')");
        cmd->add_option("--exclude", excludePatterns_, "File patterns to exclude (e.g., '*.tmp,*.log')");
        
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
            
            // Check if reading from stdin
            if (targetPath_.string() == "-") {
                return storeFromStdin(store);
            }
            
            // Validate path exists
            if (!std::filesystem::exists(targetPath_)) {
                return Error{ErrorCode::FileNotFound, "Path not found: " + targetPath_.string()};
            }
            
            // Generate snapshot ID if not provided but snapshot label is given
            if (snapshotId_.empty() && !snapshotLabel_.empty()) {
                snapshotId_ = generateSnapshotId();
            }
            
            // Handle directory vs file
            if (std::filesystem::is_directory(targetPath_)) {
                return storeDirectory(store);
            } else {
                return storeFile(store, targetPath_);
            }
            
        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string("Unexpected error: ") + e.what()};
        }
    }
    
private:
    Result<void> storeFromStdin(std::shared_ptr<api::IContentStore> store) {
        // Read all content from stdin
        std::string content;
        std::string line;
        while (std::getline(std::cin, line)) {
            content += line + "\n";
        }
        
        if (content.empty()) {
            return Error{ErrorCode::InvalidArgument, "No content received from stdin"};
        }
        
        // Create a temporary file
        std::filesystem::path tempPath = std::filesystem::temp_directory_path() / 
                                        ("yams_stdin_" + std::to_string(std::chrono::system_clock::now().time_since_epoch().count()));
        
        std::ofstream tempFile(tempPath, std::ios::binary);
        if (!tempFile) {
            return Error{ErrorCode::FileNotFound, "Failed to create temporary file"};
        }
        tempFile.write(content.data(), content.size());
        tempFile.close();
        
        // Build metadata
        api::ContentMetadata metadata;
        metadata.name = documentName_.empty() ? "stdin" : documentName_;
        metadata.mimeType = mimeType_.empty() ? "text/plain" : mimeType_;
        
        // Convert vector tags to unordered_map
        for (const auto& tag : tags_) {
            metadata.tags[tag] = "";
        }
        
        // Add custom metadata
        for (const auto& kv : metadata_) {
            auto pos = kv.find('=');
            if (pos != std::string::npos) {
                std::string key = kv.substr(0, pos);
                std::string value = kv.substr(pos + 1);
                metadata.tags[key] = value;
            }
        }
        
        // Store the file
        auto result = store->store(tempPath, metadata);
        
        // Clean up temp file
        std::filesystem::remove(tempPath);
        
        if (!result) {
            return Error{result.error().code, result.error().message};
        }
        
        // Store metadata in database
        auto metadataRepo = cli_->getMetadataRepository();
        if (metadataRepo) {
            metadata::DocumentInfo docInfo;
            docInfo.filePath = "stdin";
            docInfo.fileName = documentName_.empty() ? "stdin" : documentName_;
            docInfo.fileExtension = "";
            docInfo.fileSize = content.size();
            docInfo.sha256Hash = result.value().contentHash;
            docInfo.mimeType = metadata.mimeType;
            
            auto now = std::chrono::system_clock::now();
            docInfo.createdTime = now;
            docInfo.modifiedTime = now;
            docInfo.indexedTime = now;
            
            // Insert document
            auto insertResult = metadataRepo->insertDocument(docInfo);
            if (insertResult) {
                int64_t docId = insertResult.value();
                
                // Add tags as metadata
                for (const auto& tag : tags_) {
                    metadataRepo->setMetadata(docId, "tag", metadata::MetadataValue(tag));
                }
                
                // Add custom metadata
                for (const auto& kv : metadata_) {
                    auto pos = kv.find('=');
                    if (pos != std::string::npos) {
                        std::string key = kv.substr(0, pos);
                        std::string value = kv.substr(pos + 1);
                        metadataRepo->setMetadata(docId, key, metadata::MetadataValue(value));
                    }
                }
                
                // Index document content for full-text search
                auto indexResult = metadataRepo->indexDocumentContent(
                    docId,
                    docInfo.fileName,
                    content,  // We already have the content from stdin
                    docInfo.mimeType
                );
                if (!indexResult) {
                    spdlog::warn("Failed to index stdin content: {}", 
                               indexResult.error().message);
                }
                
                // Update fuzzy index
                metadataRepo->updateFuzzyIndex(docId);
                
                spdlog::debug("Stored metadata for stdin document ID: {}", docId);
            } else {
                spdlog::warn("Failed to store metadata for stdin: {}", insertResult.error().message);
            }
        }
        
        outputResult(result.value());
        return Result<void>();
    }
    
    Result<void> storeFile(std::shared_ptr<api::IContentStore> store, const std::filesystem::path& filePath) {
        // Build metadata
        api::ContentMetadata metadata = buildMetadata(filePath.filename().string());
        
        // Store the file
        auto result = store->store(filePath, metadata);
        if (!result) {
            return Error{result.error().code, result.error().message};
        }
        
        // Store metadata in database
        auto storeMetadataResult = storeFileMetadata(filePath, result.value());
        if (!storeMetadataResult) {
            spdlog::warn("Failed to store metadata: {}", storeMetadataResult.error().message);
        }
        
        outputResult(result.value());
        return Result<void>();
    }
    
    Result<void> storeDirectory(std::shared_ptr<api::IContentStore> store) {
        if (!recursive_) {
            return Error{ErrorCode::InvalidArgument, "Directory specified but --recursive not set"};
        }
        
        std::vector<std::filesystem::path> filesToAdd;
        
        // Collect files to add
        try {
            for (const auto& entry : std::filesystem::recursive_directory_iterator(targetPath_)) {
                if (!entry.is_regular_file()) continue;
                
                // Check include/exclude patterns
                if (!shouldIncludeFile(entry.path())) continue;
                
                filesToAdd.push_back(entry.path());
            }
        } catch (const std::filesystem::filesystem_error& e) {
            return Error{ErrorCode::FileNotFound, "Failed to traverse directory: " + std::string(e.what())};
        }
        
        if (filesToAdd.empty()) {
            return Error{ErrorCode::InvalidArgument, "No files found to add in directory"};
        }
        
        // Process each file
        size_t successCount = 0;
        size_t failureCount = 0;
        
        for (const auto& filePath : filesToAdd) {
            spdlog::info("Adding: {}", filePath.string());
            
            // Build metadata with collection/snapshot info
            api::ContentMetadata metadata = buildMetadata(filePath.filename().string());
            
            // Store the file
            auto result = store->store(filePath, metadata);
            if (!result) {
                spdlog::error("Failed to store {}: {}", filePath.string(), result.error().message);
                failureCount++;
                continue;
            }
            
            // Store metadata in database
            auto storeMetadataResult = storeFileMetadata(filePath, result.value());
            if (!storeMetadataResult) {
                spdlog::warn("Failed to store metadata for {}: {}", 
                           filePath.string(), storeMetadataResult.error().message);
            }
            
            successCount++;
        }
        
        // Output summary
        if (cli_->getJsonOutput() || cli_->getVerbose()) {
            json output;
            output["files_processed"] = filesToAdd.size();
            output["files_added"] = successCount;
            output["files_failed"] = failureCount;
            output["collection"] = collection_;
            output["snapshot_id"] = snapshotId_;
            output["snapshot_label"] = snapshotLabel_;
            std::cout << output.dump(2) << std::endl;
        } else {
            std::cout << "Directory processing complete!" << std::endl;
            std::cout << "Files added: " << successCount << std::endl;
            std::cout << "Files failed: " << failureCount << std::endl;
            if (!collection_.empty()) std::cout << "Collection: " << collection_ << std::endl;
            if (!snapshotId_.empty()) std::cout << "Snapshot ID: " << snapshotId_ << std::endl;
            if (!snapshotLabel_.empty()) std::cout << "Snapshot Label: " << snapshotLabel_ << std::endl;
        }
        
        return Result<void>();
    }
    
    api::ContentMetadata buildMetadata(const std::string& defaultName) {
        api::ContentMetadata metadata;
        metadata.name = documentName_.empty() ? defaultName : documentName_;
        metadata.mimeType = mimeType_;
        
        // Convert vector tags to unordered_map
        for (const auto& tag : tags_) {
            metadata.tags[tag] = "";
        }
        
        // Add custom metadata
        for (const auto& kv : metadata_) {
            auto pos = kv.find('=');
            if (pos != std::string::npos) {
                std::string key = kv.substr(0, pos);
                std::string value = kv.substr(pos + 1);
                metadata.tags[key] = value;
            }
        }
        
        // Add collection and snapshot metadata
        if (!collection_.empty()) {
            metadata.tags["collection"] = collection_;
        }
        if (!snapshotId_.empty()) {
            metadata.tags["snapshot_id"] = snapshotId_;
        }
        if (!snapshotLabel_.empty()) {
            metadata.tags["snapshot_label"] = snapshotLabel_;
        }
        
        return metadata;
    }
    
    Result<void> storeFileMetadata(const std::filesystem::path& filePath, const api::StoreResult& storeResult) {
        auto metadataRepo = cli_->getMetadataRepository();
        if (!metadataRepo) {
            return Error{ErrorCode::NotInitialized, "Metadata repository not initialized"};
        }
        
        metadata::DocumentInfo docInfo;
        docInfo.filePath = filePath.string();
        docInfo.fileName = filePath.filename().string();
        docInfo.fileExtension = filePath.extension().string();
        docInfo.fileSize = std::filesystem::file_size(filePath);
        docInfo.sha256Hash = storeResult.contentHash;
        docInfo.mimeType = mimeType_.empty() ? "application/octet-stream" : mimeType_;
        
        auto now = std::chrono::system_clock::now();
        docInfo.createdTime = now;
        
        // Convert file_time_type to system_clock::time_point
        auto ftime = std::filesystem::last_write_time(filePath);
        auto sctp = std::chrono::time_point_cast<std::chrono::system_clock::duration>(
            ftime - std::filesystem::file_time_type::clock::now() + std::chrono::system_clock::now());
        docInfo.modifiedTime = sctp;
        docInfo.indexedTime = now;
        
        // Insert document
        auto insertResult = metadataRepo->insertDocument(docInfo);
        if (!insertResult) {
            return insertResult.error();
        }
        
        int64_t docId = insertResult.value();
        
        // Add tags as metadata
        for (const auto& tag : tags_) {
            metadataRepo->setMetadata(docId, "tag", metadata::MetadataValue(tag));
        }
        
        // Add custom metadata
        for (const auto& kv : metadata_) {
            auto pos = kv.find('=');
            if (pos != std::string::npos) {
                std::string key = kv.substr(0, pos);
                std::string value = kv.substr(pos + 1);
                metadataRepo->setMetadata(docId, key, metadata::MetadataValue(value));
            }
        }
        
        // Add collection and snapshot metadata
        if (!collection_.empty()) {
            metadataRepo->setMetadata(docId, "collection", metadata::MetadataValue(collection_));
        }
        if (!snapshotId_.empty()) {
            metadataRepo->setMetadata(docId, "snapshot_id", metadata::MetadataValue(snapshotId_));
        }
        if (!snapshotLabel_.empty()) {
            metadataRepo->setMetadata(docId, "snapshot_label", metadata::MetadataValue(snapshotLabel_));
        }
        
        // Add relative path metadata for directory operations
        if (std::filesystem::is_directory(targetPath_)) {
            auto relativePath = std::filesystem::relative(filePath, targetPath_);
            metadataRepo->setMetadata(docId, "path", metadata::MetadataValue(relativePath.string()));
        }
        
        // Read file content for indexing
        std::string fileContent;
        try {
            std::ifstream file(filePath, std::ios::binary);
            if (file) {
                std::stringstream buffer;
                buffer << file.rdbuf();
                fileContent = buffer.str();
                
                // Index document content for full-text search
                auto indexResult = metadataRepo->indexDocumentContent(
                    docId, 
                    docInfo.fileName,
                    fileContent,
                    docInfo.mimeType
                );
                if (!indexResult) {
                    spdlog::warn("Failed to index document content: {}", 
                               indexResult.error().message);
                }
            }
        } catch (const std::exception& e) {
            spdlog::warn("Failed to read file for indexing: {}", e.what());
        }
        
        // Update fuzzy index
        metadataRepo->updateFuzzyIndex(docId);
        
        return Result<void>();
    }
    
    bool shouldIncludeFile(const std::filesystem::path& filePath) {
        std::string fileName = filePath.filename().string();
        
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
    
    std::string generateSnapshotId() {
        auto now = std::chrono::system_clock::now();
        auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
            now.time_since_epoch()).count();
        
        // Generate random component
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(1000, 9999);
        
        std::stringstream ss;
        ss << "snapshot_" << timestamp << "_" << dis(gen);
        return ss.str();
    }
    
    void outputResult(const api::StoreResult& storeResult) {
        // Output in JSON if --json flag is set, or if --verbose is set
        if (cli_->getJsonOutput() || cli_->getVerbose()) {
            json output;
            output["hash"] = storeResult.contentHash;
            output["bytes_stored"] = storeResult.bytesStored;
            output["bytes_deduped"] = storeResult.bytesDeduped;
            output["dedup_ratio"] = storeResult.dedupRatio();
            output["duration_ms"] = storeResult.duration.count();
            
            std::cout << output.dump(2) << std::endl;
        } else {
            // Simple, concise output by default
            std::cout << "Document added successfully!" << std::endl;
            std::cout << "Hash: " << storeResult.contentHash << std::endl;
            std::cout << "Bytes stored: " << storeResult.bytesStored << std::endl;
            std::cout << "Bytes deduped: " << storeResult.bytesDeduped << std::endl;
            std::cout << "Dedup ratio: " << (storeResult.dedupRatio() * 100) << "%" << std::endl;
        }
    }
    
private:
    YamsCLI* cli_ = nullptr;
    std::filesystem::path targetPath_;
    std::string documentName_;
    std::vector<std::string> tags_;
    std::vector<std::string> metadata_;
    std::string mimeType_;
    
    // Collection and snapshot options
    std::string collection_;
    std::string snapshotId_;
    std::string snapshotLabel_;
    
    // Directory options
    bool recursive_ = false;
    std::vector<std::string> includePatterns_;
    std::vector<std::string> excludePatterns_;
};

// Factory function
std::unique_ptr<ICommand> createAddCommand() {
    return std::make_unique<AddCommand>();
}

} // namespace yams::cli