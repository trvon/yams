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

namespace yams::cli {

using json = nlohmann::json;

class AddCommand : public ICommand {
public:
    std::string getName() const override { return "add"; }
    
    std::string getDescription() const override { 
        return "Add a document to the content store (file or stdin)";
    }
    
    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;
        
        auto* cmd = app.add_subcommand("add", getDescription());
        cmd->add_option("file", filePath_, "Path to the file to add (use '-' for stdin)")
            ->default_val("-");
        
        cmd->add_option("-n,--name", documentName_, "Name for the document (especially useful for stdin)");
        cmd->add_option("-t,--tags", tags_, "Tags for the document (comma-separated)");
        cmd->add_option("-m,--metadata", metadata_, "Metadata key=value pairs");
        cmd->add_option("--mime-type", mimeType_, "MIME type of the document");
        
        cmd->callback([this]() { 
            auto result = execute();
            if (!result) {
                spdlog::error("Command failed: {}", result.error().message);
                exit(1);
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
            if (filePath_.string() == "-") {
                return storeFromStdin(store);
            }
            
            // Validate file exists
            if (!std::filesystem::exists(filePath_)) {
                return Error{ErrorCode::FileNotFound, "File not found: " + filePath_.string()};
            }
            
            // Build metadata
            api::ContentMetadata metadata;
            metadata.name = documentName_.empty() ? filePath_.filename().string() : documentName_;
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
            
            // Store the file
            auto result = store->store(filePath_, metadata);
            if (!result) {
                return Error{result.error().code, result.error().message};
            }
            
            // Store metadata in database
            auto metadataRepo = cli_->getMetadataRepository();
            if (metadataRepo) {
                metadata::DocumentInfo docInfo;
                docInfo.filePath = filePath_.string();
                docInfo.fileName = documentName_.empty() ? filePath_.filename().string() : documentName_;
                docInfo.fileExtension = filePath_.extension().string();
                docInfo.fileSize = std::filesystem::file_size(filePath_);
                docInfo.sha256Hash = result.value().contentHash;
                docInfo.mimeType = mimeType_.empty() ? "application/octet-stream" : mimeType_;
                
                auto now = std::chrono::system_clock::now();
                docInfo.createdTime = now;
                // Convert file_time_type to system_clock::time_point
                auto ftime = std::filesystem::last_write_time(filePath_);
                auto sctp = std::chrono::time_point_cast<std::chrono::system_clock::duration>(
                    ftime - std::filesystem::file_time_type::clock::now() + std::chrono::system_clock::now());
                docInfo.modifiedTime = sctp;
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
                    
                    // Read file content for indexing
                    std::string fileContent;
                    try {
                        std::ifstream file(filePath_, std::ios::binary);
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
                    
                    spdlog::debug("Stored metadata for document ID: {}", docId);
                } else {
                    spdlog::warn("Failed to store metadata: {}", insertResult.error().message);
                }
            }
            
            outputResult(result.value());
            return Result<void>();
            
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
    
    void outputResult(const api::StoreResult& storeResult) {
        // Output result
        if (cli_->getDataPath().empty()) { // Check for JSON output flag
            json output;
            output["hash"] = storeResult.contentHash;
            output["bytes_stored"] = storeResult.bytesStored;
            output["bytes_deduped"] = storeResult.bytesDeduped;
            output["dedup_ratio"] = storeResult.dedupRatio();
            output["duration_ms"] = storeResult.duration.count();
            
            std::cout << output.dump(2) << std::endl;
        } else {
            std::cout << "Document added successfully!" << std::endl;
            std::cout << "Hash: " << storeResult.contentHash << std::endl;
            std::cout << "Bytes stored: " << storeResult.bytesStored << std::endl;
            std::cout << "Bytes deduped: " << storeResult.bytesDeduped << std::endl;
            std::cout << "Dedup ratio: " << (storeResult.dedupRatio() * 100) << "%" << std::endl;
        }
    }
    
private:
    YamsCLI* cli_ = nullptr;
    std::filesystem::path filePath_;
    std::string documentName_;
    std::vector<std::string> tags_;
    std::vector<std::string> metadata_;
    std::string mimeType_;
};

// Factory function
std::unique_ptr<ICommand> createAddCommand() {
    return std::make_unique<AddCommand>();
}

} // namespace yams::cli