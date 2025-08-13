#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>
#include <yams/cli/time_parser.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/detection/file_type_detector.h>
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>
#include <iostream>
#include <fstream>
#include <filesystem>
#include <algorithm>

namespace yams::cli {

using json = nlohmann::json;

class GetCommand : public ICommand {
public:
    std::string getName() const override { return "get"; }
    
    std::string getDescription() const override { 
        return "Retrieve a document from the content store";
    }
    
    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;
        
        auto* cmd = app.add_subcommand("get", getDescription());
        
        // Create option group for retrieval methods (only one can be used at a time)
        auto* group = cmd->add_option_group("retrieval_method");
        group->add_option("hash", hash_, "Hash of the document to retrieve");
        group->add_option("--name", name_, "Name of the document to retrieve");
        
        // File type filters
        group->add_option("--type", fileType_, "Filter by file type (image, document, archive, audio, video, text, executable, binary)");
        group->add_option("--mime", mimeType_, "Filter by MIME type (e.g., image/jpeg, application/pdf)");
        group->add_option("--extension", extension_, "Filter by file extension (e.g., .jpg, .pdf)");
        group->add_flag("--binary", binaryOnly_, "Get only binary files");
        group->add_flag("--text", textOnly_, "Get only text files");
        
        // Time filters
        group->add_option("--created-after", createdAfter_, "Get files created after this time");
        group->add_option("--created-before", createdBefore_, "Get files created before this time");
        group->add_option("--modified-after", modifiedAfter_, "Get files modified after this time");
        group->add_option("--modified-before", modifiedBefore_, "Get files modified before this time");
        group->add_option("--indexed-after", indexedAfter_, "Get files indexed after this time");
        group->add_option("--indexed-before", indexedBefore_, "Get files indexed before this time");
        
        // Require at least one filter/selector
        group->require_option(1);
        
        cmd->add_option("-o,--output", outputPath_, "Output file path (default: stdout)");
        cmd->add_flag("-v,--verbose", verbose_, "Enable verbose output");
        cmd->add_flag("--latest", getLatest_, "Get the most recently indexed matching document");
        cmd->add_flag("--oldest", getOldest_, "Get the oldest indexed matching document");
        
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
            
            // Resolve the hash to retrieve
            std::string hashToRetrieve;
            
            if (!hash_.empty()) {
                // Direct hash retrieval
                hashToRetrieve = hash_;
            } else if (hasFilters()) {
                // Filter-based retrieval
                auto resolveResult = resolveByFilters();
                if (!resolveResult) {
                    return resolveResult.error();
                }
                hashToRetrieve = resolveResult.value();
                
                if (verbose_) {
                    std::cerr << "Found document matching filters: " 
                             << hashToRetrieve.substr(0, 12) << "..." << std::endl;
                }
            } else if (!name_.empty()) {
                // Name-based retrieval
                auto resolveResult = resolveNameToHash(name_);
                if (!resolveResult) {
                    // If document not found in YAMS, check if it's a local file
                    if (resolveResult.error().code == ErrorCode::NotFound && 
                        std::filesystem::exists(name_)) {
                        // Fall back to local file operations
                        if (outputPath_.empty() || outputPath_ == "-") {
                            // Output to stdout
                            std::ifstream file(name_, std::ios::binary);
                            if (!file) {
                                return Error{ErrorCode::FileNotFound, "Cannot read local file: " + name_};
                            }
                            std::cout << file.rdbuf();
                        } else {
                            // Copy local file to output path
                            try {
                                std::filesystem::copy_file(name_, outputPath_, 
                                    std::filesystem::copy_options::overwrite_existing);
                                
                                auto fileSize = std::filesystem::file_size(name_);
                                
                                // Output success message to stderr
                                std::cerr << "Document retrieved successfully!" << std::endl;
                                std::cerr << "Output: " << outputPath_ << std::endl;
                                std::cerr << "Size: " << fileSize << " bytes" << std::endl;
                            } catch (const std::filesystem::filesystem_error& e) {
                                return Error{ErrorCode::WriteError, 
                                           "Failed to copy local file: " + std::string(e.what())};
                            }
                        }
                        return Result<void>();
                    }
                    // Not a local file either, return original error
                    return resolveResult.error();
                }
                hashToRetrieve = resolveResult.value();
                
                if (verbose_) {
                    std::cerr << "Resolved '" << name_ << "' to hash: " 
                             << hashToRetrieve.substr(0, 12) << "..." << std::endl;
                }
            } else {
                return Error{ErrorCode::InvalidArgument, "No retrieval criteria specified"};
            }
            
            // Check if document exists
            auto existsResult = store->exists(hashToRetrieve);
            if (!existsResult) {
                return Error{existsResult.error().code, existsResult.error().message};
            }
            
            if (!existsResult.value()) {
                return Error{ErrorCode::NotFound, "Document not found: " + hashToRetrieve};
            }
            
            // Check if outputting to stdout or file
            if (outputPath_.empty() || outputPath_ == "-") {
                // Output to stdout using stream interface
                auto result = store->retrieveStream(hashToRetrieve, std::cout);
                if (!result) {
                    return Error{result.error().code, result.error().message};
                }
                // Silent output to stdout - just the content
            } else {
                // Retrieve to file
                auto result = store->retrieve(hashToRetrieve, outputPath_);
                if (!result) {
                    return Error{result.error().code, result.error().message};
                }
                
                auto& retrieveResult = result.value();
                
                // Output success message to stderr so it doesn't interfere with piped output
                std::cerr << "Document retrieved successfully!" << std::endl;
                std::cerr << "Output: " << outputPath_ << std::endl;
                std::cerr << "Size: " << retrieveResult.size << " bytes" << std::endl;
            }
            
            return Result<void>();
            
        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string("Unexpected error: ") + e.what()};
        }
    }
    
private:
    bool hasFilters() const {
        return !fileType_.empty() || !mimeType_.empty() || !extension_.empty() ||
               binaryOnly_ || textOnly_ ||
               !createdAfter_.empty() || !createdBefore_.empty() ||
               !modifiedAfter_.empty() || !modifiedBefore_.empty() ||
               !indexedAfter_.empty() || !indexedBefore_.empty();
    }
    
    Result<std::string> resolveByFilters() {
        auto metadataRepo = cli_->getMetadataRepository();
        if (!metadataRepo) {
            return Error{ErrorCode::NotInitialized, "Metadata repository not initialized"};
        }
        
        // Get all documents
        auto documentsResult = metadataRepo->findDocumentsByPath("%");
        if (!documentsResult) {
            return Error{ErrorCode::DatabaseError, "Failed to query documents: " + documentsResult.error().message};
        }
        
        auto documents = documentsResult.value();
        
        // Initialize file type detector if needed
        bool needFileTypeDetection = !fileType_.empty() || !mimeType_.empty() || !extension_.empty() || binaryOnly_ || textOnly_;
        if (needFileTypeDetection) {
            detection::FileTypeDetectorConfig config;
            config.patternsFile = YamsCLI::findMagicNumbersFile();
            config.useCustomPatterns = !config.patternsFile.empty();
            detection::FileTypeDetector::instance().initialize(config);
        }
        
        // Apply filters
        std::vector<metadata::DocumentInfo> filtered;
        for (const auto& doc : documents) {
            if (applyTimeFilters(doc) && applyFileTypeFilters(doc)) {
                filtered.push_back(doc);
            }
        }
        
        if (filtered.empty()) {
            return Error{ErrorCode::NotFound, "No documents match the specified filters"};
        }
        
        // Sort by indexed time if needed
        if (getLatest_ || getOldest_) {
            std::sort(filtered.begin(), filtered.end(),
                [this](const metadata::DocumentInfo& a, const metadata::DocumentInfo& b) {
                    return getLatest_ ? (a.indexedTime > b.indexedTime) : (a.indexedTime < b.indexedTime);
                });
        }
        
        if (filtered.size() > 1 && !getLatest_ && !getOldest_) {
            std::cerr << "Multiple documents match the filters (" << filtered.size() << " found):" << std::endl;
            for (size_t i = 0; i < std::min(size_t(5), filtered.size()); ++i) {
                const auto& doc = filtered[i];
                std::cerr << "  " << doc.sha256Hash.substr(0, 12) << "... - " 
                         << doc.fileName << " (" << doc.fileSize << " bytes)" << std::endl;
            }
            if (filtered.size() > 5) {
                std::cerr << "  ... and " << (filtered.size() - 5) << " more" << std::endl;
            }
            std::cerr << "Use --latest or --oldest to select one, or refine your filters." << std::endl;
            return Error{ErrorCode::InvalidOperation, "Multiple documents match. Please refine filters or use --latest/--oldest."};
        }
        
        return filtered[0].sha256Hash;
    }
    
    bool applyTimeFilters(const metadata::DocumentInfo& doc) {
        // Parse and apply created time filters
        if (!createdAfter_.empty()) {
            auto afterTime = TimeParser::parse(createdAfter_);
            if (!afterTime) {
                spdlog::warn("Invalid created-after time: {}", createdAfter_);
                return true; // Don't filter on invalid input
            }
            if (doc.createdTime < afterTime.value()) {
                return false;
            }
        }
        
        if (!createdBefore_.empty()) {
            auto beforeTime = TimeParser::parse(createdBefore_);
            if (!beforeTime) {
                spdlog::warn("Invalid created-before time: {}", createdBefore_);
                return true;
            }
            if (doc.createdTime > beforeTime.value()) {
                return false;
            }
        }
        
        // Parse and apply modified time filters
        if (!modifiedAfter_.empty()) {
            auto afterTime = TimeParser::parse(modifiedAfter_);
            if (!afterTime) {
                spdlog::warn("Invalid modified-after time: {}", modifiedAfter_);
                return true;
            }
            if (doc.modifiedTime < afterTime.value()) {
                return false;
            }
        }
        
        if (!modifiedBefore_.empty()) {
            auto beforeTime = TimeParser::parse(modifiedBefore_);
            if (!beforeTime) {
                spdlog::warn("Invalid modified-before time: {}", modifiedBefore_);
                return true;
            }
            if (doc.modifiedTime > beforeTime.value()) {
                return false;
            }
        }
        
        // Parse and apply indexed time filters
        if (!indexedAfter_.empty()) {
            auto afterTime = TimeParser::parse(indexedAfter_);
            if (!afterTime) {
                spdlog::warn("Invalid indexed-after time: {}", indexedAfter_);
                return true;
            }
            if (doc.indexedTime < afterTime.value()) {
                return false;
            }
        }
        
        if (!indexedBefore_.empty()) {
            auto beforeTime = TimeParser::parse(indexedBefore_);
            if (!beforeTime) {
                spdlog::warn("Invalid indexed-before time: {}", indexedBefore_);
                return true;
            }
            if (doc.indexedTime > beforeTime.value()) {
                return false;
            }
        }
        
        return true;
    }
    
    bool applyFileTypeFilters(const metadata::DocumentInfo& doc) {
        // Extension filter
        if (!extension_.empty()) {
            std::string ext = doc.fileExtension;
            if (ext.empty() && !doc.fileName.empty()) {
                auto pos = doc.fileName.rfind('.');
                if (pos != std::string::npos) {
                    ext = doc.fileName.substr(pos);
                }
            }
            
            // Add dot if not present
            std::string targetExt = extension_;
            if (!targetExt.empty() && targetExt[0] != '.') {
                targetExt = "." + targetExt;
            }
            
            if (ext != targetExt) {
                return false;
            }
        }
        
        // MIME type filter
        if (!mimeType_.empty()) {
            if (doc.mimeType != mimeType_) {
                // Also check if it's a wildcard match (e.g., "image/*")
                if (mimeType_.back() == '*' && mimeType_.size() > 1) {
                    std::string prefix = mimeType_.substr(0, mimeType_.size() - 1);
                    if (doc.mimeType.find(prefix) != 0) {
                        return false;
                    }
                } else {
                    return false;
                }
            }
        }
        
        // File type category filter or binary/text filter
        if (!fileType_.empty() || binaryOnly_ || textOnly_) {
            // Detect file type if not already in metadata
            detection::FileSignature sig;
            
            if (!doc.mimeType.empty()) {
                sig.mimeType = doc.mimeType;
                sig.fileType = detection::FileTypeDetector::instance().getFileTypeCategory(doc.mimeType);
                sig.isBinary = detection::FileTypeDetector::instance().isBinaryMimeType(doc.mimeType);
            } else {
                // Try to detect from file path if available
                std::filesystem::path filePath = doc.filePath;
                if (std::filesystem::exists(filePath)) {
                    auto detectResult = detection::FileTypeDetector::instance().detectFromFile(filePath);
                    if (detectResult) {
                        sig = detectResult.value();
                    } else {
                        // Fall back to extension-based detection
                        std::string ext = filePath.extension().string();
                        sig.mimeType = detection::FileTypeDetector::getMimeTypeFromExtension(ext);
                        sig.fileType = detection::FileTypeDetector::instance().getFileTypeCategory(sig.mimeType);
                        sig.isBinary = detection::FileTypeDetector::instance().isBinaryMimeType(sig.mimeType);
                    }
                } else {
                    // Use extension only
                    std::string ext = doc.fileExtension;
                    if (ext.empty() && !doc.fileName.empty()) {
                        auto pos = doc.fileName.rfind('.');
                        if (pos != std::string::npos) {
                            ext = doc.fileName.substr(pos);
                        }
                    }
                    sig.mimeType = detection::FileTypeDetector::getMimeTypeFromExtension(ext);
                    sig.fileType = detection::FileTypeDetector::instance().getFileTypeCategory(sig.mimeType);
                    sig.isBinary = detection::FileTypeDetector::instance().isBinaryMimeType(sig.mimeType);
                }
            }
            
            // Apply file type category filter
            if (!fileType_.empty()) {
                if (sig.fileType != fileType_) {
                    return false;
                }
            }
            
            // Apply binary/text filter
            if (binaryOnly_ && !sig.isBinary) {
                return false;
            }
            if (textOnly_ && sig.isBinary) {
                return false;
            }
        }
        
        return true;
    }
    
    Result<std::string> resolveNameToHash(const std::string& name) {
        auto metadataRepo = cli_->getMetadataRepository();
        if (!metadataRepo) {
            return Error{ErrorCode::NotInitialized, "Metadata repository not initialized"};
        }
        
        // First try as a path suffix (for real files)
        auto documentsResult = metadataRepo->findDocumentsByPath("%/" + name);
        if (documentsResult && !documentsResult.value().empty()) {
            const auto& results = documentsResult.value();
            if (results.size() > 1) {
                std::cerr << "Multiple documents found with name '" << name << "':" << std::endl;
                for (const auto& doc : results) {
                    std::cerr << "  " << doc.sha256Hash.substr(0, 12) << "... - " 
                             << doc.filePath << std::endl;
                }
                return Error{ErrorCode::InvalidOperation, 
                            "Multiple documents with the same name. Please use hash to specify which one."};
            }
            return results[0].sha256Hash;
        }
        
        // Try exact path match
        documentsResult = metadataRepo->findDocumentsByPath(name);
        if (documentsResult && !documentsResult.value().empty()) {
            return documentsResult.value()[0].sha256Hash;
        }
        
        // For stdin documents or when path search fails, use search
        auto searchResult = metadataRepo->search(name, 100, 0);
        if (searchResult) {
            std::vector<std::string> matchingHashes;
            std::vector<std::string> matchingPaths;
            
            for (const auto& result : searchResult.value().results) {
                // SearchResult contains document directly
                const auto& doc = result.document;
                // Check if fileName matches exactly
                if (doc.fileName == name) {
                    matchingHashes.push_back(doc.sha256Hash);
                    matchingPaths.push_back(doc.filePath);
                }
            }
            
            if (!matchingHashes.empty()) {
                if (matchingHashes.size() > 1) {
                    std::cerr << "Multiple documents found with name '" << name << "':" << std::endl;
                    for (size_t i = 0; i < matchingHashes.size(); ++i) {
                        std::cerr << "  " << matchingHashes[i].substr(0, 12) << "... - " 
                                 << matchingPaths[i] << std::endl;
                    }
                    return Error{ErrorCode::InvalidOperation, 
                                "Multiple documents with the same name. Please use hash to specify which one."};
                }
                return matchingHashes[0];
            }
        }
        
        return Error{ErrorCode::NotFound, "No document found with name: " + name};
    }
    
    YamsCLI* cli_ = nullptr;
    std::string hash_;
    std::string name_;
    std::filesystem::path outputPath_;
    bool verbose_ = false;
    bool getLatest_ = false;
    bool getOldest_ = false;
    
    // File type filters
    std::string fileType_;
    std::string mimeType_;
    std::string extension_;
    bool binaryOnly_ = false;
    bool textOnly_ = false;
    
    // Time filters
    std::string createdAfter_;
    std::string createdBefore_;
    std::string modifiedAfter_;
    std::string modifiedBefore_;
    std::string indexedAfter_;
    std::string indexedBefore_;
};

// Factory function
std::unique_ptr<ICommand> createGetCommand() {
    return std::make_unique<GetCommand>();
}

} // namespace yams::cli