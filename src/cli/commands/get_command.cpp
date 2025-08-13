#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>
#include <iostream>
#include <fstream>
#include <filesystem>

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
        group->require_option(1);
        
        cmd->add_option("-o,--output", outputPath_, "Output file path (default: stdout)");
        cmd->add_flag("-v,--verbose", verbose_, "Enable verbose output");
        
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
};

// Factory function
std::unique_ptr<ICommand> createGetCommand() {
    return std::make_unique<GetCommand>();
}

} // namespace yams::cli