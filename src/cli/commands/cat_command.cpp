#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>
#include <spdlog/spdlog.h>
#include <iostream>
#include <fstream>
#include <filesystem>

namespace yams::cli {

class CatCommand : public ICommand {
public:
    std::string getName() const override { return "cat"; }
    
    std::string getDescription() const override { 
        return "Display document content to stdout";
    }
    
    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;
        
        auto* cmd = app.add_subcommand("cat", getDescription());
        
        // Create option group for retrieval methods (only one can be used at a time)
        auto* group = cmd->add_option_group("retrieval_method");
        group->add_option("hash", hash_, "Hash of the document to display");
        group->add_option("--name", name_, "Name of the document to display");
        group->require_option(1);
        
        // No output option - cat always goes to stdout
        // This is intentional for piping and viewing content directly
        
        cmd->callback([this]() { 
            auto result = execute();
            if (!result) {
                spdlog::error("Cat failed: {}", result.error().message);
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
            
            // Resolve the hash to display
            std::string hashToDisplay;
            
            if (!hash_.empty()) {
                // Direct hash display
                hashToDisplay = hash_;
            } else if (!name_.empty()) {
                // Name-based display
                auto resolveResult = resolveNameToHash(name_);
                if (!resolveResult) {
                    // If document not found in YAMS, check if it's a local file
                    if (resolveResult.error().code == ErrorCode::NotFound && 
                        std::filesystem::exists(name_)) {
                        // Fall back to reading local file
                        std::ifstream file(name_, std::ios::binary);
                        if (!file) {
                            return Error{ErrorCode::FileNotFound, "Cannot read local file: " + name_};
                        }
                        
                        // Output file contents directly to stdout
                        std::cout << file.rdbuf();
                        
                        // Successfully displayed local file
                        return Result<void>();
                    }
                    // Not a local file either, return original error
                    return resolveResult.error();
                }
                hashToDisplay = resolveResult.value();
            } else {
                return Error{ErrorCode::InvalidArgument, "No document specified"};
            }
            
            // Check if document exists
            auto existsResult = store->exists(hashToDisplay);
            if (!existsResult) {
                return Error{existsResult.error().code, existsResult.error().message};
            }
            
            if (!existsResult.value()) {
                return Error{ErrorCode::NotFound, "Document not found: " + hashToDisplay};
            }
            
            // Output to stdout using stream interface - always silent (no metadata)
            auto result = store->retrieveStream(hashToDisplay, std::cout);
            if (!result) {
                return Error{result.error().code, result.error().message};
            }
            
            // Cat command should not output any status messages
            // This allows clean piping: yams cat --name file.txt | grep something
            
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
};

// Factory function
std::unique_ptr<ICommand> createCatCommand() {
    return std::make_unique<CatCommand>();
}

} // namespace yams::cli