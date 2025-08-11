#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>
#include <spdlog/spdlog.h>
#include <iostream>

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
            
            // Resolve the hash to display
            std::string hashToDisplay;
            
            if (!hash_.empty()) {
                // Direct hash display
                hashToDisplay = hash_;
            } else if (!name_.empty()) {
                // Name-based display
                auto resolveResult = resolveNameToHash(name_);
                if (!resolveResult) {
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
        
        // Search for documents with matching fileName
        auto documentsResult = metadataRepo->findDocumentsByPath("%/" + name);
        if (!documentsResult) {
            return Error{documentsResult.error().code, 
                        "Failed to search for document: " + documentsResult.error().message};
        }
        
        const auto& results = documentsResult.value();
        
        if (results.empty()) {
            return Error{ErrorCode::NotFound, "No document found with name: " + name};
        }
        
        if (results.size() > 1) {
            // Multiple documents with the same name - output to stderr to not interfere with stdout
            std::cerr << "Multiple documents found with name '" << name << "':" << std::endl;
            for (const auto& doc : results) {
                std::cerr << "  " << doc.sha256Hash.substr(0, 12) << "... - " 
                         << doc.fileName << std::endl;
            }
            return Error{ErrorCode::InvalidOperation, 
                        "Multiple documents with the same name. Please use hash to specify which one."};
        }
        
        return results[0].sha256Hash;
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