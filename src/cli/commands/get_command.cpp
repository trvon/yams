#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>
#include <iostream>
#include <fstream>

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
        cmd->add_option("hash", hash_, "Hash of the document to retrieve")
            ->required();
        
        cmd->add_option("-o,--output", outputPath_, "Output file path (default: stdout)");
        
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
            
            // Check if document exists
            auto existsResult = store->exists(hash_);
            if (!existsResult) {
                return Error{existsResult.error().code, existsResult.error().message};
            }
            
            if (!existsResult.value()) {
                return Error{ErrorCode::NotFound, "Document not found: " + hash_};
            }
            
            // Check if outputting to stdout or file
            if (outputPath_.empty() || outputPath_ == "-") {
                // Output to stdout using stream interface
                auto result = store->retrieveStream(hash_, std::cout);
                if (!result) {
                    return Error{result.error().code, result.error().message};
                }
                // Silent output to stdout - just the content
            } else {
                // Retrieve to file
                auto result = store->retrieve(hash_, outputPath_);
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
    YamsCLI* cli_ = nullptr;
    std::string hash_;
    std::filesystem::path outputPath_;
};

// Factory function
std::unique_ptr<ICommand> createGetCommand() {
    return std::make_unique<GetCommand>();
}

} // namespace yams::cli