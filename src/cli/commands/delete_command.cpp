#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>
#include <spdlog/spdlog.h>
#include <iostream>

namespace yams::cli {

class DeleteCommand : public ICommand {
public:
    std::string getName() const override { return "delete"; }
    
    std::string getDescription() const override { 
        return "Delete documents by hash or matching criteria";
    }
    
    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;
        
        auto* cmd = app.add_subcommand("delete", getDescription());
        cmd->add_option("hash", hash_, "Document hash to delete")
            ->required();
        
        cmd->add_flag("--force", force_, "Skip confirmation prompt");
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
            
            // Validate hash format (simple check for now)
            if (hash_.length() != 64 || hash_.find_first_not_of("0123456789abcdef") != std::string::npos) {
                return Error{ErrorCode::InvalidArgument, "Invalid hash format. Expected 64 character hex string."};
            }
            
            // Confirm deletion unless --force is used
            if (!force_) {
                std::cout << "Are you sure you want to delete document " << hash_ << "? (y/N): ";
                std::string response;
                std::getline(std::cin, response);
                if (response != "y" && response != "Y") {
                    std::cout << "Deletion cancelled.\n";
                    return Result<void>();
                }
            }
            
            // Check if document exists first
            auto existsResult = store->exists(hash_);
            if (!existsResult || !existsResult.value()) {
                return Error{ErrorCode::ChunkNotFound, "Document " + hash_ + " not found."};
            }
            
            // Delete the document
            if (verbose_) {
                std::cout << "Deleting document " << hash_ << "...\n";
            }
            
            auto deleteResult = store->remove(hash_);
            if (!deleteResult) {
                return Error{deleteResult.error().code, 
                           "Failed to delete document: " + deleteResult.error().message};
            }
            
            std::cout << "Successfully deleted document " << hash_ << "\n";
            
            // Show storage stats if verbose
            if (verbose_) {
                auto stats = store->getStats();
                std::cout << "\nStorage statistics:\n";
                std::cout << "  Total objects: " << stats.totalObjects << "\n";
                std::cout << "  Unique blocks: " << stats.uniqueBlocks << "\n";
                std::cout << "  Storage size: " << (stats.totalBytes / (1024.0 * 1024.0)) 
                         << " MB\n";
            }
            
            return Result<void>();
            
        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string(e.what())};
        }
    }
    
private:
    YamsCLI* cli_ = nullptr;
    std::string hash_;
    bool force_ = false;
    bool keepRefs_ = false;
    bool verbose_ = false;
};

// Factory function
std::unique_ptr<ICommand> createDeleteCommand() {
    return std::make_unique<DeleteCommand>();
}

} // namespace yams::cli