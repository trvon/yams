#pragma once

#include <yams/cli/command.h>
#include <yams/api/content_store.h>
#include <yams/search/search_executor.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/metadata_repository.h>
#include <CLI/CLI.hpp>
#include <memory>
#include <vector>

namespace yams::cli {

/**
 * Main CLI application class
 */
class YamsCLI {
public:
    YamsCLI();
    ~YamsCLI();
    
    /**
     * Run the CLI with given arguments
     */
    int run(int argc, char* argv[]);
    
    /**
     * Get the content store instance
     */
    std::shared_ptr<api::IContentStore> getContentStore() const { return contentStore_; }
    
    /**
     * Get the metadata repository instance
     */
    std::shared_ptr<metadata::MetadataRepository> getMetadataRepository() const { return metadataRepo_; }
    
    /**
     * Get the search executor instance
     */
    std::shared_ptr<search::SearchExecutor> getSearchExecutor() const { return searchExecutor_; }
    
    /**
     * Get the connection pool instance
     */
    std::shared_ptr<metadata::ConnectionPool> getConnectionPool() const { return connectionPool_; }
    
    /**
     * Get the database instance  
     */
    std::shared_ptr<metadata::Database> getDatabase() const { return database_; }
    
    /**
     * Get the data directory path
     */
    std::filesystem::path getDataPath() const { return dataPath_; }
    
    /**
     * Set the data directory path
     */
    void setDataPath(const std::filesystem::path& p) { dataPath_ = p; }
    
    /**
     * Ensure storage is initialized (lazy)
     */
    Result<void> ensureStorageInitialized();
    
    /**
     * Whether storage is initialized
     */
    bool isStorageInitialized() const { return static_cast<bool>(contentStore_); }
    
    /**
     * Register a command
     */
    void registerCommand(std::unique_ptr<ICommand> command);
    
private:
    /**
     * Initialize the storage system
     */
    Result<void> initializeStorage();
    
    /**
     * Register all built-in commands
     */
    void registerBuiltinCommands();
    
private:
    std::unique_ptr<CLI::App> app_;
    std::vector<std::unique_ptr<ICommand>> commands_;
    
    // Core components
    std::shared_ptr<api::IContentStore> contentStore_;
    std::shared_ptr<search::SearchExecutor> searchExecutor_;
    std::shared_ptr<metadata::ConnectionPool> connectionPool_;
    std::shared_ptr<metadata::Database> database_;
    std::shared_ptr<metadata::MetadataRepository> metadataRepo_;
    
    // Configuration
    std::filesystem::path dataPath_;
    bool verbose_ = false;
    bool jsonOutput_ = false;
};

} // namespace yams::cli