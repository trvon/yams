#pragma once

#include <memory>
#include <mutex>
#include <vector>
#include <CLI/CLI.hpp>
#include <yams/api/content_store.h>
#include <yams/app/services/services.hpp>
#include <yams/cli/command.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/search/search_executor.h>
#include <yams/vector/vector_index_manager.h>

namespace yams::vector {
class EmbeddingGenerator;
}

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
    std::shared_ptr<metadata::MetadataRepository> getMetadataRepository() const {
        return metadataRepo_;
    }

    /**
     * Get the search executor instance
     */
    std::shared_ptr<search::SearchExecutor> getSearchExecutor() const { return searchExecutor_; }
    std::shared_ptr<metadata::KnowledgeGraphStore> getKnowledgeGraphStore() const {
        return kgStore_;
    }

    /**
     * Get the vector index manager instance
     */
    std::shared_ptr<vector::VectorIndexManager> getVectorIndexManager() const {
        return vectorIndexManager_;
    }

    /**
     * Get the embedding generator instance
     * Performs lazy initialization on first access to avoid unnecessary daemon connections
     */
    std::shared_ptr<vector::EmbeddingGenerator> getEmbeddingGenerator();

    /**
     * Check if embedding generator already exists (without creating it)
     */
    bool hasEmbeddingGenerator() const { return embeddingGenerator_ != nullptr; }

    /**
     * Get the connection pool instance
     */
    std::shared_ptr<metadata::ConnectionPool> getConnectionPool() const { return connectionPool_; }

    /**
     * Get the database instance
     */
    std::shared_ptr<metadata::Database> getDatabase() const { return database_; }

    /**
     * Get the application context for services
     * Lazily initializes the context on first access
     */
    std::shared_ptr<app::services::AppContext> getAppContext();

    /**
     * Get the data directory path
     */
    std::filesystem::path getDataPath() const { return dataPath_; }

    /**
     * Set the data directory path
     */
    void setDataPath(const std::filesystem::path& p) { dataPath_ = p; }

    /**
     * Get verbose flag
     */
    bool getVerbose() const { return verbose_; }

    /**
     * Get JSON output flag
     */
    bool getJsonOutput() const { return jsonOutput_; }

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

    /**
     * Defer execution of a command until after parsing; runner will execute once via async spawn.
     */
    void setPendingCommand(ICommand* cmd);

    /**
     * Find the magic_numbers.json file in standard locations
     */
    static std::filesystem::path findMagicNumbersFile();

private:
    // Track if config provided data dir and the CLI option to enforce precedence
    CLI::Option* storageOpt_{nullptr};
    bool configProvidesDataDir_{false};
    /**
     * Compression configuration loaded from config file
     */
    struct CompressionConfig {
        bool enable = true;
        std::string algorithm = "zstd";
        int level = 3;
    };
    /**
     * Initialize the storage system
     */
    Result<void> initializeStorage();

    /**
     * Register all built-in commands
     */
    void registerBuiltinCommands();

    /**
     * Load compression configuration from config file
     */
    CompressionConfig loadCompressionConfig() const;

    /**
     * Get the config file path
     */
    std::filesystem::path getConfigPath() const;

    /**
     * Parse a simple TOML config file
     */
    std::map<std::string, std::string> parseSimpleToml(const std::filesystem::path& path) const;

    /**
     * Check if config migration is needed and prompt user
     */
    void checkConfigMigration();

private:
    std::unique_ptr<CLI::App> app_;
    std::vector<std::unique_ptr<ICommand>> commands_;

    // Core components
    std::shared_ptr<api::IContentStore> contentStore_;
    std::shared_ptr<search::SearchExecutor> searchExecutor_;
    std::shared_ptr<metadata::ConnectionPool> connectionPool_;
    std::shared_ptr<metadata::Database> database_;
    std::shared_ptr<metadata::MetadataRepository> metadataRepo_;
    std::shared_ptr<metadata::KnowledgeGraphStore> kgStore_;
    std::shared_ptr<vector::VectorIndexManager> vectorIndexManager_;
    std::shared_ptr<vector::EmbeddingGenerator> embeddingGenerator_; // Unified embedding generator

    // App services context
    std::shared_ptr<app::services::AppContext> appContext_;
    mutable std::mutex appContextMutex_;

    // Configuration
    std::filesystem::path dataPath_;
    bool verbose_ = false;
    bool jsonOutput_ = false;

    // Deferred execution: a single command to run post-parse
    ICommand* pendingCommand_ = nullptr;
};

} // namespace yams::cli
