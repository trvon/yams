#pragma once

#include <yams/metadata/database.h>
#include <string>
#include <vector>
#include <functional>
#include <chrono>
#include <map>

namespace yams::metadata {

/**
 * @brief Database migration definition
 */
struct Migration {
    int version;                    ///< Migration version number
    std::string name;              ///< Human-readable name
    std::string upSQL;             ///< SQL to apply migration
    std::string downSQL;           ///< SQL to rollback migration (optional)
    std::chrono::system_clock::time_point created; ///< Creation timestamp
    
    /**
     * @brief Custom migration function (for complex migrations)
     */
    std::function<Result<void>(Database&)> upFunc;
    std::function<Result<void>(Database&)> downFunc;
};

/**
 * @brief Migration history entry
 */
struct MigrationHistory {
    int version;
    std::string name;
    std::chrono::system_clock::time_point appliedAt;
    std::chrono::milliseconds duration;
    bool success;
    std::string error;
};

/**
 * @brief Database migration manager
 */
class MigrationManager {
public:
    explicit MigrationManager(Database& db);
    
    /**
     * @brief Initialize migration system (create tables)
     */
    Result<void> initialize();
    
    /**
     * @brief Register a migration
     */
    void registerMigration(Migration migration);
    
    /**
     * @brief Register multiple migrations
     */
    void registerMigrations(std::vector<Migration> migrations);
    
    /**
     * @brief Get current schema version
     */
    Result<int> getCurrentVersion();
    
    /**
     * @brief Get latest available version
     */
    int getLatestVersion() const;
    
    /**
     * @brief Check if migrations are needed
     */
    Result<bool> needsMigration();
    
    /**
     * @brief Apply all pending migrations
     */
    Result<void> migrate();
    
    /**
     * @brief Migrate to specific version
     */
    Result<void> migrateTo(int targetVersion);
    
    /**
     * @brief Rollback to specific version
     */
    Result<void> rollbackTo(int targetVersion);
    
    /**
     * @brief Get migration history
     */
    Result<std::vector<MigrationHistory>> getHistory();
    
    /**
     * @brief Verify database integrity after migrations
     */
    Result<void> verifyIntegrity();
    
    /**
     * @brief Set callback for migration progress
     */
    using ProgressCallback = std::function<void(int current, int total, const std::string& name)>;
    void setProgressCallback(ProgressCallback callback);
    
private:
    Database& db_;
    std::map<int, Migration> migrations_;
    ProgressCallback progressCallback_;
    
    /**
     * @brief Apply a single migration
     */
    Result<void> applyMigration(const Migration& migration);
    
    /**
     * @brief Rollback a single migration
     */
    Result<void> rollbackMigration(const Migration& migration);
    
    /**
     * @brief Record migration in history
     */
    Result<void> recordMigration(int version, const std::string& name,
                                std::chrono::milliseconds duration,
                                bool success, const std::string& error = "");
    
    /**
     * @brief Create migration tables if they don't exist
     */
    Result<void> createMigrationTables();
};

/**
 * @brief Built-in migrations for YAMS metadata schema
 */
class YamsMetadataMigrations {
public:
    /**
     * @brief Get all built-in migrations
     */
    static std::vector<Migration> getAllMigrations();
    
private:
    // Version 1: Initial schema
    static Migration createInitialSchema();
    
    // Version 2: Add FTS5 tables
    static Migration createFTS5Tables();
    
    // Version 3: Add metadata indexes
    static Migration createMetadataIndexes();
    
    // Version 4: Add document relationships
    static Migration createRelationshipTables();
    
    // Version 5: Add search history and saved queries
    static Migration createSearchTables();
    
    // Version 6: Add collection and snapshot indexes
    static Migration createCollectionIndexes();
    
    // Version 7: Add knowledge graph core schema (nodes, edges, aliases, embeddings, doc_entities, stats)
    static Migration createKnowledgeGraphSchema();
    
    // Version 8: Add binary signature schema for pattern matching
    static Migration createBinarySignatureSchema();
    
    // Version 9: Add vector search schema for embeddings
    static Migration createVectorSearchSchema();
};

/**
 * @brief Migration builder for creating migrations programmatically
 */
class MigrationBuilder {
public:
    MigrationBuilder(int version, const std::string& name);
    
    // Table operations
    MigrationBuilder& createTable(const std::string& table);
    MigrationBuilder& dropTable(const std::string& table);
    MigrationBuilder& renameTable(const std::string& from, const std::string& to);
    
    // Column operations
    MigrationBuilder& addColumn(const std::string& table, const std::string& column, 
                               const std::string& type, bool nullable = true);
    MigrationBuilder& dropColumn(const std::string& table, const std::string& column);
    MigrationBuilder& renameColumn(const std::string& table, 
                                 const std::string& from, const std::string& to);
    
    // Index operations
    MigrationBuilder& createIndex(const std::string& index, const std::string& table,
                                const std::vector<std::string>& columns,
                                bool unique = false);
    MigrationBuilder& dropIndex(const std::string& index);
    
    // Raw SQL
    MigrationBuilder& up(const std::string& sql);
    MigrationBuilder& down(const std::string& sql);
    
    // Custom functions
    MigrationBuilder& upFunction(std::function<Result<void>(Database&)> func);
    MigrationBuilder& downFunction(std::function<Result<void>(Database&)> func);
    
    // Build the migration
    [[nodiscard]] Migration build() const;
    
private:
    Migration migration_;
    std::vector<std::string> upStatements_;
    std::vector<std::string> downStatements_;
};

} // namespace yams::metadata