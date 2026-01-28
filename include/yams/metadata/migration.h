#pragma once

#include <chrono>
#include <functional>
#include <map>
#include <string>
#include <vector>
#include <yams/metadata/database.h>

namespace yams::metadata {

/**
 * @brief Database migration definition
 */
struct Migration {
    int version;                                   ///< Migration version number
    std::string name;                              ///< Human-readable name
    std::string upSQL;                             ///< SQL to apply migration
    std::string downSQL;                           ///< SQL to rollback migration (optional)
    std::chrono::system_clock::time_point created; ///< Creation timestamp
    bool wrapInTransaction{true};                  ///< Whether manager should wrap in a transaction

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
                                 std::chrono::milliseconds duration, bool success,
                                 const std::string& error = "");

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

    // Version 7: Add knowledge graph core schema (nodes, edges, aliases, embeddings, doc_entities,
    // stats)
    static Migration createKnowledgeGraphSchema();

    // Version 8: Add binary signature schema for pattern matching
    static Migration createBinarySignatureSchema();

    // Version 9: Add vector search schema for embeddings
    static Migration createVectorSearchSchema();

    // Version 10: Rebuild FTS5 with improved tokenization for code identifiers
    static Migration upgradeFTS5Tokenization();

    // Version 11: Add tree snapshots schema for directory versioning
    static Migration createTreeSnapshotsSchema();

    // Version 12: Add tree diffs and changes schema for diff persistence
    static Migration createTreeDiffsSchema();

    // Version 13: Path indexing columns and FTS optimizations
    static Migration addPathIndexingSchema();

    // Version 14: Chunked path indexing backfill (resumable)
    static Migration chunkedPathIndexingBackfill();

    // Version 15: Path tree nodes schema (PBI-051 scaffold)
    static Migration createPathTreeSchema();

    // Version 16: Symbol metadata materialized view (derived from KG)
    static Migration createSymbolMetadataSchema();

    // Version 17: Enable FTS5 Porter stemmer for better search
    static Migration addFTS5PorterStemmer();

    // Version 18: Remove content_type from FTS5 index (never queried via MATCH)
    static Migration removeFTS5ContentType();

    // Version 19: Rename doc_entities to kg_doc_entities (repair for existing databases)
    static Migration renameDocEntitiesToKgDocEntities();

    // Version 20: Session-isolated memory index (PBI-082)
    static Migration createSessionIndexes();

    // Version 21: Repair tracking fields to prevent duplicate work
    static Migration createRepairTrackingSchema();

    // Version 22: Versioned symbol extraction state to avoid re-extraction
    static Migration createSymbolExtractionStateSchema();

    // Version 23: Metadata aggregation indexes for key/value counts
    static Migration createMetadataAggregationIndexes();

    // Version 24: SymSpell fuzzy search tables for fast edit-distance queries
    static Migration createSymSpellSchema();

    // Version 25: Term statistics for IDF computation and query weighting
    static Migration createTermStatsSchema();

    // Version 26: Repair symbol_metadata uniqueness for upserts
    static Migration repairSymbolMetadataUniqueness();

    // Version 27: Partial index for metadata value aggregations
    static Migration optimizeValueCountsQuery();
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
    MigrationBuilder& renameColumn(const std::string& table, const std::string& from,
                                   const std::string& to);

    // Index operations
    MigrationBuilder& createIndex(const std::string& index, const std::string& table,
                                  const std::vector<std::string>& columns, bool unique = false);
    MigrationBuilder& dropIndex(const std::string& index);

    // Raw SQL
    MigrationBuilder& up(const std::string& sql);
    MigrationBuilder& down(const std::string& sql);

    // Custom functions
    MigrationBuilder& upFunction(std::function<Result<void>(Database&)> func);
    MigrationBuilder& downFunction(std::function<Result<void>(Database&)> func);
    MigrationBuilder& wrapInTransaction(bool enabled);

    // Build the migration
    [[nodiscard]] Migration build() const;

private:
    Migration migration_;
    std::vector<std::string> upStatements_;
    std::vector<std::string> downStatements_;
};

} // namespace yams::metadata
