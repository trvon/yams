#pragma once

#include <cstddef>
#include <string>

// Forward declare Result from types.h to avoid header dependency issues
namespace yams {
template <typename T> class Result;
} // namespace yams

struct sqlite3;

namespace yams::vector {

/**
 * @brief Schema migration utilities for vector database
 *
 * Handles migration from V1 schema (doc_embeddings vec0 + doc_metadata)
 * to the unified vectors-table schema used by the current vector backends.
 */
class VectorSchemaMigration {
public:
    /// Schema version enum
    enum class SchemaVersion {
        Unknown = 0,
        V1 = 1,   ///< Old schema: doc_embeddings (vec0) + doc_metadata
        V2 = 2,   ///< Unified vectors table schema (legacy installs may still carry HNSW tables)
        V2_1 = 3, ///< V2 with embedding_dim column
        V2_2 = 4, ///< V2.1 with quantized sidecar columns (TurboQuant packed codes)
        V2_3 = 5  ///< Canonical float-only schema after TurboQuant retirement
    };

    /**
     * @brief Detect current schema version from database
     * @param db SQLite database handle
     * @return Detected schema version
     */
    static SchemaVersion detectVersion(sqlite3* db);

    /**
     * @brief Migrate from V1 to V2 schema
     *
     * Steps:
     * 1. Create V2 tables for the unified vectors schema
     * 2. Copy data from doc_metadata + doc_embeddings to vectors
     * 3. Rename old tables as backup (doc_embeddings_v1_backup, doc_metadata_v1_backup)
     * 4. Leave fresh search-index state to the active backend (vec0 or Simeon PQ)
     *
     * @param db SQLite database handle
     * @param embedding_dim Embedding dimension
     * @return Success or error
     */
    static Result<void> migrateV1ToV2(sqlite3* db, size_t embedding_dim);

    /**
     * @brief Migrate V2 schema to V2.1 (add embedding_dim column)
     *
     * Steps:
     * 1. Add embedding_dim column to vectors table
     * 2. Backfill dimension from BLOB size: LENGTH(embedding) / 4
     * 3. Create index on embedding_dim for efficient queries
     *
     * @param db SQLite database handle
     * @return Success or error
     */
    static Result<void> migrateV2ToV2_1(sqlite3* db);

    /**
     * @brief Check if vectors table has embedding_dim column
     * @param db SQLite database handle
     * @return true if embedding_dim column exists
     */
    static bool hasEmbeddingDimColumn(sqlite3* db);

    /**
     * @brief Retire unused V2.2 TurboQuant storage from the canonical schema
     *
     * The migration fails without mutation when packed-code data is present or
     * when rebuilding the vectors table would discard an unsupported custom
     * schema object.
     *
     * @param db SQLite database handle
     * @return Success or error
     */
    static Result<void> migrateV2_2ToV2_3(sqlite3* db);

    /**
     * @brief Rollback V2 migration (restore V1 schema)
     *
     * Only works if backup tables exist.
     *
     * @param db SQLite database handle
     * @return Success or error
     */
    static Result<void> rollbackV2ToV1(sqlite3* db);

    /**
     * @brief Check if migration backup tables exist
     * @param db SQLite database handle
     * @return true if backup tables exist
     */
    static bool hasBackupTables(sqlite3* db);

    /**
     * @brief Remove migration backup tables (after confirming V2 works)
     * @param db SQLite database handle
     * @return Success or error
     */
    static Result<void> removeBackupTables(sqlite3* db);

    /**
     * @brief Remove all V1 tables and their shadow tables
     *
     * Drops:
     * - doc_embeddings (vec0 virtual table)
     * - doc_metadata
     * - doc_embeddings_chunks, doc_embeddings_rowids, etc. (vec0 shadow tables)
     *
     * Use this after confirming V2 works and embeddings have been regenerated.
     *
     * @param db SQLite database handle
     * @return Success or error
     */
    static Result<void> removeV1Tables(sqlite3* db);

    /**
     * @brief Check if V1 tables exist
     * @param db SQLite database handle
     * @return true if any V1 tables exist
     */
    static bool hasV1Tables(sqlite3* db);
};

} // namespace yams::vector
