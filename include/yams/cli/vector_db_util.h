#pragma once

#include <filesystem>
#include <optional>
#include <string>
#include <yams/core/types.h>

// Forward declarations
struct sqlite3;

namespace yams::cli {

// Forward declaration
class YamsCLI;

} // namespace yams::cli

namespace yams::cli::vecutil {

// ============================================================================
// Dimension Resolution
// ============================================================================

/// Result of dimension resolution with source tracking
struct DimensionResolution {
    size_t dimension{0};
    std::string source; // "db", "config", "env", "generator", "model", "heuristic"

    explicit operator bool() const { return dimension > 0; }
};

/// Resolve embedding dimension using priority: db -> config -> env -> generator -> model ->
/// heuristic
/// @param cli The CLI context (may be nullptr for standalone use)
/// @param dataPath The data directory path
/// @return DimensionResolution with dimension and source
DimensionResolution resolveEmbeddingDimension(YamsCLI* cli, const std::filesystem::path& dataPath);

/// Get dimension from existing vector DB schema
/// @param dbPath Path to vectors.db
/// @return Dimension if found, nullopt otherwise
std::optional<size_t> getDimensionFromDb(const std::filesystem::path& dbPath);

/// Get model dimension from model metadata files (config.json, sentence_bert_config.json, etc.)
/// @param dataDir The data directory containing models/
/// @param modelName The model name
/// @return Dimension if found in metadata, nullopt otherwise
std::optional<size_t> getModelDimensionFromMetadata(const std::filesystem::path& dataDir,
                                                    const std::string& modelName);

/// Get model dimension by name heuristic (MiniLM -> 384, nomic/mpnet -> 768, etc.)
/// Use getModelDimensionFromMetadata first when data directory is available.
/// @param modelName The model name
/// @return Dimension if recognized, nullopt otherwise
std::optional<size_t> getModelDimensionHeuristic(const std::string& modelName);

// ============================================================================
// Vec0 Module and Schema Validation
// ============================================================================

/// Result of schema validation
struct SchemaValidation {
    bool exists{false};              // Does the DB file exist?
    bool vec0Available{false};       // Is vec0 module loaded?
    bool schemaValid{false};         // Is doc_embeddings table valid?
    bool usesVec0{false};            // Does schema use vec0 virtual table?
    std::optional<size_t> dimension; // Extracted dimension from schema
    std::string ddl;                 // Raw DDL for debugging
    std::string error;               // Error message if any
};

/// Validate vector DB schema and vec0 module availability
/// @param dbPath Path to vectors.db
/// @return SchemaValidation result
SchemaValidation validateVecSchema(const std::filesystem::path& dbPath);

/// Check if vec0 module is available in a database connection
/// @param db Open sqlite3 connection
/// @return true if vec0 module is loaded
bool isVec0Available(sqlite3* db);

/// Initialize vec0 module in a database connection
/// @param db Open sqlite3 connection
/// @return Result with error on failure
Result<void> initVecModule(sqlite3* db);

// ============================================================================
// Database File Operations
// ============================================================================

/// Ensure vector DB file exists (create if missing)
/// @param dbPath Path to vectors.db
/// @return Result with error on failure
Result<void> ensureDbFile(const std::filesystem::path& dbPath);

/// Write vector sentinel JSON file (metadata about vector schema)
/// @param dataDir Data directory
/// @param dim Embedding dimension
void writeVectorSentinel(const std::filesystem::path& dataDir, size_t dim);

} // namespace yams::cli::vecutil
