#pragma once

#include <filesystem>
#include <optional>
#include <string>
#include <vector>
#include <yams/daemon/client/daemon_client.h>

namespace yams::cli {

// Forward declaration
class YamsCLI;

} // namespace yams::cli

namespace yams::cli::doctor {

// ============================================================================
// Daemon Health Check
// ============================================================================

struct DaemonCheckResult {
    bool running{false};
    bool ready{false};
    std::string lifecycleState;
    std::string version;
    size_t activeConnections{0};
    double memoryMb{0.0};
    double cpuPercent{0.0};

    // Vector scoring status
    bool vectorEmbeddingsAvailable{false};
    bool vectorScoringEnabled{false};

    // Worker pool
    size_t workerThreads{0};
    size_t workerActive{0};
    size_t workerQueued{0};

    std::vector<std::string> issues;
};

/// Check daemon connectivity and health
/// @param cli CLI context
/// @param cachedStatus Optional cached status to avoid redundant RPC
/// @return DaemonCheckResult
DaemonCheckResult checkDaemon(YamsCLI* cli, std::optional<daemon::StatusResponse>& cachedStatus);

// ============================================================================
// Installed Models Check
// ============================================================================

struct ModelInfo {
    std::string name;
    int dimension{-1}; // -1 if unknown
    bool hasConfig{false};
    bool hasTokenizer{false};
    std::filesystem::path path;
};

struct ModelsCheckResult {
    std::vector<ModelInfo> installed;
    std::vector<std::string> warnings;
    size_t oldLocationCount{0}; // Models found in deprecated ~/.yams/models
    std::filesystem::path currentPath;
    std::filesystem::path deprecatedPath;
};

/// Check installed embedding models
/// @param cli CLI context
/// @return ModelsCheckResult
ModelsCheckResult checkInstalledModels(YamsCLI* cli);

// ============================================================================
// Vector Database Check
// ============================================================================

struct VectorDbCheckResult {
    bool exists{false};
    bool vec0Available{false};
    bool schemaValid{false};
    bool usesVec0{false};
    std::optional<size_t> dimension;
    std::string schemaType; // "vec0", "legacy", "none"
    std::vector<std::string> issues;
    std::filesystem::path dbPath;
};

/// Check vector database schema and vec0 module
/// @param cli CLI context
/// @return VectorDbCheckResult
VectorDbCheckResult checkVectorDb(YamsCLI* cli);

// ============================================================================
// Dimension Mismatch Check
// ============================================================================

struct DimensionCheckResult {
    bool dbReady{false};
    size_t dbDimension{0};
    size_t targetDimension{0};
    std::string targetSource; // Where target dimension came from
    bool matches{false};
    std::vector<std::string> recommendations;
};

/// Check for embedding dimension mismatches
/// @param cli CLI context
/// @param cachedStatus Optional cached daemon status
/// @return DimensionCheckResult
DimensionCheckResult checkDimensions(YamsCLI* cli,
                                     const std::optional<daemon::StatusResponse>& cachedStatus);

// ============================================================================
// Knowledge Graph Check
// ============================================================================

struct KnowledgeGraphCheckResult {
    bool available{false};
    int64_t nodeCount{0};
    int64_t edgeCount{0};
    int64_t aliasCount{0};
    int64_t embeddingCount{0};
    int64_t docEntityCount{0};
    bool isEmpty{false};
    std::vector<std::string> issues;
};

/// Check knowledge graph health
/// @param cli CLI context
/// @return KnowledgeGraphCheckResult
KnowledgeGraphCheckResult checkKnowledgeGraph(YamsCLI* cli);

// ============================================================================
// Database Migration Check
// ============================================================================

struct MigrationCheckResult {
    bool hasFailures{false};
    bool hasLeftoverTables{false};
    std::vector<std::string> failedMigrations;
    std::vector<std::string> leftoverTables;
};

/// Check database migration health
/// @param cli CLI context
/// @return MigrationCheckResult
MigrationCheckResult checkMigrations(YamsCLI* cli);

} // namespace yams::cli::doctor
