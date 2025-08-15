#pragma once

#include <chrono>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <yams/core/types.h>
#include <yams/vector/vector_database.h>

namespace yams::vector {

/**
 * Configuration for embedding lifecycle management
 */
struct EmbeddingLifecycleConfig {
    // Re-embedding settings
    size_t batch_size = 100;                    // Batch size for re-embedding
    size_t max_concurrent_reembeddings = 4;     // Parallel re-embedding jobs
    std::chrono::hours stale_threshold{24 * 7}; // Consider stale after 1 week

    // Tombstone settings
    std::chrono::hours tombstone_retention{24 * 30}; // Keep tombstones for 30 days
    size_t tombstone_purge_batch_size = 1000;        // Batch size for purging

    // Version tracking
    bool strict_versioning = true;       // Require exact version match
    bool auto_reembed_on_upgrade = true; // Auto re-embed on model upgrade
};

/**
 * Tracks embedding staleness reasons
 */
enum class StalenessReason {
    MODEL_VERSION_CHANGE,  // Model version updated
    SCHEMA_VERSION_CHANGE, // Embedding schema changed
    CONTENT_MODIFIED,      // Source content changed
    MANUAL_INVALIDATION,   // Manually marked stale
    EXPIRED_AGE            // Too old based on policy
};

/**
 * Represents a stale embedding that needs re-processing
 */
struct StaleEmbedding {
    std::string chunk_id;
    std::string document_hash;
    std::string current_model_version;
    std::string required_model_version;
    StalenessReason reason;
    std::chrono::system_clock::time_point detected_at;
    int priority = 0; // Higher priority gets re-embedded first
};

/**
 * Tombstone record for deleted embeddings
 */
struct EmbeddingTombstone {
    std::string chunk_id;
    std::string document_hash;
    std::chrono::system_clock::time_point deleted_at;
    std::string deletion_reason;
};

/**
 * Statistics for embedding lifecycle operations
 */
struct LifecycleStats {
    size_t total_embeddings = 0;
    size_t stale_embeddings = 0;
    size_t reembedded_count = 0;
    size_t tombstone_count = 0;
    size_t failed_reembeddings = 0;
    std::chrono::system_clock::time_point last_check;
    std::chrono::milliseconds last_reembedding_duration{0};
};

/**
 * Manages embedding lifecycle including versioning, staleness detection, and re-embedding
 */
class EmbeddingLifecycleManager {
public:
    EmbeddingLifecycleManager();
    explicit EmbeddingLifecycleManager(std::shared_ptr<VectorDatabase> vector_db,
                                       const EmbeddingLifecycleConfig& config = {});
    ~EmbeddingLifecycleManager();

    // Staleness detection
    Result<std::vector<StaleEmbedding>>
    detectStaleEmbeddings(const std::string& current_model_id,
                          const std::string& current_model_version);

    Result<bool> isEmbeddingStale(const VectorRecord& record,
                                  const std::string& current_model_version);

    // Re-embedding operations
    Result<void> scheduleReembedding(const std::vector<std::string>& chunk_ids);
    Result<void> reembedSingleRecord(const std::string& chunk_id);
    Result<size_t> reembedBatch(const std::vector<std::string>& chunk_ids);
    Result<void> reembedAllStale();

    // Content change detection
    Result<void> validateContentHash(const std::string& chunk_id,
                                     const std::string& current_content_hash);

    Result<std::vector<std::string>>
    detectContentChanges(const std::unordered_map<std::string, std::string>& chunk_to_hash);

    // Deletion and tombstone management
    Result<void> markEmbeddingsForDeletion(const std::string& document_hash);
    Result<void> createTombstone(const std::string& chunk_id, const std::string& reason);
    Result<size_t> purgeTombstones(std::chrono::hours age_threshold);
    Result<std::vector<EmbeddingTombstone>> getTombstones(size_t limit = 100);

    // Version migration
    Result<void> migrateToVersion(const std::string& target_model_id,
                                  const std::string& target_model_version,
                                  uint32_t target_schema_version);

    // Monitoring and statistics
    LifecycleStats getStatistics() const;
    Result<void> runHealthCheck();
    Result<std::unordered_map<std::string, size_t>> getVersionDistribution();

    // Configuration
    void setConfig(const EmbeddingLifecycleConfig& config);
    EmbeddingLifecycleConfig getConfig() const;

    // Callbacks for re-embedding progress
    using ProgressCallback = std::function<void(size_t processed, size_t total)>;
    void setProgressCallback(ProgressCallback callback);

    // Manual operations
    Result<void> invalidateEmbeddings(const std::vector<std::string>& chunk_ids);
    Result<void> forceReembedding(const std::string& document_hash);

private:
    class Impl;
    std::unique_ptr<Impl> pImpl;
};

/**
 * Factory function to create lifecycle manager with dependencies
 */
std::unique_ptr<EmbeddingLifecycleManager>
createEmbeddingLifecycleManager(std::shared_ptr<VectorDatabase> vector_db,
                                const EmbeddingLifecycleConfig& config = {});

/**
 * Utility functions for embedding versioning
 */
namespace embedding_utils {
// Compare model versions (semantic versioning)
bool isNewerVersion(const std::string& version1, const std::string& version2);

// Parse semantic version string
struct SemanticVersion {
    int major = 0;
    int minor = 0;
    int patch = 0;
    std::string prerelease;

    static SemanticVersion parse(const std::string& version);
    bool operator<(const SemanticVersion& other) const;
    bool operator==(const SemanticVersion& other) const;
    std::string toString() const;
};

// Calculate content hash for embedding validation
std::string calculateContentHash(const std::string& content);

// Determine staleness reason
StalenessReason determineStaleness(const VectorRecord& record,
                                   const std::string& current_model_version,
                                   const std::string& current_content_hash);

// Priority calculation for re-embedding queue
int calculateReembeddingPriority(const StaleEmbedding& embedding,
                                 const std::chrono::system_clock::time_point& now);
} // namespace embedding_utils

} // namespace yams::vector