#pragma once

#include <spdlog/spdlog.h>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <span>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <vector>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/metadata_concepts.h>
#include <yams/profiling.h>
#include <yams/search/bk_tree.h>

namespace yams::daemon {
class GraphComponent;
}

namespace yams::storage {
struct CorpusStats;
}

namespace yams::metadata {

namespace detail {
inline thread_local std::string_view metadata_op_tag;
}

inline bool metadata_trace_enabled() {
    static std::atomic<int> cached{-1};
    int v = cached.load(std::memory_order_relaxed);
    if (v >= 0)
        return v == 1;
    const char* env = std::getenv("YAMS_METADATA_TRACE");
    bool enabled = env && *env && std::string_view(env) != "0";
    cached.store(enabled ? 1 : 0, std::memory_order_relaxed);
    return enabled;
}

inline std::string_view current_metadata_op() {
    return detail::metadata_op_tag;
}

class MetadataOpScope {
public:
    explicit MetadataOpScope(std::string_view tag) : prev_(detail::metadata_op_tag) {
        detail::metadata_op_tag = tag;
    }
    ~MetadataOpScope() { detail::metadata_op_tag = prev_; }
    MetadataOpScope(const MetadataOpScope&) = delete;
    MetadataOpScope& operator=(const MetadataOpScope&) = delete;

private:
    std::string_view prev_;
};

// Forward declarations
class KnowledgeGraphStore;

// -----------------------------------------------------------------------------
// Tree diff records (PBI-043)
// -----------------------------------------------------------------------------

struct TreeSnapshotRecord {
    std::string snapshotId;
    std::string rootTreeHash;
    std::optional<int64_t> ingestDocumentId;
    std::int64_t createdTime = 0; // unix epoch seconds
    std::int64_t fileCount = 0;
    std::int64_t totalBytes = 0;
    std::unordered_map<std::string, std::string> metadata;
};

struct TreeDiffDescriptor {
    std::string baseSnapshotId;
    std::string targetSnapshotId;
    std::int64_t computedAt = 0;
    std::string status = "pending"; // complete|partial|failed
};

enum class TreeChangeType { Added, Deleted, Modified, Renamed, Moved };

struct TreeChangeRecord {
    TreeChangeType type{TreeChangeType::Modified};
    std::string oldPath;
    std::string newPath;
    std::string oldHash;
    std::string newHash;
    std::optional<int> mode;
    bool isDirectory = false;
    std::optional<std::string> contentDeltaHash;
};

struct TreeDiffQuery {
    std::string baseSnapshotId;
    std::string targetSnapshotId;
    std::optional<std::string> pathPrefix;
    std::optional<TreeChangeType> typeFilter;
    std::size_t limit = 1000;
    std::size_t offset = 0;
};

struct PathTreeNode {
    static constexpr int64_t kNullParent = -1;

    int64_t id{0};
    int64_t parentId{kNullParent};
    std::string pathSegment;
    std::string fullPath;
    int64_t docCount{0};
    int64_t centroidWeight{0};
    std::vector<float> centroid;
};

struct DocumentQueryOptions {
    std::optional<std::string> exactPath;
    std::optional<std::string> pathPrefix;
    std::optional<std::string> containsFragment;
    std::optional<std::string> fileName; // Exact match on file_name column
    std::optional<std::string> extension;
    std::optional<std::string> mimeType;
    bool textOnly{false};
    bool binaryOnly{false};
    std::vector<std::string> tags;
    std::optional<int64_t> modifiedAfter;
    std::optional<int64_t> modifiedBefore;
    std::optional<int64_t> indexedAfter;
    std::optional<int64_t> indexedBefore;
    int limit{0};
    int offset{0};
    bool orderByNameAsc{false};
    bool orderByIndexedDesc{false};
    bool pathsOnly{false};
    bool prefixIsDirectory{false};
    bool includeSubdirectories{true};
    bool containsUsesFts{false};
    std::optional<std::string> likePattern;
};

/**
 * @brief Repository interface for document metadata operations
 */
class IMetadataRepository {
public:
    virtual ~IMetadataRepository() = default;

    // Document CRUD operations
    virtual Result<int64_t> insertDocument(const DocumentInfo& info) = 0;
    virtual Result<std::optional<DocumentInfo>> getDocument(int64_t id) = 0;
    virtual Result<std::optional<DocumentInfo>> getDocumentByHash(const std::string& hash) = 0;
    virtual Result<void> updateDocument(const DocumentInfo& info) = 0;
    virtual Result<void> deleteDocument(int64_t id) = 0;

    // Content operations
    virtual Result<void> insertContent(const DocumentContent& content) = 0;
    virtual Result<std::optional<DocumentContent>> getContent(int64_t documentId) = 0;
    virtual Result<void> updateContent(const DocumentContent& content) = 0;
    virtual Result<void> deleteContent(int64_t documentId) = 0;

    // Metadata operations
    virtual Result<void> setMetadata(int64_t documentId, const std::string& key,
                                     const MetadataValue& value) = 0;
    /// Batch set metadata for multiple documents in a single transaction
    virtual Result<void> setMetadataBatch(
        const std::vector<std::tuple<int64_t, std::string, MetadataValue>>& entries) = 0;
    virtual Result<std::optional<MetadataValue>> getMetadata(int64_t documentId,
                                                             const std::string& key) = 0;
    virtual Result<std::unordered_map<std::string, MetadataValue>>
    getAllMetadata(int64_t documentId) = 0;
    virtual Result<void> removeMetadata(int64_t documentId, const std::string& key) = 0;

    // Relationship operations
    virtual Result<int64_t> insertRelationship(const DocumentRelationship& relationship) = 0;
    virtual Result<std::vector<DocumentRelationship>> getRelationships(int64_t documentId) = 0;
    virtual Result<void> deleteRelationship(int64_t relationshipId) = 0;

    // Search history operations
    virtual Result<int64_t> insertSearchHistory(const SearchHistoryEntry& entry) = 0;
    virtual Result<std::vector<SearchHistoryEntry>> getRecentSearches(int limit = 50) = 0;

    // Saved queries operations
    virtual Result<int64_t> insertSavedQuery(const SavedQuery& query) = 0;
    virtual Result<std::optional<SavedQuery>> getSavedQuery(int64_t id) = 0;
    virtual Result<std::vector<SavedQuery>> getAllSavedQueries() = 0;
    virtual Result<void> updateSavedQuery(const SavedQuery& query) = 0;
    virtual Result<void> deleteSavedQuery(int64_t id) = 0;

    // Full-text search operations
    virtual Result<void> indexDocumentContent(int64_t documentId, const std::string& title,
                                              const std::string& content,
                                              const std::string& contentType) = 0;
    virtual Result<void> indexDocumentContentTrusted(int64_t documentId, const std::string& title,
                                                     const std::string& content,
                                                     const std::string& contentType) = 0;
    virtual Result<void> removeFromIndex(int64_t documentId) = 0;
    virtual Result<void> removeFromIndexByHash(const std::string& hash) = 0;
    virtual Result<std::vector<int64_t>> getAllFts5IndexedDocumentIds() = 0;
    virtual Result<SearchResults>
    search(const std::string& query, int limit = 50, int offset = 0,
           const std::optional<std::vector<int64_t>>& docIds = std::nullopt) = 0;

    // Fuzzy search operations
    virtual Result<SearchResults>
    fuzzySearch(const std::string& query, float minSimilarity = 0.7f, int limit = 50,
                const std::optional<std::vector<int64_t>>& docIds = std::nullopt) = 0;
    virtual Result<void> buildFuzzyIndex() = 0;
    virtual Result<void> updateFuzzyIndex(int64_t documentId) = 0;

    // Bulk operations
    virtual Result<std::optional<DocumentInfo>>
    findDocumentByExactPath(const std::string& path) = 0;
    virtual Result<std::vector<DocumentInfo>>
    queryDocuments(const DocumentQueryOptions& options) = 0;
    virtual Result<std::vector<DocumentInfo>>
    findDocumentsByHashPrefix(const std::string& hashPrefix, std::size_t limit = 100) = 0;
    virtual Result<std::vector<DocumentInfo>>
    findDocumentsByExtension(const std::string& extension) = 0;
    virtual Result<std::vector<DocumentInfo>>
    findDocumentsModifiedSince(std::chrono::system_clock::time_point since) = 0;
    virtual Result<std::unordered_map<int64_t, std::unordered_map<std::string, MetadataValue>>>
    getMetadataForDocuments(std::span<const int64_t> documentIds) = 0;

    // Batch operations for search/grep performance (eliminates N queries → 1 query)
    virtual Result<std::unordered_map<std::string, DocumentInfo>>
    batchGetDocumentsByHash(const std::vector<std::string>& hashes) = 0;

    virtual Result<std::unordered_map<int64_t, DocumentContent>>
    batchGetContent(const std::vector<int64_t>& documentIds) = 0;

    // Collection and snapshot operations
    virtual Result<std::vector<DocumentInfo>>
    findDocumentsByCollection(const std::string& collection) = 0;
    virtual Result<std::vector<DocumentInfo>>
    findDocumentsBySnapshot(const std::string& snapshotId) = 0;
    virtual Result<std::vector<DocumentInfo>>
    findDocumentsBySnapshotLabel(const std::string& snapshotLabel) = 0;
    virtual Result<std::vector<std::string>> getCollections() = 0;
    virtual Result<std::vector<std::string>> getSnapshots() = 0;
    virtual Result<std::vector<std::string>> getSnapshotLabels() = 0;

    // Session operations (PBI-082)
    virtual Result<std::vector<DocumentInfo>>
    findDocumentsBySessionId(const std::string& sessionId) = 0;
    virtual Result<int64_t> countDocumentsBySessionId(const std::string& sessionId) = 0;
    virtual Result<void> removeSessionIdFromDocuments(const std::string& sessionId) = 0;
    virtual Result<int64_t> deleteDocumentsBySessionId(const std::string& sessionId) = 0;

    // Tag operations
    virtual Result<std::vector<DocumentInfo>>
    findDocumentsByTags(const std::vector<std::string>& tags, bool matchAll = false) = 0;
    virtual Result<std::vector<std::string>> getDocumentTags(int64_t documentId) = 0;
    virtual Result<std::unordered_map<int64_t, std::vector<std::string>>>
    batchGetDocumentTags(std::span<const int64_t> documentIds) = 0;
    virtual Result<std::vector<std::string>> getAllTags() = 0;

    // Statistics
    virtual Result<int64_t> getDocumentCount() = 0;
    virtual Result<int64_t> getIndexedDocumentCount() = 0;          // Embeddings-based
    virtual Result<int64_t> getContentExtractedDocumentCount() = 0; // New: content_extracted flag
    virtual Result<std::unordered_map<std::string, int64_t>> getDocumentCountsByExtension() = 0;
    // Count documents by extraction status
    virtual Result<int64_t> getDocumentCountByExtractionStatus(ExtractionStatus status) = 0;

    // Corpus statistics for adaptive search tuning (PBI: Adaptive Tuning Epic)
    virtual Result<storage::CorpusStats> getCorpusStats() = 0;

    // Embedding status operations
    virtual Result<void> updateDocumentEmbeddingStatus(int64_t documentId, bool hasEmbedding,
                                                       const std::string& modelId = "") = 0;
    virtual Result<void> updateDocumentEmbeddingStatusByHash(const std::string& hash,
                                                             bool hasEmbedding,
                                                             const std::string& modelId = "") = 0;

    // Extraction status operations (avoid read-modify-write)
    virtual Result<void> updateDocumentExtractionStatus(int64_t documentId, bool contentExtracted,
                                                        ExtractionStatus status,
                                                        const std::string& error = "") = 0;

    // Repair status operations
    virtual Result<void> updateDocumentRepairStatus(const std::string& hash,
                                                    RepairStatus status) = 0;

    /**
     * @brief Force a WAL checkpoint.
     */
    virtual Result<void> checkpointWal() = 0;

    // Path tree operations (PBI-051 scaffold)
    virtual Result<std::optional<PathTreeNode>> findPathTreeNode(int64_t parentId,
                                                                 std::string_view pathSegment) = 0;
    virtual Result<PathTreeNode> insertPathTreeNode(int64_t parentId, std::string_view pathSegment,
                                                    std::string_view fullPath) = 0;
    virtual Result<void> incrementPathTreeDocCount(int64_t nodeId, int64_t documentId) = 0;
    virtual Result<void> accumulatePathTreeCentroid(int64_t nodeId,
                                                    std::span<const float> embeddingValues) = 0;
    virtual Result<std::optional<PathTreeNode>>
    findPathTreeNodeByFullPath(std::string_view fullPath) = 0;
    virtual Result<std::vector<PathTreeNode>> listPathTreeChildren(std::string_view fullPath,
                                                                   std::size_t limit = 25) = 0;
    virtual Result<void> upsertPathTreeForDocument(const DocumentInfo& info, int64_t documentId,
                                                   bool isNewDocument,
                                                   std::span<const float> embeddingValues) = 0;
    virtual Result<void> removePathTreeForDocument(const DocumentInfo& info, int64_t documentId,
                                                   std::span<const float> embeddingValues) = 0;

    // Tree-based document queries (PBI-043 integration)
    virtual Result<std::vector<DocumentInfo>>
    findDocumentsByPathTreePrefix(std::string_view pathPrefix, bool includeSubdirectories = true,
                                  int limit = 0) = 0;

    // Tree diff persistence (PBI-043)
    virtual Result<void> upsertTreeSnapshot(const TreeSnapshotRecord& record) = 0;
    virtual Result<std::optional<TreeSnapshotRecord>>
    getTreeSnapshot(std::string_view snapshotId) = 0;
    virtual Result<std::vector<TreeSnapshotRecord>> listTreeSnapshots(int limit = 100) = 0;
    virtual Result<int64_t> beginTreeDiff(const TreeDiffDescriptor& descriptor) = 0;
    virtual Result<void> appendTreeChanges(int64_t diffId,
                                           const std::vector<TreeChangeRecord>& changes) = 0;
    virtual Result<std::vector<TreeChangeRecord>> listTreeChanges(const TreeDiffQuery& query) = 0;
    virtual Result<void> finalizeTreeDiff(int64_t diffId, std::size_t changeCount,
                                          std::string_view status) = 0;
};

/**
 * @brief SQLite-based implementation of metadata repository
 */
class MetadataRepository : public IMetadataRepository {
public:
    explicit MetadataRepository(ConnectionPool& pool);
    ~MetadataRepository()
        override; // Defined in cpp to allow unique_ptr<CorpusStats> with forward decl

    // Document operations
    Result<int64_t> insertDocument(const DocumentInfo& info) override;
    Result<std::optional<DocumentInfo>> getDocument(int64_t id) override;
    Result<std::optional<DocumentInfo>> getDocumentByHash(const std::string& hash) override;
    Result<void> updateDocument(const DocumentInfo& info) override;
    Result<void> deleteDocument(int64_t id) override;

    // Content operations
    Result<void> insertContent(const DocumentContent& content) override;
    Result<std::optional<DocumentContent>> getContent(int64_t documentId) override;
    Result<void> updateContent(const DocumentContent& content) override;
    Result<void> deleteContent(int64_t documentId) override;

    // Metadata operations
    Result<void> setMetadata(int64_t documentId, const std::string& key,
                             const MetadataValue& value) override;
    Result<void> setMetadataBatch(
        const std::vector<std::tuple<int64_t, std::string, MetadataValue>>& entries) override;
    Result<std::optional<MetadataValue>> getMetadata(int64_t documentId,
                                                     const std::string& key) override;
    Result<std::unordered_map<std::string, MetadataValue>>
    getAllMetadata(int64_t documentId) override;
    Result<void> removeMetadata(int64_t documentId, const std::string& key) override;

    // Relationship operations
    Result<int64_t> insertRelationship(const DocumentRelationship& relationship) override;
    Result<std::vector<DocumentRelationship>> getRelationships(int64_t documentId) override;
    Result<void> deleteRelationship(int64_t relationshipId) override;

    // Search history operations
    Result<int64_t> insertSearchHistory(const SearchHistoryEntry& entry) override;
    Result<std::vector<SearchHistoryEntry>> getRecentSearches(int limit = 50) override;

    // Saved queries operations
    Result<int64_t> insertSavedQuery(const SavedQuery& query) override;
    Result<std::optional<SavedQuery>> getSavedQuery(int64_t id) override;
    Result<std::vector<SavedQuery>> getAllSavedQueries() override;
    Result<void> updateSavedQuery(const SavedQuery& query) override;
    Result<void> deleteSavedQuery(int64_t id) override;

    // Full-text search operations
    Result<void> indexDocumentContent(int64_t documentId, const std::string& title,
                                      const std::string& content,
                                      const std::string& contentType) override;
    Result<void> indexDocumentContentTrusted(int64_t documentId, const std::string& title,
                                             const std::string& content,
                                             const std::string& contentType) override;
    Result<void> removeFromIndex(int64_t documentId) override;
    Result<void> removeFromIndexByHash(const std::string& hash) override;
    Result<std::vector<int64_t>> getAllFts5IndexedDocumentIds() override;
    Result<SearchResults>
    search(const std::string& query, int limit = 50, int offset = 0,
           const std::optional<std::vector<int64_t>>& docIds = std::nullopt) override;

    // Fuzzy search operations
    Result<SearchResults>
    fuzzySearch(const std::string& query, float minSimilarity = 0.7f, int limit = 50,
                const std::optional<std::vector<int64_t>>& docIds = std::nullopt) override;
    Result<void> buildFuzzyIndex() override;
    Result<void> updateFuzzyIndex(int64_t documentId) override;

    // Bulk operations
    Result<std::vector<DocumentInfo>> findDocumentsByHashPrefix(const std::string& hashPrefix,
                                                                std::size_t limit = 100) override;
    Result<std::vector<DocumentInfo>>
    findDocumentsByExtension(const std::string& extension) override;
    Result<std::vector<DocumentInfo>>
    findDocumentsModifiedSince(std::chrono::system_clock::time_point since) override;

    // Collection and snapshot operations
    Result<std::vector<DocumentInfo>>
    findDocumentsByCollection(const std::string& collection) override;
    Result<std::vector<DocumentInfo>>
    findDocumentsBySnapshot(const std::string& snapshotId) override;
    Result<std::vector<DocumentInfo>>
    findDocumentsBySnapshotLabel(const std::string& snapshotLabel) override;

    Result<std::optional<DocumentInfo>> findDocumentByExactPath(const std::string& path) override;
    Result<std::vector<DocumentInfo>> queryDocuments(const DocumentQueryOptions& options) override;
    Result<std::vector<std::string>> getCollections() override;
    Result<std::vector<std::string>> getSnapshots() override;
    Result<std::vector<std::string>> getSnapshotLabels() override;

    // Session operations (PBI-082)
    Result<std::vector<DocumentInfo>>
    findDocumentsBySessionId(const std::string& sessionId) override;
    Result<int64_t> countDocumentsBySessionId(const std::string& sessionId) override;
    Result<void> removeSessionIdFromDocuments(const std::string& sessionId) override;
    Result<int64_t> deleteDocumentsBySessionId(const std::string& sessionId) override;

    Result<std::unordered_map<int64_t, std::unordered_map<std::string, MetadataValue>>>
    getMetadataForDocuments(std::span<const int64_t> documentIds) override;

    // Tag operations
    Result<std::vector<DocumentInfo>> findDocumentsByTags(const std::vector<std::string>& tags,
                                                          bool matchAll = false) override;
    Result<std::vector<std::string>> getDocumentTags(int64_t documentId) override;
    Result<std::unordered_map<int64_t, std::vector<std::string>>>
    batchGetDocumentTags(std::span<const int64_t> documentIds) override;
    Result<std::vector<std::string>> getAllTags() override;

    // Statistics
    Result<int64_t> getDocumentCount() override;
    Result<int64_t> getIndexedDocumentCount() override;          // Embeddings-based
    Result<int64_t> getContentExtractedDocumentCount() override; // New
    Result<std::unordered_map<std::string, int64_t>> getDocumentCountsByExtension() override;
    Result<int64_t> getDocumentCountByExtractionStatus(ExtractionStatus status) override;
    Result<storage::CorpusStats> getCorpusStats() override;

    // Component-owned metrics (lock-free, updated on insert/delete)
    uint64_t getCachedDocumentCount() const noexcept {
        return cachedDocumentCount_.load(std::memory_order_relaxed);
    }
    uint64_t getCachedIndexedCount() const noexcept {
        return cachedIndexedCount_.load(std::memory_order_relaxed);
    }
    uint64_t getCachedExtractedCount() const noexcept {
        return cachedExtractedCount_.load(std::memory_order_relaxed);
    }
    void initializeCounters(); // Called once during startup to sync with DB

    // Batch operations for search/grep performance (eliminates N queries → 1 query)
    Result<std::unordered_map<std::string, DocumentInfo>>
    batchGetDocumentsByHash(const std::vector<std::string>& hashes) override;

    Result<std::unordered_map<int64_t, DocumentContent>>
    batchGetContent(const std::vector<int64_t>& documentIds) override;

    // Embedding status operations
    Result<void> updateDocumentEmbeddingStatus(int64_t documentId, bool hasEmbedding,
                                               const std::string& modelId = "") override;
    Result<void> updateDocumentEmbeddingStatusByHash(const std::string& hash, bool hasEmbedding,
                                                     const std::string& modelId = "") override;

    // Extraction status operations (avoid read-modify-write)
    Result<void> updateDocumentExtractionStatus(int64_t documentId, bool contentExtracted,
                                                ExtractionStatus status,
                                                const std::string& error = "") override;

    // Repair status operations
    Result<void> updateDocumentRepairStatus(const std::string& hash, RepairStatus status) override;

    Result<void> checkpointWal() override;

    /**
     * @brief Refresh all idle connections in the pool (PBI-079)
     *
     * Discards all idle connections, forcing new connections to be created.
     * This invalidates cached SQLite query plans and prepared statements,
     * ensuring queries see the latest database state after WAL checkpoint.
     */
    void refreshAllConnections();

    // Path tree operations (PBI-051 scaffold)
    Result<std::optional<PathTreeNode>> findPathTreeNode(int64_t parentId,
                                                         std::string_view pathSegment) override;
    Result<PathTreeNode> insertPathTreeNode(int64_t parentId, std::string_view pathSegment,
                                            std::string_view fullPath) override;
    Result<void> incrementPathTreeDocCount(int64_t nodeId, int64_t documentId) override;
    Result<void> accumulatePathTreeCentroid(int64_t nodeId,
                                            std::span<const float> embeddingValues) override;
    Result<std::optional<PathTreeNode>>
    findPathTreeNodeByFullPath(std::string_view fullPath) override;
    Result<std::vector<PathTreeNode>> listPathTreeChildren(std::string_view fullPath,
                                                           std::size_t limit = 25) override;
    Result<void> upsertPathTreeForDocument(const DocumentInfo& info, int64_t documentId,
                                           bool isNewDocument,
                                           std::span<const float> embeddingValues) override;
    Result<void> removePathTreeForDocument(const DocumentInfo& info, int64_t documentId,
                                           std::span<const float> embeddingValues) override;

    // Tree-based document queries (PBI-043 integration)
    Result<std::vector<DocumentInfo>>
    findDocumentsByPathTreePrefix(std::string_view pathPrefix, bool includeSubdirectories = true,
                                  int limit = 0) override;

    // Tree diff persistence
    Result<void> upsertTreeSnapshot(const TreeSnapshotRecord& record) override;
    Result<std::optional<TreeSnapshotRecord>> getTreeSnapshot(std::string_view snapshotId) override;
    Result<std::vector<TreeSnapshotRecord>> listTreeSnapshots(int limit = 100) override;
    Result<int64_t> beginTreeDiff(const TreeDiffDescriptor& descriptor) override;
    Result<void> appendTreeChanges(int64_t diffId,
                                   const std::vector<TreeChangeRecord>& changes) override;
    Result<std::vector<TreeChangeRecord>> listTreeChanges(const TreeDiffQuery& query) override;
    Result<void> finalizeTreeDiff(int64_t diffId, std::size_t changeCount,
                                  std::string_view status) override;

public:
    void setKnowledgeGraphStore(std::shared_ptr<KnowledgeGraphStore> kgStore) {
        kgStore_ = std::move(kgStore);
    }

    void setGraphComponent(std::shared_ptr<yams::daemon::GraphComponent> graphComponent) {
        graphComponent_ = std::move(graphComponent);
    }

    std::shared_ptr<KnowledgeGraphStore> getKnowledgeGraphStore() const { return kgStore_; }

private:
    ConnectionPool& pool_;
    bool hasPathIndexing_{false};
    bool pathFtsAvailable_{false};
    std::shared_ptr<KnowledgeGraphStore> kgStore_; // PBI-043: tree diff KG integration
    std::shared_ptr<yams::daemon::GraphComponent> graphComponent_; // PBI-009: centralized graph ops

    // Component-owned metrics (updated on insert/delete, read by DaemonMetrics)
    mutable std::atomic<uint64_t> cachedDocumentCount_{0};
    mutable std::atomic<uint64_t> cachedIndexedCount_{0};
    mutable std::atomic<uint64_t> cachedExtractedCount_{0};
    mutable std::atomic<bool> countersInitialized_{false};

    // Legacy makeSelect removed; callers now use sql::QuerySpec to build SELECTs

    // Fuzzy search indices
    mutable std::unique_ptr<search::HybridFuzzySearch> fuzzySearchIndex_;
    mutable YAMS_SHARED_LOCKABLE(std::shared_mutex, fuzzyIndexMutex_);

    struct PathCacheEntry {
        std::string path;
        DocumentInfo document;
    };

    // Lock-free read-mostly path cache snapshot with approximate LRU metadata
    struct PathCacheSnapshot {
        struct Entry {
            DocumentInfo doc;
            uint64_t lastHitSeq{0};
            uint64_t insertedSeq{0};
        };
        std::unordered_map<std::string, Entry> data;
        std::chrono::steady_clock::time_point timestamp{std::chrono::steady_clock::now()};
        uint64_t buildSeq{0};
    };
    // Use atomic_load/atomic_store free functions for shared_ptr
    mutable std::shared_ptr<PathCacheSnapshot> pathCacheSnapshot_{
        std::make_shared<PathCacheSnapshot>()};
    std::size_t pathCacheCapacity_ = 1024;
    struct PathCacheWriteBuffer {
        std::vector<DocumentInfo> pending;
        std::mutex mutex;
        std::atomic<std::size_t> size{0};
    };
    static constexpr std::size_t kPathCacheFlushThreshold = 32;
    mutable PathCacheWriteBuffer pathCacheWriteBuffer_{};

    struct QueryCacheEntry {
        SearchResults results;
        std::chrono::steady_clock::time_point timestamp{};
        uint64_t hits{0};
    };
    using QueryCacheMap = std::unordered_map<std::string, QueryCacheEntry>;
    static constexpr std::chrono::seconds kQueryCacheTtl{300};
    static constexpr std::size_t kQueryCacheCapacity = 512;
    mutable std::shared_ptr<QueryCacheMap> queryCacheSnapshot_{std::make_shared<QueryCacheMap>()};
    mutable std::mutex queryCacheMutex_;

    // CorpusStats cache (invalidated on document changes, TTL 30s base)
    mutable std::unique_ptr<storage::CorpusStats> cachedCorpusStats_;
    mutable std::chrono::steady_clock::time_point corpusStatsCachedAt_{};
    mutable uint64_t corpusStatsDocCount_{0}; // docCount at cache time for change detection
    mutable std::shared_mutex corpusStatsMutex_;
    static constexpr std::chrono::seconds kCorpusStatsTtl{30}; // Base TTL

    // Approximate LRU hit recording (lock-free ring of path hashes)
    mutable std::unique_ptr<std::atomic<uint64_t>[]> hitRing_;
    mutable std::size_t hitRingSize_{0};
    mutable std::atomic<uint64_t> hitSeq_{0};
    mutable std::size_t hitRingMask_{0};
    mutable std::atomic<uint64_t> globalSeq_{0};

    // Helper methods for row mapping
    DocumentInfo mapDocumentRow(Statement& stmt);
    DocumentContent mapContentRow(Statement& stmt);
    DocumentRelationship mapRelationshipRow(Statement& stmt);
    SearchHistoryEntry mapSearchHistoryRow(Statement& stmt);
    SavedQuery mapSavedQueryRow(Statement& stmt);

    Result<void> ensureFuzzyIndexInitialized();

    // Internal helpers that accept an existing Database& to avoid nested connection acquisition
    // These are used within executeQuery lambdas to prevent deadlock when sub-queries are needed
    Result<std::optional<DocumentInfo>> getDocumentInternal(Database& db, int64_t id);
    Result<std::unordered_map<std::string, MetadataValue>>
    getAllMetadataInternal(Database& db, int64_t documentId);

    std::optional<DocumentInfo> lookupPathCache(const std::string& normalizedPath) const;
    void storePathCache(const DocumentInfo& info) const;
    void flushPathCacheBuffer() const;
    void recordPathHit(const std::string& normalizedPath) const;
    void updateQueryCache(const std::string& key, const SearchResults& results) const;
    void invalidateQueryCache() const;

    // Helper method to handle nested Result from withConnection
    template <typename T> Result<T> executeQuery(std::function<Result<T>(Database&)> func) {
        auto result = pool_.withConnection(func);
        if (!result.has_value()) {
            if (metadata_trace_enabled()) {
                spdlog::warn("MetadataRepository::executeQuery op='{}' error: {}",
                             current_metadata_op(), result.error().message);
            }
            spdlog::error("MetadataRepository::executeQuery connection error: {}",
                          result.error().message);
            return Error{result.error()};
        }
        if constexpr (std::is_void_v<T>) {
            return Result<void>();
        } else {
            return result.value();
        }
    }
};

/**
 * @brief Builder for metadata queries with fluent interface
 */
class MetadataQueryBuilder {
public:
    MetadataQueryBuilder() = default;

    // Document filtering
    MetadataQueryBuilder& withExtension(const std::string& extension);
    MetadataQueryBuilder& withMimeType(const std::string& mimeType);
    MetadataQueryBuilder& withPathContaining(const std::string& pathFragment);
    MetadataQueryBuilder& modifiedAfter(std::chrono::system_clock::time_point time);
    MetadataQueryBuilder& modifiedBefore(std::chrono::system_clock::time_point time);
    MetadataQueryBuilder& indexedAfter(std::chrono::system_clock::time_point time);
    MetadataQueryBuilder& withContentExtracted(bool extracted);
    MetadataQueryBuilder& withExtractionStatus(ExtractionStatus status);

    // Metadata filtering
    MetadataQueryBuilder& withMetadata(const std::string& key, const std::string& value);
    MetadataQueryBuilder& withMetadataKey(const std::string& key);

    // Content filtering
    MetadataQueryBuilder& withContentLanguage(const std::string& language);
    MetadataQueryBuilder& withMinContentLength(int64_t minLength);
    MetadataQueryBuilder& withMaxContentLength(int64_t maxLength);

    // Sorting and pagination
    MetadataQueryBuilder& orderByModified(bool ascending = false);
    MetadataQueryBuilder& orderByIndexed(bool ascending = false);
    MetadataQueryBuilder& orderBySize(bool ascending = false);
    MetadataQueryBuilder& limit(int count);
    MetadataQueryBuilder& offset(int count);

    // Build the query
    [[nodiscard]] std::string buildQuery() const;
    [[nodiscard]] std::vector<std::string> getParameters() const;

private:
    std::vector<std::string> conditions_;
    std::vector<std::string> parameters_;
    std::string orderBy_;
    int limit_ = -1;
    int offset_ = -1;
};

/**
 * @brief Transaction helper for atomic metadata operations
 */
class MetadataTransaction {
public:
    explicit MetadataTransaction(MetadataRepository& repo);
    ~MetadataTransaction();

    // Non-copyable, non-movable
    MetadataTransaction(const MetadataTransaction&) = delete;
    MetadataTransaction& operator=(const MetadataTransaction&) = delete;
    MetadataTransaction(MetadataTransaction&&) = delete;
    MetadataTransaction& operator=(MetadataTransaction&&) = delete;

    /**
     * @brief Get reference to repository for operations within transaction
     */
    MetadataRepository& repository() { return repo_; }

private:
    MetadataRepository& repo_;
};

} // namespace yams::metadata

// Concept compliance check for compile-time guarantees
static_assert(yams::metadata::FullMetadataStore<yams::metadata::MetadataRepository>);
