#pragma once

#include <atomic>
#include <chrono>
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
#include <yams/search/bk_tree.h>

namespace yams::metadata {

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

struct DocumentQueryOptions {
    std::optional<std::string> exactPath;
    std::optional<std::string> pathPrefix;
    std::optional<std::string> containsFragment;
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
    virtual Result<void> removeFromIndex(int64_t documentId) = 0;
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

    // Tag operations
    virtual Result<std::vector<DocumentInfo>>
    findDocumentsByTags(const std::vector<std::string>& tags, bool matchAll = false) = 0;
    virtual Result<std::vector<std::string>> getDocumentTags(int64_t documentId) = 0;
    virtual Result<std::vector<std::string>> getAllTags() = 0;

    // Statistics
    virtual Result<int64_t> getDocumentCount() = 0;
    virtual Result<int64_t> getIndexedDocumentCount() = 0;          // Embeddings-based
    virtual Result<int64_t> getContentExtractedDocumentCount() = 0; // New: content_extracted flag
    virtual Result<std::unordered_map<std::string, int64_t>> getDocumentCountsByExtension() = 0;
    // Count documents by extraction status
    virtual Result<int64_t> getDocumentCountByExtractionStatus(ExtractionStatus status) = 0;

    // Embedding status operations
    virtual Result<void> updateDocumentEmbeddingStatus(int64_t documentId, bool hasEmbedding,
                                                       const std::string& modelId = "") = 0;
    virtual Result<void> updateDocumentEmbeddingStatusByHash(const std::string& hash,
                                                             bool hasEmbedding,
                                                             const std::string& modelId = "") = 0;

    /**
     * @brief Force a WAL checkpoint.
     */
    virtual Result<void> checkpointWal() = 0;

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
    ~MetadataRepository() override = default;

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
    Result<void> removeFromIndex(int64_t documentId) override;
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

    Result<std::unordered_map<int64_t, std::unordered_map<std::string, MetadataValue>>>
    getMetadataForDocuments(std::span<const int64_t> documentIds) override;

    // Tag operations
    Result<std::vector<DocumentInfo>> findDocumentsByTags(const std::vector<std::string>& tags,
                                                          bool matchAll = false) override;
    Result<std::vector<std::string>> getDocumentTags(int64_t documentId) override;
    Result<std::vector<std::string>> getAllTags() override;

    // Statistics
    Result<int64_t> getDocumentCount() override;
    Result<int64_t> getIndexedDocumentCount() override;          // Embeddings-based
    Result<int64_t> getContentExtractedDocumentCount() override; // New
    Result<std::unordered_map<std::string, int64_t>> getDocumentCountsByExtension() override;
    Result<int64_t> getDocumentCountByExtractionStatus(ExtractionStatus status) override;

    // Embedding status operations
    Result<void> updateDocumentEmbeddingStatus(int64_t documentId, bool hasEmbedding,
                                               const std::string& modelId = "") override;
    Result<void> updateDocumentEmbeddingStatusByHash(const std::string& hash, bool hasEmbedding,
                                                     const std::string& modelId = "") override;

    Result<void> checkpointWal() override;

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

private:
    ConnectionPool& pool_;
    bool hasPathIndexing_{false};
    std::shared_ptr<KnowledgeGraphStore> kgStore_; // PBI-043: tree diff KG integration

    // Legacy makeSelect removed; callers now use sql::QuerySpec to build SELECTs

    // Fuzzy search indices
    mutable std::unique_ptr<search::HybridFuzzySearch> fuzzySearchIndex_;
    mutable std::shared_mutex fuzzyIndexMutex_;

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
