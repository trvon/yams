#pragma once

#include <yams/core/types.h>
#include <yams/vector/vector_types.h>

#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace yams::vector {

class VectorDatabase {
public:
    using DatabaseStats = VectorDatabaseStats;

    explicit VectorDatabase(const VectorDatabaseConfig& config = {});
    ~VectorDatabase();

    VectorDatabase(const VectorDatabase&) = delete;
    VectorDatabase& operator=(const VectorDatabase&) = delete;
    VectorDatabase(VectorDatabase&&) noexcept;
    VectorDatabase& operator=(VectorDatabase&&) noexcept;

    bool initialize();
    bool isInitialized() const;
    void close();
    void initializeCounter();

    bool createTable();
    bool tableExists() const;
    void dropTable();
    size_t getVectorCount() const;
    size_t getEmbeddingDim() const;

    bool insertVector(const VectorRecord& record);
    bool insertVectorsBatch(const std::vector<VectorRecord>& records);
    bool updateVector(const std::string& chunk_id, const VectorRecord& record);
    bool deleteVector(const std::string& chunk_id);
    bool deleteVectorsByDocument(const std::string& document_hash);

    std::vector<VectorRecord> searchSimilar(const std::vector<float>& query_embedding,
                                            const VectorSearchParams& params = {}) const;
    std::vector<std::vector<VectorRecord>>
    searchSimilarBatch(const std::vector<std::vector<float>>& query_embeddings,
                       const VectorSearchParams& params = {}, size_t num_threads = 0) const;
    std::vector<VectorRecord> searchSimilarToDocument(const std::string& document_hash,
                                                      const VectorSearchParams& params = {}) const;
    std::vector<VectorRecord> search(const std::vector<float>& query_embedding,
                                     const VectorSearchParams& params = {}) const;

    std::optional<VectorRecord> getVector(const std::string& chunk_id) const;
    std::map<std::string, VectorRecord>
    getVectorsBatch(const std::vector<std::string>& chunk_ids) const;
    std::vector<VectorRecord> getVectorsByDocument(const std::string& document_hash) const;
    std::unordered_map<std::string, VectorRecord> getDocumentLevelVectorsAll() const;
    Result<size_t>
    forEachDocumentLevelVector(const std::function<bool(VectorRecord&&)>& visitor) const;
    bool hasEmbedding(const std::string& document_hash) const;
    std::unordered_set<std::string> getEmbeddedDocumentHashes() const;

    // Result-returning transition APIs for callers that need structured errors
    // instead of bool/empty-result side channels.
    Result<void> initializeChecked();
    Result<void> createTableChecked();
    Result<void> insertVectorChecked(const VectorRecord& record);
    Result<void> insertVectorsBatchChecked(const std::vector<VectorRecord>& records);
    Result<void> updateVectorChecked(const std::string& chunk_id, const VectorRecord& record);
    Result<void> deleteVectorChecked(const std::string& chunk_id);
    Result<void> deleteVectorsByDocumentChecked(const std::string& document_hash);
    Result<std::vector<VectorRecord>>
    searchSimilarChecked(const std::vector<float>& query_embedding,
                         const VectorSearchParams& params = {}) const;
    Result<std::vector<std::vector<VectorRecord>>>
    searchSimilarBatchChecked(const std::vector<std::vector<float>>& query_embeddings,
                              const VectorSearchParams& params = {}, size_t num_threads = 0) const;
    Result<bool> hasEmbeddingChecked(const std::string& document_hash) const;
    Result<std::unordered_set<std::string>> getEmbeddedDocumentHashesChecked() const;

    bool buildIndex();
    bool prepareSearchIndex();
    bool hasReusablePersistedSearchIndex() const;
    bool optimizeIndex();
    bool persistIndex();
    void compactDatabase();
    bool rebuildIndex();
    bool beginBulkLoad();
    bool finalizeBulkLoad();

    Result<void> buildIndexChecked();
    Result<void> prepareSearchIndexChecked();
    Result<void> optimizeIndexChecked();
    Result<void> persistIndexChecked();
    Result<void> beginBulkLoadChecked();
    Result<void> finalizeBulkLoadChecked();

    Result<void> checkpointWal();

    DatabaseStats getStats() const;

    struct OrphanCleanupStats {
        size_t metadata_removed = 0;
        size_t embeddings_removed = 0;
        size_t metadata_backfilled = 0;
    };

    Result<OrphanCleanupStats> cleanupOrphanRows();

    Result<void> updateEmbeddings(const std::vector<VectorRecord>& records);
    Result<std::vector<std::string>> getStaleEmbeddings(const std::string& model_id,
                                                        const std::string& model_version);
    Result<std::vector<VectorRecord>> getEmbeddingsByVersion(const std::string& model_version,
                                                             size_t limit = 1000);
    Result<void> markAsStale(const std::string& chunk_id);
    // Soft-delete lifecycle is not yet modeled in the current vectors schema; this API returns
    // NotSupported until deleted-state columns are introduced.
    Result<void> markAsDeleted(const std::string& chunk_id);
    // Companion to markAsDeleted(); currently returns NotSupported for the same reason.
    Result<size_t> purgeDeleted(std::chrono::hours age_threshold);

    Result<void> insertEntityVector(const EntityVectorRecord& record);
    Result<void> insertEntityVectorsBatch(const std::vector<EntityVectorRecord>& records);
    Result<void> updateEntityVector(const std::string& node_key,
                                    [[maybe_unused]] EntityEmbeddingType type,
                                    const EntityVectorRecord& record);
    Result<void> deleteEntityVectorsByNode(const std::string& node_key);
    Result<void> deleteEntityVectorsByDocument(const std::string& document_hash);
    std::vector<EntityVectorRecord> searchEntities(const std::vector<float>& query_embedding,
                                                   const EntitySearchParams& params = {}) const;
    std::vector<EntityVectorRecord> getEntityVectorsByNode(const std::string& node_key) const;
    std::vector<EntityVectorRecord>
    getEntityVectorsByDocument(const std::string& document_hash) const;
    bool hasEntityEmbedding(const std::string& node_key) const;
    size_t getEntityVectorCount() const;
    Result<std::vector<EntityVectorRecord>>
    searchEntitiesChecked(const std::vector<float>& query_embedding,
                          const EntitySearchParams& params = {}) const;
    Result<std::vector<EntityVectorRecord>>
    getEntityVectorsByNodeChecked(const std::string& node_key) const;
    Result<std::vector<EntityVectorRecord>>
    getEntityVectorsByDocumentChecked(const std::string& document_hash) const;
    Result<bool> hasEntityEmbeddingChecked(const std::string& node_key) const;
    Result<size_t> getEntityVectorCountChecked() const;
    Result<void> markEntityAsStale(const std::string& node_key);

    const VectorDatabaseConfig& getConfig() const;
    std::string getLastError() const;
    bool hasError() const;

    static bool isValidEmbedding(const std::vector<float>& embedding, size_t expected_dim);
    static double computeCosineSimilarity(const std::vector<float>& a, const std::vector<float>& b);

    Result<size_t>
    fitAndPersistPerCoordScales(const std::vector<std::vector<float>>& training_vectors);

private:
    class Impl;
    std::unique_ptr<Impl> pImpl;
};

std::unique_ptr<VectorDatabase> createVectorDatabase(const VectorDatabaseConfig& config = {});

namespace utils {

std::vector<float> normalizeVector(const std::vector<float>& vec);
std::string generateChunkId(const std::string& document_hash, size_t chunk_index);
bool validateVectorRecord(const VectorRecord& record, size_t expected_dim);
double similarityToDistance(double similarity);
double distanceToSimilarity(double distance);

} // namespace utils

} // namespace yams::vector
