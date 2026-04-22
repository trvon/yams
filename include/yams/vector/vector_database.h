#pragma once

#include <yams/core/concepts.h>
#include <yams/core/types.h>
#include <yams/vector/vector_types.h>

#include <chrono>
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

    bool insertVector(const VectorRecord& record);
    bool insertVectorsBatch(const std::vector<VectorRecord>& records);
    bool updateVector(const std::string& chunk_id, const VectorRecord& record);
    bool deleteVector(const std::string& chunk_id);
    bool deleteVectorsByDocument(const std::string& document_hash);

    std::vector<VectorRecord> searchSimilar(const std::vector<float>& query_embedding,
                                            const VectorSearchParams& params = {}) const;
    std::vector<VectorRecord> searchSimilarToDocument(const std::string& document_hash,
                                                      const VectorSearchParams& params = {}) const;
    std::vector<VectorRecord> search(const std::vector<float>& query_embedding,
                                     const VectorSearchParams& params = {}) const;

    std::optional<VectorRecord> getVector(const std::string& chunk_id) const;
    std::map<std::string, VectorRecord>
    getVectorsBatch(const std::vector<std::string>& chunk_ids) const;
    std::vector<VectorRecord> getVectorsByDocument(const std::string& document_hash) const;
    std::unordered_map<std::string, VectorRecord> getDocumentLevelVectorsAll() const;
    bool hasEmbedding(const std::string& document_hash) const;
    std::unordered_set<std::string> getEmbeddedDocumentHashes() const;

    bool buildIndex();
    bool prepareSearchIndex();
    bool hasReusablePersistedSearchIndex() const;
    bool optimizeIndex();
    bool persistIndex();
    void compactDatabase();
    bool rebuildIndex();
    bool beginBulkLoad();
    bool finalizeBulkLoad();

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
    Result<void> markAsDeleted(const std::string& chunk_id);
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
