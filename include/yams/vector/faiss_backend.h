#pragma once

#include <yams/core/types.h>
#include <yams/vector/vector_backend.h>
#include <yams/vector/vector_types.h>

#include <atomic>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace yams::vector {

struct FaissIndex;

struct FaissBackendConfig {
    size_t embeddingDim{1024};
    size_t hnswM{16};
    size_t hnswEfConstruction{200};
    size_t hnswEfSearch{64};
    size_t maxElements{0};
};

class FaissBackend final : public IVectorBackend {
public:
    explicit FaissBackend(const FaissBackendConfig& config);
    ~FaissBackend() override;

    FaissBackend(const FaissBackend&) = delete;
    FaissBackend& operator=(const FaissBackend&) = delete;
    FaissBackend(FaissBackend&&) = delete;
    FaissBackend& operator=(FaissBackend&&) = delete;

    // ── Core lifecycle ──
    Result<void> initialize(const std::string& dbPath) override;
    void close() override;
    bool isInitialized() const override;

    // ── Schema ──
    Result<void> createTables(size_t embeddingDim) override;
    bool tablesExist() const override;

    // ── Insert / Update / Delete ──
    Result<void> insertVector(const VectorRecord& record) override;
    Result<void> insertVectorsBatch(const std::vector<VectorRecord>& records) override;
    Result<void> updateVector(const std::string& chunkId, const VectorRecord& record) override;
    Result<void> deleteVector(const std::string& chunkId) override;
    Result<void> deleteVectorsByDocument(const std::string& documentHash) override;

    // ── Search ──
    Result<std::vector<VectorRecord>>
    searchSimilar(const std::vector<float>& queryEmbedding, size_t k,
                  float similarityThreshold = 0.0f,
                  const std::optional<std::string>& documentHash = std::nullopt,
                  const std::unordered_set<std::string>& candidateHashes = {},
                  const std::map<std::string, std::string>& metadataFilters = {}) override;

    Result<std::vector<std::vector<VectorRecord>>>
    searchSimilarBatch(const std::vector<std::vector<float>>& queryEmbeddings, size_t k,
                       float similarityThreshold = 0.0f, size_t numThreads = 0) override;

    // ── Lookup ──
    Result<std::optional<VectorRecord>> getVector(const std::string& chunkId) override;
    Result<std::map<std::string, VectorRecord>>
    getVectorsBatch(const std::vector<std::string>& chunkIds) override;
    Result<std::vector<VectorRecord>>
    getVectorsByDocument(const std::string& documentHash) override;

    // ── Bulk / streaming ──
    Result<std::unordered_map<std::string, VectorRecord>> getDocumentLevelVectorsAll() override;
    Result<size_t>
    forEachDocumentLevelVector(const std::function<bool(VectorRecord&&)>& visitor) override;

    // ── Count / stats ──
    Result<size_t> getVectorCount() override;
    Result<VectorDatabaseStats> getStats() override;

    // ── Embedding checks ──
    Result<bool> hasEmbedding(const std::string& documentHash) override;
    Result<std::unordered_set<std::string>> getEmbeddedDocumentHashes() override;

    // ── Index lifecycle ──
    Result<void> buildIndex() override;
    Result<void> prepareSearchIndex() override;
    Result<bool> hasReusablePersistedSearchIndex() override;
    Result<void> optimize() override;
    Result<void> persistIndex() override;

    // ── Entity vectors (UNSUPPORTED — return error) ──
    Result<void> insertEntityVector(const EntityVectorRecord& record) override;
    Result<void> insertEntityVectorsBatch(const std::vector<EntityVectorRecord>& records) override;
    Result<void> deleteEntityVectorsByNode(const std::string& nodeKey) override;
    Result<void> deleteEntityVectorsByDocument(const std::string& documentHash) override;
    Result<std::vector<EntityVectorRecord>>
    searchEntities(const std::vector<float>& queryEmbedding,
                   const EntitySearchParams& params = {}) override;
    Result<std::vector<EntityVectorRecord>>
    getEntityVectorsByNode(const std::string& nodeKey) override;
    Result<std::vector<EntityVectorRecord>>
    getEntityVectorsByDocument(const std::string& documentHash) override;
    Result<bool> hasEntityEmbedding(const std::string& nodeKey) override;
    Result<size_t> getEntityVectorCount() override;
    Result<void> markEntityAsStale(const std::string& nodeKey) override;

    // ── Transactions (UNSUPPORTED) ──
    Result<void> beginTransaction() override;
    Result<void> commitTransaction() override;
    Result<void> rollbackTransaction() override;

    // ── TurboQuant (UNSUPPORTED) ──
    Result<void> persistTurboQuantPerCoordScales(size_t dim, uint8_t bits, uint64_t seed,
                                                 const std::vector<float>& scales) override;
    Result<void> persistTurboQuantFittedModel(size_t dim, uint8_t bits, uint64_t seed,
                                              const std::vector<float>& scales,
                                              const std::vector<float>& centroids) override;

private:
    void ensureIndexReady();
    size_t nextId();
    std::filesystem::path indexFilePath() const;
    Result<void> loadOrCreateIndex();
    Result<void> saveIndex();
    void reconstructEmbedding(VectorRecord& rec, size_t recIdx) const;

    FaissBackendConfig config_;
    std::filesystem::path dbPath_;
    std::unique_ptr<FaissIndex> index_;
    std::vector<VectorRecord> records_;
    std::unordered_map<std::string, size_t> chunkToIdx_;
    std::unordered_map<std::string, std::vector<size_t>> docToIdxs_;
    std::atomic<size_t> nextId_{1};
    std::atomic<bool> initialized_{false};
    mutable std::mutex mutex_;
};

} // namespace yams::vector
