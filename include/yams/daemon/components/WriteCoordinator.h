#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>

#include <yams/core/types.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>

namespace yams::metadata {
class MetadataRepository;
} // namespace yams::metadata

namespace yams::daemon {

struct DeferredEdgeOp {
    std::string srcNodeKey;
    std::string dstNodeKey;
    std::string relation;
    float weight = 1.0f;
    std::optional<std::string> properties;
};

struct DeferredDocEntityOp {
    std::int64_t documentId;
    std::string entityText;
    std::string nodeKey;
    std::int64_t startOffset = 0;
    std::int64_t endOffset = 0;
    float confidence = 1.0f;
    std::string extractor;
};

// Transitional builder types used by daemon producers while all write execution is centralized
// through WriteCoordinator. These mirror the old KG-only deferred batch shape, but they are only
// intent containers; enqueueing happens through WriteBatch/WriteCoordinator.
using DeferredEdge = DeferredEdgeOp;
using DeferredDocEntity = DeferredDocEntityOp;

struct DeferredKGBatch {
    std::string sourceFile;
    std::vector<metadata::KGNode> nodes;
    std::vector<DeferredEdge> deferredEdges;
    std::vector<metadata::KGEdge> edges;
    std::vector<metadata::KGAlias> aliases;
    std::vector<DeferredDocEntity> deferredDocEntities;
    std::vector<metadata::DocEntity> docEntities;
    std::vector<metadata::SymbolMetadata> symbolMetadata;
    std::optional<std::int64_t> documentIdToDelete;
    std::optional<std::string> sourceFileToDelete;

    DeferredKGBatch() = default;
    DeferredKGBatch(DeferredKGBatch&&) = default;
    DeferredKGBatch& operator=(DeferredKGBatch&&) = default;
    DeferredKGBatch(const DeferredKGBatch&) = delete;
    DeferredKGBatch& operator=(const DeferredKGBatch&) = delete;
};

struct UpsertNodesOp {
    std::vector<metadata::KGNode> nodes;
};
struct AddEdgesOp {
    std::vector<metadata::KGEdge> edges;
    bool unique = true;
};
struct AddDeferredEdgesOp {
    std::vector<DeferredEdgeOp> edges;
};
struct AddAliasesOp {
    std::vector<metadata::KGAlias> aliases;
};
struct AddDocEntitiesOp {
    std::vector<metadata::DocEntity> entities;
};
struct AddDeferredDocEntitiesOp {
    std::vector<DeferredDocEntityOp> entities;
};
struct UpsertSymbolMetadataOp {
    std::vector<metadata::SymbolMetadata> symbols;
};
struct DeleteDocEntitiesForDocumentOp {
    std::int64_t documentId;
};
struct DeleteNodeByIdOp {
    std::int64_t nodeId;
};
struct DeleteNodesForDocumentHashOp {
    std::string documentHash;
};
struct DeleteEdgesForSourceFileOp {
    std::string sourceFile;
};
struct DeleteEdgesByRelationOp {
    std::string relation;
};
struct DeleteOrphanedEdgesOp {};
struct DeleteOrphanedDocEntitiesOp {};
struct InsertDocumentOp {
    metadata::DocumentInfo info;
    std::vector<std::pair<std::string, metadata::MetadataValue>> tags;
    std::optional<metadata::TreeSnapshotRecord> snapshot;
};
struct UpdateRepairStatusOp {
    std::vector<std::string> hashes;
    metadata::RepairStatus status;
};
struct UpsertTreeSnapshotOp {
    metadata::TreeSnapshotRecord record;
};
struct SetMetadataBatchOp {
    std::vector<std::tuple<std::int64_t, std::string, metadata::MetadataValue>> entries;
};
struct UpdateExtractionStatusOp {
    std::int64_t documentId;
    bool contentExtracted = false;
    metadata::ExtractionStatus status;
    std::string error;
};
struct UpdateEmbeddingStatusByHashOp {
    std::string hash;
    bool embedded = false;
    std::string modelName;
};
struct UpdateEmbeddingStatusByHashesOp {
    std::vector<std::string> hashes;
    bool embedded = false;
    std::string modelName;
};
struct UpsertSymbolExtractionStateOp {
    std::string documentHash;
    metadata::SymbolExtractionState state;
};
struct InsertRelationshipOp {
    metadata::DocumentRelationship relationship;
};

using WriteOp =
    std::variant<UpsertNodesOp, AddEdgesOp, AddDeferredEdgesOp, AddAliasesOp, AddDocEntitiesOp,
                 AddDeferredDocEntitiesOp, UpsertSymbolMetadataOp, DeleteDocEntitiesForDocumentOp,
                 DeleteNodeByIdOp, DeleteNodesForDocumentHashOp, DeleteEdgesForSourceFileOp,
                 DeleteEdgesByRelationOp, DeleteOrphanedEdgesOp, DeleteOrphanedDocEntitiesOp,
                 InsertDocumentOp, UpdateRepairStatusOp, UpsertTreeSnapshotOp, SetMetadataBatchOp,
                 UpdateExtractionStatusOp, UpdateEmbeddingStatusByHashOp,
                 UpdateEmbeddingStatusByHashesOp, UpsertSymbolExtractionStateOp,
                 InsertRelationshipOp>;

struct WriteBatch {
    std::string source;
    std::vector<WriteOp> ops;
    std::chrono::steady_clock::time_point enqueueTime{std::chrono::steady_clock::now()};

    WriteBatch() = default;
    WriteBatch(WriteBatch&&) = default;
    WriteBatch& operator=(WriteBatch&&) = default;
    WriteBatch(const WriteBatch&) = delete;
    WriteBatch& operator=(const WriteBatch&) = delete;
};

inline std::unique_ptr<WriteBatch>
makeWriteBatchFromDeferredKGBatch(std::unique_ptr<DeferredKGBatch> batch, std::string source) {
    if (!batch)
        return nullptr;
    auto wb = std::make_unique<WriteBatch>();
    wb->source = source.empty() ? ("DeferredKGBatch/" + batch->sourceFile) : std::move(source);
    if (!batch->nodes.empty())
        wb->ops.emplace_back(UpsertNodesOp{std::move(batch->nodes)});
    if (!batch->deferredEdges.empty())
        wb->ops.emplace_back(AddDeferredEdgesOp{std::move(batch->deferredEdges)});
    if (!batch->edges.empty())
        wb->ops.emplace_back(AddEdgesOp{std::move(batch->edges), true});
    if (!batch->aliases.empty())
        wb->ops.emplace_back(AddAliasesOp{std::move(batch->aliases)});
    if (!batch->deferredDocEntities.empty())
        wb->ops.emplace_back(AddDeferredDocEntitiesOp{std::move(batch->deferredDocEntities)});
    if (!batch->docEntities.empty())
        wb->ops.emplace_back(AddDocEntitiesOp{std::move(batch->docEntities)});
    if (!batch->symbolMetadata.empty())
        wb->ops.emplace_back(UpsertSymbolMetadataOp{std::move(batch->symbolMetadata)});
    if (batch->documentIdToDelete.has_value())
        wb->ops.emplace_back(DeleteDocEntitiesForDocumentOp{*batch->documentIdToDelete});
    if (batch->sourceFileToDelete.has_value())
        wb->ops.emplace_back(DeleteEdgesForSourceFileOp{std::move(*batch->sourceFileToDelete)});
    return wb;
}

class WriteCoordinator {
public:
    struct Config {
        std::size_t maxBatchSize = 50;
        std::chrono::milliseconds maxBatchDelayMs{100};
        std::size_t channelCapacity = 1000;
    };

    struct Stats {
        struct Hotspot {
            std::string source;
            std::uint64_t batches = 0;
            std::uint64_t ops = 0;
            std::uint64_t errors = 0;
            std::uint64_t totalQueueWaitMs = 0;
            std::uint64_t maxQueueWaitMs = 0;
            std::uint64_t maxExcessQueueWaitMs = 0;
            std::uint64_t totalApplyMs = 0;
            std::uint64_t maxApplyMs = 0;
        };

        std::uint64_t batchesEnqueued = 0;
        std::uint64_t batchesCommitted = 0;
        std::uint64_t opsApplied = 0;
        std::uint64_t commitErrors = 0;
        std::uint64_t documentsInserted = 0;
        std::uint64_t repairStatusesUpdated = 0;
        std::uint64_t treeSnapshotsWritten = 0;
        std::uint64_t metadataEntriesSet = 0;
        std::uint64_t extractionStatusesUpdated = 0;
        std::uint64_t embeddingStatusesUpdated = 0;
        std::uint64_t symbolExtractionStatesUpdated = 0;
        std::uint64_t relationshipsInserted = 0;
        std::uint64_t nodesUpserted = 0;
        std::uint64_t nodesDeleted = 0;
        std::uint64_t edgesAdded = 0;
        std::uint64_t aliasesAdded = 0;
        std::uint64_t docEntitiesAdded = 0;
        std::uint64_t symbolsUpserted = 0;
        std::uint64_t edgesDeleted = 0;
        std::uint64_t docEntitiesDeleted = 0;
        std::uint64_t maxBatchApplyMs = 0;
        std::uint64_t maxBatchQueueWaitMs = 0;
        std::uint64_t maxBatchExcessQueueWaitMs = 0;
        std::vector<Hotspot> hotSources;
    };

    WriteCoordinator(boost::asio::io_context& ioc,
                     std::shared_ptr<metadata::KnowledgeGraphStore> kgStore,
                     std::shared_ptr<metadata::MetadataRepository> metadataRepo, Config config);
    WriteCoordinator(boost::asio::io_context& ioc,
                     std::shared_ptr<metadata::KnowledgeGraphStore> kgStore,
                     std::shared_ptr<metadata::MetadataRepository> metadataRepo);
    ~WriteCoordinator();

    WriteCoordinator(const WriteCoordinator&) = delete;
    WriteCoordinator& operator=(const WriteCoordinator&) = delete;

    void enqueue(std::unique_ptr<WriteBatch> batch);

    bool tryEnqueue(std::unique_ptr<WriteBatch> batch);

    Result<void> flush(std::chrono::milliseconds timeout = std::chrono::seconds(60));

    void start();
    void shutdown();

    std::size_t queuedBatches() const;
    std::size_t inFlight() const { return inFlight_.load(); }
    Stats getStats() const;

private:
    boost::asio::awaitable<void> writerLoop();
    Result<void> applyBatches(std::vector<std::unique_ptr<WriteBatch>>& batches);
    Result<void> applyOp(metadata::KnowledgeGraphStore::WriteBatch& kgBatch, UpsertNodesOp& op,
                         std::unordered_map<std::string, std::int64_t>& nodeKeyToId);
    Result<void> applyOp(metadata::KnowledgeGraphStore::WriteBatch& kgBatch, AddEdgesOp& op);
    Result<void> applyOp(metadata::KnowledgeGraphStore::WriteBatch& kgBatch, AddDeferredEdgesOp& op,
                         const std::unordered_map<std::string, std::int64_t>& nodeKeyToId);
    Result<void> applyOp(metadata::KnowledgeGraphStore::WriteBatch& kgBatch, AddAliasesOp& op,
                         const std::unordered_map<std::string, std::int64_t>& nodeKeyToId);
    Result<void> applyOp(metadata::KnowledgeGraphStore::WriteBatch& kgBatch, AddDocEntitiesOp& op);
    Result<void> applyOp(metadata::KnowledgeGraphStore::WriteBatch& kgBatch,
                         AddDeferredDocEntitiesOp& op,
                         const std::unordered_map<std::string, std::int64_t>& nodeKeyToId);
    Result<void> applyOp(metadata::KnowledgeGraphStore::WriteBatch& kgBatch,
                         UpsertSymbolMetadataOp& op);
    Result<void> applyOp(metadata::KnowledgeGraphStore::WriteBatch& kgBatch,
                         DeleteDocEntitiesForDocumentOp& op);
    Result<void> applyOp(metadata::KnowledgeGraphStore::WriteBatch& kgBatch, DeleteNodeByIdOp& op);
    Result<void> applyOp(metadata::KnowledgeGraphStore::WriteBatch& kgBatch,
                         DeleteNodesForDocumentHashOp& op);
    Result<void> applyOp(metadata::KnowledgeGraphStore::WriteBatch& kgBatch,
                         DeleteEdgesForSourceFileOp& op);
    Result<void> applyOp(metadata::KnowledgeGraphStore::WriteBatch& kgBatch,
                         DeleteEdgesByRelationOp& op);
    Result<void> applyOp(metadata::KnowledgeGraphStore::WriteBatch& kgBatch,
                         DeleteOrphanedEdgesOp& op);
    Result<void> applyOp(metadata::KnowledgeGraphStore::WriteBatch& kgBatch,
                         DeleteOrphanedDocEntitiesOp& op);
    Result<void> applyMetadataOp(InsertDocumentOp& op);
    Result<void> applyMetadataOp(UpdateRepairStatusOp& op);
    Result<void> applyMetadataOp(UpsertTreeSnapshotOp& op);
    Result<void> applyMetadataOp(SetMetadataBatchOp& op);
    Result<void> applyMetadataOp(UpdateExtractionStatusOp& op);
    Result<void> applyMetadataOp(UpdateEmbeddingStatusByHashOp& op);
    Result<void> applyMetadataOp(UpdateEmbeddingStatusByHashesOp& op);
    Result<void> applyMetadataOp(UpsertSymbolExtractionStateOp& op);
    Result<void> applyMetadataOp(InsertRelationshipOp& op);

    void recordSourceQueueWait(const std::string& source, std::uint64_t queueWaitMs);
    void recordSourceApply(const std::string& source, std::uint64_t opCount, std::uint64_t applyMs,
                           bool error);

    struct SourceTiming {
        std::uint64_t batches = 0;
        std::uint64_t ops = 0;
        std::uint64_t errors = 0;
        std::uint64_t totalQueueWaitMs = 0;
        std::uint64_t maxQueueWaitMs = 0;
        std::uint64_t maxExcessQueueWaitMs = 0;
        std::uint64_t totalApplyMs = 0;
        std::uint64_t maxApplyMs = 0;
    };

    boost::asio::strand<boost::asio::io_context::executor_type> strand_;
    std::shared_ptr<metadata::KnowledgeGraphStore> kg_;
    std::shared_ptr<metadata::MetadataRepository> meta_;
    Config config_;

    mutable std::mutex queueMutex_;
    mutable std::condition_variable drainCv_;
    std::vector<std::unique_ptr<WriteBatch>> pendingBatches_;
    std::optional<Error> lastApplyError_;

    std::atomic<bool> stop_{false};
    std::atomic<std::size_t> inFlight_{0};

    mutable std::mutex statsMutex_;
    Stats stats_;
    std::unordered_map<std::string, SourceTiming> sourceTimings_;
};

} // namespace yams::daemon
