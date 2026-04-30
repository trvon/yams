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
struct DeleteEdgesForSourceFileOp {
    std::string sourceFile;
};
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

using WriteOp = std::variant<UpsertNodesOp, AddEdgesOp, AddDeferredEdgesOp, AddAliasesOp,
                             AddDocEntitiesOp, AddDeferredDocEntitiesOp, UpsertSymbolMetadataOp,
                             DeleteDocEntitiesForDocumentOp, DeleteEdgesForSourceFileOp,
                             InsertDocumentOp, UpdateRepairStatusOp, UpsertTreeSnapshotOp>;

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

class WriteCoordinator {
public:
    struct Config {
        std::size_t maxBatchSize = 50;
        std::chrono::milliseconds maxBatchDelayMs{100};
        std::size_t channelCapacity = 1000;
    };

    struct Stats {
        std::uint64_t batchesEnqueued = 0;
        std::uint64_t batchesCommitted = 0;
        std::uint64_t opsApplied = 0;
        std::uint64_t commitErrors = 0;
        std::uint64_t documentsInserted = 0;
        std::uint64_t repairStatusesUpdated = 0;
        std::uint64_t treeSnapshotsWritten = 0;
        std::uint64_t nodesUpserted = 0;
        std::uint64_t edgesAdded = 0;
        std::uint64_t aliasesAdded = 0;
        std::uint64_t docEntitiesAdded = 0;
        std::uint64_t symbolsUpserted = 0;
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
    Result<void> applyOp(metadata::KnowledgeGraphStore::WriteBatch& kgBatch, AddAliasesOp& op);
    Result<void> applyOp(metadata::KnowledgeGraphStore::WriteBatch& kgBatch, AddDocEntitiesOp& op);
    Result<void> applyOp(metadata::KnowledgeGraphStore::WriteBatch& kgBatch,
                         AddDeferredDocEntitiesOp& op,
                         const std::unordered_map<std::string, std::int64_t>& nodeKeyToId);
    Result<void> applyOp(metadata::KnowledgeGraphStore::WriteBatch& kgBatch,
                         UpsertSymbolMetadataOp& op);
    Result<void> applyOp(metadata::KnowledgeGraphStore::WriteBatch& kgBatch,
                         DeleteDocEntitiesForDocumentOp& op);
    Result<void> applyOp(metadata::KnowledgeGraphStore::WriteBatch& kgBatch,
                         DeleteEdgesForSourceFileOp& op);
    Result<void> applyMetadataOp(InsertDocumentOp& op);
    Result<void> applyMetadataOp(UpdateRepairStatusOp& op);
    Result<void> applyMetadataOp(UpsertTreeSnapshotOp& op);

    boost::asio::strand<boost::asio::io_context::executor_type> strand_;
    std::shared_ptr<metadata::KnowledgeGraphStore> kg_;
    std::shared_ptr<metadata::MetadataRepository> meta_;
    Config config_;

    mutable std::mutex queueMutex_;
    mutable std::condition_variable drainCv_;
    std::vector<std::unique_ptr<WriteBatch>> pendingBatches_;

    std::atomic<bool> stop_{false};
    std::atomic<std::size_t> inFlight_{0};

    mutable std::mutex statsMutex_;
    Stats stats_;
};

} // namespace yams::daemon
