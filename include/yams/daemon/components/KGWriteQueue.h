#pragma once

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>
#include <spdlog/spdlog.h>

#include <yams/core/types.h>
#include <yams/metadata/knowledge_graph_store.h>

namespace yams::daemon {

class WorkCoordinator;

/**
 * DeferredEdge stores edge data with nodeKey references instead of IDs.
 * Node IDs are resolved when the batch is applied.
 */
struct DeferredEdge {
    std::string srcNodeKey; // Resolved to srcNodeId at apply time
    std::string dstNodeKey; // Resolved to dstNodeId at apply time
    std::string relation;
    float weight = 1.0f;
    std::optional<std::string> properties;
};

/**
 * DeferredDocEntity stores doc entity data with nodeKey reference instead of ID.
 * Node ID is resolved when the batch is applied.
 */
struct DeferredDocEntity {
    std::int64_t documentId;
    std::string entityText;
    std::string nodeKey; // Resolved to nodeId at apply time
    std::int64_t startOffset = 0;
    std::int64_t endOffset = 0;
    float confidence = 1.0f;
    std::string extractor;
};

/**
 * DeferredKGBatch captures all KG write operations for a single document.
 * These are accumulated and applied in batch to reduce lock contention.
 *
 * Edges use nodeKey references (not IDs) so they can be resolved when the
 * batch is committed, after all nodes have been upserted.
 */
struct DeferredKGBatch {
    // Source identifier for logging
    std::string sourceFile;

    // Operations to apply
    std::vector<metadata::KGNode> nodes;
    std::vector<DeferredEdge> deferredEdges; // Edges with nodeKey refs (preferred)
    std::vector<metadata::KGEdge> edges;     // Edges with pre-resolved IDs (legacy)
    std::vector<metadata::KGAlias> aliases;
    std::vector<DeferredDocEntity> deferredDocEntities; // DocEntities with nodeKey refs (preferred)
    std::vector<metadata::DocEntity> docEntities; // DocEntities with pre-resolved IDs (legacy)
    std::vector<metadata::SymbolMetadata> symbolMetadata;

    // Optional cleanup operations
    std::optional<std::int64_t> documentIdToDelete;
    std::optional<std::string> sourceFileToDelete;

    // Node ID mappings (populated after nodes are upserted)
    // Maps original nodeKey to assigned database ID
    std::unordered_map<std::string, std::int64_t> nodeKeyToId;

    // Promise for async completion notification
    std::shared_ptr<std::promise<Result<void>>> completionPromise;

    DeferredKGBatch() = default;
    DeferredKGBatch(DeferredKGBatch&&) = default;
    DeferredKGBatch& operator=(DeferredKGBatch&&) = default;

    // Non-copyable due to promise
    DeferredKGBatch(const DeferredKGBatch&) = delete;
    DeferredKGBatch& operator=(const DeferredKGBatch&) = delete;
};

/**
 * KGWriteQueue provides batched, serialized writes to the KnowledgeGraphStore.
 *
 * Instead of each document commit competing for DB locks, writes are queued
 * and committed in batches by a single writer coroutine. This eliminates
 * lock contention during high-throughput ingestion.
 *
 * Usage:
 *   auto batch = std::make_unique<DeferredKGBatch>();
 *   batch->nodes.push_back(...);
 *   batch->edges.push_back(...);
 *   auto future = queue.enqueue(std::move(batch));
 *   // Optionally wait: future.get();
 */
class KGWriteQueue {
public:
    struct Config {
        // Maximum batches to accumulate before forcing commit
        std::size_t maxBatchSize = 50;

        // Maximum time to wait for more batches before committing
        std::chrono::milliseconds maxBatchDelayMs{100};

        // Channel capacity (backpressure threshold)
        std::size_t channelCapacity = 1000;
    };

    // Constructor with explicit config
    KGWriteQueue(boost::asio::io_context& ioc,
                 std::shared_ptr<metadata::KnowledgeGraphStore> kgStore, Config config);

    // Constructor with default config
    KGWriteQueue(boost::asio::io_context& ioc,
                 std::shared_ptr<metadata::KnowledgeGraphStore> kgStore);

    ~KGWriteQueue();

    // Non-copyable, non-movable
    KGWriteQueue(const KGWriteQueue&) = delete;
    KGWriteQueue& operator=(const KGWriteQueue&) = delete;

    /**
     * Enqueue a deferred batch for writing.
     * Returns a future that resolves when the batch is committed.
     */
    std::future<Result<void>> enqueue(std::unique_ptr<DeferredKGBatch> batch);

    /**
     * Start the writer coroutine.
     */
    void start();

    /**
     * Signal shutdown and wait for pending writes to complete.
     */
    void shutdown();

    /**
     * Get approximate queue depth.
     */
    std::size_t queuedBatches() const;

    /**
     * Get number of batches currently being processed.
     */
    std::size_t inFlight() const { return inFlight_.load(); }

    /**
     * Statistics
     */
    struct Stats {
        std::uint64_t batchesEnqueued = 0;
        std::uint64_t batchesCommitted = 0;
        std::uint64_t documentsProcessed = 0;
        std::uint64_t nodesInserted = 0;
        std::uint64_t edgesInserted = 0;
        std::uint64_t commitErrors = 0;
    };
    Stats getStats() const;

private:
    boost::asio::awaitable<void> writerLoop();
    Result<void> applyBatches(std::vector<std::unique_ptr<DeferredKGBatch>>& batches);

    boost::asio::strand<boost::asio::io_context::executor_type> strand_;
    std::shared_ptr<metadata::KnowledgeGraphStore> kg_;
    Config config_;

    // Thread-safe queue using mutex + condition variable
    // (simpler than channel for this use case)
    mutable std::mutex queueMutex_;
    std::condition_variable queueCv_;
    std::vector<std::unique_ptr<DeferredKGBatch>> pendingBatches_;

    std::atomic<bool> stop_{false};
    std::atomic<std::size_t> inFlight_{0};

    // Statistics
    mutable std::mutex statsMutex_;
    Stats stats_;
};

} // namespace yams::daemon
