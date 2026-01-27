#include <yams/daemon/components/KGWriteQueue.h>
#include <yams/daemon/components/ResourceGovernor.h>
#include <yams/daemon/components/TuneAdvisor.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>

namespace yams::daemon {

// Hard cap on concurrent KG batch processing to prevent CPU spikes
static constexpr std::size_t kMaxKgBatchesPerIteration = 8;

KGWriteQueue::KGWriteQueue(boost::asio::io_context& ioc,
                           std::shared_ptr<metadata::KnowledgeGraphStore> kgStore, Config config)
    : strand_(boost::asio::make_strand(ioc)), kg_(std::move(kgStore)), config_(config) {
    spdlog::info("[KGWriteQueue] Initialized with maxBatchSize={}, maxDelayMs={}, capacity={}",
                 config_.maxBatchSize, config_.maxBatchDelayMs.count(), config_.channelCapacity);
}

KGWriteQueue::KGWriteQueue(boost::asio::io_context& ioc,
                           std::shared_ptr<metadata::KnowledgeGraphStore> kgStore)
    : KGWriteQueue(ioc, std::move(kgStore), Config{}) {}

KGWriteQueue::~KGWriteQueue() {
    shutdown();
}

std::future<Result<void>> KGWriteQueue::enqueue(std::unique_ptr<DeferredKGBatch> batch) {
    auto promise = std::make_shared<std::promise<Result<void>>>();
    batch->completionPromise = promise;
    auto future = promise->get_future();

    {
        std::lock_guard<std::mutex> lock(queueMutex_);

        // Backpressure: if queue is full, block or drop
        if (pendingBatches_.size() >= config_.channelCapacity) {
            spdlog::warn("[KGWriteQueue] Queue full ({} batches), applying backpressure",
                         pendingBatches_.size());
            // For now, still enqueue but log warning
        }

        pendingBatches_.push_back(std::move(batch));

        {
            std::lock_guard<std::mutex> slock(statsMutex_);
            stats_.batchesEnqueued++;
        }
    }

    // Note: writerLoop uses async polling, no need for condition_variable notify
    return future;
}

void KGWriteQueue::start() {
    stop_.store(false);
    boost::asio::co_spawn(strand_, writerLoop(), boost::asio::detached);
    spdlog::info("[KGWriteQueue] Writer coroutine started");
}

void KGWriteQueue::shutdown() {
    if (stop_.exchange(true)) {
        return; // Already stopped
    }

    spdlog::info("[KGWriteQueue] Shutting down...");
    // Note: writerLoop will exit on next poll when it sees stop_ = true

    // Wait for pending batches to drain (with timeout)
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (queuedBatches() > 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    if (queuedBatches() > 0) {
        spdlog::warn("[KGWriteQueue] Shutdown with {} batches still pending", queuedBatches());
    }

    spdlog::info("[KGWriteQueue] Shutdown complete");
}

std::size_t KGWriteQueue::queuedBatches() const {
    std::lock_guard<std::mutex> lock(queueMutex_);
    return pendingBatches_.size();
}

KGWriteQueue::Stats KGWriteQueue::getStats() const {
    std::lock_guard<std::mutex> lock(statsMutex_);
    return stats_;
}

boost::asio::awaitable<void> KGWriteQueue::writerLoop() {
    spdlog::info("[KGWriteQueue] Writer loop started");

    // Use async timer instead of blocking condition_variable
    // to avoid blocking io_context threads
    boost::asio::steady_timer pollTimer(co_await boost::asio::this_coro::executor);

    while (!stop_.load()) {
        std::vector<std::unique_ptr<DeferredKGBatch>> batchesToProcess;

        // Check for batches (non-blocking)
        {
            std::lock_guard<std::mutex> lock(queueMutex_);

            if (stop_.load() && pendingBatches_.empty()) {
                break;
            }

            // Determine effective batch limit based on resource pressure
            std::size_t effectiveMax = std::min(config_.maxBatchSize, kMaxKgBatchesPerIteration);
            // Halve batch size at Warning level for CPU-aware throttling
            if (ResourceGovernor::instance().getPressureLevel() == ResourcePressureLevel::Warning) {
                effectiveMax = std::max<std::size_t>(1, effectiveMax / 2);
            }

            // Drain up to effectiveMax batches
            std::size_t count = std::min(pendingBatches_.size(), effectiveMax);
            if (count > 0) {
                batchesToProcess.reserve(count);
                for (std::size_t i = 0; i < count; ++i) {
                    batchesToProcess.push_back(std::move(pendingBatches_[i]));
                }
                pendingBatches_.erase(pendingBatches_.begin(),
                                      pendingBatches_.begin() + static_cast<long>(count));
            }
        }

        if (batchesToProcess.empty()) {
            // No batches available - async wait before checking again
            // This yields the thread to other coroutines (non-blocking)
            pollTimer.expires_after(config_.maxBatchDelayMs);
            boost::system::error_code ec;
            co_await pollTimer.async_wait(
                boost::asio::redirect_error(boost::asio::use_awaitable, ec));
            // Ignore timer errors (e.g., cancelled on shutdown)
            continue;
        }

        inFlight_.store(batchesToProcess.size());
        spdlog::debug("[KGWriteQueue] Processing {} batches", batchesToProcess.size());

        // Apply all batches in a single transaction
        auto result = applyBatches(batchesToProcess);

        // Signal completion to all waiters
        for (auto& batch : batchesToProcess) {
            if (batch->completionPromise) {
                if (result) {
                    batch->completionPromise->set_value(Result<void>());
                } else {
                    batch->completionPromise->set_value(result.error());
                }
            }
        }

        inFlight_.store(0);

        // Brief yield to allow other coroutines to run
        pollTimer.expires_after(std::chrono::microseconds(100));
        boost::system::error_code ec;
        co_await pollTimer.async_wait(boost::asio::redirect_error(boost::asio::use_awaitable, ec));
    }

    spdlog::info("[KGWriteQueue] Writer loop exited");
}

Result<void> KGWriteQueue::applyBatches(std::vector<std::unique_ptr<DeferredKGBatch>>& batches) {
    if (batches.empty() || !kg_) {
        return Result<void>();
    }

    auto startTime = std::chrono::steady_clock::now();

    // Begin a single write batch for all documents
    auto batchResult = kg_->beginWriteBatch();
    if (!batchResult) {
        spdlog::error("[KGWriteQueue] Failed to begin write batch: {}",
                      batchResult.error().message);
        {
            std::lock_guard<std::mutex> lock(statsMutex_);
            stats_.commitErrors++;
        }
        TuneAdvisor::reportDbLockError();
        return batchResult.error();
    }

    auto& writeBatch = batchResult.value();

    std::size_t totalNodes = 0;
    std::size_t totalEdges = 0;
    std::size_t totalDocs = batches.size();

    // Apply each deferred batch
    for (auto& batch : batches) {
        // Handle deletions first
        if (batch->documentIdToDelete.has_value()) {
            auto delResult = writeBatch->deleteDocEntitiesForDocument(*batch->documentIdToDelete);
            if (!delResult) {
                spdlog::warn("[KGWriteQueue] Failed to delete doc entities for doc {}: {}",
                             *batch->documentIdToDelete, delResult.error().message);
                // Continue anyway
            }
        }

        if (batch->sourceFileToDelete.has_value()) {
            auto delResult = writeBatch->deleteEdgesForSourceFile(*batch->sourceFileToDelete);
            if (!delResult) {
                spdlog::warn("[KGWriteQueue] Failed to delete edges for {}: {}",
                             *batch->sourceFileToDelete, delResult.error().message);
                // Continue anyway
            }
        }

        // Upsert nodes and track IDs
        if (!batch->nodes.empty()) {
            auto nodesResult = writeBatch->upsertNodes(batch->nodes);
            if (!nodesResult) {
                spdlog::warn("[KGWriteQueue] Failed to upsert {} nodes for {}: {}",
                             batch->nodes.size(), batch->sourceFile, nodesResult.error().message);
                // Continue with other operations
            } else {
                // Map nodeKey to assigned ID
                const auto& ids = nodesResult.value();
                for (std::size_t i = 0; i < batch->nodes.size() && i < ids.size(); ++i) {
                    batch->nodeKeyToId[batch->nodes[i].nodeKey] = ids[i];
                }
                totalNodes += ids.size();
            }
        }

        // Resolve deferred edges (nodeKey → nodeId) and add them
        if (!batch->deferredEdges.empty()) {
            std::vector<metadata::KGEdge> resolvedEdges;
            resolvedEdges.reserve(batch->deferredEdges.size());

            for (const auto& de : batch->deferredEdges) {
                auto srcIt = batch->nodeKeyToId.find(de.srcNodeKey);
                auto dstIt = batch->nodeKeyToId.find(de.dstNodeKey);

                if (srcIt == batch->nodeKeyToId.end() || dstIt == batch->nodeKeyToId.end()) {
                    spdlog::debug("[KGWriteQueue] Skipping edge {}->{}: nodeKey not found",
                                  de.srcNodeKey.substr(0, 20), de.dstNodeKey.substr(0, 20));
                    continue;
                }

                metadata::KGEdge edge;
                edge.srcNodeId = srcIt->second;
                edge.dstNodeId = dstIt->second;
                edge.relation = de.relation;
                edge.weight = de.weight;
                edge.properties = de.properties;
                resolvedEdges.push_back(std::move(edge));
            }

            if (!resolvedEdges.empty()) {
                auto edgesResult = writeBatch->addEdgesUnique(resolvedEdges);
                if (!edgesResult) {
                    spdlog::warn("[KGWriteQueue] Failed to add {} deferred edges for {}: {}",
                                 resolvedEdges.size(), batch->sourceFile,
                                 edgesResult.error().message);
                } else {
                    totalEdges += resolvedEdges.size();
                }
            }
        }

        // Add pre-resolved edges (legacy path)
        if (!batch->edges.empty()) {
            auto edgesResult = writeBatch->addEdgesUnique(batch->edges);
            if (!edgesResult) {
                spdlog::warn("[KGWriteQueue] Failed to add {} edges for {}: {}",
                             batch->edges.size(), batch->sourceFile, edgesResult.error().message);
            } else {
                totalEdges += batch->edges.size();
            }
        }

        // Add aliases (resolve nodeId if encoded in source field)
        if (!batch->aliases.empty()) {
            std::vector<metadata::KGAlias> resolvedAliases;
            resolvedAliases.reserve(batch->aliases.size());

            for (auto alias : batch->aliases) {
                if (alias.nodeId == 0 && alias.source.has_value() && !alias.source->empty()) {
                    // Check if source contains nodeKey (format: "source_type|nodeKey")
                    const auto& srcStr = *alias.source;
                    auto pipePos = srcStr.find('|');
                    if (pipePos != std::string::npos) {
                        std::string nodeKey = srcStr.substr(pipePos + 1);
                        std::string realSource = srcStr.substr(0, pipePos);

                        auto nodeIt = batch->nodeKeyToId.find(nodeKey);
                        if (nodeIt != batch->nodeKeyToId.end()) {
                            alias.nodeId = nodeIt->second;
                            alias.source = realSource;
                            resolvedAliases.push_back(std::move(alias));
                        } else {
                            spdlog::debug("[KGWriteQueue] Skipping alias '{}': nodeKey not found",
                                          alias.alias.substr(0, 30));
                        }
                    } else {
                        // No nodeKey encoded, add as-is
                        resolvedAliases.push_back(std::move(alias));
                    }
                } else {
                    // nodeId already set or no source, add as-is
                    resolvedAliases.push_back(std::move(alias));
                }
            }

            if (!resolvedAliases.empty()) {
                auto aliasResult = writeBatch->addAliases(resolvedAliases);
                if (!aliasResult) {
                    spdlog::warn("[KGWriteQueue] Failed to add {} aliases for {}: {}",
                                 resolvedAliases.size(), batch->sourceFile,
                                 aliasResult.error().message);
                }
            }
        }

        // Resolve deferred doc entities (nodeKey → nodeId) and add them
        if (!batch->deferredDocEntities.empty()) {
            std::vector<metadata::DocEntity> resolvedDocEntities;
            resolvedDocEntities.reserve(batch->deferredDocEntities.size());

            for (const auto& dde : batch->deferredDocEntities) {
                auto nodeIt = batch->nodeKeyToId.find(dde.nodeKey);
                if (nodeIt == batch->nodeKeyToId.end()) {
                    spdlog::debug("[KGWriteQueue] Skipping doc entity for nodeKey {}: not found",
                                  dde.nodeKey.substr(0, 30));
                    continue;
                }

                metadata::DocEntity de;
                de.documentId = dde.documentId;
                de.entityText = dde.entityText;
                de.nodeId = nodeIt->second;
                de.startOffset = dde.startOffset;
                de.endOffset = dde.endOffset;
                de.confidence = dde.confidence;
                de.extractor = dde.extractor;
                resolvedDocEntities.push_back(std::move(de));
            }

            if (!resolvedDocEntities.empty()) {
                auto deResult = writeBatch->addDocEntities(resolvedDocEntities);
                if (!deResult) {
                    spdlog::warn("[KGWriteQueue] Failed to add {} deferred doc entities for {}: {}",
                                 resolvedDocEntities.size(), batch->sourceFile,
                                 deResult.error().message);
                }
            }
        }

        // Add pre-resolved doc entities (legacy path)
        if (!batch->docEntities.empty()) {
            auto deResult = writeBatch->addDocEntities(batch->docEntities);
            if (!deResult) {
                spdlog::warn("[KGWriteQueue] Failed to add {} doc entities for {}: {}",
                             batch->docEntities.size(), batch->sourceFile,
                             deResult.error().message);
            }
        }

        // Upsert symbol metadata
        if (!batch->symbolMetadata.empty()) {
            auto smResult = writeBatch->upsertSymbolMetadata(batch->symbolMetadata);
            if (!smResult) {
                spdlog::warn("[KGWriteQueue] Failed to upsert {} symbols for {}: {}",
                             batch->symbolMetadata.size(), batch->sourceFile,
                             smResult.error().message);
            }
        }
    }

    // Commit the entire batch
    auto commitResult = writeBatch->commit();
    if (!commitResult) {
        spdlog::error("[KGWriteQueue] Commit failed for {} batches: {}", batches.size(),
                      commitResult.error().message);
        {
            std::lock_guard<std::mutex> lock(statsMutex_);
            stats_.commitErrors++;
        }
        if (commitResult.error().message.find("database is locked") != std::string::npos) {
            TuneAdvisor::reportDbLockError();
        }
        return commitResult.error();
    }

    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - startTime);

    {
        std::lock_guard<std::mutex> lock(statsMutex_);
        stats_.batchesCommitted += batches.size();
        stats_.documentsProcessed += totalDocs;
        stats_.nodesInserted += totalNodes;
        stats_.edgesInserted += totalEdges;
    }

    spdlog::info("[KGWriteQueue] Committed {} batches ({} nodes, {} edges) in {}ms", totalDocs,
                 totalNodes, totalEdges, elapsed.count());

    return Result<void>();
}

} // namespace yams::daemon
