#include <yams/daemon/components/ResourceGovernor.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/TuningSnapshot.h>
#include <yams/daemon/components/WriteCoordinator.h>
#include <yams/metadata/metadata_repository.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <unordered_map>

namespace yams::daemon {

static constexpr std::size_t kMaxBatchesPerIteration = 8;

WriteCoordinator::WriteCoordinator(boost::asio::io_context& ioc,
                                   std::shared_ptr<metadata::KnowledgeGraphStore> kgStore,
                                   std::shared_ptr<metadata::MetadataRepository> metadataRepo,
                                   Config config)
    : strand_(boost::asio::make_strand(ioc)), kg_(std::move(kgStore)),
      meta_(std::move(metadataRepo)), config_(config) {
    spdlog::info("[WriteCoordinator] Initialized maxBatchSize={} maxDelayMs={} capacity={}",
                 config_.maxBatchSize, config_.maxBatchDelayMs.count(), config_.channelCapacity);
}

WriteCoordinator::WriteCoordinator(boost::asio::io_context& ioc,
                                   std::shared_ptr<metadata::KnowledgeGraphStore> kgStore,
                                   std::shared_ptr<metadata::MetadataRepository> metadataRepo)
    : WriteCoordinator(ioc, std::move(kgStore), std::move(metadataRepo), Config{}) {}

WriteCoordinator::~WriteCoordinator() {
    shutdown();
}

void WriteCoordinator::enqueue(std::unique_ptr<WriteBatch> batch) {
    if (!batch)
        return;
    batch->enqueueTime = std::chrono::steady_clock::now();
    {
        std::lock_guard<std::mutex> lock(queueMutex_);
        if (pendingBatches_.size() >= config_.channelCapacity) {
            spdlog::warn("[WriteCoordinator] Queue full ({} batches), applying backpressure",
                         pendingBatches_.size());
        }
        pendingBatches_.push_back(std::move(batch));
        std::lock_guard<std::mutex> slock(statsMutex_);
        stats_.batchesEnqueued++;
    }
}

void WriteCoordinator::start() {
    stop_.store(false);
    boost::asio::co_spawn(strand_, writerLoop(), boost::asio::detached);
    spdlog::info("[WriteCoordinator] Writer coroutine started");
}

Result<void> WriteCoordinator::flush(std::chrono::milliseconds timeout) {
    if (stop_.load()) {
        return Error{ErrorCode::InvalidState, "WriteCoordinator is shutting down"};
    }
    auto deadline = std::chrono::steady_clock::now() + timeout;
    std::unique_lock<std::mutex> lock(queueMutex_);
    bool drained = drainCv_.wait_until(
        lock, deadline, [this] { return pendingBatches_.empty() && inFlight_.load() == 0; });
    if (!drained) {
        return Error{ErrorCode::Timeout, "WriteCoordinator::flush timed out"};
    }
    return Result<void>();
}

void WriteCoordinator::shutdown() {
    if (stop_.exchange(true)) {
        return;
    }
    spdlog::info("[WriteCoordinator] Shutting down...");
    {
        std::lock_guard<std::mutex> lock(queueMutex_);
        drainCv_.notify_all();
    }
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    {
        std::unique_lock<std::mutex> lock(queueMutex_);
        drainCv_.wait_until(lock, deadline, [this]() { return pendingBatches_.empty(); });
    }
    if (queuedBatches() > 0) {
        spdlog::warn("[WriteCoordinator] Shutdown with {} batches still pending", queuedBatches());
    }
    spdlog::info("[WriteCoordinator] Shutdown complete");
}

std::size_t WriteCoordinator::queuedBatches() const {
    std::lock_guard<std::mutex> lock(queueMutex_);
    return pendingBatches_.size();
}

WriteCoordinator::Stats WriteCoordinator::getStats() const {
    std::lock_guard<std::mutex> lock(statsMutex_);
    return stats_;
}

boost::asio::awaitable<void> WriteCoordinator::writerLoop() {
    spdlog::info("[WriteCoordinator] Writer loop started");
    boost::asio::steady_timer pollTimer(co_await boost::asio::this_coro::executor);

    while (!stop_.load()) {
        std::vector<std::unique_ptr<WriteBatch>> batchesToProcess;
        {
            std::lock_guard<std::mutex> lock(queueMutex_);
            if (stop_.load() && pendingBatches_.empty())
                break;

            std::size_t effectiveMax = std::min(config_.maxBatchSize, kMaxBatchesPerIteration);
            if (ResourceGovernor::instance().getPressureLevel() == ResourcePressureLevel::Warning) {
                effectiveMax = std::max<std::size_t>(1, effectiveMax / 2);
            }
            std::size_t count = std::min(pendingBatches_.size(), effectiveMax);
            if (count > 0) {
                batchesToProcess.reserve(count);
                for (std::size_t i = 0; i < count; ++i)
                    batchesToProcess.push_back(std::move(pendingBatches_[i]));
                pendingBatches_.erase(pendingBatches_.begin(),
                                      pendingBatches_.begin() + static_cast<long>(count));
                if (pendingBatches_.empty())
                    drainCv_.notify_all();
            }
        }

        if (batchesToProcess.empty()) {
            auto idleDelay = config_.maxBatchDelayMs;
            if (auto snap = TuningSnapshotRegistry::instance().get(); snap && snap->daemonIdle) {
                idleDelay =
                    std::max(idleDelay, std::chrono::milliseconds(std::max<uint32_t>(
                                            TuneAdvisor::idleTickMs(), snap->workerPollMs)));
            }
            pollTimer.expires_after(idleDelay);
            boost::system::error_code ec;
            co_await pollTimer.async_wait(
                boost::asio::redirect_error(boost::asio::use_awaitable, ec));
            continue;
        }

        inFlight_.store(batchesToProcess.size());
        spdlog::debug("[WriteCoordinator] Processing {} batches", batchesToProcess.size());
        auto result = applyBatches(batchesToProcess);
        inFlight_.store(0);
        {
            std::lock_guard<std::mutex> lock(queueMutex_);
            drainCv_.notify_all();
        }
        if (!result) {
            spdlog::warn("[WriteCoordinator] Batch apply failed: {}", result.error().message);
        }

        pollTimer.expires_after(std::chrono::milliseconds(1));
        boost::system::error_code ec;
        co_await pollTimer.async_wait(boost::asio::redirect_error(boost::asio::use_awaitable, ec));
    }
    spdlog::info("[WriteCoordinator] Writer loop exited");
}

Result<void> WriteCoordinator::applyBatches(std::vector<std::unique_ptr<WriteBatch>>& batches) {
    if (batches.empty()) {
        return Result<void>();
    }

    bool hasKgOps = false;
    bool hasMetaOps = false;
    for (const auto& batch : batches) {
        for (const auto& op : batch->ops) {
            std::visit(
                [&](const auto& concrete) {
                    using T = std::decay_t<decltype(concrete)>;
                    if constexpr (std::is_same_v<T, InsertDocumentOp> ||
                                  std::is_same_v<T, UpdateRepairStatusOp> ||
                                  std::is_same_v<T, UpsertTreeSnapshotOp> ||
                                  std::is_same_v<T, SetMetadataBatchOp> ||
                                  std::is_same_v<T, UpdateExtractionStatusOp>) {
                        hasMetaOps = true;
                    } else {
                        hasKgOps = true;
                    }
                },
                op);
        }
    }

    std::unordered_map<std::string, std::int64_t> nodeKeyToId;

    if (hasKgOps && kg_) {
        auto batchResult = kg_->beginWriteBatch();
        if (!batchResult) {
            spdlog::error("[WriteCoordinator] beginWriteBatch failed: {}",
                          batchResult.error().message);
            std::lock_guard<std::mutex> lock(statsMutex_);
            stats_.commitErrors++;
            TuneAdvisor::reportDbLockError();
            return batchResult.error();
        }
        auto& kgBatch = *batchResult.value();

        for (auto& batch : batches) {
            for (auto& op : batch->ops) {
                std::visit(
                    [&](auto& concrete) -> void {
                        using T = std::decay_t<decltype(concrete)>;
                        Result<void> r;
                        if constexpr (std::is_same_v<T, InsertDocumentOp> ||
                                      std::is_same_v<T, UpdateRepairStatusOp> ||
                                      std::is_same_v<T, UpsertTreeSnapshotOp> ||
                                      std::is_same_v<T, SetMetadataBatchOp> ||
                                      std::is_same_v<T, UpdateExtractionStatusOp>) {
                            return;
                        } else if constexpr (std::is_same_v<T, AddDeferredEdgesOp>) {
                            r = applyOp(kgBatch, concrete, nodeKeyToId);
                        } else if constexpr (std::is_same_v<T, AddDeferredDocEntitiesOp>) {
                            r = applyOp(kgBatch, concrete, nodeKeyToId);
                        } else if constexpr (std::is_same_v<T, UpsertNodesOp>) {
                            r = applyOp(kgBatch, concrete, nodeKeyToId);
                        } else if constexpr (std::is_same_v<T, AddAliasesOp>) {
                            r = applyOp(kgBatch, concrete, nodeKeyToId);
                        } else {
                            r = applyOp(kgBatch, concrete);
                        }
                        if (!r) {
                            spdlog::warn("[WriteCoordinator] op '{}' failed: {}", batch->source,
                                         r.error().message);
                        } else {
                            std::lock_guard<std::mutex> lock(statsMutex_);
                            stats_.opsApplied++;
                        }
                    },
                    op);
            }
        }
        auto commitResult = kgBatch.commit();
        if (!commitResult) {
            spdlog::error("[WriteCoordinator] KG batch commit failed: {}",
                          commitResult.error().message);
            std::lock_guard<std::mutex> lock(statsMutex_);
            stats_.commitErrors++;
            TuneAdvisor::reportDbLockError();
            return commitResult.error();
        }
    }

    if (hasMetaOps && meta_) {
        for (auto& batch : batches) {
            for (auto& op : batch->ops) {
                std::visit(
                    [&](auto& concrete) -> void {
                        using T = std::decay_t<decltype(concrete)>;
                        Result<void> r;
                        if constexpr (std::is_same_v<T, InsertDocumentOp>) {
                            r = applyMetadataOp(concrete);
                        } else if constexpr (std::is_same_v<T, UpdateRepairStatusOp>) {
                            r = applyMetadataOp(concrete);
                        } else if constexpr (std::is_same_v<T, UpsertTreeSnapshotOp>) {
                            r = applyMetadataOp(concrete);
                        } else if constexpr (std::is_same_v<T, SetMetadataBatchOp>) {
                            r = applyMetadataOp(concrete);
                        } else if constexpr (std::is_same_v<T, UpdateExtractionStatusOp>) {
                            r = applyMetadataOp(concrete);
                        } else {
                            return;
                        }
                        if (!r) {
                            spdlog::warn("[WriteCoordinator] meta op '{}' failed: {}",
                                         batch->source, r.error().message);
                        } else {
                            std::lock_guard<std::mutex> lock(statsMutex_);
                            stats_.opsApplied++;
                        }
                    },
                    op);
            }
        }
    }

    {
        std::lock_guard<std::mutex> lock(statsMutex_);
        stats_.batchesCommitted += batches.size();
    }
    return Result<void>();
}

Result<void> WriteCoordinator::applyOp(metadata::KnowledgeGraphStore::WriteBatch& kgBatch,
                                       UpsertNodesOp& op,
                                       std::unordered_map<std::string, std::int64_t>& nodeKeyToId) {
    if (op.nodes.empty())
        return Result<void>();
    auto idsResult = kgBatch.upsertNodes(op.nodes);
    if (!idsResult)
        return idsResult.error();
    const auto& ids = idsResult.value();
    for (std::size_t i = 0; i < op.nodes.size() && i < ids.size(); ++i) {
        nodeKeyToId[op.nodes[i].nodeKey] = ids[i];
    }
    {
        std::lock_guard<std::mutex> lock(statsMutex_);
        stats_.nodesUpserted += ids.size();
    }
    return Result<void>();
}

Result<void> WriteCoordinator::applyOp(metadata::KnowledgeGraphStore::WriteBatch& kgBatch,
                                       AddEdgesOp& op) {
    if (op.edges.empty())
        return Result<void>();
    Result<void> r;
    if (op.unique) {
        r = kgBatch.addEdgesUnique(op.edges);
    } else {
        auto er = kgBatch.addEdge(op.edges.front());
        if (!er)
            r = er.error();
    }
    if (r) {
        std::lock_guard<std::mutex> lock(statsMutex_);
        stats_.edgesAdded += op.edges.size();
    }
    return r;
}

Result<void>
WriteCoordinator::applyOp(metadata::KnowledgeGraphStore::WriteBatch& kgBatch,
                          AddDeferredEdgesOp& op,
                          const std::unordered_map<std::string, std::int64_t>& nodeKeyToId) {
    if (op.edges.empty())
        return Result<void>();
    std::vector<metadata::KGEdge> resolved;
    resolved.reserve(op.edges.size());
    auto resolveKey = [&](const std::string& key) -> std::optional<std::int64_t> {
        auto it = nodeKeyToId.find(key);
        if (it != nodeKeyToId.end())
            return it->second;
        if (!kg_)
            return std::nullopt;
        auto nodeRes = kg_->getNodeByKey(key);
        if (nodeRes && nodeRes.value().has_value()) {
            return nodeRes.value()->id;
        }
        return std::nullopt;
    };
    for (const auto& deferred : op.edges) {
        auto src = resolveKey(deferred.srcNodeKey);
        auto dst = resolveKey(deferred.dstNodeKey);
        if (!src || !dst)
            continue;
        metadata::KGEdge edge;
        edge.srcNodeId = *src;
        edge.dstNodeId = *dst;
        edge.relation = deferred.relation;
        edge.weight = deferred.weight;
        edge.properties = deferred.properties;
        resolved.push_back(std::move(edge));
    }
    if (resolved.empty())
        return Result<void>();
    auto r = kgBatch.addEdgesUnique(resolved);
    if (r) {
        std::lock_guard<std::mutex> lock(statsMutex_);
        stats_.edgesAdded += resolved.size();
    }
    return r;
}

Result<void>
WriteCoordinator::applyOp(metadata::KnowledgeGraphStore::WriteBatch& kgBatch, AddAliasesOp& op,
                          const std::unordered_map<std::string, std::int64_t>& nodeKeyToId) {
    if (op.aliases.empty())
        return Result<void>();
    std::vector<metadata::KGAlias> resolved;
    resolved.reserve(op.aliases.size());
    for (auto& alias : op.aliases) {
        if (alias.nodeId == 0 && alias.source.has_value() && !alias.source->empty()) {
            const auto& srcStr = *alias.source;
            auto pipePos = srcStr.find('|');
            if (pipePos != std::string::npos) {
                std::string nodeKey = srcStr.substr(pipePos + 1);
                std::string realSource = srcStr.substr(0, pipePos);
                std::int64_t nodeId = 0;
                auto it = nodeKeyToId.find(nodeKey);
                if (it != nodeKeyToId.end()) {
                    nodeId = it->second;
                } else if (kg_) {
                    auto nodeRes = kg_->getNodeByKey(nodeKey);
                    if (nodeRes && nodeRes.value().has_value()) {
                        nodeId = nodeRes.value()->id;
                    }
                }
                if (nodeId == 0)
                    continue;
                alias.nodeId = nodeId;
                alias.source = std::move(realSource);
            }
        }
        resolved.push_back(std::move(alias));
    }
    if (resolved.empty())
        return Result<void>();
    auto r = kgBatch.addAliases(resolved);
    if (r) {
        std::lock_guard<std::mutex> lock(statsMutex_);
        stats_.aliasesAdded += resolved.size();
    }
    return r;
}

Result<void> WriteCoordinator::applyOp(metadata::KnowledgeGraphStore::WriteBatch& kgBatch,
                                       AddDocEntitiesOp& op) {
    if (op.entities.empty())
        return Result<void>();
    auto r = kgBatch.addDocEntities(op.entities);
    if (r) {
        std::lock_guard<std::mutex> lock(statsMutex_);
        stats_.docEntitiesAdded += op.entities.size();
    }
    return r;
}

Result<void>
WriteCoordinator::applyOp(metadata::KnowledgeGraphStore::WriteBatch& kgBatch,
                          AddDeferredDocEntitiesOp& op,
                          const std::unordered_map<std::string, std::int64_t>& nodeKeyToId) {
    if (op.entities.empty())
        return Result<void>();
    std::vector<metadata::DocEntity> resolved;
    resolved.reserve(op.entities.size());
    for (const auto& deferred : op.entities) {
        auto it = nodeKeyToId.find(deferred.nodeKey);
        std::int64_t nodeId = 0;
        if (it != nodeKeyToId.end()) {
            nodeId = it->second;
        } else if (kg_) {
            auto nodeRes = kg_->getNodeByKey(deferred.nodeKey);
            if (nodeRes && nodeRes.value().has_value()) {
                nodeId = nodeRes.value()->id;
            }
        }
        if (nodeId == 0)
            continue;
        metadata::DocEntity entity;
        entity.documentId = deferred.documentId;
        entity.nodeId = nodeId;
        entity.entityText = deferred.entityText;
        entity.startOffset = deferred.startOffset;
        entity.endOffset = deferred.endOffset;
        entity.confidence = deferred.confidence;
        entity.extractor = deferred.extractor;
        resolved.push_back(std::move(entity));
    }
    if (resolved.empty())
        return Result<void>();
    auto r = kgBatch.addDocEntities(resolved);
    if (r) {
        std::lock_guard<std::mutex> lock(statsMutex_);
        stats_.docEntitiesAdded += resolved.size();
    }
    return r;
}

Result<void> WriteCoordinator::applyOp(metadata::KnowledgeGraphStore::WriteBatch& kgBatch,
                                       UpsertSymbolMetadataOp& op) {
    if (op.symbols.empty())
        return Result<void>();
    auto r = kgBatch.upsertSymbolMetadata(op.symbols);
    if (r) {
        std::lock_guard<std::mutex> lock(statsMutex_);
        stats_.symbolsUpserted += op.symbols.size();
    }
    return r;
}

Result<void> WriteCoordinator::applyOp(metadata::KnowledgeGraphStore::WriteBatch& kgBatch,
                                       DeleteDocEntitiesForDocumentOp& op) {
    return kgBatch.deleteDocEntitiesForDocument(op.documentId);
}

Result<void> WriteCoordinator::applyOp(metadata::KnowledgeGraphStore::WriteBatch& kgBatch,
                                       DeleteEdgesForSourceFileOp& op) {
    auto r = kgBatch.deleteEdgesForSourceFile(op.sourceFile);
    if (!r)
        return r.error();
    return Result<void>();
}

Result<void> WriteCoordinator::applyMetadataOp(InsertDocumentOp& op) {
    if (!meta_)
        return Error{ErrorCode::InvalidState, "MetadataRepository unavailable"};
    metadata::TreeSnapshotRecord* snapshotPtr =
        op.snapshot.has_value() ? &op.snapshot.value() : nullptr;
    auto r = meta_->insertDocumentWithMetadata(op.info, op.tags, snapshotPtr);
    if (!r)
        return r.error();
    {
        std::lock_guard<std::mutex> lock(statsMutex_);
        stats_.documentsInserted++;
        if (op.snapshot.has_value())
            stats_.treeSnapshotsWritten++;
    }
    return Result<void>();
}

Result<void> WriteCoordinator::applyMetadataOp(UpdateRepairStatusOp& op) {
    if (!meta_)
        return Error{ErrorCode::InvalidState, "MetadataRepository unavailable"};
    if (op.hashes.empty())
        return Result<void>();
    auto r = meta_->batchUpdateDocumentRepairStatuses(op.hashes, op.status);
    if (r) {
        std::lock_guard<std::mutex> lock(statsMutex_);
        stats_.repairStatusesUpdated += op.hashes.size();
    }
    return r;
}

Result<void> WriteCoordinator::applyMetadataOp(UpsertTreeSnapshotOp& op) {
    if (!meta_)
        return Error{ErrorCode::InvalidState, "MetadataRepository unavailable"};
    auto r = meta_->upsertTreeSnapshot(op.record);
    if (r) {
        std::lock_guard<std::mutex> lock(statsMutex_);
        stats_.treeSnapshotsWritten++;
    }
    return r;
}

Result<void> WriteCoordinator::applyMetadataOp(SetMetadataBatchOp& op) {
    if (!meta_)
        return Error{ErrorCode::InvalidState, "MetadataRepository unavailable"};
    if (op.entries.empty())
        return Result<void>();
    auto r = meta_->setMetadataBatch(op.entries);
    if (r) {
        std::lock_guard<std::mutex> lock(statsMutex_);
        stats_.metadataEntriesSet += op.entries.size();
    }
    return r;
}

Result<void> WriteCoordinator::applyMetadataOp(UpdateExtractionStatusOp& op) {
    if (!meta_)
        return Error{ErrorCode::InvalidState, "MetadataRepository unavailable"};
    auto r = meta_->updateDocumentExtractionStatus(op.documentId, op.contentExtracted, op.status,
                                                   op.error);
    if (r) {
        std::lock_guard<std::mutex> lock(statsMutex_);
        stats_.extractionStatusesUpdated++;
    }
    return r;
}

} // namespace yams::daemon
