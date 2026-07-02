#include <yams/core/assert.hpp>
#include <yams/daemon/components/ResourceGovernor.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/TuningSnapshot.h>
#include <yams/daemon/components/WriteCoordinator.h>
#include <yams/metadata/kg_write_buffer.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/profiling.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <algorithm>
#include <unordered_map>
#include <unordered_set>

namespace yams::daemon {

// Keep each writer-loop turn bounded, but allow enough batches to coalesce noisy
// status-only producers before they churn the single SQLite writer.
static constexpr std::size_t kMaxBatchesPerIteration = 64;

WriteCoordinator::WriteCoordinator(boost::asio::io_context& ioc,
                                   std::shared_ptr<metadata::KnowledgeGraphStore> kgStore,
                                   std::shared_ptr<metadata::MetadataRepository> metadataRepo,
                                   Config config)
    : strand_(boost::asio::make_strand(ioc)), kg_(std::move(kgStore)),
      meta_(std::move(metadataRepo)), config_(config),
      wakeTimer_(std::make_shared<boost::asio::steady_timer>(strand_)) {
    YAMS_PRECONDITION(config_.maxBatchSize > 0, "WriteCoordinator maxBatchSize must be positive");
    YAMS_PRECONDITION(config_.channelCapacity > 0,
                      "WriteCoordinator channelCapacity must be positive");
    YAMS_PRECONDITION(config_.maxBatchDelayMs.count() >= 0,
                      "WriteCoordinator maxBatchDelayMs must be non-negative");
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
    YAMS_ZONE_SCOPED_N("WriteCoordinator::enqueue");
    if (!batch)
        return;
    batch->enqueueTime = std::chrono::steady_clock::now();
    {
        std::lock_guard<std::mutex> lock(queueMutex_);
        if (stop_.load(std::memory_order_acquire)) {
            spdlog::warn("[WriteCoordinator] Dropping batch from '{}' after shutdown requested",
                         batch->source);
            return;
        }
        if (pendingBatches_.size() >= config_.channelCapacity) {
            spdlog::warn("[WriteCoordinator] Queue full ({} batches), applying backpressure",
                         pendingBatches_.size());
        }
        pendingBatches_.push_back(std::move(batch));
        YAMS_PLOT("wc.queue.depth", static_cast<int64_t>(pendingBatches_.size()));
    }
    {
        std::lock_guard<std::mutex> lock(statsMutex_);
        stats_.batchesEnqueued++;
    }
    wakeWriter();
}

bool WriteCoordinator::tryEnqueue(std::unique_ptr<WriteBatch>& batch) {
    YAMS_ZONE_SCOPED_N("WriteCoordinator::tryEnqueue");
    if (!batch)
        return false;
    {
        std::lock_guard<std::mutex> lock(queueMutex_);
        if (stop_.load(std::memory_order_acquire) ||
            pendingBatches_.size() >= config_.channelCapacity) {
            return false;
        }
        batch->enqueueTime = std::chrono::steady_clock::now();
        pendingBatches_.push_back(std::move(batch));
        YAMS_PLOT("wc.queue.depth", static_cast<int64_t>(pendingBatches_.size()));
    }
    {
        std::lock_guard<std::mutex> lock(statsMutex_);
        stats_.batchesEnqueued++;
    }
    wakeWriter();
    return true;
}

void WriteCoordinator::wakeWriter() {
    auto timer = wakeTimer_;
    boost::asio::post(strand_, [timer]() {
        if (!timer) {
            return;
        }
        timer->cancel();
    });
}

void WriteCoordinator::start() {
    stop_.store(false);
    writerExited_.store(false, std::memory_order_release);
    boost::asio::co_spawn(strand_, writerLoop(), boost::asio::detached);
    spdlog::info("[WriteCoordinator] Writer coroutine started");
}

Result<void> WriteCoordinator::flush(std::chrono::milliseconds timeout) {
    YAMS_ZONE_SCOPED_N("WriteCoordinator::flush");
    if (stop_.load()) {
        return Error{ErrorCode::InvalidState, "WriteCoordinator is shutting down"};
    }
    auto deadline = std::chrono::steady_clock::now() + timeout;
    std::unique_lock<std::mutex> lock(queueMutex_);
    bool drained = drainCv_.wait_until(lock, deadline, [this] {
        return pendingBatches_.empty() && inFlight_.load(std::memory_order_acquire) == 0;
    });
    if (!drained) {
        return Error{ErrorCode::Timeout, "WriteCoordinator::flush timed out"};
    }
    if (lastApplyError_) {
        Error error = *lastApplyError_;
        lastApplyError_.reset();
        return error;
    }
    return Result<void>();
}

void WriteCoordinator::shutdown() {
    if (stop_.exchange(true)) {
        return;
    }
    spdlog::info("[WriteCoordinator] Shutting down...");
    wakeWriter();
    {
        std::lock_guard<std::mutex> lock(queueMutex_);
        drainCv_.notify_all();
    }
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    {
        std::unique_lock<std::mutex> lock(queueMutex_);
        drainCv_.wait_until(lock, deadline, [this]() {
            return pendingBatches_.empty() && inFlight_.load(std::memory_order_acquire) == 0 &&
                   writerExited_.load(std::memory_order_acquire);
        });
    }
    if (queuedBatches() > 0) {
        spdlog::warn("[WriteCoordinator] Shutdown with {} batches still pending", queuedBatches());
    }
    if (!writerExited_.load(std::memory_order_acquire)) {
        spdlog::warn("[WriteCoordinator] Shutdown timed out waiting for writer loop exit");
    }
    spdlog::info("[WriteCoordinator] Shutdown complete");
}

std::size_t WriteCoordinator::queuedBatches() const {
    std::lock_guard<std::mutex> lock(queueMutex_);
    return pendingBatches_.size();
}

WriteCoordinator::Stats WriteCoordinator::getStats() const {
    std::lock_guard<std::mutex> lock(statsMutex_);
    auto out = stats_;
    out.hotSources.clear();
    out.hotSources.reserve(std::min<std::size_t>(sourceTimings_.size(), 8));
    std::vector<Stats::Hotspot> all;
    all.reserve(sourceTimings_.size());
    for (const auto& [source, timing] : sourceTimings_) {
        all.push_back(Stats::Hotspot{.source = source,
                                     .batches = timing.batches,
                                     .ops = timing.ops,
                                     .errors = timing.errors,
                                     .totalQueueWaitMs = timing.totalQueueWaitMs,
                                     .maxQueueWaitMs = timing.maxQueueWaitMs,
                                     .maxExcessQueueWaitMs = timing.maxExcessQueueWaitMs,
                                     .totalApplyMs = timing.totalApplyMs,
                                     .maxApplyMs = timing.maxApplyMs});
    }
    std::sort(all.begin(), all.end(), [](const Stats::Hotspot& lhs, const Stats::Hotspot& rhs) {
        if (lhs.totalApplyMs != rhs.totalApplyMs)
            return lhs.totalApplyMs > rhs.totalApplyMs;
        return lhs.ops > rhs.ops;
    });
    if (all.size() > 8)
        all.resize(8);
    out.hotSources = std::move(all);
    return out;
}

void WriteCoordinator::recordSourceQueueWait(const std::string& source, std::uint64_t queueWaitMs) {
    std::lock_guard<std::mutex> lock(statsMutex_);
    auto& timing = sourceTimings_[source.empty() ? std::string{"<unknown>"} : source];
    const auto expectedBatchDelay =
        static_cast<std::uint64_t>(std::max<std::int64_t>(0, config_.maxBatchDelayMs.count()));
    const auto excessWaitMs =
        queueWaitMs > expectedBatchDelay ? queueWaitMs - expectedBatchDelay : 0;
    timing.batches++;
    timing.totalQueueWaitMs += queueWaitMs;
    timing.maxQueueWaitMs = std::max(timing.maxQueueWaitMs, queueWaitMs);
    timing.maxExcessQueueWaitMs = std::max(timing.maxExcessQueueWaitMs, excessWaitMs);
    stats_.maxBatchQueueWaitMs = std::max(stats_.maxBatchQueueWaitMs, queueWaitMs);
    stats_.maxBatchExcessQueueWaitMs = std::max(stats_.maxBatchExcessQueueWaitMs, excessWaitMs);
}

void WriteCoordinator::recordSourceApply(const std::string& source, std::uint64_t opCount,
                                         std::uint64_t applyMs, bool error) {
    std::lock_guard<std::mutex> lock(statsMutex_);
    auto& timing = sourceTimings_[source.empty() ? std::string{"<unknown>"} : source];
    timing.ops += opCount;
    timing.totalApplyMs += applyMs;
    timing.maxApplyMs = std::max(timing.maxApplyMs, applyMs);
    if (error)
        timing.errors++;
    stats_.maxBatchApplyMs = std::max(stats_.maxBatchApplyMs, applyMs);
}

boost::asio::awaitable<void> WriteCoordinator::writerLoop() {
    YAMS_SET_THREAD_NAME("yams-write-coordinator");
    spdlog::info("[WriteCoordinator] Writer loop started");
    auto pollTimer = wakeTimer_;

    while (true) {
        std::vector<std::unique_ptr<WriteBatch>> batchesToProcess;
        {
            std::lock_guard<std::mutex> lock(queueMutex_);
            if (stop_.load(std::memory_order_acquire) && pendingBatches_.empty() &&
                inFlight_.load(std::memory_order_acquire) == 0) {
                break;
            }

            std::size_t effectiveMax = std::min(config_.maxBatchSize, kMaxBatchesPerIteration);
            if (ResourceGovernor::instance().getPressureLevel() == ResourcePressureLevel::Warning) {
                effectiveMax = std::max<std::size_t>(1, effectiveMax / 2);
            }
            std::size_t count = std::min(pendingBatches_.size(), effectiveMax);
            if (count > 0) {
                YAMS_ZONE_SCOPED_N("WriteCoordinator::dequeue");
                YAMS_PLOT("wc.dequeue.batch_count", static_cast<int64_t>(count));
                YAMS_PLOT("wc.queue.depth_after_dequeue",
                          static_cast<int64_t>(pendingBatches_.size() - count));
                inFlight_.store(count, std::memory_order_release);
                batchesToProcess.reserve(count);
                for (std::size_t i = 0; i < count; ++i)
                    batchesToProcess.push_back(std::move(pendingBatches_[i]));
                pendingBatches_.erase(pendingBatches_.begin(),
                                      pendingBatches_.begin() + static_cast<long>(count));
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
            pollTimer->expires_after(idleDelay);
            boost::system::error_code ec;
            co_await pollTimer->async_wait(
                boost::asio::redirect_error(boost::asio::use_awaitable, ec));
            continue;
        }

        spdlog::debug("[WriteCoordinator] Processing {} batches", batchesToProcess.size());
        YAMS_PLOT("wc.in_flight.batches", static_cast<int64_t>(batchesToProcess.size()));
        auto result = applyBatches(batchesToProcess);
        if (!result) {
            {
                std::lock_guard<std::mutex> lock(queueMutex_);
                lastApplyError_ = result.error();
            }
            spdlog::warn("[WriteCoordinator] Batch apply failed: {}", result.error().message);
        }
        bool hasPendingAfterApply = false;
        {
            std::lock_guard<std::mutex> lock(queueMutex_);
            inFlight_.store(0, std::memory_order_release);
            hasPendingAfterApply = !pendingBatches_.empty();
            drainCv_.notify_all();
        }

        pollTimer->expires_after(hasPendingAfterApply ? std::chrono::milliseconds(0)
                                                      : std::chrono::milliseconds(1));
        boost::system::error_code ec;
        co_await pollTimer->async_wait(boost::asio::redirect_error(boost::asio::use_awaitable, ec));
    }
    writerExited_.store(true, std::memory_order_release);
    {
        std::lock_guard<std::mutex> lock(queueMutex_);
        drainCv_.notify_all();
    }
    spdlog::info("[WriteCoordinator] Writer loop exited");
}

Result<void> WriteCoordinator::applyBatches(std::vector<std::unique_ptr<WriteBatch>>& batches) {
    YAMS_ZONE_SCOPED_N("WriteCoordinator::applyBatches");
    YAMS_PLOT("wc.apply.batch_count", static_cast<int64_t>(batches.size()));
    if (batches.empty()) {
        return Result<void>();
    }

    const auto applyStart = std::chrono::steady_clock::now();

    for (const auto& batch : batches) {
        if (!batch)
            continue;
        const auto waitMs = static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(applyStart - batch->enqueueTime)
                .count());
        recordSourceQueueWait(batch->source, waitMs);
        YAMS_PLOT("wc.queue.wait_ms", static_cast<int64_t>(waitMs));
        if (waitMs >= 1000) {
            spdlog::warn("[WriteCoordinator] slow queue wait source='{}' wait_ms={} ops={}",
                         batch->source, waitMs, batch->ops.size());
        }
    }

    bool hasKgOps = false;
    bool hasMetaOps = false;
    for (const auto& batch : batches) {
        for (const auto& op : batch->ops) {
            std::visit(
                [&](const auto& concrete) {
                    using T = std::decay_t<decltype(concrete)>;
                    if constexpr (std::is_same_v<T, UpdateRepairStatusOp> ||
                                  std::is_same_v<T, UpsertTreeSnapshotOp> ||
                                  std::is_same_v<T, SetMetadataBatchOp> ||
                                  std::is_same_v<T, UpdateExtractionStatusOp> ||
                                  std::is_same_v<T, UpdateEmbeddingStatusByHashOp> ||
                                  std::is_same_v<T, UpdateEmbeddingStatusByHashesOp> ||
                                  std::is_same_v<T, UpsertSymbolExtractionStateOp> ||
                                  std::is_same_v<T, InsertRelationshipOp> ||
                                  std::is_same_v<T, AddSymSpellTermsOp>) {
                        hasMetaOps = true;
                    } else {
                        hasKgOps = true;
                    }
                },
                op);
        }
    }

    std::unordered_map<std::string, std::int64_t> nodeKeyToId;
    std::optional<Error> firstOpError;

    if (hasKgOps && kg_) {
        YAMS_ZONE_SCOPED_N("WriteCoordinator::applyKG");
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

        std::optional<metadata::KGWriteBuffer> edgeBuffer;
        if (config_.kgDedupEnabled) {
            prefetchDeferredNodeKeys(batches, nodeKeyToId);
            metadata::KGWriteBufferConfig bufferConfig;
            bufferConfig.maxDocs = 0;
            bufferConfig.maxEdges = 0;
            bufferConfig.maxEntities = 0;
            bufferConfig.autoFlush = false;
            edgeBuffer.emplace(*kg_, bufferConfig);
        }
        metadata::KGWriteBuffer* bufferPtr = edgeBuffer ? &*edgeBuffer : nullptr;

        const auto drainEdgeBuffer = [&]() -> Result<void> {
            if (!bufferPtr) {
                return Result<void>();
            }
            const auto added = bufferPtr->totalEdgesAdded();
            auto r = bufferPtr->flushInto(kgBatch);
            if (r) {
                const auto flushed = bufferPtr->totalEdgesFlushed();
                std::lock_guard<std::mutex> lock(statsMutex_);
                stats_.edgesAdded += flushed;
                stats_.edgesCoalesced += added - flushed;
            }
            bufferPtr->resetCounters();
            return r;
        };

        for (auto& batch : batches) {
            const auto sourceStart = std::chrono::steady_clock::now();
            std::uint64_t sourceOps = 0;
            bool sourceError = false;
            for (auto& op : batch->ops) {
                std::visit(
                    [&](auto& concrete) -> void {
                        using T = std::decay_t<decltype(concrete)>;
                        Result<void> r;
                        if constexpr (std::is_same_v<T, UpdateRepairStatusOp> ||
                                      std::is_same_v<T, UpsertTreeSnapshotOp> ||
                                      std::is_same_v<T, SetMetadataBatchOp> ||
                                      std::is_same_v<T, UpdateExtractionStatusOp> ||
                                      std::is_same_v<T, UpdateEmbeddingStatusByHashOp> ||
                                      std::is_same_v<T, UpdateEmbeddingStatusByHashesOp> ||
                                      std::is_same_v<T, UpsertSymbolExtractionStateOp> ||
                                      std::is_same_v<T, InsertRelationshipOp> ||
                                      std::is_same_v<T, AddSymSpellTermsOp>) {
                            return;
                        } else if constexpr (std::is_same_v<T, AddDeferredEdgesOp>) {
                            r = applyOp(kgBatch, concrete, nodeKeyToId, bufferPtr);
                        } else if constexpr (std::is_same_v<T, AddEdgesOp>) {
                            r = applyOp(kgBatch, concrete, bufferPtr);
                        } else if constexpr (std::is_same_v<T, AddDeferredDocEntitiesOp> ||
                                             std::is_same_v<T, UpsertNodesOp> ||
                                             std::is_same_v<T, AddAliasesOp>) {
                            r = applyOp(kgBatch, concrete, nodeKeyToId);
                        } else if constexpr (std::is_same_v<T, DeleteDocEntitiesForDocumentOp> ||
                                             std::is_same_v<T, DeleteNodeByIdOp> ||
                                             std::is_same_v<T, DeleteNodesForDocumentHashOp> ||
                                             std::is_same_v<T, DeleteEdgesForSourceFileOp> ||
                                             std::is_same_v<T, DeleteEdgesByRelationOp> ||
                                             std::is_same_v<T, DeleteOrphanedEdgesOp> ||
                                             std::is_same_v<T, DeleteOrphanedDocEntitiesOp>) {
                            r = drainEdgeBuffer();
                            if (r) {
                                r = applyOp(kgBatch, concrete);
                            }
                        } else {
                            r = applyOp(kgBatch, concrete);
                        }
                        if (r && bufferPtr && bufferPtr->edgeCount() >= config_.kgDedupMaxEdges) {
                            r = drainEdgeBuffer();
                        }
                        if (!r) {
                            if (!firstOpError) {
                                firstOpError = r.error();
                            }
                            sourceError = true;
                            spdlog::warn("[WriteCoordinator] op '{}' failed: {}", batch->source,
                                         r.error().message);
                        } else {
                            sourceOps++;
                        }
                    },
                    op);
            }
            if (sourceOps > 0) {
                std::lock_guard<std::mutex> lock(statsMutex_);
                stats_.opsApplied += sourceOps;
            }
            const auto sourceApplyMs =
                static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                               std::chrono::steady_clock::now() - sourceStart)
                                               .count());
            if (sourceOps > 0 || sourceError) {
                recordSourceApply(batch->source, sourceOps, sourceApplyMs, sourceError);
                if (sourceApplyMs >= 250) {
                    spdlog::warn("[WriteCoordinator] slow KG apply source='{}' apply_ms={} ops={} "
                                 "error={}",
                                 batch->source, sourceApplyMs, sourceOps, sourceError);
                }
            }
        }
        if (auto drained = drainEdgeBuffer(); !drained) {
            if (!firstOpError) {
                firstOpError = drained.error();
            }
            spdlog::warn("[WriteCoordinator] coalesced edge flush failed: {}",
                         drained.error().message);
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
        if (firstOpError) {
            return *firstOpError;
        }
    }

    if (hasMetaOps && meta_) {
        YAMS_ZONE_SCOPED_N("WriteCoordinator::applyMetadata");
        struct RepairStatusGroup {
            std::string source;
            metadata::RepairStatus status;
            std::vector<std::string> hashes;
        };
        struct EmbeddingStatusGroup {
            std::string source;
            bool embedded = false;
            std::string modelName;
            std::vector<std::string> hashes;
        };
        struct ExtractionStatusGroup {
            std::string source;
            std::vector<metadata::ExtractionStatusUpdate> updates;
        };

        std::unordered_map<
            std::string,
            std::vector<std::tuple<std::int64_t, std::string, metadata::MetadataValue>>>
            metadataBySource;
        std::unordered_map<std::string, RepairStatusGroup> repairStatusBySourceAndStatus;
        std::unordered_map<std::string, EmbeddingStatusGroup> embeddingStatusBySourceAndState;
        std::unordered_map<std::string, ExtractionStatusGroup> extractionStatusBySource;
        std::unordered_map<std::string, std::unordered_map<std::string, std::int64_t>>
            symspellTermsBySource;

        auto recordMetaApply = [&](const std::string& source, std::uint64_t sourceOps,
                                   std::uint64_t sourceApplyMs, bool sourceError) {
            if (sourceOps == 0 && !sourceError)
                return;
            recordSourceApply(source, sourceOps, sourceApplyMs, sourceError);
            if (sourceApplyMs >= 250) {
                spdlog::warn("[WriteCoordinator] slow metadata apply source='{}' apply_ms={} "
                             "ops={} error={}",
                             source, sourceApplyMs, sourceOps, sourceError);
            }
        };

        for (auto& batch : batches) {
            const auto sourceStart = std::chrono::steady_clock::now();
            std::uint64_t sourceOps = 0;
            bool sourceError = false;
            for (auto& op : batch->ops) {
                std::visit(
                    [&](auto& concrete) -> void {
                        using T = std::decay_t<decltype(concrete)>;
                        Result<void> r;
                        if constexpr (std::is_same_v<T, UpsertTreeSnapshotOp> ||
                                      std::is_same_v<T, InsertRelationshipOp> ||
                                      std::is_same_v<T, UpsertSymbolExtractionStateOp>) {
                            r = applyMetadataOp(concrete);
                        } else if constexpr (std::is_same_v<T, AddSymSpellTermsOp>) {
                            if (concrete.terms.empty())
                                return;
                            auto& termFreqs = symspellTermsBySource[batch->source];
                            for (auto& term : concrete.terms) {
                                if (!term.empty())
                                    termFreqs[std::move(term)]++;
                            }
                            concrete.terms.clear();
                            return;
                        } else if constexpr (std::is_same_v<T, UpdateRepairStatusOp>) {
                            if (concrete.hashes.empty())
                                return;
                            const auto key = batch->source + "\x1f" +
                                             std::to_string(static_cast<int>(concrete.status));
                            auto& group = repairStatusBySourceAndStatus[key];
                            if (group.source.empty()) {
                                group.source = batch->source;
                                group.status = concrete.status;
                            }
                            group.hashes.insert(group.hashes.end(),
                                                std::make_move_iterator(concrete.hashes.begin()),
                                                std::make_move_iterator(concrete.hashes.end()));
                            concrete.hashes.clear();
                            return;
                        } else if constexpr (std::is_same_v<T, SetMetadataBatchOp>) {
                            if (concrete.entries.empty())
                                return;
                            auto& entries = metadataBySource[batch->source];
                            entries.insert(entries.end(),
                                           std::make_move_iterator(concrete.entries.begin()),
                                           std::make_move_iterator(concrete.entries.end()));
                            concrete.entries.clear();
                            return;
                        } else if constexpr (std::is_same_v<T, UpdateExtractionStatusOp>) {
                            auto& group = extractionStatusBySource[batch->source];
                            if (group.source.empty()) {
                                group.source = batch->source;
                            }
                            group.updates.push_back(metadata::ExtractionStatusUpdate{
                                .documentId = concrete.documentId,
                                .contentExtracted = concrete.contentExtracted,
                                .status = concrete.status,
                                .error = std::move(concrete.error)});
                            return;
                        } else if constexpr (std::is_same_v<T, UpdateEmbeddingStatusByHashOp>) {
                            if (concrete.hash.empty())
                                return;
                            const auto key = batch->source + "\x1f" +
                                             (concrete.embedded ? "1" : "0") + "\x1f" +
                                             concrete.modelName;
                            auto& group = embeddingStatusBySourceAndState[key];
                            if (group.source.empty()) {
                                group.source = batch->source;
                                group.embedded = concrete.embedded;
                                group.modelName = concrete.modelName;
                            }
                            group.hashes.emplace_back(std::move(concrete.hash));
                            return;
                        } else if constexpr (std::is_same_v<T, UpdateEmbeddingStatusByHashesOp>) {
                            if (concrete.hashes.empty())
                                return;
                            const auto key = batch->source + "\x1f" +
                                             (concrete.embedded ? "1" : "0") + "\x1f" +
                                             concrete.modelName;
                            auto& group = embeddingStatusBySourceAndState[key];
                            if (group.source.empty()) {
                                group.source = batch->source;
                                group.embedded = concrete.embedded;
                                group.modelName = concrete.modelName;
                            }
                            group.hashes.insert(group.hashes.end(),
                                                std::make_move_iterator(concrete.hashes.begin()),
                                                std::make_move_iterator(concrete.hashes.end()));
                            concrete.hashes.clear();
                            return;
                        } else {
                            return;
                        }
                        if (!r) {
                            sourceError = true;
                            spdlog::warn("[WriteCoordinator] meta op '{}' failed: {}",
                                         batch->source, r.error().message);
                        } else {
                            sourceOps++;
                        }
                    },
                    op);
            }
            if (sourceOps > 0) {
                std::lock_guard<std::mutex> lock(statsMutex_);
                stats_.opsApplied += sourceOps;
            }
            const auto sourceApplyMs =
                static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                               std::chrono::steady_clock::now() - sourceStart)
                                               .count());
            recordMetaApply(batch->source, sourceOps, sourceApplyMs, sourceError);
        }

        const auto chunkMax = std::max<std::size_t>(1, config_.maxCoalescedOpsPerApply);

        for (auto& [_, group] : extractionStatusBySource) {
            for (std::size_t off = 0; off < group.updates.size(); off += chunkMax) {
                YAMS_ZONE_SCOPED_N("WriteCoordinator::coalesceExtractionStatus");
                const auto end = std::min(group.updates.size(), off + chunkMax);
                std::vector<metadata::ExtractionStatusUpdate> chunk(
                    std::make_move_iterator(group.updates.begin() + static_cast<long>(off)),
                    std::make_move_iterator(group.updates.begin() + static_cast<long>(end)));
                const auto sourceStart = std::chrono::steady_clock::now();
                const auto updateCount = static_cast<std::uint64_t>(chunk.size());
                YAMS_PLOT("wc.coalesce.extraction_status", static_cast<int64_t>(updateCount));
                metadata::MetadataOpScope opScope("wc_extraction_status_batch");
                auto r = meta_->batchUpdateDocumentExtractionStatuses(chunk);
                const auto sourceApplyMs = static_cast<std::uint64_t>(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - sourceStart)
                        .count());
                if (!r) {
                    spdlog::warn(
                        "[WriteCoordinator] coalesced extraction-status op '{}' failed: {}",
                        group.source, r.error().message);
                } else {
                    std::lock_guard<std::mutex> lock(statsMutex_);
                    stats_.opsApplied += updateCount;
                    stats_.extractionStatusesUpdated += updateCount;
                }
                recordMetaApply(group.source, updateCount, sourceApplyMs, !r);
            }
        }

        for (auto& [source, entries] : metadataBySource) {
            for (std::size_t off = 0; off < entries.size(); off += chunkMax) {
                YAMS_ZONE_SCOPED_N("WriteCoordinator::coalesceMetadata");
                const auto end = std::min(entries.size(), off + chunkMax);
                const auto sourceStart = std::chrono::steady_clock::now();
                SetMetadataBatchOp op{
                    std::vector<std::tuple<std::int64_t, std::string, metadata::MetadataValue>>(
                        std::make_move_iterator(entries.begin() + static_cast<long>(off)),
                        std::make_move_iterator(entries.begin() + static_cast<long>(end)))};
                const auto entryCount = static_cast<std::uint64_t>(op.entries.size());
                YAMS_PLOT("wc.coalesce.metadata_entries", static_cast<int64_t>(entryCount));
                auto r = applyMetadataOp(op);
                const auto sourceApplyMs = static_cast<std::uint64_t>(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - sourceStart)
                        .count());
                if (!r) {
                    spdlog::warn("[WriteCoordinator] coalesced meta op '{}' failed: {}", source,
                                 r.error().message);
                } else {
                    std::lock_guard<std::mutex> lock(statsMutex_);
                    stats_.opsApplied += entryCount;
                }
                recordMetaApply(source, entryCount, sourceApplyMs, !r);
            }
        }

        for (auto& [_, group] : repairStatusBySourceAndStatus) {
            for (std::size_t off = 0; off < group.hashes.size(); off += chunkMax) {
                YAMS_ZONE_SCOPED_N("WriteCoordinator::coalesceRepairStatus");
                const auto end = std::min(group.hashes.size(), off + chunkMax);
                const auto sourceStart = std::chrono::steady_clock::now();
                UpdateRepairStatusOp op{
                    std::vector<std::string>(
                        std::make_move_iterator(group.hashes.begin() + static_cast<long>(off)),
                        std::make_move_iterator(group.hashes.begin() + static_cast<long>(end))),
                    group.status};
                const auto hashCount = static_cast<std::uint64_t>(op.hashes.size());
                YAMS_PLOT("wc.coalesce.repair_status", static_cast<int64_t>(hashCount));
                auto r = applyMetadataOp(op);
                const auto sourceApplyMs = static_cast<std::uint64_t>(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - sourceStart)
                        .count());
                if (!r) {
                    spdlog::warn("[WriteCoordinator] coalesced repair-status op '{}' failed: {}",
                                 group.source, r.error().message);
                } else {
                    std::lock_guard<std::mutex> lock(statsMutex_);
                    stats_.opsApplied += hashCount;
                }
                recordMetaApply(group.source, hashCount, sourceApplyMs, !r);
            }
        }

        for (auto& [_, group] : embeddingStatusBySourceAndState) {
            for (std::size_t off = 0; off < group.hashes.size(); off += chunkMax) {
                YAMS_ZONE_SCOPED_N("WriteCoordinator::coalesceEmbeddingStatus");
                const auto end = std::min(group.hashes.size(), off + chunkMax);
                const auto sourceStart = std::chrono::steady_clock::now();
                UpdateEmbeddingStatusByHashesOp op{
                    std::vector<std::string>(
                        std::make_move_iterator(group.hashes.begin() + static_cast<long>(off)),
                        std::make_move_iterator(group.hashes.begin() + static_cast<long>(end))),
                    group.embedded, group.modelName};
                const auto hashCount = static_cast<std::uint64_t>(op.hashes.size());
                YAMS_PLOT("wc.coalesce.embedding_status", static_cast<int64_t>(hashCount));
                auto r = applyMetadataOp(op);
                const auto sourceApplyMs = static_cast<std::uint64_t>(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - sourceStart)
                        .count());
                if (!r) {
                    spdlog::warn("[WriteCoordinator] coalesced embedding-status op '{}' failed: {}",
                                 group.source, r.error().message);
                } else {
                    std::lock_guard<std::mutex> lock(statsMutex_);
                    stats_.opsApplied += hashCount;
                }
                recordMetaApply(group.source, hashCount, sourceApplyMs, !r);
            }
        }

        for (auto& [source, termFreqs] : symspellTermsBySource) {
            YAMS_ZONE_SCOPED_N("WriteCoordinator::coalesceSymSpell");
            if (termFreqs.empty()) {
                continue;
            }
            const auto sourceStart = std::chrono::steady_clock::now();
            std::vector<std::pair<std::string, std::int64_t>> termPairs;
            termPairs.reserve(termFreqs.size());
            std::uint64_t termAddCount = 0;
            for (auto& [term, freq] : termFreqs) {
                termPairs.emplace_back(term, freq);
                termAddCount += static_cast<std::uint64_t>(freq);
            }
            YAMS_PLOT("wc.coalesce.symspell_unique_terms", static_cast<int64_t>(termPairs.size()));
            YAMS_PLOT("wc.coalesce.symspell_total_terms", static_cast<int64_t>(termAddCount));
            Result<void> r;
            {
                metadata::MetadataOpScope opScope("wc_symspell_terms");
                r = meta_->tryAddSymSpellTerms(termPairs);
            }
            const auto sourceApplyMs =
                static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                               std::chrono::steady_clock::now() - sourceStart)
                                               .count());
            if (!r) {
                spdlog::warn("[WriteCoordinator] coalesced SymSpell op '{}' failed: {}", source,
                             r.error().message);
            } else {
                std::lock_guard<std::mutex> lock(statsMutex_);
                stats_.opsApplied += termAddCount;
                stats_.symSpellTermsAdded += termAddCount;
            }
            recordMetaApply(source, termAddCount, sourceApplyMs, !r);
        }
    }

    {
        std::lock_guard<std::mutex> lock(statsMutex_);
        stats_.batchesCommitted += batches.size();
    }
    return Result<void>();
}

void WriteCoordinator::prefetchDeferredNodeKeys(
    const std::vector<std::unique_ptr<WriteBatch>>& batches,
    std::unordered_map<std::string, std::int64_t>& nodeKeyToId) {
    if (!kg_) {
        return;
    }
    std::vector<std::string> keys;
    std::unordered_set<std::string> seen;
    const auto offer = [&](const std::string& key) {
        if (key.empty()) {
            return;
        }
        if (seen.insert(key).second) {
            keys.push_back(key);
        }
    };
    for (const auto& batch : batches) {
        if (!batch) {
            continue;
        }
        for (const auto& op : batch->ops) {
            std::visit(
                [&](const auto& concrete) {
                    using T = std::decay_t<decltype(concrete)>;
                    if constexpr (std::is_same_v<T, AddDeferredEdgesOp>) {
                        for (const auto& edge : concrete.edges) {
                            offer(edge.srcNodeKey);
                            offer(edge.dstNodeKey);
                        }
                    } else if constexpr (std::is_same_v<T, AddDeferredDocEntitiesOp>) {
                        for (const auto& entity : concrete.entities) {
                            offer(entity.nodeKey);
                        }
                    } else if constexpr (std::is_same_v<T, AddAliasesOp>) {
                        for (const auto& alias : concrete.aliases) {
                            if (alias.nodeId == 0 && alias.source.has_value()) {
                                auto pipePos = alias.source->find('|');
                                if (pipePos != std::string::npos) {
                                    offer(alias.source->substr(pipePos + 1));
                                }
                            }
                        }
                    }
                },
                op);
        }
    }
    if (keys.empty()) {
        return;
    }
    constexpr std::size_t kLookupChunk = 900;
    std::size_t resolvedCount = 0;
    for (std::size_t offset = 0; offset < keys.size(); offset += kLookupChunk) {
        const auto end = std::min(offset + kLookupChunk, keys.size());
        std::vector<std::string> chunk(keys.begin() + static_cast<long>(offset),
                                       keys.begin() + static_cast<long>(end));
        auto nodesRes = kg_->getNodesByKeys(chunk);
        if (!nodesRes) {
            spdlog::warn("[WriteCoordinator] bulk node-key prefetch failed: {}",
                         nodesRes.error().message);
            return;
        }
        for (const auto& node : nodesRes.value()) {
            nodeKeyToId[node.nodeKey] = node.id;
            ++resolvedCount;
        }
    }
    YAMS_PLOT("wc.prefetch.node_keys", static_cast<int64_t>(keys.size()));
    {
        std::lock_guard<std::mutex> lock(statsMutex_);
        stats_.nodeKeyLookupsBatched += resolvedCount;
    }
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
                                       AddEdgesOp& op, metadata::KGWriteBuffer* edgeBuffer) {
    if (op.edges.empty())
        return Result<void>();
    Result<void> r;
    if (op.unique) {
        if (edgeBuffer) {
            return edgeBuffer->addEdges(op.edges);
        }
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
                          std::unordered_map<std::string, std::int64_t>& nodeKeyToId,
                          metadata::KGWriteBuffer* edgeBuffer) {
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
            nodeKeyToId[key] = nodeRes.value()->id;
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
    if (edgeBuffer) {
        return edgeBuffer->addEdges(resolved);
    }
    auto r = kgBatch.addEdgesUnique(resolved);
    if (r) {
        std::lock_guard<std::mutex> lock(statsMutex_);
        stats_.edgesAdded += resolved.size();
    }
    return r;
}

Result<void>
WriteCoordinator::applyOp(metadata::KnowledgeGraphStore::WriteBatch& kgBatch, AddAliasesOp& op,
                          std::unordered_map<std::string, std::int64_t>& nodeKeyToId) {
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
                        nodeKeyToId[nodeKey] = nodeId;
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
                          std::unordered_map<std::string, std::int64_t>& nodeKeyToId) {
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
                nodeKeyToId[deferred.nodeKey] = nodeId;
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
                                       DeleteNodeByIdOp& op) {
    auto r = kgBatch.deleteNodeById(op.nodeId);
    if (r) {
        std::lock_guard<std::mutex> lock(statsMutex_);
        stats_.nodesDeleted++;
    }
    return r;
}

Result<void> WriteCoordinator::applyOp(metadata::KnowledgeGraphStore::WriteBatch& kgBatch,
                                       DeleteNodesForDocumentHashOp& op) {
    auto r = kgBatch.deleteNodesForDocumentHash(op.documentHash);
    if (!r)
        return r.error();
    {
        std::lock_guard<std::mutex> lock(statsMutex_);
        stats_.nodesDeleted += static_cast<std::uint64_t>(std::max<std::int64_t>(0, r.value()));
    }
    return Result<void>();
}

Result<void> WriteCoordinator::applyOp(metadata::KnowledgeGraphStore::WriteBatch& kgBatch,
                                       DeleteEdgesForSourceFileOp& op) {
    auto r = kgBatch.deleteEdgesForSourceFile(op.sourceFile);
    if (!r)
        return r.error();
    {
        std::lock_guard<std::mutex> lock(statsMutex_);
        stats_.edgesDeleted += static_cast<std::uint64_t>(std::max<std::int64_t>(0, r.value()));
    }
    return Result<void>();
}

Result<void> WriteCoordinator::applyOp(metadata::KnowledgeGraphStore::WriteBatch& kgBatch,
                                       DeleteEdgesByRelationOp& op) {
    auto r = kgBatch.deleteEdgesByRelation(op.relation);
    if (!r)
        return r.error();
    {
        std::lock_guard<std::mutex> lock(statsMutex_);
        stats_.edgesDeleted += static_cast<std::uint64_t>(std::max<std::int64_t>(0, r.value()));
    }
    return Result<void>();
}

Result<void> WriteCoordinator::applyOp(metadata::KnowledgeGraphStore::WriteBatch& kgBatch,
                                       DeleteOrphanedEdgesOp&) {
    auto r = kgBatch.deleteOrphanedEdges();
    if (!r)
        return r.error();
    {
        std::lock_guard<std::mutex> lock(statsMutex_);
        stats_.edgesDeleted += static_cast<std::uint64_t>(std::max<std::int64_t>(0, r.value()));
    }
    return Result<void>();
}

Result<void> WriteCoordinator::applyOp(metadata::KnowledgeGraphStore::WriteBatch& kgBatch,
                                       DeleteOrphanedDocEntitiesOp&) {
    auto r = kgBatch.deleteOrphanedDocEntities();
    if (!r)
        return r.error();
    {
        std::lock_guard<std::mutex> lock(statsMutex_);
        stats_.docEntitiesDeleted +=
            static_cast<std::uint64_t>(std::max<std::int64_t>(0, r.value()));
    }
    return Result<void>();
}

Result<void> WriteCoordinator::applyMetadataOp(UpdateRepairStatusOp& op) {
    if (!meta_)
        return Error{ErrorCode::InvalidState, "MetadataRepository unavailable"};
    if (op.hashes.empty())
        return Result<void>();
    metadata::MetadataOpScope opScope("wc_repair_status_batch");
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
    metadata::MetadataOpScope opScope("wc_tree_snapshot");
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
    metadata::MetadataOpScope opScope("wc_set_metadata_batch");
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
    metadata::MetadataOpScope opScope("wc_extraction_status");
    auto r = meta_->updateDocumentExtractionStatus(op.documentId, op.contentExtracted, op.status,
                                                   op.error);
    if (r) {
        std::lock_guard<std::mutex> lock(statsMutex_);
        stats_.extractionStatusesUpdated++;
    }
    return r;
}

Result<void> WriteCoordinator::applyMetadataOp(UpdateEmbeddingStatusByHashOp& op) {
    if (!meta_)
        return Error{ErrorCode::InvalidState, "MetadataRepository unavailable"};
    metadata::MetadataOpScope opScope("wc_embedding_status");
    auto r = meta_->updateDocumentEmbeddingStatusByHash(op.hash, op.embedded, op.modelName);
    if (r) {
        std::lock_guard<std::mutex> lock(statsMutex_);
        stats_.embeddingStatusesUpdated++;
    }
    return r;
}

Result<void> WriteCoordinator::applyMetadataOp(UpdateEmbeddingStatusByHashesOp& op) {
    if (!meta_)
        return Error{ErrorCode::InvalidState, "MetadataRepository unavailable"};
    if (op.hashes.empty())
        return Result<void>();
    metadata::MetadataOpScope opScope("wc_embedding_status_batch");
    auto r =
        meta_->batchUpdateDocumentEmbeddingStatusByHashes(op.hashes, op.embedded, op.modelName);
    if (r) {
        std::lock_guard<std::mutex> lock(statsMutex_);
        stats_.embeddingStatusesUpdated += op.hashes.size();
    }
    return r;
}

Result<void> WriteCoordinator::applyMetadataOp(UpsertSymbolExtractionStateOp& op) {
    if (!kg_)
        return Error{ErrorCode::InvalidState, "KnowledgeGraphStore unavailable"};
    if (op.documentHash.empty())
        return Result<void>();
    auto r = kg_->upsertSymbolExtractionState(op.documentHash, op.state);
    if (r) {
        std::lock_guard<std::mutex> lock(statsMutex_);
        stats_.symbolExtractionStatesUpdated++;
    }
    return r;
}

Result<void> WriteCoordinator::applyMetadataOp(InsertRelationshipOp& op) {
    if (!meta_)
        return Error{ErrorCode::InvalidState, "MetadataRepository unavailable"};
    metadata::MetadataOpScope opScope("wc_insert_relationship");
    auto r = meta_->insertRelationship(op.relationship);
    if (!r)
        return r.error();
    {
        std::lock_guard<std::mutex> lock(statsMutex_);
        stats_.relationshipsInserted++;
    }
    return Result<void>();
}

Result<void> WriteCoordinator::applyMetadataOp(AddSymSpellTermsOp& op) {
    if (!meta_)
        return Error{ErrorCode::InvalidState, "MetadataRepository unavailable"};
    if (op.terms.empty())
        return Result<void>();
    metadata::MetadataOpScope opScope("wc_symspell_terms");
    const auto termCount = op.terms.size();
    auto r = meta_->tryAddSymSpellTerms(op.terms);
    if (!r) {
        return r.error();
    }
    {
        std::lock_guard<std::mutex> lock(statsMutex_);
        stats_.symSpellTermsAdded += termCount;
    }
    return Result<void>();
}

} // namespace yams::daemon
