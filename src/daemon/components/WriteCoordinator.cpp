#include <yams/core/assert.hpp>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/TuningSnapshot.h>
#include <yams/daemon/components/WriteCoordinator.h>
#include <yams/metadata/kg_write_buffer.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/profiling.h>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <algorithm>
#include <unordered_map>
#include <unordered_set>

namespace yams::daemon {

// Keep each writer-loop turn bounded, but allow enough batches to coalesce noisy
// status-only producers before they churn the single SQLite writer.
static constexpr std::size_t kMaxBatchesPerIteration = 64;

struct WriteCoordinator::EmbeddingAcknowledgementState {
    explicit EmbeddingAcknowledgementState(EmbeddingCompletionAcknowledgementCallback callbackIn)
        : callback(std::move(callbackIn)) {}

    void settle(Result<EmbeddingCompletionAcknowledgement> result) noexcept {
        if (settled.exchange(true, std::memory_order_acq_rel)) {
            return;
        }
        auto completion = std::move(callback);
        callback = {};
        try {
            completion(std::move(result));
        } catch (const std::exception& e) {
            spdlog::error("[WriteCoordinator] embedding acknowledgement threw: {}", e.what());
        } catch (...) {
            spdlog::error(
                "[WriteCoordinator] embedding acknowledgement threw an unknown exception");
        }
    }

    std::atomic<bool> settled{false};
    EmbeddingCompletionAcknowledgementCallback callback;
};

namespace {

void acknowledgeEmbeddingCompletion(CompleteDocumentEmbeddingsByHashesOp& op,
                                    Result<EmbeddingCompletionAcknowledgement> result) noexcept {
    if (!op.acknowledgement) {
        return;
    }
    auto acknowledgement = std::move(op.acknowledgement);
    op.acknowledgement = {};
    try {
        acknowledgement(std::move(result));
    } catch (const std::exception& e) {
        spdlog::error("[WriteCoordinator] embedding acknowledgement threw: {}", e.what());
    } catch (...) {
        spdlog::error("[WriteCoordinator] embedding acknowledgement threw an unknown exception");
    }
}

// Settle graph op completions of one batch. With batchError every op fails (rolled back or
// never applied); otherwise each op reports its own staged value or op error. Idempotent.
void settleGraphCompletions(WriteBatch& batch, const Error* batchError) noexcept {
    const auto settle = [&](auto& completion) {
        if (!completion || completion->settled) {
            return;
        }
        try {
            if (batchError) {
                completion->settle(*batchError);
            } else if (completion->opError) {
                completion->settle(*completion->opError);
            } else if (!completion->applied) {
                completion->settle(Error{ErrorCode::InternalError, "graph op was not applied"});
            } else {
                completion->settle(completion->staged);
            }
        } catch (...) {
            spdlog::error("[WriteCoordinator] settling a graph op completion threw");
        }
    };
    for (auto& op : batch.ops) {
        if (auto* ingest = std::get_if<IngestGraphOp>(&op)) {
            settle(ingest->completion);
        } else if (auto* removal = std::get_if<DeleteGraphNamespaceOp>(&op)) {
            settle(removal->completion);
        }
    }
}

void settleGraphCompletions(std::vector<std::unique_ptr<WriteBatch>>& batches,
                            const Error* batchError) noexcept {
    for (auto& batch : batches) {
        if (batch) {
            settleGraphCompletions(*batch, batchError);
        }
    }
}

void failEmbeddingAcknowledgements(std::vector<std::unique_ptr<WriteBatch>>& batches,
                                   const Error& error) noexcept {
    for (auto& batch : batches) {
        if (!batch) {
            continue;
        }
        for (auto& op : batch->ops) {
            if (auto* completion = std::get_if<CompleteDocumentEmbeddingsByHashesOp>(&op)) {
                acknowledgeEmbeddingCompletion(*completion, error);
            }
        }
    }
    // Graph ops still unsettled here were never committed.
    settleGraphCompletions(batches, &error);
}

void failEmbeddingAcknowledgements(WriteBatch& batch, const Error& error) noexcept {
    for (auto& op : batch.ops) {
        if (auto* completion = std::get_if<CompleteDocumentEmbeddingsByHashesOp>(&op)) {
            acknowledgeEmbeddingCompletion(*completion, error);
        }
    }
    settleGraphCompletions(batch, &error);
}

} // namespace

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
    YAMS_PRECONDITION(config_.shutdownTimeout.count() >= 0,
                      "WriteCoordinator shutdownTimeout must be non-negative");
    spdlog::info("[WriteCoordinator] Initialized maxBatchSize={} maxDelayMs={} capacity={}",
                 config_.maxBatchSize, config_.maxBatchDelayMs.count(), config_.channelCapacity);
}

WriteCoordinator::WriteCoordinator(boost::asio::io_context& ioc,
                                   std::shared_ptr<metadata::KnowledgeGraphStore> kgStore,
                                   std::shared_ptr<metadata::MetadataRepository> metadataRepo)
    : WriteCoordinator(ioc, std::move(kgStore), std::move(metadataRepo), Config{}) {}

// A failed shutdown must not return while coroutines still use this object;
// std::terminate is the safe outcome.
// NOLINTNEXTLINE(bugprone-exception-escape)
WriteCoordinator::~WriteCoordinator() {
    shutdown();
}

void WriteCoordinator::registerEmbeddingAcknowledgementsLocked(WriteBatch& batch) {
    embeddingAcknowledgements_.erase(
        std::remove_if(embeddingAcknowledgements_.begin(), embeddingAcknowledgements_.end(),
                       [](const auto& state) { return state.expired(); }),
        embeddingAcknowledgements_.end());
    for (auto& op : batch.ops) {
        auto* completion = std::get_if<CompleteDocumentEmbeddingsByHashesOp>(&op);
        if (!completion || !completion->acknowledgement) {
            continue;
        }
        auto state =
            std::make_shared<EmbeddingAcknowledgementState>(std::move(completion->acknowledgement));
        completion->acknowledgement =
            [state](Result<EmbeddingCompletionAcknowledgement> result) mutable {
                state->settle(std::move(result));
            };
        embeddingAcknowledgements_.emplace_back(state);
    }
}

void WriteCoordinator::settleOutstandingEmbeddingAcknowledgements(const Error& error) noexcept {
    std::vector<std::shared_ptr<EmbeddingAcknowledgementState>> states;
    {
        std::lock_guard<std::mutex> lock(queueMutex_);
        states.reserve(embeddingAcknowledgements_.size());
        for (auto& weakState : embeddingAcknowledgements_) {
            if (auto state = weakState.lock()) {
                states.push_back(std::move(state));
            }
        }
        embeddingAcknowledgements_.clear();
    }
    for (auto& state : states) {
        state->settle(error);
    }
}

void WriteCoordinator::enqueue(std::unique_ptr<WriteBatch> batch) {
    YAMS_ZONE_SCOPED_N("WriteCoordinator::enqueue");
    if (!batch)
        return;
    batch->enqueueTime = std::chrono::steady_clock::now();
    bool forcedOverCapacity = false;
    bool rejectedForShutdown = false;
    std::size_t queueDepth = 0;
    {
        std::lock_guard<std::mutex> lock(queueMutex_);
        if (stop_.load(std::memory_order_acquire)) {
            rejectedForShutdown = true;
        } else {
            if (pendingBatches_.size() >= config_.channelCapacity) {
                forcedOverCapacity = true;
                spdlog::warn(
                    "[WriteCoordinator] Queue full ({} batches); forcing guaranteed enqueue past "
                    "capacity={}",
                    pendingBatches_.size(), config_.channelCapacity);
            }
            registerEmbeddingAcknowledgementsLocked(*batch);
            pendingBatches_.push_back(std::move(batch));
            queueDepth = pendingBatches_.size();
            YAMS_PLOT("wc.queue.depth", static_cast<int64_t>(queueDepth));
        }
    }
    if (rejectedForShutdown) {
        // `batch` is moved only on the enqueue branch, which leaves rejectedForShutdown false.
        spdlog::warn("[WriteCoordinator] Dropping batch from '{}' after shutdown requested",
                     batch->source); // NOLINT(bugprone-use-after-move)
        failEmbeddingAcknowledgements(
            *batch, Error{ErrorCode::SystemShutdown, "WriteCoordinator is shutting down"});
        return;
    }
    {
        std::lock_guard<std::mutex> lock(statsMutex_);
        stats_.batchesEnqueued++;
        stats_.maxQueueDepth = std::max<std::uint64_t>(stats_.maxQueueDepth, queueDepth);
        if (forcedOverCapacity) {
            stats_.forcedEnqueuesOverCapacity++;
        }
    }
    wakeWriter();
}

bool WriteCoordinator::tryEnqueue(std::unique_ptr<WriteBatch>& batch) {
    YAMS_ZONE_SCOPED_N("WriteCoordinator::tryEnqueue");
    if (!batch)
        return false;
    bool rejectedAtCapacity = false;
    bool rejectedForShutdown = false;
    std::size_t queueDepth = 0;
    {
        std::lock_guard<std::mutex> lock(queueMutex_);
        if (stop_.load(std::memory_order_acquire)) {
            rejectedForShutdown = true;
        } else if (pendingBatches_.size() >= config_.channelCapacity) {
            rejectedAtCapacity = true;
        } else {
            batch->enqueueTime = std::chrono::steady_clock::now();
            registerEmbeddingAcknowledgementsLocked(*batch);
            pendingBatches_.push_back(std::move(batch));
            queueDepth = pendingBatches_.size();
            YAMS_PLOT("wc.queue.depth", static_cast<int64_t>(queueDepth));
        }
    }
    if (rejectedForShutdown) {
        return false;
    }
    if (rejectedAtCapacity) {
        {
            std::lock_guard<std::mutex> lock(statsMutex_);
            stats_.capacityRejections++;
        }
        return false;
    }
    {
        std::lock_guard<std::mutex> lock(statsMutex_);
        stats_.batchesEnqueued++;
        stats_.maxQueueDepth = std::max<std::uint64_t>(stats_.maxQueueDepth, queueDepth);
    }
    wakeWriter();
    return true;
}

boost::asio::awaitable<Result<EmbeddingCompletionAcknowledgement>, boost::asio::any_io_executor>
WriteCoordinator::asyncCompleteEmbeddingDerivations(
    std::vector<metadata::EmbeddingDerivationToken> derivations, std::string modelName,
    std::string source) {
    if (derivations.empty() || modelName.empty()) {
        co_return Error{ErrorCode::InvalidArgument,
                        "Embedding completion requires derivation tokens and a model"};
    }

    auto cancellationState = co_await boost::asio::this_coro::cancellation_state;
    if (cancellationState.cancelled() != boost::asio::cancellation_type::none) {
        co_return Error{ErrorCode::OperationCancelled, "Embedding completion was cancelled"};
    }
    auto cancellationSlot = cancellationState.slot();
    auto callerExecutor = co_await boost::asio::this_coro::executor;

    co_return co_await boost::asio::async_initiate<
        const boost::asio::use_awaitable_t<>&, void(Result<EmbeddingCompletionAcknowledgement>)>(
        [this, derivations = std::move(derivations), modelName = std::move(modelName),
         source = std::move(source), cancellationSlot, callerExecutor](auto&& handler) mutable {
            using Handler = std::decay_t<decltype(handler)>;
            auto handlerPtr = std::make_shared<Handler>(std::forward<decltype(handler)>(handler));
            auto completionExecutor =
                boost::asio::get_associated_executor(*handlerPtr, callerExecutor);
            auto completed = std::make_shared<std::atomic<bool>>(false);
            auto finish = [completed, handlerPtr, completionExecutor](
                              Result<EmbeddingCompletionAcknowledgement> result) mutable {
                if (completed->exchange(true, std::memory_order_acq_rel)) {
                    return;
                }
                boost::asio::post(completionExecutor,
                                  [handlerPtr, result = std::move(result)]() mutable {
                                      auto completionHandler = std::move(*handlerPtr);
                                      std::move(completionHandler)(std::move(result));
                                  });
            };

            if (cancellationSlot.is_connected()) {
                cancellationSlot.assign([finish](boost::asio::cancellation_type type) mutable {
                    if (type != boost::asio::cancellation_type::none) {
                        finish(Error{ErrorCode::OperationCancelled,
                                     "Embedding completion was cancelled"});
                    }
                });
            }

            auto batch = std::make_unique<WriteBatch>();
            batch->source =
                source.empty() ? "WriteCoordinator::embeddingCompletion" : std::move(source);
            CompleteDocumentEmbeddingsByHashesOp op;
            op.modelName = std::move(modelName);
            op.derivations = std::move(derivations);
            auto admissionFailure = finish;
            op.acknowledgement = std::move(finish);
            batch->ops.emplace_back(std::move(op));
            if (!tryEnqueue(batch)) {
                if (isShuttingDown()) {
                    admissionFailure(
                        Error{ErrorCode::SystemShutdown, "WriteCoordinator is shutting down"});
                } else {
                    admissionFailure(
                        Error{ErrorCode::ResourceExhausted, "WriteCoordinator queue is full"});
                }
            }
        },
        boost::asio::use_awaitable);
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
    auto deadline = std::chrono::steady_clock::now() + config_.shutdownTimeout;
    bool drained = false;
    {
        std::unique_lock<std::mutex> lock(queueMutex_);
        drained = drainCv_.wait_until(lock, deadline, [this]() {
            return pendingBatches_.empty() && inFlight_.load(std::memory_order_acquire) == 0 &&
                   writerExited_.load(std::memory_order_acquire);
        });
    }
    if (!drained) {
        settleOutstandingEmbeddingAcknowledgements(
            Error{ErrorCode::SystemShutdown,
                  "WriteCoordinator shutdown timed out before acknowledgement"});
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

std::size_t WriteCoordinator::pressureAdjustedBatchLimit(std::size_t requestedMax,
                                                         ResourcePressureLevel level,
                                                         bool stopping) noexcept {
    if (requestedMax <= 1 || stopping) {
        return requestedMax;
    }
    switch (level) {
        case ResourcePressureLevel::Normal:
        case ResourcePressureLevel::Warning:
            return requestedMax;
        case ResourcePressureLevel::Critical:
            return std::max<std::size_t>(1, requestedMax / 4);
        case ResourcePressureLevel::Emergency:
            return 1;
    }
    return requestedMax;
}

boost::asio::awaitable<void> WriteCoordinator::writerLoop() {
    // No thread name here: this coroutine runs on the shared WorkCoordinator executor and can
    // resume on any of its workers, so naming "this thread" would mislabel a pool worker.
    spdlog::info("[WriteCoordinator] Writer loop started");
    auto pollTimer = wakeTimer_;

    while (true) {
        std::vector<std::unique_ptr<WriteBatch>> batchesToProcess;
        {
            std::lock_guard<std::mutex> lock(queueMutex_);
            const bool stopping = stop_.load(std::memory_order_acquire);
            if (stopping && pendingBatches_.empty() &&
                inFlight_.load(std::memory_order_acquire) == 0) {
                break;
            }

            const std::size_t baseMax = std::min(config_.maxBatchSize, kMaxBatchesPerIteration);
            const auto pressureLevel = config_.pressureLevelProvider
                                           ? config_.pressureLevelProvider()
                                           : ResourcePressureLevel::Normal;
            const std::size_t effectiveMax =
                pressureAdjustedBatchLimit(baseMax, pressureLevel, stopping);
            YAMS_DCHECK(effectiveMax > 0, "WriteCoordinator effectiveMax must remain positive");
            const std::size_t count = std::min(pendingBatches_.size(), effectiveMax);
            if (count > 0) {
                YAMS_ZONE_SCOPED_N("WriteCoordinator::dequeue");
                YAMS_PLOT("wc.dequeue.batch_count", static_cast<int64_t>(count));
                YAMS_PLOT("wc.dequeue.effective_max", static_cast<int64_t>(effectiveMax));
                YAMS_PLOT("wc.queue.depth_after_dequeue",
                          static_cast<int64_t>(pendingBatches_.size() - count));
                inFlight_.store(count, std::memory_order_release);
                batchesToProcess.reserve(count);
                for (std::size_t i = 0; i < count; ++i) {
                    batchesToProcess.push_back(std::move(pendingBatches_.front()));
                    pendingBatches_.pop_front();
                }
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
        Result<void> result;
        try {
            result = applyBatches(batchesToProcess);
        } catch (const std::exception& e) {
            result = Error{ErrorCode::InternalError,
                           std::string{"WriteCoordinator apply threw: "} + e.what()};
        } catch (...) {
            result = Error{ErrorCode::InternalError,
                           "WriteCoordinator apply threw an unknown exception"};
        }
        if (!result) {
            failEmbeddingAcknowledgements(batchesToProcess, result.error());
        } else {
            failEmbeddingAcknowledgements(
                batchesToProcess, Error{ErrorCode::InternalError,
                                        "Embedding completion acknowledgement was not applied"});
        }
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
    std::unordered_set<const WriteBatch*> batchesWithKgOps;
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
                                  std::is_same_v<T, CompleteDocumentEmbeddingsByHashesOp> ||
                                  std::is_same_v<T, InsertRelationshipOp> ||
                                  std::is_same_v<T, AddSymSpellTermsOp> ||
                                  std::is_same_v<T, AcknowledgeKnowledgeGraphOp>) {
                        hasMetaOps = true;
                    } else {
                        hasKgOps = true;
                        batchesWithKgOps.insert(batch.get());
                    }
                },
                op);
        }
    }

    std::unordered_map<std::string, std::int64_t> nodeKeyToId;
    std::optional<Error> firstOpError;
    bool abortApply = false;

    if (hasKgOps && kg_) {
        YAMS_ZONE_SCOPED_N("WriteCoordinator::applyKG");
        if (config_.kgDedupEnabled) {
            // Prefetch committed node keys before acquiring the write-batch connection so a
            // single-connection pool cannot self-deadlock on nested connection acquisition.
            prefetchDeferredNodeKeys(batches, nodeKeyToId);
        }
        auto batchResult = kg_->beginWriteBatch();
        if (!batchResult) {
            settleGraphCompletions(batches, &batchResult.error());
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
                        if (abortApply) {
                            // The transaction is gone; running more ops would autocommit them.
                            sourceError = true;
                            return;
                        }
                        if constexpr (std::is_same_v<T, UpdateRepairStatusOp> ||
                                      std::is_same_v<T, UpsertTreeSnapshotOp> ||
                                      std::is_same_v<T, SetMetadataBatchOp> ||
                                      std::is_same_v<T, UpdateExtractionStatusOp> ||
                                      std::is_same_v<T, UpdateEmbeddingStatusByHashOp> ||
                                      std::is_same_v<T, UpdateEmbeddingStatusByHashesOp> ||
                                      std::is_same_v<T, CompleteDocumentEmbeddingsByHashesOp> ||
                                      std::is_same_v<T, InsertRelationshipOp> ||
                                      std::is_same_v<T, AddSymSpellTermsOp> ||
                                      std::is_same_v<T, AcknowledgeKnowledgeGraphOp>) {
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
                                             std::is_same_v<T, DeleteOrphanedDocEntitiesOp> ||
                                             std::is_same_v<T, DeleteGraphNamespaceOp>) {
                            r = drainEdgeBuffer();
                            if (r) {
                                r = applyOp(kgBatch, concrete);
                            }
                        } else if constexpr (std::is_same_v<T, IngestGraphOp>) {
                            // Writes straight to the transaction so its classification reads
                            // every earlier op in this apply, including buffered edges.
                            r = drainEdgeBuffer();
                            if (r) {
                                r = applyOp(kgBatch, concrete, nodeKeyToId);
                            }
                        } else {
                            r = applyOp(kgBatch, concrete);
                        }
                        if (r && bufferPtr && bufferPtr->edgeCount() >= config_.kgDedupMaxEdges) {
                            r = drainEdgeBuffer();
                        }
                        // Graph ops report their own failure through their completion; a failure
                        // their savepoint fully undid must not fail unrelated coalesced work or
                        // surface in another caller's flush().
                        bool containedGraphFailure = false;
                        if constexpr (std::is_same_v<T, IngestGraphOp> ||
                                      std::is_same_v<T, DeleteGraphNamespaceOp>) {
                            if (concrete.completion) {
                                if (concrete.completion->rollbackFailed) {
                                    abortApply = true;
                                } else {
                                    containedGraphFailure = concrete.completion->contained;
                                }
                            }
                        }
                        // Node ids cached for this apply may now name deleted rows.
                        if constexpr (std::is_same_v<T, DeleteGraphNamespaceOp> ||
                                      std::is_same_v<T, DeleteNodeByIdOp> ||
                                      std::is_same_v<T, DeleteNodesForDocumentHashOp>) {
                            nodeKeyToId.clear();
                        }
                        if (!r) {
                            if (!containedGraphFailure && !firstOpError) {
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
        Result<void> commitResult;
        if (abortApply) {
            commitResult = Error{ErrorCode::TransactionFailed,
                                 "a graph op could not roll back its savepoint; apply aborted"};
        } else if (beforeKgCommitForTesting_) {
            commitResult = beforeKgCommitForTesting_();
        }
        if (commitResult) {
            commitResult = kgBatch.commit();
        }
        if (!commitResult) {
            // Nothing in this apply is durable: the batch rolls back when it goes out of scope.
            settleGraphCompletions(batches, &commitResult.error());
            spdlog::error("[WriteCoordinator] KG batch commit failed: {}",
                          commitResult.error().message);
            std::lock_guard<std::mutex> lock(statsMutex_);
            stats_.commitErrors++;
            TuneAdvisor::reportDbLockError();
            return commitResult.error();
        }
        settleGraphCompletions(batches, nullptr);
        if (firstOpError) {
            return *firstOpError;
        }
    }

    if (hasKgOps && !kg_) {
        const Error unavailable{ErrorCode::NotInitialized, "Knowledge graph store unavailable"};
        settleGraphCompletions(batches, &unavailable);
    }

    // Release the KG write lease before acquiring a metadata connection (including size-one pools).
    if (hasKgOps && kg_) {
        std::optional<Error> firstCompletionError;
        std::vector<std::pair<int64_t, std::string>> pendingCompletions;
        pendingCompletions.reserve(batches.size());
        for (const auto& batch : batches) {
            if (batch->knowledgeGraphToken.empty())
                continue;
            if (batch->knowledgeGraphCompletion) {
                if (!batchesWithKgOps.contains(batch.get()) ||
                    !batch->knowledgeGraphCompletion->markCommitted(
                        batch->knowledgeGraphCompletionStage)) {
                    continue;
                }
            }
            if (!meta_) {
                if (!firstCompletionError) {
                    firstCompletionError =
                        Error{ErrorCode::NotInitialized, "KG completion metadata unavailable"};
                }
                continue;
            }
            pendingCompletions.emplace_back(batch->knowledgeGraphDocumentId,
                                            batch->knowledgeGraphToken);
        }
        if (!pendingCompletions.empty()) {
            metadata::MetadataOpScope opScope("wc_kg_batch_completion");
            auto completed = meta_->completeKnowledgeGraphEnrichmentBatch(pendingCompletions);
            if (!completed && !firstCompletionError)
                firstCompletionError = completed.error();
        }
        if (firstCompletionError)
            return *firstCompletionError;
    }

    if (hasMetaOps && !meta_) {
        failEmbeddingAcknowledgements(
            batches, Error{ErrorCode::InvalidState, "MetadataRepository unavailable"});
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
        struct EmbeddingCompletionGroup {
            std::string source;
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
        std::unordered_map<std::string, EmbeddingCompletionGroup>
            embeddingCompletionBySourceAndModel;
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
                                      std::is_same_v<T, AcknowledgeKnowledgeGraphOp>) {
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
                            if (!concrete.derivations.empty()) {
                                r = applyMetadataOp(concrete);
                                if (!r && !firstOpError) {
                                    firstOpError = r.error();
                                }
                            } else {
                                if (concrete.hashes.empty())
                                    return;
                                const auto key = batch->source + "\x1f" +
                                                 std::to_string(static_cast<int>(concrete.status));
                                auto& group = repairStatusBySourceAndStatus[key];
                                if (group.source.empty()) {
                                    group.source = batch->source;
                                    group.status = concrete.status;
                                }
                                group.hashes.insert(
                                    group.hashes.end(),
                                    std::make_move_iterator(concrete.hashes.begin()),
                                    std::make_move_iterator(concrete.hashes.end()));
                                concrete.hashes.clear();
                                return;
                            }
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
                        } else if constexpr (std::is_same_v<T,
                                                            CompleteDocumentEmbeddingsByHashesOp>) {
                            if (!concrete.derivations.empty() || concrete.acknowledgement) {
                                auto completion = applyMetadataOp(concrete);
                                if (!completion) {
                                    r = completion.error();
                                    acknowledgeEmbeddingCompletion(concrete, completion.error());
                                } else {
                                    r = Result<void>{};
                                    acknowledgeEmbeddingCompletion(concrete, std::move(completion));
                                }
                            } else {
                                if (concrete.hashes.empty())
                                    return;
                                const auto key = batch->source + "\x1f" + concrete.modelName;
                                auto& group = embeddingCompletionBySourceAndModel[key];
                                if (group.source.empty()) {
                                    group.source = batch->source;
                                    group.modelName = concrete.modelName;
                                }
                                group.hashes.insert(
                                    group.hashes.end(),
                                    std::make_move_iterator(concrete.hashes.begin()),
                                    std::make_move_iterator(concrete.hashes.end()));
                                concrete.hashes.clear();
                                return;
                            }
                        } else {
                            return;
                        }
                        if (!r) {
                            if (!firstOpError) {
                                firstOpError = r.error();
                            }
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

        for (auto& [_, group] : embeddingCompletionBySourceAndModel) {
            for (std::size_t off = 0; off < group.hashes.size(); off += chunkMax) {
                YAMS_ZONE_SCOPED_N("WriteCoordinator::coalesceEmbeddingCompletion");
                const auto end = std::min(group.hashes.size(), off + chunkMax);
                const auto sourceStart = std::chrono::steady_clock::now();
                CompleteDocumentEmbeddingsByHashesOp op;
                op.hashes = std::vector<std::string>(
                    std::make_move_iterator(group.hashes.begin() + static_cast<long>(off)),
                    std::make_move_iterator(group.hashes.begin() + static_cast<long>(end)));
                op.modelName = group.modelName;
                const auto hashCount = static_cast<std::uint64_t>(op.hashes.size());
                const auto logicalOpCount = hashCount * 2;
                YAMS_PLOT("wc.coalesce.embedding_completion", static_cast<int64_t>(hashCount));
                auto r = applyMetadataOp(op);
                const auto sourceApplyMs = static_cast<std::uint64_t>(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - sourceStart)
                        .count());
                if (!r) {
                    spdlog::warn("[WriteCoordinator] coalesced embedding-completion op '{}' "
                                 "failed: {}",
                                 group.source, r.error().message);
                } else {
                    std::lock_guard<std::mutex> lock(statsMutex_);
                    stats_.opsApplied += logicalOpCount;
                }
                recordMetaApply(group.source, logicalOpCount, sourceApplyMs, !r);
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

    // Token-fenced metadata operations can fail after the KG phase has succeeded.
    // Preserve that failure for flush() rather than reporting the batch as committed.
    if (firstOpError) {
        return *firstOpError;
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

// Resolve a key on the apply's own transaction so a node deleted (or created) earlier in the same
// apply is seen as such; a committed-state read could return a deleted node's id.
Result<std::optional<metadata::KGNode>>
WriteCoordinator::lookupNodeForApply(metadata::KnowledgeGraphStore::WriteBatch& kgBatch,
                                     const std::string& key) {
    auto node = kgBatch.getNodeByKey(key);
    if (!node && node.error().code == ErrorCode::NotImplemented && kg_) {
        return kg_->getNodeByKey(key);
    }
    return node;
}

Result<void> WriteCoordinator::applyOp(metadata::KnowledgeGraphStore::WriteBatch& kgBatch,
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
        auto nodeRes = lookupNodeForApply(kgBatch, key);
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

Result<void> WriteCoordinator::applyOp(metadata::KnowledgeGraphStore::WriteBatch& kgBatch,
                                       AddAliasesOp& op,
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
                    auto nodeRes = lookupNodeForApply(kgBatch, nodeKey);
                    if (nodeRes && nodeRes.value().has_value()) {
                        // NOLINTNEXTLINE(bugprone-unchecked-optional-access): checked above
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

namespace {

// Owner of a stored or requested row. Only a non-empty JSON string is a namespace; this matches
// the SQL predicate that exempts namespaced rows from cleanup.
struct RowNamespace {
    enum class Kind { Absent, Valid, Invalid };
    Kind kind = Kind::Absent;
    std::string value;

    bool operator==(const RowNamespace& other) const {
        return kind == other.kind && (kind != Kind::Valid || value == other.value);
    }
};

RowNamespace namespaceOf(const std::optional<std::string>& properties) {
    if (!properties || properties->empty()) {
        return {};
    }
    auto parsed = nlohmann::json::parse(*properties, nullptr, false);
    if (parsed.is_discarded() || !parsed.is_object()) {
        return {};
    }
    auto it = parsed.find(std::string(metadata::kExternalNamespaceProperty));
    if (it == parsed.end()) {
        return {};
    }
    if (!it->is_string() || it->get_ref<const std::string&>().empty()) {
        return {RowNamespace::Kind::Invalid, it->dump()};
    }
    return {RowNamespace::Kind::Valid, it->get<std::string>()};
}

// A stored namespace that is not a non-empty string never made the row namespaced.
RowNamespace storedNamespaceOf(const std::optional<std::string>& properties) {
    auto ns = namespaceOf(properties);
    if (ns.kind == RowNamespace::Kind::Invalid) {
        return {};
    }
    return ns;
}

bool isReservedNodeKey(std::string_view key) {
    return metadata::isYamsReservedNodeKey(key);
}

} // namespace

Result<void> WriteCoordinator::applyOp(metadata::KnowledgeGraphStore::WriteBatch& kgBatch,
                                       IngestGraphOp& op,
                                       std::unordered_map<std::string, std::int64_t>& nodeKeyToId) {
    if (!op.completion) {
        op.completion = std::make_shared<GraphIngestCompletion>();
    }
    auto& completion = *op.completion;
    using Stage = GraphIngestCompletion::Stage;
    completion.staged = GraphIngestOutcome{};
    completion.opError.reset();
    completion.failedStage = Stage::None;
    completion.applied = false;
    completion.contained = false;
    completion.rollbackFailed = false;

    if (auto opened = kgBatch.beginSavepoint(); !opened) {
        completion.opError = opened.error();
        completion.contained = true; // nothing was written
        return opened.error();
    }
    GraphIngestOutcome outcome;
    Stage stage = Stage::Nodes;
    std::uint64_t nodesWritten = 0;
    std::uint64_t edgesWritten = 0;
    std::uint64_t aliasesWritten = 0;
    const auto fail = [&](Error error) -> Result<void> {
        // All or nothing: nothing from this op survives, so nothing is reported as written.
        if (auto undone = kgBatch.rollbackToSavepoint(); !undone) {
            spdlog::error("[WriteCoordinator] graph ingest savepoint rollback failed: {}",
                          undone.error().message);
            completion.rollbackFailed = true;
        } else {
            completion.contained = true;
        }
        completion.failedStage = stage;
        completion.opError = error;
        return error;
    };

    struct NodeInfo {
        std::int64_t id = 0;
        RowNamespace ns;
    };
    // One lookup per key per op, against this transaction (earlier ops in the apply included).
    std::unordered_map<std::string, std::optional<NodeInfo>> nodes;
    const auto lookup = [&](const std::string& key) -> Result<std::optional<NodeInfo>> {
        if (key.empty()) {
            return std::optional<NodeInfo>{};
        }
        if (auto it = nodes.find(key); it != nodes.end()) {
            return it->second;
        }
        auto node = kgBatch.getNodeByKey(key);
        if (!node) {
            return node.error();
        }
        std::optional<NodeInfo> info;
        if (node.value()) {
            info = NodeInfo{node.value()->id, storedNamespaceOf(node.value()->properties)};
        }
        nodes.emplace(key, info);
        return info;
    };
    const auto nsText = [](const RowNamespace& ns) {
        return ns.kind == RowNamespace::Kind::Absent ? std::string{} : ns.value;
    };

    // ---- nodes ----
    {
        std::vector<metadata::KGNode> toUpsert;
        std::vector<RowNamespace> upsertNs;
        toUpsert.reserve(op.nodes.size());
        for (auto& node : op.nodes) {
            if (node.nodeKey.empty()) {
                ++outcome.nodesSkipped;
                continue;
            }
            auto requested = namespaceOf(node.properties);
            const auto conflict = [&](std::string existing, std::string reason) {
                ++outcome.nodesSkipped;
                outcome.nsConflicts.push_back(GraphIngestNsConflict{"node",
                                                                    node.nodeKey,
                                                                    {},
                                                                    {},
                                                                    {},
                                                                    std::move(existing),
                                                                    nsText(requested),
                                                                    std::move(reason)});
            };
            if (requested.kind == RowNamespace::Kind::Invalid) {
                conflict({}, "invalid_ns");
                continue;
            }
            if (requested.kind == RowNamespace::Kind::Valid && isReservedNodeKey(node.nodeKey)) {
                conflict({}, "reserved_key");
                continue;
            }
            auto existing = lookup(node.nodeKey);
            if (!existing) {
                return fail(existing.error());
            }
            // Omitted properties keep the stored ones (COALESCE upsert), so they keep the
            // stored owner too: a label- or type-only update is not an ownership change.
            if (existing.value() && !node.properties) {
                requested = existing.value()->ns;
            }
            if (existing.value() && !(existing.value()->ns == requested)) {
                conflict(nsText(existing.value()->ns), "ns_change");
                continue;
            }
            if (existing.value() && op.skipExistingNodes) {
                ++outcome.nodesSkipped;
                continue;
            }
            ++(existing.value() ? outcome.nodesUpdated : outcome.nodesInserted);
            // A repeated key later in this request is an update of this row.
            nodes[node.nodeKey] = NodeInfo{existing.value() ? existing.value()->id : 0, requested};
            upsertNs.push_back(requested);
            toUpsert.push_back(std::move(node));
        }
        if (!toUpsert.empty()) {
            auto ids = kgBatch.upsertNodes(toUpsert);
            if (!ids) {
                return fail(ids.error());
            }
            for (std::size_t i = 0; i < toUpsert.size() && i < ids.value().size(); ++i) {
                nodes[toUpsert[i].nodeKey] = NodeInfo{ids.value()[i], upsertNs[i]};
            }
            nodesWritten = toUpsert.size();
        }
    }

    // ---- edges ----
    stage = Stage::Edges;
    {
        struct EdgeState {
            float weight = 0.0f;
            RowNamespace ns;
            std::optional<std::string> properties;
        };
        std::unordered_map<std::string, std::optional<EdgeState>> edges;
        for (const auto& deferred : op.edges) {
            auto src = lookup(deferred.srcNodeKey);
            if (!src) {
                return fail(src.error());
            }
            auto dst = lookup(deferred.dstNodeKey);
            if (!dst) {
                return fail(dst.error());
            }
            if (!src.value() || !dst.value()) {
                ++outcome.edgesSkipped;
                outcome.unresolvedEdges.push_back(GraphIngestUnresolvedEdge{
                    deferred.srcNodeKey, deferred.dstNodeKey, deferred.relation,
                    !src.value().has_value(), !dst.value().has_value()});
                continue;
            }
            auto requested = namespaceOf(deferred.properties);
            const auto conflict = [&](std::string existing, std::string reason) {
                ++outcome.edgesSkipped;
                outcome.nsConflicts.push_back(GraphIngestNsConflict{"edge",
                                                                    {},
                                                                    deferred.srcNodeKey,
                                                                    deferred.dstNodeKey,
                                                                    deferred.relation,
                                                                    std::move(existing),
                                                                    nsText(requested),
                                                                    std::move(reason)});
            };
            if (requested.kind == RowNamespace::Kind::Invalid) {
                conflict({}, "invalid_ns");
                continue;
            }
            const auto srcId = src.value()->id;
            const auto dstId = dst.value()->id;
            const auto edgeKey =
                std::to_string(srcId) + '\x1f' + std::to_string(dstId) + '\x1f' + deferred.relation;
            auto known = edges.find(edgeKey);
            if (known == edges.end()) {
                auto stored = kgBatch.getEdgeByEndpoints(srcId, dstId, deferred.relation);
                if (!stored) {
                    return fail(stored.error());
                }
                std::optional<EdgeState> state;
                if (stored.value()) {
                    state = EdgeState{stored.value()->weight,
                                      storedNamespaceOf(stored.value()->properties),
                                      stored.value()->properties};
                }
                known = edges.emplace(edgeKey, state).first;
            }
            const auto& existing = known->second;
            // Omitted properties keep the stored owner, as for nodes.
            const bool inheritProperties = existing && !deferred.properties;
            if (inheritProperties) {
                requested = existing->ns;
            }
            if (existing && !(existing->ns == requested)) {
                conflict(nsText(existing->ns), "ns_change");
                continue;
            }
            if (existing && op.skipExistingEdges) {
                ++outcome.edgesSkipped;
                continue;
            }
            metadata::KGEdge edge;
            edge.srcNodeId = srcId;
            edge.dstNodeId = dstId;
            edge.relation = deferred.relation;
            edge.weight = deferred.weight;
            edge.properties = inheritProperties ? existing->properties : deferred.properties;
            if (requested.kind == RowNamespace::Kind::Valid) {
                // The owner's latest write is the row: weight and properties are replaced.
                if (auto r = kgBatch.upsertEdgeExact(edge, existing.has_value()); !r) {
                    return fail(r.error());
                }
                ++(existing ? outcome.edgesUpdated : outcome.edgesInserted);
                known->second = EdgeState{edge.weight, requested, edge.properties};
            } else {
                // Un-namespaced edges keep the shared merge rule: weight = MAX, properties only
                // replaced when the new weight is not lower. A lower-weight write changes
                // nothing and is counted as skipped.
                if (existing && deferred.weight < existing->weight) {
                    ++outcome.edgesSkipped;
                    continue;
                }
                if (auto r = kgBatch.addEdgesUnique({edge}); !r) {
                    return fail(r.error());
                }
                ++(existing ? outcome.edgesUpdated : outcome.edgesInserted);
                known->second = EdgeState{edge.weight, requested,
                                          edge.properties ? edge.properties
                                          : existing      ? existing->properties
                                                          : std::nullopt};
            }
            ++edgesWritten;
        }
    }

    // ---- aliases ----
    stage = Stage::Aliases;
    {
        // An alias belongs to its node's owner: the request may only alias nodes owned by the
        // request's namespace (or, without one, un-namespaced nodes).
        RowNamespace requestNs;
        if (!op.ns.empty()) {
            requestNs = RowNamespace{RowNamespace::Kind::Valid, op.ns};
        }
        for (auto& intent : op.aliases) {
            auto node = lookup(intent.nodeKey);
            if (!node) {
                return fail(node.error());
            }
            if (!node.value()) {
                ++outcome.aliasesSkipped;
                outcome.unresolvedAliasKeys.push_back(intent.nodeKey);
                continue;
            }
            if (!(node.value()->ns == requestNs)) {
                ++outcome.aliasesSkipped;
                outcome.nsConflicts.push_back(GraphIngestNsConflict{"alias",
                                                                    intent.nodeKey,
                                                                    {},
                                                                    {},
                                                                    {},
                                                                    nsText(node.value()->ns),
                                                                    nsText(requestNs),
                                                                    "ns_change"});
                continue;
            }
            metadata::KGAlias alias;
            alias.nodeId = node.value()->id;
            alias.alias = std::move(intent.alias);
            alias.source = std::move(intent.source);
            alias.confidence = intent.confidence;
            auto inserted = kgBatch.upsertAliasCounted(alias);
            if (!inserted) {
                return fail(inserted.error());
            }
            ++(inserted.value() ? outcome.aliasesInserted : outcome.aliasesUpdated);
            ++aliasesWritten;
        }
    }

    if (auto released = kgBatch.releaseSavepoint(); !released) {
        return fail(released.error());
    }
    // Only now are the writes part of the transaction; a rolled-back op adds nothing.
    {
        std::lock_guard<std::mutex> lock(statsMutex_);
        stats_.nodesUpserted += nodesWritten;
        stats_.edgesAdded += edgesWritten;
        stats_.aliasesAdded += aliasesWritten;
    }
    for (const auto& [key, info] : nodes) {
        if (info) {
            nodeKeyToId[key] = info->id;
        }
    }
    // Staged only; the result is published after the transaction commits.
    completion.staged = std::move(outcome);
    completion.applied = true;
    return Result<void>();
}

Result<void> WriteCoordinator::applyOp(metadata::KnowledgeGraphStore::WriteBatch& kgBatch,
                                       DeleteGraphNamespaceOp& op) {
    if (!op.completion) {
        op.completion = std::make_shared<GraphRemoveCompletion>();
    }
    auto& completion = *op.completion;
    completion.staged = metadata::NamespaceRemovalResult{};
    completion.opError.reset();
    completion.applied = false;
    completion.contained = false;
    completion.rollbackFailed = false;
    if (op.ns.empty()) {
        completion.opError =
            Error{ErrorCode::InvalidArgument, "Namespace removal requires a namespace"};
        completion.contained = true;
        return *completion.opError;
    }
    if (auto opened = kgBatch.beginSavepoint(); !opened) {
        completion.opError = opened.error();
        completion.contained = true;
        return opened.error();
    }
    std::optional<std::string_view> relation;
    if (op.relation) {
        relation = *op.relation;
    }
    auto removed = kgBatch.deleteByNamespace(op.ns, relation, op.dryRun, op.includeForeignEdges);
    if (!removed) {
        // Edges may already be gone when the node delete fails: undo both.
        if (auto undone = kgBatch.rollbackToSavepoint(); !undone) {
            spdlog::error("[WriteCoordinator] graph removal savepoint rollback failed: {}",
                          undone.error().message);
            completion.rollbackFailed = true;
        } else {
            completion.contained = true;
        }
        completion.opError = removed.error();
        return removed.error();
    }
    if (auto released = kgBatch.releaseSavepoint(); !released) {
        completion.opError = released.error();
        completion.rollbackFailed = true;
        return released.error();
    }
    if (!op.dryRun && !removed.value().refused) {
        std::lock_guard<std::mutex> lock(statsMutex_);
        stats_.nodesDeleted += static_cast<std::uint64_t>(removed.value().nodesDeleted);
        stats_.edgesDeleted += static_cast<std::uint64_t>(removed.value().edgesDeleted);
    }
    completion.staged = removed.value();
    completion.applied = true;
    return Result<void>();
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

Result<void> WriteCoordinator::applyOp(metadata::KnowledgeGraphStore::WriteBatch& kgBatch,
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
            auto nodeRes = lookupNodeForApply(kgBatch, deferred.nodeKey);
            if (nodeRes && nodeRes.value().has_value()) {
                // NOLINTNEXTLINE(bugprone-unchecked-optional-access): checked above
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
    if (!op.derivations.empty()) {
        if (!op.hashes.empty()) {
            return Error{ErrorCode::InvalidArgument,
                         "repair status op cannot mix hashes and derivation tokens"};
        }
        metadata::MetadataOpScope opScope("wc_repair_status_derivation_batch");
        auto r = meta_->batchUpdateEmbeddingDerivationRepairStatus(op.derivations, op.status);
        if (!r)
            return r.error();
        {
            std::lock_guard<std::mutex> lock(statsMutex_);
            stats_.repairStatusesUpdated += r.value();
        }
        return Result<void>();
    }
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

Result<EmbeddingCompletionAcknowledgement>
WriteCoordinator::applyMetadataOp(CompleteDocumentEmbeddingsByHashesOp& op) {
    if (!meta_)
        return Error{ErrorCode::InvalidState, "MetadataRepository unavailable"};
    if (!op.derivations.empty()) {
        if (!op.hashes.empty()) {
            return Error{ErrorCode::InvalidArgument,
                         "Embedding completion cannot mix legacy hashes and derivation tokens"};
        }
        metadata::MetadataOpScope opScope("wc_embedding_derivation_completion");
        auto completed =
            meta_->batchCompleteDocumentEmbeddingDerivations(op.derivations, op.modelName);
        if (!completed) {
            return completed.error();
        }
        if (completed.value() > 0) {
            std::lock_guard<std::mutex> lock(statsMutex_);
            stats_.embeddingStatusesUpdated += completed.value();
            stats_.repairStatusesUpdated += completed.value();
        }
        return EmbeddingCompletionAcknowledgement{op.derivations.size(), completed.value()};
    }
    if (op.acknowledgement) {
        return Error{ErrorCode::InvalidArgument,
                     "Acknowledged embedding completion requires derivation tokens"};
    }
    if (op.hashes.empty())
        return EmbeddingCompletionAcknowledgement{};
    metadata::MetadataOpScope opScope("wc_embedding_completion_batch");
    auto r = meta_->batchCompleteDocumentEmbeddingsByHashes(op.hashes, op.modelName);
    if (!r) {
        return r.error();
    }
    {
        std::lock_guard<std::mutex> lock(statsMutex_);
        stats_.embeddingStatusesUpdated += op.hashes.size();
        stats_.repairStatusesUpdated += op.hashes.size();
    }
    return EmbeddingCompletionAcknowledgement{op.hashes.size(), op.hashes.size()};
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

Result<void> WriteCoordinator::applyMetadataOp(AcknowledgeKnowledgeGraphOp& op) {
    if (!op.completion) {
        return Error{ErrorCode::InvalidArgument,
                     "Knowledge graph acknowledgement requires a completion barrier"};
    }
    if (op.token.empty()) {
        return Error{ErrorCode::InvalidArgument,
                     "Knowledge graph acknowledgement requires a token"};
    }
    if (!op.completion->markCommitted(op.stage)) {
        return Result<void>();
    }
    if (!meta_) {
        return Error{ErrorCode::InvalidState, "MetadataRepository unavailable"};
    }
    metadata::MetadataOpScope opScope("wc_knowledge_graph_acknowledgement");
    auto completed = meta_->completeKnowledgeGraphEnrichment(op.documentId, op.token);
    if (!completed) {
        return completed.error();
    }
    return Result<void>();
}

namespace detail {
void logWriteBatchCoalescerWarn(std::string_view source, std::string_view message) {
    spdlog::warn("[WriteBatchCoalescer:{}] {}", source, message);
}
} // namespace detail

} // namespace yams::daemon
