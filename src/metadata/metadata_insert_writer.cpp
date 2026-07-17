#include <yams/metadata/metadata_insert_writer.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace yams::metadata {

namespace {

template <typename Rep, typename Period>
std::uint64_t elapsedMicros(std::chrono::duration<Rep, Period> duration) {
    const auto micros = std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
    return micros > 0 ? static_cast<std::uint64_t>(micros) : 0;
}

void updateMaxRelaxed(std::atomic<std::uint64_t>& maximum, std::uint64_t value) {
    auto current = maximum.load(std::memory_order_relaxed);
    while (current < value &&
           !maximum.compare_exchange_weak(current, value, std::memory_order_relaxed)) {
    }
}

std::future<Result<DocumentInsertOutcome>> readyFuture(Result<DocumentInsertOutcome> result) {
    std::promise<Result<DocumentInsertOutcome>> promise;
    promise.set_value(std::move(result));
    return promise.get_future();
}

} // namespace

MetadataInsertWriter::MetadataInsertWriter(std::shared_ptr<MetadataRepository> repo)
    : MetadataInsertWriter(std::move(repo), Options{}) {}

MetadataInsertWriter::MetadataInsertWriter(std::shared_ptr<MetadataRepository> repo,
                                           Options options)
    : repo_(std::move(repo)), options_(options) {
    if (!repo_) {
        throw std::invalid_argument("MetadataInsertWriter requires a MetadataRepository");
    }
    if (options_.maxBatchCount == 0) {
        options_.maxBatchCount = 1;
    }
    worker_ = std::thread([this] { run(); });
}

MetadataInsertWriter::~MetadataInsertWriter() {
    shutdown();
}

std::future<Result<DocumentInsertOutcome>>
MetadataInsertWriter::submit(BatchDocumentInsert record) {
    std::lock_guard lock(mutex_);
    if (!accepting_) {
        metrics_.rejectedItems.fetch_add(1, std::memory_order_relaxed);
        return readyFuture(Result<DocumentInsertOutcome>(ErrorCode::InvalidState));
    }

    QueueItem item;
    item.sequence = nextSequence_++;
    item.enqueuedAt = std::chrono::steady_clock::now();
    item.record = std::move(record);
    auto future = item.promise.get_future();
    queue_.push_back(std::move(item));
    metrics_.submittedItems.fetch_add(1, std::memory_order_relaxed);
    cv_.notify_one();
    return future;
}

std::vector<MetadataInsertWriter::QueueItem>
MetadataInsertWriter::takeBatch(std::unique_lock<std::mutex>& lock) {
    if (queue_.empty()) {
        return {};
    }

    if (options_.maxDelay.count() > 0 && queue_.size() < options_.maxBatchCount && !stopping_) {
        cv_.wait_for(lock, options_.maxDelay,
                     [&] { return stopping_ || queue_.size() >= options_.maxBatchCount; });
    }

    const auto count = std::min(options_.maxBatchCount, queue_.size());
    std::vector<QueueItem> items;
    items.reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
        items.push_back(std::move(queue_.front()));
        queue_.pop_front();
    }
    return items;
}

void MetadataInsertWriter::run() {
    while (true) {
        std::vector<QueueItem> items;
        {
            std::unique_lock lock(mutex_);
            cv_.wait(lock, [&] { return stopping_ || !queue_.empty(); });
            if (queue_.empty() && stopping_) {
                break;
            }
            items = takeBatch(lock);
        }

        if (items.empty()) {
            continue;
        }

        const auto batchStartedAt = std::chrono::steady_clock::now();
        std::uint64_t queueWaitTotalUs = 0;
        std::uint64_t queueWaitMaxUs = 0;
        for (const auto& item : items) {
            const auto queueWaitUs = elapsedMicros(batchStartedAt - item.enqueuedAt);
            queueWaitTotalUs += queueWaitUs;
            queueWaitMaxUs = std::max(queueWaitMaxUs, queueWaitUs);
        }
        const auto batchSize = static_cast<std::uint64_t>(items.size());
        metrics_.batches.fetch_add(1, std::memory_order_relaxed);
        metrics_.batchItems.fetch_add(batchSize, std::memory_order_relaxed);
        updateMaxRelaxed(metrics_.maxBatchSize, batchSize);
        metrics_.queueWaitSamples.fetch_add(batchSize, std::memory_order_relaxed);
        metrics_.queueWaitTotalUs.fetch_add(queueWaitTotalUs, std::memory_order_relaxed);
        updateMaxRelaxed(metrics_.queueWaitMaxUs, queueWaitMaxUs);

        std::vector<BatchDocumentInsert> records;
        records.reserve(items.size());
        for (auto& item : items) {
            records.push_back(std::move(item.record));
        }

        const auto lastSequence = items.back().sequence;

        // Do the (potentially throwing) batch insert inside a guard so an unexpected exception
        // cannot terminate the worker thread or strand the in-flight promises.
        bool batchThrew = false;
        Result<std::vector<DocumentInsertOutcome>> result =
            Error{ErrorCode::InternalError, "uninitialized"};
        const auto applyStartedAt = std::chrono::steady_clock::now();
        try {
            result = repo_->batchInsertDocumentsWithMetadata(records);
        } catch (const std::exception& ex) {
            batchThrew = true;
            spdlog::error("[MetadataInsertWriter] batch insert threw: {}", ex.what());
        } catch (...) {
            batchThrew = true;
            spdlog::error("[MetadataInsertWriter] batch insert threw unknown exception");
        }
        const auto applyUs = elapsedMicros(std::chrono::steady_clock::now() - applyStartedAt);
        metrics_.batchApplySamples.fetch_add(1, std::memory_order_relaxed);
        metrics_.batchApplyTotalUs.fetch_add(applyUs, std::memory_order_relaxed);
        updateMaxRelaxed(metrics_.batchApplyMaxUs, applyUs);
        if (batchThrew || !result) {
            metrics_.failedBatches.fetch_add(1, std::memory_order_relaxed);
        }

        if (!batchThrew && result) {
            const auto& outcomes = result.value();
            for (std::size_t i = 0; i < items.size(); ++i) {
                items[i].promise.set_value(
                    i < outcomes.size() ? Result<DocumentInsertOutcome>(outcomes[i])
                                        : Result<DocumentInsertOutcome>(ErrorCode::InternalError));
            }
        } else if (!batchThrew) {
            // The batch transaction failed as a unit (e.g. one poison document rolling back its
            // batch-mates). Retry each document individually so only the genuinely-failing document
            // fails, preserving the pre-coalescing per-document isolation.
            metrics_.fallbackItems.fetch_add(batchSize, std::memory_order_relaxed);
            for (std::size_t i = 0; i < items.size(); ++i) {
                Result<DocumentInsertOutcome> single =
                    Error{ErrorCode::InternalError, "uninitialized"};
                try {
                    std::vector<BatchDocumentInsert> singleRecord;
                    singleRecord.reserve(1);
                    singleRecord.push_back(records[i]);
                    auto inserted = repo_->batchInsertDocumentsWithMetadata(singleRecord);
                    if (inserted && inserted.value().size() == 1) {
                        single = inserted.value().front();
                    } else if (inserted) {
                        single = Error{ErrorCode::InternalError,
                                       "single-document fallback produced no outcome"};
                    } else {
                        single = inserted.error();
                    }
                } catch (const std::exception& ex) {
                    single = Error{ErrorCode::InternalError, ex.what()};
                } catch (...) {
                    single = Error{ErrorCode::InternalError, "insert threw unknown exception"};
                }
                items[i].promise.set_value(std::move(single));
            }
        } else {
            // Worker exception: fulfill every in-flight promise so callers do not block forever.
            for (auto& item : items) {
                item.promise.set_value(Result<DocumentInsertOutcome>(
                    Error{ErrorCode::InternalError, "metadata insert writer worker exception"}));
            }
        }
        metrics_.completedItems.fetch_add(batchSize, std::memory_order_relaxed);

        {
            std::lock_guard lock(mutex_);
            completedSequence_ = std::max(completedSequence_, lastSequence);
        }
        drainedCv_.notify_all();
    }

    std::deque<QueueItem> leftovers;
    {
        std::lock_guard lock(mutex_);
        leftovers.swap(queue_);
        if (!leftovers.empty()) {
            completedSequence_ = std::max(completedSequence_, leftovers.back().sequence);
        }
    }
    for (auto& item : leftovers) {
        item.promise.set_value(Result<DocumentInsertOutcome>(ErrorCode::InvalidState));
    }
    metrics_.completedItems.fetch_add(static_cast<std::uint64_t>(leftovers.size()),
                                      std::memory_order_relaxed);
    drainedCv_.notify_all();
}

Result<void> MetadataInsertWriter::flush() {
    std::unique_lock lock(mutex_);
    const auto target = nextSequence_ - 1;
    drainedCv_.wait(lock, [&] { return completedSequence_ >= target && queue_.empty(); });
    return Result<void>{};
}

MetadataInsertWriter::MetricsSnapshot MetadataInsertWriter::metricsSnapshot() const noexcept {
    MetricsSnapshot snapshot;
    snapshot.submittedItems = metrics_.submittedItems.load(std::memory_order_relaxed);
    snapshot.completedItems = metrics_.completedItems.load(std::memory_order_relaxed);
    snapshot.rejectedItems = metrics_.rejectedItems.load(std::memory_order_relaxed);
    snapshot.batches = metrics_.batches.load(std::memory_order_relaxed);
    snapshot.batchItems = metrics_.batchItems.load(std::memory_order_relaxed);
    snapshot.maxBatchSize = metrics_.maxBatchSize.load(std::memory_order_relaxed);
    snapshot.queueWaitSamples = metrics_.queueWaitSamples.load(std::memory_order_relaxed);
    snapshot.queueWaitTotalUs = metrics_.queueWaitTotalUs.load(std::memory_order_relaxed);
    snapshot.queueWaitMaxUs = metrics_.queueWaitMaxUs.load(std::memory_order_relaxed);
    snapshot.batchApplySamples = metrics_.batchApplySamples.load(std::memory_order_relaxed);
    snapshot.batchApplyTotalUs = metrics_.batchApplyTotalUs.load(std::memory_order_relaxed);
    snapshot.batchApplyMaxUs = metrics_.batchApplyMaxUs.load(std::memory_order_relaxed);
    snapshot.failedBatches = metrics_.failedBatches.load(std::memory_order_relaxed);
    snapshot.fallbackItems = metrics_.fallbackItems.load(std::memory_order_relaxed);
    return snapshot;
}

void MetadataInsertWriter::resetMetrics() noexcept {
    metrics_.submittedItems.store(0, std::memory_order_relaxed);
    metrics_.completedItems.store(0, std::memory_order_relaxed);
    metrics_.rejectedItems.store(0, std::memory_order_relaxed);
    metrics_.batches.store(0, std::memory_order_relaxed);
    metrics_.batchItems.store(0, std::memory_order_relaxed);
    metrics_.maxBatchSize.store(0, std::memory_order_relaxed);
    metrics_.queueWaitSamples.store(0, std::memory_order_relaxed);
    metrics_.queueWaitTotalUs.store(0, std::memory_order_relaxed);
    metrics_.queueWaitMaxUs.store(0, std::memory_order_relaxed);
    metrics_.batchApplySamples.store(0, std::memory_order_relaxed);
    metrics_.batchApplyTotalUs.store(0, std::memory_order_relaxed);
    metrics_.batchApplyMaxUs.store(0, std::memory_order_relaxed);
    metrics_.failedBatches.store(0, std::memory_order_relaxed);
    metrics_.fallbackItems.store(0, std::memory_order_relaxed);
}

void MetadataInsertWriter::shutdown() {
    {
        std::lock_guard lock(mutex_);
        if (stopping_) {
            return;
        }
        accepting_ = false;
        stopping_ = true;
    }
    cv_.notify_all();
    if (worker_.joinable()) {
        worker_.join();
    }
    drainedCv_.notify_all();
}

} // namespace yams::metadata
