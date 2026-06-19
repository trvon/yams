#include <yams/metadata/metadata_insert_writer.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace yams::metadata {

namespace {

std::future<Result<int64_t>> readyFuture(Result<int64_t> result) {
    std::promise<Result<int64_t>> promise;
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

std::future<Result<int64_t>> MetadataInsertWriter::submit(BatchDocumentInsert record) {
    std::lock_guard lock(mutex_);
    if (!accepting_) {
        return readyFuture(Result<int64_t>(ErrorCode::InvalidState));
    }

    QueueItem item;
    item.sequence = nextSequence_++;
    item.enqueuedAt = std::chrono::steady_clock::now();
    item.record = std::move(record);
    auto future = item.promise.get_future();
    queue_.push_back(std::move(item));
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

        std::vector<BatchDocumentInsert> records;
        records.reserve(items.size());
        for (auto& item : items) {
            records.push_back(std::move(item.record));
        }

        const auto lastSequence = items.back().sequence;

        // Do the (potentially throwing) batch insert inside a guard so an unexpected exception
        // cannot terminate the worker thread or strand the in-flight promises.
        bool batchThrew = false;
        Result<std::vector<int64_t>> result = Error{ErrorCode::InternalError, "uninitialized"};
        try {
            result = repo_->batchInsertDocumentsWithMetadata(records);
        } catch (const std::exception& ex) {
            batchThrew = true;
            spdlog::error("[MetadataInsertWriter] batch insert threw: {}", ex.what());
        } catch (...) {
            batchThrew = true;
            spdlog::error("[MetadataInsertWriter] batch insert threw unknown exception");
        }

        if (!batchThrew && result) {
            const auto& ids = result.value();
            for (std::size_t i = 0; i < items.size(); ++i) {
                items[i].promise.set_value(i < ids.size()
                                               ? Result<int64_t>(ids[i])
                                               : Result<int64_t>(ErrorCode::InternalError));
            }
        } else if (!batchThrew) {
            // The batch transaction failed as a unit (e.g. one poison document rolling back its
            // batch-mates). Retry each document individually so only the genuinely-failing document
            // fails, preserving the pre-coalescing per-document isolation.
            for (std::size_t i = 0; i < items.size(); ++i) {
                Result<int64_t> single = Error{ErrorCode::InternalError, "uninitialized"};
                try {
                    auto& rec = records[i];
                    TreeSnapshotRecord* snap =
                        rec.snapshot.has_value() ? &rec.snapshot.value() : nullptr;
                    single = repo_->insertDocumentWithMetadata(rec.info, rec.tags, snap,
                                                               rec.updatePathTreeInTransaction);
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
                item.promise.set_value(Result<int64_t>(
                    Error{ErrorCode::InternalError, "metadata insert writer worker exception"}));
            }
        }

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
        item.promise.set_value(Result<int64_t>(ErrorCode::InvalidState));
    }
    drainedCv_.notify_all();
}

Result<void> MetadataInsertWriter::flush() {
    std::unique_lock lock(mutex_);
    const auto target = nextSequence_ - 1;
    drainedCv_.wait(lock, [&] { return completedSequence_ >= target && queue_.empty(); });
    return Result<void>{};
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
