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

MetadataInsertWriter::MetadataInsertWriter(std::shared_ptr<MetadataRepository> repo, Options options)
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

        auto result = repo_->batchInsertDocumentsWithMetadata(records);

        const auto lastSequence = items.back().sequence;
        if (result) {
            const auto& ids = result.value();
            for (std::size_t i = 0; i < items.size(); ++i) {
                if (i < ids.size()) {
                    items[i].promise.set_value(Result<int64_t>(ids[i]));
                } else {
                    items[i].promise.set_value(Result<int64_t>(ErrorCode::InternalError));
                }
            }
        } else {
            for (auto& item : items) {
                item.promise.set_value(Result<int64_t>(result.error()));
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
