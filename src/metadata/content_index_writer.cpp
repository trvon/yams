#include <yams/metadata/content_index_writer.h>

#include <stdexcept>
#include <utility>

namespace yams::metadata {

namespace {

std::future<Result<void>> readyFuture(Result<void> result) {
    std::promise<Result<void>> promise;
    promise.set_value(std::move(result));
    return promise.get_future();
}

} // namespace

ContentIndexWriter::ContentIndexWriter(std::shared_ptr<MetadataRepository> repo)
    : repo_(std::move(repo)) {
    if (!repo_) {
        throw std::invalid_argument("ContentIndexWriter requires a MetadataRepository");
    }
    worker_ = std::thread([this] { run(); });
}

ContentIndexWriter::~ContentIndexWriter() {
    shutdown();
}

std::future<Result<void>> ContentIndexWriter::submit(std::vector<BatchContentEntry> entries) {
    if (entries.empty()) {
        return readyFuture(Result<void>{});
    }
    std::lock_guard lock(mutex_);
    if (!accepting_) {
        return readyFuture(Result<void>(ErrorCode::InvalidState));
    }
    QueueItem item;
    item.entries = std::move(entries);
    auto future = item.promise.get_future();
    queue_.push_back(std::move(item));
    cv_.notify_one();
    return future;
}

void ContentIndexWriter::run() {
    while (true) {
        QueueItem item;
        {
            std::unique_lock lock(mutex_);
            cv_.wait(lock, [&] { return stopping_ || !queue_.empty(); });
            if (queue_.empty() && stopping_) {
                break;
            }
            item = std::move(queue_.front());
            queue_.pop_front();
        }

        // Guard the (potentially throwing) commit so an unexpected exception cannot terminate the
        // worker thread or strand the in-flight promise.
        Result<void> result = Result<void>();
        try {
            result = repo_->batchInsertContentAndIndex(item.entries);
        } catch (const std::exception& ex) {
            result = Error{ErrorCode::InternalError, ex.what()};
        } catch (...) {
            result =
                Error{ErrorCode::InternalError, "content index insert threw unknown exception"};
        }
        item.promise.set_value(std::move(result));
    }

    std::deque<QueueItem> leftovers;
    {
        std::lock_guard lock(mutex_);
        leftovers.swap(queue_);
    }
    for (auto& leftover : leftovers) {
        leftover.promise.set_value(Result<void>(ErrorCode::InvalidState));
    }
}

void ContentIndexWriter::shutdown() {
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
}

} // namespace yams::metadata
