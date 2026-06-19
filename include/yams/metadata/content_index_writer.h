#pragma once

#include <yams/core/types.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/metadata_repository.h>

#include <condition_variable>
#include <deque>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace yams::metadata {

/// Dedicated-thread writer for content-index commits.
///
/// Owns the content/FTS write stream (MetadataRepository::batchInsertContentAndIndex) on its own
/// worker thread, separate from the document-insert stream (MetadataInsertWriter) and the KG/status
/// stream (WriteCoordinator). Each submitted chunk is committed as its own transaction, preserving
/// the caller's per-chunk success/failure accounting.
///
/// Runs off the daemon io_context, so a caller on the io_context (PostIngestQueue) may block on the
/// returned future without deadlocking. Establishes a single, well-owned content-index write path
/// in preparation for concurrent write connections.
class ContentIndexWriter {
public:
    explicit ContentIndexWriter(std::shared_ptr<MetadataRepository> repo);
    ~ContentIndexWriter();

    ContentIndexWriter(const ContentIndexWriter&) = delete;
    ContentIndexWriter& operator=(const ContentIndexWriter&) = delete;

    std::future<Result<void>> submit(std::vector<BatchContentEntry> entries);
    void shutdown();

private:
    struct QueueItem {
        std::vector<BatchContentEntry> entries;
        std::promise<Result<void>> promise;
    };

    void run();

    std::shared_ptr<MetadataRepository> repo_;

    std::mutex mutex_;
    std::condition_variable cv_;
    std::deque<QueueItem> queue_;
    std::thread worker_;
    bool stopping_{false};
    bool accepting_{true};
};

} // namespace yams::metadata
