#pragma once

#include <yams/core/types.h>
#include <yams/metadata/metadata_repository.h>

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace yams::metadata {

/// Dedicated-thread batching writer for document inserts.
///
/// Coalesces concurrent single-document inserts into one BEGIN IMMEDIATE transaction via
/// MetadataRepository::batchInsertDocumentsWithMetadata, amortizing per-document commit cost.
/// Each submit() returns a future resolving to that document's id, so synchronous callers (e.g.
/// DocumentService::store) preserve immediate-visibility semantics.
///
/// Runs on its own worker thread (NOT the daemon io_context), so a caller running on the
/// io_context may safely block on the returned future without deadlocking the strand-based
/// WriteCoordinator that shares that io_context.
class MetadataInsertWriter {
public:
    struct Options {
        std::size_t maxBatchCount{64};
        std::chrono::microseconds maxDelay{std::chrono::microseconds{100}};
    };

    explicit MetadataInsertWriter(std::shared_ptr<MetadataRepository> repo);
    MetadataInsertWriter(std::shared_ptr<MetadataRepository> repo, Options options);
    ~MetadataInsertWriter();

    MetadataInsertWriter(const MetadataInsertWriter&) = delete;
    MetadataInsertWriter& operator=(const MetadataInsertWriter&) = delete;

    std::future<Result<int64_t>> submit(BatchDocumentInsert record);
    Result<void> flush();
    void shutdown();

private:
    struct QueueItem {
        std::uint64_t sequence{0};
        std::chrono::steady_clock::time_point enqueuedAt{};
        BatchDocumentInsert record;
        std::promise<Result<int64_t>> promise;
    };

    void run();
    std::vector<QueueItem> takeBatch(std::unique_lock<std::mutex>& lock);

    std::shared_ptr<MetadataRepository> repo_;
    Options options_;

    std::mutex mutex_;
    std::condition_variable cv_;
    std::condition_variable drainedCv_;
    std::deque<QueueItem> queue_;
    std::thread worker_;
    bool stopping_{false};
    bool accepting_{true};
    std::uint64_t nextSequence_{1};
    std::uint64_t completedSequence_{0};
};

} // namespace yams::metadata
