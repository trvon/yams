#pragma once

#include <yams/core/types.h>
#include <yams/metadata/metadata_repository.h>

#include <atomic>
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
/// Each submit() returns a future resolving to that document's insert outcome, so synchronous
/// callers (e.g. DocumentService::store) preserve immediate-visibility semantics.
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

    struct MetricsSnapshot {
        std::uint64_t submittedItems{0};
        std::uint64_t completedItems{0};
        std::uint64_t rejectedItems{0};
        std::uint64_t batches{0};
        std::uint64_t batchItems{0};
        std::uint64_t maxBatchSize{0};
        std::uint64_t queueWaitSamples{0};
        std::uint64_t queueWaitTotalUs{0};
        std::uint64_t queueWaitMaxUs{0};
        std::uint64_t batchApplySamples{0};
        std::uint64_t batchApplyTotalUs{0};
        std::uint64_t batchApplyMaxUs{0};
        std::uint64_t failedBatches{0};
        std::uint64_t fallbackItems{0};
    };

    explicit MetadataInsertWriter(std::shared_ptr<MetadataRepository> repo);
    MetadataInsertWriter(std::shared_ptr<MetadataRepository> repo, Options options);
    ~MetadataInsertWriter();

    MetadataInsertWriter(const MetadataInsertWriter&) = delete;
    MetadataInsertWriter& operator=(const MetadataInsertWriter&) = delete;

    std::future<Result<DocumentInsertOutcome>> submit(BatchDocumentInsert record);
    Result<void> flush();
    [[nodiscard]] MetricsSnapshot metricsSnapshot() const noexcept;
    void resetMetrics() noexcept;
    /// Stops the worker and joins it. Single-owner contract: shutdown() and the destructor must be
    /// invoked by the one owning thread, not called concurrently from multiple threads.
    void shutdown();

private:
    struct QueueItem {
        std::uint64_t sequence{0};
        std::chrono::steady_clock::time_point enqueuedAt{};
        BatchDocumentInsert record;
        std::promise<Result<DocumentInsertOutcome>> promise;
    };

    void run();
    std::vector<QueueItem> takeBatch(std::unique_lock<std::mutex>& lock);

    struct AtomicMetrics {
        std::atomic<std::uint64_t> submittedItems{0};
        std::atomic<std::uint64_t> completedItems{0};
        std::atomic<std::uint64_t> rejectedItems{0};
        std::atomic<std::uint64_t> batches{0};
        std::atomic<std::uint64_t> batchItems{0};
        std::atomic<std::uint64_t> maxBatchSize{0};
        std::atomic<std::uint64_t> queueWaitSamples{0};
        std::atomic<std::uint64_t> queueWaitTotalUs{0};
        std::atomic<std::uint64_t> queueWaitMaxUs{0};
        std::atomic<std::uint64_t> batchApplySamples{0};
        std::atomic<std::uint64_t> batchApplyTotalUs{0};
        std::atomic<std::uint64_t> batchApplyMaxUs{0};
        std::atomic<std::uint64_t> failedBatches{0};
        std::atomic<std::uint64_t> fallbackItems{0};
    };

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
    AtomicMetrics metrics_;
};

} // namespace yams::metadata
