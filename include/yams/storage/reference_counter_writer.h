#pragma once

#include <yams/core/types.h>
#include <yams/storage/reference_counter.h>

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

namespace yams::storage {

struct RefCounterWriterTiming {
    std::uint64_t calls{0};
    std::uint64_t totalUs{0};
    std::uint64_t maxUs{0};
};

struct RefCounterWriterValueMetric {
    std::uint64_t calls{0};
    std::uint64_t total{0};
    std::uint64_t max{0};
};

void resetRefCounterWriterMetrics();
std::unordered_map<std::string, RefCounterWriterTiming> getRefCounterWriterTimingsSnapshot();
std::unordered_map<std::string, RefCounterWriterValueMetric>
getRefCounterWriterValueMetricsSnapshot();

namespace detail {
void recordRefCounterWriterTiming(std::string_view phase,
                                  std::chrono::steady_clock::time_point start);
void recordRefCounterWriterTimingUs(std::string_view phase, std::uint64_t elapsedUs);
void recordRefCounterWriterValue(std::string_view metric, std::uint64_t value);
} // namespace detail

class RefCounterWriter {
public:
    struct Options {
        size_t maxBatchCount{64};
        std::chrono::microseconds maxDelay{std::chrono::microseconds{100}};
    };

    explicit RefCounterWriter(std::shared_ptr<ReferenceCounter> counter);
    RefCounterWriter(std::shared_ptr<ReferenceCounter> counter, Options options);
    ~RefCounterWriter();

    RefCounterWriter(const RefCounterWriter&) = delete;
    RefCounterWriter& operator=(const RefCounterWriter&) = delete;

    std::future<Result<void>> submit(RefTransactionBatch batch);
    Result<void> flush();
    /// Stops the worker and joins it. Single-owner contract: shutdown() and the destructor must be
    /// invoked by the one owning thread, not called concurrently from multiple threads.
    void shutdown();

private:
    struct QueueItem {
        std::uint64_t sequence{0};
        std::chrono::steady_clock::time_point enqueuedAt{};
        RefTransactionBatch batch;
        std::promise<Result<void>> promise;
    };

    void run();
    std::vector<QueueItem> takeBatch(std::unique_lock<std::mutex>& lock);

    std::shared_ptr<ReferenceCounter> counter_;
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

} // namespace yams::storage
