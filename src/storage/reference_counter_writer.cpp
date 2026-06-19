#include <yams/storage/reference_counter_writer.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <optional>
#include <stdexcept>
#include <utility>

namespace yams::storage {

namespace {

struct AtomicWriterTiming {
    std::atomic<std::uint64_t> calls{0};
    std::atomic<std::uint64_t> totalUs{0};
    std::atomic<std::uint64_t> maxUs{0};
};

struct AtomicWriterValueMetric {
    std::atomic<std::uint64_t> calls{0};
    std::atomic<std::uint64_t> total{0};
    std::atomic<std::uint64_t> max{0};
};

constexpr std::array<std::string_view, 10> kWriterTimingNames{
    "queue_wait",         "batch_delay_wait",   "commit_batches_total", "batch_db_mutex_wait",
    "batch_begin",        "batch_apply_ops",    "batch_audit_rows",     "batch_update_state",
    "batch_update_stats", "batch_sqlite_commit"};

constexpr std::array<std::string_view, 5> kWriterValueMetricNames{
    "submitted_batches", "submitted_ops", "writer_batch_size", "writer_batch_ops", "committed_ops"};

std::array<AtomicWriterTiming, kWriterTimingNames.size()>& writerTimings() {
    static std::array<AtomicWriterTiming, kWriterTimingNames.size()> timings;
    return timings;
}

std::array<AtomicWriterValueMetric, kWriterValueMetricNames.size()>& writerValueMetrics() {
    static std::array<AtomicWriterValueMetric, kWriterValueMetricNames.size()> metrics;
    return metrics;
}

template <std::size_t N>
std::optional<std::size_t> metricIndex(const std::array<std::string_view, N>& names,
                                       std::string_view metric) {
    for (std::size_t i = 0; i < names.size(); ++i) {
        if (names[i] == metric) {
            return i;
        }
    }
    return std::nullopt;
}

void updateMaxRelaxed(std::atomic<std::uint64_t>& target, std::uint64_t value) {
    auto current = target.load(std::memory_order_relaxed);
    while (current < value &&
           !target.compare_exchange_weak(current, value, std::memory_order_relaxed,
                                         std::memory_order_relaxed)) {
    }
}

std::future<Result<void>> readyFuture(Result<void> result) {
    std::promise<Result<void>> promise;
    promise.set_value(std::move(result));
    return promise.get_future();
}

} // namespace

void resetRefCounterWriterMetrics() {
    for (auto& timing : writerTimings()) {
        timing.calls.store(0, std::memory_order_relaxed);
        timing.totalUs.store(0, std::memory_order_relaxed);
        timing.maxUs.store(0, std::memory_order_relaxed);
    }
    for (auto& metric : writerValueMetrics()) {
        metric.calls.store(0, std::memory_order_relaxed);
        metric.total.store(0, std::memory_order_relaxed);
        metric.max.store(0, std::memory_order_relaxed);
    }
}

std::unordered_map<std::string, RefCounterWriterTiming> getRefCounterWriterTimingsSnapshot() {
    std::unordered_map<std::string, RefCounterWriterTiming> snapshot;
    snapshot.reserve(kWriterTimingNames.size());
    const auto& timings = writerTimings();
    for (std::size_t i = 0; i < kWriterTimingNames.size(); ++i) {
        snapshot.emplace(std::string(kWriterTimingNames[i]),
                         RefCounterWriterTiming{timings[i].calls.load(std::memory_order_relaxed),
                                                timings[i].totalUs.load(std::memory_order_relaxed),
                                                timings[i].maxUs.load(std::memory_order_relaxed)});
    }
    return snapshot;
}

std::unordered_map<std::string, RefCounterWriterValueMetric>
getRefCounterWriterValueMetricsSnapshot() {
    std::unordered_map<std::string, RefCounterWriterValueMetric> snapshot;
    snapshot.reserve(kWriterValueMetricNames.size());
    const auto& metrics = writerValueMetrics();
    for (std::size_t i = 0; i < kWriterValueMetricNames.size(); ++i) {
        snapshot.emplace(
            std::string(kWriterValueMetricNames[i]),
            RefCounterWriterValueMetric{metrics[i].calls.load(std::memory_order_relaxed),
                                        metrics[i].total.load(std::memory_order_relaxed),
                                        metrics[i].max.load(std::memory_order_relaxed)});
    }
    return snapshot;
}

namespace detail {

void recordRefCounterWriterTiming(std::string_view phase,
                                  std::chrono::steady_clock::time_point start) {
    const auto elapsedUs = std::chrono::duration_cast<std::chrono::microseconds>(
                               std::chrono::steady_clock::now() - start)
                               .count();
    recordRefCounterWriterTimingUs(phase,
                                   static_cast<std::uint64_t>(std::max<int64_t>(0, elapsedUs)));
}

void recordRefCounterWriterTimingUs(std::string_view phase, std::uint64_t elapsedUs) {
    const auto index = metricIndex(kWriterTimingNames, phase);
    if (!index) {
        return;
    }
    auto& timing = writerTimings()[*index];
    timing.calls.fetch_add(1, std::memory_order_relaxed);
    timing.totalUs.fetch_add(elapsedUs, std::memory_order_relaxed);
    updateMaxRelaxed(timing.maxUs, elapsedUs);
}

void recordRefCounterWriterValue(std::string_view metric, std::uint64_t value) {
    const auto index = metricIndex(kWriterValueMetricNames, metric);
    if (!index) {
        return;
    }
    auto& target = writerValueMetrics()[*index];
    target.calls.fetch_add(1, std::memory_order_relaxed);
    target.total.fetch_add(value, std::memory_order_relaxed);
    updateMaxRelaxed(target.max, value);
}

} // namespace detail

RefCounterWriter::RefCounterWriter(std::shared_ptr<ReferenceCounter> counter)
    : RefCounterWriter(std::move(counter), Options{}) {}

RefCounterWriter::RefCounterWriter(std::shared_ptr<ReferenceCounter> counter, Options options)
    : counter_(std::move(counter)), options_(options) {
    if (!counter_) {
        throw std::invalid_argument("RefCounterWriter requires a ReferenceCounter");
    }
    if (options_.maxBatchCount == 0) {
        options_.maxBatchCount = 1;
    }
    worker_ = std::thread([this] { run(); });
}

RefCounterWriter::~RefCounterWriter() {
    shutdown();
}

std::future<Result<void>> RefCounterWriter::submit(RefTransactionBatch batch) {
    if (batch.operations.empty()) {
        return readyFuture(Result<void>{});
    }

    const auto opCount = static_cast<std::uint64_t>(batch.operations.size());
    detail::recordRefCounterWriterValue("submitted_batches", 1);
    detail::recordRefCounterWriterValue("submitted_ops", opCount);

    std::lock_guard lock(mutex_);
    if (!accepting_) {
        return readyFuture(Result<void>(ErrorCode::InvalidState));
    }

    QueueItem item;
    item.sequence = nextSequence_++;
    item.enqueuedAt = std::chrono::steady_clock::now();
    item.batch = std::move(batch);
    auto future = item.promise.get_future();
    queue_.push_back(std::move(item));
    cv_.notify_one();
    return future;
}

Result<void> RefCounterWriter::flush() {
    std::unique_lock lock(mutex_);
    const auto target = nextSequence_ - 1;
    drainedCv_.wait(lock, [&] { return completedSequence_ >= target && queue_.empty(); });
    return Result<void>{};
}

void RefCounterWriter::shutdown() {
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

std::vector<RefCounterWriter::QueueItem>
RefCounterWriter::takeBatch(std::unique_lock<std::mutex>& lock) {
    if (queue_.empty()) {
        return {};
    }

    if (options_.maxDelay.count() > 0 && queue_.size() < options_.maxBatchCount && !stopping_) {
        const auto delayStart = std::chrono::steady_clock::now();
        cv_.wait_for(lock, options_.maxDelay,
                     [&] { return stopping_ || queue_.size() >= options_.maxBatchCount; });
        detail::recordRefCounterWriterTiming("batch_delay_wait", delayStart);
    }

    const auto count = std::min(options_.maxBatchCount, queue_.size());
    std::vector<QueueItem> items;
    items.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        items.push_back(std::move(queue_.front()));
        queue_.pop_front();
    }
    return items;
}

void RefCounterWriter::run() {
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

        const auto now = std::chrono::steady_clock::now();
        std::uint64_t opCount = 0;
        std::vector<RefTransactionBatch> batches;
        batches.reserve(items.size());
        for (auto& item : items) {
            if (item.enqueuedAt.time_since_epoch().count() > 0) {
                detail::recordRefCounterWriterTimingUs(
                    "queue_wait",
                    static_cast<std::uint64_t>(
                        std::chrono::duration_cast<std::chrono::microseconds>(now - item.enqueuedAt)
                            .count()));
            }
            opCount += static_cast<std::uint64_t>(item.batch.operations.size());
            batches.push_back(std::move(item.batch));
        }
        detail::recordRefCounterWriterValue("writer_batch_size",
                                            static_cast<std::uint64_t>(batches.size()));
        detail::recordRefCounterWriterValue("writer_batch_ops", opCount);

        const auto commitStart = std::chrono::steady_clock::now();
        auto result = counter_->commitTransactionBatches(
            std::span<const RefTransactionBatch>(batches.data(), batches.size()));
        detail::recordRefCounterWriterTiming("commit_batches_total", commitStart);

        const auto lastSequence = items.back().sequence;
        for (auto& item : items) {
            item.promise.set_value(result);
        }

        {
            std::lock_guard lock(mutex_);
            completedSequence_ = std::max(completedSequence_, lastSequence);
        }
        drainedCv_.notify_all();
    }

    // Defensive completion for any item left if construction/logic changes later.
    std::deque<QueueItem> leftovers;
    {
        std::lock_guard lock(mutex_);
        leftovers.swap(queue_);
        if (!leftovers.empty()) {
            completedSequence_ = std::max(completedSequence_, leftovers.back().sequence);
        }
    }
    for (auto& item : leftovers) {
        item.promise.set_value(Result<void>(ErrorCode::InvalidState));
    }
    drainedCv_.notify_all();
}

} // namespace yams::storage
