// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <yams/core/types.h>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace yams::daemon {

/**
 * @brief Priority levels for request queuing.
 *
 * Higher priority requests are dequeued first. Within the same priority,
 * requests are processed FIFO. Aging may promote lower priorities over time.
 */
enum class RequestPriority : uint8_t {
    High = 0,       ///< Status, health checks - always fast-path
    Normal = 1,     ///< Standard operations (search, list, grep)
    Low = 2,        ///< Bulk operations (add, update, delete)
    Background = 3  ///< Async tasks that can wait (embedding, indexing)
};

/**
 * @brief A queued request waiting for dispatch.
 */
struct QueuedRequest {
    uint64_t request_id{0};                                  ///< Unique request identifier
    uint64_t connection_id{0};                               ///< Owning connection
    std::vector<uint8_t> payload;                            ///< Serialized request data
    std::chrono::steady_clock::time_point enqueued_at;       ///< When request was queued
    RequestPriority priority{RequestPriority::Normal};       ///< Request priority
    std::function<void(Result<std::vector<uint8_t>>)> completion_callback;  ///< Called on completion
    std::string request_type;                                ///< For logging/metrics (e.g., "Search")
};

/**
 * @brief Bounded request queue with priority support and backpressure signaling.
 *
 * Implements a multi-level priority queue between connection acceptance and
 * request dispatch. Provides watermark-based backpressure detection and
 * timeout eviction for stale requests.
 *
 * ## Architecture
 * - 4 priority levels (High, Normal, Low, Background)
 * - MPMC-safe (multiple producers from connections, single consumer for dispatch)
 * - Watermark thresholds trigger backpressure callbacks
 * - Eviction coroutine removes stale requests
 *
 * ## Usage
 * ```cpp
 * RequestQueue::Config cfg;
 * cfg.max_queue_size = 4096;
 * auto queue = std::make_shared<RequestQueue>(cfg, coordinator->getExecutor());
 *
 * queue->set_backpressure_callback([](bool active, uint32_t retry_ms) {
 *     if (active) spdlog::warn("Backpressure active, retry in {}ms", retry_ms);
 * });
 *
 * queue->start();
 *
 * // Enqueue from connection handlers
 * if (!queue->try_enqueue(std::move(request))) {
 *     // Queue full - reject with retryAfterMs
 * }
 * ```
 */
class RequestQueue {
public:
    /**
     * @brief Configuration for RequestQueue.
     */
    struct Config {
        size_t max_queue_size = 4096;               ///< Maximum total requests across all priorities
        uint32_t high_watermark_percent = 80;       ///< % full to trigger backpressure
        uint32_t low_watermark_percent = 20;        ///< % full to clear backpressure
        std::chrono::milliseconds request_timeout{30000};  ///< Evict requests older than this
        std::chrono::milliseconds eviction_interval{1000}; ///< How often to check for stale requests
        std::chrono::milliseconds aging_interval{5000};    ///< Promote low-priority after this long
        bool enable_priority_queuing = true;        ///< If false, all requests treated as Normal
    };

    /**
     * @brief Metrics tracked by the queue.
     */
    struct Metrics {
        std::atomic<uint64_t> enqueued{0};              ///< Total requests enqueued
        std::atomic<uint64_t> dequeued{0};              ///< Total requests dequeued
        std::atomic<uint64_t> rejected{0};              ///< Rejected due to queue full
        std::atomic<uint64_t> evicted_timeout{0};       ///< Evicted due to timeout
        std::atomic<uint64_t> evicted_shutdown{0};      ///< Evicted due to shutdown
        std::atomic<uint64_t> backpressure_activations{0}; ///< Times backpressure was triggered
        std::atomic<uint64_t> current_depth{0};         ///< Current queue depth
        std::atomic<uint64_t> max_depth_seen{0};        ///< High water mark for depth
        std::atomic<uint64_t> total_wait_time_ms{0};    ///< Cumulative wait time
        std::atomic<uint64_t> promotions{0};            ///< Requests promoted due to aging
    };

    /**
     * @brief Callback invoked when backpressure state changes.
     *
     * @param active True if backpressure is now active, false if cleared
     * @param retry_after_ms Suggested retry delay in milliseconds
     */
    using BackpressureCallback = std::function<void(bool active, uint32_t retry_after_ms)>;

    /**
     * @brief Constructs a RequestQueue.
     *
     * @param config Queue configuration
     * @param executor Executor for background coroutines (eviction, aging)
     */
    explicit RequestQueue(Config config, boost::asio::any_io_executor executor);

    /**
     * @brief Destructor - stops background tasks and evicts remaining requests.
     */
    ~RequestQueue();

    // Non-copyable, non-movable (owns coroutines)
    RequestQueue(const RequestQueue&) = delete;
    RequestQueue& operator=(const RequestQueue&) = delete;
    RequestQueue(RequestQueue&&) = delete;
    RequestQueue& operator=(RequestQueue&&) = delete;

    /**
     * @brief Starts background coroutines (eviction, aging).
     *
     * Must be called before enqueuing requests.
     */
    void start();

    /**
     * @brief Stops background coroutines and clears the queue.
     *
     * Remaining requests receive cancellation errors via their callbacks.
     */
    void stop();

    /**
     * @brief Attempts to enqueue a request without blocking.
     *
     * @param request The request to enqueue (moved on success)
     * @return true if enqueued, false if queue is full
     */
    bool try_enqueue(QueuedRequest request);

    /**
     * @brief Dequeues the highest-priority request.
     *
     * @return The request, or std::nullopt if queue is empty
     */
    std::optional<QueuedRequest> try_dequeue();

    /**
     * @brief Async dequeue - waits for a request if queue is empty.
     *
     * @return Awaitable yielding the next request, or nullopt on shutdown
     */
    boost::asio::awaitable<std::optional<QueuedRequest>> async_dequeue();

    /**
     * @brief Sets the callback for backpressure state changes.
     */
    void set_backpressure_callback(BackpressureCallback cb);

    /**
     * @brief Current queue depth across all priorities.
     */
    size_t depth() const noexcept;

    /**
     * @brief Maximum queue capacity.
     */
    size_t capacity() const noexcept;

    /**
     * @brief Whether backpressure is currently active.
     */
    bool is_backpressured() const noexcept;

    /**
     * @brief Current utilization as a percentage (0-100).
     */
    float utilization_percent() const noexcept;

    /**
     * @brief Calculates suggested retry delay based on queue state.
     */
    uint32_t calculate_retry_after_ms() const noexcept;

    /**
     * @brief Read-only access to metrics.
     */
    const Metrics& metrics() const noexcept;

    /**
     * @brief Configuration (read-only).
     */
    const Config& config() const noexcept;

private:
    // Internal queue management
    void check_watermarks();
    void update_depth_metrics();

    // Background coroutines
    boost::asio::awaitable<void> eviction_loop();
    boost::asio::awaitable<void> aging_loop();

    // Configuration
    Config config_;
    boost::asio::any_io_executor executor_;

    // Priority queues (index = priority level)
    static constexpr size_t kNumPriorities = 4;
    std::array<std::deque<QueuedRequest>, kNumPriorities> queues_;
    mutable std::mutex mutex_;

    // State
    std::atomic<bool> running_{false};
    std::atomic<bool> backpressured_{false};
    std::shared_ptr<std::atomic<bool>> stop_flag_;

    // Callbacks
    BackpressureCallback backpressure_cb_;

    // Metrics
    Metrics metrics_;
};

} // namespace yams::daemon
