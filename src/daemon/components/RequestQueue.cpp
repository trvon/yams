// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <yams/daemon/components/RequestQueue.h>
#include <yams/daemon/components/TuneAdvisor.h>

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <future>

namespace yams::daemon {

RequestQueue::RequestQueue(Config config, boost::asio::any_io_executor executor)
    : config_(std::move(config)), executor_(std::move(executor)),
      stop_flag_(std::make_shared<std::atomic<bool>>(false)) {
    spdlog::debug("[RequestQueue] Created with capacity={}, high_watermark={}%, low_watermark={}%",
                  config_.max_queue_size, config_.high_watermark_percent,
                  config_.low_watermark_percent);
}

RequestQueue::~RequestQueue() {
    if (running_.load(std::memory_order_acquire)) {
        stop();
    }
}

void RequestQueue::start() {
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        spdlog::debug("[RequestQueue] Already running, skipping start");
        return;
    }

    spdlog::info("[RequestQueue] Starting background tasks");
    cancel_signal_ = std::make_shared<boost::asio::cancellation_signal>();
    stop_flag_->store(false, std::memory_order_release);

    eviction_timer_ = std::make_unique<boost::asio::steady_timer>(executor_);
    eviction_future_ = boost::asio::co_spawn(
        executor_, [this]() -> boost::asio::awaitable<void> { co_await eviction_loop(); },
        boost::asio::use_future);

    // Launch eviction coroutine
    // Launch aging coroutine (promotes stale low-priority requests)
    if (config_.enable_priority_queuing) {
        aging_timer_ = std::make_unique<boost::asio::steady_timer>(executor_);
        aging_future_ = boost::asio::co_spawn(
            executor_, [this]() -> boost::asio::awaitable<void> { co_await aging_loop(); },
            boost::asio::use_future);
    }
}

void RequestQueue::stop() {
    bool expected = true;
    if (!running_.compare_exchange_strong(expected, false, std::memory_order_acq_rel)) {
        spdlog::debug("[RequestQueue] Stop called but not running");
        return;
    }

    spdlog::info("[RequestQueue] Stopping and clearing queue");
    stop_flag_->store(true, std::memory_order_release);

    // Post timer cancellations to the executor to avoid racing with async_wait
    // operations running on IO threads. The post ensures cancellation happens
    // on the same implicit strand as the timer operations.
    std::promise<void> cancel_done;
    auto cancel_future = cancel_done.get_future();
    boost::asio::post(executor_, [this, done = std::move(cancel_done)]() mutable {
        if (eviction_timer_) {
            eviction_timer_->cancel();
        }
        if (aging_timer_) {
            aging_timer_->cancel();
        }
        done.set_value();
    });
    // Wait for cancellation to complete before proceeding
    cancel_future.wait();

    // Drain all queues and invoke callbacks with cancellation error
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto& queue : queues_) {
            while (!queue.empty()) {
                auto req = std::move(queue.front());
                queue.pop_front();
                metrics_.evicted_shutdown.fetch_add(1, std::memory_order_relaxed);
                if (req.completion_callback) {
                    try {
                        req.completion_callback(
                            Error{ErrorCode::OperationCancelled, "Request queue shutting down"});
                    } catch (const std::exception& e) {
                        spdlog::warn("[RequestQueue] Exception in completion callback: {}",
                                     e.what());
                    }
                }
            }
        }
        metrics_.current_depth.store(0, std::memory_order_relaxed);
    }

    auto await_future = [](std::future<void>& fut) {
        if (!fut.valid()) {
            return;
        }
        try {
            fut.get();
        } catch (const std::exception& e) {
            spdlog::warn("[RequestQueue] Background task exited with exception: {}", e.what());
        }
        fut = std::future<void>();
    };

    await_future(eviction_future_);
    await_future(aging_future_);

    eviction_timer_.reset();
    aging_timer_.reset();
}

bool RequestQueue::try_enqueue(QueuedRequest request) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Check capacity
    size_t total_depth = 0;
    for (const auto& q : queues_) {
        total_depth += q.size();
    }

    if (total_depth >= config_.max_queue_size) {
        metrics_.rejected.fetch_add(1, std::memory_order_relaxed);
        spdlog::debug("[RequestQueue] Rejected request (queue full: {}/{})", total_depth,
                      config_.max_queue_size);
        return false;
    }

    // Determine queue index
    size_t queue_idx = config_.enable_priority_queuing
                           ? static_cast<size_t>(request.priority)
                           : static_cast<size_t>(RequestPriority::Normal);
    if (queue_idx >= kNumPriorities) {
        queue_idx = static_cast<size_t>(RequestPriority::Normal);
    }

    // Set enqueue time if not already set
    if (request.enqueued_at == std::chrono::steady_clock::time_point{}) {
        request.enqueued_at = std::chrono::steady_clock::now();
    }

    queues_[queue_idx].push_back(std::move(request));
    metrics_.enqueued.fetch_add(1, std::memory_order_relaxed);

    // Update metrics (under lock, so safe to compute)
    total_depth++;
    metrics_.current_depth.store(total_depth, std::memory_order_relaxed);
    uint64_t max_seen = metrics_.max_depth_seen.load(std::memory_order_relaxed);
    while (total_depth > max_seen) {
        if (metrics_.max_depth_seen.compare_exchange_weak(max_seen, total_depth,
                                                          std::memory_order_relaxed)) {
            break;
        }
    }

    // Check watermarks (may invoke callback outside lock)
    check_watermarks();

    return true;
}

std::optional<QueuedRequest> RequestQueue::try_dequeue() {
    std::lock_guard<std::mutex> lock(mutex_);

    // Check queues in priority order
    for (size_t i = 0; i < kNumPriorities; ++i) {
        if (!queues_[i].empty()) {
            auto request = std::move(queues_[i].front());
            queues_[i].pop_front();

            // Update metrics
            metrics_.dequeued.fetch_add(1, std::memory_order_relaxed);
            auto now = std::chrono::steady_clock::now();
            auto wait_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(now - request.enqueued_at)
                    .count();
            metrics_.total_wait_time_ms.fetch_add(static_cast<uint64_t>(wait_ms),
                                                  std::memory_order_relaxed);

            update_depth_metrics();
            check_watermarks();

            return request;
        }
    }

    return std::nullopt;
}

boost::asio::awaitable<std::optional<QueuedRequest>> RequestQueue::async_dequeue() {
    using namespace std::chrono_literals;

    while (!stop_flag_->load(std::memory_order_acquire)) {
        // Try to dequeue
        if (auto req = try_dequeue()) {
            co_return req;
        }

        // Wait a bit before retrying (simple polling; could use condition variable)
        boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);
        timer.expires_after(10ms);
        auto [ec] = co_await timer.async_wait(boost::asio::as_tuple(boost::asio::use_awaitable));
        (void)ec; // Ignore timer errors
    }

    co_return std::nullopt;
}

void RequestQueue::set_backpressure_callback(BackpressureCallback cb) {
    std::lock_guard<std::mutex> lock(mutex_);
    backpressure_cb_ = std::move(cb);
}

size_t RequestQueue::depth() const noexcept {
    return metrics_.current_depth.load(std::memory_order_relaxed);
}

size_t RequestQueue::capacity() const noexcept {
    return config_.max_queue_size;
}

bool RequestQueue::is_backpressured() const noexcept {
    return backpressured_.load(std::memory_order_relaxed);
}

float RequestQueue::utilization_percent() const noexcept {
    if (config_.max_queue_size == 0)
        return 0.0f;
    return static_cast<float>(depth()) / static_cast<float>(config_.max_queue_size) * 100.0f;
}

uint32_t RequestQueue::calculate_retry_after_ms() const noexcept {
    float util = utilization_percent();
    if (util < 50.0f) {
        return 0; // No backpressure needed
    }

    // Base delay scales with utilization
    // 50% -> 50ms, 75% -> 150ms, 90% -> 400ms, 100% -> 1000ms
    uint32_t base_ms = 0;
    if (util >= 100.0f) {
        base_ms = 1000;
    } else if (util >= 90.0f) {
        base_ms = 400;
    } else if (util >= 75.0f) {
        base_ms = 150;
    } else {
        base_ms = 50;
    }

    // Add component based on average wait time
    uint64_t dequeued = metrics_.dequeued.load(std::memory_order_relaxed);
    if (dequeued > 0) {
        uint64_t avg_wait = metrics_.total_wait_time_ms.load(std::memory_order_relaxed) / dequeued;
        // Add 10% of average wait time, capped at 500ms
        base_ms += std::min<uint32_t>(static_cast<uint32_t>(avg_wait / 10), 500);
    }

    return std::min(base_ms, uint32_t{2000}); // Cap at 2 seconds
}

const RequestQueue::Metrics& RequestQueue::metrics() const noexcept {
    return metrics_;
}

const RequestQueue::Config& RequestQueue::config() const noexcept {
    return config_;
}

void RequestQueue::check_watermarks() {
    // Called with mutex held
    float util = utilization_percent();
    bool was_backpressured = backpressured_.load(std::memory_order_relaxed);
    bool now_backpressured = was_backpressured;

    // Hysteresis: only change state at watermarks
    if (!was_backpressured && util >= static_cast<float>(config_.high_watermark_percent)) {
        now_backpressured = true;
        metrics_.backpressure_activations.fetch_add(1, std::memory_order_relaxed);
        spdlog::warn("[RequestQueue] Backpressure activated at {:.1f}% utilization", util);
    } else if (was_backpressured && util <= static_cast<float>(config_.low_watermark_percent)) {
        now_backpressured = false;
        spdlog::info("[RequestQueue] Backpressure cleared at {:.1f}% utilization", util);
    }

    if (was_backpressured != now_backpressured) {
        backpressured_.store(now_backpressured, std::memory_order_relaxed);
        TuneAdvisor::setRequestQueueBackpressure(now_backpressured);

        // Invoke callback (copy to avoid holding lock during callback)
        BackpressureCallback cb = backpressure_cb_;
        if (cb) {
            uint32_t retry_ms = calculate_retry_after_ms();
            // Release lock before callback to avoid deadlock
            // Note: We're already holding mutex_, so we need to be careful
            // In this design, the callback should be quick and non-blocking
            try {
                cb(now_backpressured, retry_ms);
            } catch (const std::exception& e) {
                spdlog::warn("[RequestQueue] Exception in backpressure callback: {}", e.what());
            }
        }
    }
}

void RequestQueue::update_depth_metrics() {
    // Called with mutex held
    size_t total = 0;
    for (const auto& q : queues_) {
        total += q.size();
    }
    metrics_.current_depth.store(total, std::memory_order_relaxed);
    TuneAdvisor::setRequestQueueDepth(static_cast<uint32_t>(total));
}

boost::asio::awaitable<void> RequestQueue::eviction_loop() {
    using namespace std::chrono_literals;
    spdlog::debug("[RequestQueue] Eviction loop started");

    if (!eviction_timer_) {
        co_return;
    }
    auto& timer = *eviction_timer_;

    while (!stop_flag_->load(std::memory_order_acquire)) {
        timer.expires_after(config_.eviction_interval);
        // Use as_tuple to avoid exceptions on cancellation. Timer cancellation via
        // timer.cancel() in stop() is sufficient - no need for cancellation_slot binding
        // which can race with emit().
        auto [ec] = co_await timer.async_wait(boost::asio::as_tuple(boost::asio::use_awaitable));
        if (ec == boost::asio::error::operation_aborted ||
            stop_flag_->load(std::memory_order_acquire)) {
            break;
        }

        auto now = std::chrono::steady_clock::now();
        std::vector<QueuedRequest> evicted;

        {
            std::lock_guard<std::mutex> lock(mutex_);
            for (auto& queue : queues_) {
                // Remove requests older than timeout
                auto it = queue.begin();
                while (it != queue.end()) {
                    auto age = std::chrono::duration_cast<std::chrono::milliseconds>(
                        now - it->enqueued_at);
                    if (age > config_.request_timeout) {
                        evicted.push_back(std::move(*it));
                        it = queue.erase(it);
                    } else {
                        ++it;
                    }
                }
            }
            update_depth_metrics();
        }

        // Invoke callbacks outside lock
        for (auto& req : evicted) {
            metrics_.evicted_timeout.fetch_add(1, std::memory_order_relaxed);
            spdlog::debug("[RequestQueue] Evicted stale request {} (type={})", req.request_id,
                          req.request_type);
            if (req.completion_callback) {
                try {
                    req.completion_callback(
                        Error{ErrorCode::Timeout, "Request timed out in queue"});
                } catch (const std::exception& e) {
                    spdlog::warn("[RequestQueue] Exception in eviction callback: {}", e.what());
                }
            }
        }

        if (!evicted.empty()) {
            spdlog::info("[RequestQueue] Evicted {} stale requests", evicted.size());
            check_watermarks();
        }
    }

    spdlog::debug("[RequestQueue] Eviction loop stopped");
}

boost::asio::awaitable<void> RequestQueue::aging_loop() {
    using namespace std::chrono_literals;
    spdlog::debug("[RequestQueue] Aging loop started");

    if (!aging_timer_) {
        co_return;
    }
    auto& timer = *aging_timer_;

    while (!stop_flag_->load(std::memory_order_acquire)) {
        timer.expires_after(config_.aging_interval);
        // Use as_tuple to avoid exceptions on cancellation. Timer cancellation via
        // timer.cancel() in stop() is sufficient - no need for cancellation_slot binding
        // which can race with emit().
        auto [ec] = co_await timer.async_wait(boost::asio::as_tuple(boost::asio::use_awaitable));
        if (ec == boost::asio::error::operation_aborted ||
            stop_flag_->load(std::memory_order_acquire)) {
            break;
        }

        auto now = std::chrono::steady_clock::now();
        size_t promoted = 0;

        {
            std::lock_guard<std::mutex> lock(mutex_);

            // Promote requests from lower priorities to higher ones if they've waited too long
            // Only promote Background -> Low -> Normal (never to High)
            for (size_t src_idx = kNumPriorities - 1; src_idx > 1; --src_idx) {
                auto& src_queue = queues_[src_idx];
                auto& dst_queue = queues_[src_idx - 1];

                auto it = src_queue.begin();
                while (it != src_queue.end()) {
                    auto age = std::chrono::duration_cast<std::chrono::milliseconds>(
                        now - it->enqueued_at);
                    // Promote if waited longer than aging interval
                    if (age > config_.aging_interval) {
                        auto req = std::move(*it);
                        req.priority = static_cast<RequestPriority>(src_idx - 1);
                        dst_queue.push_back(std::move(req));
                        it = src_queue.erase(it);
                        promoted++;
                    } else {
                        ++it;
                    }
                }
            }
        }

        if (promoted > 0) {
            metrics_.promotions.fetch_add(promoted, std::memory_order_relaxed);
            spdlog::debug("[RequestQueue] Promoted {} aged requests", promoted);
        }
    }

    spdlog::debug("[RequestQueue] Aging loop stopped");
}

} // namespace yams::daemon
