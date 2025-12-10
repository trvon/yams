// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Unit tests for RequestQueue component

#include <gtest/gtest.h>

#include <yams/daemon/components/RequestQueue.h>

#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

using namespace yams::daemon;
using namespace std::chrono_literals;

namespace yams::daemon::test {

#define SKIP_REQUEST_QUEUE_TEST_ON_WINDOWS() ((void)0)

class RequestQueueTest : public ::testing::Test {
protected:
    void SetUp() override {
        SKIP_REQUEST_QUEUE_TEST_ON_WINDOWS();
        
        io_ = std::make_shared<boost::asio::io_context>();
        work_guard_ = std::make_unique<
            boost::asio::executor_work_guard<boost::asio::io_context::executor_type>>(
            io_->get_executor());

        // Run io_context in background thread
        io_thread_ = std::thread([this]() { io_->run(); });
    }

    void TearDown() override {
        // Guard against skipped tests where io_ is never initialized
        if (!io_) {
            return;
        }
        work_guard_.reset();
        io_->stop();
        if (io_thread_.joinable()) {
            io_thread_.join();
        }
    }

    QueuedRequest makeRequest(uint64_t id, RequestPriority priority = RequestPriority::Normal) {
        QueuedRequest req;
        req.request_id = id;
        req.connection_id = 1;
        req.priority = priority;
        req.enqueued_at = std::chrono::steady_clock::now();
        req.request_type = "Test";
        return req;
    }

    std::shared_ptr<boost::asio::io_context> io_;
    std::unique_ptr<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>>
        work_guard_;
    std::thread io_thread_;
};

TEST_F(RequestQueueTest, BasicEnqueueDequeue) {
    RequestQueue::Config cfg;
    cfg.max_queue_size = 100;
    RequestQueue queue(cfg, io_->get_executor());
    queue.start();

    auto req = makeRequest(1);
    EXPECT_TRUE(queue.try_enqueue(std::move(req)));
    EXPECT_EQ(queue.depth(), 1);

    auto dequeued = queue.try_dequeue();
    ASSERT_TRUE(dequeued.has_value());
    EXPECT_EQ(dequeued->request_id, 1);
    EXPECT_EQ(queue.depth(), 0);

    queue.stop();
}

TEST_F(RequestQueueTest, QueueFull_RejectsNew) {
    RequestQueue::Config cfg;
    cfg.max_queue_size = 3;
    RequestQueue queue(cfg, io_->get_executor());
    queue.start();

    EXPECT_TRUE(queue.try_enqueue(makeRequest(1)));
    EXPECT_TRUE(queue.try_enqueue(makeRequest(2)));
    EXPECT_TRUE(queue.try_enqueue(makeRequest(3)));
    EXPECT_EQ(queue.depth(), 3);

    // Queue is full, should reject
    EXPECT_FALSE(queue.try_enqueue(makeRequest(4)));
    EXPECT_EQ(queue.metrics().rejected.load(), 1);

    queue.stop();
}

TEST_F(RequestQueueTest, PriorityOrdering) {
    RequestQueue::Config cfg;
    cfg.max_queue_size = 100;
    cfg.enable_priority_queuing = true;
    RequestQueue queue(cfg, io_->get_executor());
    queue.start();

    // Enqueue in reverse priority order
    queue.try_enqueue(makeRequest(1, RequestPriority::Background));
    queue.try_enqueue(makeRequest(2, RequestPriority::Low));
    queue.try_enqueue(makeRequest(3, RequestPriority::Normal));
    queue.try_enqueue(makeRequest(4, RequestPriority::High));

    // Should dequeue in priority order (High first)
    auto r1 = queue.try_dequeue();
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(r1->request_id, 4); // High priority

    auto r2 = queue.try_dequeue();
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(r2->request_id, 3); // Normal priority

    auto r3 = queue.try_dequeue();
    ASSERT_TRUE(r3.has_value());
    EXPECT_EQ(r3->request_id, 2); // Low priority

    auto r4 = queue.try_dequeue();
    ASSERT_TRUE(r4.has_value());
    EXPECT_EQ(r4->request_id, 1); // Background priority

    queue.stop();
}

TEST_F(RequestQueueTest, FIFOWithinPriority) {
    RequestQueue::Config cfg;
    cfg.max_queue_size = 100;
    RequestQueue queue(cfg, io_->get_executor());
    queue.start();

    // Enqueue same priority
    queue.try_enqueue(makeRequest(1, RequestPriority::Normal));
    queue.try_enqueue(makeRequest(2, RequestPriority::Normal));
    queue.try_enqueue(makeRequest(3, RequestPriority::Normal));

    // Should dequeue in FIFO order
    EXPECT_EQ(queue.try_dequeue()->request_id, 1);
    EXPECT_EQ(queue.try_dequeue()->request_id, 2);
    EXPECT_EQ(queue.try_dequeue()->request_id, 3);

    queue.stop();
}

TEST_F(RequestQueueTest, BackpressureWatermarks) {
    RequestQueue::Config cfg;
    cfg.max_queue_size = 10;
    cfg.high_watermark_percent = 80; // 8 items
    cfg.low_watermark_percent = 20;  // 2 items
    RequestQueue queue(cfg, io_->get_executor());

    std::atomic<bool> backpressure_active{false};
    std::atomic<int> callback_count{0};

    queue.set_backpressure_callback([&](bool active, uint32_t retry_ms) {
        backpressure_active.store(active);
        callback_count.fetch_add(1);
    });

    queue.start();

    // Fill to 80% (8 items) - should trigger backpressure
    for (int i = 0; i < 8; ++i) {
        queue.try_enqueue(makeRequest(i));
    }

    // Give time for callback
    std::this_thread::sleep_for(10ms);
    EXPECT_TRUE(backpressure_active.load());
    EXPECT_GE(queue.metrics().backpressure_activations.load(), 1);

    // Drain to below 20% (2 items)
    for (int i = 0; i < 6; ++i) {
        queue.try_dequeue();
    }

    // Give time for callback
    std::this_thread::sleep_for(10ms);
    EXPECT_FALSE(backpressure_active.load());

    queue.stop();
}

TEST_F(RequestQueueTest, UtilizationPercent) {
    RequestQueue::Config cfg;
    cfg.max_queue_size = 100;
    RequestQueue queue(cfg, io_->get_executor());
    queue.start();

    EXPECT_FLOAT_EQ(queue.utilization_percent(), 0.0f);

    for (int i = 0; i < 50; ++i) {
        queue.try_enqueue(makeRequest(i));
    }
    EXPECT_FLOAT_EQ(queue.utilization_percent(), 50.0f);

    for (int i = 0; i < 50; ++i) {
        queue.try_enqueue(makeRequest(100 + i));
    }
    EXPECT_FLOAT_EQ(queue.utilization_percent(), 100.0f);

    queue.stop();
}

TEST_F(RequestQueueTest, RetryAfterMs_ScalesWithUtilization) {
    RequestQueue::Config cfg;
    cfg.max_queue_size = 100;
    RequestQueue queue(cfg, io_->get_executor());
    queue.start();

    // Below 50% - no retry needed
    for (int i = 0; i < 40; ++i) {
        queue.try_enqueue(makeRequest(i));
    }
    EXPECT_EQ(queue.calculate_retry_after_ms(), 0);

    // At 50% - minimal retry
    for (int i = 0; i < 10; ++i) {
        queue.try_enqueue(makeRequest(100 + i));
    }
    EXPECT_GT(queue.calculate_retry_after_ms(), 0);

    // At 90%+ - significant retry
    for (int i = 0; i < 40; ++i) {
        queue.try_enqueue(makeRequest(200 + i));
    }
    EXPECT_GE(queue.calculate_retry_after_ms(), 400);

    queue.stop();
}

TEST_F(RequestQueueTest, StopClearsQueue) {
    RequestQueue::Config cfg;
    cfg.max_queue_size = 100;
    RequestQueue queue(cfg, io_->get_executor());
    queue.start();

    std::atomic<int> callbacks_called{0};
    for (int i = 0; i < 5; ++i) {
        auto req = makeRequest(i);
        req.completion_callback = [&](auto result) {
            EXPECT_FALSE(result.has_value()); // Should be error
            callbacks_called.fetch_add(1);
        };
        queue.try_enqueue(std::move(req));
    }

    EXPECT_EQ(queue.depth(), 5);

    queue.stop();

    EXPECT_EQ(queue.depth(), 0);
    EXPECT_EQ(callbacks_called.load(), 5);
    EXPECT_EQ(queue.metrics().evicted_shutdown.load(), 5);
}

TEST_F(RequestQueueTest, TimeoutEviction) {
    RequestQueue::Config cfg;
    cfg.max_queue_size = 100;
    cfg.request_timeout = 50ms;   // Very short for testing
    cfg.eviction_interval = 20ms; // Check frequently
    RequestQueue queue(cfg, io_->get_executor());

    std::atomic<int> timeout_callbacks{0};
    auto req = makeRequest(1);
    req.completion_callback = [&](auto result) {
        EXPECT_FALSE(result.has_value());
        timeout_callbacks.fetch_add(1);
    };

    queue.start();
    queue.try_enqueue(std::move(req));
    EXPECT_EQ(queue.depth(), 1);

    // Wait for eviction
    std::this_thread::sleep_for(150ms);

    EXPECT_EQ(queue.depth(), 0);
    EXPECT_EQ(timeout_callbacks.load(), 1);
    EXPECT_EQ(queue.metrics().evicted_timeout.load(), 1);

    queue.stop();
}

TEST_F(RequestQueueTest, EmptyDequeue_ReturnsNullopt) {
    RequestQueue::Config cfg;
    cfg.max_queue_size = 100;
    RequestQueue queue(cfg, io_->get_executor());
    queue.start();

    auto result = queue.try_dequeue();
    EXPECT_FALSE(result.has_value());

    queue.stop();
}

TEST_F(RequestQueueTest, Metrics_TracksWaitTime) {
    RequestQueue::Config cfg;
    cfg.max_queue_size = 100;
    RequestQueue queue(cfg, io_->get_executor());
    queue.start();

    queue.try_enqueue(makeRequest(1));
    std::this_thread::sleep_for(50ms);
    queue.try_dequeue();

    // Should have recorded some wait time
    EXPECT_GT(queue.metrics().total_wait_time_ms.load(), 0);
    EXPECT_EQ(queue.metrics().dequeued.load(), 1);

    queue.stop();
}

TEST_F(RequestQueueTest, DisabledPriorityQueuing_TreatsAllAsNormal) {
    RequestQueue::Config cfg;
    cfg.max_queue_size = 100;
    cfg.enable_priority_queuing = false;
    RequestQueue queue(cfg, io_->get_executor());
    queue.start();

    // Enqueue with different priorities
    queue.try_enqueue(makeRequest(1, RequestPriority::High));
    queue.try_enqueue(makeRequest(2, RequestPriority::Background));
    queue.try_enqueue(makeRequest(3, RequestPriority::Low));

    // Should dequeue in FIFO order regardless of priority
    EXPECT_EQ(queue.try_dequeue()->request_id, 1);
    EXPECT_EQ(queue.try_dequeue()->request_id, 2);
    EXPECT_EQ(queue.try_dequeue()->request_id, 3);

    queue.stop();
}

TEST_F(RequestQueueTest, ConcurrentEnqueue) {
    RequestQueue::Config cfg;
    cfg.max_queue_size = 1000;
    RequestQueue queue(cfg, io_->get_executor());
    queue.start();

    constexpr int kNumThreads = 4;
    constexpr int kRequestsPerThread = 100;
    std::vector<std::thread> threads;
    std::atomic<int> enqueued{0};

    for (int t = 0; t < kNumThreads; ++t) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < kRequestsPerThread; ++i) {
                if (queue.try_enqueue(makeRequest(t * 1000 + i))) {
                    enqueued.fetch_add(1);
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(enqueued.load(), kNumThreads * kRequestsPerThread);
    EXPECT_EQ(queue.depth(), kNumThreads * kRequestsPerThread);
    EXPECT_EQ(queue.metrics().enqueued.load(), kNumThreads * kRequestsPerThread);

    queue.stop();
}

} // namespace yams::daemon::test
