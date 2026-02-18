// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Unit tests for RequestQueue component (PBI-090)
//
// Catch2 migration from GTest (yams-3s4 / yams-zns)

#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/ResourceGovernor.h>
#include <yams/daemon/components/RequestQueue.h>

#include <boost/asio/io_context.hpp>

#include <atomic>
#include <chrono>
#include <future>
#include <thread>
#include <vector>

using namespace yams::daemon;
using namespace std::chrono_literals;

namespace {

struct RequestQueueFixture {
    std::shared_ptr<boost::asio::io_context> io;
    std::unique_ptr<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>>
        workGuard;
    std::thread ioThread;

    RequestQueueFixture() {
        io = std::make_shared<boost::asio::io_context>();
        workGuard = std::make_unique<
            boost::asio::executor_work_guard<boost::asio::io_context::executor_type>>(
            io->get_executor());
        ioThread = std::thread([this]() { io->run(); });
    }

    ~RequestQueueFixture() {
        workGuard.reset();
        io->stop();
        if (ioThread.joinable()) {
            ioThread.join();
        }
    }

    RequestQueue::Config makeConfig(size_t maxSize = 100) {
        RequestQueue::Config cfg;
        cfg.max_queue_size = maxSize;
        cfg.request_timeout = 30000ms;
        cfg.enable_priority_queuing = true;
        cfg.high_watermark_percent = 80;
        cfg.low_watermark_percent = 20;
        return cfg;
    }

    QueuedRequest makeRequest(RequestPriority priority = RequestPriority::Normal,
                              const std::string& type = "test") {
        static std::atomic<uint64_t> id{0};
        QueuedRequest req;
        req.request_id = ++id;
        req.connection_id = 1;
        req.enqueued_at = std::chrono::steady_clock::now();
        req.priority = priority;
        req.request_type = type;
        return req;
    }
};

} // namespace

TEST_CASE_METHOD(RequestQueueFixture, "RequestQueue construction and initial state",
                 "[daemon][components][queue][catch2]") {
    auto cfg = makeConfig();
    RequestQueue queue(cfg, io->get_executor());

    SECTION("initial queue is empty") {
        CHECK(queue.depth() == 0);
        CHECK_FALSE(queue.is_backpressured());
    }

    SECTION("initial metrics are zero") {
        CHECK(queue.metrics().enqueued == 0);
        CHECK(queue.metrics().dequeued == 0);
        CHECK(queue.metrics().rejected == 0);
    }

    SECTION("capacity matches config") {
        CHECK(queue.capacity() == 100);
    }
}

TEST_CASE_METHOD(RequestQueueFixture, "RequestQueue enqueue operations",
                 "[daemon][components][queue][catch2]") {
    auto cfg = makeConfig(10);
    RequestQueue queue(cfg, io->get_executor());
    queue.start();

    SECTION("enqueue single request succeeds") {
        auto result = queue.try_enqueue(makeRequest());
        CHECK(result);
        CHECK(queue.depth() == 1);
        CHECK(queue.metrics().enqueued == 1);
    }

    SECTION("enqueue multiple requests succeeds") {
        for (int i = 0; i < 5; ++i) {
            auto result = queue.try_enqueue(makeRequest());
            CHECK(result);
        }
        CHECK(queue.depth() == 5);
        CHECK(queue.metrics().enqueued == 5);
    }

    SECTION("enqueue past max size fails") {
        for (size_t i = 0; i < 10; ++i) {
            REQUIRE(queue.try_enqueue(makeRequest()));
        }

        auto result = queue.try_enqueue(makeRequest());
        CHECK_FALSE(result);
        CHECK(queue.depth() == 10);
        CHECK(queue.metrics().rejected >= 1);
    }

    queue.stop();
}

TEST_CASE_METHOD(RequestQueueFixture, "RequestQueue dequeue operations",
                 "[daemon][components][queue][catch2]") {
    auto cfg = makeConfig();
    RequestQueue queue(cfg, io->get_executor());
    queue.start();

    SECTION("dequeue from empty queue returns nullopt") {
        auto result = queue.try_dequeue();
        CHECK_FALSE(result.has_value());
    }

    SECTION("dequeue returns enqueued request") {
        queue.try_enqueue(makeRequest(RequestPriority::Normal, "test_request"));
        auto result = queue.try_dequeue();
        REQUIRE(result.has_value());
        CHECK(result->request_type == "test_request");
        CHECK(queue.depth() == 0);
        CHECK(queue.metrics().dequeued == 1);
    }

    SECTION("FIFO order for same priority") {
        queue.try_enqueue(makeRequest(RequestPriority::Normal, "first"));
        queue.try_enqueue(makeRequest(RequestPriority::Normal, "second"));
        queue.try_enqueue(makeRequest(RequestPriority::Normal, "third"));

        CHECK(queue.try_dequeue()->request_type == "first");
        CHECK(queue.try_dequeue()->request_type == "second");
        CHECK(queue.try_dequeue()->request_type == "third");
    }

    queue.stop();
}

TEST_CASE_METHOD(RequestQueueFixture, "RequestQueue priority ordering",
                 "[daemon][components][queue][catch2]") {
    auto cfg = makeConfig();
    cfg.enable_priority_queuing = true;
    RequestQueue queue(cfg, io->get_executor());
    queue.start();

    SECTION("higher priority dequeued first") {
        queue.try_enqueue(makeRequest(RequestPriority::Low, "low"));
        queue.try_enqueue(makeRequest(RequestPriority::High, "high"));
        queue.try_enqueue(makeRequest(RequestPriority::Normal, "normal"));

        CHECK(queue.try_dequeue()->request_type == "high");
        CHECK(queue.try_dequeue()->request_type == "normal");
        CHECK(queue.try_dequeue()->request_type == "low");
    }

    SECTION("background priority is lowest") {
        queue.try_enqueue(makeRequest(RequestPriority::Background, "background"));
        queue.try_enqueue(makeRequest(RequestPriority::Low, "low"));

        CHECK(queue.try_dequeue()->request_type == "low");
        CHECK(queue.try_dequeue()->request_type == "background");
    }

    queue.stop();
}

TEST_CASE_METHOD(RequestQueueFixture, "RequestQueue backpressure",
                 "[daemon][components][queue][catch2]") {
    auto cfg = makeConfig(10);
    cfg.high_watermark_percent = 50; // 50% = 5 items
    cfg.low_watermark_percent = 20;  // 20% = 2 items
    RequestQueue queue(cfg, io->get_executor());
    queue.start();

    SECTION("backpressure activates at high watermark") {
        for (int i = 0; i < 4; ++i) {
            queue.try_enqueue(makeRequest());
        }
        CHECK_FALSE(queue.is_backpressured());

        queue.try_enqueue(makeRequest());
        queue.try_enqueue(makeRequest());
        CHECK(queue.is_backpressured());
    }

    SECTION("backpressure callback is invoked") {
        std::atomic<bool> callbackCalled{false};
        queue.set_backpressure_callback([&](bool active, uint32_t) {
            if (active)
                callbackCalled = true;
        });

        // Fill past high watermark
        for (int i = 0; i < 8; ++i) {
            queue.try_enqueue(makeRequest());
        }

        std::this_thread::sleep_for(50ms);
        CHECK(callbackCalled);
    }

    queue.stop();
}

TEST_CASE_METHOD(RequestQueueFixture, "RequestQueue utilization metrics",
                 "[daemon][components][queue][catch2]") {
    auto cfg = makeConfig(100);
    RequestQueue queue(cfg, io->get_executor());
    queue.start();

    SECTION("utilization is zero when empty") {
        CHECK(queue.utilization_percent() == 0.0f);
    }

    SECTION("utilization increases with depth") {
        for (int i = 0; i < 50; ++i) {
            queue.try_enqueue(makeRequest());
        }
        CHECK(queue.utilization_percent() >= 49.0f);
        CHECK(queue.utilization_percent() <= 51.0f);
    }

    queue.stop();
}

TEST_CASE_METHOD(RequestQueueFixture, "RequestQueue retry after calculation",
                 "[daemon][components][queue][catch2]") {
    auto cfg = makeConfig(10);
    RequestQueue queue(cfg, io->get_executor());
    queue.start();

    SECTION("retry after increases with queue depth") {
        const auto queueCapacity = static_cast<std::uint64_t>(cfg.max_queue_size);
        const auto highWatermarkDepth =
            (queueCapacity * static_cast<std::uint64_t>(cfg.high_watermark_percent)) / 100ull;
        const auto workerQueueThreshold =
            (highWatermarkDepth > 0) ? (highWatermarkDepth - 1ull) : 0ull;
        const auto controlIntervalMs = std::max<std::uint32_t>(
            100u, static_cast<std::uint32_t>(cfg.eviction_interval.count()));

        auto emptyRetry = queue.calculate_retry_after_ms();
        auto expectedEmptyRetry = ResourceGovernor::instance().recommendRetryAfterMs(
            0, workerQueueThreshold, 0, 0, 0, 0, 0, 0, controlIntervalMs);
        CHECK(emptyRetry == expectedEmptyRetry);

        for (int i = 0; i < 8; ++i) {
            queue.try_enqueue(makeRequest());
        }

        auto fullRetry = queue.calculate_retry_after_ms();
        auto expectedFullRetry = ResourceGovernor::instance().recommendRetryAfterMs(
            static_cast<std::uint64_t>(queue.depth()), workerQueueThreshold, 0, 0, 0, 0, 0, 0,
            controlIntervalMs);
        CHECK(fullRetry == expectedFullRetry);
        CHECK(fullRetry >= emptyRetry);
    }

    queue.stop();
}

TEST_CASE_METHOD(RequestQueueFixture, "RequestQueue concurrent enqueue",
                 "[daemon][components][queue][catch2]") {
    auto cfg = makeConfig(1000);
    RequestQueue queue(cfg, io->get_executor());
    queue.start();

    SECTION("concurrent enqueues are thread-safe") {
        constexpr int numThreads = 4;
        constexpr int itemsPerThread = 100;
        std::vector<std::thread> threads;

        for (int t = 0; t < numThreads; ++t) {
            threads.emplace_back([&queue, t, this]() {
                for (int i = 0; i < itemsPerThread; ++i) {
                    auto req = makeRequest(RequestPriority::Normal,
                                           "t" + std::to_string(t) + "_" + std::to_string(i));
                    queue.try_enqueue(std::move(req));
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        CHECK(queue.depth() == numThreads * itemsPerThread);
        CHECK(queue.metrics().enqueued == numThreads * itemsPerThread);
    }

    queue.stop();
}

TEST_CASE_METHOD(RequestQueueFixture, "RequestQueue start/stop lifecycle",
                 "[daemon][components][queue][catch2]") {
    auto cfg = makeConfig();
    RequestQueue queue(cfg, io->get_executor());

    SECTION("stop without start is safe") {
        queue.stop();
        // No crash = success
    }

    SECTION("double start is safe") {
        queue.start();
        queue.start();
        queue.stop();
    }

    SECTION("double stop is safe") {
        queue.start();
        queue.stop();
        queue.stop();
    }
}

TEST_CASE("RequestQueue config defaults", "[daemon][components][queue][catch2]") {
    RequestQueue::Config cfg;

    CHECK(cfg.max_queue_size > 0);
    CHECK(cfg.request_timeout.count() > 0);
    CHECK(cfg.high_watermark_percent > 0);
    CHECK(cfg.high_watermark_percent <= 100);
    CHECK(cfg.low_watermark_percent < cfg.high_watermark_percent);
}
