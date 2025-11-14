// WorkCoordinator test suite (Catch2)
// Tests the centralized async work coordination component

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <yams/daemon/components/WorkCoordinator.h>

using namespace yams::daemon;
using namespace std::chrono_literals;

TEST_CASE("WorkCoordinator lifecycle", "[daemon][work_coordinator][lifecycle]") {
    SECTION("Construct without starting") {
        WorkCoordinator coordinator;
        REQUIRE_FALSE(coordinator.isRunning());
        REQUIRE(coordinator.getWorkerCount() == 0);
        REQUIRE(coordinator.getIOContext() != nullptr);
        REQUIRE(coordinator.getIOContext()->stopped());
    }

    SECTION("Stop and join without start are no-ops") {
        WorkCoordinator coordinator;
        coordinator.stop();
        coordinator.join();

        REQUIRE_FALSE(coordinator.isRunning());
        REQUIRE(coordinator.getWorkerCount() == 0);
    }

    SECTION("Start and stop basic lifecycle") {
        WorkCoordinator coordinator;
        coordinator.start(2);

        REQUIRE(coordinator.isRunning());
        REQUIRE(coordinator.getWorkerCount() == 2);
        REQUIRE_FALSE(coordinator.getIOContext()->stopped());

        coordinator.stop();
        coordinator.join();

        REQUIRE_FALSE(coordinator.isRunning());
        REQUIRE(coordinator.getWorkerCount() == 0);
    }

    SECTION("Default thread count uses hardware_concurrency") {
        WorkCoordinator coordinator;
        coordinator.start(); // No explicit thread count

        auto expected = std::max<std::size_t>(1, std::thread::hardware_concurrency());
        REQUIRE(coordinator.getWorkerCount() == expected);

        coordinator.stop();
        coordinator.join();
    }

    SECTION("Cannot start twice") {
        WorkCoordinator coordinator;
        coordinator.start(2);

        REQUIRE_THROWS_AS(coordinator.start(2), std::runtime_error);

        coordinator.stop();
        coordinator.join();
    }

    SECTION("Stop and join are idempotent") {
        WorkCoordinator coordinator;
        coordinator.start(2);

        coordinator.stop();
        coordinator.stop(); // Should not throw

        coordinator.join();
        coordinator.join(); // Should not throw
    }

    SECTION("Destructor cleans up if not explicitly stopped") {
        {
            WorkCoordinator coordinator;
            coordinator.start(2);
            // Destructor should call stop() and join()
        }
        // Should not hang or crash
        REQUIRE(true);
    }
}

TEST_CASE("WorkCoordinator work execution", "[daemon][work_coordinator][execution]") {
    SECTION("Post simple work") {
        WorkCoordinator coordinator;
        coordinator.start(2);

        std::atomic<int> counter{0};

        for (int i = 0; i < 10; ++i) {
            boost::asio::post(coordinator.getExecutor(), [&counter]() { counter.fetch_add(1); });
        }

        // Wait for work to complete
        std::this_thread::sleep_for(50ms);

        REQUIRE(counter.load() == 10);

        coordinator.stop();
        coordinator.join();
    }

    SECTION("Work executes on worker threads") {
        WorkCoordinator coordinator;
        coordinator.start(2);

        std::atomic<bool> work_executed{false};
        auto main_thread_id = std::this_thread::get_id();
        std::atomic<bool> on_different_thread{false};

        boost::asio::post(coordinator.getExecutor(), [&]() {
            work_executed = true;
            on_different_thread = (std::this_thread::get_id() != main_thread_id);
        });

        // Wait for work
        std::this_thread::sleep_for(50ms);

        REQUIRE(work_executed);
        REQUIRE(on_different_thread);

        coordinator.stop();
        coordinator.join();
    }

    SECTION("Work drains on stop") {
        WorkCoordinator coordinator;
        coordinator.start(2);

        std::atomic<int> completed{0};
        constexpr int total_work = 100;

        for (int i = 0; i < total_work; ++i) {
            boost::asio::post(coordinator.getExecutor(), [&completed]() {
                std::this_thread::sleep_for(1ms);
                completed.fetch_add(1);
            });
        }

        coordinator.stop();
        coordinator.join();

        // All work should complete before join() returns
        REQUIRE(completed.load() == total_work);
    }
}

TEST_CASE("WorkCoordinator async operations", "[daemon][work_coordinator][async]") {
    SECTION("Spawn coroutines") {
        WorkCoordinator coordinator;
        coordinator.start(2);

        std::atomic<int> result{0};

        auto coro = [&]() -> boost::asio::awaitable<void> {
            boost::asio::steady_timer timer(*coordinator.getIOContext());
            timer.expires_after(10ms);
            co_await timer.async_wait(boost::asio::use_awaitable);
            result.store(42);
        };

        boost::asio::co_spawn(coordinator.getExecutor(), coro(), boost::asio::detached);

        std::this_thread::sleep_for(50ms);
        REQUIRE(result.load() == 42);

        coordinator.stop();
        coordinator.join();
    }

    SECTION("Multiple concurrent coroutines") {
        WorkCoordinator coordinator;
        coordinator.start(4);

        std::atomic<int> counter{0};
        constexpr int num_coros = 20;

        auto coro = [&]() -> boost::asio::awaitable<void> {
            counter.fetch_add(1);
            co_return;
        };

        for (int i = 0; i < num_coros; ++i) {
            boost::asio::co_spawn(coordinator.getExecutor(), coro(), boost::asio::detached);
        }

        std::this_thread::sleep_for(100ms);
        REQUIRE(counter.load() == num_coros);

        coordinator.stop();
        coordinator.join();
    }

    SECTION("New work is rejected after stop") {
        WorkCoordinator coordinator;
        coordinator.start(2);
        coordinator.stop();

        std::atomic<int> counter{0};
        boost::asio::post(coordinator.getExecutor(), [&counter]() { counter.fetch_add(1); });

        coordinator.join();
        REQUIRE(counter.load() == 0);
    }
}

TEST_CASE("WorkCoordinator strand isolation", "[daemon][work_coordinator][strand]") {
    SECTION("Strand serializes work") {
        WorkCoordinator coordinator;
        coordinator.start(4);

        auto strand = coordinator.makeStrand();

        std::vector<int> execution_order;
        std::mutex order_mutex;

        auto work = [&](int id) {
            boost::asio::post(strand, [&, id]() {
                std::this_thread::sleep_for(5ms);
                std::lock_guard<std::mutex> lk(order_mutex);
                execution_order.push_back(id);
            });
        };

        for (int i = 0; i < 10; ++i) {
            work(i);
        }

        std::this_thread::sleep_for(200ms);

        // Strand guarantees FIFO order
        REQUIRE(execution_order.size() == 10);
        for (size_t i = 0; i < execution_order.size(); ++i) {
            REQUIRE(execution_order[i] == static_cast<int>(i));
        }

        coordinator.stop();
        coordinator.join();
    }

    SECTION("Multiple strands can execute concurrently") {
        WorkCoordinator coordinator;
        coordinator.start(4);

        auto strand1 = coordinator.makeStrand();
        auto strand2 = coordinator.makeStrand();

        std::atomic<int> strand1_work{0};
        std::atomic<int> strand2_work{0};
        std::atomic<bool> concurrent_execution{false};

        // Post long-running work on strand1
        boost::asio::post(strand1, [&]() {
            for (int i = 0; i < 5; ++i) {
                strand1_work.fetch_add(1);
                std::this_thread::sleep_for(10ms);
                if (strand2_work.load() > 0) {
                    concurrent_execution = true;
                }
            }
        });

        // Post work on strand2 that should run concurrently
        boost::asio::post(strand2, [&]() {
            for (int i = 0; i < 5; ++i) {
                strand2_work.fetch_add(1);
                std::this_thread::sleep_for(10ms);
            }
        });

        std::this_thread::sleep_for(150ms);

        REQUIRE(strand1_work.load() == 5);
        REQUIRE(strand2_work.load() == 5);
        REQUIRE(concurrent_execution); // Strands ran in parallel

        coordinator.stop();
        coordinator.join();
    }
}

TEST_CASE("WorkCoordinator load handling", "[daemon][work_coordinator][load]") {
    SECTION("Handle high concurrent load") {
        WorkCoordinator coordinator;
        coordinator.start(4);

        std::atomic<int> completed{0};
        constexpr int heavy_load = 1000;

        for (int i = 0; i < heavy_load; ++i) {
            boost::asio::post(coordinator.getExecutor(), [&]() {
                // Simulate light work
                volatile int x = 0;
                for (int j = 0; j < 100; ++j) {
                    x += j;
                }
                completed.fetch_add(1);
            });
        }

        coordinator.stop();
        coordinator.join();

        REQUIRE(completed.load() == heavy_load);
    }

    SECTION("Work stealing across threads") {
        WorkCoordinator coordinator;
        coordinator.start(4);

        std::set<std::thread::id> thread_ids;
        std::mutex ids_mutex;

        for (int i = 0; i < 100; ++i) {
            boost::asio::post(coordinator.getExecutor(), [&]() {
                std::lock_guard<std::mutex> lk(ids_mutex);
                thread_ids.insert(std::this_thread::get_id());
            });
        }

        std::this_thread::sleep_for(100ms);

        // Work should be distributed across multiple threads
        REQUIRE(thread_ids.size() >= 2);

        coordinator.stop();
        coordinator.join();
    }
}

TEST_CASE("WorkCoordinator shutdown behavior", "[daemon][work_coordinator][shutdown]") {
    SECTION("Stop interrupts waiting work") {
        WorkCoordinator coordinator;
        coordinator.start(2);

        std::atomic<bool> work_started{false};
        std::atomic<bool> work_completed{false};

        auto long_running = [&]() -> boost::asio::awaitable<void> {
            work_started = true;
            boost::asio::steady_timer timer(*coordinator.getIOContext());
            timer.expires_after(10s); // Long delay
            try {
                co_await timer.async_wait(boost::asio::use_awaitable);
                work_completed = true;
            } catch (...) {
                // Timer cancelled during shutdown
            }
        };

        boost::asio::co_spawn(coordinator.getExecutor(), long_running(), boost::asio::detached);

        // Wait for work to start
        while (!work_started.load()) {
            std::this_thread::sleep_for(1ms);
        }

        coordinator.stop();
        coordinator.join();

        REQUIRE_FALSE(work_completed); // Should not have waited 10s
    }

    SECTION("Join waits for all work to complete") {
        WorkCoordinator coordinator;
        coordinator.start(2);

        std::atomic<int> completed{0};
        auto start = std::chrono::steady_clock::now();

        for (int i = 0; i < 10; ++i) {
            boost::asio::post(coordinator.getExecutor(), [&]() {
                std::this_thread::sleep_for(20ms);
                completed.fetch_add(1);
            });
        }

        coordinator.stop();
        coordinator.join();

        auto duration = std::chrono::steady_clock::now() - start;

        REQUIRE(completed.load() == 10);
        // Should have taken at least ~200ms with 2 threads (10 tasks * 20ms / 2 threads)
        REQUIRE(duration >= 100ms);
    }
}
