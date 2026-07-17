// PressureLimitedPoller unit tests (Catch2)
// Exercises the generic poller coroutine directly, focusing on shutdown
// invariants that are otherwise only tested indirectly through the full
// PostIngestQueue / ServiceManager integration stack.
//
// Highest-value coverage: capability-sleep shutdown path, which was traced
// to the Windows CI SIGSEGV cluster where the title poller (isCapableFn
// returning false) did not exit cleanly before PostIngestQueue teardown.

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>

#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/pressure_limited_poller.h>

using namespace yams::daemon;
using namespace std::chrono_literals;

namespace {

/// Minimal poller config for testing shutdown paths.
/// The poller is spawned with an empty queue and isCapableFn returning false
/// so it immediately enters the capability-sleep branch (250ms timer loop).
/// Fields that the poller never reaches in this path are left as no-op stubs.
template <typename Task>
PressureLimitedPollerConfig<Task> makeCapabilitySleepConfig(
    std::atomic<bool>& stopFlag, std::atomic<bool>& startedFlag, std::atomic<bool>& pauseFlag,
    std::shared_ptr<boost::asio::steady_timer> wakeTimer, std::mutex& wakeMutex) {
    PressureLimitedPollerConfig<Task> cfg;
    cfg.stageName = "test-capability-sleep";
    cfg.stopFlag = &stopFlag;
    cfg.startedFlag = &startedFlag;
    cfg.pauseFlag = &pauseFlag;
    cfg.isCapableFn = []() { return false; };
    // Stubs — never called in the capability-sleep path but must be present.
    cfg.maxConcurrentFn = []() -> std::size_t { return 1; };
    cfg.tryAcquireFn = [](GradientLimiter*, const std::string&, const std::string&) {
        return false;
    };
    cfg.getHashFn = [](const Task&) -> std::string { return ""; };
    cfg.completeJobFn = [](const std::string&, bool) {};
    cfg.processFn = [](Task&) {};
    // Required even for capability-sleep path — the template checks at entry.
    thread_local std::atomic<bool> tlsWasActive{false};
    thread_local std::atomic<std::size_t> tlsInFlight{0};
    cfg.wasActiveFlag = &tlsWasActive;
    cfg.inFlightCounter = &tlsInFlight;
    cfg.wakeTimer = std::move(wakeTimer);
    cfg.wakeTimerMutex = &wakeMutex;
    return cfg;
}

/// Normal capable poller that reaches the event-driven idle path when the
/// channel is empty.  Requires a wakeTimer so the test can exercise
/// timer-cancellation-based shutdown (the path taken by extraction, kg,
/// symbol, and entity pollers in production).
template <typename Task>
PressureLimitedPollerConfig<Task>
makeWakeTimerConfig(std::atomic<bool>& stopFlag, std::atomic<bool>& startedFlag,
                    std::atomic<bool>& pauseFlag,
                    std::shared_ptr<boost::asio::steady_timer> wakeTimer, std::mutex& wakeMutex) {
    PressureLimitedPollerConfig<Task> cfg;
    cfg.stageName = "test-wake-timer";
    cfg.stopFlag = &stopFlag;
    cfg.startedFlag = &startedFlag;
    cfg.pauseFlag = &pauseFlag;
    cfg.isCapableFn = []() { return true; };
    cfg.maxConcurrentFn = []() -> std::size_t { return 1; };
    cfg.tryAcquireFn = [](GradientLimiter*, const std::string&, const std::string&) {
        return false;
    };
    cfg.getHashFn = [](const Task&) -> std::string { return ""; };
    cfg.completeJobFn = [](const std::string&, bool) {};
    cfg.processFn = [](Task&) {};
    cfg.wakeTimer = std::move(wakeTimer);
    cfg.wakeTimerMutex = &wakeMutex;
    // Required even for wake-timer path — the template checks at entry.
    thread_local std::atomic<bool> tlsWasActive{false};
    thread_local std::atomic<std::size_t> tlsInFlight{0};
    cfg.wasActiveFlag = &tlsWasActive;
    cfg.inFlightCounter = &tlsInFlight;
    return cfg;
}

class BatchPollerHarness {
public:
    explicit BatchPollerHarness(std::chrono::milliseconds coalesceWindow)
        : coalesceWindow_(coalesceWindow),
          wakeTimer_(std::make_shared<boost::asio::steady_timer>(ioContext_)),
          channel_(std::make_shared<SpscQueue<int>>(16)) {}

    bool pushNormal(int value) { return channel_->try_push(value); }

    bool pushHighPriority(int value) {
        if (!highPriorityChannel_) {
            highPriorityChannel_ = std::make_shared<SpscQueue<int>>(16);
        }
        return highPriorityChannel_->try_push(value);
    }

    void scheduleNormal(int value, std::chrono::milliseconds delay) {
        auto timer = std::make_shared<boost::asio::steady_timer>(ioContext_, delay);
        timer->async_wait([this, value](boost::system::error_code ec) {
            if (!ec) {
                (void)channel_->try_push(value);
                cancelWakeTimer();
            }
        });
        arrivals_.push_back(std::move(timer));
    }

    void run(std::chrono::milliseconds timeout = 100ms) {
        startedAt_ = std::chrono::steady_clock::now();
        auto deadline = std::make_shared<boost::asio::steady_timer>(ioContext_, timeout);
        deadline->async_wait([this](boost::system::error_code ec) {
            if (!ec) {
                timedOut = true;
                stop();
            }
        });

        auto cfg = makeConfig();
        cfg.batchProcessFn = [this, deadline](std::vector<int>&& batch) {
            observed = std::move(batch);
            elapsed = std::chrono::steady_clock::now() - startedAt_;
            stop();
            deadline->cancel();
        };
        boost::asio::co_spawn(ioContext_, pressureLimitedPoll<int>(channel_, std::move(cfg)),
                              boost::asio::detached);
        ioContext_.run();
    }

    std::vector<int> observed;
    std::chrono::steady_clock::duration elapsed{};
    bool timedOut{false};

private:
    PressureLimitedPollerConfig<int> makeConfig() {
        PressureLimitedPollerConfig<int> cfg;
        cfg.stageName = "test-batch";
        cfg.stopFlag = &stopFlag_;
        cfg.startedFlag = &startedFlag_;
        cfg.pauseFlag = &pauseFlag_;
        cfg.wasActiveFlag = &wasActive_;
        cfg.inFlightCounter = &inFlight_;
        cfg.maxConcurrentFn = []() -> std::size_t { return 4; };
        cfg.tryAcquireFn = [](GradientLimiter*, const std::string&, const std::string&) {
            return true;
        };
        cfg.completeJobFn = [](const std::string&, bool) {};
        cfg.getHashFn = [](const int& task) { return std::to_string(task); };
        cfg.executor = ioContext_.get_executor();
        cfg.batchMode = true;
        cfg.batchSizeFn = []() -> std::size_t { return 4; };
        cfg.batchCoalesceWindowFn = [this]() { return coalesceWindow_; };
        cfg.highPriorityChannel = highPriorityChannel_;
        cfg.highPriorityMaxPerBatchFn = []() -> std::size_t { return 4; };
        cfg.enableCpuThrottling = false;
        cfg.wakeTimer = wakeTimer_;
        cfg.wakeTimerMutex = &wakeMutex_;
        return cfg;
    }

    void cancelWakeTimer() {
        std::lock_guard<std::mutex> lock(wakeMutex_);
        boost::system::error_code ec;
        wakeTimer_->cancel(ec);
    }

    void stop() {
        stopFlag_.store(true, std::memory_order_release);
        cancelWakeTimer();
        for (const auto& timer : arrivals_) {
            timer->cancel();
        }
    }

    std::chrono::milliseconds coalesceWindow_;
    boost::asio::io_context ioContext_;
    std::atomic<bool> stopFlag_{false};
    std::atomic<bool> startedFlag_{false};
    std::atomic<bool> pauseFlag_{false};
    std::atomic<bool> wasActive_{false};
    std::atomic<std::size_t> inFlight_{0};
    std::mutex wakeMutex_;
    std::shared_ptr<boost::asio::steady_timer> wakeTimer_;
    std::shared_ptr<SpscQueue<int>> channel_;
    std::shared_ptr<SpscQueue<int>> highPriorityChannel_;
    std::vector<std::shared_ptr<boost::asio::steady_timer>> arrivals_;
    std::chrono::steady_clock::time_point startedAt_{};
};

} // namespace

TEST_CASE("PressureLimitedPoller coalesces normal-priority trickle into a bounded batch",
          "[daemon][poller][batch][coalesce][catch2]") {
    BatchPollerHarness harness{10ms};
    REQUIRE(harness.pushNormal(1));
    for (int value = 2; value <= 4; ++value) {
        harness.scheduleNormal(value, value * 1ms);
    }
    harness.run();

    CHECK_FALSE(harness.timedOut);
    CHECK(harness.observed == std::vector<int>{1, 2, 3, 4});
}

TEST_CASE("PressureLimitedPoller does not coalesce high-priority batch work",
          "[daemon][poller][batch][coalesce][priority][catch2]") {
    BatchPollerHarness harness{20ms};
    REQUIRE(harness.pushHighPriority(42));
    harness.scheduleNormal(7, 2ms);
    harness.run();

    CHECK(harness.observed == std::vector<int>{42});
}

TEST_CASE("PressureLimitedPoller bounds lone normal-task coalescing latency",
          "[daemon][poller][batch][coalesce][latency][catch2]") {
    BatchPollerHarness harness{3ms};
    REQUIRE(harness.pushNormal(9));
    harness.run(50ms);

    CHECK_FALSE(harness.timedOut);
    CHECK(harness.observed == std::vector<int>{9});
    CHECK(harness.elapsed < 50ms);
}

// ---------------------------------------------------------------------------
// Capability-sleep shutdown (the Windows title-poller crash path)
// ---------------------------------------------------------------------------

TEST_CASE("PressureLimitedPoller exits on stop while sleeping in capability check",
          "[daemon][poller][shutdown][catch2]") {
    boost::asio::io_context ioc;

    std::atomic<bool> stopFlag{false};
    std::atomic<bool> startedFlag{false};
    std::atomic<bool> pauseFlag{false};
    std::mutex wakeMutex;
    auto wakeTimer = std::make_shared<boost::asio::steady_timer>(ioc);

    auto cfg =
        makeCapabilitySleepConfig<int>(stopFlag, startedFlag, pauseFlag, wakeTimer, wakeMutex);
    auto channel = std::make_shared<SpscQueue<int>>(16);

    boost::asio::co_spawn(ioc, pressureLimitedPoll<int>(channel, std::move(cfg)),
                          boost::asio::detached);

    // Step 1: advance the coroutine until it signals started.
    {
        auto deadline = std::chrono::steady_clock::now() + 500ms;
        while (!startedFlag.load(std::memory_order_acquire) &&
               std::chrono::steady_clock::now() < deadline) {
            ioc.poll_one();
        }
    }
    REQUIRE(startedFlag.load(std::memory_order_acquire));

    // Step 2: request stop and run the io_context until the poller exits.
    stopFlag.store(true, std::memory_order_release);

    // Deadline timer stops the io_context after 800ms to prevent hangs.
    boost::asio::steady_timer deadline(ioc, 800ms);
    deadline.async_wait([&ioc](boost::system::error_code) { ioc.stop(); });

    ioc.run();

    CHECK_FALSE(startedFlag.load(std::memory_order_acquire));
}

// ---------------------------------------------------------------------------
// Idempotent stop: calling stop before the poller starts should be safe
// ---------------------------------------------------------------------------

TEST_CASE("PressureLimitedPoller stop before start is safe", "[daemon][poller][shutdown][catch2]") {
    boost::asio::io_context ioc;

    std::atomic<bool> stopFlag{true}; // already stopped
    std::atomic<bool> startedFlag{false};
    std::atomic<bool> pauseFlag{false};
    std::mutex wakeMutex;
    auto wakeTimer = std::make_shared<boost::asio::steady_timer>(ioc);

    auto cfg =
        makeCapabilitySleepConfig<int>(stopFlag, startedFlag, pauseFlag, wakeTimer, wakeMutex);
    auto channel = std::make_shared<SpscQueue<int>>(16);

    boost::asio::co_spawn(ioc, pressureLimitedPoll<int>(channel, std::move(cfg)),
                          boost::asio::detached);

    // The poller should see stopFlag==true immediately and never set startedFlag.
    boost::asio::steady_timer deadline(ioc, 200ms);
    deadline.async_wait([&ioc](boost::system::error_code) { ioc.stop(); });

    ioc.run();

    // startedFlag should never have been set (or was immediately cleared).
    CHECK_FALSE(startedFlag.load(std::memory_order_acquire));
}

// ---------------------------------------------------------------------------
// Wake-timer cancellation shutdown (extraction/kg/symbol/entity poller path)
// ---------------------------------------------------------------------------

TEST_CASE("PressureLimitedPoller exits on stop when idling on wake-timer",
          "[daemon][poller][shutdown][wake-timer][catch2]") {
    boost::asio::io_context ioc;

    std::atomic<bool> stopFlag{false};
    std::atomic<bool> startedFlag{false};
    std::atomic<bool> pauseFlag{false};
    std::mutex wakeMutex;
    auto wakeTimer = std::make_shared<boost::asio::steady_timer>(ioc);

    auto cfg = makeWakeTimerConfig<int>(stopFlag, startedFlag, pauseFlag, wakeTimer, wakeMutex);
    auto channel = std::make_shared<SpscQueue<int>>(16);

    boost::asio::co_spawn(ioc, pressureLimitedPoll<int>(channel, std::move(cfg)),
                          boost::asio::detached);

    // Advance until the poller signals started.
    {
        auto deadline = std::chrono::steady_clock::now() + 500ms;
        while (!startedFlag.load(std::memory_order_acquire) &&
               std::chrono::steady_clock::now() < deadline) {
            ioc.poll_one();
        }
    }
    REQUIRE(startedFlag.load(std::memory_order_acquire));

    // The poller is now in the event-driven idle loop, waiting on wakeTimer
    // (10ms expiry).  Cancel the timer to simulate signalAllWakeTimers(),
    // then request stop.
    {
        std::lock_guard<std::mutex> lock(wakeMutex);
        wakeTimer->cancel();
    }
    stopFlag.store(true, std::memory_order_release);

    // Run until the poller exits.
    boost::asio::steady_timer deadline(ioc, 500ms);
    deadline.async_wait([&ioc](boost::system::error_code) { ioc.stop(); });
    ioc.run();

    CHECK_FALSE(startedFlag.load(std::memory_order_acquire));
}

TEST_CASE("PressureLimitedPoller exits when wake-timer expires naturally during stop",
          "[daemon][poller][shutdown][wake-timer][catch2]") {
    boost::asio::io_context ioc;

    std::atomic<bool> stopFlag{false};
    std::atomic<bool> startedFlag{false};
    std::atomic<bool> pauseFlag{false};
    std::mutex wakeMutex;
    auto wakeTimer = std::make_shared<boost::asio::steady_timer>(ioc);

    auto cfg = makeWakeTimerConfig<int>(stopFlag, startedFlag, pauseFlag, wakeTimer, wakeMutex);
    auto channel = std::make_shared<SpscQueue<int>>(16);

    boost::asio::co_spawn(ioc, pressureLimitedPoll<int>(channel, std::move(cfg)),
                          boost::asio::detached);

    // Let the poller start.
    {
        auto deadline = std::chrono::steady_clock::now() + 500ms;
        while (!startedFlag.load(std::memory_order_acquire) &&
               std::chrono::steady_clock::now() < deadline) {
            ioc.poll_one();
        }
    }
    REQUIRE(startedFlag.load(std::memory_order_acquire));

    // Set stopFlag WITHOUT explicitly cancelling the timer — the 10ms
    // wake-timer should expire naturally, the poller sees stopFlag, and exits.
    stopFlag.store(true, std::memory_order_release);

    boost::asio::steady_timer deadline(ioc, 500ms);
    deadline.async_wait([&ioc](boost::system::error_code) { ioc.stop(); });
    ioc.run();

    CHECK_FALSE(startedFlag.load(std::memory_order_acquire));
}

// ---------------------------------------------------------------------------
// Repeated stop/unstop cycles
// ---------------------------------------------------------------------------

TEST_CASE("PressureLimitedPoller survives start-stop-restart cycle",
          "[daemon][poller][shutdown][catch2]") {
    boost::asio::io_context ioc;

    std::atomic<bool> stopFlag{false};
    std::atomic<bool> startedFlag{false};
    std::atomic<bool> pauseFlag{false};
    std::mutex wakeMutex;
    auto wakeTimer = std::make_shared<boost::asio::steady_timer>(ioc);

    auto cfg =
        makeCapabilitySleepConfig<int>(stopFlag, startedFlag, pauseFlag, wakeTimer, wakeMutex);
    auto channel = std::make_shared<SpscQueue<int>>(16);

    // Cycle 1
    boost::asio::co_spawn(ioc, pressureLimitedPoll<int>(channel, cfg), boost::asio::detached);

    {
        auto deadline = std::chrono::steady_clock::now() + 500ms;
        while (!startedFlag.load(std::memory_order_acquire) &&
               std::chrono::steady_clock::now() < deadline) {
            ioc.poll_one();
        }
    }
    REQUIRE(startedFlag.load(std::memory_order_acquire));

    stopFlag.store(true, std::memory_order_release);
    {
        boost::asio::steady_timer deadline(ioc, 800ms);
        deadline.async_wait([&ioc](boost::system::error_code) { ioc.stop(); });
        ioc.run();
    }
    CHECK_FALSE(startedFlag.load(std::memory_order_acquire));

    // Cycle 2 — restart after stop
    ioc.restart();
    stopFlag.store(false, std::memory_order_release);
    startedFlag.store(false, std::memory_order_release);

    boost::asio::co_spawn(ioc, pressureLimitedPoll<int>(channel, cfg), boost::asio::detached);

    {
        auto deadline = std::chrono::steady_clock::now() + 500ms;
        while (!startedFlag.load(std::memory_order_acquire) &&
               std::chrono::steady_clock::now() < deadline) {
            ioc.poll_one();
        }
    }
    REQUIRE(startedFlag.load(std::memory_order_acquire));

    stopFlag.store(true, std::memory_order_release);
    {
        boost::asio::steady_timer deadline(ioc, 800ms);
        deadline.async_wait([&ioc](boost::system::error_code) { ioc.stop(); });
        ioc.run();
    }
    CHECK_FALSE(startedFlag.load(std::memory_order_acquire));
}
