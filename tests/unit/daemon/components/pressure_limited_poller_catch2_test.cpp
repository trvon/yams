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
#include <thread>

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
PressureLimitedPollerConfig<Task> makeCapabilitySleepConfig(std::atomic<bool>& stopFlag,
                                                            std::atomic<bool>& startedFlag,
                                                            std::atomic<bool>& pauseFlag) {
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
    // Required — dereferenced in the single-item processing path when
    // isCapableFn returns true and the channel is empty (the poller tries
    // to pop before falling through to the wake-timer idle sleep).
    thread_local std::atomic<std::size_t> tlsInFlight{0};
    cfg.inFlightCounter = &tlsInFlight;
    return cfg;
}

} // namespace

// ---------------------------------------------------------------------------
// Capability-sleep shutdown (the Windows title-poller crash path)
// ---------------------------------------------------------------------------

TEST_CASE("PressureLimitedPoller exits on stop while sleeping in capability check",
          "[daemon][poller][shutdown][catch2]") {
    boost::asio::io_context ioc;

    std::atomic<bool> stopFlag{false};
    std::atomic<bool> startedFlag{false};
    std::atomic<bool> pauseFlag{false};

    auto cfg = makeCapabilitySleepConfig<int>(stopFlag, startedFlag, pauseFlag);
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

    auto cfg = makeCapabilitySleepConfig<int>(stopFlag, startedFlag, pauseFlag);
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

    auto cfg = makeCapabilitySleepConfig<int>(stopFlag, startedFlag, pauseFlag);
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
