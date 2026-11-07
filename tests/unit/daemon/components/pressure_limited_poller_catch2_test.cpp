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
#include <cstdint>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>

#include "../../../common/test_helpers_catch2.h"
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/TuningSnapshot.h>
#include <yams/daemon/components/WorkCoordinator.h>
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
    PressureLimitedPollerConfig<Task> cfg{};
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
    PressureLimitedPollerConfig<Task> cfg{};
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
    explicit BatchPollerHarness(std::chrono::milliseconds coalesceWindow,
                                bool limiterEnabled = true)
        : coalesceWindow_(coalesceWindow), limiterEnabled_(limiterEnabled),
          wakeTimer_(std::make_shared<boost::asio::steady_timer>(ioContext_)),
          channel_(std::make_shared<SpscQueue<int>>(16)) {}

    bool pushNormal(int value) { return channel_->try_push(value); }

    void postNormalAfterAdmission(int admittedValue, std::vector<int> values) {
        postAfterAdmissionValue_ = admittedValue;
        postAfterAdmissionValues_ = std::move(values);
    }

    bool deferredPushSucceeded() const { return deferredPushSucceeded_; }

    const std::vector<std::string>& acquiredLimiterIds() const { return acquiredLimiterIds_; }
    const std::vector<std::string>& completedLimiterIds() const { return completedLimiterIds_; }
    std::size_t hashCalls() const { return hashCalls_; }

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
        cfg.getLimiterFn = [this]() -> GradientLimiter* {
            return limiterEnabled_ ? &limiter_ : nullptr;
        };
        cfg.maxConcurrentFn = []() -> std::size_t { return 4; };
        cfg.tryAcquireFn = [this](GradientLimiter*, const std::string& limiterId,
                                  const std::string&) {
            acquiredLimiterIds_.push_back(limiterId);
            if (postAfterAdmissionValue_) {
                const auto expected = std::to_string(*postAfterAdmissionValue_);
                if (limiterId == expected || limiterId.ends_with("#" + expected)) {
                    postAfterAdmissionValue_.reset();
                    auto values = std::move(postAfterAdmissionValues_);
                    boost::asio::post(ioContext_, [this, values = std::move(values)]() {
                        for (const int value : values) {
                            deferredPushSucceeded_ =
                                channel_->try_push(value) && deferredPushSucceeded_;
                        }
                        cancelWakeTimer();
                    });
                }
            }
            return true;
        };
        cfg.completeJobFn = [this](const std::string& limiterId, bool) {
            completedLimiterIds_.push_back(limiterId);
        };
        cfg.getHashFn = [this](const int& task) {
            ++hashCalls_;
            return std::to_string(task);
        };
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
        wakeTimer_->cancel();
    }

    void stop() {
        stopFlag_.store(true, std::memory_order_release);
        cancelWakeTimer();
        for (const auto& timer : arrivals_) {
            timer->cancel();
        }
    }

    std::chrono::milliseconds coalesceWindow_;
    bool limiterEnabled_{true};
    GradientLimiter limiter_{"test-batch"};
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
    std::optional<int> postAfterAdmissionValue_;
    std::vector<int> postAfterAdmissionValues_;
    std::vector<std::string> acquiredLimiterIds_;
    std::vector<std::string> completedLimiterIds_;
    std::size_t hashCalls_{0};
    bool deferredPushSucceeded_{true};
    std::chrono::steady_clock::time_point startedAt_{};
};

enum class BlockedPollerReason { Paused, ZeroConcurrency, AdmissionPaused };

void checkRegisteredWakeResumesBlockedPoller(BlockedPollerReason reason,
                                             bool stopInsteadOfResume = false,
                                             bool enqueueAfterRegistration = false) {
    boost::asio::io_context ioc;
    std::atomic<bool> stopFlag{false};
    std::atomic<bool> startedFlag{false};
    std::atomic<bool> pauseFlag{reason == BlockedPollerReason::Paused};
    std::atomic<bool> blocked{true};
    std::atomic<bool> wasActive{false};
    std::atomic<std::size_t> inFlight{0};
    std::atomic<std::size_t> callbacksInFlight{0};
    std::mutex wakeMutex;
    auto wakeTimer = std::make_shared<boost::asio::steady_timer>(ioc);
    wakeTimer->expires_at(boost::asio::steady_timer::time_point::max());
    auto watchdog = std::make_shared<boost::asio::steady_timer>(ioc);
    auto channel = std::make_shared<SpscQueue<int>>(16);
    if (!enqueueAfterRegistration) {
        REQUIRE(channel->try_push(42));
    }

    std::vector<int> observed;
    std::size_t drainChecks = 0;
    std::size_t lifecycleNotifications = 0;
    std::size_t cancelledWaits = 0;
    bool timedOut = false;

    PressureLimitedPollerConfig<int> cfg;
    cfg.stageName = "test-registered-blocked-wake";
    cfg.stopFlag = &stopFlag;
    cfg.startedFlag = &startedFlag;
    cfg.pauseFlag = &pauseFlag;
    cfg.wasActiveFlag = &wasActive;
    cfg.inFlightCounter = &inFlight;
    cfg.callbackInFlightCounter = &callbacksInFlight;
    cfg.maxConcurrentFn = [&blocked, reason]() -> std::size_t {
        return reason == BlockedPollerReason::ZeroConcurrency &&
                       blocked.load(std::memory_order_acquire)
                   ? 0
                   : 1;
    };
    cfg.admissionPausedFn = [&blocked, reason]() {
        return reason == BlockedPollerReason::AdmissionPaused &&
               blocked.load(std::memory_order_acquire);
    };
    cfg.tryAcquireFn = [](GradientLimiter*, const std::string&, const std::string&) {
        return true;
    };
    cfg.completeJobFn = [](const std::string&, bool) {};
    cfg.checkDrainFn = [&drainChecks]() { ++drainChecks; };
    cfg.notifyLifecycleFn = [&lifecycleNotifications]() { ++lifecycleNotifications; };
    cfg.getHashFn = [](const int& task) { return std::to_string(task); };
    cfg.processFn = [&observed, &stopFlag, &wakeTimer, &wakeMutex, &watchdog](int& task) {
        observed.push_back(task);
        stopFlag.store(true, std::memory_order_release);
        watchdog->cancel();
        std::lock_guard<std::mutex> lock(wakeMutex);
        wakeTimer->cancel();
    };
    cfg.executor = ioc.get_executor();
    cfg.enableCpuThrottling = false;
    cfg.wakeTimer = wakeTimer;
    cfg.wakeTimerMutex = &wakeMutex;

    boost::asio::co_spawn(ioc, pressureLimitedPoll<int>(channel, std::move(cfg)),
                          boost::asio::detached);

    // Drain ready startup handlers without assuming co_spawn uses exactly one handler.
    // The old coroutine-local timer leaves our explicitly initialized sentinel unchanged.
    REQUIRE(ioc.poll() > 0);
    REQUIRE(startedFlag.load(std::memory_order_acquire));
    CHECK(observed.empty());
    CHECK(channel->size_approx() == (enqueueAfterRegistration ? 0U : 1U));
    CHECK(wakeTimer->expiry() != boost::asio::steady_timer::time_point::max());
    if (enqueueAfterRegistration) {
        REQUIRE(channel->try_push(42));
    }

    if (stopInsteadOfResume) {
        stopFlag.store(true, std::memory_order_release);
    } else {
        blocked.store(false, std::memory_order_release);
        pauseFlag.store(false, std::memory_order_release);
    }
    auto wakeExecutor = wakeTimer->get_executor();
    boost::asio::post(wakeExecutor, [wakeTimer, watchdog, &cancelledWaits, stopInsteadOfResume]() {
        cancelledWaits = wakeTimer->cancel();
        if (stopInsteadOfResume) {
            watchdog->cancel();
        }
    });

    watchdog->expires_after(500ms);
    watchdog->async_wait([&](const boost::system::error_code& error) {
        if (!error) {
            timedOut = true;
            stopFlag.store(true, std::memory_order_release);
            std::lock_guard<std::mutex> lock(wakeMutex);
            wakeTimer->cancel();
        }
    });
    ioc.run();

    CHECK_FALSE(timedOut);
    CHECK(cancelledWaits == 1);
    if (stopInsteadOfResume) {
        CHECK(observed.empty());
        CHECK(channel->size_approx() == 1);
        CHECK(drainChecks == 0);
        CHECK(lifecycleNotifications >= 2);
    } else {
        CHECK((observed == std::vector<int>{42}));
        CHECK(channel->size_approx() == 0);
        CHECK(drainChecks == 1);
        CHECK(lifecycleNotifications >= 3);
    }
    CHECK(inFlight.load(std::memory_order_acquire) == 0);
    CHECK(callbacksInFlight.load(std::memory_order_acquire) == 0);
    CHECK_FALSE(startedFlag.load(std::memory_order_acquire));
}

} // namespace

TEST_CASE("PressureLimitedPoller coalesces normal-priority trickle into a bounded batch",
          "[daemon][poller][batch][coalesce][catch2]") {
    BatchPollerHarness harness{10ms};
    REQUIRE(harness.pushNormal(1));
    harness.postNormalAfterAdmission(1, {2, 3, 4});
    harness.run();

    CHECK_FALSE(harness.timedOut);
    CHECK(harness.deferredPushSucceeded());
    CHECK((harness.observed == std::vector<int>{1, 2, 3, 4}));
}

TEST_CASE("PressureLimitedPoller does not coalesce high-priority batch work",
          "[daemon][poller][batch][coalesce][priority][catch2]") {
    BatchPollerHarness harness{20ms};
    REQUIRE(harness.pushHighPriority(42));
    harness.scheduleNormal(7, 2ms);
    harness.run();

    CHECK((harness.observed == std::vector<int>{42}));
}

TEST_CASE("PressureLimitedPoller bounds lone normal-task coalescing latency",
          "[daemon][poller][batch][coalesce][latency][catch2]") {
    BatchPollerHarness harness{3ms};
    REQUIRE(harness.pushNormal(9));
    harness.run(50ms);

    CHECK_FALSE(harness.timedOut);
    CHECK((harness.observed == std::vector<int>{9}));
    CHECK((harness.elapsed < 50ms));
}

TEST_CASE("PressureLimitedPoller assigns unique limiter IDs to duplicate task identities",
          "[daemon][poller][limiter][identity][catch2]") {
    BatchPollerHarness harness{0ms};
    REQUIRE(harness.pushNormal(7));
    REQUIRE(harness.pushNormal(7));
    harness.run();

    REQUIRE((harness.acquiredLimiterIds().size() == 2));
    CHECK((harness.acquiredLimiterIds()[0] != harness.acquiredLimiterIds()[1]));
    CHECK((harness.completedLimiterIds() == harness.acquiredLimiterIds()));
}

TEST_CASE("PressureLimitedPoller batch path skips identities without a limiter",
          "[daemon][poller][batch][limiter][identity][catch2]") {
    BatchPollerHarness harness{0ms, false};
    REQUIRE(harness.pushNormal(7));
    REQUIRE(harness.pushNormal(8));
    harness.run();

    CHECK((harness.observed == std::vector<int>{7, 8}));
    CHECK((harness.hashCalls() == 0));
    CHECK(harness.acquiredLimiterIds().empty());
    CHECK(harness.completedLimiterIds().empty());
}

TEST_CASE("PressureLimitedPoller single-item path skips identities without a limiter",
          "[daemon][poller][single][limiter][identity][catch2]") {
    boost::asio::io_context ioc;
    std::atomic<bool> stopFlag{false};
    std::atomic<bool> startedFlag{false};
    std::atomic<bool> pauseFlag{false};
    std::atomic<bool> wasActive{false};
    std::atomic<std::size_t> inFlight{0};
    std::mutex wakeMutex;
    auto wakeTimer = std::make_shared<boost::asio::steady_timer>(ioc);
    auto channel = std::make_shared<SpscQueue<int>>(16);
    REQUIRE(channel->try_push(9));

    std::size_t hashCalls = 0;
    std::size_t completedCalls = 0;
    std::vector<int> observed;
    PressureLimitedPollerConfig<int> cfg;
    cfg.stageName = "test-single";
    cfg.stopFlag = &stopFlag;
    cfg.startedFlag = &startedFlag;
    cfg.pauseFlag = &pauseFlag;
    cfg.wasActiveFlag = &wasActive;
    cfg.inFlightCounter = &inFlight;
    cfg.getLimiterFn = []() -> GradientLimiter* { return nullptr; };
    cfg.maxConcurrentFn = []() -> std::size_t { return 1; };
    cfg.tryAcquireFn = [](GradientLimiter*, const std::string&, const std::string&) {
        return true;
    };
    cfg.completeJobFn = [&completedCalls](const std::string&, bool) { ++completedCalls; };
    cfg.getHashFn = [&hashCalls](const int& task) {
        ++hashCalls;
        return std::to_string(task);
    };
    cfg.processFn = [&observed, &stopFlag, &wakeTimer, &wakeMutex](int& task) {
        observed.push_back(task);
        stopFlag.store(true, std::memory_order_release);
        std::lock_guard<std::mutex> lock(wakeMutex);
        wakeTimer->cancel();
    };
    cfg.executor = ioc.get_executor();
    cfg.enableCpuThrottling = false;
    cfg.wakeTimer = wakeTimer;
    cfg.wakeTimerMutex = &wakeMutex;

    boost::asio::co_spawn(ioc, pressureLimitedPoll<int>(channel, std::move(cfg)),
                          boost::asio::detached);
    ioc.run();

    CHECK((observed == std::vector<int>{9}));
    CHECK((hashCalls == 0));
    CHECK((completedCalls == 0));
}

TEST_CASE("PressureLimitedPoller blocked branches arm and honor the registered wake timer",
          "[daemon][poller][wake-timer][blocked][catch2]") {
    SECTION("paused") {
        checkRegisteredWakeResumesBlockedPoller(BlockedPollerReason::Paused);
    }
    SECTION("zero concurrency") {
        checkRegisteredWakeResumesBlockedPoller(BlockedPollerReason::ZeroConcurrency);
    }
    SECTION("admission paused") {
        checkRegisteredWakeResumesBlockedPoller(BlockedPollerReason::AdmissionPaused);
    }
    SECTION("enqueue after registered wait") {
        checkRegisteredWakeResumesBlockedPoller(BlockedPollerReason::Paused, false, true);
    }
    SECTION("stop while paused preserves queued work") {
        checkRegisteredWakeResumesBlockedPoller(BlockedPollerReason::Paused, true);
    }
    SECTION("stop while concurrency is zero preserves queued work") {
        checkRegisteredWakeResumesBlockedPoller(BlockedPollerReason::ZeroConcurrency, true);
    }
    SECTION("stop while admission is paused preserves queued work") {
        checkRegisteredWakeResumesBlockedPoller(BlockedPollerReason::AdmissionPaused, true);
    }
}

TEST_CASE("PressureLimitedPoller leaves source work queued while downstream admission is paused",
          "[daemon][poller][backpressure][catch2]") {
    boost::asio::io_context ioc;
    std::atomic<bool> stopFlag{false};
    std::atomic<bool> startedFlag{false};
    std::atomic<bool> pauseFlag{false};
    std::atomic<bool> admissionPaused{true};
    std::atomic<bool> wasActive{false};
    std::atomic<std::size_t> inFlight{0};
    std::mutex wakeMutex;
    auto wakeTimer = std::make_shared<boost::asio::steady_timer>(ioc);
    auto watchdog = std::make_shared<boost::asio::steady_timer>(ioc, 250ms);
    auto channel = std::make_shared<SpscQueue<int>>(16);
    REQUIRE(channel->try_push(9));

    bool processedWhilePaused = false;
    std::vector<int> observed;
    PressureLimitedPollerConfig<int> cfg;
    cfg.stageName = "test-downstream-backpressure";
    cfg.stopFlag = &stopFlag;
    cfg.startedFlag = &startedFlag;
    cfg.pauseFlag = &pauseFlag;
    cfg.wasActiveFlag = &wasActive;
    cfg.inFlightCounter = &inFlight;
    cfg.getLimiterFn = []() -> GradientLimiter* { return nullptr; };
    cfg.maxConcurrentFn = []() -> std::size_t { return 1; };
    cfg.admissionPausedFn = [&admissionPaused] {
        return admissionPaused.load(std::memory_order_acquire);
    };
    cfg.tryAcquireFn = [](GradientLimiter*, const std::string&, const std::string&) {
        return true;
    };
    cfg.completeJobFn = [](const std::string&, bool) {};
    cfg.getHashFn = [](const int& task) { return std::to_string(task); };
    cfg.processFn = [&observed, &processedWhilePaused, &admissionPaused, &stopFlag, &wakeTimer,
                     &watchdog, &wakeMutex](int& task) {
        processedWhilePaused = admissionPaused.load(std::memory_order_acquire);
        observed.push_back(task);
        stopFlag.store(true, std::memory_order_release);
        watchdog->cancel();
        std::lock_guard<std::mutex> lock(wakeMutex);
        wakeTimer->cancel();
    };
    cfg.executor = ioc.get_executor();
    cfg.enableCpuThrottling = false;
    cfg.wakeTimer = wakeTimer;
    cfg.wakeTimerMutex = &wakeMutex;

    boost::asio::steady_timer releaseTimer(ioc, 10ms);
    releaseTimer.async_wait([&admissionPaused](const boost::system::error_code& error) {
        if (!error) {
            admissionPaused.store(false, std::memory_order_release);
        }
    });
    watchdog->async_wait(
        [&stopFlag, &wakeTimer, &wakeMutex](const boost::system::error_code& error) {
            if (!error) {
                stopFlag.store(true, std::memory_order_release);
                std::lock_guard<std::mutex> lock(wakeMutex);
                wakeTimer->cancel();
            }
        });

    boost::asio::co_spawn(ioc, pressureLimitedPoll<int>(channel, std::move(cfg)),
                          boost::asio::detached);
    ioc.run();

    CHECK_FALSE(processedWhilePaused);
    CHECK((observed == std::vector<int>{9}));
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
    // (tuning-cadence expiry).  Cancel the timer to simulate signalAllWakeTimers(),
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

    // Set stopFlag WITHOUT explicitly cancelling the timer — the wake-timer
    // (worker poll period without a tuning snapshot) should expire naturally,
    // the poller sees stopFlag, and exits.
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

// ---------------------------------------------------------------------------
// Idle cadence (B2.2): an empty channel must not wake the poller every 10 ms
// ---------------------------------------------------------------------------

namespace {

/// Restores the process-wide tuning snapshot a test replaced.
struct TuningSnapshotScope {
    std::shared_ptr<const TuningSnapshot> saved{TuningSnapshotRegistry::instance().get()};
    ~TuningSnapshotScope() { TuningSnapshotRegistry::instance().set(saved); }
};

void setIdleSnapshot(bool daemonIdle) {
    auto snap = std::make_shared<TuningSnapshot>();
    snap->daemonIdle = daemonIdle;
    TuningSnapshotRegistry::instance().set(snap);
}

/// Runs an idle single-item poller for `window` and returns how many loop iterations it made.
/// maxConcurrentFn runs exactly once per iteration, so it doubles as the counter.
std::size_t countIdlePollerIterations(std::chrono::milliseconds window) {
    boost::asio::io_context ioc;
    std::atomic<bool> stopFlag{false};
    std::atomic<bool> startedFlag{false};
    std::atomic<bool> pauseFlag{false};
    std::mutex wakeMutex;
    auto wakeTimer = std::make_shared<boost::asio::steady_timer>(ioc);
    auto cfg = makeWakeTimerConfig<int>(stopFlag, startedFlag, pauseFlag, wakeTimer, wakeMutex);
    cfg.executor = ioc.get_executor();
    cfg.enableCpuThrottling = false;
    std::size_t iterations = 0;
    cfg.maxConcurrentFn = [&iterations]() -> std::size_t {
        ++iterations;
        return 1;
    };
    auto channel = std::make_shared<SpscQueue<int>>(16);
    boost::asio::co_spawn(ioc, pressureLimitedPoll<int>(channel, std::move(cfg)),
                          boost::asio::detached);

    boost::asio::steady_timer deadline(ioc, window);
    deadline.async_wait([&](boost::system::error_code) {
        stopFlag.store(true, std::memory_order_release);
        std::lock_guard<std::mutex> lock(wakeMutex);
        wakeTimer->cancel();
    });
    ioc.run();
    return iterations;
}

} // namespace

TEST_CASE("PressureLimitedPoller idle wait follows the tuning cadence",
          "[daemon][poller][idle][catch2]") {
    TuningSnapshotScope scope;
    constexpr auto window = 400ms;

    SECTION("without a snapshot the empty-channel wait is the worker poll period") {
        TuningSnapshotRegistry::instance().set(nullptr);
        const auto pollMs = TuneAdvisor::workerPollMs();
        REQUIRE(pollMs >= 50u);
        // Start iteration, one per poll period, and slack for timer jitter. The former fixed
        // 10 ms safety net made about 40 iterations in this window.
        const std::size_t bound = 2 + static_cast<std::size_t>(window.count() / pollMs) + 1;
        const auto iterations = countIdlePollerIterations(window);
        CHECK(iterations >= 1);
        CHECK(iterations <= bound);
    }

    SECTION("while the daemon is idle the empty-channel wait is the idle tick") {
        setIdleSnapshot(true);
        REQUIRE(TuneAdvisor::idleTickMs() >= 2 * static_cast<uint32_t>(window.count()));
        CHECK(countIdlePollerIterations(window) <= 2);
    }
}

TEST_CASE("PressureLimitedPoller wakes promptly for an enqueue signalled while idle",
          "[daemon][poller][idle][wake-timer][catch2]") {
    TuningSnapshotScope scope;
    setIdleSnapshot(true); // empty-channel wait is the idle tick (1 s)

    boost::asio::io_context ioc;
    std::atomic<bool> stopFlag{false};
    std::atomic<bool> startedFlag{false};
    std::atomic<bool> pauseFlag{false};
    std::mutex wakeMutex;
    auto wakeTimer = std::make_shared<boost::asio::steady_timer>(ioc);
    auto cfg = makeWakeTimerConfig<int>(stopFlag, startedFlag, pauseFlag, wakeTimer, wakeMutex);
    cfg.executor = ioc.get_executor();
    cfg.enableCpuThrottling = false;

    boost::asio::steady_timer deadline(ioc, 3000ms);
    std::optional<std::chrono::steady_clock::time_point> pushedAt;
    std::optional<std::chrono::steady_clock::time_point> processedAt;
    cfg.processFn = [&](int&) {
        processedAt = std::chrono::steady_clock::now();
        stopFlag.store(true, std::memory_order_release);
        deadline.cancel();
        std::lock_guard<std::mutex> lock(wakeMutex);
        wakeTimer->cancel();
    };
    auto channel = std::make_shared<SpscQueue<int>>(16);
    boost::asio::co_spawn(ioc, pressureLimitedPoll<int>(channel, std::move(cfg)),
                          boost::asio::detached);

    // Enqueue once the poller is parked on its idle wait, signalling the way
    // PostIngestQueue::signalWakeTimer does: cancel posted to the timer's executor.
    boost::asio::steady_timer arrival(ioc, 60ms);
    arrival.async_wait([&](boost::system::error_code ec) {
        if (ec) {
            return;
        }
        pushedAt = std::chrono::steady_clock::now();
        REQUIRE(channel->try_push(7));
        boost::asio::post(wakeTimer->get_executor(), [wakeTimer, &wakeMutex]() {
            std::lock_guard<std::mutex> lock(wakeMutex);
            wakeTimer->cancel();
        });
    });
    deadline.async_wait([&](boost::system::error_code ec) {
        if (!ec) {
            stopFlag.store(true, std::memory_order_release);
            std::lock_guard<std::mutex> lock(wakeMutex);
            wakeTimer->cancel();
        }
    });
    ioc.run();

    REQUIRE(pushedAt.has_value());
    REQUIRE(processedAt.has_value());
    CHECK(*processedAt - *pushedAt < 100ms);
}

TEST_CASE("PressureLimitedPoller re-polls blocked work on the short cadence",
          "[daemon][poller][idle][catch2]") {
    // Queued work that cannot be admitted (here: maxConcurrent saturated by an in-flight job)
    // must not sleep the idle tick, because job completion does not signal the wake timer.
    TuningSnapshotScope scope;
    setIdleSnapshot(true);

    boost::asio::io_context ioc;
    std::atomic<bool> stopFlag{false};
    std::atomic<bool> startedFlag{false};
    std::atomic<bool> pauseFlag{false};
    std::atomic<bool> wasActive{false};
    std::atomic<std::size_t> inFlight{1}; // one phantom job occupies the single slot
    std::mutex wakeMutex;
    auto wakeTimer = std::make_shared<boost::asio::steady_timer>(ioc);
    auto cfg = makeWakeTimerConfig<int>(stopFlag, startedFlag, pauseFlag, wakeTimer, wakeMutex);
    cfg.executor = ioc.get_executor();
    cfg.enableCpuThrottling = false;
    cfg.wasActiveFlag = &wasActive;
    cfg.inFlightCounter = &inFlight;

    boost::asio::steady_timer deadline(ioc, 3000ms);
    const auto startedAt = std::chrono::steady_clock::now();
    std::optional<std::chrono::steady_clock::time_point> processedAt;
    cfg.processFn = [&](int&) {
        processedAt = std::chrono::steady_clock::now();
        stopFlag.store(true, std::memory_order_release);
        deadline.cancel();
        std::lock_guard<std::mutex> lock(wakeMutex);
        wakeTimer->cancel();
    };
    auto channel = std::make_shared<SpscQueue<int>>(16);
    REQUIRE(channel->try_push(1));
    boost::asio::co_spawn(ioc, pressureLimitedPoll<int>(channel, std::move(cfg)),
                          boost::asio::detached);

    // Release the slot without any wake signal, as completeJob does in production.
    boost::asio::steady_timer release(ioc, 100ms);
    release.async_wait([&](boost::system::error_code ec) {
        if (!ec) {
            inFlight.store(0, std::memory_order_release);
        }
    });
    deadline.async_wait([&](boost::system::error_code ec) {
        if (!ec) {
            stopFlag.store(true, std::memory_order_release);
            std::lock_guard<std::mutex> lock(wakeMutex);
            wakeTimer->cancel();
        }
    });
    ioc.run();

    REQUIRE(processedAt.has_value());
    // Blocked re-poll is at most 50 ms while idle; the idle tick would be 1 s.
    CHECK(*processedAt - startedAt < 400ms);
}

// ----------------------------------------------------------------------------
// lt-safety A2.2: exit contract after cancellation (was lt_red_poller_started_flag)
// ----------------------------------------------------------------------------

TEST_CASE("PressureLimitedPoller exits when its cancellation slot fires while the io_context "
          "keeps running",
          "[daemon][poller][shutdown][cancel][catch2]") {
    // The cancellation reaches the idle wake as operation_aborted, which the wake helper cannot
    // tell from an enqueuer's wake and swallows. Before A2.2 the loop then re-entered a co_await
    // that threw at once (throw_if_cancelled) into the loop's catch, forever, on its worker.
    boost::asio::io_context ioc;
    auto work = boost::asio::make_work_guard(ioc);

    std::atomic<bool> stopFlag{false};
    std::atomic<bool> startedFlag{false};
    std::atomic<bool> pauseFlag{false};
    std::atomic<int> lifecycleNotifications{0};
    std::mutex wakeMutex;
    auto wakeTimer = std::make_shared<boost::asio::steady_timer>(ioc);
    auto cfg = makeWakeTimerConfig<int>(stopFlag, startedFlag, pauseFlag, wakeTimer, wakeMutex);
    cfg.notifyLifecycleFn = [&lifecycleNotifications]() { ++lifecycleNotifications; };
    auto channel = std::make_shared<SpscQueue<int>>(16);

    boost::asio::cancellation_signal signal;
    std::atomic<bool> completed{false};
    boost::asio::co_spawn(
        ioc, pressureLimitedPoll<int>(channel, cfg),
        boost::asio::bind_cancellation_slot(
            signal.slot(), [&completed](std::exception_ptr) { completed.store(true); }));
    std::thread runner([&ioc]() { ioc.run(); });

    REQUIRE(yams::test::wait_for_condition(
        2000ms, 1ms, [&]() { return startedFlag.load(std::memory_order_acquire); }));
    // Emit on the io_context thread: the slot is installed there.
    boost::asio::post(ioc, [&signal]() { signal.emit(boost::asio::cancellation_type::all); });

    const bool exited = yams::test::wait_for_condition(2000ms, 1ms, [&]() {
        return completed.load() && !startedFlag.load(std::memory_order_acquire);
    });
    // Recovery for the pre-A2.2 spin: the stop flag is the only thing it still reads.
    stopFlag.store(true, std::memory_order_release);
    work.reset();
    ioc.stop();
    runner.join();

    CHECK(exited);
    CHECK_FALSE(startedFlag.load(std::memory_order_acquire));
    CHECK((lifecycleNotifications.load() >= 2)); // start and exit
}

TEST_CASE("PressureLimitedPoller clears startedFlag when an abandoned frame is destroyed",
          "[daemon][poller][shutdown][catch2]") {
    // A stopped io_context never resumes a suspended poller; the owner only learns that it is
    // gone from the frame's exit guard when the io_context is destroyed.
    std::atomic<bool> stopFlag{false};
    std::atomic<bool> startedFlag{false};
    std::atomic<bool> pauseFlag{false};
    std::atomic<int> lifecycleNotifications{0};
    std::mutex wakeMutex;
    auto channel = std::make_shared<SpscQueue<int>>(16);
    {
        boost::asio::io_context ioc;
        auto wakeTimer = std::make_shared<boost::asio::steady_timer>(ioc);
        auto cfg = makeWakeTimerConfig<int>(stopFlag, startedFlag, pauseFlag, wakeTimer, wakeMutex);
        cfg.notifyLifecycleFn = [&lifecycleNotifications]() { ++lifecycleNotifications; };
        boost::asio::co_spawn(ioc, pressureLimitedPoll<int>(channel, cfg), boost::asio::detached);

        ioc.run_for(30ms); // starts the poller and leaves it suspended on its wake timer
        REQUIRE(startedFlag.load(std::memory_order_acquire));
        ioc.stop();
        CHECK(startedFlag.load(std::memory_order_acquire));
    } // ~io_context destroys the suspended frame

    CHECK_FALSE(startedFlag.load(std::memory_order_acquire));
    CHECK((lifecycleNotifications.load() >= 2)); // start and exit
}

TEST_CASE("PressureLimitedPoller stops cleanly with WorkCoordinator::stop",
          "[daemon][poller][shutdown][work_coordinator][catch2]") {
    // lt-safety A2.2 (was lt_red_poller_started_flag): whether the poller is abandoned at its
    // wake timer or resumed with the cancellation, the workers join and the started flag is
    // clear once the coordinator is gone.
    struct Shared {
        std::atomic<bool> stop{false};
        std::atomic<bool> started{false};
        std::atomic<bool> pause{false};
        std::atomic<bool> wasActive{false};
        std::atomic<std::size_t> inFlight{0};
        std::atomic<std::uint64_t> iterations{0};
        std::mutex wakeMutex;
    };
    auto sh = std::make_shared<Shared>();

    bool joined = false;
    {
        WorkCoordinator wc;
        wc.start(2);
        auto channel = std::make_shared<SpscQueue<int>>(16);

        PressureLimitedPollerConfig<int> cfg{};
        cfg.stageName = "test-wc-stop";
        cfg.stopFlag = &sh->stop;
        cfg.startedFlag = &sh->started;
        cfg.pauseFlag = &sh->pause;
        cfg.wasActiveFlag = &sh->wasActive;
        cfg.inFlightCounter = &sh->inFlight;
        cfg.isCapableFn = [sh]() {
            sh->iterations.fetch_add(1, std::memory_order_relaxed);
            return true;
        };
        cfg.maxConcurrentFn = []() -> std::size_t { return 1; };
        cfg.tryAcquireFn = [](GradientLimiter*, const std::string&, const std::string&) {
            return true;
        };
        cfg.getHashFn = [](const int&) -> std::string { return ""; };
        cfg.completeJobFn = [](const std::string&, bool) {};
        cfg.processFn = [](int&) {};
        cfg.enableCpuThrottling = false;
        cfg.wakeTimer = std::make_shared<boost::asio::steady_timer>(*wc.getIOContext());
        cfg.wakeTimerMutex = &sh->wakeMutex;

        wc.spawnDetached(wc.getExecutor(), pressureLimitedPoll<int>(channel, cfg));

        REQUIRE(yams::test::wait_for_condition(2000ms, 1ms,
                                               [&] { return sh->iterations.load() != 0; }));
        wc.stop();
        joined = wc.joinWithTimeout(5s);
        if (!joined) {
            sh->stop.store(true, std::memory_order_release); // recovery only
            REQUIRE(wc.joinWithTimeout(10s));
        }
    }

    REQUIRE(joined);
    CHECK_FALSE(sh->started.load());
}
