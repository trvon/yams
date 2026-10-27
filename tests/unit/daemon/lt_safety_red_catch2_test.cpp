// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (c) 2024-2025 YAMS Project Contributors
//
// lt-safety track A, phase A1: red tests.
//
// Each test asserts the behaviour the shutdown design promises and fails on the current
// code. They live in their own suite ('lt-red') so the unit lane is unaffected. Two of
// them (abandoned worker, WriteCoordinator provider) are only conclusive under TSan/ASan.

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include "../../common/test_helpers_catch2.h"
#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/DaemonMetrics.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/WorkCoordinator.h>
#include <yams/daemon/pressure_limited_poller.h>

using namespace std::chrono_literals;
using namespace yams;
using namespace yams::daemon;
using yams::test::wait_for_condition;

namespace {

struct ReleaseGate {
    std::mutex m;
    std::condition_variable cv;
    bool open{false};
    void release() {
        {
            std::lock_guard<std::mutex> lk(m);
            open = true;
        }
        cv.notify_all();
    }
    void wait() {
        std::unique_lock<std::mutex> lk(m);
        cv.wait(lk, [this] { return open; });
    }
};

struct SmFixture {
    yams::test::ScopedEnvVar disableWatcher_{"YAMS_DISABLE_SESSION_WATCHER", std::string("1")};
    DaemonConfig config_;
    StateComponent state_;
    DaemonLifecycleFsm lifecycleFsm_;
    std::filesystem::path testDir_;

    SmFixture() {
        testDir_ = std::filesystem::temp_directory_path() /
                   ("lt_red_" + std::to_string(std::chrono::steady_clock::now()
                                                   .time_since_epoch()
                                                   .count()));
        std::filesystem::create_directories(testDir_);
        config_.dataDir = testDir_ / "data";
        config_.socketPath = testDir_ / "daemon.sock";
        config_.pidFile = testDir_ / "daemon.pid";
        config_.logFile = testDir_ / "daemon.log";
        config_.enableModelProvider = false;
        config_.useMockModelProvider = false;
        config_.autoLoadPlugins = false;
        config_.enableAutoRepair = false;
        std::filesystem::create_directories(config_.dataDir);
    }
    ~SmFixture() {
        std::error_code ec;
        std::filesystem::remove_all(testDir_, ec);
    }
};

} // namespace

// Bug 3 class: request_dispatcher_prune.cpp:138-213 spawns a coroutine whose only
// "suspension" is co_await this_coro::cancellation_state, which never suspends. The cancel
// that WorkCoordinator::stop() posts to that coroutine's strand cannot run until the
// coroutine returns, and stop() (WorkCoordinator.cpp:286-302) only calls ioContext->stop()
// once every posted cancel has run. One busy coroutine therefore holds every idle worker.
TEST_CASE("RED: a detached coroutine that never suspends holds the whole pool past stop()",
          "[lt-red][work_coordinator][shutdown][prune]") {
    constexpr std::size_t kWorkers = 4;
    WorkCoordinator wc;
    wc.start(kWorkers);

    auto entered = std::make_shared<std::atomic<bool>>(false);
    auto sawCancel = std::make_shared<std::atomic<bool>>(false);
    auto release = std::make_shared<std::atomic<bool>>(false);

    wc.spawnDetached(wc.getExecutor(), [entered, sawCancel, release]() -> boost::asio::awaitable<void> {
        entered->store(true, std::memory_order_release);
        const auto deadline = std::chrono::steady_clock::now() + 4s;
        while (std::chrono::steady_clock::now() < deadline &&
               !release->load(std::memory_order_acquire)) {
            const auto cs = co_await boost::asio::this_coro::cancellation_state;
            if (cs.cancelled() != boost::asio::cancellation_type::none) {
                sawCancel->store(true, std::memory_order_release);
                co_return;
            }
            std::this_thread::sleep_for(1ms); // one "candidate" of synchronous work
        }
        co_return;
    }());

    REQUIRE(wait_for_condition(2000ms, 1ms, [&] { return entered->load(); }));

    wc.stop();
    const bool joined = wc.joinWithTimeout(500ms);
    const auto stillActive = wc.getActiveWorkerCount();

    // Expected: the three idle workers exit and the busy one observes cancellation.
    CHECK(joined);
    CHECK(sawCancel->load());
    CHECK((stillActive <= 1));

    release->store(true, std::memory_order_release);
    REQUIRE(wc.joinWithTimeout(10s));
}

// Bug 4 (re-scoped): WorkCoordinator::stop() (WorkCoordinator.cpp:290-302) calls
// ioContext->stop() as soon as the posted cancel handlers have run, so a poller suspended on
// its wake timer is abandoned at that suspension point. pressureLimitedPoll's exit path
// (pressure_limited_poller.h:506-512: startedFlag=false, notifyLifecycleFn) is not RAII, so
// the owner's stageStarted_ flag stays true and PostIngestQueue::stop()/~PostIngestQueue burn
// their full quiesce budgets. (The original "swallowed operation_aborted causes a hot spin"
// hypothesis is refuted by the iteration count this test records.)
TEST_CASE("RED: pressureLimitedPoll clears startedFlag when the WorkCoordinator stops",
          "[lt-red][poller][shutdown]") {
    WorkCoordinator wc;
    wc.start(2);

    struct Shared {
        std::atomic<bool> stop{false};
        std::atomic<bool> started{false};
        std::atomic<bool> pause{false};
        std::atomic<bool> wasActive{false};
        std::atomic<std::size_t> inFlight{0};
        std::atomic<std::uint64_t> capabilityChecks{0};
        std::mutex wakeMutex;
    };
    auto sh = std::make_shared<Shared>();
    auto channel = std::make_shared<SpscQueue<int>>(16);

    PressureLimitedPollerConfig<int> cfg{};
    cfg.stageName = "lt-red";
    cfg.stopFlag = &sh->stop;
    cfg.startedFlag = &sh->started;
    cfg.pauseFlag = &sh->pause;
    cfg.wasActiveFlag = &sh->wasActive;
    cfg.inFlightCounter = &sh->inFlight;
    // Count loop iterations through the capability hook; it runs once per iteration.
    cfg.isCapableFn = [sh]() {
        sh->capabilityChecks.fetch_add(1, std::memory_order_relaxed);
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

    REQUIRE(wait_for_condition(2000ms, 1ms, [&] { return sh->started.load(); }));
    std::this_thread::sleep_for(100ms);
    const auto before = sh->capabilityChecks.load();

    wc.stop();
    std::this_thread::sleep_for(300ms);
    const auto after = sh->capabilityChecks.load();
    const bool joined = wc.joinWithTimeout(500ms);

    // Observed: 0 iterations (the coroutine is abandoned, not spinning) and a fast join.
    INFO("iterations during 300ms after stop(): " << (after - before));
    CHECK((after - before) < 200);
    CHECK(joined);

    // Expected: the poller's exit path ran and the owner can observe it. Fails today.
    sh->stop.store(true);
    REQUIRE(wc.joinWithTimeout(10s));
    CHECK(wait_for_condition(2000ms, 1ms, [&] { return !sh->started.load(); }));
}

// Bug 2: DaemonMetrics::pollingLoop stores pollingTimer_ = &timer (DaemonMetrics.cpp:767)
// and only clears it / completes pollingCompletion_ on the normal exit path (:930-931).
// When the WorkCoordinator stops first (ioContext->stop() runs as soon as the posted cancel
// handlers have run) the loop is abandoned at its co_await and never reaches that cleanup;
// stopPolling() then waits forever on a completion that never arrives (polling_completion.h:32-35
// has no timeout).
TEST_CASE_METHOD(SmFixture,
                 "RED: DaemonMetrics::stopPolling returns after the WorkCoordinator cancelled it",
                 "[lt-red][metrics][shutdown]") {
    auto sm = std::make_shared<ServiceManager>(config_, state_, lifecycleFsm_);
    REQUIRE(sm->initialize());
    auto* wc = sm->getWorkCoordinator();
    REQUIRE((wc != nullptr));
    REQUIRE(wc->isRunning());

    // Heap-allocated on purpose: if stopPolling() hangs, ~DaemonMetrics would hang too.
    auto* metrics = new DaemonMetrics(nullptr, &state_, sm.get(), wc);
    metrics->startPolling();
    std::this_thread::sleep_for(200ms);

    wc->stop();
    REQUIRE(wc->joinWithTimeout(10s));

    std::atomic<bool> stopped{false};
    std::thread stopper([&] {
        metrics->stopPolling();
        stopped.store(true, std::memory_order_release);
    });
    const bool done = wait_for_condition(3000ms, 10ms, [&] { return stopped.load(); });
    CHECK(done);
    if (done) {
        stopper.join();
        delete metrics;
    } else {
        stopper.detach(); // leaks metrics; the wait is unbounded by design
    }
    sm->shutdown();
}

// Bug 6: the worker lambda (WorkCoordinator.cpp:175-240) captures `this` and, after
// io_context::run() returns, writes workerRunEnd_/workerExited_, activeWorkers_ and
// joinCV_. abandonWorkersForShutdown() detaches the thread and clears started_, so the
// destructor neither stops nor joins; the abandoned worker then touches freed memory.
// Conclusive only under TSan/ASan (free vs. later access).
TEST_CASE("RED: an abandoned worker outlives its WorkCoordinator",
          "[lt-red][work_coordinator][abandon][sanitizer]") {
    auto wc = std::make_unique<WorkCoordinator>();
    wc->start(2);

    auto gate = std::make_shared<ReleaseGate>();
    auto entered = std::make_shared<std::atomic<bool>>(false);
    auto exited = std::make_shared<std::atomic<bool>>(false);
    boost::asio::post(wc->getExecutor(), [gate, entered, exited] {
        entered->store(true);
        gate->wait();
        exited->store(true);
    });
    REQUIRE(wait_for_condition(2000ms, 1ms, [&] { return entered->load(); }));

    wc->stop();
    REQUIRE_FALSE(wc->joinWithTimeout(200ms));
    wc->abandonWorkersForShutdown();
    CHECK_FALSE(wc->isRunning());

    wc.reset(); // destructor sees started_ == false and returns without joining

    gate->release();
    REQUIRE(wait_for_condition(2000ms, 1ms, [&] { return exited->load(); }));
    // The detached worker now runs lines 228-239 against the freed coordinator.
    std::this_thread::sleep_for(200ms);
    SUCCEED("completed; look for a heap-use-after-free / race-with-free report above");
}

// Bug 1: Phase 3.9.5 (ServiceManager.cpp:1188-1190) resets writeCoordinator_ while the
// WorkCoordinator is still running (Phase 4/5) and before RepairService stops (Phase
// 6.0.5, :1360-1363). AppContext::writeCoordinatorProvider (:3923) and RepairService
// (RepairService.cpp:1595,1710,1945,2065,2473,2523) read the raw pointer from other
// threads. A worker that is still running must never observe the provider going null.
TEST_CASE_METHOD(SmFixture,
                 "RED: WriteCoordinator stays alive while worker threads can still reach it",
                 "[lt-red][service_manager][shutdown][write_coordinator]") {
    auto sm = std::make_shared<ServiceManager>(config_, state_, lifecycleFsm_);
    REQUIRE(sm->initialize());
    sm->startAsyncInit();
    const auto ready = sm->waitForServiceManagerTerminalState(60);
    REQUIRE((ready.state == ServiceManagerState::Ready));

    auto ctx = sm->getAppContext();
    REQUIRE(ctx.writeCoordinatorProvider);
    if (ctx.writeCoordinatorProvider() == nullptr) {
        SKIP("no WriteCoordinator in this configuration");
    }

    auto sawNullWhileRunning = std::make_shared<std::atomic<bool>>(false);
    auto probeDone = std::make_shared<std::atomic<bool>>(false);
    auto probeEntered = std::make_shared<std::atomic<bool>>(false);

    boost::asio::post(sm->getWorkerExecutor(), [ctx, sawNullWhileRunning, probeDone,
                                                probeEntered] {
        probeEntered->store(true);
        const auto deadline = std::chrono::steady_clock::now() + 20s;
        while (std::chrono::steady_clock::now() < deadline) {
            auto* wcoord = ctx.writeCoordinatorProvider();
            if (wcoord == nullptr) {
                sawNullWhileRunning->store(true);
                break;
            }
            (void)wcoord->queuedBatches(); // the deref RepairService performs
            std::this_thread::sleep_for(1ms);
        }
        probeDone->store(true);
    });
    REQUIRE(wait_for_condition(2000ms, 1ms, [&] { return probeEntered->load(); }));

    std::thread shutdownThread([sm] { sm->shutdown(); });
    shutdownThread.join();
    REQUIRE(wait_for_condition(25s, 5ms, [&] { return probeDone->load(); }));

    CHECK_FALSE(sawNullWhileRunning->load());
}
