// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (c) 2024-2025 YAMS Project Contributors
//
// lt-safety: shutdown and lifetime races pinned as expected failures.
//
// Each functional case asserts the behaviour the shutdown design promises and fails on current
// code. A strict XML wrapper accepts only its named CHECK failure. Every wait is bounded; the
// hidden sanitizer-only historical reproducer is manual and unvalidated in this lane.

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
#include <utility>

#include <spdlog/spdlog.h>
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
#include <yams/daemon/components/MetricsSnapshotRegistry.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/WorkCoordinator.h>
#include <yams/daemon/pressure_limited_poller.h>

using namespace std::chrono_literals;
using namespace yams;
using namespace yams::daemon;
using yams::test::wait_for_condition;

namespace {

#if defined(__has_feature)
#if __has_feature(thread_sanitizer) || __has_feature(address_sanitizer)
constexpr bool kSanitizerBuild = true;
#else
constexpr bool kSanitizerBuild = false;
#endif
#elif defined(__SANITIZE_THREAD__) || defined(__SANITIZE_ADDRESS__)
constexpr bool kSanitizerBuild = true;
#else
constexpr bool kSanitizerBuild = false;
#endif

template <typename Function> class ScopeExit {
public:
    explicit ScopeExit(Function function) : function_(std::move(function)) {}
    ScopeExit(const ScopeExit&) = delete;
    ScopeExit& operator=(const ScopeExit&) = delete;
    ~ScopeExit() noexcept {
        if (!active_) {
            return;
        }
        try {
            function_();
        } catch (...) {
        }
    }

    void dismiss() noexcept { active_ = false; }

private:
    Function function_;
    bool active_{true};
};

template <typename Function> ScopeExit<Function> makeScopeExit(Function function) {
    return ScopeExit<Function>(std::move(function));
}

/// Raises the default logger's level for one case and restores it.
class QuietLogs {
public:
    QuietLogs() : previous_(spdlog::get_level()) { spdlog::set_level(spdlog::level::critical); }
    ~QuietLogs() { spdlog::set_level(previous_); }
    QuietLogs(const QuietLogs&) = delete;
    QuietLogs& operator=(const QuietLogs&) = delete;

private:
    spdlog::level::level_enum previous_;
};

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
                   ("lt_red_" +
                    std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
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

struct RetainedMetricsOwnerGraph {
    std::shared_ptr<SmFixture> fixture;
    std::shared_ptr<ServiceManager> serviceManager;
    std::shared_ptr<DaemonMetrics> metrics;
};

void retainMetricsOwnerGraph(const std::shared_ptr<SmFixture>& fixture,
                             const std::shared_ptr<ServiceManager>& serviceManager,
                             const std::shared_ptr<DaemonMetrics>& metrics) {
    // Deliberately never destroyed. The red branch has a thread blocked inside metrics and each
    // registered case runs in its own process. Retaining the complete graph avoids racing teardown
    // or leaving ServiceManager's fixture references dangling before process exit.
    static RetainedMetricsOwnerGraph* retained = nullptr;
    if (!retained) {
        retained = new RetainedMetricsOwnerGraph{fixture, serviceManager, metrics};
    }
}

boost::asio::awaitable<void>
runNeverSuspendingCoroutine(std::shared_ptr<std::atomic<bool>> entered,
                            std::shared_ptr<std::atomic<bool>> sawCancel,
                            std::shared_ptr<std::atomic<bool>> release) {
    entered->store(true, std::memory_order_release);
    const auto deadline = std::chrono::steady_clock::now() + 4s;
    while (std::chrono::steady_clock::now() < deadline &&
           !release->load(std::memory_order_acquire)) {
        const auto cancellationState = co_await boost::asio::this_coro::cancellation_state;
        if (cancellationState.cancelled() != boost::asio::cancellation_type::none) {
            sawCancel->store(true, std::memory_order_release);
            co_return;
        }
        std::this_thread::sleep_for(1ms);
    }
}

} // namespace

// B3 — request_dispatcher_prune.cpp:138-213 spawns a coroutine whose only "suspension" is
// co_await this_coro::cancellation_state, which never suspends. WorkCoordinator::stop()
// (WorkCoordinator.cpp:286-302) posts the cancel to that coroutine's strand and only calls
// ioContext->stop() once every posted cancel has run, so one busy strand keeps every idle
// worker inside run() past the join budget. Fix: lt-safety A2.1.
TEST_CASE("lt_red_detached_never_suspends", "[lt-red][work_coordinator][shutdown]") {
    constexpr std::size_t kWorkers = 4;
    WorkCoordinator wc;
    wc.start(kWorkers);

    auto entered = std::make_shared<std::atomic<bool>>(false);
    auto sawCancel = std::make_shared<std::atomic<bool>>(false);
    auto release = std::make_shared<std::atomic<bool>>(false);

    wc.spawnDetached(wc.getExecutor(), runNeverSuspendingCoroutine(entered, sawCancel, release));

    REQUIRE(wait_for_condition(2000ms, 1ms, [&] { return entered->load(); }));

    wc.stop();
    (void)wc.joinWithTimeout(500ms);
    const auto stillActive = wc.getActiveWorkerCount();
    INFO("busy coroutine observed cancellation: " << sawCancel->load());

    // Expected: stopping the context releases the three idle workers. The synchronous coroutine
    // itself cannot be interrupted until it returns.
    CHECK(stillActive <= 1);

    release->store(true, std::memory_order_release);
    REQUIRE(wc.joinWithTimeout(10s));
}

// B4 — one assertion pins two failure modes of pressureLimitedPoll after WorkCoordinator::stop():
// (a) ioContext->stop() runs as soon as the posted cancel handlers have run, so a poller suspended
//     on its wake timer is abandoned there. Its exit path (pressure_limited_poller.h:506-512:
//     startedFlag=false, notifyLifecycleFn) is not RAII, so the owner's stageStarted_ flag stays
//     true and PostIngestQueue::stop() / ~PostIngestQueue burn their full quiesce budgets.
// (b) When another worker dispatches the cancelled wait's completion inline (strand already free,
//     blocking.possibly) the poller resumes: awaitPressureLimitedPollerWake swallows
//     operation_aborted (:206-223), the loop continues, every later co_await throws at once
//     (throw_if_cancelled) into the catch at :504, and the poller spins on its worker without
//     suspending. The join never completes (gdb stacks: grim artifacts/s1-hang, 2026-10-28).
// Fix: lt-safety A2.2 (exit on cancellation, RAII exit path).
TEST_CASE("lt_red_poller_started_flag", "[lt-red][poller][shutdown]") {
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
    // Failure mode (b) logs one error per spin iteration; keep the captured output small.
    QuietLogs quiet;

    bool joined = false;
    {
        WorkCoordinator wc;
        wc.start(2);
        auto channel = std::make_shared<SpscQueue<int>>(16);

        PressureLimitedPollerConfig<int> cfg{};
        cfg.stageName = "lt-red";
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

        REQUIRE(wait_for_condition(2000ms, 1ms, [&] { return sh->iterations.load() != 0; }));
        wc.stop();
        joined = wc.joinWithTimeout(2s);
        if (!joined) {
            // Failure mode (b): the stop flag is the only thing a spinning poller still reads.
            // Recovery so the coordinator can be destroyed; the verdict was taken above.
            sh->stop.store(true, std::memory_order_release);
            REQUIRE(wc.joinWithTimeout(10s));
        }
    }

    // Mode (a): the stopped io_context has now destroyed the abandoned coroutine frame, and an
    // RAII exit guard in that frame must have published that the poller is gone. Mode (b): the
    // workers must have been joinable without outside help.
    const bool pollerStoppedCleanly = joined && !sh->started.load();
    CHECK(pollerStoppedCleanly);
}

// B2 — DaemonMetrics::pollingLoop stores pollingTimer_ = &timer (DaemonMetrics.cpp:767) and
// completes pollingCompletion_ only on its normal exit path (:930-931). When the WorkCoordinator
// stops first the loop is abandoned at its co_await and never reaches that cleanup, so
// stopPolling() blocks in PollingCompletion::wait() (polling_completion.h:32-35, no timeout).
// Fix: lt-safety A2.2.
TEST_CASE("lt_red_metrics_stop_after_cancel", "[lt-red][metrics][shutdown]") {
    auto fixture = std::make_shared<SmFixture>();
    auto sm =
        std::make_shared<ServiceManager>(fixture->config_, fixture->state_, fixture->lifecycleFsm_);
    REQUIRE(sm->initialize());
    auto* wc = sm->getWorkCoordinator();
    REQUIRE((wc != nullptr));
    REQUIRE(wc->isRunning());

    auto& registry = MetricsSnapshotRegistry::instance();
    const auto priorSnapshot = registry.get();
    bool retainGraph = false;
    auto restoreRegistry = makeScopeExit([&] {
        if (!retainGraph) {
            registry.set(priorSnapshot);
        }
    });

    // shared_from_this() is used by off-strand collectors. Shared ownership is required even on
    // the green path; the red path retains this object together with every referenced owner.
    auto metrics = std::make_shared<DaemonMetrics>(nullptr, &fixture->state_, sm.get(), wc);
    metrics->startPolling();
    REQUIRE(wait_for_condition(5s, 1ms, [&] {
        const auto published = registry.get();
        return published && published != priorSnapshot;
    }));

    wc->stop();
    REQUIRE(wc->joinWithTimeout(10s));

    auto stopped = std::make_shared<std::atomic<bool>>(false);
    std::thread stopper([metrics, stopped] {
        metrics->stopPolling();
        stopped->store(true, std::memory_order_release);
    });
    const bool done =
        wait_for_condition(3s, 10ms, [&] { return stopped->load(std::memory_order_acquire); });
    if (done) {
        stopper.join();
        sm->shutdown();
    } else {
        // stopPolling() has an unbounded wait. Do not concurrently destroy any owner it can reach.
        retainGraph = true;
        retainMetricsOwnerGraph(fixture, sm, metrics);
        stopper.detach();
    }
    CHECK(done);
}

// B6 — the worker lambda (WorkCoordinator.cpp:175-240) captures `this` and, after
// io_context::run() returns, writes workerRunEnd_/workerExited_, activeWorkers_ and joinCV_.
// abandonWorkersForShutdown() (:575-595) detaches the thread and clears started_, so the
// destructor (:109-135) neither stops nor joins; the abandoned worker then runs against freed
// memory. There is no UB-free observation of that, so this case is hidden and has no Meson
// registration. It remains historical source only and is unvalidated here. Fix: lt-safety A2.5.
TEST_CASE("lt_red_abandoned_worker_uaf",
          "[.manual-unvalidated][lt-red][work_coordinator][abandon][sanitizer]") {
    if (!kSanitizerBuild) {
        SKIP("only conclusive under ThreadSanitizer or AddressSanitizer");
    }
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
    // The detached worker now runs WorkCoordinator.cpp:228-239 against the freed coordinator;
    // the sanitizer reports it and the process exits non-zero.
    std::this_thread::sleep_for(200ms);
    SUCCEED("functional path complete; the sanitizer report is the failure");
}

// B1 — Phase 3.9.5 (ServiceManager.cpp:1188-1190) resets writeCoordinator_ while the
// WorkCoordinator is still running (stop/join are Phase 4/5, :1216-1248) and before RepairService
// stops (Phase 6.0.5, :1360-1363). AppContext::writeCoordinatorProvider (:3923) and
// RepairService.cpp:1595,1710,1945,2065,2473,2523 read the raw pointer from other threads. A
// coroutine still running on the worker pool must never observe the provider going null.
// Fix: lt-safety A2.3.
// This case drives a full ServiceManager::shutdown() with live PostIngestQueue pollers. Under
// TSan it intermittently (2 of 24 runs) reports "unlock of an unlocked mutex" in
// PostIngestQueue::notifyLifecycle() called from a poller's notifyLifecycleFn after the queue is
// gone (B5 class, lt-safety A2.2/A2.4), which the strict wrapper rightly rejects. Until that fix
// lands, a TSan build registers this case outside the lt-red and unit suites (tests/meson.build).
TEST_CASE_METHOD(SmFixture, "lt_red_write_coordinator_lifetime",
                 "[lt-red][service_manager][shutdown][write_coordinator]") {
    auto sm = std::make_shared<ServiceManager>(config_, state_, lifecycleFsm_);
    REQUIRE(sm->initialize());
    auto gate = std::make_shared<ReleaseGate>();
    std::thread shutdownThread;
    auto releaseAndJoin = makeScopeExit([&] {
        gate->release();
        if (shutdownThread.joinable()) {
            shutdownThread.join();
        }
    });

    sm->startAsyncInit();
    const auto ready = sm->waitForServiceManagerTerminalState(60);
    REQUIRE((ready.state == ServiceManagerState::Ready));

    auto ctx = sm->getAppContext();
    REQUIRE(ctx.writeCoordinatorProvider);
    REQUIRE((ctx.writeCoordinatorProvider() != nullptr));
    REQUIRE(ctx.cancellationSignal);

    auto* wc = sm->getWorkCoordinator();
    REQUIRE((wc != nullptr));
    auto workerEntered = std::make_shared<std::atomic<bool>>(false);
    boost::asio::post(sm->getWorkerExecutor(), [gate, workerEntered] {
        workerEntered->store(true, std::memory_order_release);
        gate->wait();
    });
    REQUIRE(wait_for_condition(2s, 1ms,
                               [&] { return workerEntered->load(std::memory_order_acquire); }));

    shutdownThread = std::thread([sm] { sm->shutdown(); });

    const bool cancellationObserved = wait_for_condition(
        30s, 1ms, [&] { return ctx.cancellationSignal->load(std::memory_order_acquire); });
    if (!cancellationObserved) {
        FAIL("setup failure: shutdown did not publish AppContext cancellation");
    }

    // Phase 3.9.5 currently resets writeCoordinator_ before Phase 4 publishes cancellation.
    // The blocked worker proves the worker pool is still live at this observation point.
    const bool providerNonnullBeforeWorkerRelease = ctx.writeCoordinatorProvider() != nullptr;
    gate->release();
    shutdownThread.join();
    releaseAndJoin.dismiss();

    CHECK(providerNonnullBeforeWorkerRelease);
}
