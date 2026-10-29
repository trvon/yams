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

#include <boost/asio/post.hpp>

#include "../../common/test_helpers_catch2.h"
#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/WorkCoordinator.h>

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

} // namespace

// B3 (a detached coroutine that never suspends held the whole pool past stop()) went green with
// lt-safety A2.1; its regression test is "WorkCoordinator stop releases idle workers past a
// detached strand that never suspends" in work_coordinator_test.cpp.

// B4 (pressureLimitedPoll abandoned with startedFlag set, or spinning after a swallowed
// cancellation) and B2 (DaemonMetrics::stopPolling blocked forever after the WorkCoordinator
// cancelled the loop) went green with lt-safety A2.2. Regression tests: pressure_limited_poller
// ("exits when its cancellation slot fires while the io_context keeps running", "clears
// startedFlag when an abandoned frame is destroyed", "stops cleanly with WorkCoordinator::stop")
// and daemon_metrics_status ("stopPolling returns after the WorkCoordinator cancelled the loop").

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
