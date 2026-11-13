#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include <atomic>
#include <chrono>
#include <exception>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <spdlog/spdlog.h>
#include "../../common/test_helpers_catch2.h"
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/daemon.h>

namespace {
using namespace std::chrono_literals;

class ScopedLogLevel {
public:
    ScopedLogLevel() : previous_(spdlog::get_level()) { spdlog::set_level(spdlog::level::warn); }
    ~ScopedLogLevel() { spdlog::set_level(previous_); }
    ScopedLogLevel(const ScopedLogLevel&) = delete;
    ScopedLogLevel& operator=(const ScopedLogLevel&) = delete;

private:
    spdlog::level::level_enum previous_;
};

class IsolatedDaemon {
public:
    IsolatedDaemon() {
        const auto& root = temporary_.path();
        for (const auto* name : {
                 "HOME",
                 "XDG_CONFIG_HOME",
                 "XDG_DATA_HOME",
                 "XDG_STATE_HOME",
                 "XDG_CACHE_HOME",
                 "XDG_RUNTIME_DIR",
             }) {
            const auto directory = root / name;
            std::filesystem::create_directories(directory);
            std::filesystem::permissions(directory, std::filesystem::perms::owner_all);
            environment_.emplace_back(name, directory.string());
        }
        for (const auto* name : {
                 "YAMS_DATA_DIR",
                 "YAMS_STORAGE",
                 "YAMS_DAEMON_SOCKET",
                 "YAMS_DAEMON_SOCKET_PATH",
                 "YAMS_EMBED_BACKEND",
                 "YAMS_PREFERRED_MODEL",
                 "YAMS_EMBED_DIM",
                 "YAMS_RERANKER_MODEL",
             }) {
            environment_.emplace_back(name, std::nullopt);
        }
        environment_.emplace_back("YAMS_DISABLE_VECTORS", "1");
        const auto configPath = root / "config.toml";
        std::ofstream configFile(configPath);
        configFile << "config_version = 3\n[embeddings]\nenabled = false\n"
                      "[memory_sync]\nenabled = false\n";
        configFile.close();
        if (!configFile) {
            throw std::runtime_error("Failed to create isolated lifecycle test configuration");
        }
        environment_.emplace_back("YAMS_CONFIG", configPath.string());
        environment_.emplace_back("YAMS_CONFIG_PATH", configPath.string());

        yams::daemon::DaemonConfig config;
        config.configFilePath = configPath;
        config.dataDir = root / "corpus";
        config.socketPath = root / "daemon.sock";
        config.pidFile = root / "daemon.pid";
        config.logFile = root / "daemon.log";
        config.enableModelProvider = false;
        config.useMockModelProvider = false;
        config.autoLoadPlugins = false;
        config.enableAutoRepair = false;
        daemon_ = std::make_unique<yams::daemon::YamsDaemon>(config);
    }

    yams::daemon::YamsDaemon& daemon() { return *daemon_; }

private:
    ScopedLogLevel logging_;
    yams::test::TempDirGuard temporary_{"yams_lifecycle_owner_"};
    std::vector<yams::test::ScopedEnvVar> environment_;
    std::unique_ptr<yams::daemon::YamsDaemon> daemon_;
};

// Only this caller touches the waiter handle after the parked entry has returned.
// Cleanup joins the baseline's unexpectedly late waiter even if a CHECK fails.
class ParkedEntry {
public:
    ParkedEntry(yams::daemon::YamsDaemon& daemon, bool useTestHelper, bool checkedEntry = false)
        : daemon_(daemon), released_(release_.get_future().share()),
          arrived_(arrival_.get_future()), caller_([this, useTestHelper, checkedEntry] {
              arrival_.set_value();
              released_.wait();
              try {
                  if (checkedEntry) {
                      checkedResult_.emplace(
                          useTestHelper ? daemon_.testingStartAsyncInitWithoutRunLoopChecked()
                                        : daemon_.runLoopChecked());
                  } else if (useTestHelper) {
                      daemon_.testingStartAsyncInitWithoutRunLoop();
                  } else {
                      daemon_.runLoop();
                  }
              } catch (...) {
                  error_ = std::current_exception();
              }
          }) {}

    ParkedEntry(const ParkedEntry&) = delete;
    ParkedEntry& operator=(const ParkedEntry&) = delete;

    ~ParkedEntry() {
        daemon_.requestStop();
        release();
        if (caller_.joinable()) {
            caller_.join();
        }
        if (daemon_.initWaiterThread_.joinable()) {
            daemon_.initWaiterThread_.join();
        }
    }

    bool awaitArrival() {
        return arrived_.wait_for(std::chrono::seconds{5}) == std::future_status::ready;
    }

    void begin() { release(); }

    const yams::Result<void>& checkedResult() const {
        if (caller_.joinable() || !checkedResult_) {
            throw std::logic_error("Checked entry result requires a joined checked caller");
        }
        return *checkedResult_;
    }

    void finish() {
        release();
        caller_.join();
        if (error_) {
            std::rethrow_exception(error_);
        }
    }

private:
    void release() {
        if (!hasReleased_) {
            release_.set_value();
            hasReleased_ = true;
        }
    }

    yams::daemon::YamsDaemon& daemon_;
    std::promise<void> release_;
    std::promise<void> arrival_;
    std::shared_future<void> released_;
    std::future<void> arrived_;
    std::exception_ptr error_;
    std::optional<yams::Result<void>> checkedResult_;
    bool hasReleased_{false};
    std::thread caller_;
};

void requireStarted(yams::daemon::YamsDaemon& daemon) {
    auto started = daemon.start();
    INFO((started ? "daemon started" : started.error().message));
    REQUIRE(started.has_value());
}

void requireStopped(yams::daemon::YamsDaemon& daemon) {
    auto stopped = daemon.stop();
    INFO((stopped ? "daemon stopped" : stopped.error().message));
    REQUIRE(stopped.has_value());
}
} // namespace

TEST_CASE("Stopping before deferred initialization leaves no waiter",
          "[daemon][startup-ownership][control]") {
    IsolatedDaemon fixture;
    auto& daemon = fixture.daemon();
    requireStarted(daemon);
    requireStopped(daemon);
    CHECK(daemon.waitForStopCompletion(0ms));
    CHECK_FALSE(daemon.asyncInitBarrierSet_.load());
    CHECK_FALSE(daemon.initWaiterThread_.joinable());
}

namespace {
void checkParkedEntry(bool useTestHelper) {
    CAPTURE(useTestHelper);
    IsolatedDaemon fixture;
    auto& daemon = fixture.daemon();
    requireStarted(daemon);

    ParkedEntry caller(daemon, useTestHelper);
    REQUIRE(caller.awaitArrival());
    requireStopped(daemon);
    REQUIRE(daemon.waitForStopCompletion(0ms));
    REQUIRE_NOTHROW(caller.finish());

    CHECK_FALSE(daemon.isRunning());
    CHECK(daemon.isStopRequested());
    CHECK_FALSE(daemon.asyncInitBarrierSet_.load());
    CHECK_FALSE(daemon.initWaiterThread_.joinable());
    CHECK(daemon.waitForStopCompletion(0ms));
}
} // namespace

TEST_CASE("A parked runLoop caller cannot publish work after completed stop",
          "[daemon][startup-ownership][late-entry][late-runloop][concurrency]") {
    checkParkedEntry(false);
}

TEST_CASE("A parked initialization helper cannot publish work after completed stop",
          "[daemon][startup-ownership][late-entry][late-helper][concurrency]") {
    checkParkedEntry(true);
}

namespace {
void checkActiveEntry(bool useTestHelper) {
    CAPTURE(useTestHelper);
    IsolatedDaemon fixture;
    auto& daemon = fixture.daemon();
    requireStarted(daemon);
    ParkedEntry caller(daemon, useTestHelper);
    REQUIRE(caller.awaitArrival());
    caller.begin();
    const auto snapshot = daemon.serviceManager_->waitForServiceManagerTerminalState(5000);
    INFO(snapshot.lastError);
    CHECK(snapshot.state == yams::daemon::ServiceManagerState::Ready);
    daemon.requestStop();
    REQUIRE_NOTHROW(caller.finish());
    requireStopped(daemon);
    CHECK(daemon.asyncInitBarrierSet_.load());
    CHECK_FALSE(daemon.initWaiterThread_.joinable());
    CHECK(daemon.waitForStopCompletion(0ms));
}
} // namespace

TEST_CASE("A running daemon still accepts its normal initialization entry",
          "[daemon][startup-ownership][active-runloop][concurrency]") {
    checkActiveEntry(false);
}

TEST_CASE("A running daemon still accepts its initialization helper entry",
          "[daemon][startup-ownership][active-helper][concurrency]") {
    checkActiveEntry(true);
}

namespace {
class OutcomeGate {
public:
    void arriveAndWait() {
        arrival_.set_value();
        released_.wait();
    }
    bool awaitArrival() { return arrived_.wait_for(5s) == std::future_status::ready; }
    void release() {
        if (!hasReleased_) {
            release_.set_value();
            hasReleased_ = true;
        }
    }

private:
    std::promise<void> arrival_;
    std::promise<void> release_;
    std::future<void> arrived_{arrival_.get_future()};
    std::shared_future<void> released_{release_.get_future().share()};
    bool hasReleased_{false};
};

class ReleasePublicationGate {
public:
    explicit ReleasePublicationGate(OutcomeGate& gate) : gate_(gate) {}
    ~ReleasePublicationGate() { gate_.release(); }
    ReleasePublicationGate(const ReleasePublicationGate&) = delete;
    ReleasePublicationGate& operator=(const ReleasePublicationGate&) = delete;

private:
    OutcomeGate& gate_;
};

class ReleaseOutcomeGates {
public:
    ReleaseOutcomeGates(OutcomeGate& loop, OutcomeGate& outcome)
        : loop_(&loop), outcome_(&outcome) {}
    ~ReleaseOutcomeGates() {
        loop_->release();
        outcome_->release();
    }
    ReleaseOutcomeGates(const ReleaseOutcomeGates&) = delete;
    ReleaseOutcomeGates& operator=(const ReleaseOutcomeGates&) = delete;
    ReleaseOutcomeGates(ReleaseOutcomeGates&&) = delete;
    ReleaseOutcomeGates& operator=(ReleaseOutcomeGates&&) = delete;

private:
    OutcomeGate* loop_;
    OutcomeGate* outcome_;
};

bool awaitLifecycleReady(yams::daemon::YamsDaemon& daemon) {
    // No lifecycle CV exists. This bounded observation is only for the positive
    // control; the adversarial close ordering uses promises and joined callers.
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (std::chrono::steady_clock::now() < deadline) {
        if (daemon.getLifecycle().snapshot().state == yams::daemon::LifecycleState::Ready) {
            return true;
        }
        std::this_thread::yield();
    }
    return false;
}

void checkPublicationAfterClose(bool useTestHelper) {
    CAPTURE(useTestHelper);
    // The gate outlives the daemon and every caller that captures it.
    OutcomeGate publication;
    IsolatedDaemon fixture;
    auto& daemon = fixture.daemon();
    REQUIRE(
        daemon.testingSetBeforeInitWaiterPublicationObserver([&] { publication.arriveAndWait(); }));
    requireStarted(daemon);
    ParkedEntry caller(daemon, useTestHelper);
    ReleasePublicationGate releaseBeforeCallerJoin{publication};
    caller.begin();
    REQUIRE(publication.awaitArrival());
    REQUIRE(daemon.asyncInitBarrierSet_.load(std::memory_order_acquire));
    // Keep the independent async-init/drain seam out of this schedule.
    const auto service = daemon.serviceManager_->waitForServiceManagerTerminalState(5000);
    INFO(service.lastError);
    REQUIRE(service.state == yams::daemon::ServiceManagerState::Ready);

    auto stopping = std::async(std::launch::async, [&] { return daemon.stop(); });
    ReleasePublicationGate releaseBeforeStopJoin{publication};
    REQUIRE(stopping.wait_for(10s) == std::future_status::ready);
    auto stopped = stopping.get();
    INFO((stopped ? "daemon stopped" : stopped.error().message));
    REQUIRE(stopped.has_value());
    REQUIRE(daemon.waitForStopCompletion(0ms));
    REQUIRE(daemon.getLifecycle().snapshot().state == yams::daemon::LifecycleState::Stopped);

    publication.release();
    REQUIRE_NOTHROW(caller.finish());
    // No concurrent member access: entry has returned, and the worker never changes its handle.
    // ParkedEntry's destructor joins the baseline's unexpected late handle even on CHECK failure.
    CHECK_FALSE(daemon.initWaiterThread_.joinable());
    CHECK_FALSE(daemon.initHandled_.load(std::memory_order_acquire));
    CHECK_FALSE(daemon.isRunning());
    CHECK(daemon.isStopRequested());
}

void checkDeniedAdmission(bool useTestHelper) {
    CAPTURE(useTestHelper);
    std::atomic<bool> permit{false};
    std::atomic<int> samples{0};
    std::atomic<bool> backendReady{false};
    IsolatedDaemon fixture;
    auto& daemon = fixture.daemon();
    REQUIRE(daemon.testingSetAdditionalInitWaiterAdmission([&] {
        samples.fetch_add(1);
        const auto service = daemon.serviceManager_->waitForServiceManagerTerminalState(5000);
        backendReady.store(service.state == yams::daemon::ServiceManagerState::Ready);
        return permit.load();
    }));
    // The old normal entry must return even when it wrongly ignores denial.
    daemon.signalCheckHook_ = [&] {
        daemon.requestStop();
        return false;
    };
    requireStarted(daemon);
    ParkedEntry caller(daemon, useTestHelper, true);
    caller.begin();
    REQUIRE_NOTHROW(caller.finish());
    const auto& denied = caller.checkedResult();
    CHECK_FALSE(denied.has_value());
    CHECK(samples.load() == 1);
    CHECK(backendReady.load());
    CHECK_FALSE(daemon.initWaiterThread_.joinable());
    if (denied) {
        // Keep the baseline's wrongly created worker under real stop ownership.
        requireStopped(daemon);
        return;
    }
    REQUIRE(denied.error().code == yams::ErrorCode::ResourceExhausted);
    CHECK(daemon.getLifecycle().snapshot().state == yams::daemon::LifecycleState::Initializing);
    CHECK_FALSE(daemon.initHandled_.load());
    CHECK(daemon.isRunning());
    CHECK_FALSE(daemon.isStopRequested());

    permit.store(true);
    ParkedEntry retry(daemon, useTestHelper, true);
    retry.begin();
    REQUIRE_NOTHROW(retry.finish());
    REQUIRE(retry.checkedResult().has_value());
    CHECK(samples.load() == 2);
    CHECK(daemon.initWaiterThread_.joinable());
    requireStopped(daemon);
}

void checkAllowedAdmission(bool useTestHelper) {
    CAPTURE(useTestHelper);
    std::atomic<int> samples{0};
    IsolatedDaemon fixture;
    auto& daemon = fixture.daemon();
    REQUIRE(daemon.testingSetAdditionalInitWaiterAdmission([&] {
        samples.fetch_add(1);
        return true;
    }));
    daemon.signalCheckHook_ = [&] {
        daemon.requestStop();
        return false;
    };
    requireStarted(daemon);
    ParkedEntry caller(daemon, useTestHelper, true);
    caller.begin();
    REQUIRE_NOTHROW(caller.finish());
    CHECK(caller.checkedResult().has_value());
    CHECK(samples.load() == 1);
    CHECK(daemon.initWaiterThread_.joinable());
    requireStopped(daemon);
    CHECK_FALSE(daemon.initWaiterThread_.joinable());
}

void checkAdmissionCancelled(bool useTestHelper) {
    CAPTURE(useTestHelper);
    OutcomeGate decision;
    IsolatedDaemon fixture;
    auto& daemon = fixture.daemon();
    REQUIRE(daemon.testingSetAdditionalInitWaiterAdmission([&] {
        decision.arriveAndWait();
        return true;
    }));
    requireStarted(daemon);
    ParkedEntry caller(daemon, useTestHelper, true);
    ReleasePublicationGate releaseBeforeCallerJoin{decision};
    caller.begin();
    REQUIRE(decision.awaitArrival());
    const auto service = daemon.serviceManager_->waitForServiceManagerTerminalState(5000);
    INFO(service.lastError);
    REQUIRE(service.state == yams::daemon::ServiceManagerState::Ready);
    auto stopping = std::async(std::launch::async, [&] { return daemon.stop(); });
    ReleasePublicationGate releaseBeforeStopJoin{decision};
    REQUIRE(stopping.wait_for(10s) == std::future_status::ready);
    auto stopped = stopping.get();
    REQUIRE(stopped.has_value());
    REQUIRE(daemon.waitForStopCompletion(0ms));
    REQUIRE(daemon.getLifecycle().snapshot().state == yams::daemon::LifecycleState::Stopped);
    decision.release();
    REQUIRE_NOTHROW(caller.finish());
    CHECK(caller.checkedResult().has_value()); // expected cancellation, not Ready
    CHECK_FALSE(daemon.initWaiterThread_.joinable());
    CHECK_FALSE(daemon.initHandled_.load());
}

void checkOutcomeAdmission(bool useTestHelper, bool readyWins) {
    CAPTURE(useTestHelper, readyWins);
    // Gates outlive the fixture, including its destructor's final stop().
    OutcomeGate loop;
    OutcomeGate outcome;
    std::promise<void> shutdownAdmitted;
    auto shutdownSeen = shutdownAdmitted.get_future();
    IsolatedDaemon fixture;
    auto& daemon = fixture.daemon();
    REQUIRE(daemon.testingSetBeforeInitOutcomeObserver([&] { outcome.arriveAndWait(); }));
    if (useTestHelper) {
        REQUIRE(
            daemon.testingSetAfterShutdownAdmissionObserver([&] { shutdownAdmitted.set_value(); }));
    } else {
        daemon.signalCheckHook_ = [&] {
            loop.arriveAndWait();
            return false;
        };
    }
    requireStarted(daemon);
    ParkedEntry caller(daemon, useTestHelper);
    // Declared AFTER caller: unblock BOTH gates before its destructor joins.
    ReleaseOutcomeGates releaseOnExit{loop, outcome};
    caller.begin();
    REQUIRE(outcome.awaitArrival());
    if (useTestHelper) {
        REQUIRE_NOTHROW(caller.finish()); // waiter member publication is complete
    } else {
        REQUIRE(loop.awaitArrival()); // loop entered after member publication
    }
    const auto service = daemon.serviceManager_->waitForServiceManagerTerminalState(0);
    REQUIRE(service.state == yams::daemon::ServiceManagerState::Ready);
    if (readyWins) {
        outcome.release();
        REQUIRE(awaitLifecycleReady(daemon));
    }
    daemon.requestStop();
    if (useTestHelper) {
        auto stopping = std::async(std::launch::async, [&] { return daemon.stop(); });
        // Also precedes destruction of the async future, which may wait for stop.
        ReleaseOutcomeGates releaseBeforeStopJoin{loop, outcome};
        REQUIRE(shutdownSeen.wait_for(5s) == std::future_status::ready);
        if (!readyWins) {
            const auto closingState = daemon.getLifecycle().snapshot().state;
            std::cerr << "[outcome-order] helper lifecycle-before-release="
                      << static_cast<int>(closingState)
                      << " stop-requested=" << daemon.isStopRequested() << '\n';
            REQUIRE(closingState == yams::daemon::LifecycleState::Stopping);
        }
        outcome.release();
        auto stopped = stopping.get();
        INFO((stopped ? "daemon stopped" : stopped.error().message));
        REQUIRE(stopped.has_value());
    } else {
        loop.release();
        REQUIRE_NOTHROW(caller.finish());
        const auto closingState = daemon.getLifecycle().snapshot().state;
        std::cerr << "[outcome-order] normal lifecycle-before-release="
                  << static_cast<int>(closingState)
                  << " stop-requested=" << daemon.isStopRequested() << '\n';
        REQUIRE(closingState == yams::daemon::LifecycleState::Stopping);
        outcome.release();
        requireStopped(daemon);
    }
    CHECK(daemon.waitForStopCompletion(0ms));
    CHECK_FALSE(daemon.initWaiterThread_.joinable());
    CHECK(daemon.initHandled_.load(std::memory_order_acquire) == readyWins);
}
} // namespace

TEST_CASE("Normal denied waiter admission defers and retries on a later loop tick",
          "[daemon][startup-ownership][waiter-admission][admission-denied-normal]") {
    std::atomic<bool> permit{false};
    std::atomic<int> samples{0};
    int ticks = 0; // loop thread only; read after the caller joins
    IsolatedDaemon fixture;
    auto& daemon = fixture.daemon();
    REQUIRE(daemon.testingSetAdditionalInitWaiterAdmission([&] {
        samples.fetch_add(1);
        return permit.load();
    }));
    // Runs on the loop thread, which is the only writer of the waiter handle here.
    daemon.signalCheckHook_ = [&] {
        ++ticks;
        if (daemon.initWaiterThread_.joinable() || ticks > 40) {
            daemon.requestStop();
        } else if (ticks == 2) {
            permit.store(true); // denied for a full tick first: the loop keeps serving
        }
        return false;
    };
    requireStarted(daemon);
    ParkedEntry caller(daemon, false, true);
    caller.begin();
    REQUIRE_NOTHROW(caller.finish());
    const auto& entry = caller.checkedResult();
    INFO((entry ? "entry ok" : entry.error().message));
    CHECK(entry.has_value());
    CHECK(samples.load() == 3); // initial denial, denied retry, admitted retry
    CHECK(ticks == 3);
    CHECK(daemon.initWaiterThread_.joinable());
    requireStopped(daemon);
    CHECK_FALSE(daemon.initWaiterThread_.joinable());
}

TEST_CASE("Helper denied waiter admission is explicit and retryable",
          "[daemon][startup-ownership][waiter-admission][admission-denied-helper]") {
    checkDeniedAdmission(true);
}

TEST_CASE("Normal admitted waiter still registers and drains",
          "[daemon][startup-ownership][waiter-admission][admission-positive]") {
    checkAllowedAdmission(false);
}

TEST_CASE("Helper admitted waiter still registers and drains",
          "[daemon][startup-ownership][waiter-admission][admission-positive]") {
    checkAllowedAdmission(true);
}

TEST_CASE("Normal close wins over a parked admission decision",
          "[daemon][startup-ownership][waiter-admission][admission-cancel-normal]") {
    checkAdmissionCancelled(false);
}

TEST_CASE("Helper close wins over a parked admission decision",
          "[daemon][startup-ownership][waiter-admission][admission-cancel-helper]") {
    checkAdmissionCancelled(true);
}

TEST_CASE("An existing waiter is not resampled as new work",
          "[daemon][startup-ownership][waiter-admission][admission-existing]") {
    std::atomic<bool> permit{true};
    std::atomic<int> samples{0};
    OutcomeGate outcome;
    IsolatedDaemon fixture;
    auto& daemon = fixture.daemon();
    REQUIRE(daemon.testingSetAdditionalInitWaiterAdmission([&] {
        samples.fetch_add(1);
        return permit.load();
    }));
    REQUIRE(daemon.testingSetBeforeInitOutcomeObserver([&] { outcome.arriveAndWait(); }));
    requireStarted(daemon);
    ReleasePublicationGate releaseBeforeFixtureStop{outcome};
    auto created = daemon.testingStartAsyncInitWithoutRunLoopChecked();
    REQUIRE(created.has_value());
    REQUIRE(outcome.awaitArrival());
    const auto owner = daemon.initWaiterThread_.get_id();
    permit.store(false);
    auto existing = daemon.testingStartAsyncInitWithoutRunLoopChecked();
    CHECK(existing.has_value());
    CHECK(samples.load() == 1);
    CHECK(daemon.initWaiterThread_.get_id() == owner);
    outcome.release();
    requireStopped(daemon);
}

TEST_CASE("A waiter rejects synchronous self-stop before changing shutdown ownership",
          "[daemon][startup-ownership][waiter-admission][admission-self-stop]") {
    OutcomeGate beforeStop;
    OutcomeGate afterStop;
    std::promise<yams::Result<void>> attempted;
    auto attempt = attempted.get_future();
    IsolatedDaemon fixture;
    auto& daemon = fixture.daemon();
    REQUIRE(daemon.testingSetBeforeInitOutcomeObserver([&] {
        beforeStop.arriveAndWait();
        attempted.set_value(daemon.stop());
        afterStop.arriveAndWait();
    }));
    requireStarted(daemon);
    ParkedEntry caller(daemon, true, true);
    ReleaseOutcomeGates releaseBeforeCallerJoin{beforeStop, afterStop};
    caller.begin();
    REQUIRE(beforeStop.awaitArrival());
    REQUIRE_NOTHROW(caller.finish()); // member publication is complete
    REQUIRE(caller.checkedResult().has_value());
    const auto before = daemon.getLifecycle().snapshot().state;
    beforeStop.release();
    REQUIRE(attempt.wait_for(5s) == std::future_status::ready);
    auto rejected = attempt.get();
    REQUIRE_FALSE(rejected.has_value());
    CHECK(rejected.error().code == yams::ErrorCode::ResourceBusy);
    REQUIRE(afterStop.awaitArrival());
    CHECK(daemon.isRunning());
    CHECK_FALSE(daemon.isStopRequested());
    CHECK(daemon.getLifecycle().snapshot().state == before);
    afterStop.release();
    requireStopped(daemon);
}

TEST_CASE("Normal waiter publication cannot follow a completed stop",
          "[daemon][startup-ownership][publication-close-normal]") {
    checkPublicationAfterClose(false);
}

TEST_CASE("Helper waiter publication cannot follow a completed stop",
          "[daemon][startup-ownership][publication-close-helper]") {
    checkPublicationAfterClose(true);
}

TEST_CASE("Normal readiness can commit before lifecycle close",
          "[daemon][startup-ownership][outcome-ready-normal]") {
    checkOutcomeAdmission(false, true);
}

TEST_CASE("Helper readiness can commit before lifecycle close",
          "[daemon][startup-ownership][outcome-ready-helper]") {
    checkOutcomeAdmission(true, true);
}

TEST_CASE("Normal late readiness cannot commit after actual lifecycle close",
          "[daemon][startup-ownership][outcome-close-normal]") {
    checkOutcomeAdmission(false, false);
}

TEST_CASE("Helper late readiness cannot commit after actual lifecycle close",
          "[daemon][startup-ownership][outcome-close-helper]") {
    checkOutcomeAdmission(true, false);
}

TEST_CASE("Initialization readiness rejection leaves lifecycle and provider bookkeeping unchanged",
          "[daemon][startup-ownership][readiness-admission]") {
    using namespace yams::daemon;
    const auto closingState = GENERATE(LifecycleState::Initializing, LifecycleState::Stopping,
                                       LifecycleState::Stopped, LifecycleState::Failed);
    const bool providerUnavailable = GENERATE(false, true);
    CAPTURE(closingState, providerUnavailable);
    DaemonLifecycleFsm fsm;
    fsm.dispatch(BootstrappedEvent{});
    fsm.setSubsystemDegraded("model_provider", true, "preserve original reason");
    if (closingState == LifecycleState::Failed) {
        fsm.dispatch(FailureEvent{"preserve failure"});
    } else if (closingState != LifecycleState::Initializing) {
        fsm.dispatch(ShutdownRequestedEvent{});
        if (closingState == LifecycleState::Stopped) {
            fsm.dispatch(StoppedEvent{});
        }
    }
    const auto before = fsm.snapshot();
    const auto degraded = fsm.degradedSubsystems();
    std::atomic<bool> cancelled{closingState == LifecycleState::Initializing};
    auto committed = fsm.tryCommitInitializationReadiness(cancelled, providerUnavailable);
    REQUIRE(committed.has_value());
    CHECK_FALSE(committed.value());
    const auto after = fsm.snapshot();
    CHECK(after.state == before.state);
    CHECK(after.lastTransition == before.lastTransition);
    CHECK(after.lastError == before.lastError);
    CHECK(fsm.degradedSubsystems() == degraded);
    CHECK(fsm.degradationReason("model_provider") == "preserve original reason");
}

TEST_CASE("Initialization readiness commits the existing provider policy",
          "[daemon][startup-ownership][readiness-admission]") {
    using namespace yams::daemon;
    const bool providerUnavailable = GENERATE(false, true);
    const bool alreadyTransitioned = GENERATE(false, true);
    DaemonLifecycleFsm fsm;
    fsm.dispatch(BootstrappedEvent{});
    if (alreadyTransitioned) {
        if (providerUnavailable) {
            fsm.dispatch(HealthyEvent{});
        } else {
            fsm.dispatch(DegradedEvent{});
        }
    }
    fsm.setSubsystemDegraded("model_provider", true, "old reason");
    const std::atomic<bool> cancelled{false};
    auto committed = fsm.tryCommitInitializationReadiness(cancelled, providerUnavailable);
    REQUIRE(committed.has_value());
    REQUIRE(committed.value());
    CHECK(fsm.snapshot().state ==
          (providerUnavailable ? LifecycleState::Degraded : LifecycleState::Ready));
    CHECK(fsm.isSubsystemDegraded("model_provider") == providerUnavailable);
    if (providerUnavailable) {
        CHECK(fsm.degradationReason("model_provider") ==
              "Model provider unavailable; embeddings disabled until provider recovery");
    } else {
        CHECK(fsm.degradationReason("model_provider").empty());
    }
}

TEST_CASE("Unexpected initialization readiness is an explicit error without mutation",
          "[daemon][startup-ownership][readiness-admission]") {
    using namespace yams::daemon;
    const auto initialState =
        GENERATE(LifecycleState::Unknown, LifecycleState::Ready, LifecycleState::Degraded);
    DaemonLifecycleFsm fsm;
    if (initialState != LifecycleState::Unknown) {
        fsm.dispatch(BootstrappedEvent{});
        if (initialState == LifecycleState::Ready) {
            fsm.dispatch(HealthyEvent{});
        } else {
            fsm.dispatch(DegradedEvent{});
        }
    }
    const auto before = fsm.snapshot();
    const auto degraded = fsm.degradedSubsystems();
    const std::atomic<bool> cancelled{false};
    auto committed =
        fsm.tryCommitInitializationReadiness(cancelled, initialState == LifecycleState::Degraded);
    REQUIRE_FALSE(committed.has_value());
    CHECK(committed.error().code == yams::ErrorCode::InvalidState);
    CHECK(fsm.snapshot().state == before.state);
    CHECK(fsm.snapshot().lastTransition == before.lastTransition);
    CHECK(fsm.degradedSubsystems() == degraded);
}

TEST_CASE("A second closer waits for an in-flight close and retries it if it fails",
          "[daemon][startup-ownership][close-handoff]") {
    const bool firstCloseFails = GENERATE(false, true);
    CAPTURE(firstCloseFails);
    OutcomeGate join;
    std::atomic<int> joinAttempts{0};
    IsolatedDaemon fixture;
    auto& daemon = fixture.daemon();
    REQUIRE(daemon.testingSetBeforeInitWaiterJoinObserver([&] {
        if (joinAttempts.fetch_add(1) == 0) {
            join.arriveAndWait();
            if (firstCloseFails) {
                throw std::runtime_error("injected init waiter join failure");
            }
        }
    }));
    requireStarted(daemon);
    ReleasePublicationGate releaseBeforeFixtureStop{join};
    REQUIRE(daemon.testingStartAsyncInitWithoutRunLoopChecked().has_value());
    REQUIRE(daemon.initWaiterThread_.joinable());

    auto first = std::async(std::launch::async, [&] { return daemon.stop(); });
    REQUIRE(join.awaitArrival()); // first close now owns Closing
    auto second = std::async(std::launch::async, [&] {
        const auto began = std::chrono::steady_clock::now();
        auto result = daemon.stopOrAwaitClose(20s);
        return std::pair{result, std::chrono::steady_clock::now() - began};
    });
    ReleasePublicationGate releaseBeforeFutureJoin{join};
    // Bounded check that the second closer is parked on the in-flight close, not returned.
    CHECK(second.wait_for(100ms) == std::future_status::timeout);
    join.release();

    REQUIRE(first.wait_for(10s) == std::future_status::ready);
    auto firstResult = first.get();
    CHECK(firstResult.has_value() == !firstCloseFails);
    REQUIRE(second.wait_for(15s) == std::future_status::ready);
    auto [secondResult, waited] = second.get();
    INFO((secondResult ? "second closer ok" : secondResult.error().message));
    CHECK(secondResult.has_value());
    CHECK(waited < 10s); // a failed close wakes the waiter instead of running out its timeout
    CHECK(joinAttempts.load() == (firstCloseFails ? 2 : 1));
    CHECK(daemon.waitForStopCompletion(0ms));
    CHECK(daemon.getLifecycle().snapshot().state == yams::daemon::LifecycleState::Stopped);
    CHECK_FALSE(daemon.initWaiterThread_.joinable());
}
