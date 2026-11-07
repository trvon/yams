#include <catch2/catch_test_macros.hpp>

// Private implementation seam: production and this fixture execute the same policy.
#include "../../../src/daemon/daemon_stop_state.h"

#include <chrono>
#include <future>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>

namespace {
using namespace std::chrono_literals;
using yams::daemon::LifecycleState;
namespace stop_detail = yams::daemon::detail;

// This owns only a pure test thread, never a daemon or its active dependencies.
struct PureWorker {
    std::promise<void>& exit;
    bool& released;
    std::thread thread;

    void release() {
        if (!released) {
            exit.set_value();
            released = true;
        }
    }
    ~PureWorker() {
        release();
        if (thread.joinable()) {
            thread.join();
        }
    }
};

class StopFixture {
public:
    StopFixture() {
        lifecycle_.reset();
        auto started = stop_detail::beginStart(state());
        if (!started) {
            throw std::runtime_error("Pure stop fixture initial start failed");
        }
        lifecycle_.dispatch(yams::daemon::BootstrappedEvent{});
        auto ready = ready_.get_future();
        worker_.thread = std::thread([payload = dependency_, exit = exitFuture_, ready = &ready_] {
            ready->set_value();
            exit.wait();
            if (*payload != 17) {
                std::terminate();
            }
        });
        if (ready.wait_for(5s) != std::future_status::ready) {
            // Fully constructed member RAII releases and joins even on constructor failure.
            throw std::runtime_error("Pure worker did not signal readiness");
        }
    }

    stop_detail::DaemonStopStateView state() {
        return {running_,      requested_, worker_.thread, completionMutex_,
                completionCv_, completed_, ownership_};
    }

    yams::Result<void> stop(bool injectFailure = false) {
        if (injectFailure) {
            failNextJoin_ = true;
        }
        return stop_detail::runStopFlow(
            state(), [this] { return lifecycle_.snapshot(); }, [] {},
            [this]() -> yams::Result<void> {
                stop_detail::signalStopRequest(state());
                stop_detail::requestLifecycleClose(lifecycle_);
                auto joined = stop_detail::joinInitWaiter(
                    state(),
                    [this] {
                        if (std::exchange(failNextJoin_, false)) {
                            throw std::runtime_error("injected pure-worker join failure");
                        }
                    },
                    [] {});
                if (!joined) {
                    return joined.error();
                }
                dependency_.reset();
                lifecycle_.dispatch(yams::daemon::StoppedEvent{});
                return yams::Result<void>();
            });
    }

    bool complete() {
        std::lock_guard<std::mutex> lock(completionMutex_);
        return completed_;
    }
    bool running() const { return running_.load(); }
    bool requested() const { return requested_.load(); }
    bool joinable() const { return worker_.thread.joinable(); }
    bool dependencyRetained() const { return static_cast<bool>(dependency_); }
    std::thread::id owner() const { return worker_.thread.get_id(); }
    LifecycleState lifecycleState() const { return lifecycle_.snapshot().state; }
    void releaseWorker() { worker_.release(); }

private:
    yams::daemon::DaemonLifecycleFsm lifecycle_;
    std::atomic<bool> running_{false};
    std::atomic<bool> requested_{false};
    std::mutex completionMutex_;
    std::condition_variable completionCv_;
    bool completed_{false};
    bool failNextJoin_{false};
    std::shared_ptr<int> dependency_{std::make_shared<int>(17)};
    std::promise<void> exit_;
    std::shared_future<void> exitFuture_{exit_.get_future().share()};
    std::promise<void> ready_;
    bool released_{false};
    yams::daemon::InitWaiterOwnership ownership_;
    // Last member: joins before captured promise/payload/storage destruction, including unwind.
    PureWorker worker_{exit_, released_, {}};
};

struct StopAttempt {
    std::optional<yams::Result<void>> result;
    std::string escaped;
};

StopAttempt captureStop(StopFixture& fixture, bool injectFailure) {
    StopAttempt attempt;
    try {
        attempt.result.emplace(fixture.stop(injectFailure));
    } catch (const std::exception& error) {
        attempt.escaped = error.what();
    }
    return attempt;
}

void checkCheckedFailure(const StopAttempt& attempt) {
    INFO(attempt.escaped);
    CHECK(attempt.escaped.empty());
    CHECK(attempt.result.has_value());
    if (attempt.result) {
        CHECK_FALSE(attempt.result->has_value());
        if (!attempt.result->has_value()) {
            CHECK(attempt.result->error().code == yams::ErrorCode::InternalError);
            CHECK(attempt.result->error().message.find("injected pure-worker join failure") !=
                  std::string::npos);
        }
    }
}
} // namespace

TEST_CASE("Join error retains actual shared-policy ownership and permits non-self retry",
          "[daemon][stop-state][stop-join-retention]") {
    StopFixture fixture;
    const auto owner = fixture.owner();
    const auto first = captureStop(fixture, true);
    checkCheckedFailure(first);
    CHECK_FALSE(fixture.complete());
    CHECK_FALSE(fixture.running());
    CHECK(fixture.requested());
    CHECK(fixture.joinable());
    CHECK(fixture.owner() == owner);
    CHECK(fixture.dependencyRetained());
    REQUIRE(fixture.lifecycleState() == LifecycleState::Stopping);

    // Do NOT call beginStart here: its baseline wrong-success would mask broken stop retry.
    fixture.releaseWorker();
    const auto retried = captureStop(fixture, false);
    REQUIRE(retried.escaped.empty());
    REQUIRE(retried.result.has_value());
    CHECK(retried.result->has_value());
    CHECK_FALSE(fixture.joinable());
    CHECK_FALSE(fixture.dependencyRetained());
    CHECK(fixture.lifecycleState() == LifecycleState::Stopped);
    CHECK(fixture.complete());
}

TEST_CASE("A failed owned join blocks start without mutating shared-policy state",
          "[daemon][stop-state][stop-join-start-busy]") {
    StopFixture fixture;
    const auto owner = fixture.owner();
    const auto first = captureStop(fixture, true);
    checkCheckedFailure(first);
    REQUIRE(fixture.joinable());
    REQUIRE(fixture.lifecycleState() == LifecycleState::Stopping);
    auto started = stop_detail::beginStart(fixture.state());
    CHECK_FALSE(started.has_value());
    if (!started) {
        CHECK(started.error().code == yams::ErrorCode::ResourceBusy);
    }
    CHECK_FALSE(fixture.running());
    CHECK_FALSE(fixture.complete());
    CHECK(fixture.owner() == owner);
    CHECK(fixture.dependencyRetained());
    // PureWorker RAII is independent of failed policy; no daemon dependency teardown occurs.
}

TEST_CASE("Normal shared-policy join closes and completes after dependency teardown",
          "[daemon][stop-state][stop-join-positive]") {
    StopFixture fixture;
    auto duplicate = stop_detail::beginStart(fixture.state());
    REQUIRE_FALSE(duplicate.has_value());
    CHECK(duplicate.error().code == yams::ErrorCode::InvalidState);
    fixture.releaseWorker();
    const auto stopped = captureStop(fixture, false);
    REQUIRE(stopped.escaped.empty());
    REQUIRE(stopped.result.has_value());
    CHECK(stopped.result->has_value());
    CHECK_FALSE(fixture.joinable());
    CHECK_FALSE(fixture.dependencyRetained());
    CHECK(fixture.lifecycleState() == LifecycleState::Stopped);
    CHECK(fixture.complete());
}
