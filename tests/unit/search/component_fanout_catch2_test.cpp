#include <catch2/catch_test_macros.hpp>

#include "src/search/search_component_fanout_internal.h"

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using yams::Error;
using yams::ErrorCode;
using yams::Result;
using yams::search::ComponentResult;
using yams::search::SearchEngineConfig;
using yams::search::SearchTraceCollector;
using yams::search::detail::ComponentFanoutCollector;
using yams::search::detail::ComponentFanoutSinks;
using yams::search::detail::ComponentStatus;
using yams::search::detail::scheduleComponent;

namespace {

struct FanoutFixture {
    SearchEngineConfig config;
    SearchTraceCollector trace{config};
    std::vector<ComponentResult> allComponentResults;
    std::map<std::string, int64_t> componentTiming;
    std::vector<std::string> contributing;
    std::vector<std::string> failed;
    std::vector<std::string> timedOut;
    std::atomic<uint64_t> timedOutQueries{0};
    std::atomic<uint64_t> queryCount{0};
    std::atomic<uint64_t> avgTime{0};
    std::shared_ptr<std::atomic<bool>> cancellationSignal;
    std::shared_ptr<std::atomic<bool>> hostCancellationSignal;

    ComponentFanoutCollector makeCollector() {
        return ComponentFanoutCollector(config, trace,
                                        ComponentFanoutSinks{allComponentResults, componentTiming,
                                                             contributing, failed, timedOut,
                                                             timedOutQueries},
                                        cancellationSignal, hostCancellationSignal);
    }
};

ComponentResult makeComponent(std::string hash, float score) {
    ComponentResult component;
    component.documentHash = std::move(hash);
    component.filePath = component.documentHash;
    component.score = score;
    component.source = ComponentResult::Source::Text;
    return component;
}

constexpr auto kHangWatchdog = std::chrono::seconds(10);

class GatedWorkState {
public:
    void enterAndWaitForRelease() {
        std::unique_lock lock(mutex_);
        entered_ = true;
        condition_.notify_all();
        condition_.wait(lock, [this]() { return released_; });
    }

    bool waitForEntry(std::chrono::steady_clock::duration timeout) {
        std::unique_lock lock(mutex_);
        return condition_.wait_for(lock, timeout, [this]() { return entered_; });
    }

    void release() noexcept {
        {
            std::lock_guard lock(mutex_);
            released_ = true;
        }
        condition_.notify_all();
    }

private:
    std::mutex mutex_;
    std::condition_variable condition_;
    bool entered_{false};
    bool released_{false};
};

class GatedExecutor {
public:
    GatedExecutor()
        : state_(std::make_shared<GatedWorkState>()), workGuard_(boost::asio::make_work_guard(io_)),
          runner_([this]() { io_.run(); }) {}

    ~GatedExecutor() {
        state_->release();
        workGuard_.reset();
        io_.stop();
        if (runner_.joinable()) {
            runner_.join();
        }
    }

    GatedExecutor(const GatedExecutor&) = delete;
    GatedExecutor& operator=(const GatedExecutor&) = delete;

    std::shared_ptr<GatedWorkState> state() const { return state_; }

    std::optional<boost::asio::any_io_executor> executor() { return io_.get_executor(); }

private:
    std::shared_ptr<GatedWorkState> state_;
    boost::asio::io_context io_;
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type> workGuard_;
    std::thread runner_; // Started last: all teardown state is already fully constructed.
};

class ScopedGateRelease {
public:
    explicit ScopedGateRelease(std::shared_ptr<GatedWorkState> state) : state_(std::move(state)) {}
    ~ScopedGateRelease() { release(); }

    ScopedGateRelease(const ScopedGateRelease&) = delete;
    ScopedGateRelease& operator=(const ScopedGateRelease&) = delete;

    void release() noexcept {
        if (state_) {
            state_->release();
            state_.reset();
        }
    }

private:
    std::shared_ptr<GatedWorkState> state_;
};

ComponentFanoutCollector::Future scheduleGatedComponent(GatedExecutor& execution,
                                                        std::string hash) {
    auto state = execution.state();
    return scheduleComponent(1.0f, execution.executor(),
                             [state = std::move(state),
                              hash = std::move(hash)]() -> Result<std::vector<ComponentResult>> {
                                 state->enterAndWaitForRelease();
                                 return std::vector<ComponentResult>{makeComponent(hash, 0.75f)};
                             });
}

void releaseAndConsumeLateResult(ComponentFanoutCollector::Future& future,
                                 ScopedGateRelease& release, const std::string& expectedHash) {
    release.release();
    REQUIRE(future.wait_for(kHangWatchdog) == std::future_status::ready);
    auto result = future.get();
    REQUIRE(result);
    REQUIRE(result.value().size() == 1);
    CHECK(result.value().front().documentHash == expectedHash);
}

} // namespace

TEST_CASE("fanout collector appends results and marks contributing on success",
          "[search][fanout][catch2]") {
    FanoutFixture fixture;
    auto collector = fixture.makeCollector();

    auto future =
        scheduleComponent(1.0f, std::nullopt, []() -> Result<std::vector<ComponentResult>> {
            return std::vector<ComponentResult>{makeComponent("doc-a", 0.9f)};
        });

    collector.collectAndRecord(future, "text", fixture.queryCount, fixture.avgTime);

    REQUIRE(fixture.allComponentResults.size() == 1);
    CHECK(fixture.allComponentResults.front().documentHash == "doc-a");
    REQUIRE(fixture.contributing == std::vector<std::string>{"text"});
    CHECK(fixture.failed.empty());
    CHECK(fixture.timedOut.empty());
    CHECK(fixture.componentTiming.contains("text"));
    CHECK(fixture.queryCount.load() == 1);
}

TEST_CASE("fanout collector treats empty success as non-contributing", "[search][fanout][catch2]") {
    FanoutFixture fixture;
    auto collector = fixture.makeCollector();

    auto future =
        scheduleComponent(1.0f, std::nullopt, []() -> Result<std::vector<ComponentResult>> {
            return std::vector<ComponentResult>{};
        });

    collector.collectAndRecord(future, "tag", fixture.queryCount, fixture.avgTime);

    CHECK(fixture.allComponentResults.empty());
    CHECK(fixture.contributing.empty());
    CHECK(fixture.failed.empty());
    CHECK(fixture.queryCount.load() == 1);
}

TEST_CASE("fanout collector records failure without appending results",
          "[search][fanout][catch2]") {
    FanoutFixture fixture;
    auto collector = fixture.makeCollector();

    auto future =
        scheduleComponent(1.0f, std::nullopt, []() -> Result<std::vector<ComponentResult>> {
            return Error{ErrorCode::InternalError, "boom"};
        });

    collector.collectAndRecord(future, "kg", fixture.queryCount, fixture.avgTime);

    CHECK(fixture.allComponentResults.empty());
    CHECK(fixture.contributing.empty());
    REQUIRE(fixture.failed == std::vector<std::string>{"kg"});
    CHECK(fixture.timedOut.empty());
    CHECK(fixture.queryCount.load() == 0);
}

TEST_CASE("fanout collector records timeout and increments timed-out counter",
          "[search][fanout][catch2]") {
    FanoutFixture fixture;
    fixture.config.componentTimeout = std::chrono::milliseconds(1);
    auto collector = fixture.makeCollector();
    GatedExecutor execution;

    auto future = scheduleGatedComponent(execution, "doc-after-timeout");
    REQUIRE(execution.state()->waitForEntry(kHangWatchdog));

    auto collection = std::async(std::launch::async, [&]() {
        return collector.collectAndRecord(future, "vector", fixture.queryCount, fixture.avgTime);
    });
    // Declared after the async future so assertion unwinding releases the provider before a
    // future destructor can wait for collection to finish.
    ScopedGateRelease releaseBeforeFutureDestruction(execution.state());

    REQUIRE(collection.wait_for(kHangWatchdog) == std::future_status::ready);
    const auto status = collection.get();

    CHECK(status == ComponentStatus::TimedOut);
    REQUIRE(future.valid());
    CHECK(future.wait_for(std::chrono::milliseconds(0)) == std::future_status::timeout);
    CHECK(fixture.allComponentResults.empty());
    CHECK(fixture.contributing.empty());
    REQUIRE(fixture.timedOut == std::vector<std::string>{"vector"});
    CHECK(fixture.failed.empty());
    CHECK(fixture.componentTiming.empty());
    CHECK(fixture.timedOutQueries.load() == 1);
    CHECK(fixture.queryCount.load() == 0);
    CHECK(fixture.avgTime.load() == 0);

    releaseAndConsumeLateResult(future, releaseBeforeFutureDestruction, "doc-after-timeout");
    CHECK(fixture.allComponentResults.empty());
    CHECK(fixture.componentTiming.empty());
    CHECK(fixture.contributing.empty());
    CHECK(fixture.failed.empty());
    REQUIRE(fixture.timedOut == std::vector<std::string>{"vector"});
    CHECK(fixture.timedOutQueries.load() == 1);
    CHECK(fixture.queryCount.load() == 0);
    CHECK(fixture.avgTime.load() == 0);
}

TEST_CASE("fanout collector stops waiting when its request is canceled",
          "[search][fanout][cancel][catch2]") {
    FanoutFixture fixture;
    fixture.config.componentTimeout = std::chrono::seconds(30);
    fixture.cancellationSignal = std::make_shared<std::atomic<bool>>(false);
    auto collector = fixture.makeCollector();
    GatedExecutor execution;

    auto future = scheduleGatedComponent(execution, "doc-after-request-cancel");
    REQUIRE(execution.state()->waitForEntry(kHangWatchdog));

    std::promise<void> collectionLaunched;
    auto collectionLaunchedFuture = collectionLaunched.get_future();
    auto collection = std::async(std::launch::async, [&]() {
        collectionLaunched.set_value();
        return collector.collect(future, "vector", fixture.queryCount, fixture.avgTime);
    });
    ScopedGateRelease releaseBeforeFutureDestruction(execution.state());

    REQUIRE(collectionLaunchedFuture.wait_for(kHangWatchdog) == std::future_status::ready);
    // This handshake proves only that the async wrapper started, not that collect entered its wait
    // loop; there is no production seam for observing that point. The pending provider below is the
    // oracle that cancellation won over provider completion.
    CHECK(collection.wait_for(std::chrono::milliseconds(0)) == std::future_status::timeout);
    fixture.cancellationSignal->store(true, std::memory_order_release);

    REQUIRE(collection.wait_for(kHangWatchdog) == std::future_status::ready);
    const auto status = collection.get();

    CHECK(status == ComponentStatus::Canceled);
    REQUIRE(future.valid());
    CHECK(future.wait_for(std::chrono::milliseconds(0)) == std::future_status::timeout);
    CHECK(fixture.allComponentResults.empty());
    CHECK(fixture.componentTiming.empty());
    CHECK(fixture.contributing.empty());
    CHECK(fixture.failed.empty());
    CHECK(fixture.timedOut.empty());
    CHECK(fixture.timedOutQueries.load() == 0);
    CHECK(fixture.queryCount.load() == 0);
    CHECK(fixture.avgTime.load() == 0);

    releaseAndConsumeLateResult(future, releaseBeforeFutureDestruction, "doc-after-request-cancel");
    CHECK(fixture.allComponentResults.empty());
    CHECK(fixture.componentTiming.empty());
    CHECK(fixture.contributing.empty());
    CHECK(fixture.failed.empty());
    CHECK(fixture.timedOut.empty());
    CHECK(fixture.timedOutQueries.load() == 0);
    CHECK(fixture.queryCount.load() == 0);
    CHECK(fixture.avgTime.load() == 0);
}

TEST_CASE("fanout collector stops an unbounded wait when the host cancels",
          "[search][fanout][cancel][catch2]") {
    FanoutFixture fixture;
    // componentTimeout == 0 gives the collector no deadline. This collector-only test verifies
    // cancellation while provider work remains pending; it does not exercise public SearchEngine
    // worker draining or prove when that higher-level request releases its worker.
    fixture.config.componentTimeout = std::chrono::milliseconds(0);
    fixture.hostCancellationSignal = std::make_shared<std::atomic<bool>>(false);
    auto collector = fixture.makeCollector();
    GatedExecutor execution;

    auto future = scheduleGatedComponent(execution, "doc-after-host-cancel");
    REQUIRE(execution.state()->waitForEntry(kHangWatchdog));

    std::promise<void> collectionLaunched;
    auto collectionLaunchedFuture = collectionLaunched.get_future();
    auto collection = std::async(std::launch::async, [&]() {
        collectionLaunched.set_value();
        return collector.collect(future, "text", fixture.queryCount, fixture.avgTime);
    });
    ScopedGateRelease releaseBeforeFutureDestruction(execution.state());

    REQUIRE(collectionLaunchedFuture.wait_for(kHangWatchdog) == std::future_status::ready);
    // Wrapper launch is not proof that collect has entered its wait loop; no such observation seam
    // exists here. The request signal is nevertheless initially false and is set asynchronously.
    CHECK(collection.wait_for(std::chrono::milliseconds(0)) == std::future_status::timeout);
    fixture.hostCancellationSignal->store(true, std::memory_order_release);

    REQUIRE(collection.wait_for(kHangWatchdog) == std::future_status::ready);
    const auto status = collection.get();

    CHECK(status == ComponentStatus::Canceled);
    REQUIRE(future.valid());
    CHECK(future.wait_for(std::chrono::milliseconds(0)) == std::future_status::timeout);
    CHECK(fixture.allComponentResults.empty());
    CHECK(fixture.componentTiming.empty());
    CHECK(fixture.contributing.empty());
    CHECK(fixture.failed.empty());
    CHECK(fixture.timedOut.empty());
    CHECK(fixture.timedOutQueries.load() == 0);
    CHECK(fixture.queryCount.load() == 0);
    CHECK(fixture.avgTime.load() == 0);

    releaseAndConsumeLateResult(future, releaseBeforeFutureDestruction, "doc-after-host-cancel");
    CHECK(fixture.allComponentResults.empty());
    CHECK(fixture.componentTiming.empty());
    CHECK(fixture.contributing.empty());
    CHECK(fixture.failed.empty());
    CHECK(fixture.timedOut.empty());
    CHECK(fixture.timedOutQueries.load() == 0);
    CHECK(fixture.queryCount.load() == 0);
    CHECK(fixture.avgTime.load() == 0);
}

TEST_CASE("scheduleComponent returns invalid future for zero weight and collect treats it as "
          "success",
          "[search][fanout][catch2]") {
    FanoutFixture fixture;
    auto collector = fixture.makeCollector();

    auto future =
        scheduleComponent(0.0f, std::nullopt, []() -> Result<std::vector<ComponentResult>> {
            return std::vector<ComponentResult>{};
        });

    CHECK_FALSE(future.valid());
    const auto status = collector.collect(future, "path", fixture.queryCount, fixture.avgTime);
    CHECK(status == ComponentStatus::Success);
    CHECK(fixture.componentTiming.empty());
    CHECK(fixture.queryCount.load() == 0);
}

TEST_CASE("fanout collector runs synchronously without executor and waits indefinitely on zero "
          "timeout",
          "[search][fanout][catch2]") {
    FanoutFixture fixture;
    fixture.config.componentTimeout = std::chrono::milliseconds(0);
    auto collector = fixture.makeCollector();

    bool ran = false;
    auto future =
        scheduleComponent(1.0f, std::nullopt, [&ran]() -> Result<std::vector<ComponentResult>> {
            ran = true;
            return std::vector<ComponentResult>{makeComponent("doc-sync", 0.5f)};
        });

    CHECK(ran);
    const auto status = collector.collect(future, "metadata", fixture.queryCount, fixture.avgTime);
    CHECK(status == ComponentStatus::Success);
    REQUIRE(fixture.allComponentResults.size() == 1);
    CHECK(fixture.allComponentResults.front().documentHash == "doc-sync");
}
