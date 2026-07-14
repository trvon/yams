#include <catch2/catch_test_macros.hpp>

#include "src/search/search_component_fanout_internal.h"

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>

#include <atomic>
#include <chrono>
#include <map>
#include <string>
#include <thread>
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

    ComponentFanoutCollector makeCollector() {
        return ComponentFanoutCollector(config, trace,
                                        ComponentFanoutSinks{allComponentResults, componentTiming,
                                                             contributing, failed, timedOut,
                                                             timedOutQueries});
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

} // namespace

TEST_CASE("fanout collector appends results and marks contributing on success",
          "[search][fanout][catch2]") {
    FanoutFixture fixture;
    auto collector = fixture.makeCollector();

    auto future = scheduleComponent(1.0f, std::nullopt, []() -> Result<std::vector<ComponentResult>> {
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

TEST_CASE("fanout collector treats empty success as non-contributing",
          "[search][fanout][catch2]") {
    FanoutFixture fixture;
    auto collector = fixture.makeCollector();

    auto future = scheduleComponent(
        1.0f, std::nullopt,
        []() -> Result<std::vector<ComponentResult>> { return std::vector<ComponentResult>{}; });

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

    auto future = scheduleComponent(1.0f, std::nullopt,
                                    []() -> Result<std::vector<ComponentResult>> {
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

    boost::asio::io_context io;
    auto guard = boost::asio::make_work_guard(io);
    std::thread runner([&io]() { io.run(); });
    std::optional<boost::asio::any_io_executor> executor{io.get_executor()};

    auto future =
        scheduleComponent(1.0f, executor, []() -> Result<std::vector<ComponentResult>> {
            std::this_thread::sleep_for(std::chrono::milliseconds(150));
            return std::vector<ComponentResult>{};
        });

    collector.collectAndRecord(future, "vector", fixture.queryCount, fixture.avgTime);

    REQUIRE(fixture.timedOut == std::vector<std::string>{"vector"});
    CHECK(fixture.failed.empty());
    CHECK(fixture.timedOutQueries.load() == 1);

    future.wait();
    guard.reset();
    io.stop();
    runner.join();
}

TEST_CASE("scheduleComponent returns invalid future for zero weight and collect treats it as "
          "success",
          "[search][fanout][catch2]") {
    FanoutFixture fixture;
    auto collector = fixture.makeCollector();

    auto future = scheduleComponent(
        0.0f, std::nullopt,
        []() -> Result<std::vector<ComponentResult>> { return std::vector<ComponentResult>{}; });

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
    auto future = scheduleComponent(1.0f, std::nullopt,
                                    [&ran]() -> Result<std::vector<ComponentResult>> {
                                        ran = true;
                                        return std::vector<ComponentResult>{
                                            makeComponent("doc-sync", 0.5f)};
                                    });

    CHECK(ran);
    const auto status = collector.collect(future, "metadata", fixture.queryCount, fixture.avgTime);
    CHECK(status == ComponentStatus::Success);
    REQUIRE(fixture.allComponentResults.size() == 1);
    CHECK(fixture.allComponentResults.front().documentHash == "doc-sync");
}
