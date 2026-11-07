#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/DaemonMetrics.h>

using yams::daemon::computePhysicalWalkTtl;
using yams::daemon::PssSampleState;
using yams::daemon::pssSampleTtl;
using yams::daemon::sampleProcessPssKb;
using namespace std::chrono_literals;

TEST_CASE("computePhysicalWalkTtl: zero last-duration returns 60s floor",
          "[unit][daemon][tuning][catch2]") {
    REQUIRE(computePhysicalWalkTtl(0) == 60'000u);
}

TEST_CASE("computePhysicalWalkTtl: short walk pinned at 60s floor",
          "[unit][daemon][tuning][catch2]") {
    REQUIRE(computePhysicalWalkTtl(200) == 60'000u);
    REQUIRE(computePhysicalWalkTtl(5'000) == 60'000u);
}

TEST_CASE("computePhysicalWalkTtl: 30s walk produces 5min TTL", "[unit][daemon][tuning][catch2]") {
    REQUIRE(computePhysicalWalkTtl(30'000) == 300'000u);
}

TEST_CASE("computePhysicalWalkTtl: 60s walk produces 10min TTL", "[unit][daemon][tuning][catch2]") {
    REQUIRE(computePhysicalWalkTtl(60'000) == 600'000u);
}

TEST_CASE("computePhysicalWalkTtl: 5min walk clamps at 30min ceiling",
          "[unit][daemon][tuning][catch2]") {
    REQUIRE(computePhysicalWalkTtl(300'000) == 1'800'000u);
}

TEST_CASE("computePhysicalWalkTtl: extremely slow walk still clamps at ceiling",
          "[unit][daemon][tuning][catch2]") {
    REQUIRE(computePhysicalWalkTtl(60'000'000) == 1'800'000u);
}

TEST_CASE("sampleProcessPssKb reads smaps_rollup at most once per TTL",
          "[unit][daemon][metrics][catch2]") {
    PssSampleState state;
    int reads = 0;
    auto reader = [&reads]() -> std::uint64_t {
        ++reads;
        return 1000ull * static_cast<std::uint64_t>(reads);
    };
    const auto t0 = std::chrono::steady_clock::time_point{} + 1h;

    CHECK(sampleProcessPssKb(state, t0, 5s, reader) == 1000u);
    CHECK(reads == 1);
    CHECK(sampleProcessPssKb(state, t0 + 100ms, 5s, reader) == 1000u); // cached
    CHECK(sampleProcessPssKb(state, t0 + 4999ms, 5s, reader) == 1000u);
    CHECK(reads == 1);
    CHECK(sampleProcessPssKb(state, t0 + 5s, 5s, reader) == 2000u); // TTL elapsed
    CHECK(reads == 2);
    CHECK(sampleProcessPssKb(state, t0 + 2s, 5s, reader) == 3000u); // clock went backwards
    CHECK(reads == 3);
}

TEST_CASE("pssSampleTtl is 5 s busy and 10 s idle", "[unit][daemon][metrics][catch2]") {
    CHECK(pssSampleTtl(false) == 5000ms);
    CHECK(pssSampleTtl(true) == 10000ms);
}
