#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/DaemonMetrics.h>

using yams::daemon::computePhysicalWalkTtl;

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
