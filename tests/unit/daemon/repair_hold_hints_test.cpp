#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/ResourceGovernor.h>
#include <yams/daemon/components/TuningManager.h>

using yams::daemon::computeRepairHoldHints;
using yams::daemon::ResourcePressureLevel;

TEST_CASE("computeRepairHoldHints: Normal pressure + empty backlog returns baseline",
          "[unit][daemon][tuning][catch2]") {
    auto h = computeRepairHoldHints(ResourcePressureLevel::Normal, 0);
    REQUIRE(h.degradeHoldMs == 750u);
    REQUIRE(h.readyHoldMs == 1500u);
}

TEST_CASE("computeRepairHoldHints: Warning stretches degrade and shrinks ready",
          "[unit][daemon][tuning][catch2]") {
    auto h = computeRepairHoldHints(ResourcePressureLevel::Warning, 0);
    REQUIRE(h.degradeHoldMs == 1500u);
    REQUIRE(h.readyHoldMs == 1125u);
}

TEST_CASE("computeRepairHoldHints: Critical multiplies degrade x4 and halves ready",
          "[unit][daemon][tuning][catch2]") {
    auto h = computeRepairHoldHints(ResourcePressureLevel::Critical, 0);
    REQUIRE(h.degradeHoldMs == 3000u);
    REQUIRE(h.readyHoldMs == 750u);
}

TEST_CASE("computeRepairHoldHints: Emergency behaves like Critical or stronger",
          "[unit][daemon][tuning][catch2]") {
    auto h = computeRepairHoldHints(ResourcePressureLevel::Emergency, 0);
    REQUIRE(h.degradeHoldMs >= 3000u);
    REQUIRE(h.readyHoldMs <= 750u);
}

TEST_CASE("computeRepairHoldHints: backlog 100<=b<=1000 amplifies degrade by 1.25",
          "[unit][daemon][tuning][catch2]") {
    auto h = computeRepairHoldHints(ResourcePressureLevel::Normal, 500);
    REQUIRE(h.degradeHoldMs == static_cast<uint32_t>(750 * 1.25));
    REQUIRE(h.readyHoldMs == 1500u);
}

TEST_CASE("computeRepairHoldHints: backlog > 1000 amplifies degrade by 1.5",
          "[unit][daemon][tuning][catch2]") {
    auto h = computeRepairHoldHints(ResourcePressureLevel::Normal, 5000);
    REQUIRE(h.degradeHoldMs == static_cast<uint32_t>(750 * 1.5));
    REQUIRE(h.readyHoldMs == 1500u);
}

TEST_CASE("computeRepairHoldHints: pressure and backlog stack multiplicatively",
          "[unit][daemon][tuning][catch2]") {
    auto h = computeRepairHoldHints(ResourcePressureLevel::Critical, 5000);
    REQUIRE(h.degradeHoldMs == static_cast<uint32_t>(750 * 4.0 * 1.5));
    REQUIRE(h.readyHoldMs == 750u);
}

TEST_CASE("computeRepairHoldHints: custom baselines respected", "[unit][daemon][tuning][catch2]") {
    auto h = computeRepairHoldHints(ResourcePressureLevel::Warning, 0, 1000, 2000);
    REQUIRE(h.degradeHoldMs == 2000u);
    REQUIRE(h.readyHoldMs == 1500u);
}
