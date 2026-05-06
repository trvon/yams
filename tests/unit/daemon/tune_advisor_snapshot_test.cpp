#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/TuningSnapshot.h>

#include <memory>

using yams::daemon::TuneAdvisor;
using yams::daemon::TuningSnapshot;
using yams::daemon::TuningSnapshotRegistry;

namespace {

void clearSnapshot() {
    TuningSnapshotRegistry::instance().set(nullptr);
}

void publish(uint32_t degradeMs, uint32_t readyMs) {
    auto s = std::make_shared<TuningSnapshot>();
    s->repairDegradeHoldMs = degradeMs;
    s->repairReadyHoldMs = readyMs;
    TuningSnapshotRegistry::instance().set(std::move(s));
}

} // namespace

TEST_CASE("TuneAdvisor: empty snapshot returns baseline defaults",
          "[unit][daemon][tune_advisor][catch2]") {
    clearSnapshot();
    REQUIRE(TuneAdvisor::repairDegradeHoldMs() == 750u);
    REQUIRE(TuneAdvisor::repairReadyHoldMs() == 1500u);
}

TEST_CASE("TuneAdvisor: populated snapshot wins over baseline",
          "[unit][daemon][tune_advisor][catch2]") {
    publish(2200u, 600u);
    REQUIRE(TuneAdvisor::repairDegradeHoldMs() == 2200u);
    REQUIRE(TuneAdvisor::repairReadyHoldMs() == 600u);
    clearSnapshot();
}

TEST_CASE("TuneAdvisor: snapshot updates are visible immediately",
          "[unit][daemon][tune_advisor][catch2]") {
    publish(1000u, 1000u);
    REQUIRE(TuneAdvisor::repairDegradeHoldMs() == 1000u);
    publish(3000u, 500u);
    REQUIRE(TuneAdvisor::repairDegradeHoldMs() == 3000u);
    REQUIRE(TuneAdvisor::repairReadyHoldMs() == 500u);
    clearSnapshot();
}
