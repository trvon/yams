// Host pressure signal: PSI/loadavg parsing, busy classification with a release band, and the
// ResourceGovernor wiring that samples it on the governor tick.

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/HostPressure.h>
#include <yams/daemon/components/ResourceGovernor.h>
#include <yams/daemon/components/TuneAdvisor.h>

#include <chrono>
#include <functional>

using namespace yams::daemon;

namespace {

constexpr std::string_view kPsiCalm = "some avg10=1.85 avg60=2.69 avg300=4.98 total=27678621495\n"
                                      "full avg10=0.00 avg60=0.00 avg300=0.00 total=0\n";

HostPressureSample psiSample(double cpu, double io, double memory) {
    HostPressureSample sample;
    sample.source = HostPressureSource::Psi;
    sample.cpuSomeAvg10 = cpu;
    sample.ioSomeAvg10 = io;
    sample.memorySomeAvg10 = memory;
    sample.loadPerCpu = 0.2;
    return sample;
}

/// Restores the governor's host sampler and the host-pressure tuning overrides.
struct HostSamplerGuard {
    HostSamplerGuard() = default;
    ~HostSamplerGuard() {
        ResourceGovernor::instance().testing_setHostPressureSampler(nullptr);
        TuneAdvisor::setHostPressureEnabled(true);
        TuneAdvisor::setHostCpuPressurePct(0.0);
        TuneAdvisor::setHostIoPressurePct(0.0);
        TuneAdvisor::setHostMemoryPressurePct(0.0);
        TuneAdvisor::setHostLoadPerCpu(0.0);
        TuneAdvisor::resetBackgroundMaxDeferralMs();
        ResourceGovernor::instance().tick(nullptr);
        ResourceGovernor::instance().testing_resetDeferralGates();
    }
    HostSamplerGuard(const HostSamplerGuard&) = delete;
    HostSamplerGuard& operator=(const HostSamplerGuard&) = delete;
};

} // namespace

TEST_CASE("parsePsiSomeAvg10 reads the some line", "[daemon][governance][host_pressure][catch2]") {
    auto value = parsePsiSomeAvg10(kPsiCalm);
    REQUIRE(value.has_value());
    CHECK(*value == Catch::Approx(1.85));

    // Only the "some" line counts, even when "full" comes first.
    auto reordered = parsePsiSomeAvg10("full avg10=9.00 avg60=0 avg300=0 total=0\n"
                                       "some avg10=63.10 avg60=20.00 avg300=5.00 total=1\n");
    REQUIRE(reordered.has_value());
    CHECK(*reordered == Catch::Approx(63.10));

    CHECK_FALSE(parsePsiSomeAvg10("").has_value());
    CHECK_FALSE(parsePsiSomeAvg10("garbage\n").has_value());
    CHECK_FALSE(parsePsiSomeAvg10("some avg60=1.0 total=0\n").has_value());
}

TEST_CASE("parseLoadAvg1 reads the one-minute average",
          "[daemon][governance][host_pressure][catch2]") {
    auto value = parseLoadAvg1("11.14 11.26 10.94 3/2154 548111\n");
    REQUIRE(value.has_value());
    CHECK(*value == Catch::Approx(11.14));
    CHECK_FALSE(parseLoadAvg1("").has_value());
    CHECK_FALSE(parseLoadAvg1("load\n").has_value());
}

TEST_CASE("hostPressureElevated applies thresholds per signal",
          "[daemon][governance][host_pressure][catch2]") {
    const HostPressureThresholds thresholds{};

    CHECK_FALSE(hostPressureElevated(psiSample(2.0, 0.0, 0.0), thresholds, false));
    CHECK(hostPressureElevated(psiSample(thresholds.cpuSomePct, 0.0, 0.0), thresholds, false));
    CHECK(hostPressureElevated(psiSample(0.0, thresholds.ioSomePct + 1.0, 0.0), thresholds, false));
    CHECK(hostPressureElevated(psiSample(0.0, 0.0, thresholds.memorySomePct + 1.0), thresholds,
                               false));

    SECTION("PSI samples ignore the load average") {
        auto sample = psiSample(0.0, 0.0, 0.0);
        sample.loadPerCpu = thresholds.loadPerCpu * 4.0;
        CHECK_FALSE(hostPressureElevated(sample, thresholds, false));
    }

    SECTION("load-average samples use the per-CPU load") {
        HostPressureSample sample;
        sample.source = HostPressureSource::LoadAverage;
        sample.loadPerCpu = thresholds.loadPerCpu * 0.5;
        CHECK_FALSE(hostPressureElevated(sample, thresholds, false));
        sample.loadPerCpu = thresholds.loadPerCpu;
        CHECK(hostPressureElevated(sample, thresholds, false));
        sample.loadPerCpu = 0.0;
        sample.vmPressureElevated = true;
        CHECK(hostPressureElevated(sample, thresholds, false));
    }

    SECTION("an unavailable signal never reports pressure") {
        HostPressureSample sample;
        sample.cpuSomeAvg10 = 100.0;
        sample.loadPerCpu = 100.0;
        CHECK_FALSE(hostPressureElevated(sample, thresholds, false));
        CHECK_FALSE(hostPressureElevated(sample, thresholds, true));
    }
}

TEST_CASE("hostPressureElevated holds until the signal clears the release band",
          "[daemon][governance][host_pressure][catch2]") {
    const HostPressureThresholds thresholds{};
    const double justBelow = thresholds.cpuSomePct - 1.0;
    const double belowBand = thresholds.cpuSomePct * thresholds.releaseRatio - 1.0;

    // Entering needs the full threshold.
    CHECK_FALSE(hostPressureElevated(psiSample(justBelow, 0.0, 0.0), thresholds, false));
    // Once elevated, dipping just under the threshold is not enough to clear.
    CHECK(hostPressureElevated(psiSample(justBelow, 0.0, 0.0), thresholds, true));
    // Dropping below threshold * releaseRatio clears.
    CHECK_FALSE(hostPressureElevated(psiSample(belowBand, 0.0, 0.0), thresholds, true));
}

TEST_CASE("sampleHostPressure reports a usable signal on supported platforms",
          "[daemon][governance][host_pressure][catch2]") {
    const auto sample = sampleHostPressure();
#if defined(__linux__) || defined(__APPLE__)
    REQUIRE(sample.source != HostPressureSource::Unavailable);
    if (sample.source == HostPressureSource::Psi) {
        CHECK(sample.cpuSomeAvg10 >= 0.0);
        CHECK(sample.ioSomeAvg10 >= 0.0);
        CHECK(sample.memorySomeAvg10 >= 0.0);
    }
    CHECK(sample.loadPerCpu >= 0.0);
#else
    CHECK(sample.source == HostPressureSource::Unavailable);
#endif
}

TEST_CASE("ResourceGovernor samples host pressure on tick",
          "[daemon][governance][host_pressure][catch2]") {
    HostSamplerGuard guard;
    auto& governor = ResourceGovernor::instance();

    governor.testing_setHostPressureSampler([] { return psiSample(95.0, 0.0, 0.0); });
    auto busy = governor.tick(nullptr);
    CHECK(busy.host.source == HostPressureSource::Psi);
    CHECK(busy.host.cpuSomeAvg10 == Catch::Approx(95.0));
    CHECK(busy.hostPressureElevated);
    CHECK(governor.hostPressureElevated());
    CHECK(governor.getSnapshot().hostPressureElevated);

    // Host load is a separate dimension: it must not throttle the daemon's foreground caps or
    // block model loads the way the daemon's own memory/CPU pressure does.
    CHECK(busy.level == ResourcePressureLevel::Normal);
    CHECK(governor.getScalingCaps().allowModelLoads);

    governor.testing_setHostPressureSampler([] { return psiSample(1.0, 0.0, 0.0); });
    auto calm = governor.tick(nullptr);
    CHECK_FALSE(calm.hostPressureElevated);
    CHECK_FALSE(governor.hostPressureElevated());
}

TEST_CASE("ResourceGovernor host thresholds come from typed tuning",
          "[daemon][governance][host_pressure][catch2]") {
    HostSamplerGuard guard;
    auto& governor = ResourceGovernor::instance();

    governor.testing_setHostPressureSampler([] { return psiSample(20.0, 0.0, 0.0); });
    CHECK_FALSE(governor.tick(nullptr).hostPressureElevated);

    TuneAdvisor::setHostCpuPressurePct(15.0);
    CHECK(TuneAdvisor::hostPressureThresholds().cpuSomePct == Catch::Approx(15.0));
    governor.testing_setHostPressureSampler([] { return psiSample(20.0, 0.0, 0.0); });
    CHECK(governor.tick(nullptr).hostPressureElevated);
}

TEST_CASE("ResourceGovernor host sampling can be disabled",
          "[daemon][governance][host_pressure][catch2]") {
    HostSamplerGuard guard;
    auto& governor = ResourceGovernor::instance();

    TuneAdvisor::setHostPressureEnabled(false);
    governor.testing_setHostPressureSampler([] { return psiSample(95.0, 95.0, 95.0); });
    auto snap = governor.tick(nullptr);
    CHECK(snap.host.source == HostPressureSource::Unavailable);
    CHECK_FALSE(snap.hostPressureElevated);
    CHECK_FALSE(governor.hostPressureElevated());
}

// =============================================================================
// Deferrable background work
// =============================================================================

TEST_CASE("DeferralGate admits immediately on a calm host",
          "[daemon][governance][host_pressure][deferral][catch2]") {
    DeferralGate gate;
    const auto t0 = DeferralGate::Clock::now();
    CHECK(gate.admit(false, t0, std::chrono::minutes(5)));
    CHECK_FALSE(gate.deferring());
    CHECK(gate.deferrals() == 0);
}

TEST_CASE("DeferralGate holds work while the host is busy and releases when it calms",
          "[daemon][governance][host_pressure][deferral][catch2]") {
    DeferralGate gate;
    const auto t0 = DeferralGate::Clock::now();
    const auto maxDeferral = std::chrono::minutes(5);

    CHECK_FALSE(gate.admit(true, t0, maxDeferral));
    CHECK(gate.deferring());
    REQUIRE(gate.deferredSince().has_value());
    CHECK(*gate.deferredSince() == t0);
    CHECK_FALSE(gate.admit(true, t0 + std::chrono::seconds(30), maxDeferral));
    CHECK(gate.deferrals() == 2);

    CHECK(gate.admit(false, t0 + std::chrono::seconds(31), maxDeferral));
    CHECK_FALSE(gate.deferring());
    CHECK(gate.forcedRuns() == 0);
}

TEST_CASE("DeferralGate starvation guard forces one run per max deferral",
          "[daemon][governance][host_pressure][deferral][catch2]") {
    DeferralGate gate;
    const auto t0 = DeferralGate::Clock::now();
    const auto maxDeferral = std::chrono::seconds(60);

    CHECK_FALSE(gate.admit(true, t0, maxDeferral));
    CHECK_FALSE(gate.admit(true, t0 + std::chrono::seconds(59), maxDeferral));

    // Waited the full budget: one run goes through even though the host is still busy.
    CHECK(gate.admit(true, t0 + std::chrono::seconds(60), maxDeferral));
    CHECK(gate.forcedRuns() == 1);
    // The wait starts over, so sustained pressure yields a low, bounded rate.
    CHECK(gate.deferring());
    CHECK_FALSE(gate.admit(true, t0 + std::chrono::seconds(61), maxDeferral));
    CHECK_FALSE(gate.admit(true, t0 + std::chrono::seconds(119), maxDeferral));
    CHECK(gate.admit(true, t0 + std::chrono::seconds(120), maxDeferral));
    CHECK(gate.forcedRuns() == 2);
}

TEST_CASE("DeferralGate with a zero max deferral never defers",
          "[daemon][governance][host_pressure][deferral][catch2]") {
    DeferralGate gate;
    const auto t0 = DeferralGate::Clock::now();
    CHECK(gate.admit(true, t0, std::chrono::milliseconds(0)));
    CHECK_FALSE(gate.deferring());
    CHECK(gate.deferrals() == 0);
}

TEST_CASE("ResourceGovernor defers background work only while the host is busy",
          "[daemon][governance][host_pressure][deferral][catch2]") {
    HostSamplerGuard guard;
    auto& governor = ResourceGovernor::instance();
    TuneAdvisor::setBackgroundMaxDeferralMs(60'000);

    governor.testing_setHostPressureSampler([] { return psiSample(1.0, 0.0, 0.0); });
    governor.tick(nullptr);
    CHECK(governor.admitDeferrable(DeferrableWork::TopologyRebuild));
    CHECK(governor.deferredWork().empty());

    governor.testing_setHostPressureSampler([] { return psiSample(95.0, 0.0, 0.0); });
    governor.tick(nullptr);
    CHECK_FALSE(governor.admitDeferrable(DeferrableWork::TopologyRebuild));
    CHECK_FALSE(governor.admitDeferrable(DeferrableWork::RepairScan));
    // Each kind is tracked on its own, and status lists what is currently held back.
    const auto deferred = governor.deferredWork();
    REQUIRE(deferred.size() == 2);
    CHECK(deferred[0] == DeferrableWork::TopologyRebuild);
    CHECK(deferred[1] == DeferrableWork::RepairScan);
    // The next tick publishes the same set in the snapshot that status reads.
    CHECK(governor.tick(nullptr).deferredWorkMask == 0b11);

    governor.testing_setHostPressureSampler([] { return psiSample(1.0, 0.0, 0.0); });
    governor.tick(nullptr);
    CHECK(governor.admitDeferrable(DeferrableWork::TopologyRebuild));
    CHECK(governor.admitDeferrable(DeferrableWork::RepairScan));
    CHECK(governor.deferredWork().empty());
}

TEST_CASE("ResourceGovernor background deferral can be turned off with a zero max deferral",
          "[daemon][governance][host_pressure][deferral][catch2]") {
    HostSamplerGuard guard;
    auto& governor = ResourceGovernor::instance();
    TuneAdvisor::setBackgroundMaxDeferralMs(0);

    governor.testing_setHostPressureSampler([] { return psiSample(95.0, 0.0, 0.0); });
    governor.tick(nullptr);
    CHECK(governor.hostPressureElevated());
    CHECK(governor.admitDeferrable(DeferrableWork::SemanticBackfill));
    CHECK(governor.deferredWork().empty());
}
