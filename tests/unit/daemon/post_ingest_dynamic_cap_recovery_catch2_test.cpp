// Regression: dynamic post-ingest caps must not poison defaults.
// If runtime caps are lowered temporarily, clearing them should allow effective
// concurrency to recover back to computed defaults.

#include <catch2/catch_test_macros.hpp>
#include <yams/daemon/components/ResourceGovernor.h>
#include <yams/daemon/components/TuneAdvisor.h>

using namespace yams::daemon;

namespace {

class ProfileGuard {
    TuneAdvisor::Profile prev_;

public:
    explicit ProfileGuard(TuneAdvisor::Profile profile) : prev_(TuneAdvisor::tuningProfile()) {
        TuneAdvisor::setTuningProfile(profile);
    }
    ~ProfileGuard() { TuneAdvisor::setTuningProfile(prev_); }
    ProfileGuard(const ProfileGuard&) = delete;
    ProfileGuard& operator=(const ProfileGuard&) = delete;
};

class HwGuard {
    unsigned prev_;

public:
    explicit HwGuard(unsigned hw) : prev_(TuneAdvisor::hardwareConcurrency()) {
        TuneAdvisor::setHardwareConcurrencyForTests(hw);
    }
    ~HwGuard() { TuneAdvisor::setHardwareConcurrencyForTests(prev_); }
    HwGuard(const HwGuard&) = delete;
    HwGuard& operator=(const HwGuard&) = delete;
};

void resetPostIngestOverridesAndCaps() {
    TuneAdvisor::setPostExtractionConcurrent(0);
    TuneAdvisor::setPostKgConcurrent(0);
    TuneAdvisor::setPostSymbolConcurrent(0);
    TuneAdvisor::setPostEntityConcurrent(0);
    TuneAdvisor::setPostTitleConcurrent(0);
    TuneAdvisor::setPostEmbedConcurrent(0);

    TuneAdvisor::setPostExtractionConcurrentDynamicCap(0);
    TuneAdvisor::setPostKgConcurrentDynamicCap(0);
    TuneAdvisor::setPostSymbolConcurrentDynamicCap(0);
    TuneAdvisor::setPostEntityConcurrentDynamicCap(0);
    TuneAdvisor::setPostTitleConcurrentDynamicCap(0);
    TuneAdvisor::setPostEmbedConcurrentDynamicCap(0);
}

} // namespace

TEST_CASE("Post-ingest dynamic caps do not poison defaults",
          "[daemon][tune][post-ingest][dynamic-cap][catch2]") {
    resetPostIngestOverridesAndCaps();

    HwGuard hwGuard(16);
    ProfileGuard profileGuard(TuneAdvisor::Profile::Balanced);

    const auto defaultExtract = TuneAdvisor::postExtractionDefaultConcurrent();
    const auto defaultKg = TuneAdvisor::postKgDefaultConcurrent();
    const auto defaultEmbed = TuneAdvisor::postEmbedDefaultConcurrent();

    REQUIRE(defaultExtract >= 1);
    REQUIRE(defaultKg >= 1);
    REQUIRE(defaultEmbed >= 1);

    // Apply strict caps (lower than defaults).
    TuneAdvisor::setPostExtractionConcurrentDynamicCap(1);
    TuneAdvisor::setPostKgConcurrentDynamicCap(1);
    TuneAdvisor::setPostEmbedConcurrentDynamicCap(1);

    CHECK(TuneAdvisor::postExtractionConcurrent() <= defaultExtract);
    CHECK(TuneAdvisor::postKgConcurrent() <= defaultKg);
    CHECK(TuneAdvisor::postEmbedConcurrent() <= defaultEmbed);

    // Defaults must remain unchanged.
    CHECK(TuneAdvisor::postExtractionDefaultConcurrent() == defaultExtract);
    CHECK(TuneAdvisor::postKgDefaultConcurrent() == defaultKg);
    CHECK(TuneAdvisor::postEmbedDefaultConcurrent() == defaultEmbed);

    // Governor caps at Normal must use defaults (not effective runtime caps).
    auto& governor = ResourceGovernor::instance();
    governor.testing_updateScalingCaps(ResourcePressureLevel::Normal);
    auto caps = governor.getScalingCaps();
    CHECK(caps.extractionConcurrency == defaultExtract);
    CHECK(caps.kgConcurrency == defaultKg);
    CHECK(caps.embedConcurrency == defaultEmbed);

    // Clearing caps must restore effective to defaults.
    TuneAdvisor::setPostExtractionConcurrentDynamicCap(0);
    TuneAdvisor::setPostKgConcurrentDynamicCap(0);
    TuneAdvisor::setPostEmbedConcurrentDynamicCap(0);

    CHECK(TuneAdvisor::postExtractionConcurrent() == defaultExtract);
    CHECK(TuneAdvisor::postKgConcurrent() == defaultKg);
    CHECK(TuneAdvisor::postEmbedConcurrent() == defaultEmbed);
}
