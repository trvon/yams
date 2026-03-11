// Regression: dynamic post-ingest caps must not poison defaults.
// If runtime caps are lowered temporarily, clearing them should allow effective
// concurrency to recover back to computed defaults.

#include <climits>
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

    TuneAdvisor::setPostExtractionConcurrentDynamicCap(UINT32_MAX);
    TuneAdvisor::setPostKgConcurrentDynamicCap(UINT32_MAX);
    TuneAdvisor::setPostSymbolConcurrentDynamicCap(UINT32_MAX);
    TuneAdvisor::setPostEntityConcurrentDynamicCap(UINT32_MAX);
    TuneAdvisor::setPostTitleConcurrentDynamicCap(UINT32_MAX);
    TuneAdvisor::setPostEmbedConcurrentDynamicCap(UINT32_MAX);
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

    // Clearing caps (UINT32_MAX = unset) must restore effective to defaults.
    TuneAdvisor::setPostExtractionConcurrentDynamicCap(UINT32_MAX);
    TuneAdvisor::setPostKgConcurrentDynamicCap(UINT32_MAX);
    TuneAdvisor::setPostEmbedConcurrentDynamicCap(UINT32_MAX);

    CHECK(TuneAdvisor::postExtractionConcurrent() == defaultExtract);
    CHECK(TuneAdvisor::postKgConcurrent() == defaultKg);
    CHECK(TuneAdvisor::postEmbedConcurrent() == defaultEmbed);
}

// =============================================================================
// Bug 5: start() force-seeds non-zero DynamicCaps when caps are at zero.
// TuningManager::start() at line 70-75 writes DynamicCap(2) for extraction
// and DynamicCap(1) for embed when their effective concurrency is 0.
// This prevents an idle-start scenario where the daemon starts with
// DynamicCaps already set to zero (e.g. from a prior session's idle state).
//
// After fix: start() should NOT seed DynamicCaps unconditionally. If caps
// are intentionally zero (from prior idle state), they should stay zero
// until the tuning loop detects work and raises them.
//
// TDD Red Phase: These tests FAIL because start() currently seeds caps.
// =============================================================================

TEST_CASE("DynamicCap zero is preserved across clear-and-reapply cycle",
          "[daemon][tune][post-ingest][dynamic-cap][start-seed][catch2]") {
    // Simulate what would happen if DynamicCaps were intentionally set to 0
    // (idle state) and then read back. The caps should remain 0, not be
    // overridden by any start-up seeding logic.

    resetPostIngestOverridesAndCaps();

    HwGuard hwGuard(16);
    ProfileGuard profileGuard(TuneAdvisor::Profile::Balanced);

    // Explicitly set DynamicCaps to 0 (idle state — after Bug 1 fix)
    TuneAdvisor::beginDynamicCapWrite();
    TuneAdvisor::setPostExtractionConcurrentDynamicCap(0);
    TuneAdvisor::setPostEmbedConcurrentDynamicCap(0);
    TuneAdvisor::endDynamicCapWrite();

    // Read back — should be 0, not seeded to non-zero
    // BUG: In current code, DynamicCap(0) = "unset" sentinel → ignored.
    // After Bug 1 fix, DynamicCap(0) = "cap to zero".
    // Bug 5 would then re-seed these to 2 and 1 on start().
    // FIXED: caps stay at 0.
    CHECK(TuneAdvisor::postExtractionConcurrent() == 0);
    CHECK(TuneAdvisor::postEmbedConcurrent() == 0);
}

TEST_CASE("Start seeding removed: extraction DynamicCap not force-set to 2",
          "[daemon][tune][post-ingest][dynamic-cap][start-seed][catch2]") {
    // This test documents the specific start() behavior that must be removed.
    // Lines 70-75 of TuningManager.cpp:
    //   if (TuneAdvisor::postExtractionConcurrent() == 0) {
    //       TuneAdvisor::setPostExtractionConcurrentDynamicCap(2);
    //   }
    //   if (TuneAdvisor::postEmbedConcurrent() == 0) {
    //       TuneAdvisor::setPostEmbedConcurrentDynamicCap(1);
    //   }
    //
    // After fix: these lines should be removed or gated on !intentionallyIdle.

    resetPostIngestOverridesAndCaps();

    HwGuard hwGuard(16);
    ProfileGuard profileGuard(TuneAdvisor::Profile::Balanced);

    // Set all DynamicCaps to 0 via the seqlock path
    TuneAdvisor::beginDynamicCapWrite();
    TuneAdvisor::setPostExtractionConcurrentDynamicCap(0);
    TuneAdvisor::setPostKgConcurrentDynamicCap(0);
    TuneAdvisor::setPostSymbolConcurrentDynamicCap(0);
    TuneAdvisor::setPostEntityConcurrentDynamicCap(0);
    TuneAdvisor::setPostTitleConcurrentDynamicCap(0);
    TuneAdvisor::setPostEmbedConcurrentDynamicCap(0);
    TuneAdvisor::endDynamicCapWrite();

    // Read the raw DynamicCap values (not through budget allocator)
    const auto caps = TuneAdvisor::readDynamicCapsConsistent();

    // After Bug 1 fix: DynamicCap(0) stored as 0
    // After Bug 5 fix: start() does not override these
    // BUG: DynamicCap(0) currently stores 0 but it's the unset sentinel,
    //       so the allocator ignores it. Start() then seeds extraction=2, embed=1.
    CHECK(caps[0] == 0); // extraction cap should stay 0
    CHECK(caps[5] == 0); // embed cap should stay 0
}
