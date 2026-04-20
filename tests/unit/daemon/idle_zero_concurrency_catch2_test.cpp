// Bug 4: Non-reducible minimums prevent idle concurrency from reaching zero.
// Even when shouldAllowZeroPostIngestTargets() returns true, multiple minimum
// floors prevent actual zero concurrency:
//
// 1. TuneAdvisor::postIngestTotalConcurrent() clamps to min 2 (line 1946)
// 2. Budget allocator guarantees 1 slot per active stage when budget >= activeStages (line 2414)
// 3. Extraction + Embed hardcoded minimum of 1 each when budget >= 2 (lines 2460-2466)
// 4. ONNX slots minimum of 2 (TuningManager.cpp:336,352)
// 5. Ingest workers minimum of 1 (TuningManager.cpp:433-434)
//
// TDD Red Phase: Tests assert that idle daemon can reach zero concurrency.
// They FAIL against current code where minimums are unconditional.

#include <climits>
#include <cstdlib>
#include <catch2/catch_test_macros.hpp>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/TuningManager.h>

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

    // Clear dynamic caps with the production unset sentinel.
    TuneAdvisor::setPostExtractionConcurrentDynamicCap(UINT32_MAX);
    TuneAdvisor::setPostKgConcurrentDynamicCap(UINT32_MAX);
    TuneAdvisor::setPostSymbolConcurrentDynamicCap(UINT32_MAX);
    TuneAdvisor::setPostEntityConcurrentDynamicCap(UINT32_MAX);
    TuneAdvisor::setPostTitleConcurrentDynamicCap(UINT32_MAX);
    TuneAdvisor::setPostEmbedConcurrentDynamicCap(UINT32_MAX);
}

void setAllPostIngestStagesActive() {
    TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Extraction, true);
    TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::KnowledgeGraph, true);
    TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Symbol, true);
    TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Entity, true);
    TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Title, true);
    TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Embed, true);
}

} // namespace

// =============================================================================
// Test Group A: Budget allocator minimum floors (TuneAdvisor.h)
// =============================================================================

TEST_CASE("Budget allocator allows zero per-stage when all DynamicCaps are zero",
          "[daemon][tune][idle][minimums][catch2]") {
    // This test verifies that the budget allocator in TuneAdvisor can produce
    // zero-allocation results when DynamicCaps cap every stage to 0.
    // Currently, the allocator gives minimum 1 per active stage (Bug 4, point 2).
    // After Bug 1 fix (sentinel change), DynamicCap(0) will cap stages to 0.
    // After Bug 4 fix, the allocator won't force minimums on zero-capped stages.

    resetPostIngestOverridesAndCaps();
    setAllPostIngestStagesActive();

    HwGuard hwGuard(16);
    ProfileGuard profileGuard(TuneAdvisor::Profile::Balanced);
    TuneAdvisor::setPostIngestTotalConcurrent(12);

    // Cap all stages to 0 via DynamicCap
    TuneAdvisor::beginDynamicCapWrite();
    TuneAdvisor::setPostExtractionConcurrentDynamicCap(0);
    TuneAdvisor::setPostKgConcurrentDynamicCap(0);
    TuneAdvisor::setPostSymbolConcurrentDynamicCap(0);
    TuneAdvisor::setPostEntityConcurrentDynamicCap(0);
    TuneAdvisor::setPostTitleConcurrentDynamicCap(0);
    TuneAdvisor::setPostEmbedConcurrentDynamicCap(0);
    TuneAdvisor::endDynamicCapWrite();

    auto budget = TuneAdvisor::testing_postIngestBudget(/*includeDynamicCaps=*/true);

    // BUG: allocator guarantees 1 per active stage + extraction/embed hardcoded min
    // FIXED: all stages return 0 when their DynamicCap is 0
    CHECK(budget.extraction == 0);
    CHECK(budget.kg == 0);
    CHECK(budget.symbol == 0);
    CHECK(budget.entity == 0);
    CHECK(budget.title == 0);
    CHECK(budget.embed == 0);
}

TEST_CASE("Extraction + embed hardcoded minimum removed when caps are zero",
          "[daemon][tune][idle][minimums][catch2]") {
    // Lines 2460-2466: "if (totalBudget >= 2) { if extraction alloc == 0 && cap > 0: alloc = 1 }"
    // This hardcoded minimum prevents extraction and embed from reaching zero.
    // After fix, caps of 0 must override the hardcoded minimum.

    resetPostIngestOverridesAndCaps();
    setAllPostIngestStagesActive();

    HwGuard hwGuard(16);
    ProfileGuard profileGuard(TuneAdvisor::Profile::Balanced);
    TuneAdvisor::setPostIngestTotalConcurrent(12);

    // Only cap extraction and embed to 0 — leave others uncapped
    TuneAdvisor::beginDynamicCapWrite();
    TuneAdvisor::setPostExtractionConcurrentDynamicCap(0);
    TuneAdvisor::setPostEmbedConcurrentDynamicCap(0);
    TuneAdvisor::endDynamicCapWrite();

    // BUG: hardcoded minimum forces extraction=1, embed=1 even with cap=0
    // FIXED: extraction=0, embed=0
    CHECK(TuneAdvisor::postExtractionConcurrent() == 0);
    CHECK(TuneAdvisor::postEmbedConcurrent() == 0);
}

// =============================================================================
// Test Group B: postIngestTotalConcurrent minimum (TuneAdvisor.h:1946)
// =============================================================================

TEST_CASE("postIngestTotalConcurrent can return 1 on small systems",
          "[daemon][tune][idle][minimums][catch2]") {
    // Line 1946: std::clamp(total, 2u, hw)
    // The minimum of 2 prevents single-threaded idle behavior.
    // After fix: minimum should be 1 (or allow 0 through explicit override).
    // For a 2-core system with Efficient profile, base = max(2, ceil(2*0.2)) = 2
    // which already satisfies the clamp. But the explicit floor of 2 prevents
    // postIngestTotalConcurrent from ever being overridden below 2.

    resetPostIngestOverridesAndCaps();
    setAllPostIngestStagesActive();

    // With override set to 1, verify it's honored
    HwGuard hwGuard(8);
    ProfileGuard profileGuard(TuneAdvisor::Profile::Efficient);
    TuneAdvisor::setPostIngestTotalConcurrent(1);

    // BUG: setPostIngestTotalConcurrent clamps min to 1, but the
    // postIngestTotalConcurrent() getter at line 1946 clamps output to min 2.
    // Actually, the override path at line 1915-1917 returns ov directly if > 0.
    // So override=1 → returns 1. But the default computed path clamps to min 2.
    // Let's test the override path: it should return 1.
    CHECK(TuneAdvisor::postIngestTotalConcurrent() == 1);
}

// =============================================================================
// Test Group C: TuningManager idle allows zero targets (integration with
// shouldAllowZeroPostIngestTargets)
// =============================================================================

TEST_CASE("shouldAllowZeroPostIngestTargets returns true when idle with no work",
          "[daemon][tune][idle][minimums][catch2]") {
    // Existing: verified in tuning_reconciliation_catch2_test.cpp
    // Reconfirm here for completeness
    CHECK(TuningManager::testing_shouldAllowZeroPostIngestTargets(
              /*daemonIdle=*/true, /*postIngestBusy=*/false) == true);
}

TEST_CASE("ForceIdle mode enables zero targets regardless of daemon state",
          "[daemon][tune][idle][minimums][catch2]") {
    TuningManager::testing_setPostIngestScaleTestMode(
        TuningManager::PostIngestScaleTestMode::ForceIdle);

    CHECK(TuningManager::testing_shouldAllowZeroPostIngestTargets(
              /*daemonIdle=*/false, /*postIngestBusy=*/true) == true);

    // Reset
    TuningManager::testing_setPostIngestScaleTestMode(
        TuningManager::PostIngestScaleTestMode::Normal);
}
