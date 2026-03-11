// Bug 1: DynamicCap uses 0 as the "unset" sentinel, which makes it impossible to
// express zero concurrency through DynamicCaps. Writing DynamicCap(0) clears the cap
// instead of capping at zero. After fix, the sentinel must be UINT32_MAX and writing
// DynamicCap(0) must actually cap a stage to zero concurrency.
//
// TDD Red Phase: These tests assert the FIXED behavior. They FAIL against current code
// because current code treats 0 as "unset" (sentinel) rather than "cap to zero".

#include <climits>
#include <cstdlib>
#include <catch2/catch_test_macros.hpp>
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

/// Clear all per-stage overrides and dynamic caps to "unset".
/// After fix: unset sentinel is UINT32_MAX, not 0.
/// For now, write 0 to overrides (which still means "no override" on the override path)
/// and UINT32_MAX to dynamic caps (the new unset sentinel).
///
/// NOTE: In the current buggy code, writing UINT32_MAX to a DynamicCap setter will
/// be clamped to the per-stage max (e.g. 64 for extraction). After fix, UINT32_MAX
/// must be recognized as unset and NOT applied as a cap.
void resetPostIngestOverridesAndCaps() {
    TuneAdvisor::setPostExtractionConcurrent(0);
    TuneAdvisor::setPostKgConcurrent(0);
    TuneAdvisor::setPostSymbolConcurrent(0);
    TuneAdvisor::setPostEntityConcurrent(0);
    TuneAdvisor::setPostTitleConcurrent(0);
    TuneAdvisor::setPostEmbedConcurrent(0);

    // Clear dynamic caps — after fix, UINT32_MAX means "unset"
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
// Test 1: Writing DynamicCap(0) must cap a stage to zero, not clear the cap
// =============================================================================

TEST_CASE("DynamicCap(0) caps extraction to zero concurrency",
          "[daemon][tune][dynamic-cap][sentinel][catch2]") {
    resetPostIngestOverridesAndCaps();
    setAllPostIngestStagesActive();

    HwGuard hwGuard(16);
    ProfileGuard profileGuard(TuneAdvisor::Profile::Balanced);
    TuneAdvisor::setPostIngestTotalConcurrent(12);

    // Baseline: with caps unset (UINT32_MAX), extraction gets a non-zero default
    const auto baseline = TuneAdvisor::postExtractionConcurrent();
    REQUIRE(baseline >= 1);

    // Now cap extraction to 0 via DynamicCap
    TuneAdvisor::beginDynamicCapWrite();
    TuneAdvisor::setPostExtractionConcurrentDynamicCap(0);
    TuneAdvisor::endDynamicCapWrite();

    const auto capped = TuneAdvisor::postExtractionConcurrent();

    // BUG: current code treats DynamicCap(0) as "unset" → capped == baseline
    // FIXED: DynamicCap(0) means "zero concurrency" → capped == 0
    CHECK(capped == 0);
}

TEST_CASE("DynamicCap(0) caps embed to zero concurrency",
          "[daemon][tune][dynamic-cap][sentinel][catch2]") {
    resetPostIngestOverridesAndCaps();
    setAllPostIngestStagesActive();

    HwGuard hwGuard(16);
    ProfileGuard profileGuard(TuneAdvisor::Profile::Balanced);
    TuneAdvisor::setPostIngestTotalConcurrent(12);

    const auto baseline = TuneAdvisor::postEmbedConcurrent();
    REQUIRE(baseline >= 1);

    TuneAdvisor::beginDynamicCapWrite();
    TuneAdvisor::setPostEmbedConcurrentDynamicCap(0);
    TuneAdvisor::endDynamicCapWrite();

    const auto capped = TuneAdvisor::postEmbedConcurrent();
    CHECK(capped == 0);
}

TEST_CASE("DynamicCap(0) on all stages yields total concurrency of zero",
          "[daemon][tune][dynamic-cap][sentinel][catch2]") {
    resetPostIngestOverridesAndCaps();
    setAllPostIngestStagesActive();

    HwGuard hwGuard(16);
    ProfileGuard profileGuard(TuneAdvisor::Profile::Balanced);
    TuneAdvisor::setPostIngestTotalConcurrent(12);

    // Cap every stage to zero
    TuneAdvisor::beginDynamicCapWrite();
    TuneAdvisor::setPostExtractionConcurrentDynamicCap(0);
    TuneAdvisor::setPostKgConcurrentDynamicCap(0);
    TuneAdvisor::setPostSymbolConcurrentDynamicCap(0);
    TuneAdvisor::setPostEntityConcurrentDynamicCap(0);
    TuneAdvisor::setPostTitleConcurrentDynamicCap(0);
    TuneAdvisor::setPostEmbedConcurrentDynamicCap(0);
    TuneAdvisor::endDynamicCapWrite();

    const uint32_t total = TuneAdvisor::postExtractionConcurrent() +
                           TuneAdvisor::postKgConcurrent() + TuneAdvisor::postSymbolConcurrent() +
                           TuneAdvisor::postEntityConcurrent() +
                           TuneAdvisor::postTitleConcurrent() + TuneAdvisor::postEmbedConcurrent();

    // BUG: current code ignores DynamicCap(0) → total >= 6
    // FIXED: total == 0
    CHECK(total == 0);
}

// =============================================================================
// Test 2: UINT32_MAX is the "unset" sentinel — must not restrict concurrency
// =============================================================================

TEST_CASE("DynamicCap(UINT32_MAX) does not restrict concurrency (unset sentinel)",
          "[daemon][tune][dynamic-cap][sentinel][catch2]") {
    resetPostIngestOverridesAndCaps();
    setAllPostIngestStagesActive();

    HwGuard hwGuard(16);
    ProfileGuard profileGuard(TuneAdvisor::Profile::Balanced);
    TuneAdvisor::setPostIngestTotalConcurrent(12);

    // Capture defaults with no caps
    const auto defaultExtract = TuneAdvisor::postExtractionDefaultConcurrent();
    const auto defaultEmbed = TuneAdvisor::postEmbedDefaultConcurrent();

    // Write UINT32_MAX to all DynamicCaps — should be treated as "unset"
    TuneAdvisor::beginDynamicCapWrite();
    TuneAdvisor::setPostExtractionConcurrentDynamicCap(UINT32_MAX);
    TuneAdvisor::setPostKgConcurrentDynamicCap(UINT32_MAX);
    TuneAdvisor::setPostSymbolConcurrentDynamicCap(UINT32_MAX);
    TuneAdvisor::setPostEntityConcurrentDynamicCap(UINT32_MAX);
    TuneAdvisor::setPostTitleConcurrentDynamicCap(UINT32_MAX);
    TuneAdvisor::setPostEmbedConcurrentDynamicCap(UINT32_MAX);
    TuneAdvisor::endDynamicCapWrite();

    // Effective concurrency must equal defaults — UINT32_MAX must not act as a cap
    // BUG: current code clamps UINT32_MAX → 64 (kMaxCap), which then restricts.
    //       At 16 cores with budget 12, extraction default ~ 2. So 64 doesn't
    //       restrict much in practice — this test mainly documents the semantics.
    //       The real failure is the companion test above: DynamicCap(0) doesn't cap.
    CHECK(TuneAdvisor::postExtractionConcurrent() == defaultExtract);
    CHECK(TuneAdvisor::postEmbedConcurrent() == defaultEmbed);
}

// =============================================================================
// Test 3: Seqlock consistency with sentinel change
// =============================================================================

TEST_CASE("readDynamicCapsConsistent returns UINT32_MAX for unset caps",
          "[daemon][tune][dynamic-cap][sentinel][seqlock][catch2]") {
    resetPostIngestOverridesAndCaps();

    // After resetting caps to UINT32_MAX (unset), a consistent read should
    // return UINT32_MAX for each stage, not 0.
    const auto caps = TuneAdvisor::readDynamicCapsConsistent();
    for (std::size_t i = 0; i < 6; ++i) {
        // BUG: current code stores min(UINT32_MAX, maxCap) = maxCap, not UINT32_MAX
        // FIXED: UINT32_MAX stored and returned as-is (sentinel bypass in setter)
        CHECK(caps[i] == UINT32_MAX);
    }
}

TEST_CASE("readDynamicCapsConsistent returns 0 for zero caps",
          "[daemon][tune][dynamic-cap][sentinel][seqlock][catch2]") {
    resetPostIngestOverridesAndCaps();

    // Write zero caps to all stages
    TuneAdvisor::beginDynamicCapWrite();
    TuneAdvisor::setPostExtractionConcurrentDynamicCap(0);
    TuneAdvisor::setPostKgConcurrentDynamicCap(0);
    TuneAdvisor::setPostSymbolConcurrentDynamicCap(0);
    TuneAdvisor::setPostEntityConcurrentDynamicCap(0);
    TuneAdvisor::setPostTitleConcurrentDynamicCap(0);
    TuneAdvisor::setPostEmbedConcurrentDynamicCap(0);
    TuneAdvisor::endDynamicCapWrite();

    const auto caps = TuneAdvisor::readDynamicCapsConsistent();
    for (std::size_t i = 0; i < 6; ++i) {
        CHECK(caps[i] == 0);
    }
}

// =============================================================================
// Test 4: DynamicCap(1) still works as a cap of 1 (regression guard)
// =============================================================================

TEST_CASE("DynamicCap(1) caps extraction to 1 (regression guard)",
          "[daemon][tune][dynamic-cap][sentinel][catch2]") {
    resetPostIngestOverridesAndCaps();
    setAllPostIngestStagesActive();

    HwGuard hwGuard(16);
    ProfileGuard profileGuard(TuneAdvisor::Profile::Balanced);
    TuneAdvisor::setPostIngestTotalConcurrent(12);

    const auto baseline = TuneAdvisor::postExtractionConcurrent();
    REQUIRE(baseline >= 1);

    TuneAdvisor::beginDynamicCapWrite();
    TuneAdvisor::setPostExtractionConcurrentDynamicCap(1);
    TuneAdvisor::endDynamicCapWrite();

    CHECK(TuneAdvisor::postExtractionConcurrent() <= 1);
}
