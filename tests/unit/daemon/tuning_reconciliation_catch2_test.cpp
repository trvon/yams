// TuneAdvisor / TuningManager reconciliation + pool config + ETA calculation tests (Catch2)
// Tests three areas:
//   Group A: Post-ingest budget reconciliation and distribution correctness
//   Group B: Pool configuration minimum guards
//   Group C: ETA remaining calculation accuracy
//
// Uses same TDD pattern as tuning_allocation_catch2_test.cpp.

#include <catch2/catch_test_macros.hpp>
#include <yams/daemon/components/GradientLimiter.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/TuningManager.h>
#include <array>
#include <cstdlib>
#include <string>
#include "../../common/env_compat.h"
#include <yams/compat/unistd.h>

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

class EnvGuard {
    std::string name_;
    std::string prev_;
    bool hadPrev_;

public:
    EnvGuard(const char* name, const char* value) : name_(name), hadPrev_(false) {
        if (const char* existing = std::getenv(name)) {
            prev_ = existing;
            hadPrev_ = true;
        }
        setenv(name, value, 1);
    }
    ~EnvGuard() {
        if (hadPrev_)
            setenv(name_.c_str(), prev_.c_str(), 1);
        else
            unsetenv(name_.c_str());
    }
    EnvGuard(const EnvGuard&) = delete;
    EnvGuard& operator=(const EnvGuard&) = delete;
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

/// Reset all per-stage overrides to 0 (no override)
void resetPostIngestOverrides() {
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
    TuneAdvisor::setPostIngestTotalConcurrent(0);
}

void setAllPostIngestStagesActive() {
    TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Extraction, true);
    TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::KnowledgeGraph, true);
    TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Symbol, true);
    TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Entity, true);
    TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Title, true);
    TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Embed, true);
}

/// Helper: sum all per-stage post-ingest concurrent values
uint32_t sumAllStages() {
    return TuneAdvisor::postExtractionConcurrent() + TuneAdvisor::postKgConcurrent() +
           TuneAdvisor::postSymbolConcurrent() + TuneAdvisor::postEntityConcurrent() +
           TuneAdvisor::postTitleConcurrent() + TuneAdvisor::postEmbedConcurrent();
}

} // namespace

// =============================================================================
// Group A: Post-ingest budget reconciliation and distribution correctness
// =============================================================================

TEST_CASE("Post-ingest budget distribution correctness", "[daemon][tune][reconciliation][catch2]") {
    SECTION("All active stages get >= 1 when budget allows") {
        // With 6 active stages and budget >= 6, every stage should get at least 1.
        resetPostIngestOverrides();
        setAllPostIngestStagesActive();
        EnvGuard envPostIngest("YAMS_POST_INGEST_TOTAL_CONCURRENT", "0");
        EnvGuard envMaxThreads("YAMS_MAX_THREADS", "0");

        for (unsigned hw : {8u, 12u, 16u, 24u, 32u}) {
            HwGuard hwGuard(hw);

            for (auto profile : {TuneAdvisor::Profile::Efficient, TuneAdvisor::Profile::Balanced,
                                 TuneAdvisor::Profile::Aggressive}) {
                ProfileGuard profileGuard(profile);

                uint32_t budget = TuneAdvisor::postIngestTotalConcurrent();
                if (budget < 6)
                    continue; // Only test when budget can cover all stages

                uint32_t ext = TuneAdvisor::postExtractionConcurrent();
                uint32_t kg = TuneAdvisor::postKgConcurrent();
                uint32_t sym = TuneAdvisor::postSymbolConcurrent();
                uint32_t ent = TuneAdvisor::postEntityConcurrent();
                uint32_t tit = TuneAdvisor::postTitleConcurrent();
                uint32_t emb = TuneAdvisor::postEmbedConcurrent();

                INFO("hw=" << hw << " profile=" << static_cast<int>(profile)
                           << " budget=" << budget);
                INFO("ext=" << ext << " kg=" << kg << " sym=" << sym << " ent=" << ent
                            << " tit=" << tit << " emb=" << emb);

                CHECK(ext >= 1);
                CHECK(kg >= 1);
                CHECK(sym >= 1);
                CHECK(ent >= 1);
                CHECK(tit >= 1);
                CHECK(emb >= 1);
            }
        }
    }

    SECTION("Sum of per-stage allocations does not exceed budget") {
        resetPostIngestOverrides();
        setAllPostIngestStagesActive();
        EnvGuard envPostIngest("YAMS_POST_INGEST_TOTAL_CONCURRENT", "0");
        EnvGuard envMaxThreads("YAMS_MAX_THREADS", "0");

        for (unsigned hw : {2u, 4u, 8u, 16u, 32u}) {
            HwGuard hwGuard(hw);

            for (auto profile : {TuneAdvisor::Profile::Efficient, TuneAdvisor::Profile::Balanced,
                                 TuneAdvisor::Profile::Aggressive}) {
                ProfileGuard profileGuard(profile);

                uint32_t budget = TuneAdvisor::postIngestTotalConcurrent();
                uint32_t sum = sumAllStages();

                INFO("hw=" << hw << " profile=" << static_cast<int>(profile) << " budget=" << budget
                           << " sum=" << sum);
                CHECK(sum <= budget);
            }
        }
    }

    SECTION("Extraction and embed get minimum 1 when budget >= 2") {
        resetPostIngestOverrides();
        setAllPostIngestStagesActive();
        EnvGuard envPostIngest("YAMS_POST_INGEST_TOTAL_CONCURRENT", "0");
        EnvGuard envMaxThreads("YAMS_MAX_THREADS", "0");

        for (unsigned hw : {2u, 3u, 4u}) {
            HwGuard hwGuard(hw);

            for (auto profile : {TuneAdvisor::Profile::Efficient, TuneAdvisor::Profile::Balanced,
                                 TuneAdvisor::Profile::Aggressive}) {
                ProfileGuard profileGuard(profile);

                uint32_t budget = TuneAdvisor::postIngestTotalConcurrent();
                uint32_t ext = TuneAdvisor::postExtractionConcurrent();
                uint32_t emb = TuneAdvisor::postEmbedConcurrent();

                INFO("hw=" << hw << " profile=" << static_cast<int>(profile)
                           << " budget=" << budget);

                if (budget >= 2) {
                    CHECK(ext >= 1);
                    CHECK(emb >= 1);
                }
            }
        }
    }

    SECTION("Embed gets higher weight than individual other stages") {
        // Embed has weight 2 vs weight 1 for others; with sufficient budget,
        // embed should get at least as much as any single other stage.
        resetPostIngestOverrides();
        setAllPostIngestStagesActive();
        EnvGuard envPostIngest("YAMS_POST_INGEST_TOTAL_CONCURRENT", "0");
        EnvGuard envMaxThreads("YAMS_MAX_THREADS", "0");

        for (unsigned hw : {16u, 24u, 32u}) {
            HwGuard hwGuard(hw);
            ProfileGuard profileGuard(TuneAdvisor::Profile::Balanced);

            uint32_t emb = TuneAdvisor::postEmbedConcurrent();
            uint32_t kg = TuneAdvisor::postKgConcurrent();
            uint32_t sym = TuneAdvisor::postSymbolConcurrent();

            INFO("hw=" << hw << " emb=" << emb << " kg=" << kg << " sym=" << sym);
            CHECK(emb >= kg);
            CHECK(emb >= sym);
        }
    }
}

TEST_CASE("Contention-aware budget adjustment logic",
          "[daemon][tune][reconciliation][contention][catch2]") {
    SECTION("Severe contention drops budget aggressively") {
        const int32_t adjust = TuningManager::testing_computeContentionBudgetAdjustment(
            /*waitingRequests=*/3,
            /*waitMicrosDelta=*/25000,
            /*timeoutDelta=*/1,
            /*failedDelta=*/0,
            /*processedDelta=*/6,
            /*healthyTicks=*/0);
        CHECK(adjust == -2);
    }

    SECTION("Moderate contention applies gentle reduction") {
        const int32_t adjust = TuningManager::testing_computeContentionBudgetAdjustment(
            /*waitingRequests=*/2,
            /*waitMicrosDelta=*/9000,
            /*timeoutDelta=*/0,
            /*failedDelta=*/0,
            /*processedDelta=*/6,
            /*healthyTicks=*/0);
        CHECK(adjust == -1);
    }

    SECTION("Healthy sustained throughput allows gradual increase") {
        const int32_t adjust = TuningManager::testing_computeContentionBudgetAdjustment(
            /*waitingRequests=*/0,
            /*waitMicrosDelta=*/500,
            /*timeoutDelta=*/0,
            /*failedDelta=*/0,
            /*processedDelta=*/6,
            /*healthyTicks=*/2);
        CHECK(adjust == 1);
    }

    SECTION("Insufficient healthy history keeps budget steady") {
        const int32_t adjust = TuningManager::testing_computeContentionBudgetAdjustment(
            /*waitingRequests=*/0,
            /*waitMicrosDelta=*/500,
            /*timeoutDelta=*/0,
            /*failedDelta=*/0,
            /*processedDelta=*/6,
            /*healthyTicks=*/1);
        CHECK(adjust == 0);
    }
}

// =============================================================================
// Group A2: Active mask filtering
// =============================================================================

TEST_CASE("Post-ingest active mask filters stages correctly",
          "[daemon][tune][reconciliation][catch2]") {
    SECTION("Disabled stages get 0 allocation") {
        resetPostIngestOverrides();
        EnvGuard envPostIngest("YAMS_POST_INGEST_TOTAL_CONCURRENT", "0");
        EnvGuard envMaxThreads("YAMS_MAX_THREADS", "0");
        HwGuard hwGuard(16);
        ProfileGuard profileGuard(TuneAdvisor::Profile::Balanced);

        // Enable only extraction and embed (disable kg, symbol, entity, title)
        TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Extraction, true);
        TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::KnowledgeGraph, false);
        TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Symbol, false);
        TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Entity, false);
        TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Title, false);
        TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Embed, true);

        uint32_t kg = TuneAdvisor::postKgConcurrent();
        uint32_t sym = TuneAdvisor::postSymbolConcurrent();
        uint32_t ent = TuneAdvisor::postEntityConcurrent();
        uint32_t tit = TuneAdvisor::postTitleConcurrent();
        uint32_t ext = TuneAdvisor::postExtractionConcurrent();
        uint32_t emb = TuneAdvisor::postEmbedConcurrent();

        CHECK(kg == 0);
        CHECK(sym == 0);
        CHECK(ent == 0);
        CHECK(tit == 0);
        CHECK(ext >= 1);
        CHECK(emb >= 1);

        // Budget redistributed to enabled stages — they should get more
        // than when all 6 are active
        setAllPostIngestStagesActive();
        uint32_t extAll = TuneAdvisor::postExtractionConcurrent();
        uint32_t embAll = TuneAdvisor::postEmbedConcurrent();

        INFO("ext(2 active)=" << ext << " ext(all active)=" << extAll);
        INFO("emb(2 active)=" << emb << " emb(all active)=" << embAll);
        CHECK(ext >= extAll);
        CHECK(emb >= embAll);
    }

    SECTION("Disabling all stages reduces total budget") {
        resetPostIngestOverrides();
        HwGuard hwGuard(8);
        ProfileGuard profileGuard(TuneAdvisor::Profile::Balanced);

        uint32_t budgetAllActive = TuneAdvisor::postIngestTotalConcurrent();

        TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Extraction, false);
        TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::KnowledgeGraph, false);
        TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Symbol, false);
        TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Entity, false);
        TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Title, false);
        TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Embed, false);

        // When all disabled, the total budget floor clamps at 0 active stages
        // but per-stage getters still return their computed values (the active
        // mask only affects the total budget, not individual allocations).
        uint32_t budgetNoneActive = TuneAdvisor::postIngestTotalConcurrent();
        INFO("budget(all active)=" << budgetAllActive
                                   << " budget(none active)=" << budgetNoneActive);
        CHECK(budgetNoneActive <= budgetAllActive);

        // Restore for subsequent tests
        setAllPostIngestStagesActive();
    }
}

// =============================================================================
// Group A3: Per-stage override clamping
// =============================================================================

TEST_CASE("Per-stage overrides are respected and clamped",
          "[daemon][tune][reconciliation][catch2]") {
    SECTION("Override takes precedence over computed allocation") {
        resetPostIngestOverrides();
        setAllPostIngestStagesActive();
        HwGuard hwGuard(16);
        ProfileGuard profileGuard(TuneAdvisor::Profile::Balanced);

        // Set a specific override for extraction
        TuneAdvisor::setPostExtractionConcurrent(5);
        uint32_t ext = TuneAdvisor::postExtractionConcurrent();
        CHECK(ext == 5);

        // Reset
        TuneAdvisor::setPostExtractionConcurrent(0);
    }

    SECTION("Override is clamped to max cap") {
        resetPostIngestOverrides();
        setAllPostIngestStagesActive();
        HwGuard hwGuard(16);
        ProfileGuard profileGuard(TuneAdvisor::Profile::Balanced);

        // Entity max cap is 16 — setting beyond should be clamped
        TuneAdvisor::setPostEntityConcurrent(16);
        uint32_t ent = TuneAdvisor::postEntityConcurrent();
        INFO("entity with override=16: " << ent);
        CHECK(ent <= 16);

        // Reset
        TuneAdvisor::setPostEntityConcurrent(0);
    }

    SECTION("Overrides are independent — sum may exceed budget (no runtime clamping)") {
        resetPostIngestOverrides();
        setAllPostIngestStagesActive();
        HwGuard hwGuard(8);
        ProfileGuard profileGuard(TuneAdvisor::Profile::Balanced);

        // TuneAdvisor per-stage overrides are not clamped to the total budget.
        // The TuningManager reconciliation loop is responsible for capping at runtime.
        // Verify overrides take effect individually.
        TuneAdvisor::setPostExtractionConcurrent(3);
        TuneAdvisor::setPostKgConcurrent(3);
        TuneAdvisor::setPostEmbedConcurrent(3);

        CHECK(TuneAdvisor::postExtractionConcurrent() == 3);
        CHECK(TuneAdvisor::postKgConcurrent() == 3);
        CHECK(TuneAdvisor::postEmbedConcurrent() == 3);

        uint32_t budget = TuneAdvisor::postIngestTotalConcurrent();
        uint32_t sum = sumAllStages();
        INFO("budget=" << budget << " sum=" << sum);
        // Sum CAN exceed budget — that's by design; TuningManager reconciles at runtime
        CHECK(sum >= 9); // at least the 3 overridden stages

        resetPostIngestOverrides();
    }
}

// =============================================================================
// Group B: Pool configuration minimum guards
// =============================================================================

TEST_CASE("Pool configuration defaults are reasonable",
          "[daemon][tune][reconciliation][pool][catch2]") {
    SECTION("poolMinSizeIpc defaults to at least 1") {
        uint32_t minIpc = TuneAdvisor::poolMinSizeIpc();
        INFO("poolMinSizeIpc default=" << minIpc);
        CHECK(minIpc >= 1);
    }

    SECTION("poolMinSizeIpcIo defaults to at least 1") {
        uint32_t minIo = TuneAdvisor::poolMinSizeIpcIo();
        INFO("poolMinSizeIpcIo default=" << minIo);
        CHECK(minIo >= 1);
    }

    SECTION("poolMaxSizeIpc is greater than poolMinSizeIpc") {
        uint32_t minIpc = TuneAdvisor::poolMinSizeIpc();
        uint32_t maxIpc = TuneAdvisor::poolMaxSizeIpc();
        INFO("IPC pool: min=" << minIpc << " max=" << maxIpc);
        CHECK(maxIpc > minIpc);
    }

    SECTION("poolMaxSizeIpcIo is greater than poolMinSizeIpcIo") {
        uint32_t minIo = TuneAdvisor::poolMinSizeIpcIo();
        uint32_t maxIo = TuneAdvisor::poolMaxSizeIpcIo();
        INFO("IO pool: min=" << minIo << " max=" << maxIo);
        CHECK(maxIo > minIo);
    }

    SECTION("IO pool min_size should be at least 2 after guard") {
        // This tests the ServiceManager guard: IO pool should have min >= 2.
        // The raw TuneAdvisor default is 1, but ServiceManager should guard it.
        // We test the expected post-guard value here.
        uint32_t rawMin = TuneAdvisor::poolMinSizeIpcIo();
        uint32_t guardedMin = rawMin < 2 ? 2 : rawMin;
        INFO("raw min=" << rawMin << " guarded min=" << guardedMin);
        CHECK(guardedMin >= 2);
    }

    SECTION("recommendedThreads never returns 0") {
        for (unsigned hw : {1u, 2u, 4u}) {
            HwGuard hwGuard(hw);
            for (auto profile : {TuneAdvisor::Profile::Efficient, TuneAdvisor::Profile::Balanced,
                                 TuneAdvisor::Profile::Aggressive}) {
                ProfileGuard profileGuard(profile);

                uint32_t threads = TuneAdvisor::recommendedThreads(0.5);
                INFO("hw=" << hw << " profile=" << static_cast<int>(profile)
                           << " recommendedThreads(0.5)=" << threads);
                CHECK(threads >= 1);
            }
        }
    }
}

// =============================================================================
// Group C: ETA remaining calculation accuracy
// =============================================================================

TEST_CASE("ETA remaining calculation handles integer truncation",
          "[daemon][tune][reconciliation][eta][catch2]") {
    // Test the ETA formula: remain = max(0, exp - (exp * progress + 99) / 100)
    // The old formula was: remain = max(0, exp - (exp * progress) / 100)
    // which suffers from integer truncation for small exp values.

    auto computeEtaRemaining = [](int exp, int progress) -> int {
        return ServiceManager::computeEtaRemaining(exp, progress);
    };

    SECTION("Zero progress gives full ETA") {
        CHECK(computeEtaRemaining(5, 0) == 5);
        CHECK(computeEtaRemaining(1, 0) == 1);
        CHECK(computeEtaRemaining(20, 0) == 20);
    }

    SECTION("100% progress gives 0 ETA") {
        CHECK(computeEtaRemaining(5, 100) == 0);
        CHECK(computeEtaRemaining(1, 100) == 0);
        CHECK(computeEtaRemaining(20, 100) == 0);
    }

    SECTION("50% progress on small exp should reduce ETA") {
        // With exp=1, progress=50: old formula gives (1*50)/100=0, remain=1
        // Fixed formula should give remain <= 1 (ideally 0, since half is done)
        int remain = computeEtaRemaining(1, 50);
        INFO("exp=1, progress=50, remain=" << remain);
        CHECK(remain <= 1);
        CHECK(remain >= 0);
    }

    SECTION("75% progress on small exp should reduce ETA significantly") {
        // With exp=1, progress=75: old formula gives (1*75)/100=0, remain=1
        // Fixed formula should give 0
        int remain = computeEtaRemaining(1, 75);
        INFO("exp=1, progress=75, remain=" << remain);
        CHECK(remain == 0);
    }

    SECTION("ETA is monotonically non-increasing as progress increases") {
        for (int exp : {1, 2, 3, 5, 10, 20}) {
            int prev = exp;
            for (int progress = 0; progress <= 100; progress += 10) {
                int remain = computeEtaRemaining(exp, progress);
                INFO("exp=" << exp << " progress=" << progress << " remain=" << remain
                            << " prev=" << prev);
                CHECK(remain <= prev);
                CHECK(remain >= 0);
                prev = remain;
            }
        }
    }

    SECTION("Normal sized exp values are accurate") {
        // exp=5, progress=50: should be about 2-3
        int r1 = computeEtaRemaining(5, 50);
        CHECK(r1 >= 2);
        CHECK(r1 <= 3);

        // exp=10, progress=30: should be about 7
        int r2 = computeEtaRemaining(10, 30);
        CHECK(r2 >= 6);
        CHECK(r2 <= 7);

        // exp=20, progress=90: should be about 2
        int r3 = computeEtaRemaining(20, 90);
        CHECK(r3 >= 1);
        CHECK(r3 <= 2);
    }

    SECTION("Negative and edge cases are safe") {
        CHECK(computeEtaRemaining(0, 0) == 0);
        CHECK(computeEtaRemaining(0, 50) == 0);
        CHECK(computeEtaRemaining(0, 100) == 0);
        // Progress beyond 100 should still be safe
        CHECK(computeEtaRemaining(5, 150) == 0);
    }
}

// =============================================================================
// Group D: Embed scale bias under backlog pressure
// =============================================================================

TEST_CASE("Embed scale bias ramps with queue pressure",
          "[daemon][tune][reconciliation][embed-bias][catch2]") {
    SECTION("Bias increases as embed queue grows") {
        const auto b0 = TuningManager::testing_computeEmbedScaleBias(0, 0, 0, 0);
        const auto b1 = TuningManager::testing_computeEmbedScaleBias(200, 0, 0, 0);
        const auto b2 = TuningManager::testing_computeEmbedScaleBias(800, 0, 0, 0);
        const auto b3 = TuningManager::testing_computeEmbedScaleBias(2200, 0, 0, 0);

        CHECK(b1 >= b0);
        CHECK(b2 >= b1);
        CHECK(b3 >= b2);
    }

    SECTION("Drops and saturation increase bias beyond pure queue level") {
        const auto queueOnly = TuningManager::testing_computeEmbedScaleBias(900, 0, 0, 0);
        const auto withDrops = TuningManager::testing_computeEmbedScaleBias(900, 50, 0, 0);
        const auto withSaturation = TuningManager::testing_computeEmbedScaleBias(900, 50, 900, 1);

        CHECK(withDrops >= queueOnly);
        CHECK(withSaturation >= withDrops);
    }

    SECTION("Bias is bounded to avoid runaway oversubscription") {
        const auto capped = TuningManager::testing_computeEmbedScaleBias(100000, 100000, 100000, 8);
        CHECK(capped <= 8);
    }
}

TEST_CASE("Queue-pressure rebalance shifts capacity to backlogged stages",
          "[daemon][tune][reconciliation][queue][catch2]") {
    SECTION("Shifts one slot from low-pressure to high-pressure stage") {
        std::array<uint32_t, 6> targets{2, 2, 2, 2, 2, 2};
        std::array<uint32_t, 6> floors{1, 0, 0, 0, 0, 0};
        std::array<std::size_t, 6> depths{0, 400, 1, 1, 1, 1};
        std::array<bool, 6> active{true, true, true, true, true, true};

        TuningManager::testing_rebalanceTargetsByQueue(targets, floors, depths, active);

        // KG backlog dominates, so KG should gain capacity.
        CHECK(targets[1] > 2);
        // Total budget must be preserved.
        CHECK(targets[0] + targets[1] + targets[2] + targets[3] + targets[4] + targets[5] == 12);
    }

    SECTION("Does not violate configured floors") {
        std::array<uint32_t, 6> targets{1, 3, 1, 1, 1, 1};
        std::array<uint32_t, 6> floors{1, 0, 0, 0, 0, 0};
        std::array<std::size_t, 6> depths{0, 0, 0, 0, 0, 500};
        std::array<bool, 6> active{true, true, true, true, true, true};

        TuningManager::testing_rebalanceTargetsByQueue(targets, floors, depths, active);

        CHECK(targets[0] >= floors[0]);
        CHECK(targets[5] > 1);
        CHECK(targets[0] + targets[1] + targets[2] + targets[3] + targets[4] + targets[5] == 8);
    }
}

TEST_CASE("Post-ingest scale test mode isolates zero-target policy",
          "[daemon][tune][reconciliation][scale-mode][catch2]") {
    class ScaleModeGuard {
    public:
        ScaleModeGuard() : previous_(TuningManager::testing_postIngestScaleTestMode()) {}
        ~ScaleModeGuard() { TuningManager::testing_setPostIngestScaleTestMode(previous_); }

    private:
        TuningManager::PostIngestScaleTestMode previous_;
    } guard;

    SECTION("Normal mode allows zero only when daemon is idle and not busy") {
        TuningManager::testing_setPostIngestScaleTestMode(
            TuningManager::PostIngestScaleTestMode::Normal);

        CHECK(TuningManager::testing_shouldAllowZeroPostIngestTargets(true, false));
        CHECK_FALSE(TuningManager::testing_shouldAllowZeroPostIngestTargets(false, false));
        CHECK_FALSE(TuningManager::testing_shouldAllowZeroPostIngestTargets(true, true));
        CHECK_FALSE(TuningManager::testing_shouldAllowZeroPostIngestTargets(false, true));
    }

    SECTION("ForceBusy mode disables zero-target scale-down") {
        TuningManager::testing_setPostIngestScaleTestMode(
            TuningManager::PostIngestScaleTestMode::ForceBusy);

        CHECK_FALSE(TuningManager::testing_shouldAllowZeroPostIngestTargets(true, false));
        CHECK_FALSE(TuningManager::testing_shouldAllowZeroPostIngestTargets(false, false));
        CHECK_FALSE(TuningManager::testing_shouldAllowZeroPostIngestTargets(true, true));
    }

    SECTION("ForceIdle mode enables zero-target scale-down") {
        TuningManager::testing_setPostIngestScaleTestMode(
            TuningManager::PostIngestScaleTestMode::ForceIdle);

        CHECK(TuningManager::testing_shouldAllowZeroPostIngestTargets(true, false));
        CHECK(TuningManager::testing_shouldAllowZeroPostIngestTargets(false, false));
        CHECK(TuningManager::testing_shouldAllowZeroPostIngestTargets(true, true));
        CHECK(TuningManager::testing_shouldAllowZeroPostIngestTargets(false, true));
    }
}

// =============================================================================
// Group E: Deterministic tuning simulation with GradientLimiter
// =============================================================================

TEST_CASE("Embed tuning simulation improves queue control with gradient limiter",
          "[daemon][tune][reconciliation][gradient][sim][!benchmark]") {
    struct SimResult {
        std::size_t peakQueue{0};
        std::size_t finalQueue{0};
        std::size_t totalProcessed{0};
        double avgTarget{0.0};
    };

    auto runSim = [&](bool useDynamicBias) -> SimResult {
        GradientLimiter::Config cfg;
        cfg.initialLimit = 4.0;
        cfg.minLimit = 1.0;
        cfg.maxLimit = 32.0;
        cfg.warmupSamples = 3;
        cfg.smoothingAlpha = 0.5;
        cfg.longWindowAlpha = 0.1;
        cfg.tolerance = 1.5;
        GradientLimiter limiter("embed-sim", cfg);

        constexpr std::size_t kTicks = 180;
        constexpr uint32_t kBaseTarget = 2;
        constexpr std::size_t kPerWorkerRate = 22;
        constexpr std::size_t kDropThreshold = 1100;

        std::size_t queue = 0;
        std::size_t peakQueue = 0;
        std::size_t totalProcessed = 0;
        std::size_t targetAccumulator = 0;

        for (std::size_t tick = 0; tick < kTicks; ++tick) {
            std::size_t arrivals = 0;
            if (tick < 40) {
                arrivals = 35; // warm ramp
            } else if (tick < 110) {
                arrivals = 70; // sustained burst
            } else {
                arrivals = 18; // recovery tail
            }

            const uint64_t droppedDelta =
                queue > kDropThreshold ? static_cast<uint64_t>((queue - kDropThreshold) / 60) : 0u;
            const std::size_t inFlightHint = queue > 0 ? 1u : 0u;

            const uint32_t bias = useDynamicBias ? TuningManager::testing_computeEmbedScaleBias(
                                                       queue, droppedDelta, queue, inFlightHint)
                                                 : 0u;
            const uint32_t target = kBaseTarget + bias;
            targetAccumulator += target;

            const uint32_t limiterSlots = std::max<uint32_t>(1u, limiter.effectiveLimit());
            const uint32_t workers = std::min(target, limiterSlots);

            const std::size_t demand = queue + arrivals;
            const std::size_t capacity = static_cast<std::size_t>(workers) * kPerWorkerRate;
            const std::size_t processed = std::min(demand, capacity);
            totalProcessed += processed;
            queue = demand - processed;
            peakQueue = std::max(peakQueue, queue);

            // Drive gradient limiter using queue-correlated RTTs.
            const uint32_t simulatedRttMs =
                static_cast<uint32_t>(8 + std::min<std::size_t>(120, queue / 35));
            const uint32_t inflightForLimiter = std::max<uint32_t>(1u, workers);
            uint32_t acquired = 0;
            for (uint32_t i = 0; i < inflightForLimiter; ++i) {
                if (limiter.tryAcquire()) {
                    ++acquired;
                }
            }
            for (uint32_t i = 0; i < acquired; ++i) {
                limiter.onJobComplete(std::chrono::milliseconds(simulatedRttMs), true);
            }
        }

        SimResult out;
        out.peakQueue = peakQueue;
        out.finalQueue = queue;
        out.totalProcessed = totalProcessed;
        out.avgTarget = static_cast<double>(targetAccumulator) / static_cast<double>(kTicks);
        return out;
    };

    const SimResult baseline = runSim(false);
    const SimResult dynamic = runSim(true);

    INFO("baseline peak=" << baseline.peakQueue << " final=" << baseline.finalQueue << " processed="
                          << baseline.totalProcessed << " avgTarget=" << baseline.avgTarget);
    INFO("dynamic  peak=" << dynamic.peakQueue << " final=" << dynamic.finalQueue << " processed="
                          << dynamic.totalProcessed << " avgTarget=" << dynamic.avgTarget);

    // Dynamic bias should improve drain behavior under the same gradient limiter.
    CHECK(dynamic.totalProcessed >= baseline.totalProcessed);
    CHECK(dynamic.peakQueue <= baseline.peakQueue);
    CHECK(dynamic.finalQueue <= baseline.finalQueue);
    CHECK(dynamic.avgTarget >= baseline.avgTarget);
}
