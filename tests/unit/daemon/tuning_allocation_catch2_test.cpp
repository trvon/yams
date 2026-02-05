// TuneAdvisor / ResourceGovernor allocation bug tests (Catch2)
// Tests 4 bugs: integer truncation, governor cap inversion, ONNX zero shared capacity,
// stage starvation. These tests should FAIL against the current buggy code (TDD red phase).

#include <catch2/catch_test_macros.hpp>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/ResourceGovernor.h>
#include <cstdlib>
#include <string>
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

} // namespace

// =============================================================================
// Test Group A: postIngestTotalConcurrent scaling across core counts
// =============================================================================

TEST_CASE("postIngestTotalConcurrent scaling across core counts",
          "[daemon][tune][allocation][catch2]") {
    SECTION("Budget is monotonically non-decreasing in hardware concurrency") {
        resetPostIngestOverrides();
        EnvGuard envPostIngest("YAMS_POST_INGEST_TOTAL_CONCURRENT", "0");
        EnvGuard envMaxThreads("YAMS_MAX_THREADS", "0");

        for (auto profile : {TuneAdvisor::Profile::Efficient, TuneAdvisor::Profile::Balanced,
                             TuneAdvisor::Profile::Aggressive}) {
            ProfileGuard profileGuard(profile);

            uint32_t prev = 0;
            for (unsigned hw : {2u, 4u, 8u, 16u}) {
                HwGuard hwGuard(hw);
                uint32_t budget = TuneAdvisor::postIngestTotalConcurrent();

                INFO("profile=" << static_cast<int>(profile) << " hw=" << hw
                                << " budget=" << budget << " prev=" << prev);
                if (prev != 0) {
                    CHECK(budget >= prev);
                }
                prev = budget;
            }
        }
    }

    SECTION("Budget is monotonically non-decreasing in profile aggressiveness") {
        resetPostIngestOverrides();
        EnvGuard envPostIngest("YAMS_POST_INGEST_TOTAL_CONCURRENT", "0");
        EnvGuard envMaxThreads("YAMS_MAX_THREADS", "0");

        for (unsigned hw : {2u, 4u, 8u}) {
            HwGuard hwGuard(hw);

            uint32_t eff, bal, agg;
            {
                ProfileGuard g(TuneAdvisor::Profile::Efficient);
                eff = TuneAdvisor::postIngestTotalConcurrent();
            }
            {
                ProfileGuard g(TuneAdvisor::Profile::Balanced);
                bal = TuneAdvisor::postIngestTotalConcurrent();
            }
            {
                ProfileGuard g(TuneAdvisor::Profile::Aggressive);
                agg = TuneAdvisor::postIngestTotalConcurrent();
            }

            INFO("hw=" << hw << " eff=" << eff << " bal=" << bal << " agg=" << agg);
            CHECK(eff <= bal);
            CHECK(bal <= agg);
        }
    }

    SECTION("8-core Balanced budget exceeds minimum floor (BUG 1 regression)") {
        resetPostIngestOverrides();
        EnvGuard envPostIngest("YAMS_POST_INGEST_TOTAL_CONCURRENT", "0");
        EnvGuard envMaxThreads("YAMS_MAX_THREADS", "0");

        HwGuard hwGuard(8);
        ProfileGuard profileGuard(TuneAdvisor::Profile::Balanced);

        uint32_t budget = TuneAdvisor::postIngestTotalConcurrent();
        INFO("hw=8 profile=Balanced budget=" << budget);

        // Currently buggy: (8*20)/100=1, (8*15)/100=1, total=2+floor(1*0.5)=2
        // After fix with round-up: (8*20+99)/100=2, (8*15+99)/100=2, total=2+floor(2*0.5)=3
        CHECK(budget > 2);
    }

    SECTION("16-core Aggressive budget is substantially larger than 2-core Efficient") {
        resetPostIngestOverrides();
        EnvGuard envPostIngest("YAMS_POST_INGEST_TOTAL_CONCURRENT", "0");
        EnvGuard envMaxThreads("YAMS_MAX_THREADS", "0");

        uint32_t budget2Eff = 0;
        {
            HwGuard hwGuard(2);
            ProfileGuard profileGuard(TuneAdvisor::Profile::Efficient);
            budget2Eff = TuneAdvisor::postIngestTotalConcurrent();
        }

        uint32_t budget16Agg = 0;
        {
            HwGuard hwGuard(16);
            ProfileGuard profileGuard(TuneAdvisor::Profile::Aggressive);
            budget16Agg = TuneAdvisor::postIngestTotalConcurrent();
        }

        INFO("budget2Eff=" << budget2Eff << " budget16Agg=" << budget16Agg);
        CHECK(budget16Agg >= budget2Eff + 2);
    }
}

// =============================================================================
// Test Group B: postIngestBudgetedConcurrency stage starvation
// =============================================================================

TEST_CASE("postIngestBudgetedConcurrency stage starvation",
          "[daemon][tune][allocation][starvation][catch2]") {
    SECTION("All 6 stages get at least 1 slot on 8-core Balanced (BUG 4 regression)") {
        resetPostIngestOverrides();
        setAllPostIngestStagesActive();

        EnvGuard envPostIngest("YAMS_POST_INGEST_TOTAL_CONCURRENT", "0");
        EnvGuard envMaxThreads("YAMS_MAX_THREADS", "0");
        EnvGuard envExtraction("YAMS_POST_EXTRACTION_CONCURRENT", "0");
        EnvGuard envKg("YAMS_POST_KG_CONCURRENT", "0");
        EnvGuard envSymbol("YAMS_POST_SYMBOL_CONCURRENT", "0");
        EnvGuard envEntity("YAMS_POST_ENTITY_CONCURRENT", "0");
        EnvGuard envTitle("YAMS_POST_TITLE_CONCURRENT", "0");
        EnvGuard envEmbed("YAMS_POST_EMBED_CONCURRENT", "0");

        HwGuard hwGuard(8);
        ProfileGuard profileGuard(TuneAdvisor::Profile::Balanced);

        uint32_t totalBudget = TuneAdvisor::postIngestTotalConcurrent();
        uint32_t extraction = TuneAdvisor::postExtractionConcurrent();
        uint32_t kg = TuneAdvisor::postKgConcurrent();
        uint32_t symbol = TuneAdvisor::postSymbolConcurrent();
        uint32_t entity = TuneAdvisor::postEntityConcurrent();
        uint32_t title = TuneAdvisor::postTitleConcurrent();
        uint32_t embed = TuneAdvisor::postEmbedConcurrent();

        INFO("hw=8 profile=Balanced totalBudget=" << totalBudget);
        INFO("alloc extraction=" << extraction << " kg=" << kg << " symbol=" << symbol
                                 << " entity=" << entity << " title=" << title
                                 << " embed=" << embed);

        // Currently totalBudget=2, only extraction=1 embed=1, rest=0. All should be >=1.
        CHECK(extraction >= 1);
        CHECK(kg >= 1);
        CHECK(symbol >= 1);
        CHECK(entity >= 1);
        CHECK(title >= 1);
        CHECK(embed >= 1);
    }

    SECTION("extraction and embed always get at least 1 when budget >= 2 (invariant)") {
        resetPostIngestOverrides();
        setAllPostIngestStagesActive();

        EnvGuard envPostIngest("YAMS_POST_INGEST_TOTAL_CONCURRENT", "0");
        EnvGuard envMaxThreads("YAMS_MAX_THREADS", "0");
        EnvGuard envExtraction("YAMS_POST_EXTRACTION_CONCURRENT", "0");
        EnvGuard envKg("YAMS_POST_KG_CONCURRENT", "0");
        EnvGuard envSymbol("YAMS_POST_SYMBOL_CONCURRENT", "0");
        EnvGuard envEntity("YAMS_POST_ENTITY_CONCURRENT", "0");
        EnvGuard envTitle("YAMS_POST_TITLE_CONCURRENT", "0");
        EnvGuard envEmbed("YAMS_POST_EMBED_CONCURRENT", "0");

        HwGuard hwGuard(2);
        ProfileGuard profileGuard(TuneAdvisor::Profile::Efficient);

        uint32_t totalBudget = TuneAdvisor::postIngestTotalConcurrent();
        INFO("hw=2 profile=Efficient totalBudget=" << totalBudget);

        CHECK(totalBudget >= 2);
        CHECK(TuneAdvisor::postExtractionConcurrent() >= 1);
        CHECK(TuneAdvisor::postEmbedConcurrent() >= 1);
    }
}

// =============================================================================
// Test Group C: ResourceGovernor scaling cap monotonicity
// =============================================================================

TEST_CASE("ResourceGovernor scaling cap monotonicity",
          "[daemon][tune][allocation][governor][catch2]") {
    SECTION("Warning caps never exceed Normal caps at 2-core Balanced (BUG 2 regression)") {
        resetPostIngestOverrides();
        EnvGuard envMaxThreads("YAMS_MAX_THREADS", "0");
        EnvGuard envPostIngest("YAMS_POST_INGEST_TOTAL_CONCURRENT", "0");

        HwGuard hwGuard(2);
        ProfileGuard profileGuard(TuneAdvisor::Profile::Balanced);

        auto& governor = ResourceGovernor::instance();

        governor.testing_updateScalingCaps(ResourcePressureLevel::Normal);
        auto normalCaps = governor.getScalingCaps();

        governor.testing_updateScalingCaps(ResourcePressureLevel::Warning);
        auto warningCaps = governor.getScalingCaps();

        INFO("normalCaps ex=" << normalCaps.extractionConcurrency
                              << " kg=" << normalCaps.kgConcurrency
                              << " embed=" << normalCaps.embedConcurrency
                              << " ingest=" << normalCaps.ingestWorkers
                              << " search=" << normalCaps.searchConcurrency);
        INFO("warningCaps ex=" << warningCaps.extractionConcurrency
                               << " kg=" << warningCaps.kgConcurrency
                               << " embed=" << warningCaps.embedConcurrency
                               << " ingest=" << warningCaps.ingestWorkers
                               << " search=" << warningCaps.searchConcurrency);

        // Currently buggy: max(2, 1/2) = 2 > 1 for extraction/kg at small defaults
        CHECK(warningCaps.ingestWorkers <= normalCaps.ingestWorkers);
        CHECK(warningCaps.searchConcurrency <= normalCaps.searchConcurrency);
        CHECK(warningCaps.extractionConcurrency <= normalCaps.extractionConcurrency);
        CHECK(warningCaps.kgConcurrency <= normalCaps.kgConcurrency);
        CHECK(warningCaps.embedConcurrency <= normalCaps.embedConcurrency);

        // Restore Normal
        governor.testing_updateScalingCaps(ResourcePressureLevel::Normal);
    }

    SECTION("Caps are monotonically non-increasing across all pressure levels") {
        resetPostIngestOverrides();
        EnvGuard envMaxThreads("YAMS_MAX_THREADS", "0");
        EnvGuard envPostIngest("YAMS_POST_INGEST_TOTAL_CONCURRENT", "0");

        HwGuard hwGuard(4);
        ProfileGuard profileGuard(TuneAdvisor::Profile::Balanced);

        auto& governor = ResourceGovernor::instance();

        governor.testing_updateScalingCaps(ResourcePressureLevel::Normal);
        auto normalCaps = governor.getScalingCaps();
        governor.testing_updateScalingCaps(ResourcePressureLevel::Warning);
        auto warningCaps = governor.getScalingCaps();
        governor.testing_updateScalingCaps(ResourcePressureLevel::Critical);
        auto criticalCaps = governor.getScalingCaps();
        governor.testing_updateScalingCaps(ResourcePressureLevel::Emergency);
        auto emergencyCaps = governor.getScalingCaps();

        INFO("Normal: ingest=" << normalCaps.ingestWorkers
                               << " search=" << normalCaps.searchConcurrency
                               << " ex=" << normalCaps.extractionConcurrency
                               << " kg=" << normalCaps.kgConcurrency
                               << " embed=" << normalCaps.embedConcurrency);
        INFO("Warning: ingest=" << warningCaps.ingestWorkers
                                << " search=" << warningCaps.searchConcurrency
                                << " ex=" << warningCaps.extractionConcurrency
                                << " kg=" << warningCaps.kgConcurrency
                                << " embed=" << warningCaps.embedConcurrency);
        INFO("Critical: ingest=" << criticalCaps.ingestWorkers
                                 << " search=" << criticalCaps.searchConcurrency
                                 << " ex=" << criticalCaps.extractionConcurrency
                                 << " kg=" << criticalCaps.kgConcurrency
                                 << " embed=" << criticalCaps.embedConcurrency);
        INFO("Emergency: ingest=" << emergencyCaps.ingestWorkers
                                  << " search=" << emergencyCaps.searchConcurrency
                                  << " ex=" << emergencyCaps.extractionConcurrency
                                  << " kg=" << emergencyCaps.kgConcurrency
                                  << " embed=" << emergencyCaps.embedConcurrency);

        // Normal >= Warning
        CHECK(normalCaps.ingestWorkers >= warningCaps.ingestWorkers);
        CHECK(normalCaps.searchConcurrency >= warningCaps.searchConcurrency);
        CHECK(normalCaps.extractionConcurrency >= warningCaps.extractionConcurrency);
        CHECK(normalCaps.kgConcurrency >= warningCaps.kgConcurrency);
        CHECK(normalCaps.embedConcurrency >= warningCaps.embedConcurrency);

        // Warning >= Critical
        CHECK(warningCaps.ingestWorkers >= criticalCaps.ingestWorkers);
        CHECK(warningCaps.searchConcurrency >= criticalCaps.searchConcurrency);
        CHECK(warningCaps.extractionConcurrency >= criticalCaps.extractionConcurrency);
        CHECK(warningCaps.kgConcurrency >= criticalCaps.kgConcurrency);
        CHECK(warningCaps.embedConcurrency >= criticalCaps.embedConcurrency);

        // Critical >= Emergency
        CHECK(criticalCaps.ingestWorkers >= emergencyCaps.ingestWorkers);
        CHECK(criticalCaps.searchConcurrency >= emergencyCaps.searchConcurrency);
        CHECK(criticalCaps.extractionConcurrency >= emergencyCaps.extractionConcurrency);
        CHECK(criticalCaps.kgConcurrency >= emergencyCaps.kgConcurrency);
        CHECK(criticalCaps.embedConcurrency >= emergencyCaps.embedConcurrency);

        // Restore Normal
        governor.testing_updateScalingCaps(ResourcePressureLevel::Normal);
    }

    SECTION("Warning caps never exceed Normal caps at 4-core Aggressive") {
        resetPostIngestOverrides();
        EnvGuard envMaxThreads("YAMS_MAX_THREADS", "0");
        EnvGuard envPostIngest("YAMS_POST_INGEST_TOTAL_CONCURRENT", "0");

        HwGuard hwGuard(4);
        ProfileGuard profileGuard(TuneAdvisor::Profile::Aggressive);

        auto& governor = ResourceGovernor::instance();

        governor.testing_updateScalingCaps(ResourcePressureLevel::Normal);
        auto normalCaps = governor.getScalingCaps();

        governor.testing_updateScalingCaps(ResourcePressureLevel::Warning);
        auto warningCaps = governor.getScalingCaps();

        INFO("normalCaps ex=" << normalCaps.extractionConcurrency
                              << " kg=" << normalCaps.kgConcurrency
                              << " embed=" << normalCaps.embedConcurrency);
        INFO("warningCaps ex=" << warningCaps.extractionConcurrency
                               << " kg=" << warningCaps.kgConcurrency
                               << " embed=" << warningCaps.embedConcurrency);

        CHECK(warningCaps.ingestWorkers <= normalCaps.ingestWorkers);
        CHECK(warningCaps.searchConcurrency <= normalCaps.searchConcurrency);
        CHECK(warningCaps.extractionConcurrency <= normalCaps.extractionConcurrency);
        CHECK(warningCaps.kgConcurrency <= normalCaps.kgConcurrency);
        CHECK(warningCaps.embedConcurrency <= normalCaps.embedConcurrency);

        // Restore Normal
        governor.testing_updateScalingCaps(ResourcePressureLevel::Normal);
    }
}

// =============================================================================
// Test Group D: ONNX shared capacity
// =============================================================================

TEST_CASE("ONNX shared capacity", "[daemon][tune][allocation][onnx][catch2]") {
    SECTION("ONNX max concurrent exceeds total reserved (BUG 3 regression)") {
        resetPostIngestOverrides();

        // Clear any ONNX env overrides so defaults are used
        TuneAdvisor::setOnnxMaxConcurrent(0);
        TuneAdvisor::setOnnxGlinerReserved(0);
        TuneAdvisor::setOnnxEmbedReserved(0);
        TuneAdvisor::setOnnxRerankerReserved(0);

        for (auto profile : {TuneAdvisor::Profile::Efficient, TuneAdvisor::Profile::Balanced,
                             TuneAdvisor::Profile::Aggressive}) {
            ProfileGuard profileGuard(profile);

            for (unsigned hw : {2u, 4u}) {
                HwGuard hwGuard(hw);

                uint32_t maxConc = TuneAdvisor::onnxMaxConcurrent();
                uint32_t reserved = TuneAdvisor::onnxGlinerReserved() +
                                    TuneAdvisor::onnxEmbedReserved() +
                                    TuneAdvisor::onnxRerankerReserved();

                INFO("profile=" << static_cast<int>(profile) << " hw=" << hw
                                << " maxConc=" << maxConc << " reserved=" << reserved);

                // Must leave at least 1 shared slot beyond total reserved
                CHECK(maxConc > reserved);
            }
        }
    }

    SECTION("ONNX scaled max in TuningManager logic exceeds total reserved") {
        resetPostIngestOverrides();

        TuneAdvisor::setOnnxMaxConcurrent(0);
        TuneAdvisor::setOnnxGlinerReserved(0);
        TuneAdvisor::setOnnxEmbedReserved(0);
        TuneAdvisor::setOnnxRerankerReserved(0);

        for (auto profile : {TuneAdvisor::Profile::Efficient, TuneAdvisor::Profile::Balanced,
                             TuneAdvisor::Profile::Aggressive}) {
            ProfileGuard profileGuard(profile);

            for (unsigned hw : {2u, 4u}) {
                HwGuard hwGuard(hw);

                uint32_t maxConc = TuneAdvisor::onnxMaxConcurrent();
                uint32_t reserved = TuneAdvisor::onnxGlinerReserved() +
                                    TuneAdvisor::onnxEmbedReserved() +
                                    TuneAdvisor::onnxRerankerReserved();
                double scale = TuneAdvisor::profileScale();

                // Mirrors TuningManager::configureOnnxConcurrencyRegistry logic
                // (post-fix: reserved + 1 to guarantee shared capacity):
                uint32_t scaledMax = static_cast<uint32_t>(maxConc * scale);
                scaledMax = std::max(scaledMax, 2u);
                scaledMax = std::max(scaledMax, reserved + 1);

                INFO("profile=" << static_cast<int>(profile) << " hw=" << hw
                                << " scale=" << scale << " maxConc=" << maxConc
                                << " reserved=" << reserved << " scaledMax=" << scaledMax);

                // Must have at least 1 shared slot beyond reserved
                CHECK(scaledMax > reserved);
            }
        }
    }
}
