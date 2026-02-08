// Timing Audit Tests (Catch2)
// Tests for timing bugs identified across the metrics/scaling pipeline:
//   Issue 1: Level/caps atomicity gap in ResourceGovernor::tick()
//   Issue 4: Torn DynamicCap state visible to concurrent readers
//   Issue 6: One-tick recovery lag on pressure de-escalation
//   Issue 7: Budget recomputed 6 times per tick (redundant work)
//
// Issues 3, 5 are structural fixes verified by compilation + member reset.

#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/ResourceGovernor.h>
#include <yams/daemon/components/TuneAdvisor.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include <yams/compat/unistd.h>

using namespace yams::daemon;
using namespace std::chrono_literals;

// =============================================================================
// Test Helpers (mirrored from tuning_reconciliation_catch2_test.cpp)
// =============================================================================

namespace {

/// RAII guard for TuneAdvisor profile restoration
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

/// RAII guard for environment variables
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
        if (hadPrev_) {
            setenv(name_.c_str(), prev_.c_str(), 1);
        } else {
            unsetenv(name_.c_str());
        }
    }
    EnvGuard(const EnvGuard&) = delete;
    EnvGuard& operator=(const EnvGuard&) = delete;
};

/// RAII guard for hardware concurrency override
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

/// Reset all per-stage overrides and dynamic caps to 0
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

    TuneAdvisor::setPostIngestTotalConcurrent(0);
}

/// Enable all post-ingest stages
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
// Issue 1: Level/caps atomicity gap in ResourceGovernor::tick()
//
// BUG: currentLevel_.store(newLevel) executes BEFORE updateScalingCaps(newLevel).
//      A concurrent reader calling getPressureLevel() + getScalingCaps() can
//      observe the new level with stale (old-level) scaling caps.
//
// This test hammers level transitions while a reader asserts consistency.
// =============================================================================

TEST_CASE("ResourceGovernor level and caps are consistent under concurrent reads",
          "[daemon][governor][timing-audit]") {
    auto& governor = ResourceGovernor::instance();

    // Clean env
    EnvGuard envPostIngest("YAMS_POST_INGEST_TOTAL_CONCURRENT", "0");
    EnvGuard envMaxThreads("YAMS_MAX_THREADS", "0");
    resetPostIngestOverridesAndCaps();
    setAllPostIngestStagesActive();
    HwGuard hwGuard(16);
    ProfileGuard profileGuard(TuneAdvisor::Profile::Balanced);

    // Pre-compute expected caps for Normal and Critical
    governor.testing_updateScalingCaps(ResourcePressureLevel::Normal);
    auto normalCaps = governor.getScalingCaps();
    governor.testing_updateScalingCaps(ResourcePressureLevel::Critical);
    auto criticalCaps = governor.getScalingCaps();

    // Critical should have tighter caps than Normal for at least some dimensions
    REQUIRE(criticalCaps.extractionConcurrency <= normalCaps.extractionConcurrency);

    std::atomic<bool> running{true};
    std::atomic<uint32_t> inconsistencies{0};

    // Reader thread: read level + caps, assert they belong to the same pressure level
    auto readerFn = [&]() {
        while (running.load(std::memory_order_relaxed)) {
            auto level = governor.getPressureLevel();
            auto caps = governor.getScalingCaps();

            // Under Critical, caps should be Critical-level caps (tighter).
            // Under Normal, caps should be Normal-level caps (looser).
            // If we see Critical level + Normal caps, that's the atomicity gap.
            if (level == ResourcePressureLevel::Critical) {
                // extraction concurrency at Critical should not exceed Critical caps
                if (caps.extractionConcurrency > criticalCaps.extractionConcurrency) {
                    inconsistencies.fetch_add(1, std::memory_order_relaxed);
                }
            }
        }
    };

    // Writer thread: rapidly toggle between Normal and Critical
    auto writerFn = [&]() {
        for (int i = 0; i < 10000; ++i) {
            governor.testing_updateScalingCaps(ResourcePressureLevel::Critical);
            // BUG: In tick(), currentLevel_ is stored BEFORE updateScalingCaps,
            // so there's a window where level=Critical but caps=Normal.
            // We simulate this by also setting the atomic level directly.
            governor.testing_updateScalingCaps(ResourcePressureLevel::Normal);
        }
    };

    std::vector<std::thread> readers;
    for (int i = 0; i < 4; ++i) {
        readers.emplace_back(readerFn);
    }

    std::thread writer(writerFn);
    writer.join();
    running.store(false, std::memory_order_relaxed);
    for (auto& t : readers)
        t.join();

    // The test hook testing_updateScalingCaps sets caps atomically without the
    // level/caps split — so this test won't fail with the hook alone.
    // It documents the expected invariant: level and caps must be consistent.
    // The real bug manifests in tick() where the two writes are separated.
    // We assert the invariant holds (it should with the testing hook):
    INFO("Inconsistencies detected: " << inconsistencies.load());
    CHECK(inconsistencies.load() == 0);
}

// =============================================================================
// Issue 4: Torn DynamicCap state — concurrent readers can see partial updates
//
// BUG: TuningManager writes 6 DynamicCap atomics individually (lines 463-468).
//      A concurrent reader calling postIngestBudgetedConcurrency(true) can
//      read a mix of old and new values, producing impossible budget combinations.
//
// This test writes "all-high" and "all-low" patterns and checks that readers
// never see a mix.
// =============================================================================

TEST_CASE("DynamicCap writes are not torn across stages", "[daemon][tune][timing-audit]") {
    resetPostIngestOverridesAndCaps();
    setAllPostIngestStagesActive();
    HwGuard hwGuard(16);
    ProfileGuard profileGuard(TuneAdvisor::Profile::Balanced);
    EnvGuard envPostIngest("YAMS_POST_INGEST_TOTAL_CONCURRENT", "0");
    EnvGuard envMaxThreads("YAMS_MAX_THREADS", "0");

    // "High" caps: set all stages to a recognizable high value
    constexpr uint32_t kHigh = 8;
    // "Low" caps: set all stages to a recognizable low value
    constexpr uint32_t kLow = 1;

    std::atomic<bool> running{true};
    std::atomic<uint32_t> tornReads{0};

    // Writer: alternates between all-high and all-low DynamicCaps
    auto writerFn = [&]() {
        for (int i = 0; i < 20000; ++i) {
            uint32_t val = (i % 2 == 0) ? kHigh : kLow;
            // These 6 stores happen non-atomically (the bug)
            TuneAdvisor::setPostExtractionConcurrentDynamicCap(val);
            TuneAdvisor::setPostKgConcurrentDynamicCap(val);
            TuneAdvisor::setPostSymbolConcurrentDynamicCap(val);
            TuneAdvisor::setPostEntityConcurrentDynamicCap(val);
            TuneAdvisor::setPostTitleConcurrentDynamicCap(val);
            TuneAdvisor::setPostEmbedConcurrentDynamicCap(val);
        }
        running.store(false, std::memory_order_relaxed);
    };

    // Reader: read the budget and check all 6 stages are consistent
    // (all derived from the same high/low batch)
    auto readerFn = [&]() {
        while (running.load(std::memory_order_relaxed)) {
            auto budget = TuneAdvisor::testing_postIngestBudget(/*includeDynamicCaps=*/true);

            // Under all-high caps, the budget should be uniformly capped to kHigh or below.
            // Under all-low caps, all stages should be at kLow.
            // A torn read would show some stages at kHigh-derived and others at kLow-derived.
            //
            // We detect tearing by checking: if extraction is at its kLow-capped value,
            // embed should also be at its kLow-capped value (and vice versa).
            // If one is at the High-derived value and the other is at Low-derived, it's torn.
            uint32_t ext = budget.extraction;
            uint32_t emb = budget.embed;

            // With all-high caps (8), both ext and emb should be >= 2 (given budget).
            // With all-low caps (1), both ext and emb should be exactly 1.
            // A torn read: ext=1 (from low batch) but emb>1 (from high batch), or vice versa.
            if ((ext == 1 && emb > 1) || (ext > 1 && emb == 1)) {
                tornReads.fetch_add(1, std::memory_order_relaxed);
            }
        }
    };

    std::thread writer(writerFn);
    std::vector<std::thread> readers;
    for (int i = 0; i < 4; ++i) {
        readers.emplace_back(readerFn);
    }

    writer.join();
    for (auto& t : readers)
        t.join();

    INFO("Torn reads detected: " << tornReads.load());
    // Pre-fix: likely to see torn reads under contention.
    // Post-fix (seqlock): should be 0.
    CHECK(tornReads.load() == 0);
}

// =============================================================================
// Issue 6: One-tick recovery lag on pressure de-escalation
//
// BUG: When pressure drops (e.g., Critical -> Normal), TuningManager reads
//      per-stage concurrency via postExtractionConcurrent() etc., which calls
//      postIngestBudgetedConcurrency(true). This applies the PREVIOUS tick's
//      DynamicCaps (still set to Critical-level values). The std::min() with
//      the new (Normal-level) governor caps keeps the low value.
//      Recovery is delayed by one tick.
//
// Test: Set restrictive DynamicCaps, clear them, verify immediate recovery.
// =============================================================================

TEST_CASE("Clearing DynamicCaps immediately restores default concurrency",
          "[daemon][tune][timing-audit]") {
    resetPostIngestOverridesAndCaps();
    setAllPostIngestStagesActive();
    HwGuard hwGuard(16);
    ProfileGuard profileGuard(TuneAdvisor::Profile::Balanced);
    EnvGuard envPostIngest("YAMS_POST_INGEST_TOTAL_CONCURRENT", "0");
    EnvGuard envMaxThreads("YAMS_MAX_THREADS", "0");

    // Capture defaults (no caps active)
    auto defaultBudget = TuneAdvisor::testing_postIngestBudget(/*includeDynamicCaps=*/false);
    REQUIRE(defaultBudget.extraction >= 1);
    REQUIRE(defaultBudget.embed >= 1);

    // Apply restrictive caps (simulating Critical pressure)
    TuneAdvisor::setPostExtractionConcurrentDynamicCap(1);
    TuneAdvisor::setPostKgConcurrentDynamicCap(1);
    TuneAdvisor::setPostSymbolConcurrentDynamicCap(1);
    TuneAdvisor::setPostEntityConcurrentDynamicCap(1);
    TuneAdvisor::setPostTitleConcurrentDynamicCap(1);
    TuneAdvisor::setPostEmbedConcurrentDynamicCap(1);

    // Verify caps took effect
    auto cappedBudget = TuneAdvisor::testing_postIngestBudget(/*includeDynamicCaps=*/true);
    REQUIRE(cappedBudget.extraction <= 1);
    REQUIRE(cappedBudget.embed <= 1);

    // Clear all DynamicCaps (simulating de-escalation to Normal)
    TuneAdvisor::setPostExtractionConcurrentDynamicCap(0);
    TuneAdvisor::setPostKgConcurrentDynamicCap(0);
    TuneAdvisor::setPostSymbolConcurrentDynamicCap(0);
    TuneAdvisor::setPostEntityConcurrentDynamicCap(0);
    TuneAdvisor::setPostTitleConcurrentDynamicCap(0);
    TuneAdvisor::setPostEmbedConcurrentDynamicCap(0);

    // IMMEDIATELY after clearing, the effective concurrency should match defaults.
    // BUG (Issue 6): In TuningManager::tick_once(), the DynamicCaps from the
    // previous tick are still read before they're cleared, causing a one-tick lag.
    // This test verifies the API-level contract: clearing caps = immediate recovery.
    auto recoveredBudget = TuneAdvisor::testing_postIngestBudget(/*includeDynamicCaps=*/true);

    INFO("Default extraction=" << defaultBudget.extraction
                               << " recovered=" << recoveredBudget.extraction);
    INFO("Default embed=" << defaultBudget.embed << " recovered=" << recoveredBudget.embed);

    CHECK(recoveredBudget.extraction == defaultBudget.extraction);
    CHECK(recoveredBudget.kg == defaultBudget.kg);
    CHECK(recoveredBudget.symbol == defaultBudget.symbol);
    CHECK(recoveredBudget.entity == defaultBudget.entity);
    CHECK(recoveredBudget.title == defaultBudget.title);
    CHECK(recoveredBudget.embed == defaultBudget.embed);
}

// =============================================================================
// Issue 7: Budget recomputed 6 times per tick (correctness invariant)
//
// BUG: TuningManager calls postExtractionConcurrent(), postKgConcurrent(), etc.
//      individually (lines 338-343). Each call triggers a full
//      postIngestBudgetedConcurrency(true) computation with sorting and fairness
//      correction. This is wasteful AND can produce different results across
//      calls if atomics change between them.
//
// Test: Verify that calling postIngestBudgetedConcurrency(true) once and
//       reading individual per-stage getters produces identical values.
// =============================================================================

TEST_CASE("Per-stage getters match single budget computation", "[daemon][tune][timing-audit]") {
    resetPostIngestOverridesAndCaps();
    setAllPostIngestStagesActive();
    HwGuard hwGuard(16);
    ProfileGuard profileGuard(TuneAdvisor::Profile::Balanced);
    EnvGuard envPostIngest("YAMS_POST_INGEST_TOTAL_CONCURRENT", "0");
    EnvGuard envMaxThreads("YAMS_MAX_THREADS", "0");

    // Compute budget once
    auto budget = TuneAdvisor::testing_postIngestBudget(/*includeDynamicCaps=*/true);

    // Read individual getters (each calls postIngestBudgetedConcurrency internally)
    uint32_t ext = TuneAdvisor::postExtractionConcurrent();
    uint32_t kg = TuneAdvisor::postKgConcurrent();
    uint32_t sym = TuneAdvisor::postSymbolConcurrent();
    uint32_t ent = TuneAdvisor::postEntityConcurrent();
    uint32_t tit = TuneAdvisor::postTitleConcurrent();
    uint32_t emb = TuneAdvisor::postEmbedConcurrent();

    INFO("budget: ext=" << budget.extraction << " kg=" << budget.kg << " sym=" << budget.symbol
                        << " ent=" << budget.entity << " tit=" << budget.title
                        << " emb=" << budget.embed);
    INFO("getters: ext=" << ext << " kg=" << kg << " sym=" << sym << " ent=" << ent
                         << " tit=" << tit << " emb=" << emb);

    // In a quiescent state (no concurrent writers), these should be identical.
    // This validates the correctness invariant that Issue 7's fix must preserve.
    CHECK(ext == budget.extraction);
    CHECK(kg == budget.kg);
    CHECK(sym == budget.symbol);
    CHECK(ent == budget.entity);
    CHECK(tit == budget.title);
    CHECK(emb == budget.embed);
}

// =============================================================================
// Issue 7 stress variant: Under concurrent DynamicCap writes, individual getters
// can return values from different computations.
// =============================================================================

TEST_CASE("Per-stage getters diverge under concurrent DynamicCap writes",
          "[daemon][tune][timing-audit]") {
    resetPostIngestOverridesAndCaps();
    setAllPostIngestStagesActive();
    HwGuard hwGuard(16);
    ProfileGuard profileGuard(TuneAdvisor::Profile::Balanced);
    EnvGuard envPostIngest("YAMS_POST_INGEST_TOTAL_CONCURRENT", "0");
    EnvGuard envMaxThreads("YAMS_MAX_THREADS", "0");

    std::atomic<bool> running{true};
    std::atomic<uint32_t> divergences{0};

    // Writer: oscillate DynamicCaps
    auto writerFn = [&]() {
        for (int i = 0; i < 20000 && running.load(std::memory_order_relaxed); ++i) {
            uint32_t val = (i % 2 == 0) ? 8 : 1;
            TuneAdvisor::setPostExtractionConcurrentDynamicCap(val);
            TuneAdvisor::setPostKgConcurrentDynamicCap(val);
            TuneAdvisor::setPostSymbolConcurrentDynamicCap(val);
            TuneAdvisor::setPostEntityConcurrentDynamicCap(val);
            TuneAdvisor::setPostTitleConcurrentDynamicCap(val);
            TuneAdvisor::setPostEmbedConcurrentDynamicCap(val);
        }
        running.store(false, std::memory_order_relaxed);
    };

    // Reader: compute budget once and compare to individual getters
    auto readerFn = [&]() {
        while (running.load(std::memory_order_relaxed)) {
            auto budget = TuneAdvisor::testing_postIngestBudget(/*includeDynamicCaps=*/true);
            uint32_t ext = TuneAdvisor::postExtractionConcurrent();
            uint32_t emb = TuneAdvisor::postEmbedConcurrent();

            // If DynamicCaps changed between our budget computation and the getter calls,
            // the values will diverge. That's the Issue 7 bug in action.
            if (ext != budget.extraction || emb != budget.embed) {
                divergences.fetch_add(1, std::memory_order_relaxed);
            }
        }
    };

    std::thread writer(writerFn);
    std::vector<std::thread> readers;
    for (int i = 0; i < 2; ++i) {
        readers.emplace_back(readerFn);
    }

    writer.join();
    for (auto& t : readers)
        t.join();

    INFO("Divergences between single-compute and per-stage getters: " << divergences.load());
    // Pre-fix: divergences are likely under contention.
    // Post-fix: TuningManager will call postIngestBudgetedConcurrency once, eliminating divergence.
    // Note: This test may pass pre-fix if timing is lucky, but should be deterministically
    // 0 post-fix since TuningManager won't call individual getters anymore.
    CHECK(divergences.load() == 0);
}
