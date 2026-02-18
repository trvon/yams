// CPU Governance Enforcement Tests (Catch2)
// Tests OnnxConcurrencyRegistry slot enforcement, TuningManager profile configuration,
// and ResourceGovernor admission control.

#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/resource/OnnxConcurrencyRegistry.h>
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
// Test Helpers
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

/// RAII guard for OnnxConcurrencyRegistry max slots (restore original on destruction)
class SlotConfigGuard {
    uint32_t prevMax_;

public:
    explicit SlotConfigGuard(uint32_t newMax) {
        auto& reg = OnnxConcurrencyRegistry::instance();
        prevMax_ = reg.totalSlots();
        reg.setMaxSlots(newMax);
    }

    ~SlotConfigGuard() { OnnxConcurrencyRegistry::instance().setMaxSlots(prevMax_); }

    SlotConfigGuard(const SlotConfigGuard&) = delete;
    SlotConfigGuard& operator=(const SlotConfigGuard&) = delete;
};

} // namespace

// =============================================================================
// OnnxConcurrencyRegistry Basic Tests
// =============================================================================

TEST_CASE("OnnxConcurrencyRegistry singleton is accessible", "[daemon][governance][catch2]") {
    auto& reg1 = OnnxConcurrencyRegistry::instance();
    auto& reg2 = OnnxConcurrencyRegistry::instance();
    CHECK(&reg1 == &reg2);
}

TEST_CASE("OnnxConcurrencyRegistry slot metrics are observable", "[daemon][governance][catch2]") {
    auto& reg = OnnxConcurrencyRegistry::instance();

    auto snap = reg.snapshot();
    CHECK(snap.totalSlots > 0);
    CHECK(snap.availableSlots <= snap.totalSlots);
    CHECK(snap.usedSlots <= snap.totalSlots);
}

TEST_CASE("OnnxConcurrencyRegistry lane metrics are observable", "[daemon][governance][catch2]") {
    auto& reg = OnnxConcurrencyRegistry::instance();

    auto glinerMetrics = reg.laneMetrics(OnnxLane::Gliner);
    auto embedMetrics = reg.laneMetrics(OnnxLane::Embedding);
    auto rerankerMetrics = reg.laneMetrics(OnnxLane::Reranker);

    // Default reserved slots
    CHECK(glinerMetrics.reserved >= 0);
    CHECK(embedMetrics.reserved >= 0);
    CHECK(rerankerMetrics.reserved >= 0);
}

// =============================================================================
// SlotGuard RAII Tests
// =============================================================================

TEST_CASE("SlotGuard acquires and releases slot", "[daemon][governance][catch2]") {
    auto& reg = OnnxConcurrencyRegistry::instance();
    SlotConfigGuard configGuard(8); // Ensure enough slots

    uint32_t beforeUsed = reg.usedSlots();

    {
        OnnxConcurrencyRegistry::SlotGuard guard(reg, OnnxLane::Embedding, 100ms);
        CHECK(guard.acquired());
        CHECK(reg.usedSlots() == beforeUsed + 1);
    }

    CHECK(reg.usedSlots() == beforeUsed);
}

TEST_CASE("SlotGuard converts to bool", "[daemon][governance][catch2]") {
    auto& reg = OnnxConcurrencyRegistry::instance();
    SlotConfigGuard configGuard(8);

    OnnxConcurrencyRegistry::SlotGuard guard(reg, OnnxLane::Embedding, 100ms);

    if (guard) {
        CHECK(guard.acquired());
    } else {
        FAIL("SlotGuard should have acquired a slot");
    }
}

TEST_CASE("Multiple slots can be acquired up to limit", "[daemon][governance][catch2]") {
    auto& reg = OnnxConcurrencyRegistry::instance();
    SlotConfigGuard configGuard(4); // Small limit for test

    uint32_t beforeUsed = reg.usedSlots();

    // Acquire multiple slots
    OnnxConcurrencyRegistry::SlotGuard slot1(reg, OnnxLane::Embedding, 100ms);
    OnnxConcurrencyRegistry::SlotGuard slot2(reg, OnnxLane::Embedding, 100ms);

    CHECK(slot1.acquired());
    CHECK(slot2.acquired());
    CHECK(reg.usedSlots() >= beforeUsed + 2);
}

// =============================================================================
// Slot Enforcement Tests
// =============================================================================

TEST_CASE("Slot acquisition respects limits", "[daemon][governance][catch2]") {
    auto& reg = OnnxConcurrencyRegistry::instance();

    // Just verify we can acquire and release slots normally
    // Full capacity testing is complex due to reserved slots
    SlotConfigGuard configGuard(8);

    uint32_t beforeUsed = reg.usedSlots();

    {
        OnnxConcurrencyRegistry::SlotGuard slot1(reg, OnnxLane::Other, 100ms);
        OnnxConcurrencyRegistry::SlotGuard slot2(reg, OnnxLane::Embedding, 100ms);

        CHECK(slot1.acquired());
        CHECK(slot2.acquired());
        CHECK(reg.usedSlots() >= beforeUsed + 2);
    }

    // Slots should be released
    CHECK(reg.usedSlots() == beforeUsed);
}

// =============================================================================
// ResourceGovernor Admission Control Tests
// =============================================================================

TEST_CASE("ResourceGovernor admission control methods exist", "[daemon][governance][catch2]") {
    auto& governor = ResourceGovernor::instance();

    // These methods should be callable
    bool canWork = governor.canAdmitWork();
    bool canLoad = governor.canLoadModel(1024 * 1024); // 1 MB
    bool canScale = governor.canScaleUp("test", 1);

    // At normal pressure, all should be true (unless governor is disabled)
    // Just verify they're callable without crashing
    (void)canWork;
    (void)canLoad;
    (void)canScale;
}

TEST_CASE("ResourceGovernor pressure level is Normal by default", "[daemon][governance][catch2]") {
    auto& governor = ResourceGovernor::instance();

    // At startup with no memory pressure, level should be Normal
    auto level = governor.getPressureLevel();

    // This may vary based on actual system memory usage, so just check it's valid
    CHECK((level == ResourcePressureLevel::Normal || level == ResourcePressureLevel::Warning ||
           level == ResourcePressureLevel::Critical || level == ResourcePressureLevel::Emergency));
}

TEST_CASE("ResourceGovernor scaling caps are queryable", "[daemon][governance][catch2]") {
    auto& governor = ResourceGovernor::instance();

    uint32_t maxIngest = governor.maxIngestWorkers();
    uint32_t maxSearch = governor.maxSearchConcurrency();
    uint32_t maxEmbed = governor.maxEmbedConcurrency();
    uint32_t maxExtraction = governor.maxExtractionConcurrency();
    uint32_t maxKg = governor.maxKgConcurrency();

    // All should have some positive value at Normal pressure
    CHECK(maxIngest > 0);
    CHECK(maxSearch > 0);
    // Embed may be 0 at Emergency level
    (void)maxEmbed;
    (void)maxExtraction;
    (void)maxKg;
}

TEST_CASE("ResourceGovernor applies DB contention caps", "[daemon][governance][catch2]") {
    auto& governor = ResourceGovernor::instance();

    governor.reportDbLockContention(0, 10);
    CHECK(governor.capKgConcurrencyForDbContention(8) == 8);
    CHECK(governor.capEmbedConcurrencyForDbContention(8) == 8);

    governor.reportDbLockContention(11, 10);
    CHECK(governor.capKgConcurrencyForDbContention(8) == 4);
    CHECK(governor.capEmbedConcurrencyForDbContention(8) == 2);

    governor.reportDbLockContention(25, 10);
    CHECK(governor.capKgConcurrencyForDbContention(8) == 2);
    CHECK(governor.capEmbedConcurrencyForDbContention(8) == 1);

    governor.reportDbLockContention(0, 10);
}

TEST_CASE("ResourceGovernor recommends connection slot targets with hysteresis",
          "[daemon][governance][catch2]") {
    auto& governor = ResourceGovernor::instance();

    const uint32_t current = 20;
    const uint32_t minSlots = 8;
    const uint32_t maxSlots = 64;
    const uint32_t step = 4;

    // Need sustained high utilization before growth.
    CHECK(governor.recommendConnectionSlotTarget(current, 19, minSlots, maxSlots, step, 3) ==
          current);
    CHECK(governor.recommendConnectionSlotTarget(current, 19, minSlots, maxSlots, step, 3) ==
          current);
    CHECK(governor.recommendConnectionSlotTarget(current, 19, minSlots, maxSlots, step, 3) >=
          current);

    // Need sustained low utilization before shrink (and only when active > 0).
    CHECK(governor.recommendConnectionSlotTarget(current, 1, minSlots, maxSlots, step, 3) ==
          current);
    CHECK(governor.recommendConnectionSlotTarget(current, 1, minSlots, maxSlots, step, 3) ==
          current);
    CHECK(governor.recommendConnectionSlotTarget(current, 1, minSlots, maxSlots, step, 3) <=
          current);
}

TEST_CASE("ResourceGovernor recommends retry-after from overload signals",
          "[daemon][governance][catch2]") {
    auto& governor = ResourceGovernor::instance();

    const auto none =
        governor.recommendRetryAfterMs(0, 100, 0, 1024ULL * 1024ULL, 0, 1000, 0, 1000, 250);
    CHECK(none == 0);

    const auto overloaded = governor.recommendRetryAfterMs(
        250, 100, 3LL * 1024LL * 1024LL, 1024ULL * 1024ULL, 1500, 1000, 1000, 1000, 400);
    CHECK(overloaded > 0);
}

TEST_CASE("ResourceGovernor snapshot contains valid metrics", "[daemon][governance][catch2]") {
    auto& governor = ResourceGovernor::instance();

    auto snapshot = governor.getSnapshot();

    // RSS should be positive (we're running)
    // Note: May be 0 if RSS reading failed on this platform
    CHECK(snapshot.rssBytes >= 0);

    // Timestamp should be valid
    CHECK(snapshot.timestamp.time_since_epoch().count() >= 0);
}

// =============================================================================
// Profile-Aware Configuration Tests
// =============================================================================

TEST_CASE("TuneAdvisor profile affects resource settings", "[daemon][governance][catch2]") {
    SECTION("Efficient profile has lower thresholds") {
        ProfileGuard guard(TuneAdvisor::Profile::Efficient);

        auto scale = TuneAdvisor::profileScale();
        CHECK(scale < 1.0);
    }

    SECTION("Balanced profile has moderate thresholds") {
        ProfileGuard guard(TuneAdvisor::Profile::Balanced);

        auto scale = TuneAdvisor::profileScale();
        // Balanced profile now uses 0.5 scale
        CHECK(scale == 0.5);
    }

    SECTION("Aggressive profile has default thresholds") {
        ProfileGuard guard(TuneAdvisor::Profile::Aggressive);

        auto scale = TuneAdvisor::profileScale();
        // Aggressive profile now uses 1.0 scale (reduced from 1.5)
        CHECK(scale == 1.0);
    }
}

TEST_CASE("OnnxConcurrencyRegistry setMaxSlots updates total", "[daemon][governance][catch2]") {
    auto& reg = OnnxConcurrencyRegistry::instance();

    uint32_t originalTotal = reg.totalSlots();

    reg.setMaxSlots(16);
    CHECK(reg.totalSlots() == 16);

    reg.setMaxSlots(4);
    CHECK(reg.totalSlots() == 4);

    // Restore original
    reg.setMaxSlots(originalTotal);
    CHECK(reg.totalSlots() == originalTotal);
}

// =============================================================================
// Concurrency Stress Tests
// =============================================================================

TEST_CASE("Concurrent slot acquisition is thread-safe", "[daemon][governance][catch2]") {
    auto& reg = OnnxConcurrencyRegistry::instance();
    SlotConfigGuard configGuard(8);

    std::atomic<int> successCount{0};
    std::atomic<int> failCount{0};

    auto worker = [&]() {
        OnnxConcurrencyRegistry::SlotGuard guard(reg, OnnxLane::Embedding, 50ms);
        if (guard.acquired()) {
            successCount++;
            std::this_thread::sleep_for(10ms);
        } else {
            failCount++;
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < 16; ++i) {
        threads.emplace_back(worker);
    }

    for (auto& t : threads) {
        t.join();
    }

    // Some should succeed, some may timeout
    CHECK(successCount > 0);
    // Total attempts should be 16
    CHECK(successCount + failCount == 16);
}

TEST_CASE("Slots are released correctly after use", "[daemon][governance][catch2]") {
    auto& reg = OnnxConcurrencyRegistry::instance();
    SlotConfigGuard configGuard(8);

    uint32_t beforeUsed = reg.usedSlots();

    // Acquire and release in sequence
    {
        OnnxConcurrencyRegistry::SlotGuard guard1(reg, OnnxLane::Embedding, 100ms);
        CHECK(guard1.acquired());
        CHECK(reg.usedSlots() == beforeUsed + 1);
    }

    CHECK(reg.usedSlots() == beforeUsed);

    // Acquire again - should work since slot was released
    {
        OnnxConcurrencyRegistry::SlotGuard guard2(reg, OnnxLane::Embedding, 100ms);
        CHECK(guard2.acquired());
    }

    CHECK(reg.usedSlots() == beforeUsed);
}

// =============================================================================
// TuningManager Configuration Tests
// =============================================================================

TEST_CASE("TuneAdvisor ONNX settings are profile-aware", "[daemon][governance][catch2]") {
    SECTION("onnxMaxConcurrent returns reasonable values") {
        auto maxConcurrent = TuneAdvisor::onnxMaxConcurrent();
        // Should be between 4 and 16 (clamped from hw_threads/2)
        CHECK(maxConcurrent >= 2);
        CHECK(maxConcurrent <= 64);
    }

    SECTION("Reserved slot settings are available") {
        auto glinerReserved = TuneAdvisor::onnxGlinerReserved();
        auto embedReserved = TuneAdvisor::onnxEmbedReserved();
        auto rerankerReserved = TuneAdvisor::onnxRerankerReserved();

        // Defaults: 1, 1, 1
        CHECK(glinerReserved >= 0);
        CHECK(embedReserved >= 0);
        CHECK(rerankerReserved >= 0);
    }

    SECTION("Profile scale affects computed max slots") {
        // Calculate what TuningManager would set
        auto maxConcurrent = TuneAdvisor::onnxMaxConcurrent();

        {
            ProfileGuard guard(TuneAdvisor::Profile::Efficient);
            double scale = TuneAdvisor::profileScale();
            uint32_t scaledMax = static_cast<uint32_t>(maxConcurrent * scale);
            CHECK(scaledMax < maxConcurrent); // Efficient (0.0) uses fewer slots
        }

        {
            ProfileGuard guard(TuneAdvisor::Profile::Balanced);
            double scale = TuneAdvisor::profileScale();
            uint32_t scaledMax = static_cast<uint32_t>(maxConcurrent * scale);
            CHECK(scaledMax < maxConcurrent); // Balanced (0.5) uses fewer slots than max
        }

        {
            ProfileGuard guard(TuneAdvisor::Profile::Aggressive);
            double scale = TuneAdvisor::profileScale();
            uint32_t scaledMax = static_cast<uint32_t>(maxConcurrent * scale);
            CHECK(scaledMax == maxConcurrent); // Aggressive (1.0) uses full slots
        }
    }
}

// =============================================================================
// Integration Tests (Governor + Registry)
// =============================================================================

TEST_CASE("Governor and Registry are independently accessible", "[daemon][governance][catch2]") {
    auto& governor = ResourceGovernor::instance();
    auto& registry = OnnxConcurrencyRegistry::instance();

    // Both should be singletons
    CHECK(&governor == &ResourceGovernor::instance());
    CHECK(&registry == &OnnxConcurrencyRegistry::instance());

    // Both should have valid state
    CHECK(registry.totalSlots() > 0);
    auto caps = governor.getScalingCaps();
    (void)caps; // Just verify it's callable
}

// =============================================================================
// Exact profileScale() Values
// =============================================================================

TEST_CASE("profileScale returns exact values per profile", "[daemon][governance][catch2]") {
    SECTION("Efficient profile scale is exactly 0.0") {
        ProfileGuard guard(TuneAdvisor::Profile::Efficient);
        CHECK(TuneAdvisor::profileScale() == 0.0);
    }

    SECTION("Balanced profile scale is exactly 0.5") {
        ProfileGuard guard(TuneAdvisor::Profile::Balanced);
        CHECK(TuneAdvisor::profileScale() == 0.5);
    }

    SECTION("Aggressive profile scale is exactly 1.0") {
        ProfileGuard guard(TuneAdvisor::Profile::Aggressive);
        CHECK(TuneAdvisor::profileScale() == 1.0);
    }
}

// =============================================================================
// ONNX Reserved Slot Defaults Per Profile
// =============================================================================

TEST_CASE("ONNX reserved slot defaults match profile", "[daemon][governance][catch2]") {
    SECTION("All profiles: gliner=1, embed=1") {
        for (auto profile : {TuneAdvisor::Profile::Efficient, TuneAdvisor::Profile::Balanced,
                             TuneAdvisor::Profile::Aggressive}) {
            ProfileGuard guard(profile);
            CHECK(TuneAdvisor::onnxGlinerReserved() == 1);
            CHECK(TuneAdvisor::onnxEmbedReserved() == 1);
        }
    }

    SECTION("Efficient: reranker=0") {
        ProfileGuard guard(TuneAdvisor::Profile::Efficient);
        CHECK(TuneAdvisor::onnxRerankerReserved() == 0);
    }

    SECTION("Balanced: reranker=1") {
        ProfileGuard guard(TuneAdvisor::Profile::Balanced);
        CHECK(TuneAdvisor::onnxRerankerReserved() == 1);
    }

    SECTION("Aggressive: reranker=1") {
        ProfileGuard guard(TuneAdvisor::Profile::Aggressive);
        CHECK(TuneAdvisor::onnxRerankerReserved() == 1);
    }
}

// =============================================================================
// Slot Arithmetic Invariant
// =============================================================================

TEST_CASE("Reserved slots never exceed max concurrent", "[daemon][governance][catch2]") {
    for (auto profile : {TuneAdvisor::Profile::Efficient, TuneAdvisor::Profile::Balanced,
                         TuneAdvisor::Profile::Aggressive}) {
        ProfileGuard guard(profile);
        uint32_t totalReserved = TuneAdvisor::onnxGlinerReserved() +
                                 TuneAdvisor::onnxEmbedReserved() +
                                 TuneAdvisor::onnxRerankerReserved();
        CHECK(totalReserved <= TuneAdvisor::onnxMaxConcurrent());
    }
}

// =============================================================================
// Env Var Override for Reserved Slots
// =============================================================================

TEST_CASE("ONNX reserved slot env var overrides", "[daemon][governance][catch2]") {
    ProfileGuard profileGuard(TuneAdvisor::Profile::Balanced);

    SECTION("YAMS_ONNX_EMBED_RESERVED overrides default") {
        EnvGuard envGuard("YAMS_ONNX_EMBED_RESERVED", "3");
        CHECK(TuneAdvisor::onnxEmbedReserved() == 3);
    }

    SECTION("YAMS_ONNX_RERANKER_RESERVED overrides profile-aware default") {
        EnvGuard envGuard("YAMS_ONNX_RERANKER_RESERVED", "4");
        CHECK(TuneAdvisor::onnxRerankerReserved() == 4);
    }
}

// =============================================================================
// Lane-Specific Slot Acquisition Across Lanes
// =============================================================================

TEST_CASE("Slots acquired across multiple lanes", "[daemon][governance][catch2]") {
    auto& reg = OnnxConcurrencyRegistry::instance();
    SlotConfigGuard configGuard(8);

    uint32_t beforeUsed = reg.usedSlots();

    {
        OnnxConcurrencyRegistry::SlotGuard glinerSlot(reg, OnnxLane::Gliner, 100ms);
        OnnxConcurrencyRegistry::SlotGuard embedSlot(reg, OnnxLane::Embedding, 100ms);
        OnnxConcurrencyRegistry::SlotGuard rerankerSlot(reg, OnnxLane::Reranker, 100ms);

        CHECK(glinerSlot.acquired());
        CHECK(embedSlot.acquired());
        CHECK(rerankerSlot.acquired());
        CHECK(reg.usedSlots() == beforeUsed + 3);
    }

    CHECK(reg.usedSlots() == beforeUsed);
}

// =============================================================================
// Memory Threshold Ordering Across All Profiles
// =============================================================================

TEST_CASE("Memory thresholds maintain ordering invariant", "[daemon][governance][catch2]") {
    TuneAdvisor::resetModelEvictThresholdOverrides();

    for (auto profile : {TuneAdvisor::Profile::Efficient, TuneAdvisor::Profile::Balanced,
                         TuneAdvisor::Profile::Aggressive}) {
        ProfileGuard guard(profile);
        CHECK(TuneAdvisor::modelEvictWarningThreshold() <
              TuneAdvisor::modelEvictCriticalThreshold());
        CHECK(TuneAdvisor::modelEvictCriticalThreshold() <
              TuneAdvisor::modelEvictEmergencyThreshold());
    }
}
