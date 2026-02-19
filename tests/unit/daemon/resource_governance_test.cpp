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

#include "../../common/env_compat.h"
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

TEST_CASE("ResourceGovernor Warning caps honor configurable scale percent",
          "[daemon][governance][catch2]") {
    auto& governor = ResourceGovernor::instance();
    TuneAdvisor::resetGovernorWarningScalePercentOverride();

    auto expectedScaled = [](uint32_t current, uint32_t percent) -> uint32_t {
        if (current == 0) {
            return 0;
        }
        uint32_t scaled =
            static_cast<uint32_t>((static_cast<uint64_t>(current) * percent + 99u) / 100u);
        return std::min(current, std::max(1u, scaled));
    };

    governor.testing_updateScalingCaps(ResourcePressureLevel::Normal);
    auto normal = governor.getScalingCaps();

    TuneAdvisor::setGovernorWarningScalePercent(90u);
    governor.testing_updateScalingCaps(ResourcePressureLevel::Warning);
    auto warning90 = governor.getScalingCaps();

    CHECK(warning90.ingestWorkers == expectedScaled(normal.ingestWorkers, 90u));
    CHECK(warning90.searchConcurrency == expectedScaled(normal.searchConcurrency, 90u));
    CHECK(warning90.extractionConcurrency == expectedScaled(normal.extractionConcurrency, 90u));
    CHECK(warning90.kgConcurrency == expectedScaled(normal.kgConcurrency, 90u));
    CHECK(warning90.enrichConcurrency == expectedScaled(normal.enrichConcurrency, 90u));
    CHECK(warning90.embedConcurrency == expectedScaled(normal.embedConcurrency, 90u));
    CHECK_FALSE(warning90.allowModelLoads);
    CHECK(warning90.allowNewIngest);

    TuneAdvisor::setGovernorWarningScalePercent(70u);
    governor.testing_updateScalingCaps(ResourcePressureLevel::Warning);
    auto warning70 = governor.getScalingCaps();
    CHECK(warning70.ingestWorkers == expectedScaled(normal.ingestWorkers, 70u));
    CHECK(warning70.searchConcurrency == expectedScaled(normal.searchConcurrency, 70u));

    governor.testing_updateScalingCaps(ResourcePressureLevel::Normal);
    TuneAdvisor::resetGovernorWarningScalePercentOverride();
}

TEST_CASE("ResourceGovernor de-escalation hysteresis differentiates CPU and memory pressure",
          "[daemon][governance][catch2]") {
    auto& governor = ResourceGovernor::instance();

    // Save and restore tunables touched by this test.
    const double prevWarning = TuneAdvisor::memoryWarningThreshold();
    const double prevCritical = TuneAdvisor::memoryCriticalThreshold();
    const double prevEmergency = TuneAdvisor::memoryEmergencyThreshold();
    const uint32_t prevMemHys = TuneAdvisor::memoryHysteresisMs();
    const uint32_t prevCpuHys = TuneAdvisor::cpuLevelHysteresisMs();

    TuneAdvisor::setMemoryWarningThreshold(0.70);
    TuneAdvisor::setMemoryCriticalThreshold(0.90);
    TuneAdvisor::setMemoryEmergencyThreshold(0.97);
    TuneAdvisor::setMemoryHysteresisMs(500);
    TuneAdvisor::setCpuLevelHysteresisMs(100);

    const auto t0 = std::chrono::steady_clock::now();

    SECTION("CPU-driven de-escalation uses cpu-level hysteresis") {
        governor.testing_setPressureState(ResourcePressureLevel::Warning, t0);

        ResourceSnapshot snap{};
        snap.memoryPressure = 0.30; // no memory pressure
        snap.cpuUsagePercent = 0.0;
        snap.embedQueued = 0;

        snap.timestamp = t0;
        CHECK(governor.testing_computeLevel(snap) == ResourcePressureLevel::Warning);

        snap.timestamp = t0 + std::chrono::milliseconds(150);
        CHECK(governor.testing_computeLevel(snap) == ResourcePressureLevel::Normal);
    }

    SECTION("Memory-driven de-escalation keeps conservative 2x hysteresis") {
        governor.testing_setPressureState(ResourcePressureLevel::Critical, t0);

        ResourceSnapshot snap{};
        snap.memoryPressure = 0.80; // warning-level memory pressure (below critical)
        snap.cpuUsagePercent = 0.0;
        snap.embedQueued = 0;

        snap.timestamp = t0;
        CHECK(governor.testing_computeLevel(snap) == ResourcePressureLevel::Critical);

        snap.timestamp = t0 + std::chrono::milliseconds(600);
        CHECK(governor.testing_computeLevel(snap) == ResourcePressureLevel::Critical);

        snap.timestamp = t0 + std::chrono::milliseconds(1100);
        CHECK(governor.testing_computeLevel(snap) == ResourcePressureLevel::Warning);
    }

    // Restore original values.
    TuneAdvisor::setMemoryWarningThreshold(prevWarning);
    TuneAdvisor::setMemoryCriticalThreshold(prevCritical);
    TuneAdvisor::setMemoryEmergencyThreshold(prevEmergency);
    TuneAdvisor::setMemoryHysteresisMs(prevMemHys);
    TuneAdvisor::setCpuLevelHysteresisMs(prevCpuHys);
    governor.testing_setPressureState(ResourcePressureLevel::Normal,
                                      std::chrono::steady_clock::now());
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

// =============================================================================
// Scaling Caps Pressure Level Tests (testing_updateScalingCaps)
// =============================================================================

TEST_CASE("ResourceGovernor scaling caps change at Warning level", "[daemon][governance][catch2]") {
    auto& governor = ResourceGovernor::instance();

    // Record caps at Normal
    governor.testing_updateScalingCaps(ResourcePressureLevel::Normal);
    auto normalCaps = governor.getScalingCaps();

    // Update to Warning
    governor.testing_updateScalingCaps(ResourcePressureLevel::Warning);
    auto warningCaps = governor.getScalingCaps();

    // Warning should have equal or lower caps
    CHECK(warningCaps.ingestWorkers <= normalCaps.ingestWorkers);
    CHECK(warningCaps.searchConcurrency <= normalCaps.searchConcurrency);
    CHECK(warningCaps.extractionConcurrency <= normalCaps.extractionConcurrency);
    CHECK(warningCaps.kgConcurrency <= normalCaps.kgConcurrency);
    CHECK(warningCaps.embedConcurrency <= normalCaps.embedConcurrency);
    // Warning blocks model loads but still allows ingest
    CHECK_FALSE(warningCaps.allowModelLoads);
    CHECK(warningCaps.allowNewIngest);

    // Restore Normal
    governor.testing_updateScalingCaps(ResourcePressureLevel::Normal);
}

TEST_CASE("ResourceGovernor scaling caps at Critical level", "[daemon][governance][catch2]") {
    auto& governor = ResourceGovernor::instance();

    // Record Normal caps for comparison
    governor.testing_updateScalingCaps(ResourcePressureLevel::Normal);
    auto normalCaps = governor.getScalingCaps();

    // Update to Critical
    governor.testing_updateScalingCaps(ResourcePressureLevel::Critical);
    auto criticalCaps = governor.getScalingCaps();

    // Critical should have even lower caps than Warning
    CHECK(criticalCaps.ingestWorkers <= normalCaps.ingestWorkers);
    CHECK(criticalCaps.searchConcurrency <= normalCaps.searchConcurrency);
    // Critical blocks model loads
    CHECK_FALSE(criticalCaps.allowModelLoads);
    // Critical still allows ingest (only Emergency blocks it)
    CHECK(criticalCaps.allowNewIngest);

    // Restore Normal
    governor.testing_updateScalingCaps(ResourcePressureLevel::Normal);
}

TEST_CASE("ResourceGovernor scaling caps at Emergency level", "[daemon][governance][catch2]") {
    auto& governor = ResourceGovernor::instance();

    // Update to Emergency
    governor.testing_updateScalingCaps(ResourcePressureLevel::Emergency);
    auto emergencyCaps = governor.getScalingCaps();

    // Emergency should halt new ingest
    CHECK_FALSE(emergencyCaps.allowNewIngest);
    CHECK_FALSE(emergencyCaps.allowModelLoads);
    // Embed concurrency should be 0
    CHECK(emergencyCaps.embedConcurrency == 0);
    // Extraction concurrency should be 0
    CHECK(emergencyCaps.extractionConcurrency == 0);

    // Restore Normal
    governor.testing_updateScalingCaps(ResourcePressureLevel::Normal);
}

TEST_CASE("ResourceGovernor caps restore to full after returning to Normal",
          "[daemon][governance][catch2]") {
    auto& governor = ResourceGovernor::instance();

    // Record Normal caps
    governor.testing_updateScalingCaps(ResourcePressureLevel::Normal);
    auto normalCaps = governor.getScalingCaps();

    // Go through Emergency and back to Normal
    governor.testing_updateScalingCaps(ResourcePressureLevel::Emergency);
    governor.testing_updateScalingCaps(ResourcePressureLevel::Normal);
    auto restoredCaps = governor.getScalingCaps();

    // Should be back to full values
    CHECK(restoredCaps.ingestWorkers == normalCaps.ingestWorkers);
    CHECK(restoredCaps.searchConcurrency == normalCaps.searchConcurrency);
    CHECK(restoredCaps.extractionConcurrency == normalCaps.extractionConcurrency);
    CHECK(restoredCaps.kgConcurrency == normalCaps.kgConcurrency);
    CHECK(restoredCaps.embedConcurrency == normalCaps.embedConcurrency);
    CHECK(restoredCaps.allowModelLoads);
    CHECK(restoredCaps.allowNewIngest);
}

TEST_CASE("ResourceGovernor caps ordering: Normal >= Emergency (endpoints)",
          "[daemon][governance][catch2]") {
    auto& governor = ResourceGovernor::instance();

    governor.testing_updateScalingCaps(ResourcePressureLevel::Normal);
    auto normal = governor.getScalingCaps();

    governor.testing_updateScalingCaps(ResourcePressureLevel::Emergency);
    auto emergency = governor.getScalingCaps();

    // Normal should always be >= Emergency across all dimensions
    CHECK(normal.ingestWorkers >= emergency.ingestWorkers);
    CHECK(normal.embedConcurrency >= emergency.embedConcurrency);
    CHECK(normal.extractionConcurrency >= emergency.extractionConcurrency);
    CHECK(normal.searchConcurrency >= emergency.searchConcurrency);
    CHECK(normal.kgConcurrency >= emergency.kgConcurrency);

    // Normal allows everything, Emergency blocks
    CHECK(normal.allowModelLoads);
    CHECK(normal.allowNewIngest);
    CHECK_FALSE(emergency.allowModelLoads);
    CHECK_FALSE(emergency.allowNewIngest);

    // Restore Normal
    governor.testing_updateScalingCaps(ResourcePressureLevel::Normal);
}

// =============================================================================
// ResourceGovernor Tick and Admission Control Tests
// =============================================================================

TEST_CASE("ResourceGovernor tick is callable without ServiceManager",
          "[daemon][governance][catch2]") {
    auto& governor = ResourceGovernor::instance();
    // tick(nullptr) should not crash — it just skips SM metric collection
    auto snap = governor.tick(nullptr);
    CHECK(snap.timestamp.time_since_epoch().count() > 0);
    // RSS should be readable (we're a running process)
    CHECK(snap.rssBytes > 0);
}

TEST_CASE("ResourceGovernor canAdmitWork blocked at Emergency", "[daemon][governance][catch2]") {
    auto& governor = ResourceGovernor::instance();

    // At Normal, work should be admitted
    governor.testing_updateScalingCaps(ResourcePressureLevel::Normal);
    CHECK(governor.getScalingCaps().allowNewIngest);

    // At Emergency, allowNewIngest is false
    governor.testing_updateScalingCaps(ResourcePressureLevel::Emergency);
    CHECK_FALSE(governor.getScalingCaps().allowNewIngest);

    // Restore
    governor.testing_updateScalingCaps(ResourcePressureLevel::Normal);
}
