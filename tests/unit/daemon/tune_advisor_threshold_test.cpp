// TuneAdvisor model eviction threshold tests (Catch2)
// Tests profile defaults, env var overrides, programmatic setters, and validation

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/resource/gpu_info.h>

#include <cmath>
#include <cstdlib>
#include <limits>
#include <string>

#include "../../common/env_compat.h"
#include <yams/compat/unistd.h>

using namespace yams::daemon;

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

/// Reset all eviction threshold overrides to default (unset)
void resetEvictionOverrides() {
    TuneAdvisor::resetModelEvictThresholdOverrides();
}

} // namespace

// =============================================================================
// Profile Default Tests
// =============================================================================

TEST_CASE("Model eviction threshold profile defaults", "[daemon][tune][advisor][catch2]") {
    resetEvictionOverrides();

    SECTION("Efficient profile thresholds") {
        ProfileGuard guard(TuneAdvisor::Profile::Efficient);

        CHECK(TuneAdvisor::modelEvictWarningThreshold() == Catch::Approx(0.30));
        CHECK(TuneAdvisor::modelEvictCriticalThreshold() == Catch::Approx(0.50));
        CHECK(TuneAdvisor::modelEvictEmergencyThreshold() == Catch::Approx(0.70));
    }

    SECTION("Balanced profile thresholds") {
        ProfileGuard guard(TuneAdvisor::Profile::Balanced);

        CHECK(TuneAdvisor::modelEvictWarningThreshold() == Catch::Approx(0.60));
        CHECK(TuneAdvisor::modelEvictCriticalThreshold() == Catch::Approx(0.75));
        CHECK(TuneAdvisor::modelEvictEmergencyThreshold() == Catch::Approx(0.90));
    }

    SECTION("Aggressive profile thresholds") {
        ProfileGuard guard(TuneAdvisor::Profile::Aggressive);

        CHECK(TuneAdvisor::modelEvictWarningThreshold() == Catch::Approx(0.75));
        CHECK(TuneAdvisor::modelEvictCriticalThreshold() == Catch::Approx(0.85));
        CHECK(TuneAdvisor::modelEvictEmergencyThreshold() == Catch::Approx(0.95));
    }
}

// =============================================================================
// Environment Variable Override Tests
// =============================================================================

TEST_CASE("Model eviction threshold env var overrides", "[daemon][tune][advisor][catch2]") {
    resetEvictionOverrides();
    ProfileGuard profileGuard(TuneAdvisor::Profile::Balanced);

    SECTION("Valid warning threshold env var overrides profile default") {
        EnvGuard envGuard("YAMS_MODEL_EVICT_WARNING_THRESHOLD", "0.45");

        CHECK(TuneAdvisor::modelEvictWarningThreshold() == Catch::Approx(0.45));
    }

    SECTION("Valid critical threshold env var overrides profile default") {
        EnvGuard envGuard("YAMS_MODEL_EVICT_CRITICAL_THRESHOLD", "0.80");

        CHECK(TuneAdvisor::modelEvictCriticalThreshold() == Catch::Approx(0.80));
    }

    SECTION("Valid emergency threshold env var overrides profile default") {
        EnvGuard envGuard("YAMS_MODEL_EVICT_EMERGENCY_THRESHOLD", "0.92");

        CHECK(TuneAdvisor::modelEvictEmergencyThreshold() == Catch::Approx(0.92));
    }

    SECTION("Invalid env var (non-numeric) falls back to profile default") {
        EnvGuard envGuard("YAMS_MODEL_EVICT_WARNING_THRESHOLD", "invalid");

        // Should return profile default (0.60 for Balanced), not crash
        CHECK(TuneAdvisor::modelEvictWarningThreshold() == Catch::Approx(0.60));
    }

    SECTION("Env var exactly 1.0 is rejected (must be < 1.0)") {
        EnvGuard envGuard("YAMS_MODEL_EVICT_WARNING_THRESHOLD", "1.0");

        // 1.0 should be rejected, falls back to profile default
        CHECK(TuneAdvisor::modelEvictWarningThreshold() == Catch::Approx(0.60));
    }

    SECTION("Env var exactly 0.0 is rejected (must be > 0.0)") {
        EnvGuard envGuard("YAMS_MODEL_EVICT_WARNING_THRESHOLD", "0.0");

        // 0.0 should be rejected, falls back to profile default
        CHECK(TuneAdvisor::modelEvictWarningThreshold() == Catch::Approx(0.60));
    }

    SECTION("Negative env var is rejected") {
        EnvGuard envGuard("YAMS_MODEL_EVICT_WARNING_THRESHOLD", "-0.5");

        CHECK(TuneAdvisor::modelEvictWarningThreshold() == Catch::Approx(0.60));
    }

    SECTION("Env var > 1.0 is rejected") {
        EnvGuard envGuard("YAMS_MODEL_EVICT_WARNING_THRESHOLD", "1.5");

        CHECK(TuneAdvisor::modelEvictWarningThreshold() == Catch::Approx(0.60));
    }
}

// =============================================================================
// Programmatic Setter Tests
// =============================================================================

TEST_CASE("Model eviction threshold setter validation", "[daemon][tune][advisor][catch2]") {
    resetEvictionOverrides();
    ProfileGuard profileGuard(TuneAdvisor::Profile::Balanced);

    SECTION("Valid values are accepted") {
        TuneAdvisor::setModelEvictWarningThreshold(0.5);
        CHECK(TuneAdvisor::modelEvictWarningThreshold() == Catch::Approx(0.5));

        TuneAdvisor::setModelEvictCriticalThreshold(0.7);
        CHECK(TuneAdvisor::modelEvictCriticalThreshold() == Catch::Approx(0.7));

        TuneAdvisor::setModelEvictEmergencyThreshold(0.85);
        CHECK(TuneAdvisor::modelEvictEmergencyThreshold() == Catch::Approx(0.85));
    }

    SECTION("Boundary values near 0 and 1 are handled correctly") {
        // Just above 0 should be accepted
        TuneAdvisor::setModelEvictWarningThreshold(0.001);
        CHECK(TuneAdvisor::modelEvictWarningThreshold() == Catch::Approx(0.001));

        // Just below 1 should be accepted
        TuneAdvisor::setModelEvictCriticalThreshold(0.999);
        CHECK(TuneAdvisor::modelEvictCriticalThreshold() == Catch::Approx(0.999));
    }

    SECTION("Negative values are rejected") {
        double prev = TuneAdvisor::modelEvictWarningThreshold();
        TuneAdvisor::setModelEvictWarningThreshold(-0.5);
        CHECK(TuneAdvisor::modelEvictWarningThreshold() == Catch::Approx(prev));
    }

    SECTION("Zero is rejected") {
        double prev = TuneAdvisor::modelEvictCriticalThreshold();
        TuneAdvisor::setModelEvictCriticalThreshold(0.0);
        CHECK(TuneAdvisor::modelEvictCriticalThreshold() == Catch::Approx(prev));
    }

    SECTION("Values >= 1.0 are rejected") {
        double prev = TuneAdvisor::modelEvictEmergencyThreshold();

        TuneAdvisor::setModelEvictEmergencyThreshold(1.0);
        CHECK(TuneAdvisor::modelEvictEmergencyThreshold() == Catch::Approx(prev));

        TuneAdvisor::setModelEvictEmergencyThreshold(1.5);
        CHECK(TuneAdvisor::modelEvictEmergencyThreshold() == Catch::Approx(prev));

        TuneAdvisor::setModelEvictEmergencyThreshold(100.0);
        CHECK(TuneAdvisor::modelEvictEmergencyThreshold() == Catch::Approx(prev));
    }

    SECTION("NaN is rejected") {
        double prev = TuneAdvisor::modelEvictWarningThreshold();
        TuneAdvisor::setModelEvictWarningThreshold(std::numeric_limits<double>::quiet_NaN());
        CHECK(TuneAdvisor::modelEvictWarningThreshold() == Catch::Approx(prev));
    }

    SECTION("Positive infinity is rejected") {
        double prev = TuneAdvisor::modelEvictCriticalThreshold();
        TuneAdvisor::setModelEvictCriticalThreshold(std::numeric_limits<double>::infinity());
        CHECK(TuneAdvisor::modelEvictCriticalThreshold() == Catch::Approx(prev));
    }

    SECTION("Negative infinity is rejected") {
        double prev = TuneAdvisor::modelEvictEmergencyThreshold();
        TuneAdvisor::setModelEvictEmergencyThreshold(-std::numeric_limits<double>::infinity());
        CHECK(TuneAdvisor::modelEvictEmergencyThreshold() == Catch::Approx(prev));
    }
}

// =============================================================================
// Override Precedence Tests
// =============================================================================

TEST_CASE("Model eviction threshold override precedence", "[daemon][tune][advisor][catch2]") {
    resetEvictionOverrides();

    SECTION("Programmatic setter takes precedence over env var") {
        ProfileGuard profileGuard(TuneAdvisor::Profile::Balanced);
        EnvGuard envGuard("YAMS_MODEL_EVICT_WARNING_THRESHOLD", "0.45");

        // Env var would give 0.45, but setter should override
        TuneAdvisor::setModelEvictWarningThreshold(0.55);
        CHECK(TuneAdvisor::modelEvictWarningThreshold() == Catch::Approx(0.55));
    }

    SECTION("Env var takes precedence over profile default") {
        ProfileGuard profileGuard(TuneAdvisor::Profile::Efficient);
        EnvGuard envGuard("YAMS_MODEL_EVICT_CRITICAL_THRESHOLD", "0.65");

        // Efficient profile default is 0.50, but env var overrides to 0.65
        CHECK(TuneAdvisor::modelEvictCriticalThreshold() == Catch::Approx(0.65));
    }

    SECTION("Profile default is used when no overrides set") {
        ProfileGuard profileGuard(TuneAdvisor::Profile::Aggressive);

        CHECK(TuneAdvisor::modelEvictEmergencyThreshold() == Catch::Approx(0.95));
    }
}

TEST_CASE("Connection lifetime seconds env var", "[daemon][tune][advisor][catch2]") {
    TuneAdvisor::resetConnectionLifetimeSecondsOverride();

    SECTION("Default value is 300 seconds") {
        CHECK(TuneAdvisor::connectionLifetimeSeconds() == 300);
    }

    SECTION("Valid env var overrides default") {
        EnvGuard envGuard("YAMS_CONNECTION_LIFETIME_S", "1800");
        CHECK(TuneAdvisor::connectionLifetimeSeconds() == 1800);
    }

    SECTION("Zero env var disables lifetime enforcement") {
        EnvGuard envGuard("YAMS_CONNECTION_LIFETIME_S", "0");
        CHECK(TuneAdvisor::connectionLifetimeSeconds() == 0);
    }

    SECTION("Out-of-range env var falls back to default") {
        EnvGuard envGuard("YAMS_CONNECTION_LIFETIME_S", "90000");
        CHECK(TuneAdvisor::connectionLifetimeSeconds() == 300);
    }

    SECTION("Programmatic setter overrides env") {
        EnvGuard envGuard("YAMS_CONNECTION_LIFETIME_S", "1800");
        TuneAdvisor::setConnectionLifetimeSeconds(42);
        CHECK(TuneAdvisor::connectionLifetimeSeconds() == 42);
        TuneAdvisor::resetConnectionLifetimeSecondsOverride();
    }

    TuneAdvisor::resetConnectionLifetimeSecondsOverride();
}

// =============================================================================
// Threshold Ordering Invariant Tests
// =============================================================================

TEST_CASE("Threshold ordering invariant: warning < critical < emergency",
          "[daemon][tune][advisor][catch2]") {
    resetEvictionOverrides();

    SECTION("Efficient profile maintains ordering") {
        ProfileGuard guard(TuneAdvisor::Profile::Efficient);

        CHECK(TuneAdvisor::modelEvictWarningThreshold() <
              TuneAdvisor::modelEvictCriticalThreshold());
        CHECK(TuneAdvisor::modelEvictCriticalThreshold() <
              TuneAdvisor::modelEvictEmergencyThreshold());
    }

    SECTION("Balanced profile maintains ordering") {
        ProfileGuard guard(TuneAdvisor::Profile::Balanced);

        CHECK(TuneAdvisor::modelEvictWarningThreshold() <
              TuneAdvisor::modelEvictCriticalThreshold());
        CHECK(TuneAdvisor::modelEvictCriticalThreshold() <
              TuneAdvisor::modelEvictEmergencyThreshold());
    }

    SECTION("Aggressive profile maintains ordering") {
        ProfileGuard guard(TuneAdvisor::Profile::Aggressive);

        CHECK(TuneAdvisor::modelEvictWarningThreshold() <
              TuneAdvisor::modelEvictCriticalThreshold());
        CHECK(TuneAdvisor::modelEvictCriticalThreshold() <
              TuneAdvisor::modelEvictEmergencyThreshold());
    }
}

// =============================================================================
// Repair Batch Size Profile Tests
// =============================================================================

TEST_CASE("Repair batch sizes are profile-aware", "[daemon][tune][advisor][catch2]") {
    SECTION("Efficient profile uses smaller batch sizes") {
        ProfileGuard guard(TuneAdvisor::Profile::Efficient);

        // Efficient profile scale is 0.0
        // repairMaxBatch: 8 + 24*0.0 = 8
        // repairStartupBatchSize: 25 + 75*0.0 = 25
        CHECK(TuneAdvisor::repairMaxBatch() == 8u);
        CHECK(TuneAdvisor::repairStartupBatchSize() == 25u);
    }

    SECTION("Balanced profile uses default batch sizes") {
        ProfileGuard guard(TuneAdvisor::Profile::Balanced);

        // Balanced profile scale is 0.5
        // repairMaxBatch: 8 + 24*0.5 = 20
        // repairStartupBatchSize: 25 + 75*0.5 = 62
        CHECK(TuneAdvisor::repairMaxBatch() == 20u);
        CHECK(TuneAdvisor::repairStartupBatchSize() == 62u);
    }

    SECTION("Aggressive profile uses larger batch sizes") {
        ProfileGuard guard(TuneAdvisor::Profile::Aggressive);

        // Aggressive profile scale is 1.0
        // repairMaxBatch: 8 + 24*1.0 = 32
        // repairStartupBatchSize: 25 + 75*1.0 = 100
        CHECK(TuneAdvisor::repairMaxBatch() == 32u);
        CHECK(TuneAdvisor::repairStartupBatchSize() == 100u);
    }
}

TEST_CASE("Repair batch size env var overrides", "[daemon][tune][advisor][catch2]") {
    ProfileGuard profileGuard(TuneAdvisor::Profile::Balanced);

    SECTION("YAMS_REPAIR_MAX_BATCH env var overrides profile") {
        EnvGuard envGuard("YAMS_REPAIR_MAX_BATCH", "64");

        CHECK(TuneAdvisor::repairMaxBatch() == 64u);
    }

    SECTION("YAMS_REPAIR_STARTUP_BATCH env var overrides profile") {
        EnvGuard envGuard("YAMS_REPAIR_STARTUP_BATCH", "200");

        CHECK(TuneAdvisor::repairStartupBatchSize() == 200u);
    }

    SECTION("Invalid env var falls back to profile default") {
        EnvGuard envGuard("YAMS_REPAIR_MAX_BATCH", "invalid");

        // Should fall back to profile default (20 for Balanced at 0.5 scale)
        CHECK(TuneAdvisor::repairMaxBatch() == 20u);
    }

    SECTION("Zero env var falls back to profile default") {
        EnvGuard envGuard("YAMS_REPAIR_MAX_BATCH", "0");

        // 0 is rejected, should fall back to profile default
        CHECK(TuneAdvisor::repairMaxBatch() == 20u);
    }

    SECTION("Env var > 1000 falls back to profile default") {
        EnvGuard envGuard("YAMS_REPAIR_MAX_BATCH", "1001");

        // > 1000 is rejected, should fall back to profile default
        CHECK(TuneAdvisor::repairMaxBatch() == 20u);
    }
}

TEST_CASE("Embed doc cap resolves to safe default when unset",
          "[daemon][tune][advisor][embed][catch2]") {
    struct EmbedCapGuard {
        std::size_t prev{TuneAdvisor::getEmbedDocCap()};
        ~EmbedCapGuard() { TuneAdvisor::setEmbedDocCap(prev); }
    } guard;

    SECTION("Unset cap uses built-in default (no singleton fallback)") {
        // Ensure no explicit override is active.
        TuneAdvisor::setEmbedDocCap(0);
        // Invalid env value should be ignored and still resolve to default.
        EnvGuard envGuard("YAMS_EMBED_DOC_CAP", "0");
        CHECK(TuneAdvisor::getEmbedDocCap() == 0u);
        CHECK(TuneAdvisor::resolvedEmbedDocCap() == TuneAdvisor::kDefaultEmbedDocCap);
        CHECK(TuneAdvisor::resolvedEmbedDocCap() == 64u);
    }

    SECTION("Explicit cap overrides default") {
        TuneAdvisor::setEmbedDocCap(128);
        CHECK(TuneAdvisor::resolvedEmbedDocCap() == 128u);
    }
}

// =============================================================================
// Worker poll cadence overrides
// =============================================================================

TEST_CASE("Worker poll cadence overrides", "[daemon][tune][advisor][catch2]") {
    SECTION("Dynamic updates apply when not pinned") {
        EnvGuard envGuard("YAMS_WORKER_POLL_MS", "0");
        TuneAdvisor::setWorkerPollMs(0);

        TuneAdvisor::setWorkerPollMsDynamic(200);
        CHECK(TuneAdvisor::workerPollMs() == 200u);

        TuneAdvisor::setWorkerPollMs(0);
        TuneAdvisor::setWorkerPollMsDynamic(120);
        CHECK(TuneAdvisor::workerPollMs() == 120u);
    }

    SECTION("Dynamic updates ignored when pinned by setter") {
        EnvGuard envGuard("YAMS_WORKER_POLL_MS", "0");
        TuneAdvisor::setWorkerPollMs(250);
        CHECK(TuneAdvisor::workerPollMs() == 250u);

        TuneAdvisor::setWorkerPollMsDynamic(100);
        CHECK(TuneAdvisor::workerPollMs() == 250u);

        TuneAdvisor::setWorkerPollMs(0);
    }

    SECTION("Dynamic updates ignored when env pins cadence") {
        EnvGuard envGuard("YAMS_WORKER_POLL_MS", "300");
        TuneAdvisor::setWorkerPollMs(0);

        TuneAdvisor::setWorkerPollMsDynamic(100);
        CHECK(TuneAdvisor::workerPollMs() == 300u);
    }
}

// =============================================================================
// GPU-Aware Batch Size Tests
// =============================================================================

TEST_CASE("gpuAwareBatchSize returns sensible values", "[daemon][tune][advisor][catch2]") {
    SECTION("CPU fallback (0 VRAM) returns 32") {
        CHECK(TuneAdvisor::gpuAwareBatchSize(0, 768) == 32);
        CHECK(TuneAdvisor::gpuAwareBatchSize(0, 384) == 32);
    }

    SECTION("Small VRAM (4 GB) with typical embedding dim") {
        auto batch = TuneAdvisor::gpuAwareBatchSize(4ULL * 1024 * 1024 * 1024, 768);
        CHECK(batch >= 8);
        CHECK(batch <= 256);
    }

    SECTION("Medium VRAM (8 GB) with small embedding dim") {
        auto batch = TuneAdvisor::gpuAwareBatchSize(8ULL * 1024 * 1024 * 1024, 384);
        CHECK(batch >= 8);
        CHECK(batch <= 256);
    }

    SECTION("Large VRAM (24 GB) with typical embedding dim") {
        auto batch = TuneAdvisor::gpuAwareBatchSize(24ULL * 1024 * 1024 * 1024, 768);
        CHECK(batch >= 8);
        CHECK(batch <= 256);
    }

    SECTION("More VRAM produces equal or larger batch for same dim") {
        auto batchSmall = TuneAdvisor::gpuAwareBatchSize(4ULL * 1024 * 1024 * 1024, 768);
        auto batchLarge = TuneAdvisor::gpuAwareBatchSize(24ULL * 1024 * 1024 * 1024, 768);
        CHECK(batchLarge >= batchSmall);
    }

    SECTION("Larger embedding dim produces equal or smaller batch for same VRAM") {
        auto batchSmallDim = TuneAdvisor::gpuAwareBatchSize(8ULL * 1024 * 1024 * 1024, 384);
        auto batchLargeDim = TuneAdvisor::gpuAwareBatchSize(8ULL * 1024 * 1024 * 1024, 1536);
        CHECK(batchSmallDim >= batchLargeDim);
    }

    SECTION("Env override YAMS_GPU_BATCH_SIZE takes precedence") {
        EnvGuard envGuard("YAMS_GPU_BATCH_SIZE", "42");
        CHECK(TuneAdvisor::gpuAwareBatchSize(8ULL * 1024 * 1024 * 1024, 768) == 42);
        CHECK(TuneAdvisor::gpuAwareBatchSize(0, 768) == 42);
    }
}

// =============================================================================
// GPU Detection Tests (Apple Silicon sysctl path)
// =============================================================================

TEST_CASE("detectGpu returns consistent cached result", "[daemon][gpu][catch2]") {
    using namespace yams::daemon::resource;

    const auto& first = detectGpu();
    const auto& second = detectGpu();

    // std::call_once caching: same static object returned each time
    CHECK(&first == &second);

    // Basic structural sanity: fields are default-initialized or populated
    CHECK(first.detected == second.detected);
    CHECK(first.name == second.name);
    CHECK(first.vramBytes == second.vramBytes);
    CHECK(first.provider == second.provider);
    CHECK(first.unifiedMemory == second.unifiedMemory);
}

TEST_CASE("effectiveGpuBatchBudgetBytes applies unified-memory safety defaults",
          "[daemon][gpu][batch-budget][catch2]") {
    using namespace yams::daemon::resource;

    constexpr uint64_t GiB = 1024ULL * 1024ULL * 1024ULL;

    SECTION("Dedicated memory uses full detected budget by default") {
        EnvGuard clear1("YAMS_GPU_BATCH_BUDGET_MB", "");
        EnvGuard clear2("YAMS_GPU_UNIFIED_BUDGET_MB", "");
        EnvGuard clear3("YAMS_GPU_UNIFIED_BUDGET_FRACTION", "");

        GpuInfo gpu;
        gpu.detected = true;
        gpu.unifiedMemory = false;
        gpu.provider = "migraphx";
        gpu.vramBytes = 12ULL * GiB;

        CHECK(effectiveGpuBatchBudgetBytes(gpu) == 12ULL * GiB);
    }

    SECTION("Unified memory defaults to conservative 4 GiB cap") {
        EnvGuard clear1("YAMS_GPU_BATCH_BUDGET_MB", "");
        EnvGuard clear2("YAMS_GPU_UNIFIED_BUDGET_MB", "");
        EnvGuard clear3("YAMS_GPU_UNIFIED_BUDGET_FRACTION", "");

        GpuInfo gpu;
        gpu.detected = true;
        gpu.unifiedMemory = true;
        gpu.provider = "coreml";
        gpu.vramBytes = 24ULL * GiB;

        CHECK(effectiveGpuBatchBudgetBytes(gpu) == 4ULL * GiB);
    }

    SECTION("Unified memory MB override is honored") {
        EnvGuard clear1("YAMS_GPU_BATCH_BUDGET_MB", "");
        EnvGuard envMb("YAMS_GPU_UNIFIED_BUDGET_MB", "6144");
        EnvGuard clear3("YAMS_GPU_UNIFIED_BUDGET_FRACTION", "");

        GpuInfo gpu;
        gpu.detected = true;
        gpu.unifiedMemory = true;
        gpu.provider = "coreml";
        gpu.vramBytes = 24ULL * GiB;

        CHECK(effectiveGpuBatchBudgetBytes(gpu) == 6ULL * GiB);
    }

    SECTION("Global budget override takes precedence") {
        EnvGuard envGlobal("YAMS_GPU_BATCH_BUDGET_MB", "2048");
        EnvGuard envMb("YAMS_GPU_UNIFIED_BUDGET_MB", "6144");
        EnvGuard envFrac("YAMS_GPU_UNIFIED_BUDGET_FRACTION", "0.5");

        GpuInfo gpu;
        gpu.detected = true;
        gpu.unifiedMemory = true;
        gpu.provider = "coreml";
        gpu.vramBytes = 24ULL * GiB;

        CHECK(effectiveGpuBatchBudgetBytes(gpu) == 2ULL * GiB);
    }
}

#if defined(__APPLE__) && defined(__aarch64__)
TEST_CASE("detectAppleSiliconGpu populates all fields", "[daemon][gpu][catch2]") {
    using namespace yams::daemon::resource;

    GpuInfo info;
    bool result = detail::detectAppleSiliconGpu(info);

    CHECK(result == true);
    CHECK(info.detected == true);
    CHECK(info.provider == "coreml");
    CHECK(info.unifiedMemory == true);

    // Brand string should contain "Apple" (e.g. "Apple M3 Max")
    CHECK(info.name.find("Apple") != std::string::npos);

    // VRAM should be populated
    CHECK(info.vramBytes > 0);

    // Cross-check: read hw.memsize via sysctl, verify vramBytes ≈ 75% of memsize
    uint64_t memsize = 0;
    size_t len = sizeof(memsize);
    REQUIRE(detail::sysctlbyname("hw.memsize", &memsize, &len, nullptr, 0) == 0);
    REQUIRE(memsize > 0);

    uint64_t expected = static_cast<uint64_t>(static_cast<double>(memsize) * 0.75);
    double ratio = static_cast<double>(info.vramBytes) / static_cast<double>(expected);
    CHECK(ratio == Catch::Approx(1.0).margin(0.01));

    // Sanity bounds: > 1 GB and < 1 TB
    constexpr uint64_t oneGB = 1024ULL * 1024ULL * 1024ULL;
    constexpr uint64_t oneTB = 1024ULL * oneGB;
    CHECK(info.vramBytes > oneGB);
    CHECK(info.vramBytes < oneTB);
}
#endif // __APPLE__ && __aarch64__

#if defined(__APPLE__)
TEST_CASE("sysctlString returns values for valid keys", "[daemon][gpu][catch2]") {
    using namespace yams::daemon::resource::detail;

    CHECK_FALSE(sysctlString("hw.machine").empty());
    CHECK_FALSE(sysctlString("machdep.cpu.brand_string").empty());

    // Invalid key should return empty gracefully (no crash)
    CHECK(sysctlString("totally.invalid.key.xyz").empty());
}
#endif // __APPLE__

#if defined(__APPLE__) && !defined(__aarch64__)
TEST_CASE("detectAppleSiliconGpu returns false on non-ARM64", "[daemon][gpu][catch2]") {
    using namespace yams::daemon::resource;

    GpuInfo info;
    bool result = detail::detectAppleSiliconGpu(info);

    CHECK(result == false);
    CHECK(info.detected == false);
}
#endif // __APPLE__ && !__aarch64__

// =============================================================================
// PostIngestQueue Profile-Aware Concurrency Tests
// =============================================================================

TEST_CASE("PostIngestQueue methods are profile-aware", "[daemon][tune][advisor][catch2]") {
    resetEvictionOverrides();

    auto setHardwareConcurrency = [](unsigned value) {
        yams::daemon::TuneAdvisor::setHardwareConcurrencyForTests(value);
    };

    SECTION("Efficient profile uses 0.0 scaling") {
        ProfileGuard guard(TuneAdvisor::Profile::Efficient);
        EnvGuard maxThreadsGuard("YAMS_MAX_THREADS", "0");
        EnvGuard postIngestGuard("YAMS_POST_INGEST_TOTAL_CONCURRENT", "0");
        setHardwareConcurrency(8);

        // With hw=8, all 6 stages active: totalBudget = max(formula, 6) = 6.
        // Each stage gets exactly 1 slot (budget distributed evenly).
        CHECK(TuneAdvisor::postExtractionConcurrent() == 1u);
        CHECK(TuneAdvisor::postKgConcurrent() == 1u);
        CHECK(TuneAdvisor::postSymbolConcurrent() == 1u);
        CHECK(TuneAdvisor::postEntityConcurrent() == 1u);
        CHECK(TuneAdvisor::postTitleConcurrent() == 1u);
        CHECK(TuneAdvisor::postEmbedConcurrent() == 1u);
    }

    SECTION("Balanced profile uses 0.5 scaling") {
        ProfileGuard guard(TuneAdvisor::Profile::Balanced);
        EnvGuard maxThreadsGuard("YAMS_MAX_THREADS", "0");
        EnvGuard postIngestGuard("YAMS_POST_INGEST_TOTAL_CONCURRENT", "0");
        setHardwareConcurrency(8);

        // With hw=8, all 6 stages active: totalBudget = max(3, 6) = 6.
        // Each stage gets exactly 1 slot.
        CHECK(TuneAdvisor::postExtractionConcurrent() == 1u);
        CHECK(TuneAdvisor::postKgConcurrent() == 1u);
        CHECK(TuneAdvisor::postSymbolConcurrent() == 1u);
        CHECK(TuneAdvisor::postEntityConcurrent() == 1u);
        CHECK(TuneAdvisor::postTitleConcurrent() == 1u);
        CHECK(TuneAdvisor::postEmbedConcurrent() == 1u);
    }

    SECTION("Aggressive profile uses 1.0x (defaults)") {
        ProfileGuard guard(TuneAdvisor::Profile::Aggressive);
        EnvGuard maxThreadsGuard("YAMS_MAX_THREADS", "0");
        EnvGuard postIngestGuard("YAMS_POST_INGEST_TOTAL_CONCURRENT", "0");
        setHardwareConcurrency(8);

        // With hw=8, round-up division: base=max(2,2)=2, scaleRange=max(1,2)=2.
        // total=2+2=4. But 6 active stages, total=max(4,6)=6. Each gets 1.
        CHECK(TuneAdvisor::postExtractionConcurrent() == 1u);
        CHECK(TuneAdvisor::postKgConcurrent() == 1u);
        CHECK(TuneAdvisor::postSymbolConcurrent() == 1u);
        CHECK(TuneAdvisor::postEntityConcurrent() == 1u);
        CHECK(TuneAdvisor::postTitleConcurrent() == 1u);
        CHECK(TuneAdvisor::postEmbedConcurrent() == 1u);
    }
}

// =============================================================================
// statusTickMs Default and Override Tests
// =============================================================================

TEST_CASE("statusTickMs defaults to 5", "[daemon][governance][catch2]") {
    // Ensure no env override is active
    EnvGuard envGuard("YAMS_STATUS_TICK_MS", "0"); // 0 fails validation, falls through to default
    // Actually 0 is rejected by the > 0 check, so we need to unset it
    unsetenv("YAMS_STATUS_TICK_MS");
    CHECK(TuneAdvisor::statusTickMs() == 5);
}

TEST_CASE("statusTickMs env var override", "[daemon][governance][catch2]") {
    SECTION("Valid override is respected") {
        EnvGuard envGuard("YAMS_STATUS_TICK_MS", "100");
        CHECK(TuneAdvisor::statusTickMs() == 100);
    }

    SECTION("Zero is rejected, falls back to default") {
        EnvGuard envGuard("YAMS_STATUS_TICK_MS", "0");
        CHECK(TuneAdvisor::statusTickMs() == 5);
    }

    SECTION("10000 or above is rejected") {
        EnvGuard envGuard("YAMS_STATUS_TICK_MS", "10000");
        CHECK(TuneAdvisor::statusTickMs() == 5);
    }
}

// =============================================================================
// computeCpuThrottleDelayMs Tests
// =============================================================================

TEST_CASE("computeCpuThrottleDelayMs returns 0 below threshold", "[daemon][governance][catch2]") {
    ProfileGuard guard(TuneAdvisor::Profile::Balanced);
    // Balanced threshold = 50 + 0.5*35 = 67.5%
    CHECK(TuneAdvisor::computeCpuThrottleDelayMs(60.0) == 0);
    CHECK(TuneAdvisor::computeCpuThrottleDelayMs(0.0) == 0);
}

TEST_CASE("computeCpuThrottleDelayMs returns clamped delay above threshold",
          "[daemon][governance][catch2]") {
    ProfileGuard guard(TuneAdvisor::Profile::Aggressive);
    // Aggressive threshold = 50 + 1.0*35 = 85%
    double threshold = TuneAdvisor::cpuHighThresholdPercent();
    int32_t delay = TuneAdvisor::computeCpuThrottleDelayMs(threshold + 10.0);
    CHECK(delay >= 2);
    CHECK(delay <= 25);
}

TEST_CASE("computeCpuThrottleDelayMs clamps to max 25ms", "[daemon][governance][catch2]") {
    // Even at 100% CPU, delay should not exceed 25ms
    int32_t delay = TuneAdvisor::computeCpuThrottleDelayMs(100.0);
    CHECK(delay <= 25);
}
