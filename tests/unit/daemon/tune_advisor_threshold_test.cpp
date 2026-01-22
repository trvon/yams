// TuneAdvisor model eviction threshold tests (Catch2)
// Tests profile defaults, env var overrides, programmatic setters, and validation

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/TuneAdvisor.h>

#include <cmath>
#include <cstdlib>
#include <limits>
#include <string>

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

        // Efficient profile scale is 0.75
        // repairMaxBatch: 32 * 0.75 = 24
        // repairStartupBatchSize: 100 * 0.75 = 75
        CHECK(TuneAdvisor::repairMaxBatch() == 24u);
        CHECK(TuneAdvisor::repairStartupBatchSize() == 75u);
    }

    SECTION("Balanced profile uses default batch sizes") {
        ProfileGuard guard(TuneAdvisor::Profile::Balanced);

        // Balanced profile scale is 1.0
        CHECK(TuneAdvisor::repairMaxBatch() == 32u);
        CHECK(TuneAdvisor::repairStartupBatchSize() == 100u);
    }

    SECTION("Aggressive profile uses larger batch sizes") {
        ProfileGuard guard(TuneAdvisor::Profile::Aggressive);

        // Aggressive profile scale is 1.5
        // repairMaxBatch: 32 * 1.5 = 48
        // repairStartupBatchSize: 100 * 1.5 = 150
        CHECK(TuneAdvisor::repairMaxBatch() == 48u);
        CHECK(TuneAdvisor::repairStartupBatchSize() == 150u);
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

        // Should fall back to profile default (32 for Balanced)
        CHECK(TuneAdvisor::repairMaxBatch() == 32u);
    }

    SECTION("Zero env var falls back to profile default") {
        EnvGuard envGuard("YAMS_REPAIR_MAX_BATCH", "0");

        // 0 is rejected, should fall back to profile default
        CHECK(TuneAdvisor::repairMaxBatch() == 32u);
    }

    SECTION("Env var > 1000 falls back to profile default") {
        EnvGuard envGuard("YAMS_REPAIR_MAX_BATCH", "1001");

        // > 1000 is rejected, should fall back to profile default
        CHECK(TuneAdvisor::repairMaxBatch() == 32u);
    }
}

// =============================================================================
// PostIngestQueue Profile-Aware Concurrency Tests
// =============================================================================

TEST_CASE("PostIngestQueue methods are profile-aware", "[daemon][tune][advisor][catch2]") {
    resetEvictionOverrides();

    SECTION("Efficient profile uses 0.5x scaling") {
        ProfileGuard guard(TuneAdvisor::Profile::Efficient);

        CHECK(TuneAdvisor::postExtractionConcurrent() == 2u); // 4 * 0.5 = 2
        CHECK(TuneAdvisor::postKgConcurrent() == 4u);         // 8 * 0.5 = 4
        CHECK(TuneAdvisor::postSymbolConcurrent() == 2u);     // 4 * 0.5 = 2
        CHECK(TuneAdvisor::postEntityConcurrent() == 1u);     // 2 * 0.5 = 1
        CHECK(TuneAdvisor::postEmbedConcurrent() == 2u);      // 4 * 0.5 = 2
    }

    SECTION("Balanced profile uses 0.75x scaling") {
        ProfileGuard guard(TuneAdvisor::Profile::Balanced);

        CHECK(TuneAdvisor::postExtractionConcurrent() == 3u); // 4 * 0.75 = 3
        CHECK(TuneAdvisor::postKgConcurrent() == 6u);         // 8 * 0.75 = 6
        CHECK(TuneAdvisor::postSymbolConcurrent() == 3u);     // 4 * 0.75 = 3
        CHECK(TuneAdvisor::postEntityConcurrent() == 1u);     // 2 * 0.75 = 1.5, truncates to 1
        CHECK(TuneAdvisor::postEmbedConcurrent() == 3u);      // 4 * 0.75 = 3
    }

    SECTION("Aggressive profile uses 1.0x (defaults)") {
        ProfileGuard guard(TuneAdvisor::Profile::Aggressive);

        CHECK(TuneAdvisor::postExtractionConcurrent() == 4u);
        CHECK(TuneAdvisor::postKgConcurrent() == 8u);
        CHECK(TuneAdvisor::postSymbolConcurrent() == 4u);
        CHECK(TuneAdvisor::postEntityConcurrent() == 2u);
        CHECK(TuneAdvisor::postEmbedConcurrent() == 4u);
    }
}
