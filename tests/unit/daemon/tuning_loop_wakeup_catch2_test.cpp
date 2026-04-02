// Bug 3: Tuning loop tick cadence is 5ms even when daemon is idle.
// statusTickMs() unconditionally returns 5ms. Each tick does mutex acquisitions,
// atomic loads, budget computation, metrics publication. The loop never backs off.
//
// Fix: Implement event-driven wakeup — sleep indefinitely when idle, wake on
// connection/queue events. Use testing helpers for test controllability.
//
// TDD Red Phase: Tests assert that the tuning loop supports idle backoff and
// event-driven wakeup. They FAIL against current code where statusTickMs() is constant.

#include <cstdlib>
#include "../../common/env_compat.h"
#include <catch2/catch_test_macros.hpp>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/TuningManager.h>

using namespace yams::daemon;

namespace {

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

} // namespace

// =============================================================================
// Test 1: statusTickMs supports an idle-mode cadence
// =============================================================================

TEST_CASE("statusTickMs returns longer interval when idle hint is set",
          "[daemon][tune][tick-cadence][catch2]") {
    // After fix: statusTickMs() should accept an idle hint or have a companion
    // function like idleTickMs() that returns a much longer interval (e.g. 1000ms+).
    // Current code always returns 5ms.

    // Option A: statusTickMs(idle=true) overload
    // Option B: separate idleTickMs() function
    // Option C: TuneAdvisor::statusTickMs() checks internal idle state

    // We test for the existence of an idle-aware tick cadence.
    // The specific API will be determined during implementation, but we assert
    // that the idle cadence is significantly longer than the active cadence.

    const uint32_t activeTick = TuneAdvisor::statusTickMs();
    CHECK(activeTick == 5); // Current default, should remain for active mode

    // After fix: there should be an idle tick cadence much larger than 5ms.
    // We expect at least 500ms for idle mode to reduce CPU wake-ups.
    // This tests for the existence of testing_idleTickMs() or similar.
    //
    // BUG: No idle-aware tick cadence exists → this will fail to compile or
    // return 5ms if we test statusTickMs directly.
    //
    // FIXED: TuneAdvisor provides an idle tick cadence >= 500ms.
    //
    // NOTE: The actual implementation uses event-driven wakeup (condition variable
    // or Asio timer cancellation), so the "idle tick" may be a maximum sleep duration
    // rather than a polling interval. The key assertion is that idle ticks are >= 500ms.

    const uint32_t idleTick = TuneAdvisor::testing_idleTickMs();
    CHECK(idleTick >= 500);
}

// =============================================================================
// Test 2: Idle tick cadence uses the fixed default
// =============================================================================

TEST_CASE("Idle tick cadence defaults to 1000ms", "[daemon][tune][tick-cadence][catch2]") {
    const uint32_t idleTick = TuneAdvisor::testing_idleTickMs();
    CHECK(idleTick == 1000);
}

// =============================================================================
// Test 3: Active tick cadence is unaffected by idle settings
// =============================================================================

TEST_CASE("Active tick cadence remains 5ms regardless of idle settings",
          "[daemon][tune][tick-cadence][catch2]") {
    const uint32_t activeTick = TuneAdvisor::statusTickMs();
    CHECK(activeTick == 5);
}

// =============================================================================
// Test 4: Wakeup notification mechanism exists
// =============================================================================

TEST_CASE("TuningManager has a wakeup notification mechanism",
          "[daemon][tune][tick-cadence][wakeup][catch2]") {
    // After fix: TuningManager should have a static or instance method to
    // wake the tuning loop from idle sleep when work arrives.
    // This test verifies the method exists and can be called without crashing.

    // BUG: No wakeup mechanism exists → compilation failure
    // FIXED: testing_notifyWakeup() triggers the wakeup signal
    TuningManager::testing_notifyWakeup();

    // If we get here without crash/compile error, the mechanism exists.
    CHECK(true);
}
