#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/GradientLimiter.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

using namespace yams::daemon;
using Catch::Approx;

// ============================================================================
// Helper: simulate a batch of concurrent jobs
// ============================================================================
// Acquires up to `concurrency` slots, then completes them all with the given RTT.
// This is necessary because the app-limited guard skips adjustment when
// inFlight < limit * appLimitedRatio.  Serial acquire+complete patterns have
// inFlight=1 which falls below the guard once limit > 2.
static int simulateBatch(GradientLimiter& lim, int concurrency, std::chrono::nanoseconds rtt,
                         bool success = true) {
    int acquired = 0;
    for (int i = 0; i < concurrency; ++i) {
        if (lim.tryAcquire())
            ++acquired;
    }
    for (int i = 0; i < acquired; ++i) {
        lim.onJobComplete(rtt, success);
    }
    return acquired;
}

// ============================================================================
// Constructor & Defaults
// ============================================================================

TEST_CASE("GradientLimiter constructor uses default config", "[daemon][gradient][catch2]") {
    GradientLimiter lim("test");
    REQUIRE(lim.effectiveLimit() == 4); // default initialLimit = 4.0
    REQUIRE(lim.inFlight() == 0);

    auto m = lim.metrics();
    REQUIRE(m.limit == Approx(4.0));
    REQUIRE(m.minRtt == 0.0);
    REQUIRE(m.smoothedRtt == 0.0);
    REQUIRE(m.longRtt == 0.0);
    REQUIRE(m.gradient == Approx(1.0));
    REQUIRE(m.inFlight == 0);
    REQUIRE(m.acquireCount == 0);
    REQUIRE(m.rejectCount == 0);
}

TEST_CASE("GradientLimiter constructor uses custom config", "[daemon][gradient][catch2]") {
    GradientLimiter::Config cfg;
    cfg.initialLimit = 8.0;
    cfg.minLimit = 2.0;
    cfg.maxLimit = 16.0;
    cfg.smoothingAlpha = 0.3;
    cfg.longWindowAlpha = 0.1;
    cfg.tolerance = 2.0;
    cfg.warmupSamples = 5;

    GradientLimiter lim("custom", cfg);
    REQUIRE(lim.effectiveLimit() == 8);
    REQUIRE(lim.inFlight() == 0);
}

// ============================================================================
// tryAcquire: Under / At Limit
// ============================================================================

TEST_CASE("tryAcquire succeeds when under limit", "[daemon][gradient][catch2]") {
    GradientLimiter::Config cfg;
    cfg.initialLimit = 3.0;
    GradientLimiter lim("test", cfg);

    REQUIRE(lim.tryAcquire());
    REQUIRE(lim.inFlight() == 1);
    REQUIRE(lim.tryAcquire());
    REQUIRE(lim.inFlight() == 2);
    REQUIRE(lim.tryAcquire());
    REQUIRE(lim.inFlight() == 3);

    auto m = lim.metrics();
    REQUIRE(m.acquireCount == 3);
    REQUIRE(m.rejectCount == 0);
}

TEST_CASE("tryAcquire rejects when at limit", "[daemon][gradient][catch2]") {
    GradientLimiter::Config cfg;
    cfg.initialLimit = 2.0;
    GradientLimiter lim("test", cfg);

    REQUIRE(lim.tryAcquire());
    REQUIRE(lim.tryAcquire());
    REQUIRE_FALSE(lim.tryAcquire());
    REQUIRE(lim.inFlight() == 2);

    auto m = lim.metrics();
    REQUIRE(m.acquireCount == 2);
    REQUIRE(m.rejectCount == 1);
}

// ============================================================================
// AIMD Reduction on Failure
// ============================================================================

TEST_CASE("onJobComplete with failure reduces limit by 10%", "[daemon][gradient][catch2]") {
    GradientLimiter::Config cfg;
    cfg.initialLimit = 10.0;
    cfg.minLimit = 1.0;
    GradientLimiter lim("test", cfg);

    REQUIRE(lim.tryAcquire());

    // Fail the job — should reduce limit by 10%
    lim.onJobComplete(std::chrono::nanoseconds(1000000), false);

    auto m = lim.metrics();
    REQUIRE(m.limit == Approx(9.0)); // 10 * 0.9
    REQUIRE(m.inFlight == 0);
}

TEST_CASE("AIMD reduction respects minLimit", "[daemon][gradient][catch2]") {
    GradientLimiter::Config cfg;
    cfg.initialLimit = 1.0;
    cfg.minLimit = 1.0;
    GradientLimiter lim("test", cfg);

    REQUIRE(lim.tryAcquire());
    lim.onJobComplete(std::chrono::nanoseconds(1000000), false);

    auto m = lim.metrics();
    REQUIRE(m.limit >= 1.0); // Should not go below minLimit
}

// ============================================================================
// Gradient Increase on Success (post-warmup)
// ============================================================================

TEST_CASE("Successful completions during warmup do not adjust limit",
          "[daemon][gradient][catch2]") {
    GradientLimiter::Config cfg;
    cfg.initialLimit = 4.0;
    cfg.warmupSamples = 5;
    GradientLimiter lim("test", cfg);

    // Complete 4 jobs (below warmupSamples=5)
    for (int i = 0; i < 4; ++i) {
        REQUIRE(lim.tryAcquire());
        lim.onJobComplete(std::chrono::milliseconds(10), true);
    }

    // Limit should still be at initial (warmup phase)
    REQUIRE(lim.effectiveLimit() == 4);
}

TEST_CASE("Gradient increases limit after warmup with stable RTT", "[daemon][gradient][catch2]") {
    GradientLimiter::Config cfg;
    cfg.initialLimit = 4.0;
    cfg.minLimit = 1.0;
    cfg.maxLimit = 32.0;
    cfg.warmupSamples = 3;
    cfg.smoothingAlpha = 0.5;
    cfg.longWindowAlpha = 0.1;
    cfg.tolerance = 1.5;
    GradientLimiter lim("test", cfg);

    // Pass warmup and grow limit with batch access (avoids app-limited guard)
    for (int i = 0; i < 30; ++i) {
        simulateBatch(lim, 4, std::chrono::milliseconds(10));
    }

    // With stable RTT, gradient ≈ 1.0, limit should increase due to queueAllowance (sqrt)
    auto m = lim.metrics();
    REQUIRE(m.limit >= 4.0);
}

// ============================================================================
// Min / Max Limit Boundaries
// ============================================================================

TEST_CASE("Limit never exceeds maxLimit", "[daemon][gradient][catch2]") {
    GradientLimiter::Config cfg;
    cfg.initialLimit = 4.0;
    cfg.maxLimit = 6.0;
    cfg.warmupSamples = 2;
    cfg.smoothingAlpha = 0.9;
    cfg.longWindowAlpha = 0.01;
    cfg.tolerance = 3.0;
    GradientLimiter lim("test", cfg);

    // Many successful completions with batch access to push limit up
    for (int i = 0; i < 100; ++i) {
        simulateBatch(lim, 6, std::chrono::milliseconds(1));
    }

    REQUIRE(lim.effectiveLimit() <= 6);
}

TEST_CASE("Limit never drops below minLimit from failures", "[daemon][gradient][catch2]") {
    GradientLimiter::Config cfg;
    cfg.initialLimit = 4.0;
    cfg.minLimit = 2.0;
    GradientLimiter lim("test", cfg);

    // Many failures
    for (int i = 0; i < 50; ++i) {
        if (lim.tryAcquire()) {
            lim.onJobComplete(std::chrono::milliseconds(100), false);
        }
    }

    REQUIRE(lim.effectiveLimit() >= 2);
}

// ============================================================================
// Metrics Accuracy
// ============================================================================

TEST_CASE("Metrics track acquire and reject counts accurately", "[daemon][gradient][catch2]") {
    GradientLimiter::Config cfg;
    cfg.initialLimit = 1.0;
    GradientLimiter lim("test", cfg);

    REQUIRE(lim.tryAcquire());       // acquireCount=1
    REQUIRE_FALSE(lim.tryAcquire()); // rejectCount=1
    REQUIRE_FALSE(lim.tryAcquire()); // rejectCount=2

    // Release
    lim.onJobComplete(std::chrono::milliseconds(5), true);

    REQUIRE(lim.tryAcquire()); // acquireCount=2

    auto m = lim.metrics();
    REQUIRE(m.acquireCount == 2);
    REQUIRE(m.rejectCount == 2);
}

// ============================================================================
// Reset
// ============================================================================

TEST_CASE("Reset restores initial state", "[daemon][gradient][catch2]") {
    GradientLimiter::Config cfg;
    cfg.initialLimit = 5.0;
    GradientLimiter lim("test", cfg);

    // Do some work
    REQUIRE(lim.tryAcquire());
    lim.onJobComplete(std::chrono::milliseconds(10), true);
    REQUIRE(lim.tryAcquire());
    lim.onJobComplete(std::chrono::milliseconds(10), false);

    lim.reset();

    auto m = lim.metrics();
    REQUIRE(m.limit == Approx(5.0));
    REQUIRE(m.inFlight == 0);
    REQUIRE(m.acquireCount == 0);
    REQUIRE(m.rejectCount == 0);
    REQUIRE(m.minRtt == 0.0);
    REQUIRE(m.smoothedRtt == 0.0);
    REQUIRE(m.longRtt == 0.0);
    REQUIRE(m.gradient == Approx(1.0));
}

// ============================================================================
// applyPressure (Governor bridge)
// ============================================================================

TEST_CASE("applyPressure Normal from Normal is a no-op", "[daemon][gradient][catch2]") {
    GradientLimiter::Config cfg;
    cfg.initialLimit = 10.0;
    cfg.maxLimit = 20.0;
    GradientLimiter lim("test", cfg);

    lim.applyPressure(0); // Normal
    REQUIRE(lim.effectiveLimit() == 10);
}

TEST_CASE("applyPressure Normal after pressure restores throughput floor",
          "[daemon][gradient][catch2]") {
    GradientLimiter::Config cfg;
    cfg.initialLimit = 10.0;
    cfg.minLimit = 2.0;
    cfg.maxLimit = 20.0;
    GradientLimiter lim("test", cfg);

    lim.applyPressure(2); // Critical -> minLimit
    REQUIRE(lim.effectiveLimit() == 2);

    lim.applyPressure(0); // Normal -> restore to at least initialLimit
    REQUIRE(lim.effectiveLimit() >= 10);
}

TEST_CASE("applyPressure Warning clamps to 75% of maxLimit", "[daemon][gradient][catch2]") {
    GradientLimiter::Config cfg;
    cfg.initialLimit = 20.0;
    cfg.maxLimit = 20.0;
    GradientLimiter lim("test", cfg);

    lim.applyPressure(1);                // Warning
    REQUIRE(lim.effectiveLimit() == 15); // 20 * 0.75
}

TEST_CASE("applyPressure Warning no-ops when already below cap", "[daemon][gradient][catch2]") {
    GradientLimiter::Config cfg;
    cfg.initialLimit = 4.0;
    cfg.maxLimit = 20.0;
    GradientLimiter lim("test", cfg);

    lim.applyPressure(1); // Warning: cap is 15, current is 4 — no change
    REQUIRE(lim.effectiveLimit() == 4);
}

TEST_CASE("applyPressure Critical forces to minLimit", "[daemon][gradient][catch2]") {
    GradientLimiter::Config cfg;
    cfg.initialLimit = 10.0;
    cfg.minLimit = 2.0;
    GradientLimiter lim("test", cfg);

    lim.applyPressure(2); // Critical
    REQUIRE(lim.effectiveLimit() == 2);
}

TEST_CASE("applyPressure Emergency forces to minLimit", "[daemon][gradient][catch2]") {
    GradientLimiter::Config cfg;
    cfg.initialLimit = 10.0;
    cfg.minLimit = 1.0;
    GradientLimiter lim("test", cfg);

    lim.applyPressure(3);               // Emergency
    REQUIRE(lim.effectiveLimit() == 1); // minLimit, not zero

    // At least one acquire should succeed (minimum progress guarantee)
    REQUIRE(lim.tryAcquire());
    // Second acquire should fail (at limit)
    REQUIRE_FALSE(lim.tryAcquire());
    // Clean up
    lim.onJobEnd();
}

TEST_CASE("Emergency pressure allows minimum progress to prevent starvation",
          "[daemon][gradient][catch2][timing-audit]") {
    // Regression test: Emergency must not set limit to 0 (causes deadlock).
    // At least one worker must always be able to make progress so that
    // partially-processed items can drain, allowing pressure to de-escalate.
    GradientLimiter::Config cfg;
    cfg.initialLimit = 16.0;
    cfg.minLimit = 1.0;
    cfg.maxLimit = 32.0;
    cfg.warmupSamples = 3;
    GradientLimiter lim("starvation_test", cfg);

    // Warm up with good RTT
    for (int i = 0; i < 10; ++i) {
        REQUIRE(lim.tryAcquire());
        lim.onJobComplete(std::chrono::milliseconds(10), true);
    }

    // Apply Emergency pressure
    lim.applyPressure(3);
    REQUIRE(lim.effectiveLimit() >= 1); // Must allow at least 1

    // Simulate work completing under Emergency pressure
    // This must succeed — otherwise the system deadlocks
    REQUIRE(lim.tryAcquire());
    lim.onJobComplete(std::chrono::milliseconds(10), true);

    // After successful completion, limit should still be at minLimit
    // (updateLimit ceiling for Emergency = minLimit)
    REQUIRE(lim.effectiveLimit() >= 1);
    REQUIRE(lim.effectiveLimit() <= static_cast<uint32_t>(cfg.minLimit));

    // Verify de-escalation path: release pressure and confirm recovery
    lim.applyPressure(0); // Normal
    for (int i = 0; i < 50; ++i) {
        simulateBatch(lim, 4, std::chrono::milliseconds(10));
    }
    CHECK(lim.effectiveLimit() > 1); // Should recover
}

// ============================================================================
// Thread Safety (basic stress)
// ============================================================================

TEST_CASE("Concurrent tryAcquire/onJobComplete does not corrupt state",
          "[daemon][gradient][catch2]") {
    GradientLimiter::Config cfg;
    cfg.initialLimit = 8.0;
    cfg.minLimit = 1.0;
    cfg.maxLimit = 32.0;
    cfg.warmupSamples = 2;
    GradientLimiter lim("stress", cfg);

    constexpr int kThreads = 4;
    constexpr int kOpsPerThread = 500;
    std::atomic<uint64_t> totalAcquired{0};
    std::atomic<uint64_t> totalRejected{0};

    auto worker = [&]() {
        for (int i = 0; i < kOpsPerThread; ++i) {
            if (lim.tryAcquire()) {
                totalAcquired.fetch_add(1, std::memory_order_relaxed);
                // Simulate some work
                std::this_thread::yield();
                lim.onJobComplete(std::chrono::microseconds(100 + (i % 50)),
                                  (i % 7) != 0); // ~85% success
            } else {
                totalRejected.fetch_add(1, std::memory_order_relaxed);
            }
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back(worker);
    }
    for (auto& t : threads) {
        t.join();
    }

    auto m = lim.metrics();
    // Invariants after all threads complete:
    REQUIRE(m.inFlight == 0); // All jobs completed
    REQUIRE(m.acquireCount == totalAcquired.load());
    REQUIRE(m.rejectCount == totalRejected.load());
    REQUIRE(m.acquireCount + m.rejectCount == static_cast<uint64_t>(kThreads * kOpsPerThread));
    REQUIRE(m.limit >= cfg.minLimit);
    REQUIRE(m.limit <= cfg.maxLimit);
}

// ============================================================================
// Issue 2: Pressure clamp must survive subsequent successful completions
// ============================================================================

TEST_CASE("applyPressure Critical survives subsequent successful completions",
          "[daemon][gradient][catch2][timing-audit]") {
    // Bug: applyPressure(2) sets limit_ = minLimit, but onJobComplete with
    // good RTT calls updateLimit() which overwrites it back above minLimit
    // because updateLimit() is unaware of pressure state.
    // Expected: limit stays at or below minLimit while pressure is Critical.
    GradientLimiter::Config cfg;
    cfg.initialLimit = 10.0;
    cfg.minLimit = 2.0;
    cfg.maxLimit = 32.0;
    cfg.warmupSamples = 3;
    cfg.smoothingAlpha = 0.5;
    cfg.longWindowAlpha = 0.1;
    cfg.tolerance = 2.0;
    GradientLimiter lim("pressure_test", cfg);

    // Warm up the limiter with good RTT so gradient is healthy
    for (int i = 0; i < 10; ++i) {
        REQUIRE(lim.tryAcquire());
        lim.onJobComplete(std::chrono::milliseconds(10), true);
    }

    // Verify limit has grown above minLimit during warmup
    REQUIRE(lim.effectiveLimit() > cfg.minLimit);

    // Apply Critical pressure — should force limit to minLimit
    lim.applyPressure(2);
    REQUIRE(lim.effectiveLimit() == 2);

    // Now simulate more successful completions with good RTT.
    // BUG: updateLimit() will overwrite the pressure clamp.
    // EXPECTED: limit should stay clamped at minLimit.
    for (int i = 0; i < 20; ++i) {
        if (lim.tryAcquire()) {
            lim.onJobComplete(std::chrono::milliseconds(10), true);
        }
    }

    INFO("Limit after 20 successful completions under Critical pressure: " << lim.effectiveLimit());
    // Under Critical pressure, limit must not exceed minLimit
    CHECK(lim.effectiveLimit() <= static_cast<uint32_t>(cfg.minLimit));
}

TEST_CASE("applyPressure Warning cap survives successful completions",
          "[daemon][gradient][catch2][timing-audit]") {
    // Similar to Critical test but for Warning level: limit should stay
    // at or below 75% of maxLimit.
    GradientLimiter::Config cfg;
    cfg.initialLimit = 20.0;
    cfg.minLimit = 1.0;
    cfg.maxLimit = 20.0;
    cfg.warmupSamples = 3;
    cfg.smoothingAlpha = 0.5;
    cfg.longWindowAlpha = 0.1;
    cfg.tolerance = 2.0;
    GradientLimiter lim("warning_test", cfg);

    // Warm up with good RTT
    for (int i = 0; i < 10; ++i) {
        if (lim.tryAcquire()) {
            lim.onJobComplete(std::chrono::milliseconds(10), true);
        }
    }

    // Apply Warning pressure — should cap at 75% of maxLimit = 15
    lim.applyPressure(1);
    uint32_t capAfterPressure = lim.effectiveLimit();
    REQUIRE(capAfterPressure <= 15);

    // More successful completions — limit should not exceed the Warning cap
    for (int i = 0; i < 30; ++i) {
        if (lim.tryAcquire()) {
            lim.onJobComplete(std::chrono::milliseconds(10), true);
        }
    }

    INFO("Limit after 30 successful completions under Warning pressure: " << lim.effectiveLimit());
    // Warning cap is 75% of maxLimit = 15
    CHECK(lim.effectiveLimit() <= 15);
}

TEST_CASE("applyPressure clamp is released when Normal is applied",
          "[daemon][gradient][catch2][timing-audit]") {
    // Verify that after pressure is cleared (Normal), the limiter can
    // recover organically.
    GradientLimiter::Config cfg;
    cfg.initialLimit = 10.0;
    cfg.minLimit = 2.0;
    cfg.maxLimit = 32.0;
    cfg.warmupSamples = 3;
    cfg.smoothingAlpha = 0.5;
    cfg.longWindowAlpha = 0.1;
    cfg.tolerance = 2.0;
    GradientLimiter lim("release_test", cfg);

    // Warm up
    for (int i = 0; i < 10; ++i) {
        REQUIRE(lim.tryAcquire());
        lim.onJobComplete(std::chrono::milliseconds(10), true);
    }

    // Apply Critical pressure
    lim.applyPressure(2);
    REQUIRE(lim.effectiveLimit() == 2);

    // Release pressure
    lim.applyPressure(0);

    // Now completions should allow limit to grow again (use batch to avoid app-limited guard)
    for (int i = 0; i < 50; ++i) {
        simulateBatch(lim, 4, std::chrono::milliseconds(10));
    }

    INFO("Limit after release and 50 batch completions: " << lim.effectiveLimit());
    // After release, limit should recover above minLimit
    CHECK(lim.effectiveLimit() > static_cast<uint32_t>(cfg.minLimit));
}

// ============================================================================
// EMA Smoothed RTT Convergence
// ============================================================================

// ============================================================================
// Netflix Gradient2 Algorithm Regression Tests
// ============================================================================

TEST_CASE("Congestion reduces limit", "[daemon][gradient][catch2][gradient2]") {
    // Proves the gradient inversion fix: rising short RTT relative to long RTT
    // must cause the limit to DECREASE. Before the fix, gradient was inverted
    // (shortRtt/longRtt) which made the limit increase under congestion.
    GradientLimiter::Config cfg;
    cfg.initialLimit = 16.0;
    cfg.minLimit = 1.0;
    cfg.maxLimit = 32.0;
    cfg.warmupSamples = 3;
    cfg.smoothingAlpha = 0.5;
    cfg.longWindowAlpha = 0.05;
    cfg.tolerance = 1.5;
    cfg.limitSmoothing = 0.2;
    GradientLimiter lim("congestion_test", cfg);

    // Warm up with low RTT to establish a healthy baseline
    for (int i = 0; i < 10; ++i) {
        simulateBatch(lim, 16, std::chrono::milliseconds(10));
    }
    double limitAfterWarmup = lim.metrics().limit;
    REQUIRE(limitAfterWarmup >= cfg.initialLimit);

    // Simulate congestion: RTT increases sharply
    // shortRtt rises well above tolerance * longRtt → gradient < 1.0 → limit shrinks
    for (int i = 0; i < 30; ++i) {
        simulateBatch(lim, 16, std::chrono::milliseconds(100));
    }

    double limitAfterCongestion = lim.metrics().limit;
    INFO("Limit after warmup: " << limitAfterWarmup
                                << ", after congestion: " << limitAfterCongestion);
    CHECK(limitAfterCongestion < limitAfterWarmup);
}

TEST_CASE("Recovery after pressure is smooth (no snap-back)",
          "[daemon][gradient][catch2][gradient2]") {
    // Verifies that after Critical pressure is released, the limit grows
    // gradually (smoothed) rather than jumping immediately back to a high value.
    GradientLimiter::Config cfg;
    cfg.initialLimit = 16.0;
    cfg.minLimit = 1.0;
    cfg.maxLimit = 32.0;
    cfg.warmupSamples = 3;
    cfg.smoothingAlpha = 0.3;
    cfg.longWindowAlpha = 0.05;
    cfg.tolerance = 1.5;
    cfg.limitSmoothing = 0.2;
    GradientLimiter lim("smooth_recovery_test", cfg);

    // Warm up to a high limit
    for (int i = 0; i < 40; ++i) {
        simulateBatch(lim, 16, std::chrono::milliseconds(10));
    }
    double limitBeforePressure = lim.metrics().limit;
    REQUIRE(limitBeforePressure > 5.0);

    // Slam limit down with Critical pressure
    lim.applyPressure(2);
    REQUIRE(lim.effectiveLimit() == 1);

    // Release pressure
    lim.applyPressure(0);

    // Track recovery: limit should increase monotonically (or near-monotonically)
    // and never jump above 50% of pre-pressure limit in a single step
    double prevLimit = lim.metrics().limit;
    int regressions = 0;
    for (int i = 0; i < 40; ++i) {
        simulateBatch(lim, 4, std::chrono::milliseconds(10));
        double currentLimit = lim.metrics().limit;

        // No single step should more than double the limit (smooth growth)
        if (prevLimit > 1.0) {
            CHECK(currentLimit <= prevLimit * 2.5);
        }

        if (currentLimit < prevLimit - 0.01) {
            ++regressions;
        }
        prevLimit = currentLimit;
    }

    // Allow at most a few minor regressions from smoothing noise
    INFO("Regressions during recovery: " << regressions);
    CHECK(regressions <= 5);

    // Limit should have recovered meaningfully but not instantly
    CHECK(lim.metrics().limit > cfg.minLimit);
}

TEST_CASE("Limit smoothing dampens per-sample jitter", "[daemon][gradient][catch2][gradient2]") {
    // With limit smoothing enabled (0.2), alternating RTTs should produce
    // smaller limit oscillations than without smoothing.
    GradientLimiter::Config cfgSmoothed;
    cfgSmoothed.initialLimit = 10.0;
    cfgSmoothed.minLimit = 1.0;
    cfgSmoothed.maxLimit = 15.0;
    cfgSmoothed.warmupSamples = 3;
    cfgSmoothed.smoothingAlpha = 0.5;
    cfgSmoothed.longWindowAlpha = 0.05;
    cfgSmoothed.tolerance = 1.5;
    cfgSmoothed.limitSmoothing = 0.2; // Smoothing ON

    GradientLimiter::Config cfgRaw = cfgSmoothed;
    cfgRaw.limitSmoothing = 1.0; // Smoothing OFF (new = raw)

    GradientLimiter limSmoothed("smoothed", cfgSmoothed);
    GradientLimiter limRaw("raw", cfgRaw);

    // Warm up both with stable RTT
    for (int i = 0; i < 10; ++i) {
        simulateBatch(limSmoothed, 10, std::chrono::milliseconds(10));
        simulateBatch(limRaw, 10, std::chrono::milliseconds(10));
    }

    // Feed alternating RTTs to create jitter
    double maxDeltaSmoothed = 0.0;
    double maxDeltaRaw = 0.0;
    double prevSmoothed = limSmoothed.metrics().limit;
    double prevRaw = limRaw.metrics().limit;

    for (int i = 0; i < 20; ++i) {
        auto rtt = (i % 2 == 0) ? std::chrono::milliseconds(5) : std::chrono::milliseconds(50);
        simulateBatch(limSmoothed, 10, rtt);
        simulateBatch(limRaw, 10, rtt);

        double curSmoothed = limSmoothed.metrics().limit;
        double curRaw = limRaw.metrics().limit;

        maxDeltaSmoothed = std::max(maxDeltaSmoothed, std::abs(curSmoothed - prevSmoothed));
        maxDeltaRaw = std::max(maxDeltaRaw, std::abs(curRaw - prevRaw));

        prevSmoothed = curSmoothed;
        prevRaw = curRaw;
    }

    INFO("Max delta smoothed: " << maxDeltaSmoothed << ", raw: " << maxDeltaRaw);
    // Smoothed version should have smaller max step changes
    CHECK(maxDeltaSmoothed <= maxDeltaRaw);
}

TEST_CASE("App-limited guard prevents growth when under-utilized",
          "[daemon][gradient][catch2][gradient2]") {
    // When inFlight < limit * appLimitedRatio (0.5), the limiter should skip
    // adjustment. This means serial single-slot usage should not grow the limit.
    GradientLimiter::Config cfg;
    cfg.initialLimit = 10.0;
    cfg.minLimit = 1.0;
    cfg.maxLimit = 32.0;
    cfg.warmupSamples = 3;
    cfg.smoothingAlpha = 0.5;
    cfg.longWindowAlpha = 0.05;
    cfg.tolerance = 1.5;
    cfg.appLimitedRatio = 0.5;
    GradientLimiter lim("app_limited_test", cfg);

    // Warm up with enough concurrency to pass the guard
    for (int i = 0; i < 10; ++i) {
        simulateBatch(lim, 10, std::chrono::milliseconds(10));
    }

    double limitAfterWarmup = lim.metrics().limit;

    // Now do serial single-slot work (inFlight=1, limit~10 → 1 < 10*0.5=5)
    // This should NOT change the limit because the guard fires
    for (int i = 0; i < 30; ++i) {
        if (lim.tryAcquire()) {
            lim.onJobComplete(std::chrono::milliseconds(10), true);
        }
    }

    double limitAfterSerial = lim.metrics().limit;
    INFO("Limit after warmup: " << limitAfterWarmup << ", after serial: " << limitAfterSerial);
    // Limit should be essentially unchanged (app-limited guard blocks adjustment)
    CHECK(limitAfterSerial == Approx(limitAfterWarmup).margin(0.1));
}

// ============================================================================
// EMA Smoothed RTT Convergence
// ============================================================================

TEST_CASE("EMA smoothed RTT converges toward actual RTT", "[daemon][gradient][catch2]") {
    GradientLimiter::Config cfg;
    cfg.initialLimit = 8.0;
    cfg.warmupSamples = 3;
    cfg.smoothingAlpha = 0.5;
    GradientLimiter lim("ema_test", cfg);

    // Pass warmup with varying RTT
    for (int i = 0; i < 5; ++i) {
        REQUIRE(lim.tryAcquire());
        lim.onJobComplete(std::chrono::milliseconds(100), true);
    }

    // Feed consistent 50ms RTT for many samples
    for (int i = 0; i < 50; ++i) {
        if (lim.tryAcquire()) {
            lim.onJobComplete(std::chrono::milliseconds(50), true);
        }
    }

    auto m = lim.metrics();
    // smoothedRtt is in nanoseconds internally: 50ms = 50,000,000 ns
    constexpr double target_ns = 50'000'000.0;
    // Smoothed RTT should converge close to 50ms (within 20%)
    CHECK(m.smoothedRtt > target_ns * 0.8);
    CHECK(m.smoothedRtt < target_ns * 1.2);
}
