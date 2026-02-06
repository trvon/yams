#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_approx.hpp>

#include <yams/daemon/components/GradientLimiter.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

using namespace yams::daemon;
using Catch::Approx;

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

TEST_CASE("Successful completions during warmup do not adjust limit", "[daemon][gradient][catch2]") {
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

    // Pass warmup with consistent RTT
    for (int i = 0; i < 10; ++i) {
        REQUIRE(lim.tryAcquire());
        lim.onJobComplete(std::chrono::milliseconds(10), true);
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

    // Many successful completions with improving RTT to push limit up
    for (int i = 0; i < 100; ++i) {
        if (lim.tryAcquire()) {
            lim.onJobComplete(std::chrono::milliseconds(1), true);
        }
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

    REQUIRE(lim.tryAcquire());    // acquireCount=1
    REQUIRE_FALSE(lim.tryAcquire()); // rejectCount=1
    REQUIRE_FALSE(lim.tryAcquire()); // rejectCount=2

    // Release
    lim.onJobComplete(std::chrono::milliseconds(5), true);

    REQUIRE(lim.tryAcquire());    // acquireCount=2

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

TEST_CASE("applyPressure Normal is a no-op", "[daemon][gradient][catch2]") {
    GradientLimiter::Config cfg;
    cfg.initialLimit = 10.0;
    cfg.maxLimit = 20.0;
    GradientLimiter lim("test", cfg);

    lim.applyPressure(0); // Normal
    REQUIRE(lim.effectiveLimit() == 10);
}

TEST_CASE("applyPressure Warning clamps to 75% of maxLimit", "[daemon][gradient][catch2]") {
    GradientLimiter::Config cfg;
    cfg.initialLimit = 20.0;
    cfg.maxLimit = 20.0;
    GradientLimiter lim("test", cfg);

    lim.applyPressure(1); // Warning
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

TEST_CASE("applyPressure Emergency forces to zero", "[daemon][gradient][catch2]") {
    GradientLimiter::Config cfg;
    cfg.initialLimit = 10.0;
    GradientLimiter lim("test", cfg);

    lim.applyPressure(3); // Emergency
    REQUIRE(lim.effectiveLimit() == 0);

    // All acquires should fail at limit 0
    REQUIRE_FALSE(lim.tryAcquire());
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
    REQUIRE(m.acquireCount + m.rejectCount ==
            static_cast<uint64_t>(kThreads * kOpsPerThread));
    REQUIRE(m.limit >= cfg.minLimit);
    REQUIRE(m.limit <= cfg.maxLimit);
}
