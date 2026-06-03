// Assertion validation tests (Catch2)
// Verifies that YAMS_ASSERT, YAMS_PRECONDITION, and YAMS_DCHECK fire
// correctly when invariants are violated.  Uses fork() to test that
// violating code aborts with SIGABRT.
//
// Skipped on Windows (no fork) and when assertions are compiled out.

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <cstdlib>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>

#ifndef _WIN32
#include <sys/wait.h>
#include <unistd.h>
#endif

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>

#include <yams/core/assert.hpp>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/pressure_limited_poller.h>

using namespace yams::daemon;
using namespace std::chrono_literals;

namespace {

/// Fork a child process that runs @p fn.  Returns true if the child
/// was terminated by SIGABRT (assertion fired) and false otherwise.
/// On platforms without fork(), returns false (test skipped).
bool assertionFires(std::function<void()> fn) {
#ifdef _WIN32
    (void)fn;
    return false;
#else
    pid_t pid = fork();
    if (pid < 0) {
        // fork failed — skip
        return false;
    }
    if (pid == 0) {
        // Child: suppress assertion diagnostic noise on stderr during tests.
        // The test only cares about the exit signal.
        fn();
        _exit(0); // should not reach here if assertion fired
    }
    // Parent
    int status = 0;
    waitpid(pid, &status, 0);
    return WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT;
#endif
}

// Sanity check: verify the fork machinery works.
TEST_CASE("assertion test harness detects SIGABRT", "[assert][harness]") {
#ifdef _WIN32
    SKIP("fork() not available on Windows");
#else
    bool fired = assertionFires([]() { std::abort(); });
    CHECK(fired);
#endif
}

TEST_CASE("assertion test harness detects clean exit", "[assert][harness]") {
#ifdef _WIN32
    SKIP("fork() not available on Windows");
#else
    bool fired = assertionFires([]() { _exit(0); });
    CHECK_FALSE(fired);
#endif
}

} // namespace

// ==========================================================================
// PressureLimitedPoller — YAMS_ASSERT on null inFlightCounter
// ==========================================================================

TEST_CASE("YAMS_ASSERT fires when PressureLimitedPoller has null inFlightCounter",
          "[assert][poller]") {
#ifdef _WIN32
    SKIP("fork() not available on Windows");
#else
    bool fired = assertionFires([]() {
        boost::asio::io_context ioc;
        std::atomic<bool> sf{false}, stf{false}, pf{false}, waf{false};

        PressureLimitedPollerConfig<int> cfg;
        cfg.stageName = "null-inflight-assert";
        cfg.stopFlag = &stf;
        cfg.startedFlag = &sf;
        cfg.pauseFlag = &pf;
        cfg.wasActiveFlag = &waf;
        cfg.maxConcurrentFn = []() -> std::size_t { return 1; };
        cfg.tryAcquireFn = [](GradientLimiter*, const std::string&, const std::string&) {
            return false;
        };
        cfg.getHashFn = [](const int&) -> std::string { return ""; };
        cfg.completeJobFn = [](const std::string&, bool) {};
        cfg.processFn = [](int&) {};
        // Deliberately leave inFlightCounter = nullptr

        auto ch = std::make_shared<SpscQueue<int>>(16);
        boost::asio::co_spawn(ioc, pressureLimitedPoll<int>(ch, std::move(cfg)),
                              boost::asio::detached);
        // Run until the assertion fires
        auto dl = std::chrono::steady_clock::now() + 500ms;
        while (std::chrono::steady_clock::now() < dl) {
            ioc.poll_one();
        }
    });
    CHECK(fired);
#endif
}

// ==========================================================================
// TurboQuant — YAMS_PRECONDITION / YAMS_ASSERT violations
// ==========================================================================

#include <yams/vector/turboquant.h>

namespace {

void trigger_turboquant_precondition_zero_dimension() {
    yams::vector::TurboQuantConfig config;
    config.dimension = 0;
    [[maybe_unused]] yams::vector::TurboQuantMSE tq(config);
}

void trigger_turboquant_precondition_bits_out_of_range() {
    yams::vector::TurboQuantConfig config;
    config.bits_per_channel = 9;
    [[maybe_unused]] yams::vector::TurboQuantMSE tq(config);
}

void trigger_turboquant_precondition_codec_encode_wrong_dimension() {
    yams::vector::TurboQuantConfig config;
    config.dimension = 128;
    config.bits_per_channel = 4;
    yams::vector::TurboQuantMSE tq(config);
    std::vector<float> wrongDim(64);
    tq.encode(wrongDim);
}

void trigger_turboquant_assert_empty_centroids() {
    yams::vector::TurboQuantConfig config;
    config.dimension = 128;
    config.bits_per_channel = 4;
    yams::vector::TurboQuantMSE tq(config);
    // scoreFromPacked asserts !centroids_.empty() before scoring.
    // We provide a valid query but no training → assertion fires.
    std::vector<float> query(128, 0.0f);
    query[0] = 1.0f;
    std::vector<uint8_t> emptyCodes;
    tq.scoreFromPacked(query, emptyCodes);
}

void trigger_turboquant_precondition_centroid_idx_out_of_bounds() {
    yams::vector::TurboQuantConfig config;
    config.dimension = 128;
    config.bits_per_channel = 4; // 16 centroids; index 255 is invalid.
    yams::vector::TurboQuantMSE tq(config);
    std::vector<uint8_t> badIndices(128, 255);
    tq.decode(badIndices);
}

} // namespace

TEST_CASE("YAMS_PRECONDITION fires on zero dimension", "[assert][turboquant]") {
#ifdef _WIN32
    SKIP("fork() not available on Windows");
#else
    CHECK(assertionFires([]() { trigger_turboquant_precondition_zero_dimension(); }));
#endif
}

TEST_CASE("YAMS_PRECONDITION fires on bits-per-channel out of range", "[assert][turboquant]") {
#ifdef _WIN32
    SKIP("fork() not available on Windows");
#else
    CHECK(assertionFires([]() { trigger_turboquant_precondition_bits_out_of_range(); }));
#endif
}

TEST_CASE("YAMS_PRECONDITION fires on centroid index out of bounds", "[assert][turboquant]") {
#ifdef _WIN32
    SKIP("fork() not available on Windows");
#else
    CHECK(assertionFires([]() { trigger_turboquant_precondition_centroid_idx_out_of_bounds(); }));
#endif
}

TEST_CASE("YAMS_ASSERT fires on empty centroids", "[assert][turboquant]") {
#ifdef _WIN32
    SKIP("fork() not available on Windows");
#else
    CHECK(assertionFires([]() { trigger_turboquant_assert_empty_centroids(); }));
#endif
}

TEST_CASE("YAMS_PRECONDITION fires on encode with wrong vector dimension", "[assert][turboquant]") {
#ifdef _WIN32
    SKIP("fork() not available on Windows");
#else
    CHECK(assertionFires([]() { trigger_turboquant_precondition_codec_encode_wrong_dimension(); }));
#endif
}

// ==========================================================================
// YAMS_POSTCONDITION — always-on postcondition check
// ==========================================================================

namespace {
void trigger_postcondition_failure() {
    YAMS_POSTCONDITION(false, "test: postcondition must pass");
}
} // namespace

TEST_CASE("YAMS_POSTCONDITION fires on violation", "[assert][postcondition]") {
#ifdef _WIN32
    SKIP("fork() not available on Windows");
#else
    CHECK(assertionFires([]() { trigger_postcondition_failure(); }));
#endif
}

// ==========================================================================
// YAMS_DCHECK — debug-only sanity check (active in non-NDEBUG builds)
// ==========================================================================

namespace {
void trigger_dcheck_failure() {
    YAMS_DCHECK(false, "test: dcheck must pass in debug builds");
}
} // namespace

TEST_CASE("YAMS_DCHECK fires in debug build", "[assert][dcheck]") {
#ifdef _WIN32
    SKIP("fork() not available on Windows");
#else
    // DCHECK is active when NDEBUG is NOT defined.  In shipping builds
    // (NDEBUG) this test would pass without firing — the DCHECK is a no-op.
    // Our builddir-local is debug (NDEBUG not defined), so DCHECK fires.
    bool fired = assertionFires([]() { trigger_dcheck_failure(); });

    // In debug: fires.  In release (NDEBUG): does not fire.
    // We can't test both in one binary, so we assert the debug behavior.
#ifndef NDEBUG
    CHECK(fired);
#else
    CHECK_FALSE(fired);
#endif
#endif
}

// ==========================================================================
// YAMS_UNREACHABLE — impossible control-flow marker
// ==========================================================================

namespace {
[[noreturn]] void trigger_unreachable() {
    YAMS_UNREACHABLE("test: unreachable code reached");
}
} // namespace

TEST_CASE("YAMS_UNREACHABLE fires when reached", "[assert][unreachable]") {
#ifdef _WIN32
    SKIP("fork() not available on Windows");
#else
    CHECK(assertionFires([]() { trigger_unreachable(); }));
#endif
}
