// Assertion validation tests (Catch2)
// Verifies that YAMS_ASSERT, YAMS_PRECONDITION, and YAMS_DCHECK fire
// correctly when invariants are violated.  Uses fork() to test that
// violating code aborts with SIGABRT.
//
// Skipped on Windows (no fork) and when assertions are compiled out.

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>

#ifndef _WIN32
#include <unistd.h>
#include <sys/wait.h>
#endif

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>

#include <yams/core/assert.hpp>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/WorkCoordinator.h>
#include <yams/daemon/pressure_limited_poller.h>
#include <yams/storage/reference_counter.h>

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
// PressureLimitedPoller — YAMS_ASSERT on invalid required config
// ==========================================================================

namespace {

void runPollerConfigUntilAssertion(std::shared_ptr<SpscQueue<int>> channel,
                                   PressureLimitedPollerConfig<int> cfg) {
    boost::asio::io_context ioc;
    boost::asio::co_spawn(ioc, pressureLimitedPoll<int>(channel, cfg), boost::asio::detached);
    auto deadline = std::chrono::steady_clock::now() + 500ms;
    while (std::chrono::steady_clock::now() < deadline) {
        ioc.poll_one();
    }
}

PressureLimitedPollerConfig<int> makeMinimalValidPollerConfig(std::atomic<bool>& startedFlag,
                                                              std::atomic<bool>& stopFlag,
                                                              std::atomic<bool>& pauseFlag,
                                                              std::atomic<bool>& wasActiveFlag,
                                                              std::atomic<std::size_t>& inFlight) {
    PressureLimitedPollerConfig<int> cfg;
    cfg.stageName = "assert-test";
    cfg.stopFlag = &stopFlag;
    cfg.startedFlag = &startedFlag;
    cfg.pauseFlag = &pauseFlag;
    cfg.wasActiveFlag = &wasActiveFlag;
    cfg.inFlightCounter = &inFlight;
    cfg.maxConcurrentFn = []() -> std::size_t { return 1; };
    cfg.tryAcquireFn = [](GradientLimiter*, const std::string&, const std::string&) {
        return false;
    };
    cfg.getHashFn = [](const int&) -> std::string { return ""; };
    cfg.completeJobFn = [](const std::string&, bool) {};
    cfg.processFn = [](int&) {};
    return cfg;
}

} // namespace

TEST_CASE("YAMS_ASSERT fires when PressureLimitedPoller has null channel", "[assert][poller]") {
#ifdef _WIN32
    SKIP("fork() not available on Windows");
#else
    bool fired = assertionFires([]() {
        std::atomic<bool> sf{false}, stf{false}, pf{false}, waf{false};
        std::atomic<std::size_t> inFlight{0};
        auto cfg = makeMinimalValidPollerConfig(sf, stf, pf, waf, inFlight);
        runPollerConfigUntilAssertion(nullptr, cfg);
    });
    CHECK(fired);
#endif
}

TEST_CASE("YAMS_ASSERT fires when PressureLimitedPoller has null startedFlag", "[assert][poller]") {
#ifdef _WIN32
    SKIP("fork() not available on Windows");
#else
    bool fired = assertionFires([]() {
        std::atomic<bool> sf{false}, stf{false}, pf{false}, waf{false};
        std::atomic<std::size_t> inFlight{0};
        auto cfg = makeMinimalValidPollerConfig(sf, stf, pf, waf, inFlight);
        cfg.startedFlag = nullptr;
        auto ch = std::make_shared<SpscQueue<int>>(16);
        runPollerConfigUntilAssertion(ch, cfg);
    });
    CHECK(fired);
#endif
}

TEST_CASE("YAMS_ASSERT fires when PressureLimitedPoller has null inFlightCounter",
          "[assert][poller]") {
#ifdef _WIN32
    SKIP("fork() not available on Windows");
#else
    bool fired = assertionFires([]() {
        std::atomic<bool> sf{false}, stf{false}, pf{false}, waf{false};
        std::atomic<std::size_t> inFlight{0};
        auto cfg = makeMinimalValidPollerConfig(sf, stf, pf, waf, inFlight);
        cfg.inFlightCounter = nullptr;
        auto ch = std::make_shared<SpscQueue<int>>(16);
        runPollerConfigUntilAssertion(ch, cfg);
    });
    CHECK(fired);
#endif
}

TEST_CASE("YAMS_ASSERT fires when batch PressureLimitedPoller has null batchProcessFn",
          "[assert][poller]") {
#ifdef _WIN32
    SKIP("fork() not available on Windows");
#else
    bool fired = assertionFires([]() {
        std::atomic<bool> sf{false}, stf{false}, pf{false}, waf{false};
        std::atomic<std::size_t> inFlight{0};
        auto cfg = makeMinimalValidPollerConfig(sf, stf, pf, waf, inFlight);
        cfg.batchMode = true;
        cfg.batchProcessFn = {};
        auto ch = std::make_shared<SpscQueue<int>>(16);
        runPollerConfigUntilAssertion(ch, cfg);
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
#if !defined(NDEBUG) && !defined(YAMS_DISABLE_DCHECK)
    CHECK(fired);
#else
    CHECK_FALSE(fired);
#endif
#endif
}

namespace {
std::filesystem::path assertion_test_refcount_db_path(std::string_view suffix) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::filesystem::temp_directory_path() /
           ("yams_assert_refcount_" + std::to_string(stamp) + "_" + std::string(suffix) + ".db");
}

void trigger_refcount_increment_after_commit_dcheck() {
    const auto dbPath = assertion_test_refcount_db_path("increment");
    std::error_code ec;
    std::filesystem::remove(dbPath, ec);
    std::filesystem::remove(dbPath.string() + "-wal", ec);
    std::filesystem::remove(dbPath.string() + "-shm", ec);

    yams::storage::ReferenceCounter counter(
        yams::storage::ReferenceCounter::Config{.databasePath = dbPath});
    auto txn = counter.beginTransaction();
    txn->increment("assert_refcount_increment", 1, 1);
    auto commit = txn->commit();
    if (!commit) {
        std::abort();
    }
    txn->increment("assert_refcount_increment", 1, 1);
}

void trigger_refcount_decrement_after_commit_dcheck() {
    const auto dbPath = assertion_test_refcount_db_path("decrement");
    std::error_code ec;
    std::filesystem::remove(dbPath, ec);
    std::filesystem::remove(dbPath.string() + "-wal", ec);
    std::filesystem::remove(dbPath.string() + "-shm", ec);

    yams::storage::ReferenceCounter counter(
        yams::storage::ReferenceCounter::Config{.databasePath = dbPath});
    auto txn = counter.beginTransaction();
    txn->increment("assert_refcount_decrement", 1, 1);
    auto commit = txn->commit();
    if (!commit) {
        std::abort();
    }
    txn->decrement("assert_refcount_decrement");
}

void trigger_work_coordinator_double_start_precondition() {
    yams::daemon::WorkCoordinator coordinator;
    coordinator.start(1);
    coordinator.start(1);
}
} // namespace

TEST_CASE("YAMS_DCHECK fires on refcount increment after commit", "[assert][refcount]") {
#ifdef _WIN32
    SKIP("fork() not available on Windows");
#else
    const bool fired = assertionFires([]() { trigger_refcount_increment_after_commit_dcheck(); });
#if !defined(NDEBUG) && !defined(YAMS_DISABLE_DCHECK)
    CHECK(fired);
#else
    CHECK_FALSE(fired);
#endif
#endif
}

TEST_CASE("YAMS_DCHECK fires on refcount decrement after commit", "[assert][refcount]") {
#ifdef _WIN32
    SKIP("fork() not available on Windows");
#else
    const bool fired = assertionFires([]() { trigger_refcount_decrement_after_commit_dcheck(); });
#if !defined(NDEBUG) && !defined(YAMS_DISABLE_DCHECK)
    CHECK(fired);
#else
    CHECK_FALSE(fired);
#endif
#endif
}

TEST_CASE("YAMS_PRECONDITION fires on WorkCoordinator double start", "[assert][work_coordinator]") {
#ifdef _WIN32
    SKIP("fork() not available on Windows");
#else
    CHECK(assertionFires([]() { trigger_work_coordinator_double_start_precondition(); }));
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
