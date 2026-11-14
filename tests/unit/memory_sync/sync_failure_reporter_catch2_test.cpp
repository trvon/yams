#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <yams/memory_sync/sync_failure_reporter.h>

#include <chrono>

using Catch::Matchers::ContainsSubstring;
using yams::memory_sync::SyncFailureReporter;
using namespace std::chrono_literals;

TEST_CASE("SyncFailureReporter logs a new error at once and rate-limits repeats",
          "[memory_sync][catch2]") {
    SyncFailureReporter reporter(10min);
    const auto t0 = SyncFailureReporter::Clock::now();

    auto first = reporter.failed("budget exhausted", t0);
    REQUIRE(first.has_value());
    CHECK_THAT(*first, ContainsSubstring("budget exhausted"));

    // The same error within the interval is suppressed.
    CHECK_FALSE(reporter.failed("budget exhausted", t0 + 5s).has_value());
    CHECK_FALSE(reporter.failed("budget exhausted", t0 + 9min).has_value());

    // After the interval it is reported again with the cycles it stood for.
    auto repeat = reporter.failed("budget exhausted", t0 + 10min);
    REQUIRE(repeat.has_value());
    CHECK_THAT(*repeat, ContainsSubstring("3 cycles since the last report"));
    CHECK_THAT(*repeat, ContainsSubstring("4 in a row"));
}

TEST_CASE("SyncFailureReporter reports a changed error immediately", "[memory_sync][catch2]") {
    SyncFailureReporter reporter(10min);
    const auto t0 = SyncFailureReporter::Clock::now();
    REQUIRE(reporter.failed("first error", t0).has_value());

    auto changed = reporter.failed("second error", t0 + 1s);
    REQUIRE(changed.has_value());
    CHECK_THAT(*changed, ContainsSubstring("second error"));
}

TEST_CASE("SyncFailureReporter logs recovery once and clears the error", "[memory_sync][catch2]") {
    SyncFailureReporter reporter(10min);
    const auto t0 = SyncFailureReporter::Clock::now();

    CHECK_FALSE(reporter.succeeded().has_value()); // nothing failed yet

    (void)reporter.failed("lease renewal failed", t0);
    (void)reporter.failed("lease renewal failed", t0 + 5s);
    auto snapshot = reporter.snapshot();
    CHECK(snapshot.lastError == "lease renewal failed");
    CHECK(snapshot.consecutive == 2);
    CHECK(snapshot.lastAt == t0 + 5s);

    auto recovered = reporter.succeeded();
    REQUIRE(recovered.has_value());
    CHECK_THAT(*recovered, ContainsSubstring("2 failed cycles"));
    CHECK(reporter.snapshot().lastError.empty());
    CHECK_FALSE(reporter.succeeded().has_value());

    // The next failure after a recovery is reported at once, even within the interval.
    CHECK(reporter.failed("lease renewal failed", t0 + 6s).has_value());
}
