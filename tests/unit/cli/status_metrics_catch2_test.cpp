#include <catch2/catch_test_macros.hpp>

#include <yams/cli/status_metrics.h>

using yams::cli::effectiveSearchMetrics;
using yams::daemon::StatusResponse;
using yams::daemon::metrics::kSearchActive;
using yams::daemon::metrics::kSearchAvgLatencyUs;
using yams::daemon::metrics::kSearchCacheHitRatePct;
using yams::daemon::metrics::kSearchConcurrencyLimit;
using yams::daemon::metrics::kSearchExecuted;
using yams::daemon::metrics::kSearchQueued;

TEST_CASE("effectiveSearchMetrics preserves populated typed metrics", "[cli][status][metrics]") {
    StatusResponse status;
    status.searchMetrics.active = 3;
    status.searchMetrics.queued = 2;
    status.searchMetrics.executed = 41;
    status.searchMetrics.cacheHitRate = 0.73;
    status.searchMetrics.avgLatencyUs = 8123;
    status.searchMetrics.concurrencyLimit = 12;

    status.requestCounts[std::string(kSearchActive)] = 99;
    status.requestCounts[std::string(kSearchQueued)] = 98;
    status.requestCounts[std::string(kSearchExecuted)] = 97;
    status.requestCounts[std::string(kSearchCacheHitRatePct)] = 96;
    status.requestCounts[std::string(kSearchAvgLatencyUs)] = 95;
    status.requestCounts[std::string(kSearchConcurrencyLimit)] = 94;

    const auto effective = effectiveSearchMetrics(status);

    CHECK(effective.active == 3);
    CHECK(effective.queued == 2);
    CHECK(effective.executed == 41);
    CHECK(effective.cacheHitRate == 0.73);
    CHECK(effective.avgLatencyUs == 8123);
    CHECK(effective.concurrencyLimit == 12);
}

TEST_CASE("effectiveSearchMetrics recovers zeroed typed metrics from requestCounts",
          "[cli][status][metrics]") {
    StatusResponse status;
    status.requestCounts[std::string(kSearchActive)] = 4;
    status.requestCounts[std::string(kSearchQueued)] = 1;
    status.requestCounts[std::string(kSearchExecuted)] = 28;
    status.requestCounts[std::string(kSearchCacheHitRatePct)] = 64;
    status.requestCounts[std::string(kSearchAvgLatencyUs)] = 10230234;
    status.requestCounts[std::string(kSearchConcurrencyLimit)] = 7;

    const auto effective = effectiveSearchMetrics(status);

    CHECK(effective.active == 4);
    CHECK(effective.queued == 1);
    CHECK(effective.executed == 28);
    CHECK(effective.cacheHitRate == 0.64);
    CHECK(effective.avgLatencyUs == 10230234);
    CHECK(effective.concurrencyLimit == 7);
}

TEST_CASE("effectiveSearchMetrics fills only missing typed fields", "[cli][status][metrics]") {
    StatusResponse status;
    status.searchMetrics.executed = 11;
    status.searchMetrics.avgLatencyUs = 4200;

    status.requestCounts[std::string(kSearchActive)] = 2;
    status.requestCounts[std::string(kSearchQueued)] = 5;
    status.requestCounts[std::string(kSearchExecuted)] = 99;
    status.requestCounts[std::string(kSearchCacheHitRatePct)] = 25;
    status.requestCounts[std::string(kSearchAvgLatencyUs)] = 123456;
    status.requestCounts[std::string(kSearchConcurrencyLimit)] = 8;

    const auto effective = effectiveSearchMetrics(status);

    CHECK(effective.active == 2);
    CHECK(effective.queued == 5);
    CHECK(effective.executed == 11);
    CHECK(effective.cacheHitRate == 0.25);
    CHECK(effective.avgLatencyUs == 4200);
    CHECK(effective.concurrencyLimit == 8);
}
