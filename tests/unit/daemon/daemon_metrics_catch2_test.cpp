// DaemonMetrics unit tests — focused on testable surface without live daemon
#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <yams/core/types.h>
#include <yams/daemon/components/DaemonMetrics.h>

#include <cstdint>

namespace yams::daemon::test {

TEST_CASE("MetricsSnapshot default values are zero / empty", "[metrics][snapshot]") {
    MetricsSnapshot snap{};

    REQUIRE(snap.running == true);
    REQUIRE(snap.ready == false);
    REQUIRE(snap.version.empty());
    REQUIRE(snap.uptimeSeconds == 0);
    REQUIRE(snap.requestsProcessed == 0);
    REQUIRE(snap.activeConnections == 0);
    REQUIRE(snap.memoryUsageMb == 0.0);
    REQUIRE(snap.cpuUsagePercent == 0.0);
    REQUIRE(snap.fsmTransitions == 0);
    REQUIRE(snap.databasePhase.empty());
    REQUIRE(snap.vectorDbDim == 0);
}

TEST_CASE("MetricsSnapshot has reasonable default readiness", "[metrics][snapshot]") {
    MetricsSnapshot snap{};

    REQUIRE(snap.searchActive == 0);
    REQUIRE(snap.searchQueued == 0);
    REQUIRE(snap.searchExecuted == 0);
    REQUIRE(snap.searchCacheHitRate == 0.0);
    REQUIRE(snap.searchAvgLatencyUs == 0);
    REQUIRE(snap.embeddingAvailable == false);

    REQUIRE(snap.workerThreads == 0);
    REQUIRE(snap.workerActive == 0);
    REQUIRE(snap.workerQueued == 0);

    REQUIRE(snap.repairRunning == false);
    REQUIRE(snap.repairInProgress == false);
    REQUIRE(snap.repairQueueDepth == 0);

    REQUIRE(snap.topologyRebuildRunning == false);
    REQUIRE(snap.topologyArtifactsFresh == false);
    REQUIRE(snap.topologyLastRunSucceeded == false);

    REQUIRE(snap.governorPressureLevel == 0);
    REQUIRE(snap.governorHeadroomPct == 100);
}

TEST_CASE("MetricsSnapshot fields can be set", "[metrics][snapshot]") {
    MetricsSnapshot snap{};

    snap.running = false;
    snap.ready = true;
    snap.version = "0.16.2";
    snap.uptimeSeconds = 3600;
    snap.requestsProcessed = 1000;
    snap.activeConnections = 5;
    snap.memoryUsageMb = 512.0;
    snap.cpuUsagePercent = 25.5;

    REQUIRE(snap.running == false);
    REQUIRE(snap.ready == true);
    REQUIRE(snap.version == "0.16.2");
    REQUIRE(snap.uptimeSeconds == 3600);
    REQUIRE(snap.requestsProcessed == 1000);
    REQUIRE(snap.activeConnections == 5);
    REQUIRE(snap.memoryUsageMb == Catch::Approx(512.0));
    REQUIRE(snap.cpuUsagePercent == Catch::Approx(25.5));
}

TEST_CASE("MetricsSnapshot nested structures are default-initialized", "[metrics][snapshot]") {
    MetricsSnapshot snap{};

    REQUIRE(snap.glExtraction.limit == 0.0);
    REQUIRE(snap.glExtraction.smoothedRtt == 0.0);
    REQUIRE(snap.glExtraction.gradient == 0.0);
    REQUIRE(snap.glExtraction.inFlight == 0);
    REQUIRE(snap.glExtraction.acquireCount == 0);
    REQUIRE(snap.glExtraction.rejectCount == 0);

    REQUIRE(snap.glKg.limit == 0.0);
    REQUIRE(snap.glSymbol.limit == 0.0);
    REQUIRE(snap.glEntity.limit == 0.0);
    REQUIRE(snap.glEmbed.limit == 0.0);
    REQUIRE(snap.gradientLimitersEnabled == false);
}

TEST_CASE("MetricsSnapshot pipeline counters start at zero", "[metrics][snapshot]") {
    MetricsSnapshot snap{};

    REQUIRE(snap.extractionInFlight == 0);
    REQUIRE(snap.kgQueued == 0);
    REQUIRE(snap.kgDropped == 0);
    REQUIRE(snap.kgConsumed == 0);
    REQUIRE(snap.kgInFlight == 0);
    REQUIRE(snap.symbolInFlight == 0);
    REQUIRE(snap.entityQueued == 0);
    REQUIRE(snap.entityDropped == 0);
    REQUIRE(snap.embedQueued == 0);
    REQUIRE(snap.embedDropped == 0);
    REQUIRE(snap.embedInFlight == 0);
}

TEST_CASE("MetricsSnapshot DB pool defaults are sane", "[metrics][snapshot]") {
    MetricsSnapshot snap{};

    REQUIRE(snap.dbWritePoolAvailable == false);
    REQUIRE(snap.dbWritePoolTotalConnections == 0);
    REQUIRE(snap.dbWritePoolAvailableConnections == 0);
    REQUIRE(snap.dbWritePoolActiveConnections == 0);
    REQUIRE(snap.dbWritePoolWaitingRequests == 0);

    REQUIRE(snap.dbReadPoolAvailable == false);
    REQUIRE(snap.dbReadPoolTotalConnections == 0);
    REQUIRE(snap.dbReadPoolAvailableConnections == 0);
    REQUIRE(snap.dbReadPoolActiveConnections == 0);
}

TEST_CASE("computePhysicalWalkTtl returns clamping behavior", "[metrics][ttl]") {
    // Zero duration → minimum TTL
    REQUIRE(computePhysicalWalkTtl(0) == 60'000);

    // Very fast walk → minimum TTL
    REQUIRE(computePhysicalWalkTtl(1) == 60'000);

    // Normal walk → amplified
    std::uint64_t ttl = computePhysicalWalkTtl(5'000);
    REQUIRE(ttl >= 60'000);
    REQUIRE(ttl <= 1'800'000);

    // Very slow walk → capped at max
    std::uint64_t maxTtl = computePhysicalWalkTtl(1'000'000);
    REQUIRE(maxTtl <= 1'800'000);
}

} // namespace yams::daemon::test
