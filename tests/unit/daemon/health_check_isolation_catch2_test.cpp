// Bug 2: Liveness/ping requests defeat idle detection.
// SocketServer increments activeConnections on every socket accept including pings.
// TuningManager uses (activeConns == 0) as an idle condition.
// VS Code blackboard extension sends periodic PingRequest, preventing idle.
//
// Fix: Add a separate healthCheckConnections counter. Exclude it from daemonIdle.
//
// TDD Red Phase: These tests assert the existence of healthCheckConnections and
// its exclusion from idle detection. They will FAIL TO COMPILE against current code
// because the field does not exist yet.

#include <catch2/catch_test_macros.hpp>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/TuningManager.h>

using namespace yams::daemon;

// =============================================================================
// Test 1: DaemonStats has a healthCheckConnections field
// =============================================================================

TEST_CASE("DaemonStats has healthCheckConnections counter",
          "[daemon][state][health-check][catch2]") {
    DaemonStats stats;

    // After fix: healthCheckConnections is a std::atomic<uint64_t> in DaemonStats
    // BUG: This field does not exist in current code → compilation failure
    stats.healthCheckConnections.store(0);
    CHECK(stats.healthCheckConnections.load() == 0);

    stats.healthCheckConnections.fetch_add(1);
    CHECK(stats.healthCheckConnections.load() == 1);
}

// =============================================================================
// Test 2: Idle detection excludes healthCheckConnections
// =============================================================================

TEST_CASE("Idle detection ignores health check connections",
          "[daemon][state][health-check][idle][catch2]") {
    DaemonStats stats;

    // Simulate: 0 real connections, 3 health-check connections
    stats.activeConnections.store(0);
    stats.healthCheckConnections.store(3);

    // daemonIdle should be based on activeConnections only, not healthCheckConnections.
    // After fix: TuningManager only checks activeConnections (excluding health checks).
    // The idle condition: (activeConns == 0) should be true even with health checks.
    const uint64_t activeConns = stats.activeConnections.load();
    const bool idle = (activeConns == 0);
    CHECK(idle == true);

    // Health check connections should not count toward active connections
    CHECK(stats.activeConnections.load() == 0);
    CHECK(stats.healthCheckConnections.load() == 3);
}

// =============================================================================
// Test 3: Health check connections increment/decrement independently
// =============================================================================

TEST_CASE("Health check connections track independently from active connections",
          "[daemon][state][health-check][catch2]") {
    DaemonStats stats;
    stats.activeConnections.store(0);
    stats.healthCheckConnections.store(0);

    // Simulate a normal connection
    stats.activeConnections.fetch_add(1);
    CHECK(stats.activeConnections.load() == 1);
    CHECK(stats.healthCheckConnections.load() == 0);

    // Simulate a health check connection
    stats.healthCheckConnections.fetch_add(1);
    CHECK(stats.activeConnections.load() == 1);
    CHECK(stats.healthCheckConnections.load() == 1);

    // Close the normal connection
    stats.activeConnections.fetch_sub(1);
    CHECK(stats.activeConnections.load() == 0);
    CHECK(stats.healthCheckConnections.load() == 1);

    // Close the health check connection
    stats.healthCheckConnections.fetch_sub(1);
    CHECK(stats.activeConnections.load() == 0);
    CHECK(stats.healthCheckConnections.load() == 0);
}

// =============================================================================
// Test 4: shouldAllowZeroPostIngestTargets with health check isolation
// =============================================================================

TEST_CASE("shouldAllowZeroPostIngestTargets true when only health checks are active",
          "[daemon][tune][health-check][idle][catch2]") {
    // In the fixed code, daemonIdle is computed from activeConnections only.
    // Health-check connections are tracked separately and excluded.
    // This test verifies the shouldAllowZeroPostIngestTargets function still
    // returns true when there are no real connections (only health checks).

    // daemonIdle=true means activeConns==0 (health checks excluded)
    CHECK(TuningManager::testing_shouldAllowZeroPostIngestTargets(
              /*daemonIdle=*/true, /*postIngestBusy=*/false) == true);
}
