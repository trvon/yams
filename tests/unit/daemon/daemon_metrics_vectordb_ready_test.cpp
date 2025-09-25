#include <filesystem>
#include <gtest/gtest.h>
#include <yams/daemon/components/DaemonMetrics.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>

using namespace yams::daemon;

// This test exercises DaemonMetrics healing logic for vector DB readiness:
// if the actual VectorDatabase is initialized but the readiness flag was not
// updated (e.g., due to reordered init or prior lock-skip), the snapshot should
// still report vectorDbReady=true and backfill state.
TEST(DaemonMetrics, VectorDbReadyHealsFromHandle) {
    StateComponent state;
    DaemonConfig cfg;
    cfg.dataDir = std::filesystem::temp_directory_path() / "yams_test_metrics_vdb_ready";

    // Create ServiceManager and force a single-shot vector DB init to ensure the DB exists
    ServiceManager svc(cfg, state);
    auto r = svc.__test_forceVectorDbInitOnce(cfg.dataDir);
    ASSERT_TRUE(r) << "Vector DB init path should be callable";

    // Simulate an inconsistent state where the DB handle is initialized but the flag is false
    state.readiness.vectorDbReady.store(false, std::memory_order_relaxed);
    state.readiness.vectorDbDim.store(0, std::memory_order_relaxed);

    DaemonMetrics metrics(nullptr, &state, &svc);
    auto snap = metrics.getSnapshot(/*detailed*/ false);

    EXPECT_TRUE(snap.vectorDbReady) << "Snapshot should reflect live-ready DB";
    // Best-effort backfill should set the state flag as well
    EXPECT_TRUE(state.readiness.vectorDbReady.load()) << "State flag should be healed to ready";
    EXPECT_GT(snap.vectorDbDim, 0u) << "Dimension should be populated when available";
}
