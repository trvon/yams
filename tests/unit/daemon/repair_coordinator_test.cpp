#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <thread>

#include <yams/daemon/components/RepairCoordinator.h>
#include <yams/daemon/components/StateComponent.h>

namespace yams::daemon {

TEST(RepairCoordinatorTest, RespectsMaintenanceGatingAndTicksMetrics) {
    StateComponent state;

    // Active connection function flips from busy to idle
    std::atomic<size_t> active{1};
    auto activeFn = [&]() -> size_t { return active.load(); };

    RepairCoordinator::Config rcfg;
    rcfg.enable = true;
    rcfg.dataDir = std::filesystem::temp_directory_path();
    rcfg.maxBatch = 1;
    rcfg.tickMs = 50; // fast tick for test

    RepairCoordinator rc(nullptr, &state, activeFn, rcfg);
    rc.start();

    // Let a few busy ticks accumulate
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    active = 0; // allow maintenance

    // Then let a few idle ticks happen
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    rc.stop();

    // Expect both counters to have incremented
    EXPECT_GT(state.stats.repairBusyTicks.load(), 0u);
    EXPECT_GT(state.stats.repairIdleTicks.load(), 0u);
}
} // namespace yams::daemon
