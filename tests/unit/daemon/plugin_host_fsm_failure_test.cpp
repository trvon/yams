// Verifies PluginHostFsm enters Failed on load/trust failure events.
#include <gtest/gtest.h>

#include <yams/daemon/components/PluginHostFsm.h>

using namespace yams::daemon;

TEST(PluginHostFsmFailure, TransitionsToFailedAndStoresError) {
    PluginHostFsm fsm;
    // Start with a scan to leave NotInitialized
    fsm.dispatch(PluginScanStartedEvent{1});
    auto s1 = fsm.snapshot();
    EXPECT_EQ(s1.state, PluginHostState::ScanningDirectories);

    // Simulate a plugin load failure
    fsm.dispatch(PluginLoadFailedEvent{"dlopen error: symbol not found"});
    auto s2 = fsm.snapshot();
    EXPECT_EQ(s2.state, PluginHostState::Failed);
    EXPECT_NE(s2.lastError.find("symbol not found"), std::string::npos);
}
