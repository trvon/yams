// Ensures PluginHostFsm can recover from a failure to Ready when plugins finish loading.
#include <gtest/gtest.h>
#include <yams/daemon/components/PluginHostFsm.h>

using namespace yams::daemon;

TEST(PluginHostFsmRecovery, FailureThenReadyOnAllLoaded) {
    PluginHostFsm fsm;
    // Simulate a prior scan
    fsm.dispatch(PluginScanStartedEvent{1});
    EXPECT_NE(fsm.snapshot().state, PluginHostState::NotInitialized);

    // Failure path
    fsm.dispatch(PluginLoadFailedEvent{"dlopen error"});
    EXPECT_EQ(fsm.snapshot().state, PluginHostState::Failed);

    // Recovery when scan completes successfully with N>0 loaded
    fsm.dispatch(AllPluginsLoadedEvent{2});
    auto s2 = fsm.snapshot();
    EXPECT_EQ(s2.state, PluginHostState::Ready);
    EXPECT_EQ(s2.loadedCount, 2u);
}
