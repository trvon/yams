#include <gtest/gtest.h>

#include <yams/daemon/components/ServiceManager.h>

using namespace yams::daemon;

TEST(ServiceManagerShutdownOwnership, AbiHostReleasedOnShutdownAndFsmStopped) {
    DaemonConfig cfg;
    cfg.workerThreads = 1;
    StateComponent state{};

    ServiceManager sm(cfg, state);

    // Abi host is created during construction when possible
    auto* hostBefore = sm.__test_getAbiHost();
    EXPECT_NE(hostBefore, nullptr);

    // Shutdown should release plugin hosts/loaders and mark FSM stopped
    sm.shutdown();

    auto* hostAfter = sm.__test_getAbiHost();
    EXPECT_EQ(hostAfter, nullptr);

    auto fsnap = sm.getServiceManagerFsmSnapshot();
    EXPECT_TRUE(fsnap.state == ServiceManagerState::ShuttingDown ||
                fsnap.state == ServiceManagerState::Stopped);
}
