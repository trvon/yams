#include <gtest/gtest.h>
#include <yams/daemon/client/daemon_client.h>

using namespace yams::daemon;

TEST(IntegrationSmoke, DaemonClientConstructNoAutoStart) {
    ClientConfig cfg;
    cfg.autoStart = false;           // do not launch daemon in smoke
    cfg.singleUseConnections = true; // no pooling needed here
    DaemonClient client{cfg};
    EXPECT_FALSE(client.isConnected());
}
