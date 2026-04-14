#include <catch2/catch_test_macros.hpp>
#include <yams/daemon/client/daemon_client.h>

using namespace yams::daemon;

TEST_CASE("IntegrationSmoke.DaemonClientConstructNoAutoStart", "[smoke][integration][daemon]") {
    ClientConfig cfg;
    cfg.autoStart = false;           // do not launch daemon in smoke
    cfg.singleUseConnections = true; // no pooling needed here
    DaemonClient client{cfg};
    CHECK_FALSE(client.isConnected());
}
