#include <gtest/gtest.h>
#include <yams/daemon/ipc/connection_fsm.h>

using yams::daemon::ConnectionFsm;

TEST(ConnectionFsmErrorFlow, ErrorThenGracefulClose) {
    ConnectionFsm fsm;
    fsm.on_connect(3);
    ASSERT_EQ(fsm.state(), ConnectionFsm::State::Connected);
    // Simulate an error (e.g., parse failure)
    fsm.on_error(22);
    EXPECT_EQ(fsm.state(), ConnectionFsm::State::Error);
    // Request close from Error should move to Closing
    fsm.on_close_request();
    EXPECT_EQ(fsm.state(), ConnectionFsm::State::Closing);
}
