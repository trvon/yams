#include <gtest/gtest.h>
#include <yams/daemon/ipc/connection_fsm.h>

using yams::daemon::ConnectionFsm;

TEST(ConnectionFsmTimeoutRetry, HeaderTimeoutBoundedRetries) {
    ConnectionFsm fsm;
    fsm.set_header_timeout_ms(10);
    fsm.set_max_retries(2); // allow 2 retries, then fail on 3rd timeout

    // Drive to ReadingHeader
    fsm.on_connect(3);
    ASSERT_EQ(fsm.state(), ConnectionFsm::State::Connected);
    fsm.on_readable(1);
    ASSERT_EQ(fsm.state(), ConnectionFsm::State::ReadingHeader);

    // First timeout: still ReadingHeader (retry armed)
    fsm.on_timeout(ConnectionFsm::Operation::Header);
    EXPECT_EQ(fsm.state(), ConnectionFsm::State::ReadingHeader);

    // Second timeout: still ReadingHeader (retry armed)
    fsm.on_timeout(ConnectionFsm::Operation::Header);
    EXPECT_EQ(fsm.state(), ConnectionFsm::State::ReadingHeader);

    // Third timeout: exceed retries -> Error
    fsm.on_timeout(ConnectionFsm::Operation::Header);
    EXPECT_EQ(fsm.state(), ConnectionFsm::State::Error);
}
