#include <gtest/gtest.h>
#include <yams/daemon/ipc/connection_fsm.h>

using yams::daemon::ConnectionFsm;

TEST(ConnectionFsmShutdown, CloseFromConnected) {
    ConnectionFsm fsm;
    fsm.on_connect(3);
    ASSERT_EQ(fsm.state(), ConnectionFsm::State::Connected);
    fsm.on_close_request();
    EXPECT_EQ(fsm.state(), ConnectionFsm::State::Closing);
    // Idempotent close request keeps Closing and then Closed
    fsm.on_close_request();
    EXPECT_EQ(fsm.state(), ConnectionFsm::State::Closed);
}

TEST(ConnectionFsmShutdown, CloseFromStreaming) {
    ConnectionFsm fsm;
    fsm.on_connect(3);
    fsm.on_readable(1);
    ConnectionFsm::FrameInfo info{};
    info.payload_size = 1; // drive to payload -> then body_parsed -> WritingHeader
    fsm.on_header_parsed(info);
    fsm.on_body_parsed();
    ASSERT_EQ(fsm.state(), ConnectionFsm::State::WritingHeader);
    fsm.on_stream_next(false);
    ASSERT_EQ(fsm.state(), ConnectionFsm::State::StreamingChunks);
    fsm.on_close_request();
    EXPECT_EQ(fsm.state(), ConnectionFsm::State::Closing);
}
