#include <gtest/gtest.h>
#include <yams/daemon/ipc/connection_fsm.h>

using yams::daemon::ConnectionFsm;

TEST(ConnectionFsmLegality, AllowedHappyPathUnaryAndStreaming) {
    ConnectionFsm fsm;
    EXPECT_EQ(fsm.state(), ConnectionFsm::State::Disconnected);

    fsm.on_connect(3);
    EXPECT_EQ(fsm.state(), ConnectionFsm::State::Connected);

    fsm.on_readable(1);
    EXPECT_EQ(fsm.state(), ConnectionFsm::State::ReadingHeader);

    // Unary: payload_size=0 -> WritingHeader
    ConnectionFsm::FrameInfo info0{};
    info0.payload_size = 0;
    fsm.on_header_parsed(info0);
    EXPECT_EQ(fsm.state(), ConnectionFsm::State::WritingHeader);

    // Start streaming, then finish
    fsm.on_stream_next(false);
    EXPECT_EQ(fsm.state(), ConnectionFsm::State::StreamingChunks);
    fsm.on_stream_next(true);
    EXPECT_EQ(fsm.state(), ConnectionFsm::State::Closing);

    // Close request transitions Closing -> Closed
    fsm.on_close_request();
    EXPECT_EQ(fsm.state(), ConnectionFsm::State::Closed);
}

TEST(ConnectionFsmLegality, IllegalEventDoesNotChangeState) {
    ConnectionFsm fsm;
    fsm.on_connect(3);
    ASSERT_EQ(fsm.state(), ConnectionFsm::State::Connected);
    // on_stream_next is illegal in Connected; should not change state
    fsm.on_stream_next(false);
    EXPECT_EQ(fsm.state(), ConnectionFsm::State::Connected);
}
