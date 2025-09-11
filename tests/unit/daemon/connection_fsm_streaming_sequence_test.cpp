#include <gtest/gtest.h>
#include <yams/daemon/ipc/connection_fsm.h>

using yams::daemon::ConnectionFsm;

TEST(ConnectionFsmStreamingSequence, HeaderThenStreamingThenKeepAlive) {
    ConnectionFsm fsm;
    fsm.on_connect(3);
    fsm.on_readable(1);
    // Unary header (payload_size=0) drives to WritingHeader
    ConnectionFsm::FrameInfo info0{};
    info0.payload_size = 0;
    fsm.on_header_parsed(info0);
    ASSERT_EQ(fsm.state(), ConnectionFsm::State::WritingHeader);
    // Begin streaming (e.g., keepalive before payload)
    fsm.on_stream_next(false);
    EXPECT_EQ(fsm.state(), ConnectionFsm::State::StreamingChunks);
    // Another keepalive should remain in StreamingChunks
    fsm.on_stream_next(false);
    EXPECT_EQ(fsm.state(), ConnectionFsm::State::StreamingChunks);
    // Final chunk completes
    fsm.on_stream_next(true);
    EXPECT_EQ(fsm.state(), ConnectionFsm::State::Closing);
}
