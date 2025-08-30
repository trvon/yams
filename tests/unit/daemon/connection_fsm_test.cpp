#include <gtest/gtest.h>
#include <yams/daemon/ipc/connection_fsm.h>

using yams::daemon::ConnectionFsm;

TEST(ConnectionFsm, InitialStateIsDisconnected) {
    ConnectionFsm fsm;
    EXPECT_EQ(fsm.state(), ConnectionFsm::State::Disconnected);
}

TEST(ConnectionFsm, ReadWriteGuardsRespectState) {
    ConnectionFsm fsm;
    // Initially cannot read or write
    EXPECT_FALSE(fsm.can_read());
    EXPECT_FALSE(fsm.can_write());

    // After connect, writes and reads should be allowed (use a dummy valid fd)
    fsm.on_connect(3);
    EXPECT_TRUE(fsm.can_read());
    EXPECT_TRUE(fsm.can_write());

    // Header parsed transitions to ReadingPayload via header->body
    ConnectionFsm::FrameInfo info{};
    info.payload_size = 128; // drive to ReadingPayload
    fsm.on_header_parsed(info);
    EXPECT_TRUE(fsm.can_read());

    // Body parsed should re-enable write path
    fsm.on_body_parsed();
    EXPECT_TRUE(fsm.can_write());
}
