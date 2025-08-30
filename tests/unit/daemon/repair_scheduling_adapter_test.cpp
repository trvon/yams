#include <gtest/gtest.h>
#include <yams/daemon/ipc/connection_fsm.h>
#include <yams/daemon/ipc/repair_scheduling_adapter.h>

using yams::daemon::ConnectionFsm;
using yams::daemon::RepairSchedulingAdapter;

TEST(RepairSchedulingAdapter, DerivesHintsFromConnectionState) {
    ConnectionFsm fsm;
    // Initially disconnected -> closing=false, maintenance=false, high_load=false
    auto h0 = RepairSchedulingAdapter::derive(fsm);
    EXPECT_FALSE(h0.streaming_high_load);
    EXPECT_FALSE(h0.maintenance_allowed);
    EXPECT_FALSE(h0.closing);

    // Connected -> maintenance allowed
    fsm.on_connect(3);
    auto h1 = RepairSchedulingAdapter::derive(fsm);
    EXPECT_FALSE(h1.streaming_high_load);
    EXPECT_TRUE(h1.maintenance_allowed);
    EXPECT_FALSE(h1.closing);

    // Reading header -> still maintenance allowed
    fsm.on_readable(1);
    auto h2 = RepairSchedulingAdapter::derive(fsm);
    EXPECT_FALSE(h2.streaming_high_load);
    EXPECT_TRUE(h2.maintenance_allowed);
    EXPECT_FALSE(h2.closing);

    // Payload -> high load
    ConnectionFsm::FrameInfo info{};
    info.payload_size = 1024;
    fsm.on_header_parsed(info);
    auto h3 = RepairSchedulingAdapter::derive(fsm);
    EXPECT_TRUE(h3.streaming_high_load);
    EXPECT_FALSE(h3.maintenance_allowed);

    // Finish stream -> closing
    fsm.on_body_parsed();
    fsm.on_stream_next(true);
    auto h4 = RepairSchedulingAdapter::derive(fsm);
    EXPECT_TRUE(h4.closing);
}
