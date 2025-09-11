#include <gtest/gtest.h>
#include <yams/daemon/ipc/connection_fsm.h>

using yams::daemon::ConnectionFsm;

TEST(ConnectionFsmBackpressure, HysteresisAndCapacity) {
    ConnectionFsm fsm;
    fsm.set_write_cap_bytes(100);
    fsm.set_backpressure_watermarks(50, 75); // low=50%, high=75%

    EXPECT_FALSE(fsm.backpressured());
    EXPECT_EQ(fsm.write_capacity_remaining(), static_cast<std::size_t>(100));

    // Queue 80 bytes => 80% >= 75% => ON
    fsm.on_write_queued(80);
    EXPECT_TRUE(fsm.backpressured());
    EXPECT_EQ(fsm.write_capacity_remaining(), static_cast<std::size_t>(20));

    // Flush down to 30 bytes => 30% <= 50% => OFF
    fsm.on_write_flushed(50);
    EXPECT_FALSE(fsm.backpressured());
    EXPECT_EQ(fsm.write_capacity_remaining(), static_cast<std::size_t>(50));
}
