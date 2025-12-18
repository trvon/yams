// Catch2 migration of repair_scheduling_adapter_test.cpp
// Migration: yams-3s4 (daemon unit tests)

#include <catch2/catch_test_macros.hpp>
#include <yams/daemon/ipc/connection_fsm.h>
#include <yams/daemon/ipc/repair_scheduling_adapter.h>

using yams::daemon::ConnectionFsm;
using yams::daemon::RepairSchedulingAdapter;

TEST_CASE("RepairSchedulingAdapter derives hints from connection state",
          "[daemon][repair][scheduling]") {
    ConnectionFsm fsm;

    SECTION("initially disconnected -> all hints false") {
        auto h0 = RepairSchedulingAdapter::derive(fsm);
        REQUIRE_FALSE(h0.streaming_high_load);
        REQUIRE_FALSE(h0.maintenance_allowed);
        REQUIRE_FALSE(h0.closing);
    }

    SECTION("connected -> maintenance allowed") {
        fsm.on_connect(3);
        auto h1 = RepairSchedulingAdapter::derive(fsm);
        REQUIRE_FALSE(h1.streaming_high_load);
        REQUIRE(h1.maintenance_allowed);
        REQUIRE_FALSE(h1.closing);
    }

    SECTION("reading header -> still maintenance allowed") {
        fsm.on_connect(3);
        fsm.on_readable(1);
        auto h2 = RepairSchedulingAdapter::derive(fsm);
        REQUIRE_FALSE(h2.streaming_high_load);
        REQUIRE(h2.maintenance_allowed);
        REQUIRE_FALSE(h2.closing);
    }

    SECTION("payload -> high load") {
        fsm.on_connect(3);
        fsm.on_readable(1);
        ConnectionFsm::FrameInfo info{};
        info.payload_size = 1024;
        fsm.on_header_parsed(info);
        auto h3 = RepairSchedulingAdapter::derive(fsm);
        REQUIRE(h3.streaming_high_load);
        REQUIRE_FALSE(h3.maintenance_allowed);
    }

    SECTION("finish stream -> closing") {
        fsm.on_connect(3);
        fsm.on_readable(1);
        ConnectionFsm::FrameInfo info{};
        info.payload_size = 1024;
        fsm.on_header_parsed(info);
        fsm.on_body_parsed();
        fsm.on_stream_next(true);
        auto h4 = RepairSchedulingAdapter::derive(fsm);
        REQUIRE(h4.closing);
    }
}
