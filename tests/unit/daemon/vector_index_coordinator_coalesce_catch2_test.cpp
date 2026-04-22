// Tests: requestRebuild coalescing — truly concurrent callers should share one rebuild.
//
// The coordinator now accepts rebuild requests from any executor and uses
// atomic admission (`rebuildInFlight_`) plus a waiter list for coalescing.
// To exercise that contract, spawn callers on the io_context executor rather
// than serializing them through the coordinator strand.
//
// Compile with -DYAMS_TESTING=1

#include <catch2/catch_test_macros.hpp>
#include <yams/daemon/components/VectorIndexCoordinator.h>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>
#include <future>

using namespace yams::daemon;

TEST_CASE("requestRebuild: single call advances epoch by 1", "[coordinator][coalesce][single]") {
    boost::asio::io_context io;
    VectorIndexCoordinator coord(io.get_executor(), nullptr, nullptr);
    REQUIRE(coord.testing_rebuildEpoch() == 0u);

    auto f = boost::asio::co_spawn(io, coord.requestRebuild(RebuildReason::Manual),
                                   boost::asio::use_future);
    io.run();
    REQUIRE_NOTHROW(f.get());
    REQUIRE(coord.testing_rebuildEpoch() == 1u);
}

TEST_CASE("requestRebuild: three concurrent Manual requests coalesce into one rebuild",
          "[coordinator][coalesce]") {
    boost::asio::io_context io;
    VectorIndexCoordinator coord(io.get_executor(), nullptr, nullptr);
    REQUIRE(coord.testing_rebuildEpoch() == 0u);

    auto f1 = boost::asio::co_spawn(io, coord.requestRebuild(RebuildReason::Manual),
                                    boost::asio::use_future);
    auto f2 = boost::asio::co_spawn(io, coord.requestRebuild(RebuildReason::Manual),
                                    boost::asio::use_future);
    auto f3 = boost::asio::co_spawn(io, coord.requestRebuild(RebuildReason::Manual),
                                    boost::asio::use_future);

    io.run();

    REQUIRE_NOTHROW(f1.get());
    REQUIRE_NOTHROW(f2.get());
    REQUIRE_NOTHROW(f3.get());

    // Exactly one rebuild: epoch incremented by 1.
    REQUIRE(coord.testing_rebuildEpoch() == 1u);
    REQUIRE(coord.testing_pendingReasons() == 0u);
}

TEST_CASE("requestRebuild: different reasons coalesce if in-flight", "[coordinator][coalesce]") {
    boost::asio::io_context io;
    VectorIndexCoordinator coord(io.get_executor(), nullptr, nullptr);
    auto fa = boost::asio::co_spawn(io, coord.requestRebuild(RebuildReason::DimensionMismatch),
                                    boost::asio::use_future);
    auto fb = boost::asio::co_spawn(io, coord.requestRebuild(RebuildReason::TopologyDirty),
                                    boost::asio::use_future);

    io.run();
    REQUIRE_NOTHROW(fa.get());
    REQUIRE_NOTHROW(fb.get());

    // At most 2 rebuilds (1 coalesced + 1 follow-up for the second reason); at least 1.
    REQUIRE(coord.testing_rebuildEpoch() >= 1u);
    REQUIRE(coord.testing_rebuildEpoch() <= 2u);
}
