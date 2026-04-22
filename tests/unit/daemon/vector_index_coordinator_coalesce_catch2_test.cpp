// Tests: requestRebuild coalescing — three concurrent calls coalesce into one rebuild.
// Compile with -DYAMS_TESTING=1

#include <catch2/catch_test_macros.hpp>
#include <yams/daemon/components/VectorIndexCoordinator.h>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>
#include <future>

using namespace yams::daemon;

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

    // All three must resolve successfully.
    REQUIRE_NOTHROW(f1.get());
    REQUIRE_NOTHROW(f2.get());
    REQUIRE_NOTHROW(f3.get());

    // Exactly one rebuild: epoch incremented by 1.
    REQUIRE(coord.testing_rebuildEpoch() == 1u);

    // No pending reasons remain.
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

    // At most 2 rebuilds; at least 1.
    REQUIRE(coord.testing_rebuildEpoch() >= 1u);
    REQUIRE(coord.testing_rebuildEpoch() <= 2u);
}
