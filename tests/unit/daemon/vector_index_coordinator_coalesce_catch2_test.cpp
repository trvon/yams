// Tests: requestRebuild coalescing — truly concurrent callers should share one rebuild.
//
// The coordinator now accepts rebuild requests from any executor and uses
// atomic admission (`rebuildInFlight_`) plus a waiter list for coalescing.
// To exercise that contract, spawn callers on the io_context executor rather
// than serializing them through the coordinator strand.
//
// Compile with -DYAMS_TESTING=1

#include <future>
#include <thread>
// pi-lens-ignore: fatal error
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>
#include <catch2/catch_test_macros.hpp>
#include <yams/compat/thread_stop_compat.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/VectorIndexCoordinator.h>

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

TEST_CASE("failed rebuild remains not ready", "[coordinator][rebuild][error][readiness]") {
    boost::asio::io_context io;
    StateComponent state;
    state.readiness.vectorIndexReady.store(true, std::memory_order_relaxed);
    state.readiness.vectorIndexProgress.store(100, std::memory_order_relaxed);
    VectorIndexCoordinator coord(io.get_executor(), nullptr, &state);

    auto future = boost::asio::co_spawn(io, coord.requestRebuild(RebuildReason::Manual),
                                        boost::asio::use_future);
    io.run();

    const auto result = future.get();
    REQUIRE_FALSE(result.has_value());
    const auto telemetry = coord.snapshot();
    CHECK_FALSE(telemetry.ready);
    CHECK_FALSE(telemetry.rebuilding);
    CHECK(telemetry.progressPct == 0u);
    CHECK_FALSE(state.readiness.vectorIndexReady.load(std::memory_order_relaxed));
    CHECK(state.readiness.vectorIndexProgress.load(std::memory_order_relaxed) == 0u);
}

TEST_CASE("foreign-executor rebuild preserves failure when completion wins the race",
          "[coordinator][rebuild][epoch][race]") {
    boost::asio::io_context coordinatorIo;
    const auto coordinatorWork = boost::asio::make_work_guard(coordinatorIo);
    VectorIndexCoordinator coord(coordinatorIo.get_executor(), nullptr, nullptr);
    std::atomic<bool> completionObservedDuringAdmission{false};
    coord.testing_setAfterRebuildAdmission([&] {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
        while (coord.testing_rebuildEpoch() == 0u && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::yield();
        }
        completionObservedDuringAdmission.store(coord.testing_rebuildEpoch() == 1u,
                                                std::memory_order_release);
    });
    yams::compat::jthread coordinatorRunner([&] { coordinatorIo.run(); });

    boost::asio::io_context callerIo;
    auto future = boost::asio::co_spawn(callerIo, coord.requestRebuild(RebuildReason::Manual),
                                        boost::asio::use_future);
    callerIo.run();

    const auto result = future.get();
    CHECK(completionObservedDuringAdmission.load(std::memory_order_acquire));
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error().code == yams::ErrorCode::InvalidState);
    coordinatorIo.stop();
}

TEST_CASE("requestRebuildBlocking propagates rebuild failure", "[coordinator][rebuild][error]") {
    boost::asio::io_context io;
    const auto work = boost::asio::make_work_guard(io);
    VectorIndexCoordinator coord(io.get_executor(), nullptr, nullptr);
    yams::compat::jthread runner([&] { io.run(); });

    const auto result = coord.requestRebuildBlocking(RebuildReason::EmbeddingBatch);
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error().code == yams::ErrorCode::InvalidState);

    io.stop();
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
