// Tests: seqlock telemetry snapshot — consistent reads from another thread, monotonic epoch.
// Compile with -DYAMS_TESTING=1

#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/VectorIndexCoordinator.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>

#include <atomic>
#include <future>
#include <thread>
#include <vector>

using namespace yams::daemon;

TEST_CASE("snapshot: lock-free reads return consistent telemetry during rebuild",
          "[coordinator][telemetry][seqlock]") {
    boost::asio::io_context io;
    VectorIndexCoordinator coord(io.get_executor(), nullptr, nullptr);

    constexpr int kReaderIterations = 5000;
    constexpr int kRebuildCount = 10;

    std::atomic<bool> done{false};
    std::vector<VectorIndexTelemetry> observations;
    observations.reserve(kReaderIterations);

    // Spawn a background reader thread that samples the snapshot repeatedly.
    auto readerThread = std::thread([&] {
        while (!done.load(std::memory_order_relaxed)) {
            observations.push_back(coord.snapshot());
        }
    });

    // Trigger a series of rebuilds on the io_context thread.
    for (int i = 0; i < kRebuildCount; ++i) {
        auto f = boost::asio::co_spawn(io, coord.requestRebuild(RebuildReason::Manual),
                                       boost::asio::use_future);
        io.run();
        io.restart();
        REQUIRE_NOTHROW(f.get());
    }

    done.store(true, std::memory_order_relaxed);
    readerThread.join();

    // Verify: rebuildEpoch is monotonically non-decreasing in the observations.
    uint64_t prevEpoch = 0;
    for (const auto& t : observations) {
        REQUIRE(t.rebuildEpoch >= prevEpoch);
        prevEpoch = t.rebuildEpoch;
    }

    // Final epoch must equal kRebuildCount (one per rebuild, no extra).
    REQUIRE(coord.testing_rebuildEpoch() == static_cast<uint64_t>(kRebuildCount));

    // No active scopes remain.
    REQUIRE(coord.testing_activeScopes() == 0u);

    // Final snapshot must show ready=true and epoch==kRebuildCount.
    const auto final_snap = coord.snapshot();
    REQUIRE(final_snap.rebuildEpoch == static_cast<uint64_t>(kRebuildCount));
    REQUIRE(final_snap.rebuilding == false);
    REQUIRE(final_snap.ready == true);
}

TEST_CASE("snapshot: epoch is monotonic across BulkScope releases", "[coordinator][telemetry]") {
    boost::asio::io_context io;
    VectorIndexCoordinator coord(io.get_executor(), nullptr, nullptr);

    const uint64_t e0 = coord.snapshot().rebuildEpoch;
    REQUIRE(e0 == 0u);

    {
        auto s = coord.beginBulkIngest(RebuildReason::EmbeddingBatch);
    }
    io.run();
    const uint64_t e1 = coord.snapshot().rebuildEpoch;
    REQUIRE(e1 == 1u);
    REQUIRE(e1 > e0);

    io.restart();
    {
        auto s = coord.beginBulkIngest(RebuildReason::EmbeddingBatch);
    }
    io.run();
    const uint64_t e2 = coord.snapshot().rebuildEpoch;
    REQUIRE(e2 == 2u);
    REQUIRE(e2 > e1);
}
