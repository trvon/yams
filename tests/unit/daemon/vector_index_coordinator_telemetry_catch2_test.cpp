// Tests: synchronized telemetry snapshot — consistent reads from another thread, monotonic epoch.
// Compile with -DYAMS_TESTING=1

#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/VectorIndexCoordinator.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <mutex>
#include <thread>
#include <vector>

using namespace yams::daemon;
using namespace std::chrono_literals;

TEST_CASE("requestCheckpoint: stalled strand does not block the caller forever",
          "[coordinator][checkpoint]") {
    boost::asio::io_context io;
    VectorIndexCoordinator coord(io.get_executor(), nullptr, nullptr);
    coord.testing_setCheckpointWaitTimeout(20ms);

    std::mutex mutex;
    std::condition_variable completedCv;
    bool completed = false;
    bool result = true;
    std::thread checkpointThread([&] {
        result = coord.requestCheckpoint();
        {
            std::lock_guard lock(mutex);
            completed = true;
        }
        completedCv.notify_one();
    });

    bool completedBeforeExecutorRan = false;
    {
        std::unique_lock lock(mutex);
        completedBeforeExecutorRan = completedCv.wait_for(lock, 250ms, [&] { return completed; });
    }

    // Let the old unbounded implementation finish so a red test cannot hang its process.
    if (!completedBeforeExecutorRan) {
        io.run();
    }
    checkpointThread.join();

    REQUIRE(completedBeforeExecutorRan);
    CHECK_FALSE(result);
}

TEST_CASE("requestCheckpoint: repeated timeouts keep one persistence callback queued",
          "[coordinator][checkpoint][coalesce]") {
    boost::asio::io_context io;
    VectorIndexCoordinator coord(io.get_executor(), nullptr, nullptr);
    coord.testing_setCheckpointWaitTimeout(10ms);

    constexpr std::size_t kCallers = 8;
    std::mutex gateMutex;
    std::condition_variable gateCv;
    std::size_t ready = 0;
    bool start = false;
    std::array<bool, kCallers> results{};
    std::vector<std::thread> callers;
    callers.reserve(kCallers);

    for (std::size_t i = 0; i < kCallers; ++i) {
        callers.emplace_back([&, i] {
            {
                std::unique_lock lock(gateMutex);
                ++ready;
                gateCv.notify_all();
                gateCv.wait(lock, [&] { return start; });
            }
            results[i] = coord.requestCheckpoint();
        });
    }

    {
        std::unique_lock lock(gateMutex);
        REQUIRE(gateCv.wait_for(lock, 1s, [&] { return ready == kCallers; }));
        start = true;
    }
    gateCv.notify_all();

    for (auto& caller : callers) {
        caller.join();
    }
    for (bool result : results) {
        CHECK_FALSE(result);
    }

    const auto queued = coord.checkpointSnapshot();
    CHECK(queued.phase == VectorCheckpointPhase::Queued);
    CHECK(queued.generation == 1);
    CHECK(queued.requests == kCallers);
    CHECK(queued.coalesced == kCallers - 1);
    CHECK(queued.started == 0);
    CHECK(queued.completed == 0);
    CHECK(queued.timedOut == kCallers);

    io.run();

    const auto completed = coord.checkpointSnapshot();
    CHECK(coord.testing_checkpointCallbacksStarted() == 1);
    CHECK(completed.phase == VectorCheckpointPhase::Idle);
    CHECK(completed.started == 1);
    CHECK(completed.completed == 1);
    CHECK(completed.postFailures == 0);
}

TEST_CASE("requestCheckpoint: timed-out callback may run after coordinator destruction",
          "[coordinator][checkpoint][lifetime]") {
    boost::asio::io_context io;
    {
        VectorIndexCoordinator coord(io.get_executor(), nullptr, nullptr);
        coord.testing_setCheckpointWaitTimeout(5ms);
        CHECK_FALSE(coord.requestCheckpoint());
        CHECK(coord.checkpointSnapshot().phase == VectorCheckpointPhase::Queued);
    }

    CHECK(io.run() == 1);
}

TEST_CASE("snapshot: concurrent reads return consistent telemetry during rebuild",
          "[coordinator][telemetry]") {
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

    // A coordinator without a vector database records each failed epoch but never reports ready.
    const auto final_snap = coord.snapshot();
    REQUIRE(final_snap.rebuildEpoch == static_cast<uint64_t>(kRebuildCount));
    REQUIRE(final_snap.rebuilding == false);
    REQUIRE(final_snap.ready == false);
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
