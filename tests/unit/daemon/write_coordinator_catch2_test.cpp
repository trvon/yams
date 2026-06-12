#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/WriteCoordinator.h>

#include <boost/asio/io_context.hpp>

#include <chrono>
#include <memory>
#include <thread>

using yams::daemon::AddSymSpellTermsOp;
using yams::daemon::WriteBatch;
using yams::daemon::WriteCoordinator;

namespace {

std::unique_ptr<WriteBatch> makeTermBatch(const std::string& term) {
    auto batch = std::make_unique<WriteBatch>();
    batch->source = "test/write_coordinator_capacity";
    batch->ops.emplace_back(AddSymSpellTermsOp{{term}});
    return batch;
}

} // namespace

TEST_CASE("WriteCoordinator: tryEnqueue rejects batches at channel capacity",
          "[unit][daemon][write-coordinator]") {
    boost::asio::io_context io;
    WriteCoordinator::Config config;
    config.maxBatchSize = 8;
    config.maxBatchDelayMs = std::chrono::milliseconds{1};
    config.channelCapacity = 2;
    WriteCoordinator coordinator(io, {}, {}, config);

    coordinator.enqueue(makeTermBatch("alpha"));
    coordinator.enqueue(makeTermBatch("beta"));
    REQUIRE((coordinator.queuedBatches() == 2));

    auto rejected = makeTermBatch("gamma");
    CHECK_FALSE(coordinator.tryEnqueue(rejected));
    REQUIRE((rejected != nullptr));
    CHECK((coordinator.queuedBatches() == 2));

    coordinator.start();
    std::thread writerLoop([&io] { io.run(); });

    auto flushResult = coordinator.flush(std::chrono::seconds{10});
    REQUIRE((flushResult.has_value()));

    CHECK(coordinator.tryEnqueue(rejected));
    CHECK((rejected == nullptr));

    auto secondFlush = coordinator.flush(std::chrono::seconds{10});
    REQUIRE((secondFlush.has_value()));

    auto stats = coordinator.getStats();
    CHECK((stats.batchesEnqueued == 3));
    CHECK((stats.batchesCommitted == 3));

    coordinator.shutdown();
    io.stop();
    if (writerLoop.joinable()) {
        writerLoop.join();
    }
}

TEST_CASE("WriteCoordinator: shutdown drains queued batches and rejects new enqueue",
          "[unit][daemon][write-coordinator]") {
    boost::asio::io_context io;
    WriteCoordinator::Config config;
    config.maxBatchSize = 1;
    config.maxBatchDelayMs = std::chrono::milliseconds{1};
    config.channelCapacity = 8;
    WriteCoordinator coordinator(io, {}, {}, config);
    CHECK_FALSE(coordinator.isShuttingDown());

    coordinator.enqueue(makeTermBatch("alpha"));
    coordinator.enqueue(makeTermBatch("beta"));
    REQUIRE((coordinator.queuedBatches() == 2));

    coordinator.start();
    std::thread writerLoop([&io] { io.run(); });

    coordinator.shutdown();
    io.stop();
    if (writerLoop.joinable()) {
        writerLoop.join();
    }

    CHECK((coordinator.queuedBatches() == 0));
    CHECK(coordinator.isShuttingDown());
    auto stats = coordinator.getStats();
    CHECK((stats.batchesEnqueued == 2));
    CHECK((stats.batchesCommitted == 2));

    auto lateBatch = makeTermBatch("gamma");
    CHECK_FALSE(coordinator.tryEnqueue(lateBatch));
    CHECK((lateBatch != nullptr));
    CHECK((coordinator.queuedBatches() == 0));
}
