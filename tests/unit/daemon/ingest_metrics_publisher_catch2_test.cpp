#include <catch2/catch_test_macros.hpp>

#include <thread>
#include <vector>
#include <yams/daemon/components/IngestMetricsPublisher.h>

using namespace yams::daemon;

TEST_CASE("IngestMetricsPublisher publish/read round-trip", "[daemon][ingest-metrics][catch2]") {
    IngestMetricsPublisher pub;

    pub.publishIngest(10, 3);
    auto snap = pub.snapshot();
    CHECK(snap.queued == 10);
    CHECK(snap.active == 3);
    CHECK(snap.target == 1);

    pub.publishQueued(42);
    pub.publishActive(7);
    snap = pub.snapshot();
    CHECK(snap.queued == 42);
    CHECK(snap.active == 7);
}

TEST_CASE("IngestMetricsPublisher workerTarget clamps to 1", "[daemon][ingest-metrics][catch2]") {
    IngestMetricsPublisher pub;

    pub.setWorkerTarget(0);
    CHECK(pub.workerTarget() == 1);

    pub.setWorkerTarget(16);
    CHECK(pub.workerTarget() == 16);
}

TEST_CASE("IngestMetricsPublisher worker job tracking", "[daemon][ingest-metrics][catch2]") {
    IngestMetricsPublisher pub;

    pub.onWorkerJobStart();
    pub.onWorkerJobStart();
    CHECK(pub.workerActive() == 2);
    CHECK(pub.workerPosted() == 2);
    CHECK(pub.workerCompleted() == 0);

    pub.onWorkerJobEnd();
    CHECK(pub.workerActive() == 1);
    CHECK(pub.workerCompleted() == 1);
}

TEST_CASE("IngestMetricsPublisher workerJobSignal callback", "[daemon][ingest-metrics][catch2]") {
    IngestMetricsPublisher pub;
    auto signal = pub.workerJobSignal();

    signal(true);
    signal(true);
    CHECK(pub.workerActive() == 2);
    CHECK(pub.workerPosted() == 2);

    signal(false);
    CHECK(pub.workerActive() == 1);
    CHECK(pub.workerCompleted() == 1);
}

TEST_CASE("IngestMetricsPublisher snapshot persistence counter",
          "[daemon][ingest-metrics][catch2]") {
    IngestMetricsPublisher pub;

    CHECK(pub.snapshotsPersistedCount() == 0);
    pub.onSnapshotPersisted();
    pub.onSnapshotPersisted();
    pub.onSnapshotPersisted();
    CHECK(pub.snapshotsPersistedCount() == 3);
}

TEST_CASE("IngestMetricsPublisher concurrent job signals", "[daemon][ingest-metrics][catch2]") {
    IngestMetricsPublisher pub;
    constexpr int iterations = 500;

    std::vector<std::thread> threads;
    threads.reserve(iterations * 2);

    for (int i = 0; i < iterations; ++i) {
        threads.emplace_back([&] { pub.onWorkerJobStart(); });
    }
    for (auto& t : threads)
        t.join();
    threads.clear();

    CHECK(pub.workerActive() == iterations);
    CHECK(pub.workerPosted() == iterations);

    for (int i = 0; i < iterations; ++i) {
        threads.emplace_back([&] { pub.onWorkerJobEnd(); });
    }
    for (auto& t : threads)
        t.join();

    CHECK(pub.workerActive() == 0);
    CHECK(pub.workerPosted() == iterations);
    CHECK(pub.workerCompleted() == iterations);
}
