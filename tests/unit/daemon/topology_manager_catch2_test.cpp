#include <catch2/catch_test_macros.hpp>

#include <thread>
#include <vector>
#include <yams/daemon/components/TopologyManager.h>

using namespace yams::daemon;

static TopologyManager::Dependencies nullDeps() {
    return {
        [] { return std::shared_ptr<yams::metadata::MetadataRepository>{}; },
        [] { return std::shared_ptr<yams::metadata::KnowledgeGraphStore>{}; },
        [] { return std::shared_ptr<yams::vector::VectorDatabase>{}; },
    };
}

TEST_CASE("TopologyManager markDirty and hasDirtyHashes", "[daemon][topology-manager][catch2]") {
    TopologyManager mgr(nullDeps());

    CHECK_FALSE(mgr.hasDirtyHashes());
    mgr.markDirty("hash1");
    CHECK(mgr.hasDirtyHashes());
}

TEST_CASE("TopologyManager markDirty ignores empty strings", "[daemon][topology-manager][catch2]") {
    TopologyManager mgr(nullDeps());

    mgr.markDirty("");
    CHECK_FALSE(mgr.hasDirtyHashes());
}

TEST_CASE("TopologyManager markDirtyBatch inserts multiple hashes",
          "[daemon][topology-manager][catch2]") {
    TopologyManager mgr(nullDeps());

    mgr.markDirtyBatch({"a", "b", "", "c"});
    auto overlay = mgr.getOverlayHashes(100);
    CHECK(overlay.size() == 3);
}

TEST_CASE("TopologyManager getOverlayHashes respects limit", "[daemon][topology-manager][catch2]") {
    TopologyManager mgr(nullDeps());

    mgr.markDirtyBatch({"a", "b", "c", "d", "e"});
    auto limited = mgr.getOverlayHashes(2);
    CHECK(limited.size() == 2);
}

TEST_CASE("TopologyManager drainDirtyHashes clears state", "[daemon][topology-manager][catch2]") {
    TopologyManager mgr(nullDeps());

    mgr.markDirtyBatch({"x", "y", "z"});
    auto drained = mgr.drainDirtyHashes();
    CHECK(drained.size() == 3);
    CHECK_FALSE(mgr.hasDirtyHashes());
}

TEST_CASE("TopologyManager restoreDirtyHashes re-inserts", "[daemon][topology-manager][catch2]") {
    TopologyManager mgr(nullDeps());

    mgr.markDirty("a");
    auto drained = mgr.drainDirtyHashes();
    CHECK_FALSE(mgr.hasDirtyHashes());

    mgr.restoreDirtyHashes(drained);
    CHECK(mgr.hasDirtyHashes());
    CHECK(mgr.getOverlayHashes().size() == 1);
}

TEST_CASE("TopologyManager tryScheduleRebuild is one-shot", "[daemon][topology-manager][catch2]") {
    TopologyManager mgr(nullDeps());

    CHECK(mgr.tryScheduleRebuild());
    CHECK_FALSE(mgr.tryScheduleRebuild());

    mgr.clearScheduled();
    CHECK(mgr.tryScheduleRebuild());
}

TEST_CASE("TopologyManager initial state", "[daemon][topology-manager][catch2]") {
    TopologyManager mgr(nullDeps());

    CHECK_FALSE(mgr.isRebuildInProgress());
    CHECK(mgr.publishedEpoch() == 0);

    auto telemetry = mgr.getTelemetrySnapshot();
    CHECK_FALSE(telemetry.rebuildRunning);
    CHECK(telemetry.rebuildsTotal == 0);
    CHECK(telemetry.dirtyDocumentCount == 0);
}

TEST_CASE("TopologyManager telemetry tracks dirty count", "[daemon][topology-manager][catch2]") {
    TopologyManager mgr(nullDeps());

    mgr.markDirtyBatch({"a", "b"});
    auto telemetry = mgr.getTelemetrySnapshot();
    CHECK(telemetry.dirtyDocumentCount == 2);
    CHECK(telemetry.dirtySinceUnixMillis > 0);
}

TEST_CASE("TopologyManager rebuildArtifacts fails without deps",
          "[daemon][topology-manager][catch2]") {
    TopologyManager mgr(nullDeps());

    auto result = mgr.rebuildArtifacts("test", false, {"hash1"}, "connected");
    CHECK_FALSE(result.has_value());

    auto telemetry = mgr.getTelemetrySnapshot();
    CHECK(telemetry.rebuildsTotal == 1);
    CHECK(telemetry.rebuildFailuresTotal == 1);
    CHECK_FALSE(telemetry.rebuildRunning);
}

TEST_CASE("TopologyManager rebuildArtifacts concurrency guard",
          "[daemon][topology-manager][catch2]") {
    TopologyManager mgr(nullDeps());

    std::atomic<bool> firstStarted{false};
    std::atomic<bool> secondDone{false};
    TopologyManager::Dependencies slowDeps;
    slowDeps.getMetadataRepo = [&]() -> std::shared_ptr<yams::metadata::MetadataRepository> {
        firstStarted.store(true);
        while (!secondDone.load(std::memory_order_relaxed))
            std::this_thread::yield();
        return nullptr;
    };
    slowDeps.getKgStore = nullDeps().getKgStore;
    slowDeps.getVectorDatabase = nullDeps().getVectorDatabase;

    TopologyManager slowMgr(std::move(slowDeps));
    std::thread t1([&] { slowMgr.rebuildArtifacts("first", false, {"h"}, "connected"); });

    while (!firstStarted.load(std::memory_order_relaxed))
        std::this_thread::yield();

    auto result = slowMgr.rebuildArtifacts("second", false, {"h"}, "connected");
    CHECK(result.has_value());
    CHECK(result.value().skipped);
    CHECK(result.value().issues.size() == 1);

    secondDone.store(true);
    t1.join();
}
