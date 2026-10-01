// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <string>
#include <vector>

#include <yams/memory_sync/apply_health.h>

using yams::memory_sync::ApplyHealth;
using yams::memory_sync::ApplyStage;
using namespace std::chrono_literals;

TEST_CASE("apply health keeps the first-seen time of a deferral across passes",
          "[memory-sync][apply-health][defer]") {
    ApplyHealth health;
    const auto start = ApplyHealth::Clock::time_point{} + 1h;

    const std::vector<std::string> both{"embedding/m/a", "embedding/m/b"};
    CHECK(health.recordDeferred(ApplyStage::Vector, both, start).empty());
    auto snapshot = health.snapshot(start + 5s);
    CHECK(snapshot.deferredIn(ApplyStage::Vector) == 2);
    CHECK(snapshot.deferredTotal() == 2);
    CHECK(snapshot.oldestDeferralAgeMs == 5000);

    // `a` resolves; `b` is still deferred and keeps its original first-seen time.
    const std::vector<std::string> onlyB{"embedding/m/b"};
    CHECK(health.recordDeferred(ApplyStage::Vector, onlyB, start + 10s).empty());
    snapshot = health.snapshot(start + 12s);
    CHECK(snapshot.deferredIn(ApplyStage::Vector) == 1);
    CHECK(snapshot.oldestDeferralAgeMs == 12000);

    // A newly deferred record in another stage does not reset the oldest age.
    const std::vector<std::string> edge{"topology-edge/x/rel/y"};
    CHECK(health.recordDeferred(ApplyStage::Topology, edge, start + 12s).empty());
    snapshot = health.snapshot(start + 13s);
    CHECK(snapshot.deferredTotal() == 2);
    CHECK(snapshot.oldestDeferralAgeMs == 13000);

    // Once every prerequisite lands, nothing is deferred and the age resets.
    CHECK(health.recordDeferred(ApplyStage::Vector, {}, start + 20s).empty());
    CHECK(health.recordDeferred(ApplyStage::Topology, {}, start + 20s).empty());
    snapshot = health.snapshot(start + 21s);
    CHECK(snapshot.deferredTotal() == 0);
    CHECK(snapshot.oldestDeferralAgeMs == 0);
}

TEST_CASE("apply health reports a deferral that outlives the stale age exactly once",
          "[memory-sync][apply-health][defer]") {
    ApplyHealth health;
    const auto start = ApplyHealth::Clock::time_point{} + 1h;
    const std::vector<std::string> keys{"document/abc"};

    CHECK(health.recordDeferred(ApplyStage::Metadata, keys, start).empty());
    CHECK(health.recordDeferred(ApplyStage::Metadata, keys, start + 1min).empty());
    const auto stale =
        health.recordDeferred(ApplyStage::Metadata, keys, start + ApplyHealth::kStaleDeferralAge);
    CHECK(stale == keys);
    CHECK(health
              .recordDeferred(ApplyStage::Metadata, keys,
                              start + ApplyHealth::kStaleDeferralAge + 1min)
              .empty());
}

TEST_CASE("apply health bounds tracked deferrals but keeps an exact count",
          "[memory-sync][apply-health][defer][bounded]") {
    ApplyHealth health;
    const auto start = ApplyHealth::Clock::time_point{} + 1h;
    const std::vector<std::string> oldest{"content-blob/oldest"};
    CHECK(health.recordDeferred(ApplyStage::Content, oldest, start).empty());

    std::vector<std::string> many{"content-blob/oldest"};
    for (std::size_t i = 0; i < ApplyHealth::kMaxTrackedDeferralsPerStage + 10; ++i) {
        many.push_back("content-blob/" + std::to_string(i));
    }
    CHECK(health.recordDeferred(ApplyStage::Content, many, start + 1min).empty());
    const auto snapshot = health.snapshot(start + 2min);
    CHECK(snapshot.deferredIn(ApplyStage::Content) == many.size());
    // The cap never evicts the longest-standing deferral.
    CHECK(snapshot.oldestDeferralAgeMs == 120000);
}

TEST_CASE("apply health counts failures, failed cycles, and publish outcomes",
          "[memory-sync][apply-health]") {
    ApplyHealth health;
    const auto start = ApplyHealth::Clock::time_point{} + 1h;

    auto snapshot = health.snapshot(start);
    CHECK(snapshot.applyCycles == 0);
    CHECK(snapshot.lastFailureStage.empty());

    health.recordCycle(false);
    health.recordFailure(ApplyStage::Topology, "edge identity mismatch", start);
    health.recordCycle(true);
    health.recordPublishSkipped();
    health.recordPublishFailed();
    health.recordPublishFailed();

    snapshot = health.snapshot(start + 3s);
    CHECK(snapshot.applyCycles == 2);
    CHECK(snapshot.applyFailedCycles == 1);
    CHECK(snapshot.failuresIn(ApplyStage::Topology) == 1);
    CHECK(snapshot.failuresIn(ApplyStage::Vector) == 0);
    CHECK(snapshot.lastFailureStage == "topology");
    CHECK(snapshot.lastFailure == "edge identity mismatch");
    CHECK(snapshot.lastFailureAgeMs == 3000);
    CHECK(snapshot.publishSkippedCycles == 1);
    CHECK(snapshot.publishFailedCycles == 2);
}
