// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
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

    // The still-deferred tracked key comes after more new keys than the cap holds, so a fill
    // that stops at the cap in input order would drop it and reset the oldest age.
    std::vector<std::string> many;
    for (std::size_t i = 0; i < ApplyHealth::kMaxTrackedDeferralsPerStage + 10; ++i) {
        many.push_back("content-blob/" + std::to_string(i));
    }
    many.push_back("content-blob/oldest");
    CHECK(health.recordDeferred(ApplyStage::Content, many, start + 1min).empty());
    const auto snapshot = health.snapshot(start + 2min);
    CHECK(snapshot.deferredIn(ApplyStage::Content) == many.size());
    // The cap never evicts the longest-standing deferral.
    CHECK(snapshot.oldestDeferralAgeMs == 120000);
    // ...and its stale warning still fires on time.
    const auto stale =
        health.recordDeferred(ApplyStage::Content, many, start + ApplyHealth::kStaleDeferralAge);
    CHECK(std::find(stale.begin(), stale.end(), "content-blob/oldest") != stale.end());
}

TEST_CASE("apply health keeps every tracked deferral when the tracked set is full",
          "[memory-sync][apply-health][defer][bounded]") {
    ApplyHealth health;
    const auto start = ApplyHealth::Clock::time_point{} + 1h;
    std::vector<std::string> first;
    for (std::size_t i = 0; i < ApplyHealth::kMaxTrackedDeferralsPerStage; ++i) {
        first.push_back("old/" + std::to_string(i));
    }
    CHECK(health.recordDeferred(ApplyStage::Topology, first, start).empty());

    // New keys first, then every old key: none of the old first-seen times may be replaced.
    std::vector<std::string> next;
    for (std::size_t i = 0; i < ApplyHealth::kMaxTrackedDeferralsPerStage; ++i) {
        next.push_back("new/" + std::to_string(i));
    }
    next.insert(next.end(), first.begin(), first.end());
    CHECK(health.recordDeferred(ApplyStage::Topology, next, start + 1min).empty());
    // Only old keys are tracked, so all of them turn stale together at the stale age.
    const auto stale =
        health.recordDeferred(ApplyStage::Topology, next, start + ApplyHealth::kStaleDeferralAge);
    REQUIRE(stale.size() == ApplyHealth::kMaxStaleReportsPerPass);
    for (const auto& key : stale) {
        CHECK(key.starts_with("old/"));
    }
    CHECK(health.snapshot(start + 2min).oldestDeferralAgeMs == 120000);
}

TEST_CASE("apply health records a pass that both failed and deferred",
          "[memory-sync][apply-health][defer]") {
    ApplyHealth health;
    const auto start = ApplyHealth::Clock::time_point{} + 1h;

    // A full content scan hit one corrupt blob and one blob whose bytes are not hydrated yet.
    yams::memory_sync::ApplyStagePass pass;
    pass.deferredKeys = {"content-blob/not-hydrated"};
    pass.failure = "replicated content bytes do not match the claimed hash";
    CHECK(health.recordPass(ApplyStage::Content, pass, start).empty());

    auto snapshot = health.snapshot(start + 1s);
    CHECK(snapshot.failuresIn(ApplyStage::Content) == 1);
    CHECK(snapshot.lastFailureStage == "content");
    CHECK(snapshot.deferredIn(ApplyStage::Content) == 1);
    CHECK(snapshot.oldestDeferralAgeMs == 1000);

    // A clean pass replaces the deferred set and adds no failure.
    CHECK(health.recordPass(ApplyStage::Content, {}, start + 2s).empty());
    snapshot = health.snapshot(start + 3s);
    CHECK(snapshot.failuresIn(ApplyStage::Content) == 1);
    CHECK(snapshot.deferredIn(ApplyStage::Content) == 0);
}

TEST_CASE("apply health keeps exact totals separate from bounded diagnostic samples",
          "[memory-sync][apply-health][sampled-total]") {
    ApplyHealth health;
    const auto start = ApplyHealth::Clock::time_point{} + 1h;
    yams::memory_sync::ApplyStagePass topology;
    topology.deferredKeys = {"topology-edge/a/rel/b", "topology-edge/c/rel/d"};
    topology.deferredCount = 70;
    topology.failure = "independent row failed";
    CHECK(health.recordPass(ApplyStage::Topology, topology, start).empty());
    auto snapshot = health.snapshot(start + 5s);
    CHECK(snapshot.deferredIn(ApplyStage::Topology) == 70);
    CHECK(snapshot.failuresIn(ApplyStage::Topology) == 1);
    CHECK(snapshot.oldestDeferralAgeMs == 5000);
    yams::memory_sync::ApplyStagePass metadata;
    metadata.deferredKeys = {"document/sampled"};
    metadata.deferredCount = 100;
    CHECK(health.recordPass(ApplyStage::Metadata, metadata, start + 5s).empty());
    CHECK(health.snapshot(start + 6s).deferredTotal() == 170);
    // Incomplete scans cannot overwrite the previously observed exact total.
    health.recordFailure(ApplyStage::Topology, "later scan interrupted", start + 6s);
    CHECK(health.snapshot(start + 7s).deferredIn(ApplyStage::Topology) == 70);
    topology.failure.reset();
    topology.deferredKeys = {"topology-edge/c/rel/d"};
    topology.deferredCount = 80;
    CHECK(health.recordPass(ApplyStage::Topology, topology, start + 10s).empty());
    snapshot = health.snapshot(start + 12s);
    CHECK(snapshot.deferredIn(ApplyStage::Topology) == 80);
    CHECK(snapshot.deferredTotal() == 180);
    CHECK(snapshot.oldestDeferralAgeMs == 12000);
    CHECK(snapshot.failuresIn(ApplyStage::Topology) == 2);
    CHECK(health.recordPass(ApplyStage::Topology, {}, start + 20s).empty());
    CHECK(health.recordPass(ApplyStage::Metadata, {}, start + 20s).empty());
    snapshot = health.snapshot(start + 21s);
    CHECK(snapshot.deferredTotal() == 0);
    CHECK(snapshot.oldestDeferralAgeMs == 0);
}

TEST_CASE("apply health accepts counter-only passes without undercounting known sampled keys",
          "[memory-sync][apply-health][sampled-total]") {
    ApplyHealth health;
    const auto start = ApplyHealth::Clock::time_point{} + 1h;
    yams::memory_sync::ApplyStagePass pass;
    pass.deferredCount = 70;
    CHECK(health.recordPass(ApplyStage::Topology, pass, start).empty());
    CHECK(health.snapshot(start + 1s).deferredIn(ApplyStage::Topology) == 70);
    pass.deferredKeys = {"a", "b"};
    pass.deferredCount = 1; // An inconsistent producer must never hide known keys.
    CHECK(health.recordPass(ApplyStage::Topology, pass, start + 2s).empty());
    CHECK(health.snapshot(start + 3s).deferredIn(ApplyStage::Topology) == 2);
    pass.deferredCount.reset();
    CHECK(health.recordPass(ApplyStage::Topology, pass, start + 4s).empty());
    CHECK(health.snapshot(start + 5s).deferredIn(ApplyStage::Topology) == 2);
    pass.deferredKeys.clear();
    pass.deferredCount = 0;
    CHECK(health.recordPass(ApplyStage::Topology, pass, start + 6s).empty());
    CHECK(health.snapshot(start + 7s).deferredTotal() == 0);
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

TEST_CASE("apply health preserves the last complete observation during admission deferral",
          "[memory-sync][apply-health][incomplete]") {
    ApplyHealth health;
    const auto start = ApplyHealth::Clock::time_point{} + 1h;
    CHECK_FALSE(health.snapshot(start).scannedAllIn(ApplyStage::Topology));
    yams::memory_sync::ApplyStagePass complete;
    complete.deferredKeys = {"topology-edge/a/rel/b"};
    complete.deferredCount = 70;
    CHECK(health.recordPass(ApplyStage::Topology, complete, start).empty());
    CHECK(health.snapshot(start + 5s).scannedAllIn(ApplyStage::Topology));

    yams::memory_sync::ApplyStagePass interrupted;
    interrupted.scanComplete = false;
    interrupted.deferredKeys = {"partial-observation-is-not-an-exact-set"};
    interrupted.deferredCount = 1;
    CHECK(health.recordPass(ApplyStage::Topology, interrupted, start + 6s).empty());
    const auto pending = health.snapshot(start + 7s);
    CHECK_FALSE(pending.scannedAllIn(ApplyStage::Topology));
    CHECK(pending.deferredIn(ApplyStage::Topology) == 70);
    CHECK(pending.oldestDeferralAgeMs == 7000);
    CHECK(pending.failuresIn(ApplyStage::Topology) == 0);
    CHECK(pending.lastFailureStage.empty());

    CHECK(health.recordPass(ApplyStage::Topology, {}, start + 8s).empty());
    const auto recovered = health.snapshot(start + 9s);
    CHECK(recovered.scannedAllIn(ApplyStage::Topology));
    CHECK(recovered.deferredIn(ApplyStage::Topology) == 0);
    CHECK(recovered.oldestDeferralAgeMs == 0);
}

TEST_CASE("apply health interruption cannot hide hard failures or clear known counts",
          "[memory-sync][apply-health][incomplete]") {
    ApplyHealth health;
    const auto start = ApplyHealth::Clock::time_point{} + 1h;
    yams::memory_sync::ApplyStagePass complete;
    complete.deferredKeys = {"topology-edge/a/rel/b"};
    complete.deferredCount = 70;
    CHECK(health.recordPass(ApplyStage::Topology, complete, start).empty());
    yams::memory_sync::ApplyStagePass interrupted;
    interrupted.scanComplete = false;
    interrupted.failure = "actual scratch capacity failure";
    CHECK(health.recordPass(ApplyStage::Topology, interrupted, start + 1s).empty());
    auto snapshot = health.snapshot(start + 2s);
    CHECK_FALSE(snapshot.scannedAllIn(ApplyStage::Topology));
    CHECK(snapshot.deferredIn(ApplyStage::Topology) == 70);
    CHECK(snapshot.failuresIn(ApplyStage::Topology) == 1);
    CHECK(snapshot.lastFailureStage == "topology");
    CHECK(snapshot.lastFailure == "actual scratch capacity failure");
    health.recordFailure(ApplyStage::Topology, "later I/O failure", start + 3s);
    snapshot = health.snapshot(start + 4s);
    CHECK_FALSE(snapshot.scannedAllIn(ApplyStage::Topology));
    CHECK(snapshot.deferredIn(ApplyStage::Topology) == 70);
    CHECK(snapshot.failuresIn(ApplyStage::Topology) == 2);
    CHECK(snapshot.lastFailure == "later I/O failure");
}
