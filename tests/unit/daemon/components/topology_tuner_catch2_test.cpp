// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/TopologyTuner.h>

#include <chrono>

using Catch::Approx;
using yams::daemon::computeIntrinsicReward;
using yams::daemon::defaultArmGrid;
using yams::daemon::IntrinsicRewardWeights;
using yams::daemon::TopologyArm;
using yams::daemon::TopologyTuner;
using yams::daemon::TopologyTunerConfig;
using RebuildStats = yams::daemon::TopologyManager::RebuildStats;

namespace {
RebuildStats makeStats(double singleton, double giant, double gini, double intra,
                       std::uint64_t docs = 1000) {
    RebuildStats s;
    s.skipped = false;
    s.documentsProcessed = docs;
    s.singletonRatio = singleton;
    s.giantClusterRatio = giant;
    s.clusterSizeGini = gini;
    s.avgIntraEdgeWeight = intra;
    return s;
}
} // namespace

TEST_CASE("TopologyTuner: defaultArmGrid covers engine + HDBSCAN params",
          "[unit][daemon][topology_tuner][catch2]") {
    auto arms = defaultArmGrid(5183);

    REQUIRE(!arms.empty());

    bool seenConnected = false;
    bool seenHdbscanWithSmoothing = false;
    bool seenHdbscanNoSmoothing = false;
    for (const auto& arm : arms) {
        if (arm.engine == "connected") {
            seenConnected = true;
        }
        if (arm.engine == "hdbscan" && arm.featureSmoothingHops > 0) {
            seenHdbscanWithSmoothing = true;
        }
        if (arm.engine == "hdbscan" && arm.featureSmoothingHops == 0) {
            seenHdbscanNoSmoothing = true;
        }
    }
    CHECK(seenConnected);
    CHECK(seenHdbscanWithSmoothing);
    CHECK(seenHdbscanNoSmoothing);

    // No two arms should share the same id (deduplication invariant).
    for (std::size_t i = 0; i < arms.size(); ++i) {
        for (std::size_t j = i + 1; j < arms.size(); ++j) {
            CHECK(arms[i].id != arms[j].id);
        }
    }
}

TEST_CASE("TopologyTuner: computeIntrinsicReward favors balanced clusterings",
          "[unit][daemon][topology_tuner][catch2]") {
    IntrinsicRewardWeights w;

    SECTION("singleton-heavy clustering yields a low reward") {
        auto stats = makeStats(/*singleton=*/0.9, /*giant=*/0.0, /*gini=*/0.4, /*intra=*/0.3);
        const double r = computeIntrinsicReward(stats, w);
        CHECK(r < 0.7);
    }

    SECTION("giant-cluster collapse yields a low reward") {
        auto stats = makeStats(/*singleton=*/0.0, /*giant=*/0.9, /*gini=*/0.4, /*intra=*/0.3);
        const double r = computeIntrinsicReward(stats, w);
        CHECK(r < 0.7);
    }

    SECTION("balanced clustering yields a high reward") {
        auto stats = makeStats(/*singleton=*/0.05, /*giant=*/0.10, /*gini=*/0.4, /*intra=*/0.5);
        const double r = computeIntrinsicReward(stats, w);
        CHECK(r > 0.85);
    }

    SECTION("skipped or empty rebuilds yield zero") {
        RebuildStats skipped;
        skipped.skipped = true;
        skipped.documentsProcessed = 1000;
        CHECK(computeIntrinsicReward(skipped, w) == Approx(0.0));

        RebuildStats empty;
        empty.skipped = false;
        empty.documentsProcessed = 0;
        CHECK(computeIntrinsicReward(empty, w) == Approx(0.0));
    }

    SECTION("reward stays clamped to [0, 1] under extreme inputs") {
        auto stats = makeStats(/*singleton=*/2.0, /*giant=*/2.0, /*gini=*/-5.0, /*intra=*/100.0);
        const double r = computeIntrinsicReward(stats, w);
        CHECK(r >= 0.0);
        CHECK(r <= 1.0);
    }
}

TEST_CASE("TopologyTuner: cooldown gate blocks back-to-back pulls",
          "[unit][daemon][topology_tuner][catch2]") {
    TopologyTunerConfig cfg;
    cfg.enabled = true;
    cfg.cooldown = std::chrono::minutes(10);
    cfg.docCountDelta = 100;

    TopologyTuner tuner(cfg);
    tuner.setArms(defaultArmGrid(1000));

    using clock = std::chrono::steady_clock;
    const auto t0 = clock::now();

    SECTION("just-past-rebuild blocks") {
        const auto justAfter = t0 + std::chrono::seconds(30);
        CHECK_FALSE(tuner.canPullArm(justAfter, t0, std::chrono::milliseconds(1500),
                                     /*corpusDocCount=*/1000, /*lastPullDocCount=*/900));
    }

    SECTION("after cooldown but no new docs blocks") {
        const auto afterCooldown = t0 + std::chrono::minutes(11);
        CHECK_FALSE(tuner.canPullArm(afterCooldown, t0, std::chrono::milliseconds(500),
                                     /*corpusDocCount=*/950, /*lastPullDocCount=*/900));
    }

    SECTION("after cooldown and enough new docs unblocks") {
        const auto afterCooldown = t0 + std::chrono::minutes(11);
        CHECK(tuner.canPullArm(afterCooldown, t0, std::chrono::milliseconds(500),
                               /*corpusDocCount=*/1100, /*lastPullDocCount=*/900));
    }

    SECTION("cooldown scales with last rebuild duration") {
        // 60s rebuild → 600s scaled cooldown; 11min wall is enough but
        // a 12min rebuild → scaled cooldown 120 min, blocks at 11 min.
        const auto eleven = t0 + std::chrono::minutes(11);
        CHECK(tuner.canPullArm(eleven, t0, std::chrono::seconds(60),
                               /*corpus=*/1100, /*last=*/900));
        CHECK_FALSE(tuner.canPullArm(eleven, t0, std::chrono::minutes(12),
                                     /*corpus=*/1100, /*last=*/900));
    }

    SECTION("disabled tuner never pulls") {
        TopologyTunerConfig off;
        off.enabled = false;
        TopologyTuner offTuner(off);
        offTuner.setArms(defaultArmGrid(1000));
        const auto wayLater = t0 + std::chrono::hours(24);
        CHECK_FALSE(offTuner.canPullArm(wayLater, t0, std::chrono::milliseconds(0),
                                        /*corpus=*/100000, /*last=*/0));
    }
}

TEST_CASE("TopologyTuner: selectArm + observeRebuildStats round-trip via UCB1",
          "[unit][daemon][topology_tuner][catch2]") {
    TopologyTunerConfig cfg;
    cfg.enabled = true;
    TopologyTuner tuner(cfg);
    tuner.setArms(defaultArmGrid(1000));

    // Pull every arm once with a constant reward so UCB1 is fully warmed.
    auto good = makeStats(0.05, 0.10, 0.4, 0.5);
    for (std::size_t i = 0; i < tuner.arms().size(); ++i) {
        auto picked = tuner.selectArm();
        REQUIRE(picked.has_value());
        tuner.observeRebuildStats(picked->id, good);
    }

    // After every arm has at least one observation, bestArmId should be
    // populated; with identical rewards it just picks the first by id order
    // — the contract is "non-empty after at least one pull", not a specific id.
    auto best = tuner.bestArmId();
    CHECK(best.has_value());
}

TEST_CASE("TopologyTuner: disabled tuner returns nullopt from selectArm",
          "[unit][daemon][topology_tuner][catch2]") {
    TopologyTunerConfig cfg;
    cfg.enabled = false;
    TopologyTuner tuner(cfg);
    tuner.setArms(defaultArmGrid(1000));
    CHECK_FALSE(tuner.selectArm().has_value());
}
