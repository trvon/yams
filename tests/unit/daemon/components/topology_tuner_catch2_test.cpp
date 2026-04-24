// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/TopologyTuner.h>

#include <chrono>
#include <filesystem>
#include <fstream>

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

    SECTION("quadratic penalty: giant=1.0 collapse scores well below half") {
        // With the plan-G quadratic formula (α=β=0.7), giant=1.0 removes 0.7
        // from the baseline 1.0, leaving ~0.3 plus a small intra bonus. This
        // is the pathological all-in-one-cluster case from Phase G bench.
        auto stats = makeStats(/*singleton=*/0.0, /*giant=*/1.0, /*gini=*/0.0, /*intra=*/0.5);
        const double r = computeIntrinsicReward(stats, w);
        CHECK(r < 0.45);
    }

    SECTION("quadratic penalty: moderate imbalance (~0.2) is nearly free") {
        // Same inputs under linear would be 1 - 0.14 = 0.86; under quadratic
        // are 1 - 0.028 = 0.97. The bandit should tolerate normal variance.
        auto stats = makeStats(/*singleton=*/0.2, /*giant=*/0.2, /*gini=*/0.4, /*intra=*/0.5);
        const double r = computeIntrinsicReward(stats, w);
        CHECK(r > 0.9);
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

TEST_CASE("TopologyTuner: saveState / loadState round-trip preserves arm rewards",
          "[unit][daemon][topology_tuner][catch2]") {
    namespace fs = std::filesystem;
    auto tmpDir = fs::temp_directory_path() /
                  ("yams_topology_tuner_state_" +
                   std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    fs::create_directories(tmpDir);
    const auto statePath = tmpDir / "topology_tuner_state.json";

    // Producer: tune an arm with a known reward, save.
    {
        TopologyTunerConfig cfg;
        cfg.enabled = true;
        TopologyTuner tuner(cfg);
        tuner.setArms(defaultArmGrid(1000));
        auto arm = tuner.selectArm();
        REQUIRE(arm.has_value());
        auto good = makeStats(0.05, 0.10, 0.4, 0.5);
        tuner.observeRebuildStats(arm->id, good);
        auto saved = tuner.saveState(statePath);
        REQUIRE(saved.has_value());
        REQUIRE(fs::exists(statePath));
    }

    // Consumer: construct a new tuner, load state, verify bestArmId is
    // populated (non-empty) — the loaded pulls count carried over.
    {
        TopologyTunerConfig cfg;
        cfg.enabled = true;
        TopologyTuner tuner(cfg);
        tuner.setArms(defaultArmGrid(1000));
        auto loaded = tuner.loadState(statePath);
        REQUIRE(loaded.has_value());
        auto best = tuner.bestArmId();
        CHECK(best.has_value());
    }

    std::error_code ec;
    fs::remove_all(tmpDir, ec);
}

TEST_CASE("TopologyTuner: loadState on missing path returns FileNotFound",
          "[unit][daemon][topology_tuner][catch2]") {
    namespace fs = std::filesystem;
    const auto missingPath = fs::temp_directory_path() / "yams_topology_tuner_does_not_exist.json";
    std::error_code ec;
    fs::remove(missingPath, ec);

    TopologyTunerConfig cfg;
    cfg.enabled = true;
    TopologyTuner tuner(cfg);
    tuner.setArms(defaultArmGrid(1000));
    auto result = tuner.loadState(missingPath);
    CHECK_FALSE(result.has_value());
}

TEST_CASE("TopologyTuner: rebuildArmGridForCorpusSize is no-op when ids match",
          "[unit][daemon][topology_tuner][catch2]") {
    TopologyTunerConfig cfg;
    cfg.enabled = true;
    TopologyTuner tuner(cfg);
    tuner.setArms(defaultArmGrid(5000));
    const auto initialCount = tuner.arms().size();

    // Same corpus size → no-op
    CHECK_FALSE(tuner.rebuildArmGridForCorpusSize(5000));
    CHECK(tuner.arms().size() == initialCount);

    // Substantially different corpus size → grid changes (cluster sizes scale)
    const bool changed = tuner.rebuildArmGridForCorpusSize(50000);
    if (changed) {
        // Grid was rebuilt — at least one arm id should differ
        bool anyDiff = false;
        auto post = tuner.arms();
        if (post.size() == initialCount) {
            for (std::size_t i = 0; i < post.size(); ++i) {
                if (post[i].id != defaultArmGrid(5000)[i].id) {
                    anyDiff = true;
                    break;
                }
            }
        } else {
            anyDiff = true;
        }
        CHECK(anyDiff);
    }
}

TEST_CASE("TopologyTuner: UCB1 explores arms then converges to highest reward",
          "[unit][daemon][topology_tuner][catch2]") {
    // Build a tiny 3-arm bandit with deterministic synthetic rewards. Goal:
    // verify that after a burn-in of pulls, bestArmId matches the arm that
    // received consistently higher reward observations. This proves the
    // bandit isn't degenerate (always-first-arm) when reward signals differ.
    TopologyTunerConfig cfg;
    cfg.enabled = true;
    TopologyTuner tuner(cfg);

    std::vector<TopologyArm> arms = {
        TopologyArm{"arm_low", "hdbscan", 4, 4, 0},
        TopologyArm{"arm_mid", "hdbscan", 8, 8, 0},
        TopologyArm{"arm_high", "hdbscan", 16, 16, 0},
    };
    tuner.setArms(arms);

    // Synthetic rewards per arm: arm_high gives the best clusterings.
    auto statsForArm = [](const std::string& id) {
        if (id == "arm_high") {
            return makeStats(/*singleton=*/0.05, /*giant=*/0.10, /*gini=*/0.4, /*intra=*/0.7);
        }
        if (id == "arm_mid") {
            return makeStats(/*singleton=*/0.20, /*giant=*/0.30, /*gini=*/0.5, /*intra=*/0.4);
        }
        return makeStats(/*singleton=*/0.50, /*giant=*/0.60, /*gini=*/0.7, /*intra=*/0.2);
    };

    // Pull 30 times — UCB1 should explore each arm a few times, then exploit
    // arm_high. Without sufficient pulls, exploration dominates.
    for (int i = 0; i < 30; ++i) {
        auto picked = tuner.selectArm();
        REQUIRE(picked.has_value());
        tuner.observeRebuildStats(picked->id, statsForArm(picked->id));
    }

    auto best = tuner.bestArmId();
    REQUIRE(best.has_value());
    CHECK(*best == "arm_high");
}

TEST_CASE("TopologyTuner: shadow-divergence detects intrinsic-up + extrinsic-down trend",
          "[unit][daemon][topology_tuner][catch2]") {
    TopologyTunerConfig cfg;
    cfg.enabled = true;
    TopologyTuner tuner(cfg);
    tuner.setArms(defaultArmGrid(1000));

    // Pre-condition: no warnings yet.
    CHECK(tuner.divergenceWarningCount() == 0u);

    // Three pulls each with monotonically improving intrinsic reward (lower
    // singleton/giant) but monotonically worsening synthetic extrinsic signal.
    auto pullAndShadow = [&](double singleton, double giant, double intra, double extrinsicSignal) {
        auto picked = tuner.selectArm();
        REQUIRE(picked.has_value());
        auto stats = makeStats(singleton, giant, /*gini=*/0.4, intra);
        tuner.observeRebuildStats(picked->id, stats);
        tuner.observeShadowExtrinsic(extrinsicSignal);
    };

    // Intrinsic improves: 0.30 → 0.35 → 0.40
    // Extrinsic drops: 0.55 → 0.52 → 0.45 (>0.02 cumulative drop)
    pullAndShadow(/*sing=*/0.5, /*giant=*/0.5, /*intra=*/0.3, /*ext=*/0.55);
    pullAndShadow(/*sing=*/0.4, /*giant=*/0.4, /*intra=*/0.4, /*ext=*/0.52);
    pullAndShadow(/*sing=*/0.3, /*giant=*/0.3, /*intra=*/0.5, /*ext=*/0.45);

    CHECK(tuner.divergenceWarningCount() >= 1u);
}

TEST_CASE("TopologyTuner: shadow-divergence does NOT trigger when both signals agree",
          "[unit][daemon][topology_tuner][catch2]") {
    TopologyTunerConfig cfg;
    cfg.enabled = true;
    TopologyTuner tuner(cfg);
    tuner.setArms(defaultArmGrid(1000));

    auto pullAndShadow = [&](double singleton, double giant, double intra, double extrinsicSignal) {
        auto picked = tuner.selectArm();
        REQUIRE(picked.has_value());
        auto stats = makeStats(singleton, giant, /*gini=*/0.4, intra);
        tuner.observeRebuildStats(picked->id, stats);
        tuner.observeShadowExtrinsic(extrinsicSignal);
    };

    // Both signals improve in lockstep — no divergence.
    pullAndShadow(0.5, 0.5, 0.3, 0.40);
    pullAndShadow(0.4, 0.4, 0.4, 0.50);
    pullAndShadow(0.3, 0.3, 0.5, 0.55);

    CHECK(tuner.divergenceWarningCount() == 0u);
}

TEST_CASE("TopologyTuner: cross-restart persistence preserves UCB1 convergence",
          "[unit][daemon][topology_tuner][catch2]") {
    // Simulates the daemon-restart pattern: tuner A pulls + observes some
    // arms, persists; tuner B loads the same state and continues pulling.
    // By the end, the bandit should converge on the synthetic best arm
    // even though no single tuner instance saw all 30 pulls.
    namespace fs = std::filesystem;
    auto tmpDir = fs::temp_directory_path() /
                  ("yams_topology_tuner_xrestart_" +
                   std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    fs::create_directories(tmpDir);
    const auto statePath = tmpDir / "tuner_state.json";

    std::vector<TopologyArm> arms = {
        TopologyArm{"arm_low", "hdbscan", 4, 4, 0},
        TopologyArm{"arm_mid", "hdbscan", 8, 8, 0},
        TopologyArm{"arm_high", "hdbscan", 16, 16, 0},
    };
    auto statsForArm = [](const std::string& id) {
        if (id == "arm_high") {
            return makeStats(/*singleton=*/0.05, /*giant=*/0.10, /*gini=*/0.4, /*intra=*/0.7);
        }
        if (id == "arm_mid") {
            return makeStats(/*singleton=*/0.20, /*giant=*/0.30, /*gini=*/0.5, /*intra=*/0.4);
        }
        return makeStats(/*singleton=*/0.50, /*giant=*/0.60, /*gini=*/0.7, /*intra=*/0.2);
    };

    // Session 1: 10 pulls, persist via auto-save.
    {
        TopologyTunerConfig cfg;
        cfg.enabled = true;
        cfg.statePath = statePath;
        TopologyTuner tuner(cfg);
        tuner.setArms(arms);
        for (int i = 0; i < 10; ++i) {
            auto picked = tuner.selectArm();
            REQUIRE(picked.has_value());
            tuner.observeRebuildStats(picked->id, statsForArm(picked->id));
        }
        REQUIRE(fs::exists(statePath));
    }

    // Session 2 (simulating daemon restart): load state, continue 20 pulls.
    {
        TopologyTunerConfig cfg;
        cfg.enabled = true;
        cfg.statePath = statePath;
        TopologyTuner tuner(cfg);
        tuner.setArms(arms);
        auto loaded = tuner.loadState(statePath);
        REQUIRE(loaded.has_value());
        for (int i = 0; i < 20; ++i) {
            auto picked = tuner.selectArm();
            REQUIRE(picked.has_value());
            tuner.observeRebuildStats(picked->id, statsForArm(picked->id));
        }
        auto best = tuner.bestArmId();
        REQUIRE(best.has_value());
        CHECK(*best == "arm_high");
    }

    std::error_code ec;
    fs::remove_all(tmpDir, ec);
}

TEST_CASE("TopologyTuner: auto-save fires when statePath is configured",
          "[unit][daemon][topology_tuner][catch2]") {
    namespace fs = std::filesystem;
    auto tmpDir = fs::temp_directory_path() /
                  ("yams_topology_tuner_autosave_" +
                   std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    fs::create_directories(tmpDir);
    const auto statePath = tmpDir / "tuner_state.json";

    TopologyTunerConfig cfg;
    cfg.enabled = true;
    cfg.statePath = statePath;
    TopologyTuner tuner(cfg);
    tuner.setArms(defaultArmGrid(1000));
    auto arm = tuner.selectArm();
    REQUIRE(arm.has_value());
    auto good = makeStats(0.05, 0.10, 0.4, 0.5);
    tuner.observeRebuildStats(arm->id, good);

    // observeRebuildStats with statePath set auto-persists the state.
    CHECK(fs::exists(statePath));

    std::error_code ec;
    fs::remove_all(tmpDir, ec);
}
