// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2025 YAMS Contributors
//
// Unit tests for per-profile simeon bandit (TunerMAB integration + config).
// Tests cover:
//   - CorpusProfile string conversion
//   - Bandit initialization (arms per profile, correct arm names)
//   - Arm selection (UCB1 returns valid indices, MIXED fallback)
//   - Reward recording (pull count + reward sum updates)
//   - Per-request state clearing (arm, profile fields reset)
//   - Both exploration and exploitation phases
//   - Bandit state serialization round-trip

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <string>
#include <unordered_map>
#include <vector>

#include <yams/search/bandit_reward.h>
#include <yams/search/search_engine_config.h>
#include <yams/search/tuner_mab.h>

using namespace yams::search;

TEST_CASE("corpusProfileToString: all profiles map correctly", "[unit][search][bandit][config]") {
    using CP = SearchEngineConfig::CorpusProfile;
    CHECK(std::string(SearchEngineConfig::corpusProfileToString(CP::CODE)) == "CODE");
    CHECK(std::string(SearchEngineConfig::corpusProfileToString(CP::PROSE)) == "PROSE");
    CHECK(std::string(SearchEngineConfig::corpusProfileToString(CP::DOCS)) == "DOCS");
    CHECK(std::string(SearchEngineConfig::corpusProfileToString(CP::MIXED)) == "MIXED");
    CHECK(std::string(SearchEngineConfig::corpusProfileToString(CP::CUSTOM)) == "CUSTOM");
}

TEST_CASE("TunerMAB: setArms creates correct arm set", "[unit][search][bandit]") {
    TunerMAB mab;

    std::vector<TunerMAB::Arm> arms;
    arms.push_back({"sab_smooth", 0.0, {}});
    arms.push_back({"sab_smooth_rm3_adaptive", 0.0, {}});
    arms.push_back({"sab_smooth_rm3_diverse", 0.0, {}});
    mab.setArms(std::move(arms));

    CHECK(mab.arms().size() == 3);
    CHECK(mab.arms()[0].id == "sab_smooth");
    CHECK(mab.arms()[1].id == "sab_smooth_rm3_adaptive");
    CHECK(mab.arms()[2].id == "sab_smooth_rm3_diverse");
    CHECK(mab.totalPulls() == 0);
}

TEST_CASE("TunerMAB: selectArm returns valid index", "[unit][search][bandit]") {
    TunerMAB mab;
    std::vector<TunerMAB::Arm> arms;
    arms.push_back({"arm_a", 0.0, {}});
    arms.push_back({"arm_b", 0.0, {}});
    mab.setArms(std::move(arms));

    auto idx = mab.selectArm();
    REQUIRE(idx.has_value());
    CHECK(*idx < mab.arms().size());
}

TEST_CASE("TunerMAB: selectArm explores all arms at least once", "[unit][search][bandit]") {
    TunerMAB mab;
    std::vector<TunerMAB::Arm> arms;
    for (int i = 0; i < 5; ++i) {
        arms.push_back({"arm_" + std::to_string(i), 0.0, {}});
    }
    mab.setArms(std::move(arms));

    // After 5 selects, every arm should have been pulled at least once.
    for (int i = 0; i < 5; ++i) {
        auto idx = mab.selectArm();
        REQUIRE(idx.has_value());
        mab.recordReward(*idx, 0.5, TunerMAB::RewardSource::Proxy);
    }

    for (const auto& a : mab.arms()) {
        CHECK(a.stats.pulls >= 1);
    }
}

TEST_CASE("TunerMAB: recordReward updates arm stats", "[unit][search][bandit]") {
    TunerMAB mab;
    std::vector<TunerMAB::Arm> arms;
    arms.push_back({"test_arm", 0.0, {}});
    mab.setArms(std::move(arms));

    auto idx = mab.selectArm();
    REQUIRE(idx.has_value());
    CHECK(*idx == 0);

    mab.recordReward(0, 0.75, TunerMAB::RewardSource::Proxy);
    CHECK(mab.arms()[0].stats.pulls == 1);
    // Reward clamped to [0, 1]; 0.75 should pass through.
    CHECK(mab.arms()[0].stats.rewardSum > 0.0);
    CHECK(mab.totalPulls() == 1);
}

TEST_CASE("TunerMAB: bestArmId identifies highest-mean arm", "[unit][search][bandit]") {
    TunerMAB mab;
    std::vector<TunerMAB::Arm> arms;
    arms.push_back({"low", 0.0, {}});
    arms.push_back({"high", 0.0, {}});
    mab.setArms(std::move(arms));

    // Pull both arms once each.
    for (int i = 0; i < 2; ++i) {
        auto idx = mab.selectArm();
        REQUIRE(idx.has_value());
        double reward = (*idx == 0) ? 0.3 : 0.9;
        mab.recordReward(*idx, reward, TunerMAB::RewardSource::Proxy);
    }

    auto best = mab.bestArmId();
    REQUIRE(best.has_value());
    CHECK(*best == "high");
}

TEST_CASE("TunerMAB: selectArm returns nullopt for empty arm set", "[unit][search][bandit]") {
    TunerMAB mab;
    auto idx = mab.selectArm();
    CHECK_FALSE(idx.has_value());
}

TEST_CASE("TunerMAB: reward clamping to [0, 1]", "[unit][search][bandit]") {
    TunerMAB mab;
    std::vector<TunerMAB::Arm> arms;
    arms.push_back({"a", 0.0, {}});
    mab.setArms(std::move(arms));

    mab.recordReward(0, 1.5, TunerMAB::RewardSource::Proxy);  // should clamp to 1.0
    mab.recordReward(0, -0.5, TunerMAB::RewardSource::Proxy); // should clamp to 0.0

    // Two pulls, sum should be 1.0 (clamped from 1.5 + 0.0).
    CHECK(mab.arms()[0].stats.pulls == 2);
    CHECK(mab.arms()[0].stats.rewardSum <= 1.01); // ~1.0
}

TEST_CASE("TunerMAB: serialization round-trip preserves arm state", "[unit][search][bandit]") {
    TunerMAB mab;
    std::vector<TunerMAB::Arm> arms;
    arms.push_back({"keep", 0.0, {}});
    arms.push_back({"drop", 0.0, {}});
    mab.setArms(std::move(arms));

    auto idx = mab.selectArm();
    REQUIRE(idx.has_value());
    mab.recordReward(*idx, 0.8, TunerMAB::RewardSource::Proxy);

    auto json = mab.toJson();
    REQUIRE(json.is_object());
    CHECK(json.contains("arms"));
    CHECK(json["arms"].is_array());
    CHECK(json["arms"].size() == 2);

    // Restore into a fresh bandit.
    TunerMAB restored;
    std::vector<TunerMAB::Arm> newArms;
    newArms.push_back({"keep", 0.0, {}});
    newArms.push_back({"drop", 0.0, {}});
    restored.setArms(std::move(newArms));
    auto result = restored.fromJson(json);
    CHECK(result.has_value());

    CHECK(restored.arms().size() == 2);
    CHECK(restored.totalPulls() == 1);
}

TEST_CASE("TunerMAB: fromJson with mismatched schema resets cleanly", "[unit][search][bandit]") {
    TunerMAB mab;
    std::vector<TunerMAB::Arm> arms;
    arms.push_back({"a", 0.0, {}});
    mab.setArms(std::move(arms));

    auto idx = mab.selectArm();
    mab.recordReward(*idx, 0.5, TunerMAB::RewardSource::Proxy);

    nlohmann::json bad;
    bad["schema_version"] = 999;
    bad["arms"] = nlohmann::json::array();

    auto result = mab.fromJson(bad);
    // Schema mismatch returns error but bandit keeps runtime state.
    CHECK_FALSE(result.has_value());
    CHECK(mab.totalPulls() == 1); // unchanged — runtime state preserved
}

TEST_CASE("per-profile bandits: each profile gets independent arms",
          "[unit][search][bandit][profile]") {
    using ProfileKey = std::string;
    std::unordered_map<ProfileKey, TunerMAB> bandits;

    std::vector<TunerMAB::Arm> arms;
    arms.push_back({"sab_smooth", 0.0, {}});
    arms.push_back({"sab_smooth_rm3_adaptive", 0.0, {}});

    using CP = SearchEngineConfig::CorpusProfile;
    for (auto profile : {CP::CODE, CP::PROSE, CP::DOCS, CP::MIXED, CP::CUSTOM}) {
        bandits[SearchEngineConfig::corpusProfileToString(profile)].setArms(arms);
    }

    CHECK(bandits.size() == 5);
    for (const auto& [key, mab] : bandits) {
        CHECK(mab.arms().size() == 2);
    }

    // Pull arms differently on CODE vs MIXED profiles.
    auto codeIdx = bandits["CODE"].selectArm();
    REQUIRE(codeIdx.has_value());
    bandits["CODE"].recordReward(*codeIdx, 0.8, TunerMAB::RewardSource::Proxy);
    CHECK(bandits["CODE"].totalPulls() == 1);
    // MIXED profile should be independent.
    CHECK(bandits["MIXED"].totalPulls() == 0);
}

TEST_CASE("per-profile bandits: MIXED fallback for unknown profile",
          "[unit][search][bandit][profile]") {
    using ProfileKey = std::string;
    std::unordered_map<ProfileKey, TunerMAB> bandits;

    std::vector<TunerMAB::Arm> arms;
    arms.push_back({"arm", 0.0, {}});
    bandits["MIXED"].setArms(arms);

    // Look up a non-existent profile, fall back to MIXED.
    auto it = bandits.find("NONEXISTENT");
    if (it == bandits.end()) {
        it = bandits.find("MIXED");
    }
    REQUIRE(it != bandits.end());
    CHECK(it->second.arms().size() == 1);
}

TEST_CASE("SearchEngineConfig: simeonBanditArm default empty", "[unit][search][bandit][config]") {
    SearchEngineConfig cfg;
    CHECK(cfg.simeonBanditArm.empty());
}

TEST_CASE("SearchEngineConfig: simeonBanditArm set and read", "[unit][search][bandit][config]") {
    SearchEngineConfig cfg;
    cfg.simeonBanditArm = "sab_smooth_rm3_adaptive";
    CHECK(cfg.simeonBanditArm == "sab_smooth_rm3_adaptive");
    CHECK_FALSE(cfg.simeonBanditArm.empty());
}

TEST_CASE("computeSimeonBanditReward: arm-name independent (no rm3 bonus)",
          "[unit][search][bandit]") {
    // Same result count + latency must yield the same reward regardless of the
    // arm name — the previous +0.05 "rm3" bonus is gone.
    const double plain = computeSimeonBanditReward(5, 10.0, "sab_smooth");
    const double rm3 = computeSimeonBanditReward(5, 10.0, "sab_smooth_rm3_adaptive");
    const double rm3d = computeSimeonBanditReward(5, 10.0, "sab_smooth_rm3_diverse");
    CHECK(plain == rm3);
    CHECK(plain == rm3d);
}

TEST_CASE("computeSimeonBanditReward: monotone in result density, clamped",
          "[unit][search][bandit]") {
    // More results (same latency) -> not-smaller reward.
    CHECK(computeSimeonBanditReward(2, 10.0, "a") <= computeSimeonBanditReward(8, 10.0, "a"));
    // Faster (same results) -> not-smaller reward.
    CHECK(computeSimeonBanditReward(5, 20.0, "a") <= computeSimeonBanditReward(5, 5.0, "a"));
    // Clamped into [0, 0.95]; high density saturates at the cap, zero results -> 0.
    const double hi = computeSimeonBanditReward(1000, 1.0, "a");
    CHECK(hi <= 0.95);
    CHECK(hi >= 0.0);
    CHECK(computeSimeonBanditReward(0, 10.0, "a") == 0.0);
    // elapsedMs floored at 1.0, so tiny/zero latency never divides by zero.
    CHECK(computeSimeonBanditReward(1, 0.0, "a") >= 0.0);
}
