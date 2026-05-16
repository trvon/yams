// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2025 YAMS Contributors
//
// Micro-benchmarks for simeon bandit overhead.
// Measures: arm selection, reward recording, init, and dispatch latency.

#include <catch2/benchmark/catch_benchmark.hpp>
#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <string>
#include <vector>

#include <yams/search/search_engine_config.h>
#include <yams/search/tuner_mab.h>

using namespace yams::search;

namespace {

std::vector<TunerMAB::Arm> standardArms() {
    std::vector<TunerMAB::Arm> arms;
    arms.push_back({"sab_smooth", 0.0, {}});
    arms.push_back({"sab_smooth_rm3_adaptive", 0.0, {}});
    arms.push_back({"sab_smooth_rm3_diverse", 0.0, {}});
    arms.push_back({"keyphrase", 0.0, {}});
    arms.push_back({"lead_field", 0.0, {}});
    return arms;
}

} // namespace

TEST_CASE("Bench: bandit arm selection on warm bandit", "[.bench][search][bandit][bench]") {
    TunerMAB mab;
    mab.setArms(standardArms());

    // Warm up: pull each arm once.
    for (size_t i = 0; i < mab.arms().size(); ++i) {
        auto idx = mab.selectArm();
        mab.recordReward(*idx, 0.5, TunerMAB::RewardSource::Proxy);
    }

    BENCHMARK("selectArm warm") {
        return mab.selectArm();
    };
}

TEST_CASE("Bench: bandit reward recording", "[.bench][search][bandit][bench]") {
    TunerMAB mab;
    mab.setArms(standardArms());

    // Ensure at least one arm has been pulled.
    auto idx = mab.selectArm();
    REQUIRE(idx.has_value());

    BENCHMARK("recordReward") {
        mab.recordReward(0, 0.5, TunerMAB::RewardSource::Proxy);
    };
}

TEST_CASE("Bench: bandit init with 5 profiles × 5 arms", "[.bench][search][bandit][bench]") {
    using ProfileKey = std::string;

    BENCHMARK("init 5x5 bandits") {
        std::unordered_map<ProfileKey, TunerMAB> bandits;
        using CP = SearchEngineConfig::CorpusProfile;
        for (auto profile : {CP::CODE, CP::PROSE, CP::DOCS, CP::MIXED, CP::CUSTOM}) {
            bandits[SearchEngineConfig::corpusProfileToString(profile)].setArms(standardArms());
        }
        return bandits.size();
    };
}

TEST_CASE("Bench: per-profile arm selection + reward", "[.bench][search][bandit][bench]") {
    using ProfileKey = std::string;
    std::unordered_map<ProfileKey, TunerMAB> bandits;
    using CP = SearchEngineConfig::CorpusProfile;
    for (auto profile : {CP::CODE, CP::PROSE, CP::DOCS, CP::MIXED, CP::CUSTOM}) {
        bandits[SearchEngineConfig::corpusProfileToString(profile)].setArms(standardArms());
    }

    // Warm all bandits.
    for (auto& [_, mab] : bandits) {
        for (size_t i = 0; i < mab.arms().size(); ++i) {
            auto idx = mab.selectArm();
            mab.recordReward(*idx, 0.5, TunerMAB::RewardSource::Proxy);
        }
    }

    BENCHMARK("select+record per-profile") {
        std::string profileKey = "MIXED";
        auto it = bandits.find(profileKey);
        auto idx = it->second.selectArm();
        if (idx.has_value()) {
            it->second.recordReward(*idx, 0.5, TunerMAB::RewardSource::Proxy);
        }
        return idx.has_value();
    };
}

TEST_CASE("Bench: corpusProfileToString lookup", "[.bench][search][bandit][bench]") {
    BENCHMARK("corpusProfileToString") {
        return SearchEngineConfig::corpusProfileToString(SearchEngineConfig::CorpusProfile::MIXED);
    };
}

TEST_CASE("Bench: bandit UCB1 score computation (150 iterations)",
          "[.bench][search][bandit][bench]") {
    TunerMAB mab;
    mab.setArms(standardArms());

    // Warm up: 10 pulls per arm with varying rewards.
    for (int round = 0; round < 10; ++round) {
        for (size_t i = 0; i < mab.arms().size(); ++i) {
            auto idx = mab.selectArm();
            double reward = (i == 0) ? 0.9 : 0.5; // arm 0 is best
            mab.recordReward(*idx, reward, TunerMAB::RewardSource::Proxy);
        }
    }

    BENCHMARK("bestArmId after convergence") {
        return mab.bestArmId();
    };
}
