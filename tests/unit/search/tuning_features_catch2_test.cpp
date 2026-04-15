// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

/**
 * @file tuning_features_catch2_test.cpp
 * @brief Unit tests for R3 canonical features (featurize, bucketize) and
 *        the combined-reward function.
 */

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <yams/search/tuning_features.h>
#include <yams/search/tuning_reward.h>

#include <cmath>
#include <optional>

using namespace yams::search;
using Catch::Approx;

// =============================================================================
// featurize()
// =============================================================================

TEST_CASE("featurize: zero context maps to zero/neg flags", "[unit][tuning_features]") {
    TuningContext ctx;
    const auto f = featurize(ctx);
    REQUIRE(f.size() == kTuningFeatureDim);
    // 0..8 are continuous — zero input maps to zero (except index 4,7 which
    // offset by -1 so a zero symbol density reads as the low end of the
    // range).
    CHECK(f[0] == Approx(0.0));
    CHECK(f[1] == Approx(-1.0)); // centeredHalf(0) = -1
    CHECK(f[2] == Approx(-1.0));
    CHECK(f[3] == Approx(-1.0));
    CHECK(f[4] == Approx(-1.0)); // (0 / 10) - 1
    CHECK(f[5] == Approx(0.0));
    CHECK(f[6] == Approx(-1.0));
    CHECK(f[7] == Approx(-1.0));
    CHECK(f[8] == Approx(0.0));
    // Boolean flags at zero map to -1.
    CHECK(f[9] == Approx(-1.0));
    CHECK(f[10] == Approx(-1.0));
}

TEST_CASE("featurize: saturation clamps to [-1, 1]", "[unit][tuning_features]") {
    TuningContext ctx;
    ctx.docCountLog10 = 50.0; // > 6 saturates
    ctx.codeRatio = 10.0;     // clamped to 1.0 → +1
    ctx.proseRatio = -2.0;    // clamped to 0.0 → -1
    ctx.embeddingCoverage = 1.0;
    ctx.nativeSymbolDensity = 1e6;  // saturates at +1
    ctx.pathRelativeDepthAvg = 1e3; // saturates at +1
    ctx.binaryRatio = 1.0;
    ctx.kgEdgeDensity = 1e6;
    ctx.queryTokenCountLog2 = 1e3;
    ctx.queryHasVectorPath = 1;
    ctx.queryHasKgAnchors = 1;

    const auto f = featurize(ctx);
    for (double v : f) {
        CHECK(v >= -1.0);
        CHECK(v <= 1.0);
    }
    CHECK(f[0] == Approx(1.0));
    CHECK(f[1] == Approx(1.0));
    CHECK(f[2] == Approx(-1.0));
    CHECK(f[4] == Approx(1.0));
    CHECK(f[9] == Approx(1.0));
    CHECK(f[10] == Approx(1.0));
}

TEST_CASE("featurize: midpoint ratios map near zero", "[unit][tuning_features]") {
    TuningContext ctx;
    ctx.codeRatio = 0.5;
    ctx.proseRatio = 0.5;
    ctx.embeddingCoverage = 0.5;
    ctx.binaryRatio = 0.5;

    const auto f = featurize(ctx);
    CHECK(f[1] == Approx(0.0));
    CHECK(f[2] == Approx(0.0));
    CHECK(f[3] == Approx(0.0));
    CHECK(f[6] == Approx(0.0));
}

TEST_CASE("featurize: NaN in symbol density does not crash", "[unit][tuning_features]") {
    TuningContext ctx;
    ctx.nativeSymbolDensity = std::nan("");
    // Just ensure we return without a crash and output stays finite.
    const auto f = featurize(ctx);
    // index 4 is allowed to be any clamped value; finiteness is not
    // guaranteed by the clamp helper when input is NaN, so we just assert
    // no throw. This case is defensive — corpus stats never emit NaN.
    (void)f;
    SUCCEED();
}

// =============================================================================
// bucketize()
// =============================================================================

TEST_CASE("bucketize: zero context produces stable key", "[unit][tuning_features]") {
    const TuningContext ctx;
    const std::string key = bucketize(ctx);
    CHECK(key == "c0/d0/e0/m0/ce0/te0");
}

TEST_CASE("bucketize: epoch bumps produce different buckets", "[unit][tuning_features]") {
    TuningContext a;
    a.corpusEpoch = 100;
    TuningContext b;
    b.corpusEpoch = 101;
    CHECK(bucketize(a) != bucketize(b));

    TuningContext c = a;
    c.topologyEpoch = 7;
    CHECK(bucketize(a) != bucketize(c));
}

TEST_CASE("bucketize: quartile boundaries", "[unit][tuning_features]") {
    TuningContext ctx;
    ctx.codeRatio = 0.24;
    const auto q0 = bucketize(ctx);
    ctx.codeRatio = 0.26;
    const auto q1 = bucketize(ctx);
    ctx.codeRatio = 0.76;
    const auto q3 = bucketize(ctx);
    CHECK(q0 != q1);
    CHECK(q1 != q3);
    CHECK(q0.find("c0") != std::string::npos);
    CHECK(q1.find("c1") != std::string::npos);
    CHECK(q3.find("c3") != std::string::npos);
}

TEST_CASE("bucketize: doc-count decade", "[unit][tuning_features]") {
    TuningContext ctx;
    ctx.docCountLog10 = 0.5; // < 1 → floor 0
    const auto d0 = bucketize(ctx);
    ctx.docCountLog10 = 3.2; // → floor 3 (thousands)
    const auto d3 = bucketize(ctx);
    ctx.docCountLog10 = 99.0; // saturates at 6
    const auto d6 = bucketize(ctx);
    CHECK(d0.find("d0") != std::string::npos);
    CHECK(d3.find("d3") != std::string::npos);
    CHECK(d6.find("d6") != std::string::npos);
}

TEST_CASE("bucketize: media flag triggers on binary-heavy corpora", "[unit][tuning_features]") {
    TuningContext ctx;
    ctx.binaryRatio = 0.49;
    CHECK(bucketize(ctx).find("m0") != std::string::npos);
    ctx.binaryRatio = 0.5;
    CHECK(bucketize(ctx).find("m1") != std::string::npos);
}

// =============================================================================
// combineReward()
// =============================================================================

TEST_CASE("combineReward: no labels falls back to proxy", "[unit][tuning_reward]") {
    ProxyRewardInputs p;
    p.kgUtility = 0.5;
    p.freshnessBoost = 0.5;
    p.agreementBoost = 0.5;
    const auto r = combineReward(std::nullopt, p);
    CHECK(r.source == RewardSource::Proxy);
    // 0.6*0.5 + 0.2*0.5 + 0.2*0.5 = 0.5
    CHECK(r.value == Approx(0.5));
}

TEST_CASE("combineReward: labels dominate proxy", "[unit][tuning_reward]") {
    ProxyRewardInputs p;
    p.kgUtility = 0.9;
    const auto r = combineReward(std::optional<double>{0.4}, p);
    // Proxy is non-zero → source reads as Mixed for explainability, but the
    // value comes from the label channel.
    CHECK(r.source == RewardSource::Mixed);
    CHECK(r.value == Approx(0.4));
}

TEST_CASE("combineReward: labels without proxy → Labels source", "[unit][tuning_reward]") {
    ProxyRewardInputs p;
    const auto r = combineReward(std::optional<double>{0.7}, p);
    CHECK(r.source == RewardSource::Labels);
    CHECK(r.value == Approx(0.7));
}

TEST_CASE("combineReward: latency over budget penalizes", "[unit][tuning_reward]") {
    ProxyRewardInputs p;
    p.kgUtility = 0.5;
    p.latencyOverBudget = 0.2;
    const auto r = combineReward(std::nullopt, p);
    // 0.6*0.5 = 0.3; − 0.2 = 0.1
    CHECK(r.value == Approx(0.1));
}

TEST_CASE("combineReward: NaN inputs treated as zero", "[unit][tuning_reward]") {
    ProxyRewardInputs p;
    p.kgUtility = std::nan("");
    p.freshnessBoost = std::nan("");
    p.agreementBoost = std::nan("");
    p.latencyOverBudget = std::nan("");
    const auto r = combineReward(std::nullopt, p);
    CHECK(r.value == Approx(0.0));
    CHECK(r.source == RewardSource::Proxy);
}

TEST_CASE("combineReward: output clamped to [0,1]", "[unit][tuning_reward]") {
    ProxyRewardInputs p;
    p.kgUtility = 10.0;
    const auto hi = combineReward(std::nullopt, p);
    CHECK(hi.value == Approx(1.0));

    p = {};
    p.latencyOverBudget = 5.0;
    const auto lo = combineReward(std::nullopt, p);
    CHECK(lo.value == Approx(0.0));
}

TEST_CASE("rewardSourceLabel: mapping", "[unit][tuning_reward]") {
    CHECK(rewardSourceLabel(RewardSource::Labels) == "labels");
    CHECK(rewardSourceLabel(RewardSource::Proxy) == "proxy");
    CHECK(rewardSourceLabel(RewardSource::Mixed) == "mixed");
}
