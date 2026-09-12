// One policy for turning Simeon's dense score buffer into candidate-order scores. Before this
// header existed, the routed, strategy-routed, and bandit-routed paths each carried their own
// copy of the fast path and disagreed on what a non-finite score falls back to.

#include <catch2/catch_test_macros.hpp>

#include "src/search/simeon_score_materialize_internal.h"

#include <cmath>
#include <limits>

using namespace yams::search::simeon_internal;

namespace {
const std::vector<std::int64_t> kCorpus{100, 200, 300, 400};
const std::unordered_map<std::int64_t, std::uint32_t> kIndex{
    {100, 0}, {200, 1}, {300, 2}, {400, 3}};
constexpr float kNaN = std::numeric_limits<float>::quiet_NaN();
constexpr float kInf = std::numeric_limits<float>::infinity();
} // namespace

TEST_CASE("dense corpus order is recognised by buffer identity, not by content",
          "[search][simeon][materialize][catch2]") {
    CHECK(isDenseCorpusOrder(kCorpus, kCorpus));
    std::vector<std::int64_t> copy = kCorpus;
    CHECK_FALSE(isDenseCorpusOrder(copy, kCorpus));
    CHECK_FALSE(isDenseCorpusOrder(std::span<const std::int64_t>(kCorpus).first(3), kCorpus));
}

TEST_CASE("non-finite scores fall back to the lexical baseline, then to zero",
          "[search][simeon][materialize][catch2]") {
    std::vector<float> full{1.0f, kNaN, kInf, 4.0f};
    const std::vector<float> lexical{9.0f, 2.5f, kNaN, 9.0f};

    SECTION("owned buffer, dense order: returned in place") {
        auto out = materializeScores(std::move(full), lexical, kIndex, kCorpus, kCorpus);
        REQUIRE(out.size() == 4);
        CHECK(out[0] == 1.0f);
        CHECK(out[1] == 2.5f);
        CHECK(out[2] == 0.0f);
        CHECK(out[3] == 4.0f);
    }

    SECTION("owned buffer, dense order, no baseline: zero") {
        auto out = materializeScores(std::move(full), {}, kIndex, kCorpus, kCorpus);
        CHECK(out[1] == 0.0f);
        CHECK(out[2] == 0.0f);
    }

    SECTION("borrowed buffer, dense order: copied, source untouched") {
        auto out =
            materializeScores(std::span<const float>(full), lexical, kIndex, kCorpus, kCorpus);
        CHECK(out[1] == 2.5f);
        CHECK(std::isnan(full[1]));
    }

    SECTION("sparse candidates: gathered in candidate order, unknown ids score zero") {
        const std::vector<std::int64_t> candidates{300, 999, 200, 100};
        auto owned = materializeScores(std::move(full), lexical, kIndex, kCorpus, candidates);
        REQUIRE(owned.size() == 4);
        CHECK(owned[0] == 0.0f);
        CHECK(owned[1] == 0.0f);
        CHECK(owned[2] == 2.5f);
        CHECK(owned[3] == 1.0f);

        std::vector<float> again{1.0f, kNaN, kInf, 4.0f};
        auto borrowed =
            materializeScores(std::span<const float>(again), lexical, kIndex, kCorpus, candidates);
        CHECK(borrowed == owned);
    }
}
