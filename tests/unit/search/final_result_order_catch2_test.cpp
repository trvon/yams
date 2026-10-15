// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>

#include "src/search/final_result_order_internal.h"

#include <cstddef>
#include <random>
#include <string>
#include <vector>

using yams::search::SearchResult;
using yams::search::detail::finalLexicalAwareLess;

namespace {

SearchResult makeResult(std::string id, float score, double keyword, double vector) {
    SearchResult r;
    r.document.sha256Hash = id;
    r.document.filePath = "/x/" + id;
    r.score = score;
    r.keywordScore = keyword;
    r.vectorScore = vector;
    return r;
}

} // namespace

TEST_CASE("final lexical tie-break prefers more vector evidence", "[search][order][catch2]") {
    // Same tie bucket, same lexical and keyword evidence; only vector support differs. The doc
    // ids put the low-vector doc first so the docId fallback cannot mask the vector decision.
    const auto lowVector = makeResult("a", 0.512F, 0.4, 0.1);
    const auto highVector = makeResult("b", 0.514F, 0.4, 0.8);

    CHECK(finalLexicalAwareLess(highVector, lowVector, true, 0.01F));
    CHECK_FALSE(finalLexicalAwareLess(lowVector, highVector, true, 0.01F));
}

TEST_CASE("final lexical tie-break comparator is a strict weak ordering",
          "[search][order][catch2]") {
    std::mt19937 rng(20261015U);
    // Scores clustered around a few centers so many pairs fall inside the tie epsilon.
    std::uniform_real_distribution<float> centerJitter(-0.02F, 0.02F);
    std::uniform_int_distribution<int> center(0, 4);
    std::uniform_int_distribution<int> evidence(0, 3);

    std::vector<SearchResult> results;
    for (int i = 0; i < 300; ++i) {
        const float score = 0.2F + 0.05F * static_cast<float>(center(rng)) + centerJitter(rng);
        results.push_back(
            makeResult("d" + std::to_string(i), score, 0.1 * evidence(rng), 0.1 * evidence(rng)));
    }

    const float epsilon = 0.01F;
    const auto less = [epsilon](const SearchResult& a, const SearchResult& b) {
        return finalLexicalAwareLess(a, b, true, epsilon);
    };

    std::size_t asymmetryViolations = 0;
    std::size_t irreflexivityViolations = 0;
    for (const auto& a : results) {
        if (less(a, a)) {
            ++irreflexivityViolations;
        }
        for (const auto& b : results) {
            if (less(a, b) && less(b, a)) {
                ++asymmetryViolations;
            }
        }
    }
    CHECK(irreflexivityViolations == 0);
    CHECK(asymmetryViolations == 0);

    // Transitivity of the order and of incomparability over sampled triples.
    std::uniform_int_distribution<std::size_t> pick(0, results.size() - 1);
    std::size_t transitivityViolations = 0;
    for (int t = 0; t < 200000; ++t) {
        const auto& a = results[pick(rng)];
        const auto& b = results[pick(rng)];
        const auto& c = results[pick(rng)];
        if (less(a, b) && less(b, c) && !less(a, c)) {
            ++transitivityViolations;
        }
        const bool abEquiv = !less(a, b) && !less(b, a);
        const bool bcEquiv = !less(b, c) && !less(c, b);
        const bool acEquiv = !less(a, c) && !less(c, a);
        if (abEquiv && bcEquiv && !acEquiv) {
            ++transitivityViolations;
        }
    }
    CHECK(transitivityViolations == 0);
}

TEST_CASE("final comparator without tie-break orders by score then doc id",
          "[search][order][catch2]") {
    const auto a = makeResult("a", 0.512F, 0.0, 0.9);
    const auto b = makeResult("b", 0.514F, 0.9, 0.0);
    const auto c = makeResult("c", 0.514F, 0.0, 0.0);

    CHECK(finalLexicalAwareLess(b, a, false, 0.01F));
    CHECK_FALSE(finalLexicalAwareLess(a, b, false, 0.01F));
    CHECK(finalLexicalAwareLess(b, c, false, 0.01F)); // equal score -> doc id
    CHECK_FALSE(finalLexicalAwareLess(c, b, false, 0.01F));
}
