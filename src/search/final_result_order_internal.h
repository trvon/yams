// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <yams/search/search_models.h>
#include <yams/search/search_tracing.h>

#include <algorithm>
#include <cmath>

namespace yams::search::detail {

// Lexical evidence used to break near-ties in the final ordering.
inline double finalLexicalAnchorScore(const SearchResult& r) {
    return r.keywordScore.value_or(0.0) + r.graphTextScore.value_or(0.0) +
           r.pathScore.value_or(0.0) + r.tagScore.value_or(0.0) + r.symbolScore.value_or(0.0);
}

// Final-result comparator (true when `a` ranks before `b`). Used by std::sort/partial_sort, so it
// must be a strict weak ordering.
//
// With the lexical tie-break on, scores are compared by tie bucket floor(score / epsilon); inside a
// bucket lexical anchor, keyword and vector evidence decide (more evidence first), then doc id.
// Bucketing is a per-result key, which keeps the ordering transitive; a pairwise |a - b| <= epsilon
// test is not. Epsilon 0 breaks only exact score ties. With the tie-break off this is score
// desc, then doc id.
inline bool finalLexicalAwareLess(const SearchResult& a, const SearchResult& b,
                                  bool enableLexicalTieBreak, float lexicalTieBreakEpsilon) {
    const double tieEpsilon = std::max(0.0, static_cast<double>(lexicalTieBreakEpsilon));

    if (!enableLexicalTieBreak) {
        if (a.score != b.score) {
            return a.score > b.score;
        }
    } else {
        const auto tieBucket = [tieEpsilon](const SearchResult& r) {
            const auto score = static_cast<double>(r.score);
            return tieEpsilon > 0.0 ? std::floor(score / tieEpsilon) : score;
        };
        const double bucketA = tieBucket(a);
        const double bucketB = tieBucket(b);
        if (bucketA != bucketB) {
            return bucketA > bucketB;
        }

        const double lexicalA = finalLexicalAnchorScore(a);
        const double lexicalB = finalLexicalAnchorScore(b);
        if (lexicalA != lexicalB) {
            return lexicalA > lexicalB;
        }

        const double keywordA = a.keywordScore.value_or(0.0);
        const double keywordB = b.keywordScore.value_or(0.0);
        if (keywordA != keywordB) {
            return keywordA > keywordB;
        }

        const double vectorA = a.vectorScore.value_or(0.0);
        const double vectorB = b.vectorScore.value_or(0.0);
        if (vectorA != vectorB) {
            return vectorA > vectorB;
        }
    }

    return documentIdForTrace(a.document.filePath, a.document.sha256Hash) <
           documentIdForTrace(b.document.filePath, b.document.sha256Hash);
}

} // namespace yams::search::detail
