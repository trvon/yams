// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#include "lexical_rerank_internal.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <cmath>
#include <limits>
#include <numeric>
#include <string_view>

namespace yams::search::detail {

namespace {

// English function words. Their posting lists dominate the OR MATCH cost and add almost nothing to
// bm25 (2026-10-30 bench: same nDCG, half the SQL time).
constexpr std::array<std::string_view, 55> kStopwords = {
    "an",     "and",  "are",  "as",   "at",    "be",    "been",  "being", "by",   "can",   "could",
    "did",    "do",   "does", "for",  "from",  "had",   "has",   "have",  "how",  "in",    "into",
    "is",     "it",   "its",  "may",  "might", "no",    "not",   "of",    "on",   "or",    "shall",
    "should", "than", "that", "the",  "their", "then",  "there", "these", "this", "those", "to",
    "was",    "were", "what", "when", "where", "which", "who",   "whom",  "will", "with",  "would"};

bool isStopword(std::string_view term) {
    return std::find(kStopwords.begin(), kStopwords.end(), term) != kStopwords.end();
}

// z-scores in place; a flat vector maps to zeros.
void zScore(std::vector<double>& values) {
    const double n = static_cast<double>(values.size());
    const double mean = std::accumulate(values.begin(), values.end(), 0.0) / n;
    double var = 0.0;
    for (double v : values) {
        var += (v - mean) * (v - mean);
    }
    const double sd = std::sqrt(var / n);
    for (double& v : values) {
        v = sd > 0.0 ? (v - mean) / sd : 0.0;
    }
}

} // namespace

std::vector<std::string> lexicalRerankTerms(const std::string& query) {
    std::vector<std::string> terms;
    std::string current;
    auto flush = [&]() {
        if (current.size() > 1 && !isStopword(current)) {
            terms.push_back(current);
        }
        current.clear();
    };
    for (unsigned char c : query) {
        if (std::isalnum(c) != 0) {
            current.push_back(static_cast<char>(std::tolower(c)));
        } else {
            flush();
        }
    }
    flush();
    std::sort(terms.begin(), terms.end());
    terms.erase(std::unique(terms.begin(), terms.end()), terms.end());
    return terms;
}

LexicalRerankOutcome applyLexicalDeepRerank(std::vector<SearchResult>& results,
                                            const std::string& query,
                                            const SearchEngineConfig& config,
                                            const LexicalRerankScorerFn& scorer) {
    LexicalRerankOutcome outcome;
    const std::size_t depth = std::min(config.lexicalRerankDepth, results.size());
    if (depth == 0) {
        outcome.skipReason = config.lexicalRerankDepth == 0 ? "disabled" : "no_candidates";
        return outcome;
    }
    if (!scorer) {
        outcome.skipReason = "scorer_unavailable";
        return outcome;
    }
    const auto terms = lexicalRerankTerms(query);
    if (terms.empty()) {
        outcome.skipReason = "no_terms";
        return outcome;
    }

    std::vector<std::string> ids;
    ids.reserve(depth);
    for (std::size_t i = 0; i < depth; ++i) {
        ids.push_back(results[i].document.sha256Hash);
    }
    auto scored = scorer(terms, ids);
    if (!scored) {
        outcome.status = LexicalRerankOutcome::Status::Failed;
        outcome.errorMessage = scored.error().message;
        return outcome;
    }
    const auto& lexical = scored.value();

    std::vector<double> fusedZ(depth);
    std::vector<double> lexicalZ(depth, 0.0);
    for (std::size_t i = 0; i < depth; ++i) {
        fusedZ[i] = results[i].score;
        if (const auto it = lexical.find(ids[i]); it != lexical.end()) {
            lexicalZ[i] = it->second;
            ++outcome.matched;
        }
    }
    outcome.depth = depth;
    if (outcome.matched == 0) {
        outcome.skipReason = "no_matches";
        return outcome;
    }

    // Original pool range, kept strictly above the tail for later raw-score sorts.
    const auto [minIt, maxIt] = std::minmax_element(fusedZ.begin(), fusedZ.end());
    const double origMax = *maxIt;
    double low = *minIt;
    if (results.size() > depth) {
        const double tailMax =
            std::max_element(
                results.begin() + static_cast<std::ptrdiff_t>(depth), results.end(),
                [](const SearchResult& a, const SearchResult& b) { return a.score < b.score; })
                ->score;
        if (tailMax >= low) {
            low = std::nextafter(tailMax, std::numeric_limits<double>::infinity());
        }
    }
    const double span = origMax > low ? origMax - low : 1.0;

    zScore(fusedZ);
    zScore(lexicalZ);
    const double w = std::clamp(static_cast<double>(config.lexicalRerankWeight), 0.0, 1.0);
    std::vector<double> blended(depth);
    for (std::size_t i = 0; i < depth; ++i) {
        blended[i] = (1.0 - w) * fusedZ[i] + w * lexicalZ[i];
    }
    const auto [bMinIt, bMaxIt] = std::minmax_element(blended.begin(), blended.end());
    const double bMin = *bMinIt;
    const double bRange = *bMaxIt - bMin;
    for (std::size_t i = 0; i < depth; ++i) {
        const double norm = bRange > 0.0 ? (blended[i] - bMin) / bRange : 1.0;
        results[i].score = low + norm * span;
    }

    // Stable: equal blends keep fused order.
    std::stable_sort(
        results.begin(), results.begin() + static_cast<std::ptrdiff_t>(depth),
        [](const SearchResult& a, const SearchResult& b) { return a.score > b.score; });
    outcome.status = LexicalRerankOutcome::Status::Applied;
    return outcome;
}

} // namespace yams::search::detail
