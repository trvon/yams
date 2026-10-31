// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#include "lexical_rerank_internal.h"

#include <simeon/static_embedding.hpp>

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
                                            const LexicalRerankScorerFn& scorer,
                                            const SemanticScorerFn& semanticScorer) {
    LexicalRerankOutcome outcome;
    if (config.lexicalRerankDepth == 0) {
        outcome.skipReason = "disabled";
        return outcome;
    }
    if (results.empty()) {
        outcome.skipReason = "no_candidates";
        return outcome;
    }
    if (!scorer) {
        outcome.skipReason = "scorer_unavailable";
        return outcome;
    }
    const auto terms = lexicalRerankTerms(query);

    // Semantic scores over the deeper static pool, when available. A failure falls back to the
    // lexical-only blend rather than failing the stage.
    std::unordered_map<std::string, double> semantic;
    std::size_t depth = std::min(config.lexicalRerankDepth, results.size());
    if (semanticScorer && config.staticRerankDepth > 0) {
        const std::size_t staticDepth = std::min(config.staticRerankDepth, results.size());
        std::vector<std::string> hashes;
        hashes.reserve(staticDepth);
        for (std::size_t i = 0; i < staticDepth; ++i) {
            hashes.push_back(results[i].document.sha256Hash);
        }
        if (auto scoredSemantic = semanticScorer(query, hashes); !scoredSemantic) {
            outcome.semanticError = scoredSemantic.error().message;
        } else if (!scoredSemantic.value().empty()) {
            semantic = std::move(scoredSemantic).value();
            outcome.semanticApplied = true;
            depth = staticDepth;
        }
    }
    if (terms.empty() && !outcome.semanticApplied) {
        outcome.skipReason = "no_terms";
        return outcome;
    }

    std::vector<std::string> ids;
    ids.reserve(depth);
    for (std::size_t i = 0; i < depth; ++i) {
        ids.push_back(results[i].document.sha256Hash);
    }
    std::unordered_map<std::string, double> lexical;
    if (!terms.empty()) {
        auto scored = scorer(terms, ids);
        if (!scored) {
            outcome.status = LexicalRerankOutcome::Status::Failed;
            outcome.errorMessage = scored.error().message;
            return outcome;
        }
        lexical = std::move(scored).value();
    }

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
    if (outcome.matched == 0 && !outcome.semanticApplied) {
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
    std::vector<double> blended(depth);
    if (outcome.semanticApplied) {
        // Documents the semantic scorer could not score get the pool mean (z = 0).
        std::vector<double> semanticZ(depth, 0.0);
        std::vector<bool> present(depth, false);
        double sum = 0.0;
        std::size_t n = 0;
        for (std::size_t i = 0; i < depth; ++i) {
            if (const auto it = semantic.find(ids[i]); it != semantic.end()) {
                semanticZ[i] = it->second;
                present[i] = true;
                sum += it->second;
                ++n;
            }
        }
        const double mean = n > 0 ? sum / static_cast<double>(n) : 0.0;
        for (std::size_t i = 0; i < depth; ++i) {
            if (!present[i]) {
                semanticZ[i] = mean;
            }
        }
        zScore(semanticZ);
        const double wl =
            std::clamp(static_cast<double>(config.staticRerankLexicalWeight), 0.0, 1.0);
        const double ws = std::clamp(static_cast<double>(config.staticRerankWeight), 0.0, 1.0 - wl);
        for (std::size_t i = 0; i < depth; ++i) {
            blended[i] = (1.0 - wl - ws) * fusedZ[i] + wl * lexicalZ[i] + ws * semanticZ[i];
        }
    } else {
        const double w = std::clamp(static_cast<double>(config.lexicalRerankWeight), 0.0, 1.0);
        for (std::size_t i = 0; i < depth; ++i) {
            blended[i] = (1.0 - w) * fusedZ[i] + w * lexicalZ[i];
        }
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

Result<std::unordered_map<std::string, double>>
staticEmbeddingScores(const simeon::StaticEmbedding& table, metadata::MetadataRepository& repo,
                      const std::string& query, const std::vector<std::string>& documentHashes,
                      int maxChars) {
    std::unordered_map<std::string, double> scores;
    std::vector<float> q(table.dim());
    if (documentHashes.empty() || !table.encode(query, q)) {
        return scores;
    }
    auto previews = repo.batchGetDocumentsWithContentPreview(documentHashes, maxChars);
    if (!previews) {
        return previews.error();
    }
    std::vector<float> d(table.dim());
    for (const auto& [hash, docAndPreview] : previews.value()) {
        if (!table.encode(docAndPreview.second, d)) {
            continue;
        }
        double dot = 0.0;
        for (std::uint32_t k = 0; k < table.dim(); ++k) {
            dot += static_cast<double>(q[k]) * d[k];
        }
        scores.emplace(hash, dot);
    }
    return scores;
}

} // namespace yams::search::detail
